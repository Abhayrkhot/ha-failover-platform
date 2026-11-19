# Design

A failover platform for a 3-node MySQL cluster: a Go coordinator detects a
failed primary, fences it, promotes the most up-to-date replica, and moves
client traffic, with every claim backed by a repeatable experiment.

## Topology

```
            clients / load generator (Go, records every acknowledged write)
                          │
                    ┌─────▼─────┐   reads {epoch, primary} from a Redis majority
                    │  router   │   :3306 -> primary, :3307 -> healthy replicas
                    └─────┬─────┘
       ┌──────────────────┼──────────────────┐
   node-1              node-2             node-3
   mysql 8.4 (P)       mysql (R)          mysql (R)     GTID + semi-sync replication
   redis-1             redis-2            redis-3       health records + leases, majority = 2 of 3
   agent-1             agent-2            agent-3       probe, lag, self-fence, /metrics
       └──────────── coordinator x3 (one leader, lease held on a Redis majority) ──┘
                          │
                     Prometheus (1s scrape/eval) + alert rules
```

## Why three Redis instances

A single Redis would be both a single point of failure and a fake quorum.
Health records and leases are written to all three and count only when a
majority (2 of 3) agrees. This is Redlock-shaped, so Kleppmann's critique
applies: a lease alone cannot stop a paused process that wakes up believing it
still holds it. The fencing layers below are the answer to that.

Redis runs without persistence. A Redis that restarts empty could let a second
coordinator win a majority while the first still holds the lease on the other
two, so each instance sleeps 4s before starting, longer than the longest lease
TTL (leader: 3s). A restarted instance cannot vote until every lease it might
have held has expired ("delayed restart"). The topology has no TTL: the
leader writes it back onto a restarted instance, and if a majority restarts
at once it is rebuilt from MySQL (see the coordinator section).

## Fencing: four layers against split brain

1. **Boot read-only (M0).** `super_read_only=ON` is persisted on every node. A
   node that restarts never comes back writable on its own.
2. **Lease-based self-fencing (M2).** The primary's agent renews a primary
   lease (2s TTL) on the Redis majority every 250ms, but only while its node
   is the committed topology's primary *and* reachable, writable and running
   semi-sync. Without a majority renewal for 1.5s, it makes its own server
   read-only. The coordinator promotes only after the lease is gone from a
   majority plus one tick, i.e. at least 0.75s after the old primary
   stopped taking writes. The agent also fences its server when the
   topology names a different primary (a paused primary waking up).
3. **Claim before act, epochs (M2).** The coordinator claims epoch N+1 on
   the Redis majority (compare-and-set) before it touches MySQL, so only one
   coordinator can own a failover. The router stops sending writes the moment
   it sees the claim, follows only the committed topology, and closes every
   connection to the old primary. Before opening writes on the new primary,
   the coordinator fences every other node it saw writable.
4. **Semi-sync as the last fence (M0).** `rpl_semi_sync_source_timeout` is at
   its max (~49 days), so the source never falls back to async. An old
   primary whose replicas have been repointed cannot commit anything, because
   nobody will acknowledge it. Cost: if both replicas are down, writes block.
   This is a deliberate CP choice.

### How a node is fenced

Fencing a primary is subtle because of layer 4: a commit parked waiting for a
semi-sync ack holds up `SET GLOBAL super_read_only`, and `KILL` does not
interrupt that wait. `mysqlx.Fence` therefore does, on one session:

1. `offline_mode = ON`. Every client session is disconnected and new ones are
   refused. A disconnected session's socket is shut at once, so a commit that
   completes later can never deliver its OK.
2. Kill every thread parked on a semi-sync ack.
3. Turn the semi-sync source off, which releases the parked commits.
4. `super_read_only = ON`, with a 1s lock wait.
5. `offline_mode = OFF`, so the read-only node can serve reads.

Step 3 alone (M2) left a gap (M3 finding): a client connecting between 3 and
4 would commit with semi-sync off, i.e. be acknowledged without any replica
holding the write. Step 1 closes it. If step 4 fails, `offline_mode` stays
on, so a still-writable node keeps clients out.

## Recovery budget (target: < 5s)

Measured over the 30 primary failures of the M3 campaign (kill, pause,
partition; p50 / max):

| Phase | Time |
|---|---|
| Suspicion → primary lease gone from the majority, plus one tick | 1.50s / 1.75s |
| Lease gone → new primary published (claim, drain, promote, repoint, open writes) | 0.06s / 0.15s |
| Outside the coordinator: fault → suspicion (agent's 3 failed probes, 2 ticks), router pickup (50ms poll), the client's next commit | 0.94s / 1.18s |
| **Client-visible recovery: fault → first write acknowledged by the new primary** | **2.49s / 2.87s** |

The lease wait is deliberate: it is what guarantees the old primary has
stopped (layer 2) before anyone else takes writes. Promotion itself takes
about 60ms.

## Promotion rules

- Candidate = replica whose `gtid_executed` is a superset of every other
  reachable replica's. Refuse any candidate with GTIDs the others lack
  (errant transactions).
- Lag is measured from `ha_meta.heartbeat` (the primary writes `NOW(6)` every
  250ms), not `Seconds_Behind_Source`, which has 1s granularity and is
  misleading during IO-thread stalls.

## Coordinator and router (M2)

**Leadership.** Three coordinators compete for a leader lease (3s TTL) on
the Redis majority. Acquire and renew are one Lua script per instance, and
the lease counts only on a majority. The lease is renewed every 500ms through
its own client with a 300ms deadline. Validity is counted from *before* the
request (minus 10% drift), and a failed renewal does not step down: the
leader keeps acting until the lease it already has runs out. Only the
leader acts, and it re-checks leadership before every MySQL change.

**Detection.** The leader declares the primary failed only when three
independent signals agree:
- its own probe of the primary fails, or finds it read-only;
- the primary's agent reports it unreachable or read-only (or no majority
  holds the agent's record);
- the primary's lease is gone from a Redis majority.

The first two must agree for 2 ticks (250ms each), then the lease must stay
absent for one more tick. The lease wait doubles as confirmation: a stall
shorter than the lease TTL lets the agent renew, which resets detection.

**Failover**, in this order:

1. Claim epoch N+1 on the majority (compare-and-set: one winner per epoch).
2. Fence the old primary in the background (it is usually unreachable).
3. Stop every reachable replica's IO thread and wait for its applier to
   drain the relay log.
4. Pick the candidate whose `gtid_executed` contains every other's. If none
   does, the replicas have diverged: the epoch is marked `failed` and a
   human decides.
5. Promote it (`RESET REPLICA ALL`, semi-sync source on), repoint the other
   replicas, and wait until one streams from it.
6. Fence any other node it saw writable, open writes, and publish epoch
   N+1 with the new primary. Then record the epoch in `ha_meta.cluster`, the
   replicated epoch history.

A promotion left unfinished stays `promoting`. Its own coordinator retries
after one step timeout (2s). A new leader resumes someone else's after
holding the lease for one step timeout, so no statement the old leader sent
can still be running. Each attempt claims a fresh epoch.

**Every tick, the leader also:**
- writes the committed topology back onto any instance that lacks it;
- fences any writable node that is not the primary;
- rejoins read-only, non-replicating nodes as replicas, but only if their
  GTIDs are a subset of the primary's.

A node holding transactions the primary lacks (errant GTIDs) is left fenced
and reported (`HafoErrantNode`). Its data is never discarded automatically;
`make rebuild NODE=…` replaces it with a Clone of the primary (M3).

**Topology lost from Redis.** If a Redis majority answers but no record is
held by a majority (e.g. two instances restarted empty), the leader waits
5s, longer than any transient state. It then runs an ordinary promotion with
every reachable node as a candidate and an epoch above anything seen in
Redis or in `ha_meta.cluster`. The most advanced node wins, normally the
old primary, which self-fenced when it lost its lease, so nothing diverged.

**Router.** A TCP proxy (writes on 3306, reads on 3307) that follows only
the committed topology. When the primary changes it closes every write
connection. While an epoch is `promoting` it refuses new write connections.
Reads go round robin over replicas that a majority-held record calls
healthy. If the Redis majority is unreachable it keeps its last view; the
fencing layers, not the router, stop a stale primary.

## Agent (M1)

One agent container per node, next to its MySQL container rather than inside
it, so a paused or killed `mysqld` cannot silence the process that reports it.
Every 250ms (probe timeout 200ms) the agent:

1. **Probes** with one status query (read-only flags, GTIDs, semi-sync state,
   replication threads and errors) plus the heartbeat-lag query.
2. **Folds the probe into a debounced verdict** (`internal/health.Tracker`):
   - *Unreachable* only after 3 failed probes in a row. One dropped packet
     must not pull a replica out of the read pool.
   - Anything a reachable server reports about itself flips the verdict on
     the first probe: a stopped thread, lag > 1s, commits blocked on semi-sync,
     writable without semi-sync, writable while replicating.
   - *Recovery* needs 2 clean probes in a row, so a flapping node does not
     bounce in and out of the pool.
3. **Publishes** the record to all three Redis instances with a 1.5s TTL, each
   write bounded by 100ms. A dead or cut-off agent's record expires on its own,
   and readers take the freshest copy only when a majority hold one.
4. **Writes the heartbeat**, but only on the designated primary: writable *and*
   semi-sync source enabled (see findings). At most one heartbeat commit can
   be parked waiting for an ack. After a timeout the writer waits for a probe
   that confirms the parked commit has finished.

Metrics on `:9100`, all labelled `node`: `hafo_mysql_up`, `hafo_node_healthy`,
`hafo_node_role{role}`, `hafo_replication_lag_seconds`,
`hafo_replication_{io,sql}_running`, `hafo_semi_sync_source_{active,clients}`,
`hafo_heartbeat_blocked_writes`, probe counters, errors by kind and a duration
histogram, and Redis publish results plus quorum. Replica-only and
primary-only series are emitted only while they apply, so "absent" means
unknown, never a misleading zero. Alerts live in
[deploy/prometheus/rules.yml](../deploy/prometheus/rules.yml).

### Detection budget (hung replica, the slowest case)

| Step | Worst case |
|---|---|
| Next probe starts | 250ms |
| 3 consecutive failed probes (200ms timeout, 250ms apart) | +700ms |
| Publish to Redis | +100ms |
| **Agent verdict visible on the Redis majority** | **~1.05–1.2s** |
| Prometheus scrape + rule evaluation (250ms each) | +500ms |
| **Alert firing** | **~1.7s** |

Prometheus is the observability path for humans. The failover path in M2
(agent → Redis → coordinator) never waits on a scrape.

### How detection is measured

`hafoctl bench detect` (`make bench-detect`) breaks one replica, mysql-3, in a
known way and polls two observers from the host clock: the Redis majority's
record (every 10ms) and Prometheus's firing alerts (every 50ms). Before each
trial the node must be healthy and alert-free for 2s. Faults:

| Fault | What it simulates | How |
|---|---|---|
| `pause` | hung process (TCP accepts, nothing answers) | `docker pause` |
| `kill` | crash | `docker kill`, then `docker start` |
| `partition` | network cut, server alive | `docker network disconnect` |
| `stop-sql` | applier stopped | `STOP REPLICA SQL_THREAD` |
| `applier-stall` | applier blocked, threads still ON | `LOCK TABLES ha_meta.heartbeat READ` |
| `control` | nothing: false-positive check | — |

Rules that keep the numbers honest:
- **Conservative start time.** Latency is measured from *before* the
  injecting call, so it includes the `docker` CLI's own time.
- **Threshold-aware lag timing.** For `applier-stall`, the replica only
  becomes unhealthy once lag crosses 1s. The bench reads the lag at
  lock time to compute that moment and measures detection from it.
- **Noise is counted, not hidden.** `control` trials run the same procedure
  with no fault; any detection there is a false positive. A trial is marked
  *suspect*, reported, and excluded from the statistics if the agent's reason
  doesn't match the fault (e.g. `unreachable` during `stop-sql`) or if an
  outage was "detected" faster than 3 probes allow.
- **Only real detection alerts count:** `HafoNodeUnhealthy`,
  `HafoReplicationLagHigh` and `HafoReplicationStopped`. The error-rate alert
  is a warning, not detection.

Each run writes a JSON report to `results/` with the commit, the agent and
Prometheus timing config, every trial, and the summary.

## Findings in M1

- **Writable is not primary.** A freshly initialised MySQL boots writable, and
  for about 6s during bootstrap the first agent build wrote heartbeats to *every*
  writable node. They failed only because `ha_meta` didn't exist yet. Had they
  succeeded, each replica would have had errant GTIDs, so bootstrap would
  refuse to run and later promotion would be unsafe. The agent now writes
  heartbeats only where semi-sync source is enabled, a runtime-only flag that
  only the process opening the primary sets.
- **The lab host is overcommitted, and it shows.** With three kind clusters
  sharing the Docker Desktop VM, pressure-stall counters read CPU 57–68%,
  memory 3–12%, IO 9–23% (some tasks stalled, 60s/300s averages). Swap was
  full and only 151MB was free. Stalls of 0.5–2.6s hit every container at once:
  probe queries that normally take 3ms spiked to 2.6s server-side, and all three
  agents missed Redis writes together. In one 60s idle window, the agents
  flipped unhealthy 15 times between them. This is why the bench has `control`
  trials. Benchmarks should be run, or re-run, with the other workloads stopped.
- **Commit latency on this disk.** Heartbeat commits (semi-sync, `sync_binlog=1`)
  averaged 59ms server-side, with p99 500ms. That matters for M2's recovery
  budget: promotion is only finished once the new primary can commit.
- **Docker Desktop serves truncated bind-mounted files.** A running container
  keeps the *old size* of a file edited on the host, so it reads the new content
  cut off at the old length. It happened twice. First a reload silently dropped
  a rule group. Then a reload of the 250ms `prometheus.yml` was cut right after
  the first job, so the agents scrape job and every target vanished. The rules
  still evaluated, over no data, and the first full benchmark recorded *zero*
  alerts. Both cuts parsed as valid YAML, so Prometheus reported success.
  `make prom-reload` now compares checksums inside and outside the container,
  recreates the container when they differ (a fresh mount reads the real
  file), and only then checks that every rule loaded and all 3 agent targets
  are up. The same caution applies to `deploy/mysql/hafo.cnf`: after editing
  it, recreate the MySQL containers (`up --force-recreate`), not just restart them.
- **Partition healing is slow and noisy.** After `docker network connect`,
  the agent couldn't reach mysql-3 for about 7s (stale neighbour entries), and
  agents on *other* nodes briefly missed Redis writes. M3's chaos runs must
  treat network faults as having blast radius beyond their target.
- **500ms scrape/eval was not enough** for the alert path: hang detection
  through Prometheus reached 2.4s in validation. It now runs at 250ms (above).

## Findings in M2

- **A metastable probe collapse.** With three coordinators added, every node
  got 16 status probes a second, and a probe that timed out (200ms) was
  abandoned while MySQL kept running it. The probe read
  `performance_schema.global_status`, which aggregates every connection's
  counters under one global mutex. Abandoned probes queued on that mutex,
  each new one waited longer, and within a minute 54 queries were stuck in
  "statistics" for up to 33s and bootstrap failed with `Too many
  connections`. Fix: the probe no longer reads global status (it counts
  binlog dump threads instead), `MAX_EXECUTION_TIME` caps abandoned probes
  server-side, and follower coordinators probe once a second.
- **Leadership flapped 8 times in 3.5 minutes.** The leader stepped down,
  and deleted its key, on the first renewal that missed a 100ms Redis
  deadline. Now a missed renewal changes nothing until the lease already
  granted runs out, and lease traffic has its own client with a 300ms
  deadline, renewed every 500ms.
- **A margin quantized to ticks.** A 300ms lease margin checked on 250ms
  ticks meant two ticks (500ms) on every failover; it is now one tick.
- **Fencing a primary with parked commits** (found by the e2e suite on the
  box). `KILL` does not interrupt a semi-sync wait and that wait blocks
  read-only, so a woken paused primary stayed writable for 180s. Releasing
  the parked commits brought it to 300ms; M3 made the release safe (see
  "How a node is fenced").
- **Rebuild by replay** needed `replicate_same_server_id` (the IO thread
  dropped events carrying the node's own server_id, leaving a hole) and a
  serial applier for the first catch-up. M3 replaced replay with Clone.

## Chaos campaign (M3)

`make bench-failover ARGS="-faults=control,kill,pause,partition,leader-midway -trials=10"`
breaks the current primary 10 times per fault while one client writes
through the router, about 60 inserts a second, each row recording which
server executed it. The next trial fails whichever node is primary by then.
After each fault the harness heals the old primary and waits until it is a
healthy replica of the new one. If it holds errant transactions, the harness
first records what it alone has, then rebuilds it with Clone.

Invariants checked on every trial:

| Invariant | How |
|---|---|
| No acknowledged write lost | every write the client saw succeed is on the new primary |
| The client never saw two primaries | no write acknowledged by the old primary after the new one acknowledged one; none by a third node |
| Rows only the old primary had were never acknowledged | before a rebuild, diff its `app.writes` against the primary's and check each extra row against the client's log |
| Exactly one failover per fault | epoch delta (a killed mid-promotion leader accounts for one abandoned epoch) |
| Never two nodes writable at once | `@@read_only` sampled on every node every 100ms |
| All nodes identical afterwards | pause writes, wait for every replica to reach the primary's GTID set, compare `CHECKSUM TABLE app.writes` |
| No failover without a fault | control trials |

Results ([report](../results/failover-20261119-055014.json), on the Linux box):

| Fault | Trials | Recovery p50 / max | Lost acked | Two primaries seen | Diverged rows acked | Extra failovers | Inconsistent | Rebuilt (errant) |
|---|---|---|---|---|---|---|---|---|
| control | 10 | n/a | 0 | 0 | 0 | 0 | 0 | 0 |
| kill | 10 | 2.47s / 2.86s | 0 | 0 | 0 | 0 | 0 | 0 |
| pause (hung, then wakes) | 10 | 2.30s / 2.81s | 0 | 0 | 0 of 10 | 0 | 0 | 10 |
| partition | 10 | 2.53s / 2.87s | 0 | 0 | 0 of 1 | 0 | 0 | 1 |
| leader-midway | 10 | 7.52s / 8.60s | 0 | 0 | 0 of 1 | 0 | 0 | 1 |

- **30 primary failures, 0 acknowledged writes lost** out of 21,451
  acknowledged in the measured windows. No client ever saw two primaries.
- **The 12 rows only an old primary held were all unacknowledged.** They are
  commits that reached its binlog while it waited for an ack no replica
  would give. They are why pause trials always need a rebuild, and why the
  rebuild is safe.
- **The read-only flag was off on two nodes for up to 200ms.** That happened
  when a paused primary woke up, before its agent fenced it. That node had no
  route from the router and no replica to acknowledge it, so it took no
  writes; the acked-write invariants above are the actual split-brain test.
- **`leader-midway`** kills the leading coordinator right after it claims the
  next epoch, before it promotes anyone. All 10 recovered under a newer
  epoch. The extra ~5s is the leader lease (3s TTL) plus the one step
  timeout (2s) the new leader waits before taking over a foreign claim.

The e2e suite (`make e2e`, 12 scenarios, 78 checks) covers what the campaign
does not: replica loss, both replicas lost (writes stop rather than fail
over), one Redis down, two Redis down (the primary self-fences, then the
topology is rebuilt from MySQL), router restart, coordinator-leader crash,
and leader and primary dying together. All 12 pass
([report](../results/e2e-20261119-054454.json)).

## Findings in M3

- **Releasing parked commits opened an ack-without-replica window.** M2's
  fence turned semi-sync off to release commits parked on an ack. A client
  connecting between that and `super_read_only` would commit asynchronously
  and be told it succeeded while no replica had the write; a failover could
  then lose it. `offline_mode` now shuts clients out first (see "How a node
  is fenced").
- **Rebuild by replay stopped finishing.** Replaying the whole binlog into an
  empty datadir with a serial, fsync-per-transaction applier took longer
  with every write. By ~18,000 transactions a rebuild no longer finished
  within 3 minutes, and the e2e suite failed on it. A rebuild now clones the
  primary (MySQL Clone plugin) in about 9s, copy plus restart plus rejoin,
  independent of history. This pulls part of M4 forward.
- **CLONE is refused under `super_read_only`.** The rebuild drops only
  `super_read_only` and keeps plain `read_only` on. That still blocks every
  client without SUPER, and it is the flag the agents, coordinator and
  router read, so nobody treats the node as a writable stray during the
  copy. The restart restores the persisted `super_read_only`.

## Backup and restore (M4)

**Backups** (`make backup`, `scripts/backup.sh`) are hot physical copies made
with MySQL Clone: `CLONE LOCAL DATA DIRECTORY` on a healthy replica, so the
primary's I/O is left alone. The copy goes into a `backups` volume that
every MySQL node mounts and that outlives their data volumes. Each backup
is a directory plus a JSON file recording the source, size, duration and
the GTID sets on either side of the snapshot. The newest `KEEP` (3) are kept.

**The binlog archive** (`cmd/hafo-archiver`) runs `mysqlbinlog --raw
--stop-never` against a healthy replica and writes the stream into a
`binlogs` volume, one directory per source `server_uuid`. It keeps the same
source while it stays healthy, switches when it doesn't, and after a
restart re-fetches the newest, possibly partial, file. It archives a
*replica* on purpose. A primary's binlog can hold a commit that no replica
ever acknowledged (the errant rows of M3), and replaying the archive must
never bring back data the cluster threw away. The cost is RPO: a write
acknowledged into a replica's relay log but not yet applied is not in the
archive yet.

**The restore drill** (`make drill`, `hafoctl drill`) proves both:

1. Take a backup, then keep writing through the router (20s by default) so
   the restore has to roll forward.
2. Disaster: kill all three MySQL nodes at once, delete their data volumes,
   and remove Redis and every platform container. Only `backups` and
   `binlogs` survive.
3. Restore, timed from here:
   - Copy the backup into three fresh volumes in parallel. Drop `auto.cnf`,
     so each node generates its own `server_uuid`.
   - Start MySQL, and clear the replication state the copy carried over from
     the replica it was taken on.
   - On one node, replay the archive oldest source first, through
     `mysqlbinlog --exclude-gtids=<the backup's GTID set>`. That skips
     what the backup already holds without decoding it, and the server
     auto-skips anything else already applied. Per-commit fsyncs are off
     during the replay; nobody is served from that node yet.
   - Start the platform. The coordinator finds no topology, rebuilds it from
     the epoch history inside the restored data, and promotes the most
     advanced node, the one that replayed the archive. The other two catch
     up by replication.
4. Verify:
   - every write acknowledged before the disaster is back (RPO in writes,
     and how far back the loss reaches);
   - the seeded `app.payload` checksum on all three nodes equals its
     pre-disaster value;
   - all three nodes hold identical `app.writes`.

Results (Linux box, NVMe; one JSON report per drill in `results/drill-*`):

| Dataset (InnoDB data + index) | Backup (size / time) | Restore, empty → 3 healthy nodes serving | Acked writes lost |
|---|---|---|---|
| 0.2 GiB | 0.3 GiB / 0.6s | 35.7s | 0 of 759 |
| 1.4 GiB (2 drills) | 1.5 GiB / 1.1–2.6s | 44.0s, 45.0s | 1 of 2,322; 0 of 1,981 |
| 5.5 GiB (3 drills, before the replay fix below) | 5.6 GiB / 6.6–8.7s | 62.2s, 64.8s, 68.7s | 1, 1, 1 |
| **5.5 GiB (3 drills, final)** | **5.6 GiB / 6.3–7.9s** | **51.3s, 46.8s, 49.5s** | **0, 0, 1** |

Where the time goes at 5.5 GiB (final drills):

| Phase | Time |
|---|---|
| Copy the backup into 3 volumes in parallel | 12.2–13.2s |
| Start MySQL on the restored data | 6.1s |
| Replay the archive (~1,170 transactions written after the backup) | 10.7–12.9s |
| Start Redis (4s delayed start), agents, coordinators, router | 7.0–9.2s |
| Coordinator: topology-lost wait (5s) + promotion | 5.5–6.0s |
| Replicas catch up, writes acknowledged | 3.7–6.0s |

- **Restore is linear in data size only where it has to be.** Only the copy
  and the MySQL start depend on the dataset; the rest is fixed cost.
- **The data checks passed in every drill they ran in.**
  - After every drill, `app.writes` was identical on all three nodes.
  - In all 7 drills run with the fixed checksum, the seeded payload's checksum
    on all three nodes equals its value before the disaster.
- **RPO is 0 or 1 acknowledged write**, and the lost one was always written
  in the last ~0.4–0.5s before the disaster. That is the cost of archiving a
  replica (see above). Inside the cluster, failover RPO is 0 (semi-sync); the
  archive trades half a second for never resurrecting an unacknowledged write.

## Findings in M4

- **Replay time grew with the archive, not with the work.**
  `--exclude-gtids` skips old transactions but still reads every file, and
  the archive held every past source plus the 3 GiB seed. Replay rose from
  25s to 30s across three drills that each rolled forward the same ~1,150
  transactions. The replay now skips whole files by their `Previous-GTIDs`
  header: a file is skipped if the next one starts from a GTID set the backup
  already holds. Replay fell to 11–13s and the restore from ~65s to ~49s.
  What remains is the last file of each dead source, which retention pruning
  of the archive (not built) would remove.
- **A checksum that timed out read as 0.** `CHECKSUM TABLE` on 1.4 GiB
  outlasted the harness connection's 5s read timeout. The helper swallowed
  the error, and one drill reported a payload "mismatch" between a real
  checksum and that 0. Checksums now run through the `mysql` client inside
  the container, errors fail the drill, and the payload is compared on all
  three nodes.
- **The official image has no `mysqlbinlog`.** The client package conflicts
  with the image's server-minimal package. The tools image extracts just
  `mysqlbinlog` from MySQL's signed RPM, checking the signature against the
  key the image already trusts.
- **`information_schema` caches table sizes for a day**
  (`information_schema_stats_expiry`), so a freshly seeded table reported
  0 MiB. Sizes are read with the expiry set to 0 on the same session.

## Findings in M5

- **Grafana's `$__rate_interval` breaks with a sub-second scrape interval.**
  With `timeInterval: 250ms`, Grafana 11.5 renders it as decimal seconds,
  for example `rate(x[1.165s])`. Prometheus rejects that ("unknown unit
  '.'"), so a panel failed or rendered depending on its width and time
  range. The dashboard uses fixed `[5s]` windows (20 scrapes) instead.
- **The role and health timelines rendered all grey** with colors set only
  in their value mappings, under the "thresholds" color mode. Giving each
  value a threshold color fixed it; the mappings still supply the labels.

## Behaviors found in M0 that shape later milestones

- **A timed-out write can still commit.** With semi-sync AFTER_SYNC, a
  transaction is in the source's binlog before it waits for an ack. In M0, a
  client gave up after 5s on an isolated primary, the server thread kept
  waiting, and when replicas reconnected the row committed on all three nodes.
  - M3 load generator: classify each write as acked, failed, or *unknown*
    (timeout). The invariant is "no acked write lost", and unknown writes are
    reported separately, not counted as lost.
  - M2 rejoin: a fenced old primary may hold such a transaction that no
    replica has. That is an errant GTID; the coordinator must detect it and
    rebuild the node (Clone plugin) instead of repointing it.
- **Docker Desktop and init scripts.** Bind-mounted files pass `test -x` for
  root even at mode 644, so the mysql entrypoint executes them instead of
  sourcing them. `01-users.sh` is a real executable for that reason.
- **Init must not write GTIDs.** Users are created with `sql_log_bin=0`, and
  the entrypoint's timezone load is skipped, so every node starts with an
  empty `gtid_executed`. Bootstrap refuses to run otherwise.

## Claims and how each is measured

| Resume claim | Experiment | Output |
|---|---|---|
| Recover service within 5s | `hafoctl bench failover`: break the primary under write load through the router; time from fault to first write acknowledged by the new primary | **Measured (M3):** 30/30 primary failures recovered, p50 2.49s, max 2.87s ([report](../results/failover-20261119-055014.json)) |
| No split brain in 30 injected failures | Same campaign (kill, pause, partition; plus 10 coordinator-leader kills mid-promotion). Per trial: no acked write lost, client never saw two primaries, rows only the old primary had were never acked, one epoch per fault, checksums equal on all nodes | **Measured (M3):** 0 violations of any invariant across 30 + 10 failures; 0 of 21,451 acked writes lost; 0 failovers in 10 control trials |
| Detect unhealthy replica within 2s | `hafoctl bench detect`: pause, kill, partition, stopped applier, stalled applier, plus control trials; time to "unhealthy" on the Redis majority and to the alert firing | **Measured (M3 re-run, Linux box):** 50/50 detected; agent max 0.92s, alert max 1.23s; 0/10 control false positives ([report](../results/detect-20261119-060340.json)). M1 on the overloaded Mac: agent max 1.31s, alert max 1.98s ([report](../results/detect-20261118-191421.json)) |
| Restore under 90s across 3 nodes | `hafoctl drill`: Clone backup of a replica, keep writing, then kill all 3 nodes and delete their data and Redis; restore all 3 from the backup, replay the binlog archive, let the coordinator rebuild the topology; verify acked writes, payload checksums on all 3 nodes, consistency | **Measured (M4):** 5.5 GiB restored onto 3 nodes in 46.8–51.3s (3 drills); 0–1 acked writes lost (the last ~0.5s); checksums identical on all nodes ([reports](../results/)) |

The numbers in the resume are targets until these runs produce them; the
resume quotes what was measured.

## Milestones

- [x] **M0** Compose stack (3x MySQL GTID + semi-sync, 3x Redis, Prometheus),
      bootstrap, status, netem check, fencing defaults verified.
- [x] **M1** Agent: probes, heartbeat lag, health to Redis majority, metrics,
      alert rules, detection bench with control trials.
- [x] **M2** Coordinator + router: leader lease, detection, promotion, epochs.
      Measured: 11/11 e2e scenarios pass ([report](../results/e2e-20261119-033232.json)); kill-primary
      recovery 2.7-3.5s, 0 acked writes lost.
      Found in the e2e: Fence now turns semi-sync off to release parked commits (a woken
      paused primary stayed writable 180s, now 300ms); a node rebuilt by replay needs
      `replicate_same_server_id` and a serial applier. M4 replaces replay with Clone.
- [x] **M3** Chaos campaign: 30 primary failures plus 10 mid-promotion leader
      kills, 0 invariant violations, recovery max 2.87s. Safe fencing
      (`offline_mode`), divergence check before rebuilds, Clone-based
      rebuild. (Self-fencing itself landed in M2.)
- [x] **M4** Backups (Clone, Bash), binlog archive of a replica (Go),
      restore drill (Go): 5.5 GiB onto 3 nodes in ~49s, RPO ≤1 acked write.
- [x] **M5** Grafana dashboard (provisioned, 22 panels, `make screenshot`),
      [trade-offs](tradeoffs.md), [interview notes](interview-notes.md),
      results in the README.
