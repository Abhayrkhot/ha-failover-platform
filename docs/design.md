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
two. Planned for M2: delay Redis start by more than the max lease TTL
("delayed restart"), so a restarted instance cannot vote until every lease it
might have held has expired.

## Fencing: four layers against split brain

1. **Boot read-only.** `super_read_only=ON` is persisted on every node. A node
   that restarts never comes back writable on its own. (Verified in M0.)
2. **Self-fencing.** The primary's agent sets `super_read_only=ON` if it cannot
   renew its lease on a Redis majority, before the lease's TTL runs out.
3. **Lease wait + epoch.** The coordinator promotes only after the old lease
   has expired plus a clock-drift margin. Each promotion bumps a monotonic epoch;
   the router routes only to the node advertised at the highest epoch and drops
   connections to the old one.
4. **Semi-sync as the last fence.** `rpl_semi_sync_source_timeout` is at its
   max (~49 days), so the source never falls back to async. An old primary whose
   replicas have been repointed cannot commit anything, because nobody will
   acknowledge it. (Verified in M0.) Cost: if both replicas are down, writes
   block. This is a deliberate CP choice.

## Recovery budget (target: < 5s)

| Phase | Time from failure |
|---|---|
| Detection: 250ms probes, several in a row must fail, majority must agree | ~1.5s |
| Old lease (2s TTL, 500ms renew) expires + drift margin | ~2.3s |
| Promote: stop replica, apply relay log, open writes, repoint peer | ~3.0s |
| Router sees new epoch, clients reconnect | ~3.5s |

Detection and the lease wait overlap, which is where the headroom comes from.

## Promotion rules

- Candidate = replica whose `gtid_executed` is a superset of every other
  reachable replica's. Refuse any candidate with GTIDs the others lack
  (errant transactions).
- Lag is measured from `ha_meta.heartbeat` (the primary writes `NOW(6)` every
  250ms), not `Seconds_Behind_Source`, which has 1s granularity and is
  misleading during IO-thread stalls.

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
| Recover service within 5s | Kill primary under load through the router; time from last acked write before to first acked write after | p50 / p95 / max per trial |
| No split brain in 30 injected failures | Mix of kill, `docker pause`, network partition, coordinator-leader kill. Watcher samples every node's `read_only` every 100ms (at most one writable). After each trial: no errant GTIDs, no acked write lost | JSON report, violation count |
| Detect unhealthy replica within 2s | `hafoctl bench detect`: pause, kill, partition, stopped applier, stalled applier, plus control trials; time to "unhealthy" on the Redis majority and to the alert firing | **Measured (M1):** 49/49 valid trials detected; agent max 1.31s, alert max 1.98s; 0/10 control false positives ([report](../results/detect-20261118-191421.json)) |
| Restore under 90s across 3 nodes | Wipe all volumes, restore primary from backup + binlogs, rebuild replicas with Clone, compare checksums | Per-phase timings at a stated dataset size |

The numbers in the resume are targets until these runs produce them; the
resume quotes what was measured.

## Milestones

- [x] **M0** Compose stack (3x MySQL GTID + semi-sync, 3x Redis, Prometheus),
      bootstrap, status, netem check, fencing defaults verified.
- [x] **M1** Agent: probes, heartbeat lag, health to Redis majority, metrics,
      alert rules, detection bench with control trials.
- [ ] **M2** Coordinator + router: leader lease, detection, promotion, epochs.
- [ ] **M3** Self-fencing, chaos harness, invariant checker, 30-trial campaign.
- [ ] **M4** Backups (Bash + Go), binlog archiving, timed restore drills.
- [ ] **M5** Results write-up, Grafana dashboard, trade-offs (vs Orchestrator,
      Group Replication).
