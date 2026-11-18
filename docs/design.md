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
| Detect unhealthy replica within 2s | Inject pause / netem delay / stopped SQL thread; time to "unhealthy" in the coordinator and to the alert firing | Detection latency histogram |
| Restore under 90s across 3 nodes | Wipe all volumes, restore primary from backup + binlogs, rebuild replicas with Clone, compare checksums | Per-phase timings at a stated dataset size |

The numbers in the resume are targets until these runs produce them; the
resume quotes what was measured.

## Milestones

- [x] **M0** Compose stack (3x MySQL GTID + semi-sync, 3x Redis, Prometheus),
      bootstrap, status, netem check, fencing defaults verified.
- [ ] **M1** Agent: probes, heartbeat lag, health to Redis majority, metrics.
- [ ] **M2** Coordinator + router: leader lease, detection, promotion, epochs.
- [ ] **M3** Self-fencing, chaos harness, invariant checker, 30-trial campaign.
- [ ] **M4** Backups (Bash + Go), binlog archiving, timed restore drills.
- [ ] **M5** Results write-up, Grafana dashboard, trade-offs (vs Orchestrator,
      Group Replication).
