# Interview notes

Talking points for the resume project, with the measured evidence behind
each claim. Every number links back to a JSON report in `results/`.

## The 60-second version

A failover system for a 3-node MySQL cluster, written in Go. Each node runs
an agent that probes MySQL four times a second and publishes a health record
to three independent Redis instances. Three coordinators elect a leader
through a lease held on a Redis majority. When the primary dies, the leader
waits until the old primary's lease has lapsed, which guarantees it has
stopped taking writes. It then claims a new epoch, promotes the replica that
holds every other replica's transactions, and publishes the new topology. A
Go router follows that topology. In a chaos campaign of 30 primary failures,
writes came back in 2.5s (p50) and 2.9s (max), with zero acknowledged writes
lost and zero split-brain invariant violations.

## The resume bullets and the evidence

| Bullet | Measured | Where |
|---|---|---|
| Go failover coordinator using MySQL replication and Redis health state; recovers service within 5s | 30/30 primary failures (crash, hang, partition) recovered; fault → first write acknowledged by the new primary: p50 2.49s, max 2.87s | [failover report](../results/failover-20261119-055014.json) |
| Quorum-based leader fencing and replica promotion; no split brain during 30 injected node failures | 0 violations across 30 primary failures plus 10 coordinator-leader kills mid-promotion: 0 of 21,451 acked writes lost, the client never saw two primaries, every row only an old primary held was unacknowledged, one epoch per fault, identical checksums after every trial, 0 failovers in 10 control trials | same report; [e2e](../results/e2e-20261119-054454.json) |
| Prometheus metrics for replication lag and error rates; detects unhealthy replicas within 2s | 50/50 replica faults (hang, crash, partition, stopped and stalled applier) detected; agent verdict on the Redis majority ≤0.92s, Prometheus alert firing ≤1.23s; 0/10 control false positives | [detect report](../results/detect-20261119-060340.json) |
| Backups and recovery drills; restores data in under 90s across 3 nodes | M4, not built yet. A single-node rebuild with MySQL Clone already takes ~9s | — |

If the numbers are quoted, quote the measured ones: "within 3 seconds
(p50 2.5s)" is more impressive and more defensible than "within 5".

## Questions to expect, and the answers

**Why three Redis instances instead of one?** One Redis is a single point of
failure and a fake quorum. Everything (health records, leases, the topology)
goes to all three and counts only when a majority agrees. Losing one Redis
changes nothing (tested). Losing two stops writes by design (tested, below).

**Isn't a Redis lock unsafe? (Kleppmann vs Redlock.)** Yes. A lease alone
cannot stop a process that pauses and wakes up believing it still holds it.
That is why there are four fencing layers, and why the last one does not
depend on clocks or Redis at all:
1. Every node boots read-only.
2. The primary fences *itself* if it cannot renew its lease on a majority
   within 1.5s, before the 2s lease can expire.
3. The coordinator claims a monotonically increasing epoch
   (compare-and-set) before touching MySQL; the router follows only the
   committed epoch.
4. Semi-sync replication never falls back to async, so an old primary whose
   replicas have moved on cannot commit anything.

The `pause` faults are exactly the Kleppmann scenario: the primary freezes,
a new one is promoted, the old one wakes up still thinking it is primary. In
10/10 trials it took no client writes, and it was fenced (by its agent or the
coordinator, whichever acted first) within about 200ms.

**Why wait ~1.5s for the lease? Promotion only takes 60ms.** That wait is the
safety margin. It guarantees the old primary has stopped taking writes, by
its own self-fence, before anyone else is allowed to. Recovery is 2.5s
instead of ~1s, and the trade buys "never two writers". The lease TTL is the
knob.

**How do you pick the new primary?** Stop every replica's IO thread, let its
applier drain the relay log, then pick the replica whose `gtid_executed`
contains every other replica's. If none does, the replicas have diverged:
the coordinator stops and marks the epoch `failed` for a human, rather than
promoting something that would lose data.

**What's an errant GTID and why does it happen here?** With semi-sync
`AFTER_SYNC`, a commit is in the primary's binlog *before* a replica
acknowledges it. If the primary dies or freezes at that moment, it holds a
transaction nobody else has. That transaction was never acknowledged to the
client, so it is safe to discard, but the node cannot simply rejoin. The
coordinator leaves it fenced and reports it. Before every rebuild the test
harness diffs the node's rows against the primary's and checks each extra
row against the client's log: 12 such rows across the campaign, 0 had been
acknowledged.

**What happens if the coordinator dies mid-failover?** Tested 10 times by
killing the leader right after it claimed the next epoch. Another coordinator
takes the lease (3s TTL), waits one more step timeout (2s) so nothing the
dead leader sent can still be running, and finishes under a newer epoch.
Recovery was 7.5s p50, with 0 writes lost.

**What if Redis loses its majority?** Tested: stop 2 of 3. The primary
cannot renew its lease, so it fences itself after 1.5s and writes stop. That
is the CP choice: without the coordination store nothing can be promoted
safely, so nothing is. When Redis comes back it is empty (no persistence),
so the topology is gone. The coordinator rebuilds it from the epoch history
replicated inside MySQL (`ha_meta.cluster`) and promotes the most advanced
node. Writes resumed about 11s after Redis returned.

**What if both replicas die?** Writes block (semi-sync with no fallback) and
the coordinator does not fail over, because the primary is fine. Writes
resume as soon as one replica is back. Tested.

## Bugs worth telling as stories

- **A metastable failure in the monitoring itself.** Adding coordinators
  quadrupled probe traffic. Probes that timed out were abandoned client-side
  but kept running in MySQL, all queued on the global mutex behind
  `performance_schema.global_status`. Each new probe waited longer, so more
  timed out, until `max_connections` was exhausted. The fix went at all
  three causes: no global-status reads in the probe, `MAX_EXECUTION_TIME`
  server-side, and followers probing at 1Hz.
- **Leadership flapping.** The leader gave up its lease on the first renewal
  that missed a 100ms deadline: 8 leader changes in 3.5 minutes. A lease
  already granted is valid until it expires; a missed renewal should change
  nothing.
- **A fence that opened a hole.** To make a node read-only you must first
  release commits parked on a semi-sync ack, and turning semi-sync off does
  that. But a client connecting in the gap would then commit asynchronously
  and be told it succeeded, with no replica holding the write. The fix is
  `offline_mode`, which disconnects clients and refuses new ones before the
  release.
- **Rebuild by binlog replay stopped scaling.** It took longer with every
  write, and stopped finishing within 3 minutes after ~18,000 transactions.
  MySQL Clone copies the primary physically in about 9s, regardless of
  history.
- **A truncated config file.** Docker Desktop served a running container a
  stale-size copy of an edited config. It still parsed, so Prometheus lost
  its scrape targets without any error. The reload script now compares
  checksums inside and outside the container.

## Limitations, and what production would add

- **The router is a single instance.** Production would run several behind
  a VIP or DNS, or put the routing into the client library.
- **Gray failures are not handled.** If only the router→primary link breaks,
  the primary looks healthy to everyone else and nothing fails over.
  Detection from the client's side (error rates at the router) would cover it.
- **Redis has no persistence**, by design: the topology can be rebuilt from
  MySQL. But a total Redis loss halts writes until Redis returns.
- **Leases assume bounded clock-rate drift.** This is standard, but worth
  saying; epochs and semi-sync are the clock-free backstops.
- **The lab runs on Docker on one machine**, so a "partition" is
  `docker network disconnect`. That cuts the whole MySQL container off, not
  an asymmetric slice of the network.
- **Off-the-shelf alternatives:** Orchestrator plus a proxy, MySQL Group
  Replication / InnoDB Cluster (Paxos-based), Vitess. This project rebuilds
  the core of the Orchestrator-plus-proxy pattern to understand and measure
  the failure handling.
