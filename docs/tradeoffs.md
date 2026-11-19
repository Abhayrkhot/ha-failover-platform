# Trade-offs

This project rebuilds the core of the "external failover manager plus proxy"
pattern for MySQL in order to measure failure handling end to end. This page
covers how it compares with the usual off-the-shelf choices, which design
decisions it makes on purpose and what each one costs, and what a production
deployment would do differently.

## Compared with off-the-shelf options

| | This project | Orchestrator + ProxySQL/HAProxy | Group Replication / InnoDB Cluster | Vitess |
|---|---|---|---|---|
| Replication | Async with semi-sync (`AFTER_SYNC`), no async fallback | Async or semi-sync | Group communication (Paxos-based): each transaction is certified by a majority | Async or semi-sync per shard |
| Failure detection | Coordinator's probe + node agent + primary lease on a majority | "Holistic": the primary looks dead *and* its replicas agree they lost it | Group membership protocol | VTOrc (Orchestrator lineage) |
| Coordination state | Three Redis instances, majority reads/writes, Lua compare-and-set | Its own backend DB, or Raft between Orchestrator nodes | Inside the group itself | A topology service (etcd, ZooKeeper, Consul) |
| Fencing the old primary | Built in: self-fence on lease loss, epochs, router, semi-sync | Left to user-written hooks | A minority partition loses quorum and stops accepting writes | Semi-sync durability policies, plus VTOrc |
| Client routing | Go TCP router that follows the committed topology | ProxySQL or HAProxy, updated by hooks | MySQL Router | vtgate |
| Cost on every commit | One semi-sync round trip | Same, if semi-sync is on | A consensus round, plus flow control | Same as the replication mode |

The managed and consensus-based options are the right answer for running
MySQL in production. This project is the pattern built from first
principles, with every failure mode measured.

## Decisions made on purpose, and what they cost

1. **Leases on a Redis majority instead of consensus (etcd/Raft).** Simple,
   and every operation is one Lua script per instance. Redis leases are
   famously unsafe on their own (Kleppmann vs Redlock): a paused process can
   wake up believing it still holds a lease. So the design does not rely on
   them alone. Epochs order every topology change, the primary self-fences
   before its lease can expire, and semi-sync makes an isolated old primary
   unable to commit. *Cost:* the safety argument has four layers instead of
   one linearizable primitive. With etcd, a compare-and-swap on a revision
   would give the fencing token directly.

2. **Semi-sync with no async fallback (CP over AP).** No acknowledged write is
   ever lost in a failover: 0 of 21,451 in the campaign. *Cost:* with both
   replicas down, writes block rather than continue unprotected (tested in
   `replicas-down`). Every commit also pays a replica round trip.

3. **Waiting for the old primary's lease before promoting.** This is about
   1.5s of the 2.5s recovery, and it guarantees the old primary has stopped
   taking writes. Promotion itself takes about 60ms. *Cost:* recovery time,
   tunable with the lease TTL. A shorter TTL raises the risk of fencing a
   primary that was only slow.

4. **Conservative detection** (3 failed probes 250ms apart, two observers
   that must agree, then the lease). *Cost:* about 1s of detection. The
   payoff is no false positive in any control trial, on a host where probes
   stalled for seconds.

5. **Errant nodes are fenced, never rebuilt automatically.** A node holding
   transactions the primary lacks is reported (`HafoErrantNode`) and left
   alone. Rebuilding is an explicit operation (`make rebuild`, Clone, ~9s).
   *Cost:* a human, or the test harness, has to act. The design never
   silently discards data.

6. **Archiving a replica's binlog, not the primary's.** A primary's binlog can
   hold a commit that no replica acknowledged, and replaying it during a
   restore would bring back data the cluster threw away. *Cost:* restore
   RPO of about 0.5s (0–1 acknowledged writes per drill), the writes still in
   a replica's relay log.

7. **Redis without persistence.** Losing the coordination state is
   survivable, because the topology is rebuilt from the epoch history stored
   inside MySQL (`ha_meta.cluster`). *Cost:* losing a Redis majority stops
   writes until it returns (tested in `redis-majority-down`). That is by
   design, because nothing can be promoted safely without it.

8. **Monitoring that cannot hurt the thing it monitors.** The probe avoids
   `performance_schema.global_status`, which aggregates under a global mutex
   and caused a metastable collapse in M2. `MAX_EXECUTION_TIME` caps
   abandoned probes, and follower coordinators probe at 1Hz.

## What production would change

- **Coordination:** etcd, or Raft inside the coordinators, instead of three
  Redis instances.
- **Routing:** several routers behind a VIP or service discovery, connection
  draining on switch-over, and client-side retries with idempotency keys.
- **Gray failures:** client-side signals (router error rates, latency) as an
  input to detection. Today a broken router→primary link alone does not
  trigger a failover.
- **Backups:** shipped off-host (object storage, encrypted, another failure
  domain), archive pruned alongside backup retention, drills run on a
  schedule.
- **Security:** TLS for replication and clients, and secrets from a manager
  instead of `deploy/dev.env`.
- **Testing:** real hosts with `iptables`/`tc` partitions (including
  asymmetric ones), clock-drift injection, and many concurrent writers. The
  harness's single writer does about 60 commits a second.
