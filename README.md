# ha-failover-platform

Failover coordinator for a 3-node MySQL cluster, written in Go: Redis-majority
health state and leases, layered fencing against split brain, Prometheus
metrics, chaos testing, and timed backup/restore drills.

Status: **M3**. The cluster, health agents, coordinators and router are
done, plus a chaos campaign: 30 primary failures, recovered in 2.49s p50 and
2.87s max, with 0 acknowledged writes lost and 0 split-brain invariant
violations. Backup and restore drills (M4) are next. See
[docs/design.md](docs/design.md) for the architecture, fencing model,
budgets, findings and milestone plan, and
[docs/interview-notes.md](docs/interview-notes.md) for the claims, the
evidence behind each, and the limitations.

## Quickstart

Requires Docker (Desktop on macOS is fine; images are multi-arch), Go 1.27+, and `make`.

```bash
make bootstrap       # build images, start everything, wire mysql-1 -> mysql-2/3 semi-sync, publish epoch 1
make status          # what MySQL itself says: role, read-only flags, replication threads, GTIDs
make health          # topology, leader, and each agent's verdict as a Redis majority sees it
make test            # unit tests (race detector on)
make e2e             # 12 scenarios that break and repair the cluster; ARGS=-only=primary-hang
make bench-failover  # fail the primary under write load, check split-brain invariants; ARGS=-trials=3
make bench-detect    # break a replica repeatedly, time detection; ARGS=-trials=3
make rebuild NODE=mysql-1  # replace a fenced node holding errant transactions with a Clone of the primary
make clean           # stop and delete all data
```

Clients connect to the router: `127.0.0.1:23306` for writes, `23307` for
reads. The benches and e2e suite break the cluster on purpose and repair it.
Run only one of them at a time against a cluster.

`make down` keeps the MySQL data, but every node comes back **read-only**
(intentional fencing), and Redis comes back empty, so the topology is gone.
After `make up` the coordinator rebuilds it from the epoch history stored in
MySQL and reopens the most advanced node. Measured: serving with all nodes
healthy 6s after the containers were up.

After editing anything under `deploy/prometheus/`, run `make prom-reload`. It
reloads and then checks that Prometheus loaded every rule; see the M1 findings
in the design doc for why that check exists.

## What's running

| Service | Host port | Notes |
|---|---|---|
| mysql-1 / 2 / 3 | 127.0.0.1:33061 / 33062 / 33063 | MySQL 8.4.13, GTID, semi-sync AFTER_SYNC, never falls back to async |
| agent-1 / 2 / 3 | 127.0.0.1:19101 / 19102 / 19103 | `/metrics`, `/status` (config + latest record), `/healthz` |
| redis-1 / 2 / 3 | 127.0.0.1:63791 / 63792 / 63793 | No persistence; health records carry a 1.5s TTL |
| coordinator-1 / 2 / 3 | 127.0.0.1:19201 / 19202 / 19203 | One leads (3s lease on the Redis majority); `/status` shows its view |
| router | 127.0.0.1:23306 / 23307, metrics 19300 | Writes to the committed primary, reads to healthy replicas |
| prometheus | 127.0.0.1:19090 | 250ms scrape and rule evaluation; alerts in `deploy/prometheus/rules.yml` |

Users (created on each node with `sql_log_bin=0`, passwords in
[deploy/dev.env](deploy/dev.env), lab use only): `repl` (replication), `hafo`
(agent/coordinator, Clone donor), `app` (clients).

## Layout

```
cmd/hafo-agent/            per-node agent: probe, verdict, heartbeat, primary lease, self-fencing, metrics
cmd/hafo-coordinator/      leader election, failure detection, promotion, fencing, rejoin
cmd/hafo-router/           TCP proxy that follows the committed topology
cmd/hafoctl/               host-side CLI: `health`, `bench detect`, `bench failover`, `e2e`
internal/cluster/          topology, leases and failover records on the Redis majority (Lua CAS)
internal/coordinator/      detector, candidate choice, failover, reconcile loop
internal/router/           proxying, switching and refusing write connections
internal/mysqlx/           the shared probe and fence
internal/health/           observation -> debounced verdict; the published record
internal/store/            parallel reads/writes across the Redis instances, quorum math
compose.yaml, Dockerfile   nodes, network, volumes; one image with all three binaries
deploy/mysql/              replication/durability/semi-sync settings, first-boot users
deploy/prometheus/         scrape config (250ms), alert rules
deploy/nettools/           alpine + tc/iptables, run in a node's netns
scripts/                   bootstrap, status, rebuild (Clone), prom-reload, netem check
results/                   JSON reports from benchmark runs
docs/design.md             architecture, fencing, budgets, findings, claims -> experiments
```

## Results

### Failover and split brain (M3)

`make bench-failover ARGS="-faults=control,kill,pause,partition,leader-midway -trials=10"`,
run 2026-11-19 on the Linux box, report
[results/failover-20261119-055014.json](results/failover-20261119-055014.json).
One client wrote through the router the whole time. Recovery is measured
from the fault to the first write acknowledged by the new primary:

| Fault on the primary | Trials | Recovery p50 / max | Acked writes lost | Client saw two primaries | Rows only the old primary had that were acked |
|---|---|---|---|---|---|
| kill (crash) | 10 | 2.47s / 2.86s | 0 | 0 | none |
| pause (hang, then wake up) | 10 | 2.30s / 2.81s | 0 | 0 | 0 of 10 |
| partition (network cut) | 10 | 2.53s / 2.87s | 0 | 0 | 0 of 1 |
| leader-midway (primary dies, then the coordinator leader dies mid-promotion) | 10 | 7.52s / 8.60s | 0 | 0 | 0 of 1 |
| control (no fault) | 10 | no failover | 0 | 0 | none |

- **0 of 21,451 acknowledged writes lost.** Every trial ended with exactly
  one failover and identical checksums on all three nodes.
- **About 60ms of recovery is promotion.** The rest is the deliberate wait
  for the old primary's 2s lease, which guarantees it has stopped, plus
  detection.
- **Errant old primaries were rebuilt with Clone** (12 rebuilds, ~9s each).
  Before each rebuild, every row only that node held was checked against the
  client's log, and none had been acknowledged.
- **The e2e suite passes all 12 scenarios** (78 checks,
  [report](results/e2e-20261119-054454.json)). That includes losing both
  replicas (writes stop rather than fail over), losing 2 of 3 Redis (the
  primary fences itself, then the topology is rebuilt from MySQL), and the
  coordinator leader dying together with the primary.

### Detection (M1, re-run in M3)

Re-run on 2026-11-19 on the Linux box with the M2 probe
([report](results/detect-20261119-060340.json)): all 50 fault trials
detected, none suspect. The agent's worst case was **0.92s** and the alert's
**1.23s**, with 0/10 control false positives. The M1 run below was on the
overloaded Mac.

`make bench-detect ARGS=-trials=10`, run 2026-11-18, report
[results/detect-20261118-191421.json](results/detect-20261118-191421.json).
Replica mysql-3, 10 trials per fault. Each number is the latency from the
moment the replica became unhealthy to the moment it was detected, in ms:

| Fault | Agent → Redis majority (p50 / p95 / max) | Prometheus alert firing (p50 / p95 / max) |
|---|---|---|
| pause (hung process) | 779 / 980 / 980 | 1127 / 1234 / 1234 |
| kill (crash) | 768 / 1265 / 1265 | 1182 / 1325 / 1325 |
| partition (network cut) | 801 / 1313 / 1313 | 1428 / 1980 / **1980** |
| stop-sql (applier stopped) | 98 / 459 / 459 | 566 / 1672 / 1672 |
| applier-stall (lag > 1s)\* | 167 / 248 / 248 | 563 / 1008 / 1008 |

- **All 49 valid fault trials were detected by both paths:** worst case
  1.31s for the agent and 1.98s for the alert.
- **Control trials (no fault, 10 × 3s windows): 0 false positives** on
  either path.
- **1 trial is suspect and excluded.** In one applier-stall trial, a host
  stall made the agent report `unreachable` instead of `lag_high`. The bench
  flags reason mismatches like this rather than counting them.
- \*applier-stall is measured from when replication lag crossed the 1s
  threshold, which was 0.76–0.96s after the applier was blocked.
- With n=10, p95 is the worst of 10. Latencies are measured from before the
  injecting `docker`/SQL call, so they include the CLI's own time.
- **Host conditions:** a Docker Desktop VM shared with three kind clusters
  (CPU pressure ~57–68%, swap full). Results on a quiet host should be
  tighter; see the M1 findings in the design doc.
- At the earlier 500ms scrape/eval setting, hang alerts reached 2.4s
  ([validation run](results/detect-20261118-185638-scrape500ms-validation.json)),
  which is why both now run at 250ms.
- The report's `commit` field reads `4fb3ef0-dirty`: it ran on the M1
  working tree before M1 was committed.

## Verified in M0

- Fresh bootstrap in ~25s; all nodes hold only the primary's GTIDs (no errant transactions).
- With both replicas disconnected, a commit on the primary blocks (semi-sync stays ON,
  `no_tx=0`). This is the fence that stops an old primary from accepting writes.
- A restarted primary returns with `super_read_only=ON` and rejects writes (ERROR 1290).
- A 100ms `tc netem` delay raises peer RTT from 0.17ms to 101ms.
