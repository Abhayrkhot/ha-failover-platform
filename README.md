# ha-failover-platform

Failover coordinator for a 3-node MySQL cluster, written in Go: Redis-majority
health state and leases, layered fencing against split brain, Prometheus
metrics, chaos testing, and timed backup/restore drills.

Status: **M1**: lab cluster plus per-node health agents. See
[docs/design.md](docs/design.md) for the architecture, fencing model, detection
budget, and milestone plan.

## Quickstart

Requires Docker (Desktop on macOS is fine; images are multi-arch), Go 1.27+, and `make`.

```bash
make bootstrap     # build agents, start everything, wire mysql-1 -> mysql-2/3 semi-sync replication
make status        # what MySQL itself says: role, read-only flags, replication threads, GTIDs
make health        # what the agents say: verdict, reasons and lag, as a Redis majority sees it
make test          # unit tests (race detector on)
make bench-detect  # break a replica repeatedly, time detection; ARGS=-trials=3 for a quick run
make clean         # stop and delete all data
```

`make down` keeps the data, but every node comes back **read-only** on
restart, including the old primary. That is intentional fencing. Until the
coordinator exists (M2), use `make clean bootstrap` for a fresh writable cluster.

After editing anything under `deploy/prometheus/`, run `make prom-reload`. It
reloads and then checks that Prometheus loaded every rule; see the M1 findings
in the design doc for why that check exists.

## What's running

| Service | Host port | Notes |
|---|---|---|
| mysql-1 / 2 / 3 | 127.0.0.1:33061 / 33062 / 33063 | MySQL 8.4.13, GTID, semi-sync AFTER_SYNC, never falls back to async |
| agent-1 / 2 / 3 | 127.0.0.1:19101 / 19102 / 19103 | `/metrics`, `/status` (config + latest record), `/healthz` |
| redis-1 / 2 / 3 | 127.0.0.1:63791 / 63792 / 63793 | No persistence; health records carry a 1.5s TTL |
| prometheus | 127.0.0.1:19090 | 250ms scrape and rule evaluation; alerts in `deploy/prometheus/rules.yml` |

Users (created on each node with `sql_log_bin=0`, passwords in
[deploy/dev.env](deploy/dev.env), lab use only): `repl` (replication), `hafo`
(agent/coordinator), `app` (load generator).

## Layout

```
cmd/hafo-agent/            per-node agent: probe, verdict, heartbeat, Redis publish, metrics
cmd/hafoctl/               host-side CLI: `health`, `bench detect`
internal/health/           observation -> debounced verdict; the published record
internal/store/            parallel reads/writes across the Redis instances, quorum math
compose.yaml, Dockerfile   nodes, network, volumes; agent image
deploy/mysql/              replication/durability/semi-sync settings, first-boot users
deploy/prometheus/         scrape config (250ms), alert rules
deploy/nettools/           alpine + tc/iptables, run in a node's netns
scripts/                   bootstrap, status, prom-reload, netem check
results/                   JSON reports from benchmark runs
docs/design.md             architecture, fencing, budgets, findings, claims -> experiments
```

## Results

### Detection (M1)

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
