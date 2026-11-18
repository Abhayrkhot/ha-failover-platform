# ha-failover-platform

Failover coordinator for a 3-node MySQL cluster, written in Go: Redis-majority
health state and leases, layered fencing against split brain, Prometheus
metrics, chaos testing, and timed backup/restore drills.

Status: **M0** — the lab cluster. See [docs/design.md](docs/design.md) for the
architecture, fencing model, and milestone plan.

## Quickstart

Requires Docker (Desktop on macOS is fine; images are multi-arch) and `make`.

```bash
make bootstrap     # start everything, wire mysql-1 -> mysql-2/3 semi-sync replication
make status        # role, read-only flags, semi-sync clients, replication threads, GTIDs
make netem-check   # confirm tc netem works in the Docker VM (needed for fault injection)
make clean         # stop and delete all data
```

`make down` keeps the data, but every node comes back **read-only** on
restart, including the old primary. That is intentional fencing. Until the
coordinator exists (M2), use `make clean bootstrap` for a fresh writable cluster.

## What's running

| Service | Host port | Notes |
|---|---|---|
| mysql-1 / 2 / 3 | 127.0.0.1:33061 / 33062 / 33063 | MySQL 8.4.13, GTID, semi-sync AFTER_SYNC, never falls back to async |
| redis-1 / 2 / 3 | 127.0.0.1:63791 / 63792 / 63793 | No persistence; health and leases are ephemeral |
| prometheus | 127.0.0.1:19090 | 1s scrape and eval interval |

Users (created on each node with `sql_log_bin=0`, passwords in
[deploy/dev.env](deploy/dev.env), lab use only): `repl` (replication), `hafo`
(agent/coordinator), `app` (load generator).

## Layout

```
compose.yaml                   nodes, network, volumes
deploy/mysql/hafo.cnf          replication, durability, semi-sync settings
deploy/mysql/initdb/           first-boot users (not binlogged)
deploy/prometheus/             scrape config
deploy/nettools/               alpine + tc/iptables, run in a node's netns
scripts/bootstrap.sh           one-time replication setup; refuses non-fresh nodes
scripts/status.sh              cluster view
scripts/netem-check.sh         netem smoke test
docs/design.md                 architecture, fencing, budgets, claims -> experiments
```

## Verified in M0

- Fresh bootstrap in ~25s; all nodes hold only the primary's GTIDs (no errant transactions).
- With both replicas disconnected, a commit on the primary blocks (semi-sync stays ON,
  `no_tx=0`). This is the fence that stops an old primary from accepting writes.
- A restarted primary returns with `super_read_only=ON` and rejects writes (ERROR 1290).
- `tc netem` delay of 100ms raises peer RTT from 0.17ms to 101ms.
- The whole stack uses about 1.1 GB of RAM.
