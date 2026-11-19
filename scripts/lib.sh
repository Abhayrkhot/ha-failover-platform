# shellcheck shell=bash source-path=SCRIPTDIR
# Shared helpers for the lab scripts. Source this; don't execute it.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../deploy/dev.env
source "$ROOT/deploy/dev.env"

# shellcheck disable=SC2034  # used by the scripts that source this
NODES=(mysql-1 mysql-2 mysql-3)

log() { printf '\033[1m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# sql NODE STATEMENTS...  — run as root over the container's local socket,
# tab-separated output with no header. The password stays inside the container.
sql() {
  local node=$1; shift
  docker exec -i "hafo-$node" sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --skip-column-names' <<<"$*"
}

# primary_from_redis — the primary named by the topology a Redis majority holds.
primary_from_redis() {
  for i in 1 2 3; do docker exec "hafo-redis-$i" redis-cli GET hafo:topology 2>/dev/null || true; done |
    python3 -I -c '
import collections, json, sys
seen = collections.Counter()
for line in sys.stdin:
    try:
        t = json.loads(line)
    except ValueError:
        continue
    seen[(t["epoch"], t.get("state"), t.get("primary", ""))] += 1
committed = [k for k, n in seen.items() if n >= 2 and k[1] == "ok"]
print(max(committed)[2] if committed else "")'
}

# healthy_replica_from_redis — a node whose majority-held health record says
# healthy replica (first by name), or nothing.
healthy_replica_from_redis() {
  for n in "${NODES[@]}"; do
    for i in 1 2 3; do
      printf '%s\t%s\n' "$n" "$(docker exec "hafo-redis-$i" redis-cli GET "hafo:health:$n" 2>/dev/null || true)"
    done
  done | python3 -I -c '
import collections, json, sys
votes = collections.Counter()
for line in sys.stdin:
    node, _, raw = line.rstrip("\n").partition("\t")
    try:
        r = json.loads(raw)
    except ValueError:
        continue
    if r.get("healthy") and r.get("role") == "replica":
        votes[node] += 1
ok = sorted(n for n, v in votes.items() if v >= 2)
print(ok[0] if ok else "")'
}

# now_ms — wall clock in milliseconds (macOS date has no %N).
now_ms() { python3 -I -c 'import time; print(int(time.time() * 1000))'; }

# wait_for NODE SECONDS QUERY  — poll until QUERY returns "1".
wait_for() {
  local node=$1 secs=$2 query=$3 i
  for ((i = 0; i < secs * 4; i++)); do
    [[ "$(sql "$node" "$query" 2>/dev/null)" == "1" ]] && return 0
    sleep 0.25
  done
  return 1
}
