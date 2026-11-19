#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# Hot physical backup with MySQL Clone.
#
# CLONE LOCAL DATA DIRECTORY copies a consistent snapshot of a running
# server's InnoDB data (rows, the data dictionary, users, and the GTID set)
# into a directory. It runs on a healthy replica, so the primary's I/O is
# left alone, and writes into the backups volume, which every MySQL node
# mounts at /backups and which outlives their data volumes. Each backup is
# /backups/<UTC timestamp>/ plus a <timestamp>.json describing it. Binary
# logs are not part of a backup; the archiver (cmd/hafo-archiver) keeps
# those, and a restore replays them on top (hafoctl drill).
#
# Usage: scripts/backup.sh [mysql-N]    (default: a healthy replica)
#        KEEP=3 keeps the newest three backups.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

src=${1:-$(healthy_replica_from_redis)}
[[ " ${NODES[*]} " == *" $src "* ]] || die "no healthy replica to back up from (pass mysql-N to choose one)"
keep=${KEEP:-3}
ts=$(date -u +%Y%m%dT%H%M%SZ)

# mysqld (uid 999) writes the copy; the volume starts out owned by root.
docker exec -u 0 "hafo-$src" sh -c 'mkdir -p /backups && chown mysql:mysql /backups'

log "backing up $src to /backups/$ts"
gtid_before=$(sql "$src" "SELECT REPLACE(@@GLOBAL.gtid_executed, '\n', '')")
start=$(now_ms)
sql "$src" "CLONE LOCAL DATA DIRECTORY = '/backups/$ts'"
took_ms=$(( $(now_ms) - start ))
gtid_after=$(sql "$src" "SELECT REPLACE(@@GLOBAL.gtid_executed, '\n', '')")
bytes=$(docker exec "hafo-$src" du -sb "/backups/$ts" | cut -f1)

# The snapshot's exact GTID set is inside the copy; it lies between these two.
docker exec -i -u 0 "hafo-$src" sh -c "cat > /backups/$ts.json" <<JSON
{
  "backup": "$ts",
  "source": "$src",
  "method": "CLONE LOCAL DATA DIRECTORY",
  "bytes": $bytes,
  "took_ms": $took_ms,
  "gtid_executed_before": "$gtid_before",
  "gtid_executed_after": "$gtid_after"
}
JSON

old=$(docker exec "hafo-$src" sh -c "ls -1 /backups | grep -E '^[0-9]{8}T[0-9]{6}Z$' | sort -r | tail -n +$((keep + 1))")
for b in $old; do
  docker exec -u 0 "hafo-$src" rm -rf "/backups/$b" "/backups/$b.json"
  log "pruned backup $b"
done
log "backup $ts: $((bytes / 1024 / 1024)) MiB from $src in ${took_ms}ms"
echo "$ts"
