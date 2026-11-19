#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# Rebuild a node from a physical copy of the current primary (MySQL Clone).
#
# For a node the coordinator left fenced because it holds transactions the
# primary lacks (errant GTIDs). Clone copies the donor's InnoDB data, so a
# rebuild takes seconds whatever the cluster's history. The M2 version
# replayed the whole binlog into an empty datadir, which grew with every
# write and stopped finishing within minutes during long runs. The node keeps
# its own server_uuid and server_id.
#
# MySQL must restart after the copy, and nothing supervises mysqld inside the
# container, so CLONE ends with ERROR 3707 ("not managed by supervisor"); the
# script restarts the container. The node boots read-only (persisted), and the
# coordinator rejoins it as a replica of the primary: by construction it holds
# nothing the primary lacks.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

node=${1:-}
[[ " ${NODES[*]} " == *" $node "* ]] || die "usage: $0 mysql-N [donor]"
donor=${2:-$(primary_from_redis)}
[[ -n "$donor" && "$donor" != "$node" ]] || die "no usable donor (topology primary: '${donor}')"

cd "$ROOT"
start=$SECONDS
docker start "hafo-$node" >/dev/null 2>&1 || true
wait_for "$node" 60 "SELECT 1" || die "$node is not reachable"

log "cloning $donor into $node"
# CLONE is refused under super_read_only. Drop only that: plain read_only
# stays on, which still blocks every client without SUPER, and read_only is
# what the agents, coordinator and router read, so nobody treats the node as
# a writable stray during the copy. The restart brings back the persisted
# super_read_only.
out=$(sql "$node" "STOP REPLICA;
                   SET GLOBAL super_read_only = OFF;
                   SET GLOBAL clone_valid_donor_list = '$donor:3306';
                   CLONE INSTANCE FROM 'hafo'@'$donor':3306 IDENTIFIED BY '$HAFO_PASSWORD'" 2>&1) || true
case "$out" in
  "" | *3707*) ;; # 3707: cloned; restart needed (no supervisor in the container)
  *) die "clone failed: $out" ;;
esac

log "restarting $node on the cloned data"
docker restart "hafo-$node" >/dev/null
for _ in $(seq 120); do
  [[ "$(docker inspect -f '{{.State.Health.Status}}' "hafo-$node")" == healthy ]] && break
  sleep 1
done
persisted_ro=$(sql "$node" "SELECT @@super_read_only")
log "$node is up (super_read_only=$persisted_ro); waiting for the coordinator to rejoin it"
wait_for "$node" 120 "SELECT COUNT(*) = 1 FROM performance_schema.replication_connection_status
                      WHERE CHANNEL_NAME = '' AND SERVICE_STATE = 'ON'" \
  || die "$node was not rejoined within 120s"
log "$node rebuilt from $donor in $((SECONDS - start))s; replicating from $(sql "$node" "SELECT HOST FROM performance_schema.replication_connection_configuration")"
