#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# Rebuild a node from an empty datadir. Use it on a node the coordinator left
# fenced because it holds transactions the primary lacks (errant GTIDs).
#
# A fresh MySQL boots writable with no GTIDs. The coordinator fences it as a
# stray within a tick, then rejoins it as a replica of the current primary,
# which replays the cluster's whole history from its binlog (every node logs
# what it applies: log_replica_updates). M4 replaces this with Clone.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

node=${1:-}
[[ " ${NODES[*]} " == *" $node "* ]] || die "usage: $0 mysql-N"

cd "$ROOT"
log "rebuilding $node from an empty datadir"
docker compose rm -sf "$node" >/dev/null 2>&1
docker volume rm "hafo_${node}-data" >/dev/null
docker compose up -d --wait "$node" 2>&1 | grep -v -E 'Running|Waiting|Healthy' || true

log "waiting for the coordinator to rejoin $node as a replica"
wait_for "$node" 120 "SELECT COUNT(*) = 1 FROM performance_schema.replication_connection_status
                      WHERE CHANNEL_NAME = '' AND SERVICE_STATE = 'ON'" \
  || die "$node was not rejoined within 120s"
log "$node is replicating from $(sql "$node" "SELECT HOST FROM performance_schema.replication_connection_configuration")"
