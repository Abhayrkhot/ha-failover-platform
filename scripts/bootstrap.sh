#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# One-time setup of a fresh cluster: mysql-1 primary, mysql-2/3 semi-sync
# replicas. Refuses to touch a cluster that already has data or replication.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

PRIMARY=mysql-1
REPLICAS=(mysql-2 mysql-3)

log "checking that every node is fresh"
uuids=()
for n in "${NODES[@]}"; do
  sql "$n" "SELECT 1" >/dev/null || die "$n is not reachable; run 'make up' first"
  configured=$(sql "$n" "SELECT COUNT(*) FROM performance_schema.replication_connection_configuration")
  [[ "$configured" == "0" ]] || die "$n already has replication configured; 'make clean' for a fresh cluster"
  gtids=$(sql "$n" "SELECT @@GLOBAL.gtid_executed")
  [[ -z "$gtids" ]] || die "$n has local transactions ($gtids) that would be errant GTIDs on its peers"
  uuids+=("$(sql "$n" "SELECT @@server_uuid")")
done
[[ -z "$(printf '%s\n' "${uuids[@]}" | sort | uniq -d)" ]] || die "nodes share a server_uuid (cloned datadir?)"

# Persisted, so any node that restarts comes back read-only and stays that way
# until something with authority (bootstrap now, the coordinator later) opens it.
log "persisting super_read_only=ON on all nodes"
for n in "${NODES[@]}"; do
  sql "$n" "SET PERSIST super_read_only = ON"
done

# Source side first: replicas register as semi-sync when their IO thread
# connects. Nothing can commit yet because the primary is still read-only.
log "enabling semi-sync source on $PRIMARY"
sql "$PRIMARY" "SET GLOBAL rpl_semi_sync_source_enabled = ON"

for n in "${REPLICAS[@]}"; do
  log "starting replication $PRIMARY -> $n"
  sql "$n" "CHANGE REPLICATION SOURCE TO
              SOURCE_HOST = '$PRIMARY', SOURCE_PORT = 3306,
              SOURCE_USER = 'repl', SOURCE_PASSWORD = '$REPL_PASSWORD',
              SOURCE_AUTO_POSITION = 1, GET_SOURCE_PUBLIC_KEY = 1,
              SOURCE_CONNECT_RETRY = 1, SOURCE_RETRY_COUNT = 86400;
            START REPLICA;"
done

for n in "${REPLICAS[@]}"; do
  wait_for "$n" 20 "SELECT SERVICE_STATE = 'ON' FROM performance_schema.replication_connection_status" \
    || die "$n IO thread did not connect: $(sql "$n" "SELECT LAST_ERROR_MESSAGE FROM performance_schema.replication_connection_status")"
done

want=${#REPLICAS[@]}
wait_for "$PRIMARY" 10 "SELECT VARIABLE_VALUE = $want FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_source_clients'" \
  || die "expected $want semi-sync clients on $PRIMARY"
[[ "$(sql "$PRIMARY" "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_source_status'")" == "ON" ]] \
  || die "semi-sync is not active on $PRIMARY"

# Runtime only (not persisted): a restart of mysql-1 closes it again.
log "opening $PRIMARY for writes"
sql "$PRIMARY" "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF"

# Created through the primary so the schemas replicate. Each commit here
# returns only after a replica acknowledged it (semi-sync).
log "creating schemas through $PRIMARY"
sql "$PRIMARY" "CREATE DATABASE IF NOT EXISTS ha_meta;
                CREATE TABLE IF NOT EXISTS ha_meta.heartbeat (
                  id        TINYINT UNSIGNED PRIMARY KEY,
                  server_id INT UNSIGNED NOT NULL,
                  ts        DATETIME(6)  NOT NULL
                );
                REPLACE INTO ha_meta.heartbeat VALUES (1, @@server_id, NOW(6));
                -- One row per epoch, written by whoever opened that primary:
                -- the epoch history, replicated with the data.
                CREATE TABLE IF NOT EXISTS ha_meta.cluster (
                  epoch        BIGINT UNSIGNED PRIMARY KEY,
                  primary_node VARCHAR(64)  NOT NULL,
                  promoted_by  VARCHAR(128) NOT NULL,
                  promoted_at  DATETIME(6)  NOT NULL
                );
                INSERT INTO ha_meta.cluster VALUES (1, '$PRIMARY', 'bootstrap', NOW(6));
                CREATE DATABASE IF NOT EXISTS app;
                -- The failover bench's write load; served_by records which
                -- server executed each insert.
                CREATE TABLE IF NOT EXISTS app.writes (
                  id         BIGINT UNSIGNED PRIMARY KEY,
                  written_at DATETIME(6) NOT NULL,
                  served_by  VARCHAR(64) NOT NULL
                );"

gtid=$(sql "$PRIMARY" "SELECT @@GLOBAL.gtid_executed")
for n in "${REPLICAS[@]}"; do
  [[ "$(sql "$n" "SELECT WAIT_FOR_EXECUTED_GTID_SET('$gtid', 10)")" == "0" ]] \
    || die "$n did not apply $gtid within 10s"
done

# Last: publishing the topology is what lets the agents take the primary
# lease, the coordinators start reconciling, and the router accept writes.
topology=$(printf '{"epoch":1,"primary":"%s","state":"ok","claim":"bootstrap","by":"bootstrap","updated_at":"%s"}' \
  "$PRIMARY" "$(date -u +%Y-%m-%dT%H:%M:%SZ)")
acks=0
for i in 1 2 3; do
  [[ "$(docker exec "hafo-redis-$i" redis-cli SET hafo:topology "$topology")" == "OK" ]] && acks=$((acks + 1))
done
((acks >= 2)) || die "topology reached only $acks of 3 Redis instances"
log "published topology epoch 1 (primary $PRIMARY) to $acks/3 Redis instances"

log "bootstrap complete"
"$(dirname "$0")/status.sh"
