#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# One line per node: role, read-only flags, semi-sync, replication threads, GTIDs.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

line() { printf '%-8s %-8s %-4s %-5s %-9s %-8s %-4s %-4s %s\n' "$@"; }
line NODE ROLE SRO RO SEMI_SRC SOURCE IO SQL GTID_EXECUTED

for n in "${NODES[@]}"; do
  row=$(sql "$n" "SELECT @@super_read_only, @@read_only,
      IF(@@rpl_semi_sync_source_enabled,
         CONCAT('on/', (SELECT VARIABLE_VALUE FROM performance_schema.global_status
                        WHERE VARIABLE_NAME = 'Rpl_semi_sync_source_clients')), 'off'),
      IFNULL((SELECT HOST FROM performance_schema.replication_connection_configuration LIMIT 1), '-'),
      IFNULL((SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1), '-'),
      IFNULL((SELECT SERVICE_STATE FROM performance_schema.replication_applier_status LIMIT 1), '-'),
      IF(@@gtid_executed = '', '-', REPLACE(@@gtid_executed, CHAR(10), ''))" 2>/dev/null) || {
    line "$n" DOWN - - - - - - -
    continue
  }
  IFS=$'\t' read -r sro ro semi src io sqlt gtid <<<"$row"
  if [[ "$ro" == "0" ]]; then role=primary
  elif [[ "$src" != "-" ]]; then role=replica
  else role=fenced
  fi
  line "$n" "$role" "$sro" "$ro" "$semi" "$src" "$io" "$sqlt" "$gtid"
done
