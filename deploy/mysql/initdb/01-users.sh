#!/bin/bash
# Run by the mysql image entrypoint on first boot (empty datadir) only, after
# it has set the root password and while its init server listens on the socket.
#
# Executable on purpose: on Docker Desktop, bind-mounted files pass `test -x`
# for root even at mode 644, so the entrypoint tries to execute rather than
# source them. Being a real executable works on Docker Desktop and Linux alike.
#
# Runs on every node with sql_log_bin=0: each node gets identical local users
# and no GTIDs. If these statements were binlogged, each replica would carry
# transactions its source never had (errant GTIDs), which breaks promotion.
# Schemas are created later through the primary so they replicate normally.
set -euo pipefail

MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot --protocol=socket <<EOSQL
SET SESSION sql_log_bin = 0;

CREATE USER 'repl'@'%' IDENTIFIED BY '${REPL_PASSWORD}';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';

-- Agent / coordinator: probe health, flip read_only, repoint replication
-- (RESET REPLICA needs RELOAD), kill client connections on a fenced node,
-- write the heartbeat and epoch rows.
CREATE USER 'hafo'@'%' IDENTIFIED BY '${HAFO_PASSWORD}';
GRANT REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, SYSTEM_VARIABLES_ADMIN,
      CONNECTION_ADMIN, PROCESS, RELOAD ON *.* TO 'hafo'@'%';
GRANT SELECT ON performance_schema.* TO 'hafo'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON ha_meta.* TO 'hafo'@'%';

CREATE USER 'app'@'%' IDENTIFIED BY '${APP_PASSWORD}';
GRANT SELECT, INSERT, UPDATE, DELETE ON app.* TO 'app'@'%';
EOSQL
