package agent

import (
	"context"
	"database/sql"
	"errors"
	"net"

	"github.com/go-sql-driver/mysql"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
)

// statusQuery reads everything the verdict needs in one round trip. Every
// subquery is wrapped so a node with no replication configured still returns
// one row.
const statusQuery = `SELECT
  @@server_id, @@server_uuid, @@read_only, @@super_read_only,
  REPLACE(@@gtid_executed, '\n', ''),
  @@rpl_semi_sync_source_enabled,
  IFNULL((SELECT VARIABLE_VALUE = 'ON' FROM performance_schema.global_status
          WHERE VARIABLE_NAME = 'Rpl_semi_sync_source_status'), 0),
  IFNULL((SELECT VARIABLE_VALUE FROM performance_schema.global_status
          WHERE VARIABLE_NAME = 'Rpl_semi_sync_source_clients'), 0),
  (SELECT COUNT(*) FROM performance_schema.processlist
    WHERE USER = SUBSTRING_INDEX(CURRENT_USER(), '@', 1) AND STATE LIKE '%semi-sync%'),
  IFNULL((SELECT HOST FROM performance_schema.replication_connection_configuration
          WHERE CHANNEL_NAME = ''), ''),
  IFNULL((SELECT SERVICE_STATE FROM performance_schema.replication_connection_status
          WHERE CHANNEL_NAME = ''), ''),
  IFNULL((SELECT LAST_ERROR_MESSAGE FROM performance_schema.replication_connection_status
          WHERE CHANNEL_NAME = ''), ''),
  IFNULL((SELECT SERVICE_STATE FROM performance_schema.replication_applier_status
          WHERE CHANNEL_NAME = ''), ''),
  IFNULL((SELECT LAST_ERROR_MESSAGE FROM performance_schema.replication_applier_status_by_worker
          WHERE CHANNEL_NAME = '' AND LAST_ERROR_NUMBER <> 0 LIMIT 1), '')`

// lagQuery is the age of the newest heartbeat applied on this node. The
// primary writes NOW(6) there every heartbeat interval; all nodes share the
// Docker VM clock, so no skew correction is needed in the lab.
const lagQuery = `SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6)) FROM ha_meta.heartbeat WHERE id = 1`

const heartbeatStmt = `INSERT INTO ha_meta.heartbeat (id, server_id, ts)
  VALUES (1, @@server_id, NOW(6)) AS new
  ON DUPLICATE KEY UPDATE server_id = new.server_id, ts = new.ts`

const (
	errNoSuchTable = 1146
	errNoSuchDB    = 1049
)

func probe(ctx context.Context, db *sql.DB) (health.Observation, error) {
	var o health.Observation
	err := db.QueryRowContext(ctx, statusQuery).Scan(
		&o.ServerID, &o.ServerUUID, &o.ReadOnly, &o.SuperReadOnly, &o.GTIDExecuted,
		&o.SemiSyncSourceEnabled, &o.SemiSyncSourceActive, &o.SemiSyncClients, &o.BlockedWrites,
		&o.SourceHost, &o.IOState, &o.IOError, &o.SQLState, &o.SQLError,
	)
	if err != nil {
		return health.Observation{ProbeError: err.Error()}, err
	}
	if o.LagSeconds, err = lag(ctx, db); err != nil {
		return health.Observation{ProbeError: err.Error()}, err
	}
	o.Reachable = true
	return o, nil
}

// lag returns nil when the heartbeat table does not exist yet (before
// bootstrap) rather than failing the whole probe.
func lag(ctx context.Context, db *sql.DB) (*float64, error) {
	var us int64
	err := db.QueryRowContext(ctx, lagQuery).Scan(&us)
	var me *mysql.MySQLError
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case errors.As(err, &me) && (me.Number == errNoSuchTable || me.Number == errNoSuchDB):
		return nil, nil
	case err != nil:
		return nil, err
	}
	s := float64(us) / 1e6
	return &s, nil
}

// errorKind labels a probe failure for the error-rate metric.
func errorKind(err error) string {
	var me *mysql.MySQLError
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case errors.As(err, &me):
		return "query"
	default:
		return "connect"
	}
}
