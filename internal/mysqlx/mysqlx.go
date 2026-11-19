// Package mysqlx holds the MySQL access shared by the agent and the
// coordinator: connection setup and the health probe.
package mysqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
)

// The driver logs connection errors on its own ("packets.go: unexpected
// EOF") in addition to returning them; during failovers that floods every
// component's output with duplicates of errors the callers already handle.
func init() { _ = mysql.SetLogger(log.New(io.Discard, "", 0)) }

// Open returns a pool to addr. dial bounds connection setup; queries are
// bounded by their contexts. Parameters are interpolated client-side, so a
// statement with arguments costs one round trip, not a prepare plus an execute.
func Open(addr, user, password string, dial time.Duration, maxConns int) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd = user, password
	cfg.Net, cfg.Addr = "tcp", addr
	cfg.Timeout = dial
	cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 5*time.Second
	cfg.InterpolateParams = true
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

// statusQuery reads everything the verdict needs in one round trip. Every
// subquery is wrapped so a node with no replication configured still returns
// one row.
//
// It must stay cheap under contention, because it runs 4 times a second from
// every agent and from the leader coordinator. In particular it does not read
// performance_schema.global_status: that aggregates every connection's
// counters under one global mutex, and in M2 concurrent probes convoyed on it
// until abandoned (timed-out) probes filled max_connections. Connected
// replicas are counted from binlog dump threads instead; every replica in this
// cluster is semi-sync, so that is also the count of replicas able to ack.
// MAX_EXECUTION_TIME bounds a probe the client has already given up on.
const statusQuery = `SELECT /*+ MAX_EXECUTION_TIME(1000) */
  @@server_id, @@server_uuid, @@read_only, @@super_read_only,
  REPLACE(@@gtid_executed, '\n', ''),
  @@rpl_semi_sync_source_enabled,
  ` + ConnectedReplicasExpr + `,
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
const lagQuery = `SELECT /*+ MAX_EXECUTION_TIME(1000) */ TIMESTAMPDIFF(MICROSECOND, ts, NOW(6)) FROM ha_meta.heartbeat WHERE id = 1`

// ConnectedReplicasExpr counts replicas streaming from this server.
const ConnectedReplicasExpr = `(SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_COMMAND LIKE 'Binlog Dump%')`

// Probe reads one observation of the server behind db.
func Probe(ctx context.Context, db *sql.DB) (health.Observation, error) {
	var o health.Observation
	err := db.QueryRowContext(ctx, statusQuery).Scan(
		&o.ServerID, &o.ServerUUID, &o.ReadOnly, &o.SuperReadOnly, &o.GTIDExecuted,
		&o.SemiSyncSourceEnabled, &o.ConnectedReplicas, &o.BlockedWrites,
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
	switch {
	case errors.Is(err, sql.ErrNoRows), MissingSchema(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	s := float64(us) / 1e6
	return &s, nil
}

// Fence makes the server read-only and drops its client connections.
//
// The order is what makes it safe:
//
//  1. offline_mode ON disconnects every session without CONNECTION_ADMIN
//     (all clients) and refuses new ones, so from here on no client can
//     start a commit. KILL-style disconnects shut the socket at once, so a
//     killed session whose commit completes later can never receive its OK.
//  2. Kill every thread parked waiting for a semi-sync ack (any user).
//  3. Turn the semi-sync source off. A commit parked on an ack that will
//     never come holds up read-only, and KILL does not interrupt that wait;
//     disabling semi-sync releases them all at once. Their sessions are gone
//     (1, 2), so nobody is told they succeeded.
//  4. super_read_only ON, capped at a 1s lock wait.
//  5. offline_mode OFF: a read-only node may serve reads again.
//
// Without step 1, a client connecting between steps 3 and 4 would commit
// with semi-sync off, i.e. be acknowledged without any replica holding the
// write, and a failover could then lose it. If step 4 fails, offline_mode
// stays on: the node is still writable, so clients stay out.
//
// It returns how many parked threads it killed.
func Fence(ctx context.Context, db *sql.DB, clientUser string) (int, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION lock_wait_timeout = 1"); err != nil {
		return 0, err
	}
	if _, err := conn.ExecContext(ctx, "SET GLOBAL offline_mode = ON"); err != nil {
		return 0, fmt.Errorf("offline_mode: %w", err)
	}
	var ids []int64
	rows, err := conn.QueryContext(ctx, `SELECT ID FROM performance_schema.processlist
		WHERE ID <> CONNECTION_ID() AND (USER = ? OR STATE LIKE '%semi-sync%')`, clientUser)
	if err == nil {
		for rows.Next() {
			var id int64
			if rows.Scan(&id) == nil {
				ids = append(ids, id)
			}
		}
		rows.Close()
	}
	for _, id := range ids {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("KILL CONNECTION %d", id))
	}
	// Best effort: a node without the plugin has nothing parked.
	_, _ = conn.ExecContext(ctx, "SET GLOBAL rpl_semi_sync_source_enabled = OFF")
	if _, err := conn.ExecContext(ctx, "SET GLOBAL super_read_only = ON"); err != nil {
		return len(ids), fmt.Errorf("super_read_only: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SET GLOBAL offline_mode = OFF"); err != nil {
		return len(ids), fmt.Errorf("leave offline_mode: %w", err)
	}
	return len(ids), nil
}

// MySQL error numbers used across the project.
const (
	ErrNoSuchTable = 1146
	ErrNoSuchDB    = 1049
	ErrReadOnly    = 1290 // --super-read-only / --read-only
)

// Number returns the MySQL error number in err, or 0.
func Number(err error) uint16 {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

// MissingSchema reports an unknown database or table.
func MissingSchema(err error) bool {
	n := Number(err)
	return n == ErrNoSuchTable || n == ErrNoSuchDB
}

// ErrorKind labels a failure for error-rate metrics: connect, timeout or query.
func ErrorKind(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case Number(err) != 0:
		return "query"
	default:
		return "connect"
	}
}
