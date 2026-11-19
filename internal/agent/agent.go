// Package agent runs next to one MySQL server: it probes the server, writes
// the replication heartbeat when the server is the primary, publishes a health
// record to a Redis majority, and exports Prometheus metrics.
package agent

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// Config is the agent's tuning. Defaults live in cmd/hafo-agent.
type Config struct {
	Node          string
	MySQLAddr     string
	MySQLUser     string
	MySQLPassword string
	RedisAddrs    []string

	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	PublishTimeout    time.Duration
	HealthTTL         time.Duration
	Thresholds        health.Thresholds
}

// public is the config as reported on /status (no password, readable durations).
func (c Config) public() map[string]any {
	return map[string]any{
		"node": c.Node, "mysql_addr": c.MySQLAddr, "redis_addrs": c.RedisAddrs,
		"probe_interval": c.ProbeInterval.String(), "probe_timeout": c.ProbeTimeout.String(),
		"heartbeat_interval": c.HeartbeatInterval.String(), "heartbeat_timeout": c.HeartbeatTimeout.String(),
		"publish_timeout": c.PublishTimeout.String(), "health_ttl": c.HealthTTL.String(),
		"fail_after": c.Thresholds.FailAfter, "recover_after": c.Thresholds.RecoverAfter,
		"max_lag": c.Thresholds.MaxLag.String(),
	}
}

// Agent is safe for concurrent use by its own loops and HTTP handlers.
type Agent struct {
	cfg     Config
	log     *slog.Logger
	probeDB *sql.DB
	writeDB *sql.DB // one connection: at most one heartbeat in flight
	store   *store.Majority
	tracker *health.Tracker
	m       *metrics
	boot    string
	seq     uint64
	role    health.Role // from the last successful probe

	mu    sync.Mutex
	state snapshot
}

type snapshot struct {
	rec    health.Record
	acks   int
	quorum int
}

// New connects lazily: nothing here fails because MySQL or Redis is down.
func New(cfg Config, log *slog.Logger, reg prometheus.Registerer) (*Agent, error) {
	dsn := mysql.NewConfig()
	dsn.User, dsn.Passwd = cfg.MySQLUser, cfg.MySQLPassword
	dsn.Net, dsn.Addr = "tcp", cfg.MySQLAddr
	dsn.Timeout = cfg.ProbeTimeout // dial
	dsn.ReadTimeout, dsn.WriteTimeout = 5*time.Second, 5*time.Second
	connector, err := mysql.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	probeDB := sql.OpenDB(connector)
	probeDB.SetMaxOpenConns(2)
	probeDB.SetConnMaxLifetime(5 * time.Minute)
	writeDB := sql.OpenDB(connector)
	writeDB.SetMaxOpenConns(1)

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	a := &Agent{
		cfg: cfg, log: log, probeDB: probeDB, writeDB: writeDB,
		store:   store.NewRedis(cfg.RedisAddrs, cfg.PublishTimeout),
		tracker: health.NewTracker(cfg.Thresholds, time.Now()),
		boot:    hex.EncodeToString(b),
		role:    health.RoleUnknown,
	}
	a.m = newMetrics(reg, a)
	return a, nil
}

// Run probes until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) {
	go a.heartbeatLoop(ctx)
	t := time.NewTicker(a.cfg.ProbeInterval)
	defer t.Stop()
	for {
		a.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Close releases connections.
func (a *Agent) Close() {
	_ = a.probeDB.Close()
	_ = a.writeDB.Close()
	_ = a.store.Close()
}

func (a *Agent) tick(ctx context.Context) {
	start := time.Now()
	pctx, cancel := context.WithTimeout(ctx, a.cfg.ProbeTimeout)
	obs, err := probe(pctx, a.probeDB)
	cancel()
	a.m.probes.Inc()
	a.m.probeDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		a.m.probeErrors.WithLabelValues(errorKind(err)).Inc()
	} else {
		a.role = obs.Role()
	}

	now := time.Now()
	a.seq++
	rec := health.Record{
		Node: a.cfg.Node, Boot: a.boot, Seq: a.seq, ObservedAt: now,
		Role: a.role, Verdict: a.tracker.Observe(obs, now), Observation: obs,
	}
	acks := a.publish(ctx, rec)

	a.mu.Lock()
	prev := a.state
	a.state = snapshot{rec: rec, acks: acks, quorum: a.store.Quorum()}
	a.mu.Unlock()

	if prev.rec.Seq == 0 || prev.rec.Healthy != rec.Healthy || prev.rec.Role != rec.Role {
		a.m.healthChanges.WithLabelValues(map[bool]string{true: "healthy", false: "unhealthy"}[rec.Healthy]).Inc()
		a.log.Info("health", "healthy", rec.Healthy, "role", rec.Role, "reasons", fmt.Sprint(rec.Reasons))
	}
	if q := a.store.Quorum(); (prev.rec.Seq == 0 || prev.acks >= q) && acks < q {
		a.log.Warn("health record did not reach a Redis majority", "acks", acks, "quorum", q)
	} else if prev.rec.Seq != 0 && prev.acks < q && acks >= q {
		a.log.Info("Redis majority restored", "acks", acks)
	}
}

func (a *Agent) publish(ctx context.Context, rec health.Record) int {
	b, err := json.Marshal(rec)
	if err != nil {
		a.log.Error("marshal health record", "err", err)
		return 0
	}
	rs := a.store.Put(ctx, health.Key(a.cfg.Node), b, a.cfg.HealthTTL)
	for _, r := range rs {
		result := "ok"
		if r.Err != nil {
			result = "error"
		}
		a.m.publishes.WithLabelValues(r.Addr, result).Inc()
	}
	return store.Succeeded(rs)
}

// heartbeatLoop writes the heartbeat row while the local server is the
// designated primary: writable *and* semi-sync source enabled. Writable alone
// is not enough. A freshly initialised MySQL boots writable, and heartbeats
// written there would be errant GTIDs that block bootstrap and promotion. The
// semi-sync source flag is runtime-only and set solely by whoever opens the
// primary (bootstrap now, the coordinator in M2).
//
// With semi-sync and no async fallback, a commit on an isolated primary waits
// for an ack indefinitely, and the server keeps waiting even after the client
// gives up. Re-issuing the write every interval would park a new server thread
// each time, so after a timeout the loop waits for a probe taken after the
// timeout and then skips while the probe still sees a parked heartbeat.
func (a *Agent) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.HeartbeatInterval)
	defer t.Stop()
	var timedOutAt time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s := a.snapshot()
		o := s.rec.Observation
		if !o.Reachable || o.ReadOnly || !o.SemiSyncSourceEnabled {
			continue
		}
		if !s.rec.ObservedAt.After(timedOutAt) || o.BlockedWrites > 0 {
			a.m.heartbeats.WithLabelValues("skipped_blocked").Inc()
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, a.cfg.HeartbeatTimeout)
		_, err := a.writeDB.ExecContext(wctx, heartbeatStmt)
		cancel()
		var me *mysql.MySQLError
		switch {
		case err == nil:
			a.m.heartbeats.WithLabelValues("ok").Inc()
		case errors.As(err, &me) && (me.Number == errNoSuchDB || me.Number == errNoSuchTable):
			// Bootstrap opens the primary just before it creates ha_meta.
			a.m.heartbeats.WithLabelValues("no_table").Inc()
		case errors.Is(err, context.DeadlineExceeded):
			timedOutAt = time.Now()
			a.m.heartbeats.WithLabelValues("timeout").Inc()
			a.log.Warn("heartbeat commit timed out; likely waiting for a semi-sync ack")
		default:
			a.m.heartbeats.WithLabelValues("error").Inc()
			a.log.Warn("heartbeat write failed", "err", err)
		}
	}
}

func (a *Agent) snapshot() snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

// StatusHandler serves the agent's config and latest record as JSON.
func (a *Agent) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s := a.snapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"config": a.cfg.public(), "record": s.rec, "redis_acks": s.acks, "redis_quorum": s.quorum,
		})
	})
}
