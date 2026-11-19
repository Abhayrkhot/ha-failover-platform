// Package agent runs next to one MySQL server: it probes the server, writes
// the replication heartbeat when the server is the primary, publishes a health
// record to a Redis majority, renews the primary lease while its node is the
// serving primary, fences its own server when it must not be writable, and
// exports Prometheus metrics.
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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/mysqlx"
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
	LeaseTTL          time.Duration
	// SelfFenceAfter: a primary whose lease has not been renewed on a Redis
	// majority for this long makes itself read-only. It must be shorter than
	// LeaseTTL plus the coordinator's lease margin, so the old primary stops
	// taking writes before anyone else can be promoted. 0 disables it.
	SelfFenceAfter time.Duration
	ClientUser     string // whose connections are killed when fencing
	Thresholds     health.Thresholds
}

const heartbeatStmt = `INSERT INTO ha_meta.heartbeat (id, server_id, ts)
  VALUES (1, @@server_id, NOW(6)) AS new
  ON DUPLICATE KEY UPDATE server_id = new.server_id, ts = new.ts`

// public is the config as reported on /status (no password, readable durations).
func (c Config) public() map[string]any {
	return map[string]any{
		"node": c.Node, "mysql_addr": c.MySQLAddr, "redis_addrs": c.RedisAddrs,
		"probe_interval": c.ProbeInterval.String(), "probe_timeout": c.ProbeTimeout.String(),
		"heartbeat_interval": c.HeartbeatInterval.String(), "heartbeat_timeout": c.HeartbeatTimeout.String(),
		"publish_timeout": c.PublishTimeout.String(), "health_ttl": c.HealthTTL.String(),
		"lease_ttl": c.LeaseTTL.String(), "self_fence_after": c.SelfFenceAfter.String(),
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

	// Primary tenure, owned by the tick goroutine. primary and stray come
	// from the last committed topology this agent read; they persist while
	// the topology cannot be read, so losing the Redis majority keeps the
	// lease clock running and ends in a self-fence (no lease, no writes).
	primary bool      // the topology names this node the serving primary
	stray   bool      // the topology names a different serving primary
	epoch   uint64    // epoch of that topology
	leaseOK time.Time // last renewal accepted by a majority (or start of tenure)

	mu    sync.Mutex
	state snapshot
}

type snapshot struct {
	rec    health.Record
	acks   int
	quorum int
	// Primary lease: whether this tick tried to renew it, and how many
	// instances accepted.
	leasing   bool
	leaseAcks int
}

// New connects lazily: nothing here fails because MySQL or Redis is down.
func New(cfg Config, log *slog.Logger, reg prometheus.Registerer) (*Agent, error) {
	probeDB, err := mysqlx.Open(cfg.MySQLAddr, cfg.MySQLUser, cfg.MySQLPassword, cfg.ProbeTimeout, 2)
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	writeDB, err := mysqlx.Open(cfg.MySQLAddr, cfg.MySQLUser, cfg.MySQLPassword, cfg.ProbeTimeout, 1)
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}

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
	obs, err := mysqlx.Probe(pctx, a.probeDB)
	cancel()
	a.m.probes.Inc()
	a.m.probeDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		a.m.probeErrors.WithLabelValues(mysqlx.ErrorKind(err)).Inc()
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
	leasing, leaseAcks := a.renewLease(ctx, obs)
	a.selfFence(ctx, obs)

	a.mu.Lock()
	prev := a.state
	a.state = snapshot{rec: rec, acks: acks, quorum: a.store.Quorum(), leasing: leasing, leaseAcks: leaseAcks}
	a.mu.Unlock()

	if q := a.store.Quorum(); leasing && leaseAcks < q && (!prev.leasing || prev.leaseAcks >= q) {
		a.log.Warn("primary lease renewal did not reach a Redis majority", "acks", leaseAcks)
	} else if leasing && !prev.leasing {
		a.log.Info("holding the primary lease")
	} else if !leasing && prev.leasing {
		a.log.Info("stopped renewing the primary lease")
	}

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

// renewLease extends the primary lease while this node is the committed
// topology's serving primary and its MySQL is reachable, writable and has
// semi-sync on. A hung, crashed or fenced primary therefore stops renewing by
// itself, and the coordinator waits for the lease to lapse before promoting
// anyone else.
func (a *Agent) renewLease(ctx context.Context, o health.Observation) (bool, int) {
	if v := cluster.ReadTopology(ctx, a.store); v.OK {
		me := v.Committed.Serving() && v.Committed.Primary == a.cfg.Node
		if me && !a.primary {
			a.leaseOK = time.Now() // a new tenure starts with a full lease of grace
		}
		a.primary, a.epoch = me, v.Committed.Epoch
		a.stray = v.Committed.Serving() && !me
	}
	if !a.primary || !o.Reachable || o.ReadOnly || !o.SemiSyncSourceEnabled {
		return false, 0
	}
	start := time.Now() // the lease counts from before the request was sent
	acks := cluster.RenewPrimaryLease(ctx, a.store, cluster.Lease{Node: a.cfg.Node, Epoch: a.epoch}, a.cfg.LeaseTTL)
	if acks >= a.store.Quorum() {
		a.leaseOK = start
	}
	return true, acks
}

// selfFence makes the local server read-only when it is writable but must
// not be: either the topology names another primary (a paused old primary
// waking up after a failover), or this node is the primary but has not
// renewed its lease on a Redis majority for SelfFenceAfter (cut off from the
// coordination majority, or the majority itself is down). The second case is
// what makes the lease a fence and not just a hint: the coordinator promotes
// only after the lease is gone from a majority, and by then this server has
// already stopped taking writes. Clearing the fence is the coordinator's job.
func (a *Agent) selfFence(ctx context.Context, o health.Observation) {
	if a.cfg.SelfFenceAfter <= 0 || !o.Reachable || o.ReadOnly {
		return
	}
	var reason, why string
	switch {
	case a.stray:
		reason, why = "stray", "writable, but the topology names another primary"
	case a.primary && time.Since(a.leaseOK) > a.cfg.SelfFenceAfter:
		reason, why = "lease", fmt.Sprintf("primary lease not renewed on a Redis majority for %s", time.Since(a.leaseOK).Round(time.Millisecond))
	default:
		return
	}
	fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	killed, err := mysqlx.Fence(fctx, a.probeDB, a.cfg.ClientUser)
	if err != nil {
		a.log.Error("self-fence failed", "why", why, "err", err)
		a.m.selfFences.WithLabelValues(reason, "failed").Inc()
		return
	}
	a.log.Warn("self-fenced: made the local server read-only", "why", why, "killed_connections", killed)
	a.m.selfFences.WithLabelValues(reason, "ok").Inc()
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
		switch {
		case err == nil:
			a.m.heartbeats.WithLabelValues("ok").Inc()
		case mysqlx.MissingSchema(err):
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
			"primary_lease": map[string]any{"renewing": s.leasing, "acks": s.leaseAcks},
		})
	})
}
