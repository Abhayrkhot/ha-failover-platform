// Package coordinator watches the cluster, and on the instance that holds the
// leader lease, keeps it correct: it fails over a dead primary, fences any
// writable node that is not the primary, and rejoins recovered nodes as
// replicas when that is safe.
package coordinator

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/mysqlx"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// Config is the coordinator's tuning. Defaults live in cmd/hafo-coordinator.
type Config struct {
	ID            string
	Nodes         []string // node names; MySQL listens on <node>:3306
	MySQLUser     string
	MySQLPassword string
	ReplUser      string
	ReplPassword  string
	ClientUser    string // whose connections are killed when a node is fenced
	RedisAddrs    []string

	Tick         time.Duration
	ProbeTimeout time.Duration
	RedisTimeout time.Duration
	FailAfter    int           // consecutive ticks both observers must agree
	LeaseMargin  time.Duration // extra wait after the primary lease is gone
	LeaderTTL    time.Duration
	LeaderRenew  time.Duration // how often the leader lease is renewed
	StepTimeout  time.Duration // bound on each promotion step
	RejoinEvery  time.Duration // minimum spacing of rejoin attempts per node
}

// Coordinator runs one tick loop and one leadership loop.
type Coordinator struct {
	cfg   Config
	log   *slog.Logger
	id    string // ID plus a per-process suffix, so a restart never reuses a lease
	store *store.Majority
	// The leader lease gets its own client with a longer deadline: a renewal
	// that misses a 100ms deadline on a loaded host is not a reason to lose
	// leadership, and lease traffic is tiny.
	leaderStore *store.Majority
	dbs         map[string]*sql.DB
	m           *metrics

	leaderMu    sync.Mutex
	leaderUntil time.Time
	leaderSince time.Time

	// Owned by the tick goroutine.
	lastProbe  time.Time
	obs        map[string]health.Observation
	det        detector
	lastRejoin map[string]time.Time
	errant     map[string]string // node -> gtid_executed holding transactions the primary lacks
	noTopoWarn bool
	lostSince  time.Time // a Redis majority answered but held no committed topology

	statusMu sync.Mutex
	status   status
}

type status struct {
	ID           string                        `json:"id"`
	Leader       bool                          `json:"leader"`
	Topology     cluster.View                  `json:"topology"`
	Observations map[string]health.Observation `json:"observations"`
	Errant       map[string]string             `json:"errant,omitempty"`
	LastFailover *cluster.Failover             `json:"last_failover,omitempty"`
}

// New prepares connections lazily; nothing fails because a node is down.
func New(cfg Config, log *slog.Logger, reg prometheus.Registerer) (*Coordinator, error) {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	c := &Coordinator{
		cfg: cfg, log: log, id: cfg.ID + "/" + hex.EncodeToString(b),
		store:       store.NewRedis(cfg.RedisAddrs, cfg.RedisTimeout),
		leaderStore: store.NewRedis(cfg.RedisAddrs, 3*cfg.RedisTimeout),
		dbs:         map[string]*sql.DB{},
		det:         detector{failAfter: cfg.FailAfter, margin: cfg.LeaseMargin},
		lastRejoin:  map[string]time.Time{},
		errant:      map[string]string{},
	}
	for _, n := range cfg.Nodes {
		db, err := mysqlx.Open(n+":3306", cfg.MySQLUser, cfg.MySQLPassword, cfg.ProbeTimeout, 4)
		if err != nil {
			return nil, err
		}
		c.dbs[n] = db
	}
	c.m = newMetrics(reg, cfg.Nodes)
	return c, nil
}

// Run blocks until ctx is cancelled, then gives up leadership.
func (c *Coordinator) Run(ctx context.Context) {
	go c.leaderLoop(ctx)
	t := time.NewTicker(c.cfg.Tick)
	defer t.Stop()
	for {
		c.tick(ctx)
		select {
		case <-ctx.Done():
			rctx, cancel := context.WithTimeout(context.Background(), time.Second)
			cluster.ReleaseLeader(rctx, c.leaderStore, c.id)
			cancel()
			return
		case <-t.C:
		}
	}
}

// Close releases connections.
func (c *Coordinator) Close() {
	for _, db := range c.dbs {
		_ = db.Close()
	}
	_ = c.store.Close()
	_ = c.leaderStore.Close()
}

// leaderLoop takes or extends the leader lease every LeaderRenew. The lease
// counts from before the request was sent, minus a drift allowance, so this
// process stops acting before any Redis instance can expire the key.
//
// A failed renewal changes nothing: the lease already granted stays valid
// until leaderUntil, and the next attempt may extend it. Stepping down (and
// deleting the key) on the first missed renewal made leadership flap every
// few seconds on a loaded host, and a leader change mid-failover costs a
// stale-claim delay. The key is released only once this process is no longer
// leader, to free a minority grab.
func (c *Coordinator) leaderLoop(ctx context.Context) {
	ttl := c.cfg.LeaderTTL
	t := time.NewTicker(c.cfg.LeaderRenew)
	defer t.Stop()
	was := false
	for {
		start := time.Now()
		acks := cluster.AcquireLeader(ctx, c.leaderStore, c.id, ttl)
		if acks >= c.leaderStore.Quorum() {
			c.leaderMu.Lock()
			c.leaderUntil = start.Add(ttl - ttl/10)
			c.leaderMu.Unlock()
		}
		now := c.leading()
		switch {
		case now && !was:
			c.leaderMu.Lock()
			c.leaderSince = time.Now()
			c.leaderMu.Unlock()
			c.log.Info("became leader", "id", c.id, "acks", acks)
		case !now && was:
			c.log.Warn("lost leadership: lease lapsed without a majority renewal", "acks", acks)
		}
		if !now && acks > 0 {
			cluster.ReleaseLeader(ctx, c.leaderStore, c.id)
		}
		was = now
		c.m.leader.Set(b2f(now))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Coordinator) leading() bool {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()
	return time.Now().Before(c.leaderUntil)
}

// leaderFor is how long this process has held leadership without a lapse.
func (c *Coordinator) leaderFor() time.Duration {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()
	if !time.Now().Before(c.leaderUntil) {
		return 0
	}
	return time.Since(c.leaderSince)
}

func (c *Coordinator) tick(ctx context.Context) {
	// Only the leader acts on observations, so only it probes every tick;
	// followers probe once a second for /status and metrics. Probes are not
	// free: see mysqlx.statusQuery.
	if c.leading() || time.Since(c.lastProbe) >= time.Second {
		c.obs, c.lastProbe = c.probeAll(ctx), time.Now()
	}
	obs := c.obs
	view := cluster.ReadTopology(ctx, c.store)
	c.record(view, obs)

	if !c.leading() {
		c.det.reset()
		return
	}
	if !view.OK {
		c.recoverTopology(ctx, view, obs)
		return
	}
	c.noTopoWarn, c.lostSince = false, time.Time{}
	c.repair(ctx, view)

	topo := view.Committed
	switch topo.State {
	case cluster.StatePromoting:
		// Our own unfinished attempt (e.g. no candidate could be prepared):
		// retry after a step timeout. Someone else's: that coordinator stopped
		// acting when its lease lapsed, which was before we could take the
		// lease; one more step timeout ensures no statement it sent just before
		// can still be running. Each retry claims a fresh epoch.
		mine := topo.By == c.id
		if (mine && time.Since(topo.UpdatedAt) > c.cfg.StepTimeout) || (!mine && c.leaderFor() > c.cfg.StepTimeout) {
			c.failover(ctx, view, obs, topo.Previous, "resuming a promotion left unfinished by "+topo.By)
		}
		return
	case cluster.StateFailed:
		return // a human has to look (topo.Reason)
	}

	if c.det.observe(c.evidence(ctx, topo.Primary, obs), time.Now()) {
		c.failover(ctx, view, obs, topo.Primary, "primary failed")
		return
	}
	c.fenceStrays(ctx, topo, obs)
	c.rejoin(ctx, topo, obs)
}

func (c *Coordinator) evidence(ctx context.Context, primary string, obs map[string]health.Observation) evidence {
	direct := obs[primary]
	ev := evidence{coordDown: !direct.Reachable || direct.ReadOnly}

	rec, holders, ok := health.Latest(c.store.Get(ctx, health.Key(primary)))
	ev.agentDown = !ok || holders < c.store.Quorum() ||
		(!rec.Healthy && slices.ContainsFunc(rec.Reasons, func(r health.Reason) bool { return r.Code == health.CodeUnreachable })) ||
		(rec.Observation.Reachable && rec.Observation.ReadOnly)

	if ev.coordDown && ev.agentDown {
		ev.leaseAbsent = cluster.PrimaryLeaseAbsent(ctx, c.store)
	}
	return ev
}

// probeAll probes every node in parallel.
func (c *Coordinator) probeAll(ctx context.Context) map[string]health.Observation {
	out := make(map[string]health.Observation, len(c.dbs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for n, db := range c.dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
			defer cancel()
			o, _ := mysqlx.Probe(pctx, db)
			mu.Lock()
			out[n] = o
			mu.Unlock()
			c.m.reachable.WithLabelValues(n).Set(b2f(o.Reachable))
		}()
	}
	wg.Wait()
	return out
}

// topologyLostAfter is how long a Redis majority must answer without holding
// a committed topology before the coordinator rebuilds one. It is longer than
// any transient state (a claim racing a final write) and than bootstrap's gap
// between writing the first epoch row and publishing the topology.
const topologyLostAfter = 5 * time.Second

// recoverTopology handles a Redis majority that answers but holds no
// committed topology. Redis keeps no data on disk, so if a majority of the
// instances restart, the topology is gone. The epoch history survives in
// MySQL (ha_meta.cluster, replicated with the data), so once the loss has
// lasted topologyLostAfter, the coordinator runs an ordinary promotion with
// every reachable node as a candidate and an epoch above anything Redis or
// MySQL has seen. The most advanced node wins, which is normally the old
// primary (it self-fenced when it lost its lease, so nothing diverged).
func (c *Coordinator) recoverTopology(ctx context.Context, v cluster.View, obs map[string]health.Observation) {
	warn := func(msg string, args ...any) {
		if !c.noTopoWarn {
			c.log.Warn(msg, args...)
			c.noTopoWarn = true
		}
	}
	if v.Reachable < c.store.Quorum() {
		c.lostSince = time.Time{}
		warn("Redis majority unreachable: cannot act", "reachable", v.Reachable)
		return
	}
	epoch, last, ok := c.epochHistory(ctx, obs)
	if !ok {
		c.lostSince = time.Time{}
		warn("no committed topology and no epoch history in MySQL; waiting for bootstrap")
		return
	}
	if c.lostSince.IsZero() {
		c.lostSince = time.Now()
		c.log.Warn("no committed topology on a Redis majority; will rebuild it from MySQL's epoch history",
			"after", topologyLostAfter, "mysql_epoch", epoch, "last_primary", last, "redis_max_epoch", v.MaxEpoch)
		return
	}
	if time.Since(c.lostSince) < topologyLostAfter {
		return
	}
	c.lostSince = time.Time{}
	v.MaxEpoch = max(v.MaxEpoch, epoch)
	c.failover(ctx, v, obs, "", "rebuilding the topology the Redis majority lost (last primary "+last+")")
}

// epochHistory returns the highest epoch recorded in ha_meta.cluster on any
// reachable node, and the primary it named.
func (c *Coordinator) epochHistory(ctx context.Context, obs map[string]health.Observation) (uint64, string, bool) {
	var best uint64
	var primary string
	found := false
	for _, n := range c.cfg.Nodes {
		if !obs[n].Reachable {
			continue
		}
		qctx, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
		var e uint64
		var p string
		err := c.dbs[n].QueryRowContext(qctx, "SELECT epoch, primary_node FROM ha_meta.cluster ORDER BY epoch DESC LIMIT 1").Scan(&e, &p)
		cancel()
		if err == nil && (!found || e > best) {
			best, primary, found = e, p, true
		}
	}
	return best, primary, found
}

// repair rewrites the committed topology onto instances that lack it, such
// as a Redis that restarted empty. The compare-and-set never lowers an epoch.
func (c *Coordinator) repair(ctx context.Context, v cluster.View) {
	if v.Holders < c.store.Size() {
		cluster.WriteTopology(ctx, c.store, v.Committed)
	}
}

// fenceStrays makes any writable node other than the primary read-only. This
// is how a paused old primary that wakes up after a failover is stopped.
func (c *Coordinator) fenceStrays(ctx context.Context, topo cluster.Topology, obs map[string]health.Observation) {
	for _, n := range c.cfg.Nodes {
		o := obs[n]
		if n == topo.Primary || !o.Reachable || o.ReadOnly {
			continue
		}
		c.log.Warn("fencing a writable node that is not the primary", "node", n, "primary", topo.Primary, "epoch", topo.Epoch)
		c.fence(ctx, n, "stray")
	}
}

// rejoin points a read-only, non-replicating node at the primary, but only
// if every transaction it holds is already on the primary. A node with
// transactions the primary lacks (errant GTIDs, e.g. a commit that reached
// the old primary's binlog but no replica) would break replication or
// silently diverge, so it is left fenced and reported for rebuild.
func (c *Coordinator) rejoin(ctx context.Context, topo cluster.Topology, obs map[string]health.Observation) {
	p := obs[topo.Primary]
	if !p.Reachable {
		return
	}
	for _, n := range c.cfg.Nodes {
		o := obs[n]
		if n == topo.Primary || !o.Reachable || !o.ReadOnly || o.SourceHost == topo.Primary {
			if n != topo.Primary && o.SourceHost == topo.Primary {
				delete(c.errant, n)
			}
			continue
		}
		if time.Since(c.lastRejoin[n]) < c.cfg.RejoinEvery {
			continue
		}
		c.lastRejoin[n] = time.Now()
		in, err := c.subset(ctx, topo.Primary, o.GTIDExecuted, p.GTIDExecuted)
		if err != nil {
			c.log.Warn("rejoin: compare GTIDs", "node", n, "err", err)
			continue
		}
		if !in {
			if c.errant[n] != o.GTIDExecuted {
				c.log.Warn("node holds transactions the primary lacks; leaving it fenced for rebuild",
					"node", n, "node_gtids", o.GTIDExecuted, "primary_gtids", p.GTIDExecuted)
				c.errant[n] = o.GTIDExecuted
				c.m.rejoins.WithLabelValues("errant").Inc()
			}
			continue
		}
		delete(c.errant, n)
		if o.GTIDExecuted == "" {
			// A node with no history replays the whole binlog. After a
			// failover the new primary's first transaction has
			// last_committed = 0, so the parallel applier may run it before
			// the DDL it depends on (Unknown database). Replay serially; the
			// setting is not persisted, so a restart returns to the default
			// once the node has caught up.
			if err := c.exec(ctx, n, q("SET GLOBAL replica_parallel_workers = 1")); err != nil {
				c.log.Warn("rejoin: could not serialize the applier", "node", n, "err", err)
			}
		}
		if err := c.repoint(ctx, n, topo.Primary); err != nil {
			c.log.Warn("rejoin failed", "node", n, "err", err)
			c.m.rejoins.WithLabelValues("error").Inc()
			continue
		}
		if err := c.exec(ctx, n, q("SET PERSIST super_read_only = ON"), q("SET GLOBAL rpl_semi_sync_source_enabled = OFF")); err != nil {
			c.log.Warn("rejoin: could not persist read-only", "node", n, "err", err)
		}
		c.log.Info("rejoined node as a replica", "node", n, "source", topo.Primary)
		c.m.rejoins.WithLabelValues("ok").Inc()
	}
}

func (c *Coordinator) record(v cluster.View, obs map[string]health.Observation) {
	if v.OK {
		c.m.epoch.Set(float64(v.Committed.Epoch))
	}
	errant := make(map[string]string, len(c.errant))
	for k, val := range c.errant {
		errant[k] = val
	}
	c.m.errant.Set(float64(len(errant)))
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status.ID, c.status.Leader, c.status.Topology = c.id, c.leading(), v
	c.status.Observations, c.status.Errant = obs, errant
}

// StatusHandler serves the coordinator's view as JSON.
func (c *Coordinator) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		c.statusMu.Lock()
		defer c.statusMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c.status)
	})
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
