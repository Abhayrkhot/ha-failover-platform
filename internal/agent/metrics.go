package agent

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
)

type metrics struct {
	probes        prometheus.Counter
	probeErrors   *prometheus.CounterVec
	probeDuration prometheus.Histogram
	heartbeats    *prometheus.CounterVec
	publishes     *prometheus.CounterVec
	healthChanges *prometheus.CounterVec
	selfFences    *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer, a *Agent) *metrics {
	m := &metrics{
		probes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hafo_mysql_probes_total", Help: "Probes of the local MySQL server.",
		}),
		probeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_mysql_probe_errors_total", Help: "Failed probes by kind (connect, timeout, query).",
		}, []string{"kind"}),
		probeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "hafo_mysql_probe_duration_seconds",
			Help:    "Wall time of one probe, including timeouts.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .2, .5},
		}),
		heartbeats: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_heartbeat_writes_total", Help: "Heartbeat writes on the primary by result.",
		}, []string{"result"}),
		publishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_redis_publish_total", Help: "Health record writes per Redis instance by result.",
		}, []string{"redis", "result"}),
		healthChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_health_transitions_total", Help: "Verdict changes, by the state entered.",
		}, []string{"to"}),
	}
	m.selfFences = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hafo_self_fence_total", Help: "Times the agent made its own server read-only, by reason (stray, lease) and outcome.",
	}, []string{"reason", "result"})
	for _, kind := range []string{"connect", "timeout", "query"} {
		m.probeErrors.WithLabelValues(kind) // export zeros so rate() works from the start
	}
	reg.MustRegister(m.probes, m.probeErrors, m.probeDuration, m.heartbeats, m.publishes, m.healthChanges, m.selfFences,
		&stateCollector{a: a})
	return m
}

// stateCollector exports the latest record as gauges at scrape time. Replica-
// or primary-only series are emitted only while they apply and the server is
// reachable, so "absent" means "unknown", never a misleading zero.
type stateCollector struct{ a *Agent }

var (
	descUp       = prometheus.NewDesc("hafo_mysql_up", "1 if the last probe of the local MySQL succeeded.", nil, nil)
	descHealthy  = prometheus.NewDesc("hafo_node_healthy", "1 if the agent's debounced verdict is healthy.", nil, nil)
	descRole     = prometheus.NewDesc("hafo_node_role", "1 for the node's current role.", []string{"role"}, nil)
	descReadOnly = prometheus.NewDesc("hafo_mysql_read_only", "@@read_only.", nil, nil)
	descSuperRO  = prometheus.NewDesc("hafo_mysql_super_read_only", "@@super_read_only.", nil, nil)
	descLag      = prometheus.NewDesc("hafo_replication_lag_seconds", "Age of the newest applied primary heartbeat (replicas).", nil, nil)
	descIO       = prometheus.NewDesc("hafo_replication_io_running", "1 if the replication IO thread is ON (replicas).", nil, nil)
	descSQL      = prometheus.NewDesc("hafo_replication_sql_running", "1 if the replication applier is ON (replicas).", nil, nil)
	descSemiOn   = prometheus.NewDesc("hafo_semi_sync_source_enabled", "1 if semi-sync is enabled on this source (primary).", nil, nil)
	descReplicas = prometheus.NewDesc("hafo_replicas_connected", "Replicas streaming from this node; all are semi-sync (primary).", nil, nil)
	descBlocked  = prometheus.NewDesc("hafo_heartbeat_blocked_writes", "Heartbeat commits waiting for a semi-sync ack (primary).", nil, nil)
	descAcks     = prometheus.NewDesc("hafo_redis_publish_acks", "Redis instances that accepted the last health record.", nil, nil)
	descQuorum   = prometheus.NewDesc("hafo_redis_quorum", "1 if the last health record reached a Redis majority.", nil, nil)
	descLease    = prometheus.NewDesc("hafo_primary_lease_held", "1 if this agent renewed the primary lease on a Redis majority this tick.", nil, nil)
)

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descUp, descHealthy, descRole, descReadOnly, descSuperRO, descLag,
		descIO, descSQL, descSemiOn, descReplicas, descBlocked, descAcks, descQuorum, descLease} {
		ch <- d
	}
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.a.snapshot()
	if s.rec.Seq == 0 {
		return // no probe yet
	}
	g := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	o := s.rec.Observation
	g(descUp, b2f(o.Reachable))
	g(descHealthy, b2f(s.rec.Healthy))
	for _, r := range []health.Role{health.RolePrimary, health.RoleReplica, health.RoleFenced, health.RoleUnknown} {
		g(descRole, b2f(s.rec.Role == r), string(r))
	}
	g(descAcks, float64(s.acks))
	g(descQuorum, b2f(s.acks >= s.quorum))
	g(descLease, b2f(s.leasing && s.leaseAcks >= s.quorum))
	if !o.Reachable {
		return
	}
	g(descReadOnly, b2f(o.ReadOnly))
	g(descSuperRO, b2f(o.SuperReadOnly))
	switch o.Role() {
	case health.RoleReplica:
		g(descIO, b2f(o.IOState == "ON"))
		g(descSQL, b2f(o.SQLState == "ON"))
		if o.LagSeconds != nil {
			g(descLag, *o.LagSeconds)
		}
	case health.RolePrimary:
		g(descSemiOn, b2f(o.SemiSyncSourceEnabled))
		g(descReplicas, float64(o.ConnectedReplicas))
		g(descBlocked, float64(o.BlockedWrites))
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
