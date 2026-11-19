package coordinator

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	leader          prometheus.Gauge
	epoch           prometheus.Gauge
	errant          prometheus.Gauge
	reachable       *prometheus.GaugeVec
	failovers       *prometheus.CounterVec
	failoverSeconds prometheus.Histogram
	fenced          *prometheus.CounterVec
	rejoins         *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer, nodes []string) *metrics {
	m := &metrics{
		leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_coordinator_leader", Help: "1 while this coordinator holds the leader lease.",
		}),
		epoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_topology_epoch", Help: "Epoch of the committed topology this coordinator sees.",
		}),
		errant: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_coordinator_errant_nodes", Help: "Nodes left fenced because they hold transactions the primary lacks.",
		}),
		reachable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hafo_coordinator_node_reachable", Help: "1 if this coordinator's own probe of the node succeeded.",
		}, []string{"target"}),
		failovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_failovers_total", Help: "Failovers started by this coordinator, by outcome.",
		}, []string{"result"}),
		failoverSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "hafo_failover_duration_seconds",
			Help:    "From first suspecting the primary to publishing the new one.",
			Buckets: []float64{1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 7, 10},
		}),
		fenced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_fence_attempts_total", Help: "Attempts to make a node read-only, by reason and outcome.",
		}, []string{"why", "result"}),
		rejoins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_rejoins_total", Help: "Attempts to rejoin a node as a replica, by outcome.",
		}, []string{"result"}),
	}
	for _, n := range nodes {
		m.reachable.WithLabelValues(n)
	}
	for _, r := range []string{"promoted", "stopped"} {
		m.failovers.WithLabelValues(r)
	}
	reg.MustRegister(m.leader, m.epoch, m.errant, m.reachable, m.failovers, m.failoverSeconds, m.fenced, m.rejoins)
	return m
}
