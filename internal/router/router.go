// Package router is a TCP proxy that sends client connections to the node
// the committed topology names: one port for writes (the primary) and one for
// reads (healthy replicas). It does not parse MySQL's protocol.
//
// The router follows the topology and nothing else. It never routes writes to
// a node because that node is writable; when the primary changes it closes
// every write connection to the old one; and while an epoch is being promoted
// it refuses new write connections, so clients see a short, clean outage
// instead of writing to a node that is being fenced.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
)

// Source supplies the cluster state the router follows.
type Source interface {
	Topology(ctx context.Context) cluster.View
	ReadPool(ctx context.Context) []string // healthy replicas of the current primary
}

// Config is the router's tuning.
type Config struct {
	WriteListen string
	ReadListen  string
	Poll        time.Duration
	DialTimeout time.Duration
}

// Router proxies connections; safe for concurrent use.
type Router struct {
	cfg  Config
	log  *slog.Logger
	src  Source
	addr func(node string) string
	m    *metrics

	mu      sync.Mutex
	epoch   uint64
	primary string // empty while no primary is serving
	pool    []string
	next    int
	links   map[*link]struct{}
}

type link struct {
	kind, node     string
	client, server net.Conn
	once           sync.Once
}

func (l *link) close() {
	l.once.Do(func() {
		_ = l.client.Close()
		_ = l.server.Close()
	})
}

// New builds a router. addr maps a node name to its MySQL address.
func New(cfg Config, log *slog.Logger, src Source, addr func(string) string, reg prometheus.Registerer) *Router {
	return &Router{cfg: cfg, log: log, src: src, addr: addr, m: newMetrics(reg), links: map[*link]struct{}{}}
}

// Run listens until ctx is cancelled. It returns once both listeners are
// open; serve errors after that are logged.
func (r *Router) Run(ctx context.Context) error {
	wl, err := net.Listen("tcp", r.cfg.WriteListen)
	if err != nil {
		return err
	}
	rl, err := net.Listen("tcp", r.cfg.ReadListen)
	if err != nil {
		_ = wl.Close()
		return err
	}
	r.refresh(ctx)
	go r.watch(ctx)
	go r.serve(wl, "write")
	go r.serve(rl, "read")
	go func() {
		<-ctx.Done()
		_ = wl.Close()
		_ = rl.Close()
		r.mu.Lock()
		r.closeLocked(func(*link) bool { return true })
		r.mu.Unlock()
	}()
	return nil
}

func (r *Router) watch(ctx context.Context) {
	t := time.NewTicker(r.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refresh(ctx)
		}
	}
}

// refresh applies the latest committed topology and read pool. Without a
// committed view (Redis majority unreachable) the router keeps what it has:
// the semi-sync fence, not the router, is what stops a stale primary.
func (r *Router) refresh(ctx context.Context) {
	v := r.src.Topology(ctx)
	pool := r.src.ReadPool(ctx)

	r.mu.Lock()
	defer r.mu.Unlock()
	if v.OK && v.Committed.Epoch >= r.epoch {
		next := ""
		if v.Committed.Serving() {
			next = v.Committed.Primary
		}
		if v.Committed.Epoch != r.epoch || next != r.primary {
			r.log.Info("topology", "epoch", v.Committed.Epoch, "state", v.Committed.State, "primary", next, "was", r.primary)
			if next != r.primary {
				n := r.closeLocked(func(l *link) bool { return l.kind == "write" && l.node != next })
				r.m.switches.Inc()
				if n > 0 {
					r.log.Info("closed write connections to the previous primary", "count", n)
				}
			}
			r.epoch, r.primary = v.Committed.Epoch, next
		}
	}
	r.pool = pool
	r.closeLocked(func(l *link) bool { return l.kind == "read" && !slices.Contains(pool, l.node) && l.node != r.primary })
	r.m.epoch.Set(float64(r.epoch))
	r.m.serving.Set(b2f(r.primary != ""))
	r.m.pool.Set(float64(len(pool)))
}

func (r *Router) closeLocked(match func(*link) bool) int {
	n := 0
	for l := range r.links {
		if match(l) {
			l.close()
			delete(r.links, l)
			n++
		}
	}
	return n
}

// pick chooses a backend: the primary for writes; a healthy replica (round
// robin) for reads, falling back to the primary when none is healthy.
func (r *Router) pick(kind string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if kind == "write" || len(r.pool) == 0 {
		return r.primary
	}
	r.next = (r.next + 1) % len(r.pool)
	return r.pool[r.next]
}

func (r *Router) serve(ln net.Listener, kind string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				r.log.Error("accept", "port", kind, "err", err)
			}
			return
		}
		node := r.pick(kind)
		if node == "" {
			r.m.conns.WithLabelValues(kind, "refused").Inc()
			_ = c.Close()
			continue
		}
		go r.proxy(c, kind, node)
	}
}

func (r *Router) proxy(client net.Conn, kind, node string) {
	server, err := net.DialTimeout("tcp", r.addr(node), r.cfg.DialTimeout)
	if err != nil {
		r.m.conns.WithLabelValues(kind, "dial_error").Inc()
		_ = client.Close()
		return
	}
	l := &link{kind: kind, node: node, client: client, server: server}

	r.mu.Lock()
	if kind == "write" && node != r.primary { // the primary changed while dialling
		r.mu.Unlock()
		l.close()
		r.m.conns.WithLabelValues(kind, "refused").Inc()
		return
	}
	r.links[l] = struct{}{}
	r.mu.Unlock()
	r.m.conns.WithLabelValues(kind, "proxied").Inc()
	r.m.active.WithLabelValues(kind).Inc()

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(server, client)
	go pipe(client, server)
	<-done
	l.close()

	r.mu.Lock()
	delete(r.links, l)
	r.mu.Unlock()
	r.m.active.WithLabelValues(kind).Dec()
}

// StatusHandler serves the router's current routing as JSON.
func (r *Router) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		s := map[string]any{"epoch": r.epoch, "primary": r.primary, "read_pool": r.pool, "links": len(r.links)}
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	})
}

type metrics struct {
	conns    *prometheus.CounterVec
	active   *prometheus.GaugeVec
	switches prometheus.Counter
	epoch    prometheus.Gauge
	serving  prometheus.Gauge
	pool     prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		conns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_router_connections_total", Help: "Client connections by port and outcome (proxied, refused, dial_error).",
		}, []string{"port", "result"}),
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hafo_router_active_connections", Help: "Open proxied connections by port.",
		}, []string{"port"}),
		switches: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hafo_router_primary_switches_total", Help: "Times the write backend changed (including to none).",
		}),
		epoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_router_topology_epoch", Help: "Epoch of the topology the router is following.",
		}),
		serving: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_router_serving", Help: "1 while the router has a primary to send writes to.",
		}),
		pool: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_router_read_pool_size", Help: "Healthy replicas the read port balances over.",
		}),
	}
	for _, p := range []string{"write", "read"} {
		m.active.WithLabelValues(p)
		for _, res := range []string{"proxied", "refused", "dial_error"} {
			m.conns.WithLabelValues(p, res)
		}
	}
	reg.MustRegister(m.conns, m.active, m.switches, m.epoch, m.serving, m.pool)
	return m
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
