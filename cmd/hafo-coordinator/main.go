// Command hafo-coordinator keeps the cluster's topology correct. Run several;
// the one holding the leader lease acts.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Abhayrkhot/ha-failover-platform/internal/coordinator"
)

func main() {
	var (
		cfg                coordinator.Config
		nodes, redis, addr string
	)
	flag.StringVar(&cfg.ID, "id", "", "coordinator name, e.g. coordinator-1 (required)")
	flag.StringVar(&nodes, "nodes", "mysql-1,mysql-2,mysql-3", "comma-separated MySQL nodes (port 3306)")
	flag.StringVar(&redis, "redis-addrs", "redis-1:6379,redis-2:6379,redis-3:6379", "comma-separated Redis instances")
	flag.StringVar(&addr, "listen", ":9100", "HTTP address for /metrics, /status, /healthz")
	flag.StringVar(&cfg.MySQLUser, "mysql-user", "hafo", "MySQL user; password from $HAFO_PASSWORD")
	flag.StringVar(&cfg.ReplUser, "repl-user", "repl", "replication user; password from $REPL_PASSWORD")
	flag.StringVar(&cfg.ClientUser, "client-user", "app", "MySQL user whose connections are killed on a fenced node")
	flag.DurationVar(&cfg.Tick, "tick", 250*time.Millisecond, "time between reconcile passes")
	flag.DurationVar(&cfg.ProbeTimeout, "probe-timeout", 200*time.Millisecond, "deadline for one probe of a node")
	flag.DurationVar(&cfg.RedisTimeout, "redis-timeout", 100*time.Millisecond, "deadline for each Redis operation")
	flag.IntVar(&cfg.FailAfter, "fail-after", 2, "consecutive ticks both observers must see the primary down")
	// Checked on ticks, so it rounds up to a whole tick: one tick by default.
	flag.DurationVar(&cfg.LeaseMargin, "lease-margin", 250*time.Millisecond, "extra wait once the primary lease is gone from a majority")
	flag.DurationVar(&cfg.LeaderTTL, "leader-ttl", 3*time.Second, "TTL of the leader lease")
	flag.DurationVar(&cfg.LeaderRenew, "leader-renew", 500*time.Millisecond, "how often the leader lease is renewed")
	flag.DurationVar(&cfg.StepTimeout, "step-timeout", 2*time.Second, "deadline for each promotion step")

	flag.DurationVar(&cfg.RejoinEvery, "rejoin-every", 2*time.Second, "minimum spacing of rejoin attempts per node")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("coordinator", cfg.ID)
	cfg.Nodes, cfg.RedisAddrs = strings.Split(nodes, ","), strings.Split(redis, ",")
	cfg.MySQLPassword, cfg.ReplPassword = os.Getenv("HAFO_PASSWORD"), os.Getenv("REPL_PASSWORD")
	if cfg.ID == "" {
		log.Error("-id is required")
		os.Exit(2)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	c, err := coordinator.New(cfg, log, prometheus.WrapRegistererWith(prometheus.Labels{"coordinator": cfg.ID}, reg))
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	defer c.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/status", c.StatusHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()
	log.Info("coordinator started", "nodes", cfg.Nodes, "redis", cfg.RedisAddrs)
	c.Run(ctx)

	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}
