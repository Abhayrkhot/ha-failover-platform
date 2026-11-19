// Command hafo-agent watches one MySQL server and publishes its health.
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

	"github.com/Abhayrkhot/ha-failover-platform/internal/agent"
)

func main() {
	var (
		cfg         agent.Config
		redisAddrs  string
		metricsAddr string
	)
	flag.StringVar(&cfg.Node, "node", "", "node name, e.g. mysql-1 (required)")
	flag.StringVar(&cfg.MySQLAddr, "mysql-addr", "", "local MySQL host:port (required)")
	flag.StringVar(&cfg.MySQLUser, "mysql-user", "hafo", "MySQL user; password comes from $HAFO_PASSWORD")
	flag.StringVar(&redisAddrs, "redis-addrs", "redis-1:6379,redis-2:6379,redis-3:6379", "comma-separated Redis instances")
	flag.StringVar(&metricsAddr, "listen", ":9100", "HTTP address for /metrics, /status, /healthz")
	flag.DurationVar(&cfg.ProbeInterval, "probe-interval", 250*time.Millisecond, "time between probes")
	flag.DurationVar(&cfg.ProbeTimeout, "probe-timeout", 200*time.Millisecond, "deadline for one probe, including connect")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", 250*time.Millisecond, "time between heartbeat writes on the primary")
	flag.DurationVar(&cfg.HeartbeatTimeout, "heartbeat-timeout", time.Second, "deadline for one heartbeat commit")
	flag.DurationVar(&cfg.PublishTimeout, "publish-timeout", 100*time.Millisecond, "deadline for each Redis write")
	flag.DurationVar(&cfg.HealthTTL, "health-ttl", 1500*time.Millisecond, "TTL of each published health record")
	flag.IntVar(&cfg.Thresholds.FailAfter, "fail-after", 3, "consecutive failed probes before the node is unreachable")
	flag.IntVar(&cfg.Thresholds.RecoverAfter, "recover-after", 2, "consecutive clean probes before the node is healthy again")
	flag.DurationVar(&cfg.Thresholds.MaxLag, "max-lag", time.Second, "replica lag above this is unhealthy")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("node", cfg.Node)
	cfg.MySQLPassword = os.Getenv("HAFO_PASSWORD")
	cfg.RedisAddrs = strings.Split(redisAddrs, ",")
	if cfg.Node == "" || cfg.MySQLAddr == "" {
		log.Error("-node and -mysql-addr are required")
		os.Exit(2)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	a, err := agent.New(cfg, log, prometheus.WrapRegistererWith(prometheus.Labels{"node": cfg.Node}, reg))
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	defer a.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/status", a.StatusHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()

	log.Info("agent started", "mysql", cfg.MySQLAddr, "redis", cfg.RedisAddrs, "listen", metricsAddr)
	a.Run(ctx)

	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}
