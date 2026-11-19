// Command hafo-router proxies MySQL clients to the primary (writes) or a
// healthy replica (reads), following the topology on the Redis majority.
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

	"github.com/Abhayrkhot/ha-failover-platform/internal/router"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

func main() {
	var (
		cfg                       router.Config
		nodes, redisAddrs, listen string
	)
	flag.StringVar(&cfg.WriteListen, "write-listen", ":3306", "address for write connections (to the primary)")
	flag.StringVar(&cfg.ReadListen, "read-listen", ":3307", "address for read connections (to healthy replicas)")
	flag.StringVar(&listen, "listen", ":9100", "HTTP address for /metrics, /status, /healthz")
	flag.StringVar(&nodes, "nodes", "mysql-1,mysql-2,mysql-3", "comma-separated MySQL nodes (port 3306)")
	flag.StringVar(&redisAddrs, "redis-addrs", "redis-1:6379,redis-2:6379,redis-3:6379", "comma-separated Redis instances")
	flag.DurationVar(&cfg.Poll, "poll", 50*time.Millisecond, "how often to re-read the topology")
	flag.DurationVar(&cfg.DialTimeout, "dial-timeout", 500*time.Millisecond, "deadline for connecting to a backend")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "router")
	m := store.NewRedis(strings.Split(redisAddrs, ","), 100*time.Millisecond)
	defer m.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	src := router.RedisSource{Store: m, Nodes: strings.Split(nodes, ",")}
	r := router.New(cfg, log, src, func(n string) string { return n + ":3306" }, reg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := r.Run(ctx); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/status", r.StatusHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()
	log.Info("router started", "write", cfg.WriteListen, "read", cfg.ReadListen)
	<-ctx.Done()

	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}
