// Command hafo-archiver streams a healthy replica's binary log into an
// archive directory for point-in-time restores.
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

	"github.com/Abhayrkhot/ha-failover-platform/internal/archiver"
)

func main() {
	var (
		cfg                       archiver.Config
		nodes, redisAddrs, listen string
	)
	flag.StringVar(&cfg.Dir, "dir", "/archive", "archive directory (one subdirectory per source server_uuid)")
	flag.StringVar(&nodes, "nodes", "mysql-1,mysql-2,mysql-3", "comma-separated MySQL nodes (port 3306)")
	flag.StringVar(&redisAddrs, "redis-addrs", "redis-1:6379,redis-2:6379,redis-3:6379", "comma-separated Redis instances")
	flag.StringVar(&listen, "listen", ":9100", "HTTP address for /metrics, /status, /healthz")
	flag.StringVar(&cfg.ReplUser, "repl-user", "repl", "user mysqlbinlog streams as; password from $REPL_PASSWORD")
	flag.StringVar(&cfg.MySQLUser, "mysql-user", "hafo", "user that reads server_uuid and the binlog list; password from $HAFO_PASSWORD")
	flag.IntVar(&cfg.ServerID, "server-id", 990, "server_id of the dump connection (must not be a node's)")
	flag.DurationVar(&cfg.Poll, "poll", time.Second, "how often to re-check which replica to archive")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "archiver")
	cfg.Nodes, cfg.RedisAddrs = strings.Split(nodes, ","), strings.Split(redisAddrs, ",")
	cfg.ReplPassword, cfg.MySQLPassword = os.Getenv("REPL_PASSWORD"), os.Getenv("HAFO_PASSWORD")

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	a, err := archiver.New(cfg, log, reg)
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	defer a.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/status", a.StatusHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 2 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()
	log.Info("archiver started", "dir", cfg.Dir, "nodes", cfg.Nodes)
	a.Run(ctx)

	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}
