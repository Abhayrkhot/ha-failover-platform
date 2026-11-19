// Command hafoctl inspects the cluster and runs experiments against it from
// the host, through the ports compose publishes on 127.0.0.1.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

const usage = `usage:
  hafoctl health [flags]          topology, leader, and health records as a Redis majority sees them
  hafoctl bench detect [flags]    time fault -> detection for one replica
  hafoctl bench failover [flags]  fail the primary under write load; time recovery, check acked writes
  hafoctl e2e [-only a,b]         scenario suite with pass/fail assertions (breaks and repairs the cluster)
  hafoctl seed [-mib 1024]        load app.payload with random rows through the router
  hafoctl drill [flags]           destroy the database tier, restore it from backup + binlog archive, time it
`

var (
	defaultRedis = "127.0.0.1:63791,127.0.0.1:63792,127.0.0.1:63793"
	defaultNodes = "mysql-1,mysql-2,mysql-3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	args := os.Args[1:]
	var err error
	switch {
	case len(args) >= 1 && args[0] == "health":
		err = runHealth(ctx, args[1:])
	case len(args) >= 2 && args[0] == "bench" && args[1] == "detect":
		err = runDetect(ctx, args[2:])
	case len(args) >= 2 && args[0] == "bench" && args[1] == "failover":
		err = runFailover(ctx, args[2:])
	case len(args) >= 1 && args[0] == "e2e":
		err = runE2E(ctx, args[1:])
	case len(args) >= 1 && args[0] == "seed":
		err = runSeed(ctx, args[1:])
	case len(args) >= 1 && args[0] == "drill":
		err = runDrill(ctx, args[1:])
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// readHealth returns the freshest record for node and how many instances hold one.
func readHealth(ctx context.Context, m *store.Majority, node string) (health.Record, int, bool) {
	return health.Latest(m.Get(ctx, health.Key(node)))
}

func split(s string) []string { return strings.Split(s, ",") }

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
