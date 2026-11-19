package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

func runHealth(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	redisAddrs := fs.String("redis", defaultRedis, "comma-separated Redis instances")
	nodes := fs.String("nodes", defaultNodes, "comma-separated node names")
	_ = fs.Parse(args)

	m := store.NewRedis(split(*redisAddrs), 200*time.Millisecond)
	defer m.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tROLE\tHEALTHY\tCOPIES\tAGE\tLAG\tREASONS")
	for _, n := range split(*nodes) {
		rec, holders, ok := readHealth(ctx, m, n)
		if !ok {
			fmt.Fprintf(w, "%s\t-\t-\t0/%d\t-\t-\tno record (agent down or cut off from Redis)\n", n, m.Size())
			continue
		}
		lag := "-"
		if l := rec.Observation.LagSeconds; l != nil && rec.Role == "replica" {
			lag = fmt.Sprintf("%.3fs", *l)
		}
		reasons := make([]string, len(rec.Reasons))
		for i, r := range rec.Reasons {
			reasons[i] = r.String()
		}
		fmt.Fprintf(w, "%s\t%s\t%t\t%d/%d\t%s\t%s\t%s\n", n, rec.Role, rec.Healthy, holders, m.Size(),
			time.Since(rec.ObservedAt).Round(time.Millisecond), lag, strings.Join(reasons, "; "))
	}
	return w.Flush()
}
