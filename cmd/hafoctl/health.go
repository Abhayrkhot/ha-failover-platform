package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

func runHealth(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	redisAddrs := fs.String("redis", defaultRedis, "comma-separated Redis instances")
	nodes := fs.String("nodes", defaultNodes, "comma-separated node names")
	_ = fs.Parse(args)

	m := store.NewRedis(split(*redisAddrs), 200*time.Millisecond)
	defer m.Close()

	if v := cluster.ReadTopology(ctx, m); v.OK {
		t := v.Committed
		fmt.Printf("topology  epoch %d, primary %q, state %s (%d/%d copies)", t.Epoch, t.Primary, t.State, v.Holders, m.Size())
		if t.Reason != "" && t.State != cluster.StateOK {
			fmt.Printf(", reason: %s", t.Reason)
		}
		fmt.Println()
	} else {
		fmt.Printf("topology  none committed (%d/%d Redis reachable, max epoch %d)\n", v.Reachable, m.Size(), v.MaxEpoch)
	}
	if id, n := cluster.Leader(ctx, m); id != "" {
		fmt.Printf("leader    %s (%d/%d copies)\n\n", id, n, m.Size())
	} else {
		fmt.Printf("leader    none\n\n")
	}

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
