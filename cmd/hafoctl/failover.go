package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
)

// The failover bench breaks the current primary while a client writes
// through the router, and measures what the client sees:
//
//	recovery: fault start -> first acknowledged write served by the new primary
//	gap:      last write acknowledged by the old primary -> that same write
//
// Per trial it also checks the invariants that define "no split brain":
//
//   - lost_acked: every acknowledged write is on the new primary
//   - interleaved: no write was acknowledged by the old primary after the new
//     one had acknowledged one (the client never saw two primaries)
//   - extra_failovers: exactly one epoch per fault
//   - dual_writable_ms: how long two nodes reported read_only=OFF at once
//     (sampled every 100ms; a node that cannot accept commits can still show
//     the flag, so this is reported, and the acked-write checks decide)
//   - consistent: after the old primary rejoins (or is rebuilt), app.writes
//     has the same checksum on every node
//
// "control" trials inject nothing and must show no failover. "leader-midway"
// kills the primary, waits until the leading coordinator has claimed the next
// epoch (state "promoting"), and kills that coordinator before it can finish:
// another coordinator must take the lease and complete the promotion under a
// newer epoch.

type failoverOpts struct {
	faults, redis, router, out           string
	trials                               int
	writeEvery, recoverTimeout, settle   time.Duration
	convergeTimeout, controlWindow, hold time.Duration
}

type ftrial struct {
	Fault      string    `json:"fault"`
	N          int       `json:"n"`
	StartedAt  time.Time `json:"started_at"`
	OldPrimary string    `json:"old_primary"`
	NewPrimary string    `json:"new_primary,omitempty"`
	Epoch      uint64    `json:"epoch,omitempty"`

	clientView
	ExtraFailovers int        `json:"extra_failovers"`
	Watch          watchStats `json:"watch"`
	// The coordinator's own clock, in ms from first suspecting the primary.
	Phases     map[string]float64 `json:"coordinator_phases_ms,omitempty"`
	Rejoin     string             `json:"rejoin,omitempty"`
	Diverged   convergeResult     `json:"old_primary_divergence"`
	Note       string             `json:"note,omitempty"`
	Consistent bool               `json:"consistent"`
	Checksum   string             `json:"checksum,omitempty"`
	Error      string             `json:"error,omitempty"`
}

func runFailover(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench failover", flag.ExitOnError)
	var o failoverOpts
	fs.StringVar(&o.faults, "faults", "control,kill,pause,partition", "faults to apply to the primary, in order (also: leader-midway)")
	fs.IntVar(&o.trials, "trials", 10, "trials per fault")
	fs.StringVar(&o.redis, "redis", defaultRedis, "comma-separated Redis instances")
	fs.StringVar(&o.router, "router", "127.0.0.1:23306", "router write port")
	fs.DurationVar(&o.writeEvery, "write-every", 10*time.Millisecond, "pause between writes")
	fs.DurationVar(&o.recoverTimeout, "recover-timeout", 30*time.Second, "give up waiting for a new primary after this long")
	fs.DurationVar(&o.convergeTimeout, "converge-timeout", 2*time.Minute, "give up waiting for the old primary to rejoin after this long")
	fs.DurationVar(&o.settle, "settle", 3*time.Second, "cluster must be healthy and acking writes this long before each trial")
	fs.DurationVar(&o.controlWindow, "control-window", 8*time.Second, "how long a control trial watches for an uncommanded failover")
	fs.DurationVar(&o.hold, "hold", 2*time.Second, "keep the fault in place this long after the new primary is published")
	fs.StringVar(&o.out, "out", "results", "directory for the JSON report")
	_ = fs.Parse(args)

	l, closeLab, err := openLab(ctx, o.redis, o.router, o.writeEvery)
	if err != nil {
		return err
	}
	defer closeLab()

	rep := struct {
		Kind       string    `json:"kind"`
		Commit     string    `json:"commit"`
		StartedAt  time.Time `json:"started_at"`
		FinishedAt time.Time `json:"finished_at"`
		WriteEvery string    `json:"write_every"`
		Trials     []ftrial  `json:"trials"`
	}{Kind: "failover", Commit: gitCommit(), StartedAt: time.Now(), WriteEvery: o.writeEvery.String()}

	var runErr error
run:
	for _, f := range split(o.faults) {
		for i := 1; i <= o.trials; i++ {
			if runErr = l.waitSteady(ctx, o.settle, o.convergeTimeout); runErr != nil {
				break run
			}
			t := failoverTrial(ctx, l, o, f, i)
			rep.Trials = append(rep.Trials, t)
			fmt.Println(t.line())
			if ctx.Err() != nil {
				runErr = ctx.Err()
				break run
			}
		}
	}
	rep.FinishedAt = time.Now()

	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return err
	}
	path := filepath.Join(o.out, "failover-"+rep.StartedAt.Format("20060102-150405")+".json")
	if err := writeJSON(path, rep); err != nil {
		return err
	}
	printFailoverSummary(rep.Trials)
	fmt.Printf("\nreport: %s\n", path)
	return runErr
}

func failoverTrial(ctx context.Context, l *lab, o failoverOpts, fault string, n int) ftrial {
	v0 := l.topology(ctx)
	t := ftrial{Fault: fault, N: n, StartedAt: time.Now(), OldPrimary: v0.Committed.Primary}
	l.watch.take()
	from := time.Now().Add(-3 * time.Second)

	if fault == "control" {
		time.Sleep(o.controlWindow)
		v := l.topology(ctx)
		t.NewPrimary, t.Epoch = v.Committed.Primary, v.Committed.Epoch
		t.ExtraFailovers = int(v.Committed.Epoch - v0.Committed.Epoch)
		t.Watch = l.watch.take()
		if cv, err := l.readBack(ctx, from, time.Now(), t.OldPrimary, t.OldPrimary); err == nil {
			t.clientView = cv
		}
		t.Consistent, t.Checksum = l.consistent(ctx)
		return t
	}

	inject, heal, err := primaryFault(fault, t.OldPrimary)
	if err != nil {
		t.Error = err.Error()
		return t
	}
	leader := ""
	if fault == "leader-midway" {
		if leader, _ = l.leader(ctx); leader == "" {
			t.Error = "no coordinator leader to kill"
			return t
		}
	}
	t0 := time.Now()
	if err := inject(ctx); err != nil {
		t.Error = "inject: " + err.Error()
		_ = heal(context.Background())
		return t
	}
	healed := false
	defer func() {
		if !healed {
			_ = heal(context.Background())
		}
	}()
	midway := false
	if leader != "" {
		defer func() { _ = docker(context.Background(), "start", leader) }()
		midway, _ = until(ctx, 10*time.Second, 5*time.Millisecond, func() bool {
			v := l.topology(ctx)
			return v.OK && v.Committed.Epoch > v0.Committed.Epoch && v.Committed.State == cluster.StatePromoting
		})
		if midway {
			_ = docker(ctx, "kill", leader)
			t.Note = fmt.Sprintf("killed %s after it claimed epoch %d, before it promoted anyone", leader, v0.Committed.Epoch+1)
		} else {
			t.Note = "claim not observed; leader not killed"
		}
	}

	ok, _ := until(ctx, o.recoverTimeout, 20*time.Millisecond, func() bool {
		v := l.topology(ctx)
		if v.OK && v.Committed.Serving() && v.Committed.Epoch > v0.Committed.Epoch {
			t.NewPrimary, t.Epoch = v.Committed.Primary, v.Committed.Epoch
			return true
		}
		return false
	})
	if !ok {
		t.Error = "no new primary within " + o.recoverTimeout.String()
		return t
	}
	time.Sleep(o.hold) // fault still in place: the new primary must carry writes alone

	if fo, ok := cluster.ReadFailover(ctx, l.redis); ok && fo.Epoch == t.Epoch {
		at := func(x time.Time) float64 { return ms(x.Sub(fo.SuspectedAt)) }
		t.Phases = map[string]float64{
			"lease_expired": at(fo.LeaseExpiredAt), "claimed": at(fo.ClaimedAt), "prepared": at(fo.PreparedAt),
			"writable": at(fo.WritableAt), "published": at(fo.PublishedAt),
		}
	}
	if cv, err := l.readBack(ctx, from, t0, t.OldPrimary, t.NewPrimary); err != nil {
		t.Error = "read back: " + err.Error()
	} else {
		t.clientView = cv
	}

	healed = true
	if err := heal(context.Background()); err != nil {
		t.Error = strings.TrimSpace(t.Error + " heal: " + err.Error())
		return t
	}
	t.Diverged, err = l.converge(ctx, t.OldPrimary, t.NewPrimary, o.convergeTimeout)
	t.Rejoin = t.Diverged.Outcome
	if err != nil {
		t.Error = strings.TrimSpace(t.Error + " converge: " + err.Error())
	}
	t.Watch = l.watch.take()
	v := l.topology(ctx)
	t.ExtraFailovers = int(v.Committed.Epoch-v0.Committed.Epoch) - 1
	if midway {
		t.ExtraFailovers-- // the abandoned claim plus the completing one
	}
	t.Consistent, t.Checksum = l.consistent(ctx)
	return t
}

func primaryFault(name, node string) (inject, heal func(context.Context) error, err error) {
	c := "hafo-" + node
	run := func(args ...string) func(context.Context) error {
		return func(ctx context.Context) error { return docker(ctx, args...) }
	}
	switch name {
	case "kill", "leader-midway":
		return run("kill", c), run("start", c), nil
	case "pause":
		return run("pause", c), run("unpause", c), nil
	case "partition":
		return run("network", "disconnect", "hafo-net", c), run("network", "connect", "--alias", node, "hafo-net", c), nil
	}
	return nil, nil, fmt.Errorf("unknown fault %q", name)
}

func (t ftrial) line() string {
	f := func(p *float64) string {
		if p == nil {
			return "-"
		}
		return fmt.Sprintf("%.0fms", *p)
	}
	s := fmt.Sprintf("%-9s #%-2d %s -> %-7s recovery %-7s gap %-7s acked %-4d lost %d interleaved %d phantom %d dual %dms extra %d consistent %v %s",
		t.Fault, t.N, t.OldPrimary, t.NewPrimary, f(t.RecoveryMS), f(t.GapMS), t.Acked, t.LostAcked, t.Interleaved,
		t.PhantomRows, t.Watch.DualWritableMS, t.ExtraFailovers, t.Consistent, t.Rejoin)
	if t.Diverged.ErrantGTIDs != "" {
		s += fmt.Sprintf(" (errant %s, %d diverged rows, %d of them acked)", t.Diverged.ErrantGTIDs, t.Diverged.DivergedRows, t.Diverged.DivergedAcked)
	}
	if p := t.Phases; p != nil {
		s += fmt.Sprintf(" [lease %.0f claim %.0f writable %.0f published %.0f]", p["lease_expired"], p["claimed"], p["writable"], p["published"])
	}
	if t.Note != "" {
		s += " NOTE " + t.Note
	}
	if t.Error != "" {
		s += " ERROR " + t.Error
	}
	return s
}

func printFailoverSummary(trials []ftrial) {
	type agg struct {
		n, failed, lost, interleaved, other, phantom, extra, inconsistent, errant, dualMax, divAcked int
		recovery, gap, lease, promote                                                                []float64
	}
	var order []string
	by := map[string]*agg{}
	for _, t := range trials {
		a, ok := by[t.Fault]
		if !ok {
			a = &agg{}
			by[t.Fault] = a
			order = append(order, t.Fault)
		}
		a.n++
		a.lost += t.LostAcked
		a.interleaved += t.Interleaved
		a.other += t.ServedByOther
		a.phantom += t.PhantomRows
		a.extra += t.ExtraFailovers
		a.dualMax = max(a.dualMax, t.Watch.DualWritableMS)
		a.divAcked += t.Diverged.DivergedAcked
		if !t.Consistent {
			a.inconsistent++
		}
		if strings.HasPrefix(t.Rejoin, "rebuilt") {
			a.errant++
		}
		if t.Fault == "control" {
			continue
		}
		if t.RecoveryMS == nil {
			a.failed++
			continue
		}
		a.recovery = append(a.recovery, *t.RecoveryMS)
		if t.GapMS != nil {
			a.gap = append(a.gap, *t.GapMS)
		}
		if p := t.Phases; p != nil {
			a.lease = append(a.lease, p["lease_expired"])
			a.promote = append(a.promote, p["published"]-p["lease_expired"])
		}
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Println("\nclient-visible recovery (ms) and split-brain invariants")
	fmt.Fprintln(w, "fault\ttrials\trecovery p50\tp95\tmax\tno recovery\tlost acked\tinterleaved\tother\tdiverged acked\textra failovers\tdual-writable max ms\tinconsistent\tphantom\trebuilt\t")
	for _, f := range order {
		a := by[f]
		r := summary(a.recovery)
		fmt.Fprintf(w, "%s\t%d\t%.0f\t%.0f\t%.0f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\n", f, a.n, r.P50, r.P95, r.Max, a.failed,
			a.lost, a.interleaved, a.other, a.divAcked, a.extra, a.dualMax, a.inconsistent, a.phantom, a.errant)
	}
	_ = w.Flush()
	fmt.Println("\ncoordinator phases, p50 ms (lease wait = first suspicion -> lease gone; promote = lease gone -> published)")
	for _, f := range order {
		if a := by[f]; len(a.lease) > 0 {
			fmt.Printf("  %-10s lease wait %.0f  promote %.0f  gap p50 %.0f\n", f, summary(a.lease).P50, summary(a.promote).P50, summary(a.gap).P50)
		}
	}
}
