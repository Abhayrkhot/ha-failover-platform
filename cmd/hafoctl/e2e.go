package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/mysqlx"
)

// e2e runs scenario tests against the live cluster. Each scenario starts from
// a steady cluster (serving primary, every node healthy, writes flowing),
// breaks one thing, asserts what must and must not happen, and repairs it.
// The suite exits non-zero if any assertion fails.

type check struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail,omitempty"`
}

type scenarioResult struct {
	Name    string  `json:"name"`
	What    string  `json:"what"`
	Checks  []check `json:"checks"`
	Seconds float64 `json:"seconds"`
	Error   string  `json:"error,omitempty"`
}

func (r scenarioResult) passed() bool {
	if r.Error != "" {
		return false
	}
	for _, c := range r.Checks {
		if !c.Pass {
			return false
		}
	}
	return len(r.Checks) > 0
}

type e2e struct {
	l    *lab
	opts failoverOpts
	cur  *scenarioResult
}

func (e *e2e) check(name string, pass bool, format string, args ...any) {
	c := check{Name: name, Pass: pass, Detail: fmt.Sprintf(format, args...)}
	e.cur.Checks = append(e.cur.Checks, c)
	mark := "PASS"
	if !pass {
		mark = "FAIL"
	}
	fmt.Printf("    %s  %s: %s\n", mark, name, c.Detail)
}

type scenario struct {
	name, what string
	run        func(*e2e, context.Context) error
}

var scenarios = []scenario{
	{"steady", "no faults: routing, leadership and writes behave", (*e2e).steady},
	{"router-restart", "the router restarts under write load", (*e2e).routerRestart},
	{"replica-crash", "one replica is killed", (*e2e).replicaCrash},
	{"replicas-down", "both replicas are killed: writes must stop, not fail over", (*e2e).replicasDown},
	{"redis-minority-down", "one of three Redis instances stops", (*e2e).redisMinorityDown},
	{"leader-crash", "the leading coordinator is killed", (*e2e).leaderCrash},
	{"primary-crash", "the primary is killed", func(e *e2e, ctx context.Context) error { return e.primaryFailure(ctx, "kill") }},
	{"primary-hang", "the primary freezes (docker pause) and later wakes up", func(e *e2e, ctx context.Context) error { return e.primaryFailure(ctx, "pause") }},
	{"primary-partition", "the primary is cut off the network", func(e *e2e, ctx context.Context) error { return e.primaryFailure(ctx, "partition") }},
	{"leader-and-primary-crash", "the leading coordinator and the primary die together", (*e2e).leaderAndPrimaryCrash},
	{"leader-dies-mid-failover", "the leader is killed after claiming an epoch, before it promotes", (*e2e).leaderMidway},
	{"redis-majority-down", "two of three Redis instances stop: the primary must fence itself", (*e2e).redisMajorityDown},
}

func runE2E(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("e2e", flag.ExitOnError)
	only := fs.String("only", "", "comma-separated scenarios to run (default: all)")
	out := fs.String("out", "results", "directory for the JSON report")
	redis := fs.String("redis", defaultRedis, "comma-separated Redis instances")
	router := fs.String("router", "127.0.0.1:23306", "router write port")
	_ = fs.Parse(args)

	o := failoverOpts{recoverTimeout: 30 * time.Second, convergeTimeout: 3 * time.Minute, hold: 2 * time.Second,
		settle: 3 * time.Second, writeEvery: 10 * time.Millisecond}
	l, closeLab, err := openLab(ctx, *redis, *router, o.writeEvery)
	if err != nil {
		return err
	}
	defer closeLab()
	e := &e2e{l: l, opts: o}

	rep := struct {
		Kind       string           `json:"kind"`
		Commit     string           `json:"commit"`
		StartedAt  time.Time        `json:"started_at"`
		FinishedAt time.Time        `json:"finished_at"`
		Scenarios  []scenarioResult `json:"scenarios"`
	}{Kind: "e2e", Commit: gitCommit(), StartedAt: time.Now()}

	for _, sc := range scenarios {
		if *only != "" && !slices.Contains(split(*only), sc.name) {
			continue
		}
		fmt.Printf("\n%s: %s\n", sc.name, sc.what)
		r := scenarioResult{Name: sc.name, What: sc.what}
		e.cur = &r
		start := time.Now()
		if err := l.waitSteady(ctx, o.settle, o.convergeTimeout); err != nil {
			r.Error = "not steady before the scenario: " + err.Error()
		} else {
			l.watch.take()
			if err := sc.run(e, ctx); err != nil {
				r.Error = err.Error()
			}
		}
		if r.Error != "" {
			fmt.Printf("    ERROR %s\n", r.Error)
		}
		r.Seconds = time.Since(start).Seconds()
		rep.Scenarios = append(rep.Scenarios, r)
		if ctx.Err() != nil {
			break
		}
	}
	rep.FinishedAt = time.Now()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	path := filepath.Join(*out, "e2e-"+rep.StartedAt.Format("20060102-150405")+".json")
	if err := writeJSON(path, rep); err != nil {
		return err
	}
	failed := 0
	fmt.Println("\nscenario                    checks   result   seconds")
	for _, r := range rep.Scenarios {
		passing := 0
		for _, c := range r.Checks {
			if c.Pass {
				passing++
			}
		}
		res := "PASS"
		if !r.passed() {
			res, failed = "FAIL", failed+1
		}
		fmt.Printf("%-27s %2d/%-2d    %-6s   %5.0f\n", r.Name, passing, len(r.Checks), res, r.Seconds)
	}
	fmt.Printf("\nreport: %s\n", path)
	if failed > 0 {
		return fmt.Errorf("%d of %d scenarios failed", failed, len(rep.Scenarios))
	}
	return nil
}

// --- scenarios

func (e *e2e) steady(ctx context.Context) error {
	v := e.l.topology(ctx)
	p := v.Committed.Primary
	e.check("topology committed on every Redis", v.OK && v.Committed.Serving() && v.Holders == 3,
		"epoch %d, primary %s, %d/3 copies", v.Committed.Epoch, p, v.Holders)
	_, id := e.l.leader(ctx)
	e.check("one coordinator leads", id != "", "%s", id)
	e.check("every node healthy", e.l.allHealthy(ctx), "")

	last, ok := e.l.w.lastAcked()
	var by string
	if ok {
		_ = e.l.root[p].QueryRowContext(ctx, "SELECT served_by FROM app.writes WHERE id = ?", last.ID).Scan(&by)
	}
	e.check("router sends writes to the primary", by == p, "last acked write served by %q", by)

	reads, err := mysqlx.Open("127.0.0.1:23307", "app", os.Getenv("APP_PASSWORD"), time.Second, 1)
	if err != nil {
		return err
	}
	defer reads.Close()
	var host string
	err = reads.QueryRowContext(ctx, "SELECT @@hostname").Scan(&host)
	e.check("router sends reads to a replica", err == nil && host != p && slices.Contains(e.l.nodes, host), "read served by %q (%v)", host, err)

	e0, start := v.Committed.Epoch, time.Now()
	time.Sleep(10 * time.Second)
	gap := e.l.w.maxAckGap(start, time.Now())
	e.check("writes flow without interruption", gap < time.Second, "longest gap between acks %s", gap.Round(time.Millisecond))
	e.check("no failover without a fault", e.l.epoch(ctx) == e0, "epoch %d", e.l.epoch(ctx))
	ws := e.l.watch.take()
	e.check("never two writable nodes", ws.DualWritableMS == 0, "%d samples, %dms dual-writable", ws.Samples, ws.DualWritableMS)
	return nil
}

func (e *e2e) routerRestart(ctx context.Context) error {
	e0, t0 := e.l.epoch(ctx), time.Now()
	if err := docker(ctx, "restart", "hafo-router"); err != nil {
		return err
	}
	ok, d := until(ctx, 15*time.Second, 50*time.Millisecond, func() bool { return e.l.w.lastAck().After(t0.Add(500 * time.Millisecond)) })
	e.check("writes resume after the router restarts", ok && d < 10*time.Second, "first ack %s after the restart began", d.Round(time.Millisecond))
	e.check("no failover", e.l.epoch(ctx) == e0, "epoch %d", e.l.epoch(ctx))
	return nil
}

func (e *e2e) replicaCrash(ctx context.Context) error {
	v := e.l.topology(ctx)
	p, e0 := v.Committed.Primary, v.Committed.Epoch
	r := e.replicas(p)[0]
	start := time.Now()
	if err := docker(ctx, "kill", "hafo-"+r); err != nil {
		return err
	}
	ok, d := until(ctx, 6*time.Second, 100*time.Millisecond, func() bool {
		_, pool, err := e.l.routerStatus(ctx)
		return err == nil && !slices.Contains(pool, r)
	})
	e.check("router stops sending reads to it", ok, "after %s", d.Round(time.Millisecond))
	time.Sleep(5 * time.Second)
	gap := e.l.w.maxAckGap(start, time.Now())
	e.check("writes keep flowing with one replica", gap < 1500*time.Millisecond, "longest gap %s", gap.Round(time.Millisecond))
	e.check("no failover", e.l.epoch(ctx) == e0, "epoch %d", e.l.epoch(ctx))

	if err := docker(ctx, "start", "hafo-"+r); err != nil {
		return err
	}
	res, err := e.l.converge(ctx, r, p, e.opts.convergeTimeout)
	e.check("replica comes back as a healthy replica", err == nil, "%s %v", res.Outcome, errStr(err))
	ok, d = until(ctx, 10*time.Second, 100*time.Millisecond, func() bool {
		_, pool, err := e.l.routerStatus(ctx)
		return err == nil && slices.Contains(pool, r)
	})
	e.check("router reads from it again", ok, "after %s", d.Round(time.Millisecond))
	return nil
}

func (e *e2e) replicasDown(ctx context.Context) error {
	v := e.l.topology(ctx)
	p, e0 := v.Committed.Primary, v.Committed.Epoch
	rs := e.replicas(p)
	t0 := time.Now()
	for _, r := range rs {
		if err := docker(ctx, "kill", "hafo-"+r); err != nil {
			return err
		}
	}
	time.Sleep(5 * time.Second)
	acks := e.l.w.acksBetween(t0.Add(2*time.Second), time.Now())
	e.check("writes stop: nothing can acknowledge them", acks == 0, "%d writes acknowledged 2-5s after both replicas died", acks)
	e.check("no failover: the primary is fine", e.l.epoch(ctx) == e0, "epoch %d", e.l.epoch(ctx))
	rec, _ := e.l.record(ctx, p)
	e.check("primary reports why", hasReason(rec, health.CodeNoReplicas, health.CodeWritesBlocked), "%v", rec.Reasons)
	e.check("primary is not fenced", slices.Contains(e.l.watch.writable(), p), "writable: %v", e.l.watch.writable())

	restart := time.Now()
	for _, r := range rs {
		if err := docker(ctx, "start", "hafo-"+r); err != nil {
			return err
		}
	}
	ok, d := until(ctx, 60*time.Second, 100*time.Millisecond, func() bool { return e.l.w.lastAck().After(restart) })
	e.check("writes resume once a replica is back", ok, "after %s", d.Round(time.Millisecond))
	for _, r := range rs {
		res, err := e.l.converge(ctx, r, p, e.opts.convergeTimeout)
		e.check(r+" rejoins", err == nil, "%s %v", res.Outcome, errStr(err))
	}
	cv, err := e.l.readBack(ctx, t0.Add(-3*time.Second), t0, p, p)
	if err != nil {
		return err
	}
	e.check("no acknowledged write lost", cv.LostAcked == 0, "%d acked, %d not acked, %d of those committed anyway", cv.Acked, cv.NotAcked, cv.PhantomRows)
	ok, sum := e.l.consistent(ctx)
	e.check("all nodes identical afterwards", ok, "%s", sum)
	return nil
}

func (e *e2e) redisMinorityDown(ctx context.Context) error {
	e0, t0 := e.l.epoch(ctx), time.Now()
	if err := docker(ctx, "stop", "-t", "1", "hafo-redis-3"); err != nil {
		return err
	}
	time.Sleep(8 * time.Second)
	v := e.l.topology(ctx)
	e.check("topology still committed on the other two", v.OK && v.Holders == 2, "%d copies", v.Holders)
	_, id := e.l.leader(ctx)
	e.check("a coordinator still leads", id != "", "%s", id)
	gap := e.l.w.maxAckGap(t0, time.Now())
	e.check("writes unaffected", gap < time.Second, "longest gap %s", gap.Round(time.Millisecond))
	e.check("no failover", e.l.epoch(ctx) == e0, "epoch %d", e.l.epoch(ctx))

	if err := docker(ctx, "start", "hafo-redis-3"); err != nil {
		return err
	}
	ok, d := until(ctx, 30*time.Second, 200*time.Millisecond, func() bool { return e.l.topology(ctx).Holders == 3 })
	e.check("restarted Redis gets the topology back", ok, "3/3 copies after %s (it waits 4s before starting)", d.Round(time.Millisecond))
	return nil
}

func (e *e2e) leaderCrash(ctx context.Context) error {
	c, id := e.l.leader(ctx)
	if c == "" {
		return fmt.Errorf("no leader to kill")
	}
	e0, t0 := e.l.epoch(ctx), time.Now()
	if err := docker(ctx, "kill", c); err != nil {
		return err
	}
	defer func() { _ = docker(context.Background(), "start", c) }()
	var next string
	ok, d := until(ctx, 10*time.Second, 100*time.Millisecond, func() bool {
		_, next = e.l.leader(ctx)
		return next != "" && next != id
	})
	e.check("another coordinator takes over", ok, "%s after %s (lease TTL 3s)", next, d.Round(time.Millisecond))
	time.Sleep(3 * time.Second)
	e.check("no failover", e.l.epoch(ctx) == e0, "epoch %d", e.l.epoch(ctx))
	gap := e.l.w.maxAckGap(t0, time.Now())
	e.check("writes unaffected", gap < time.Second, "longest gap %s", gap.Round(time.Millisecond))
	return nil
}

func (e *e2e) primaryFailure(ctx context.Context, fault string) error {
	t := failoverTrial(ctx, e.l, e.opts, fault, 1)
	fmt.Println("    " + t.line())
	e.failoverChecks(t, 5*time.Second)
	if fault != "kill" {
		e.check("woken old primary is fenced quickly", t.Watch.DualWritableMS <= 1000,
			"%dms with two nodes reporting writable", t.Watch.DualWritableMS)
	}
	return nil
}

func (e *e2e) leaderMidway(ctx context.Context) error {
	t := failoverTrial(ctx, e.l, e.opts, "leader-midway", 1)
	fmt.Println("    " + t.line())
	e.check("leader was killed mid-promotion", strings.HasPrefix(t.Note, "killed"), "%s", t.Note)
	// The next leader needs the lease (3s TTL) and one step timeout (2s) before it resumes.
	e.failoverChecks(t, 15*time.Second)
	return nil
}

func (e *e2e) leaderAndPrimaryCrash(ctx context.Context) error {
	c, _ := e.l.leader(ctx)
	v0 := e.l.topology(ctx)
	p := v0.Committed.Primary
	from, t0 := time.Now().Add(-3*time.Second), time.Now()
	if err := dockerKillBoth(ctx, c, "hafo-"+p); err != nil {
		return err
	}
	defer func() { _ = docker(context.Background(), "start", c) }()

	var t ftrial
	t.Fault, t.OldPrimary = "leader+primary kill", p
	ok, _ := until(ctx, e.opts.recoverTimeout, 20*time.Millisecond, func() bool {
		v := e.l.topology(ctx)
		if v.OK && v.Committed.Serving() && v.Committed.Epoch > v0.Committed.Epoch {
			t.NewPrimary, t.Epoch = v.Committed.Primary, v.Committed.Epoch
			return true
		}
		return false
	})
	if !ok {
		e.check("a new primary is promoted", false, "none within %s", e.opts.recoverTimeout)
		_ = docker(context.Background(), "start", "hafo-"+p)
		return nil
	}
	time.Sleep(e.opts.hold)
	cv, err := e.l.readBack(ctx, from, t0, p, t.NewPrimary)
	if err != nil {
		return err
	}
	t.clientView = cv
	if err := docker(ctx, "start", "hafo-"+p); err != nil {
		return err
	}
	t.Diverged, err = e.l.converge(ctx, p, t.NewPrimary, e.opts.convergeTimeout)
	t.Rejoin = t.Diverged.Outcome
	if err != nil {
		t.Error = err.Error()
	}
	t.ExtraFailovers = int(e.l.epoch(ctx)-v0.Committed.Epoch) - 1
	t.Consistent, t.Checksum = e.l.consistent(ctx)
	fmt.Println("    " + t.line())
	// The new leader must first take the lease (3s TTL) before it can detect anything.
	e.failoverChecks(t, 10*time.Second)
	return nil
}

func (e *e2e) redisMajorityDown(ctx context.Context) error {
	v0 := e.l.topology(ctx)
	p := v0.Committed.Primary
	from, t0 := time.Now().Add(-3*time.Second), time.Now()
	for _, r := range []string{"hafo-redis-2", "hafo-redis-3"} {
		if err := docker(ctx, "stop", "-t", "1", r); err != nil {
			return err
		}
	}
	restarted := false
	defer func() {
		if !restarted {
			_ = docker(context.Background(), "start", "hafo-redis-2", "hafo-redis-3")
		}
	}()

	ok, d := until(ctx, 6*time.Second, 50*time.Millisecond, func() bool { return len(e.l.watch.writable()) == 0 })
	e.check("primary fences itself once it cannot renew its lease", ok, "no writable node %s after losing the Redis majority (self-fence after 1.5s)", d.Round(time.Millisecond))
	time.Sleep(3 * time.Second)
	acks := e.l.w.acksBetween(t0.Add(3*time.Second), time.Now())
	e.check("writes stop while there is no majority", acks == 0, "%d acks 3s+ after the loss", acks)
	e.check("nothing is promoted without a majority", e.l.epoch(ctx) <= v0.Committed.Epoch || !e.l.topology(ctx).OK, "")

	back := time.Now()
	if err := docker(ctx, "start", "hafo-redis-2", "hafo-redis-3"); err != nil {
		return err
	}
	restarted = true
	var np string
	ok, d = until(ctx, 60*time.Second, 100*time.Millisecond, func() bool {
		v := e.l.topology(ctx)
		np = v.Committed.Primary
		return v.OK && v.Committed.Serving() && v.Committed.Epoch > v0.Committed.Epoch && e.l.w.lastAck().After(back)
	})
	e.check("cluster recovers: topology rebuilt from MySQL's epoch history", ok,
		"writes resume %s after Redis restarts (4s delayed start + 5s loss confirmation + promotion); primary %s", d.Round(time.Millisecond), np)
	if !ok {
		return nil
	}
	time.Sleep(e.opts.hold)
	cv, err := e.l.readBack(ctx, from, t0, p, np)
	if err != nil {
		return err
	}
	e.check("no acknowledged write lost", cv.LostAcked == 0, "%d acked, %d not acked", cv.Acked, cv.NotAcked)
	for _, n := range e.l.nodes {
		if n != np {
			res, err := e.l.converge(ctx, n, np, e.opts.convergeTimeout)
			e.check(n+" is a healthy replica", err == nil, "%s %v", res.Outcome, errStr(err))
		}
	}
	ok, sum := e.l.consistent(ctx)
	e.check("all nodes identical afterwards", ok, "%s", sum)
	return nil
}

func (e *e2e) failoverChecks(t ftrial, bound time.Duration) {
	rec := "-"
	if t.RecoveryMS != nil {
		rec = fmt.Sprintf("%.0fms", *t.RecoveryMS)
	}
	e.check("writes recover on a new primary in time", t.RecoveryMS != nil && *t.RecoveryMS < float64(bound.Milliseconds()),
		"%s (bound %s), %s -> %s, epoch %d", rec, bound, t.OldPrimary, t.NewPrimary, t.Epoch)
	e.check("no acknowledged write lost", t.LostAcked == 0, "%d acked in the window, %d lost", t.Acked, t.LostAcked)
	e.check("client never saw two primaries", t.Interleaved == 0 && t.ServedByOther == 0,
		"interleaved %d, served by another node %d", t.Interleaved, t.ServedByOther)
	e.check("exactly one failover", t.ExtraFailovers == 0, "%d extra", t.ExtraFailovers)
	e.check("old primary rejoins (rebuilt if it holds errant transactions)", t.Error == "" && t.Rejoin != "", "%s %s", t.Rejoin, t.Error)
	e.check("rows only the old primary had were never acknowledged", t.Diverged.DivergedAcked == 0,
		"%d diverged rows, %d acked (errant GTIDs %q)", t.Diverged.DivergedRows, t.Diverged.DivergedAcked, t.Diverged.ErrantGTIDs)
	e.check("all nodes identical afterwards", t.Consistent, "%s", t.Checksum)
}

func (e *e2e) replicas(primary string) []string {
	var out []string
	for _, n := range e.l.nodes {
		if n != primary {
			out = append(out, n)
		}
	}
	return out
}

func hasReason(rec health.Record, codes ...string) bool {
	return slices.ContainsFunc(rec.Reasons, func(r health.Reason) bool { return slices.Contains(codes, r.Code) })
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// dockerKillBoth kills two containers in one CLI call, so they die together.
func dockerKillBoth(ctx context.Context, a, b string) error {
	if a == "" {
		return fmt.Errorf("no leader container")
	}
	return docker(ctx, "kill", a, b)
}
