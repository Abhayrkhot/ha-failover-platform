package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// The detection bench breaks one replica in a known way, then measures how
// long two observers take to notice, both polled from the host clock:
//
//	agent: the node's record on a Redis majority turns unhealthy
//	alert: Prometheus reports a firing alert labelled with the node
//
// Times are measured from the moment the bench starts injecting (before the
// docker/SQL call returns), so they are upper bounds on the real latency.
//
// A noisy host can make a healthy replica look unhealthy for a moment, and a
// trial can't tell that apart from detection. Two guards keep that visible:
// "control" trials run the same procedure without injecting anything (any
// detection there is a false positive), and a trial is marked suspect when the
// agent's reason doesn't match the fault or the detection is faster than the
// probe schedule allows. Suspect trials are reported but excluded from the
// latency statistics.

type detectOpts struct {
	node, faults, alerts, redis, prom, mysqlAddr, agentURL, out string
	trials                                                      int
	timeout, settle, controlWindow                              time.Duration
}

type trial struct {
	Fault     string    `json:"fault"`
	N         int       `json:"n"`
	StartedAt time.Time `json:"started_at"`
	InjectMS  float64   `json:"inject_ms"` // how long the injecting call took
	// UnhealthyAtMS is when, after the fault started, the replica objectively
	// became unhealthy. 0 for faults that break it at once; later for a
	// stalled applier, where lag must first grow past the agent's max-lag.
	UnhealthyAtMS float64  `json:"unhealthy_at_ms"`
	AgentMS       *float64 `json:"agent_ms,omitempty"` // fault start -> unhealthy on a Redis majority
	AgentReasons  []string `json:"agent_reasons,omitempty"`
	AlertMS       *float64 `json:"alert_ms,omitempty"` // fault start -> alert firing
	Alert         string   `json:"alert,omitempty"`
	Suspect       string   `json:"suspect,omitempty"` // why this trial may have caught noise, not the fault
	Error         string   `json:"error,omitempty"`
}

type stats struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	Max float64 `json:"max"`
}

type summaryRow struct {
	Fault       string `json:"fault"`
	Trials      int    `json:"trials"`
	AgentMissed int    `json:"agent_missed"`
	AlertMissed int    `json:"alert_missed"`
	Suspect     int    `json:"suspect"` // excluded from the latency stats
	// Latency from the moment the replica became unhealthy.
	Agent stats `json:"agent_ms"`
	Alert stats `json:"alert_ms"`
}

type report struct {
	Kind       string            `json:"kind"`
	Commit     string            `json:"commit"`
	Target     string            `json:"target"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
	Agent      map[string]any    `json:"agent_config"`
	Prometheus map[string]string `json:"prometheus_global"`
	Summary    []summaryRow      `json:"summary"`
	Trials     []trial           `json:"trials"`
}

type bench struct {
	opts   detectOpts
	redis  *store.Majority
	db     *sql.DB // root on the target, for SQL-level faults
	http   *http.Client
	alerts map[string]bool // alert names that count as "detected unhealthy"
	// From the agent's config: used to flag detections the probe schedule
	// makes impossible.
	maxLag        time.Duration
	probeInterval time.Duration
	failAfter     int
}

func runDetect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench detect", flag.ExitOnError)
	var o detectOpts
	fs.StringVar(&o.node, "node", "mysql-3", "replica to break")
	fs.StringVar(&o.faults, "faults", "control,pause,kill,stop-sql,applier-stall,partition", "faults to inject, in order (control = inject nothing)")
	fs.StringVar(&o.alerts, "alerts", "HafoNodeUnhealthy,HafoReplicationLagHigh,HafoReplicationStopped", "alerts that count as detecting an unhealthy replica")
	fs.IntVar(&o.trials, "trials", 10, "trials per fault")
	fs.StringVar(&o.redis, "redis", defaultRedis, "comma-separated Redis instances")
	fs.StringVar(&o.prom, "prometheus", "http://127.0.0.1:19090", "Prometheus base URL")
	fs.StringVar(&o.mysqlAddr, "mysql-addr", "", "target MySQL from the host (default 127.0.0.1:3306N for mysql-N)")
	fs.StringVar(&o.agentURL, "agent-url", "", "target agent from the host (default http://127.0.0.1:1910N for mysql-N)")
	fs.DurationVar(&o.timeout, "timeout", 10*time.Second, "give up on detection after this long")
	fs.DurationVar(&o.settle, "settle", 2*time.Second, "node must be healthy and alert-free this long before each trial")
	fs.DurationVar(&o.controlWindow, "control-window", 3*time.Second, "how long a control trial watches for false positives")
	fs.StringVar(&o.out, "out", "results", "directory for the JSON report")
	_ = fs.Parse(args)

	idx := o.node[len(o.node)-1:]
	if o.mysqlAddr == "" {
		o.mysqlAddr = "127.0.0.1:3306" + idx
	}
	if o.agentURL == "" {
		o.agentURL = "http://127.0.0.1:1910" + idx
	}

	dsn := mysql.NewConfig()
	dsn.User, dsn.Passwd = "root", os.Getenv("MYSQL_ROOT_PASSWORD")
	dsn.Net, dsn.Addr = "tcp", o.mysqlAddr
	dsn.Timeout = 2 * time.Second
	connector, err := mysql.NewConnector(dsn)
	if err != nil {
		return err
	}
	b := &bench{
		opts:   o,
		redis:  store.NewRedis(split(o.redis), 200*time.Millisecond),
		db:     sql.OpenDB(connector),
		http:   &http.Client{Timeout: 2 * time.Second},
		alerts: map[string]bool{},
	}
	for _, a := range split(o.alerts) {
		b.alerts[a] = true
	}
	defer b.redis.Close()
	defer b.db.Close()

	rep := report{Kind: "detect", Commit: gitCommit(), Target: o.node, StartedAt: time.Now()}
	if rep.Agent, err = b.agentConfig(ctx); err != nil {
		return fmt.Errorf("read agent config from %s: %w", o.agentURL, err)
	}
	if b.maxLag, err = time.ParseDuration(fmt.Sprint(rep.Agent["max_lag"])); err != nil {
		return fmt.Errorf("agent max_lag: %w", err)
	}
	if b.probeInterval, err = time.ParseDuration(fmt.Sprint(rep.Agent["probe_interval"])); err != nil {
		return fmt.Errorf("agent probe_interval: %w", err)
	}
	if fa, ok := rep.Agent["fail_after"].(float64); ok {
		b.failAfter = int(fa)
	}
	rep.Prometheus = b.promGlobal(ctx)

	var faults []fault
	for _, name := range split(o.faults) {
		f, err := b.fault(name)
		if err != nil {
			return err
		}
		faults = append(faults, f)
	}

	fmt.Printf("target %s, %d trials per fault, agent %v, prometheus %v\n\n", o.node, o.trials, rep.Agent, rep.Prometheus)
	var runErr error
run:
	for _, f := range faults {
		for i := 1; i <= o.trials; i++ {
			if runErr = b.waitSteady(ctx); runErr != nil {
				break run
			}
			t := b.runTrial(ctx, f, i)
			rep.Trials = append(rep.Trials, t)
			fmt.Println(t.line())
			if ctx.Err() != nil {
				runErr = ctx.Err()
				break run
			}
		}
	}

	rep.FinishedAt = time.Now()
	rep.Summary = summarize(rep.Trials)
	path, err := writeReport(o.out, rep)
	if err != nil {
		return err
	}
	printSummary(rep.Summary)
	fmt.Printf("\nreport: %s\n", path)
	return runErr
}

func (b *bench) runTrial(ctx context.Context, f fault, n int) trial {
	t := trial{Fault: f.name, N: n, StartedAt: time.Now()}
	t0 := time.Now()
	until, err := f.inject(ctx)
	injected := time.Now()
	t.InjectMS = ms(injected.Sub(t0))

	// Always heal, even if the run is being interrupted.
	defer func() {
		hctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if herr := f.heal(hctx); herr != nil {
			t.Error = strings.TrimSpace(t.Error + " heal: " + herr.Error())
		}
	}()
	if err != nil {
		t.Error = "inject: " + err.Error()
		return t
	}
	if until != nil {
		t.UnhealthyAtMS = ms(injected.Sub(t0) + *until)
	}

	window := b.opts.timeout
	if f.name == "control" {
		window = b.opts.controlWindow
	}
	deadline := t0.Add(window)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // agent path
		defer wg.Done()
		for time.Now().Before(deadline) && ctx.Err() == nil {
			rec, holders, ok := readHealth(ctx, b.redis, b.opts.node)
			if ok && holders >= b.redis.Quorum() && !rec.Healthy {
				v := ms(time.Since(t0))
				t.AgentMS = &v
				for _, r := range rec.Reasons {
					t.AgentReasons = append(t.AgentReasons, r.String())
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	go func() { // alert path
		defer wg.Done()
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if name, _, err := b.nodeAlerts(ctx); err == nil && name != "" {
				v := ms(time.Since(t0))
				t.AlertMS, t.Alert = &v, name
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	wg.Wait()
	t.Suspect = b.suspect(f, t)
	return t
}

// suspect explains why a detection may have been noise rather than the fault.
func (b *bench) suspect(f fault, t trial) string {
	if f.name == "control" || t.AgentMS == nil {
		return ""
	}
	if f.expect != "" && !slices.ContainsFunc(t.AgentReasons, func(r string) bool { return strings.HasPrefix(r, f.expect) }) {
		return fmt.Sprintf("agent reported %v, expected %s", t.AgentReasons, f.expect)
	}
	// An outage needs fail-after failed probes, one probe interval apart.
	if minimum := ms(time.Duration(b.failAfter-1) * b.probeInterval); f.down && *t.AgentMS < minimum {
		return fmt.Sprintf("agent detected in %.0fms, under the %.0fms that %d probes need", *t.AgentMS, minimum, b.failAfter)
	}
	return ""
}

// waitSteady blocks until the target is healthy on a Redis majority and has
// no pending or firing alerts, continuously for the settle period.
func (b *bench) waitSteady(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Minute)
	var since time.Time
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rec, holders, ok := readHealth(ctx, b.redis, b.opts.node)
		_, active, err := b.nodeAlerts(ctx)
		if ok && holders >= b.redis.Quorum() && rec.Healthy && !active && err == nil {
			if since.IsZero() {
				since = time.Now()
			}
			if time.Since(since) >= b.opts.settle {
				return nil
			}
		} else {
			since = time.Time{}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s did not return to healthy and alert-free within 2m", b.opts.node)
}

// --- faults

type fault struct {
	name   string
	expect string // reason code the agent should report
	down   bool   // node unreachable: detection waits for fail-after probes
	// inject starts the fault. A non-nil duration means the node becomes
	// unhealthy that long after inject returns, rather than immediately.
	inject func(context.Context) (*time.Duration, error)
	heal   func(context.Context) error
}

func (b *bench) fault(name string) (fault, error) {
	c := "hafo-" + b.opts.node
	run := func(args ...string) func(context.Context) error {
		return func(ctx context.Context) error { return docker(ctx, args...) }
	}
	now := func(step func(context.Context) error) func(context.Context) (*time.Duration, error) {
		return func(ctx context.Context) (*time.Duration, error) { return nil, step(ctx) }
	}
	sqlStep := func(stmt string) func(context.Context) error {
		return func(ctx context.Context) error { _, err := b.db.ExecContext(ctx, stmt); return err }
	}
	noop := func(context.Context) error { return nil }
	unreachable := func(in, out func(context.Context) error) fault {
		return fault{name: name, expect: health.CodeUnreachable, down: true, inject: now(in), heal: out}
	}
	switch name {
	case "control": // nothing: measures false positives on this host
		return fault{name: name, inject: now(noop), heal: noop}, nil
	case "pause": // process hang: TCP accepts, nothing answers
		return unreachable(run("pause", c), run("unpause", c)), nil
	case "kill": // crash: container gone until restarted
		return unreachable(run("kill", c), run("start", c)), nil
	case "partition": // network cut: server alive, unreachable
		return unreachable(run("network", "disconnect", "hafo-net", c),
			run("network", "connect", "--alias", b.opts.node, "hafo-net", c)), nil
	case "stop-sql": // replication applier stopped
		return fault{name: name, expect: health.CodeSQLThread,
			inject: now(sqlStep("STOP REPLICA SQL_THREAD")), heal: sqlStep("START REPLICA SQL_THREAD")}, nil
	case "applier-stall": // applier blocked on a lock: threads ON, lag grows
		return b.applierStall(), nil
	}
	return fault{}, fmt.Errorf("unknown fault %q", name)
}

// applierStall holds a READ lock on the heartbeat table, which the replica's
// applier must write. Both replication threads still report ON; only the lag
// shows the problem. The lag at lock time tells exactly when it will cross
// max-lag, which is when the replica becomes unhealthy.
func (b *bench) applierStall() fault {
	var conn *sql.Conn
	return fault{
		name:   "applier-stall",
		expect: health.CodeLagHigh,
		inject: func(ctx context.Context) (*time.Duration, error) {
			var err error
			if conn, err = b.db.Conn(ctx); err != nil {
				return nil, err
			}
			if _, err = conn.ExecContext(ctx, "LOCK TABLES ha_meta.heartbeat READ"); err != nil {
				return nil, err
			}
			var us int64
			if err = conn.QueryRowContext(ctx, "SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6)) FROM ha_meta.heartbeat WHERE id = 1").Scan(&us); err != nil {
				return nil, err
			}
			until := max(b.maxLag-time.Duration(us)*time.Microsecond, 0)
			return &until, nil
		},
		heal: func(ctx context.Context) error {
			if conn == nil {
				return nil
			}
			_, err := conn.ExecContext(ctx, "UNLOCK TABLES")
			return errors.Join(err, conn.Close())
		},
	}
}

func docker(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// --- Prometheus and agent

type promAlert struct {
	Labels map[string]string `json:"labels"`
	State  string            `json:"state"`
}

// nodeAlerts returns the name of a firing alert for the target (if any) and
// whether any alert for it is pending or firing.
func (b *bench) nodeAlerts(ctx context.Context) (firing string, active bool, err error) {
	var resp struct {
		Data struct {
			Alerts []promAlert `json:"alerts"`
		} `json:"data"`
	}
	if err := b.getJSON(ctx, b.opts.prom+"/api/v1/alerts", &resp); err != nil {
		return "", false, err
	}
	for _, a := range resp.Data.Alerts {
		if a.Labels["node"] != b.opts.node || !b.alerts[a.Labels["alertname"]] {
			continue
		}
		active = true
		if a.State == "firing" && firing == "" {
			firing = a.Labels["alertname"]
		}
	}
	return firing, active, nil
}

func (b *bench) agentConfig(ctx context.Context) (map[string]any, error) {
	var resp struct {
		Config map[string]any `json:"config"`
	}
	err := b.getJSON(ctx, b.opts.agentURL+"/status", &resp)
	return resp.Config, err
}

var promIntervalRE = regexp.MustCompile(`(?m)^\s+(scrape_interval|scrape_timeout|evaluation_interval):\s*(\S+)`)

// promGlobal extracts the global timing settings (the first occurrence of each
// key is in the global block).
func (b *bench) promGlobal(ctx context.Context) map[string]string {
	var resp struct {
		Data struct {
			YAML string `json:"yaml"`
		} `json:"data"`
	}
	out := map[string]string{}
	if b.getJSON(ctx, b.opts.prom+"/api/v1/status/config", &resp) != nil {
		return out
	}
	for _, m := range promIntervalRE.FindAllStringSubmatch(resp.Data.YAML, -1) {
		if _, seen := out[m[1]]; !seen {
			out[m[1]] = m[2]
		}
	}
	return out
}

func (b *bench) getJSON(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// --- reporting

func (t trial) line() string {
	f := func(p *float64) string {
		if p == nil {
			return "missed"
		}
		return fmt.Sprintf("%.0fms", *p-t.UnhealthyAtMS)
	}
	s := fmt.Sprintf("%-14s #%-2d agent %-7s alert %-7s", t.Fault, t.N, f(t.AgentMS), f(t.AlertMS))
	if t.UnhealthyAtMS > 0 {
		s += fmt.Sprintf(" (unhealthy %.0fms after fault start)", t.UnhealthyAtMS)
	}
	if t.Alert != "" {
		s += " " + t.Alert
	}
	if t.Suspect != "" {
		s += " SUSPECT " + t.Suspect
	}
	if t.Error != "" {
		s += " ERROR " + t.Error
	}
	return s
}

func summarize(trials []trial) []summaryRow {
	var order []string
	byFault := map[string][]trial{}
	for _, t := range trials {
		if _, ok := byFault[t.Fault]; !ok {
			order = append(order, t.Fault)
		}
		byFault[t.Fault] = append(byFault[t.Fault], t)
	}
	var rows []summaryRow
	for _, name := range order {
		ts := byFault[name]
		row := summaryRow{Fault: name, Trials: len(ts)}
		var agent, alert []float64
		for _, t := range ts {
			if t.Suspect != "" {
				row.Suspect++
				continue
			}
			if t.AgentMS == nil {
				row.AgentMissed++
			} else {
				agent = append(agent, *t.AgentMS-t.UnhealthyAtMS)
			}
			if t.AlertMS == nil {
				row.AlertMissed++
			} else {
				alert = append(alert, *t.AlertMS-t.UnhealthyAtMS)
			}
		}
		row.Agent, row.Alert = summary(agent), summary(alert)
		rows = append(rows, row)
	}
	return rows
}

// summary uses nearest-rank percentiles.
func summary(xs []float64) stats {
	if len(xs) == 0 {
		return stats{}
	}
	sort.Float64s(xs)
	rank := func(p float64) float64 { return xs[int(math.Ceil(p*float64(len(xs))))-1] }
	return stats{P50: rank(.50), P95: rank(.95), Max: xs[len(xs)-1]}
}

func printSummary(rows []summaryRow) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Println("\nlatency from replica unhealthy -> detected (ms)")
	fmt.Fprintln(w, "fault\ttrials\tsuspect\tagent p50\tp95\tmax\tmissed\talert p50\tp95\tmax\tmissed\t")
	var control *summaryRow
	for _, r := range rows {
		if r.Fault == "control" {
			control = &r
			continue
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%.0f\t%.0f\t%.0f\t%d\t%.0f\t%.0f\t%.0f\t%d\t\n", r.Fault, r.Trials, r.Suspect,
			r.Agent.P50, r.Agent.P95, r.Agent.Max, r.AgentMissed, r.Alert.P50, r.Alert.P95, r.Alert.Max, r.AlertMissed)
	}
	_ = w.Flush()
	if control != nil {
		fmt.Printf("\ncontrol (no fault): false positives agent %d/%d, alert %d/%d\n",
			control.Trials-control.AgentMissed, control.Trials, control.Trials-control.AlertMissed, control.Trials)
	}
}

func writeReport(dir string, rep report) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "detect-"+rep.StartedAt.Format("20060102-150405")+".json")
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(b, '\n'), 0o644)
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	c := strings.TrimSpace(string(out))
	if dirty, _ := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); len(dirty) > 0 {
		c += "-dirty"
	}
	return c
}
