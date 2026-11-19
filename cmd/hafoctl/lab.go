package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/mysqlx"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// lab is the host's view of the running cluster, shared by `bench failover`
// and `e2e`: root connections to every node (through the published ports), a
// client writing through the router, and a watcher sampling which nodes are
// writable.
type lab struct {
	redis  *store.Majority
	root   map[string]*sql.DB
	nodes  []string
	w      *writer
	watch  *watcher
	router string // router HTTP base URL
}

func openLab(ctx context.Context, redisAddrs, routerWrite string, writeEvery time.Duration) (*lab, func(), error) {
	l := &lab{
		redis:  store.NewRedis(split(redisAddrs), 200*time.Millisecond),
		root:   map[string]*sql.DB{},
		nodes:  split(defaultNodes),
		router: "http://127.0.0.1:19300",
	}
	var dbs []*sql.DB
	for _, n := range l.nodes {
		db, err := mysqlx.Open("127.0.0.1:3306"+n[len(n)-1:], "root", os.Getenv("MYSQL_ROOT_PASSWORD"), time.Second, 4)
		if err != nil {
			return nil, nil, err
		}
		l.root[n] = db
		dbs = append(dbs, db)
	}
	app, err := mysqlx.Open(routerWrite, "app", os.Getenv("APP_PASSWORD"), 500*time.Millisecond, 1)
	if err != nil {
		return nil, nil, err
	}
	dbs = append(dbs, app)
	// IDs unique across runs on the same cluster.
	l.w = &writer{db: app, every: writeEvery, next: time.Now().UnixMilli() * 1000}
	l.watch = &watcher{l: l}
	bg, cancel := context.WithCancel(ctx)
	go l.w.run(bg)
	go l.watch.run(bg)
	return l, func() {
		cancel()
		for _, db := range dbs {
			_ = db.Close()
		}
		_ = l.redis.Close()
	}, nil
}

// --- write load

type write struct {
	ID    int64
	Sent  time.Time
	Done  time.Time
	Acked bool
	Err   string
}

// writer inserts one row at a time through the router. Each row records the
// server that executed it, so "who acknowledged this write" is read back from
// the data rather than inferred.
type writer struct {
	db    *sql.DB
	every time.Duration
	next  int64
	gate  sync.Mutex // held by pause(): no write in flight while held

	mu     sync.Mutex
	writes []write
}

const insertWrite = "INSERT INTO app.writes (id, written_at, served_by) VALUES (?, NOW(6), @@hostname)"

func (w *writer) run(ctx context.Context) {
	for ctx.Err() == nil {
		w.gate.Lock()
		w.next++
		wr := write{ID: w.next, Sent: time.Now()}
		wctx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := w.db.ExecContext(wctx, insertWrite, wr.ID)
		cancel()
		w.gate.Unlock()
		wr.Done, wr.Acked = time.Now(), err == nil
		if err != nil {
			wr.Err = err.Error()
		}
		w.mu.Lock()
		w.writes = append(w.writes, wr)
		w.mu.Unlock()
		time.Sleep(w.every)
	}
}

func (w *writer) pause()  { w.gate.Lock() }
func (w *writer) resume() { w.gate.Unlock() }

func (w *writer) since(t time.Time) []write {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []write
	for _, wr := range w.writes {
		if !wr.Sent.Before(t) {
			out = append(out, wr)
		}
	}
	return out
}

func (w *writer) lastAck() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.writes) - 1; i >= 0; i-- {
		if w.writes[i].Acked {
			return w.writes[i].Done
		}
	}
	return time.Time{}
}

// maxAckGap is the longest stretch without an acknowledged write in [from, to].
func (w *writer) maxAckGap(from, to time.Time) time.Duration {
	prev, gap := from, time.Duration(0)
	for _, wr := range w.since(from) {
		if wr.Acked && !wr.Done.After(to) {
			gap = max(gap, wr.Done.Sub(prev))
			prev = wr.Done
		}
	}
	return max(gap, to.Sub(prev))
}

// --- split-brain watcher

// watcher samples @@read_only on every node every 100ms. A node that does
// not answer (paused, partitioned, down) is unknown, not writable.
type watcher struct {
	l *lab

	mu        sync.Mutex
	samples   int
	dual      int // samples with two or more nodes writable
	dualNodes map[string]bool
	latest    []string // writable nodes in the latest sample
}

func (wt *watcher) run(ctx context.Context) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var mu sync.Mutex
		var writable []string
		var wg sync.WaitGroup
		for _, n := range wt.l.nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				qctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
				defer cancel()
				var ro int
				if wt.l.root[n].QueryRowContext(qctx, "SELECT @@read_only").Scan(&ro) == nil && ro == 0 {
					mu.Lock()
					writable = append(writable, n)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		wt.mu.Lock()
		wt.samples++
		wt.latest = writable
		if len(writable) >= 2 {
			wt.dual++
			if wt.dualNodes == nil {
				wt.dualNodes = map[string]bool{}
			}
			for _, n := range writable {
				wt.dualNodes[n] = true
			}
		}
		wt.mu.Unlock()
	}
}

type watchStats struct {
	Samples        int      `json:"samples"`
	DualWritableMS int      `json:"dual_writable_ms"` // ~100ms per sample with >1 node writable
	DualNodes      []string `json:"dual_nodes,omitempty"`
}

// take returns the stats since the last call and resets them.
func (wt *watcher) take() watchStats {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	s := watchStats{Samples: wt.samples, DualWritableMS: wt.dual * 100}
	for n := range wt.dualNodes {
		s.DualNodes = append(s.DualNodes, n)
	}
	wt.samples, wt.dual, wt.dualNodes = 0, 0, nil
	return s
}

func (wt *watcher) writable() []string {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	return append([]string(nil), wt.latest...)
}

// --- cluster state

func (l *lab) topology(ctx context.Context) cluster.View { return cluster.ReadTopology(ctx, l.redis) }

// record returns node's health record if a Redis majority holds one.
func (l *lab) record(ctx context.Context, node string) (health.Record, bool) {
	rec, holders, ok := readHealth(ctx, l.redis, node)
	return rec, ok && holders >= l.redis.Quorum()
}

func (l *lab) allHealthy(ctx context.Context) bool {
	for _, n := range l.nodes {
		if rec, ok := l.record(ctx, n); !ok || !rec.Healthy {
			return false
		}
	}
	return true
}

// until polls cond every poll until it holds or timeout passes; it returns
// whether cond held and how long that took.
func until(ctx context.Context, timeout, poll time.Duration, cond func() bool) (bool, time.Duration) {
	start := time.Now()
	for {
		if cond() {
			return true, time.Since(start)
		}
		if time.Since(start) > timeout || ctx.Err() != nil {
			return false, time.Since(start)
		}
		time.Sleep(poll)
	}
}

// waitSteady waits until the topology names a serving primary, every node is
// healthy, and writes are being acknowledged, continuously for settle.
func (l *lab) waitSteady(ctx context.Context, settle, timeout time.Duration) error {
	var since time.Time
	ok, _ := until(ctx, timeout, 100*time.Millisecond, func() bool {
		v := l.topology(ctx)
		good := v.OK && v.Committed.Serving() && time.Since(l.w.lastAck()) < 500*time.Millisecond && l.allHealthy(ctx)
		switch {
		case !good:
			since = time.Time{}
		case since.IsZero():
			since = time.Now()
		}
		return good && time.Since(since) >= settle
	})
	if !ok {
		return fmt.Errorf("cluster not healthy with writes flowing within %s", timeout)
	}
	return nil
}

// leader returns the leading coordinator's container and lease id.
func (l *lab) leader(ctx context.Context) (container, id string) {
	id, n := cluster.Leader(ctx, l.redis)
	if id == "" || n < l.redis.Quorum() {
		return "", ""
	}
	return "hafo-" + strings.SplitN(id, "/", 2)[0], id
}

// errant reports whether node, read-only and not replicating from primary,
// holds transactions the primary lacks.
func (l *lab) errant(ctx context.Context, node, primary string) (bool, error) {
	qctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var ro int
	var source, gtid string
	err := l.root[node].QueryRowContext(qctx, `SELECT @@read_only,
		IFNULL((SELECT HOST FROM performance_schema.replication_connection_configuration WHERE CHANNEL_NAME = ''), ''),
		REPLACE(@@gtid_executed, '\n', '')`).Scan(&ro, &source, &gtid)
	if err != nil || ro == 0 || source == primary {
		return false, err
	}
	var in bool
	err = l.root[primary].QueryRowContext(qctx, `SELECT GTID_SUBSET(?, REPLACE(@@gtid_executed, '\n', ''))`, gtid).Scan(&in)
	return !in, err
}

// converge waits for node to be a healthy replica of primary, rebuilding it
// if it holds errant GTIDs. It returns "rejoined" or "rebuilt (errant)".
func (l *lab) converge(ctx context.Context, node, primary string, timeout time.Duration) (string, error) {
	outcome := "rejoined"
	ok, _ := until(ctx, timeout, 500*time.Millisecond, func() bool {
		rec, found := l.record(ctx, node)
		if found && rec.Healthy && rec.Role == health.RoleReplica && rec.Observation.SourceHost == primary {
			return true
		}
		if outcome == "rejoined" {
			if errant, err := l.errant(ctx, node, primary); err == nil && errant {
				outcome = "rebuilt (errant)"
				if out, err := exec.CommandContext(ctx, "scripts/rebuild.sh", node).CombinedOutput(); err != nil {
					outcome = "rebuild failed: " + strings.TrimSpace(string(out))
				}
			}
		}
		return false
	})
	if !ok {
		return outcome, fmt.Errorf("%s did not rejoin within %s", node, timeout)
	}
	return outcome, nil
}

// consistent pauses the writer, waits for every replica to apply everything
// the primary has, and compares CHECKSUM TABLE app.writes on all nodes.
func (l *lab) consistent(ctx context.Context) (bool, string) {
	v := l.topology(ctx)
	if !v.OK || !v.Committed.Serving() {
		return false, "no serving primary"
	}
	p := v.Committed.Primary
	l.w.pause()
	defer l.w.resume()
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var gtid string
	if err := l.root[p].QueryRowContext(qctx, `SELECT REPLACE(@@gtid_executed, '\n', '')`).Scan(&gtid); err != nil {
		return false, "read primary gtid: " + err.Error()
	}
	sums := map[string]int64{}
	for _, n := range l.nodes {
		if n != p {
			var timedOut int
			if err := l.root[n].QueryRowContext(qctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 15)", gtid).Scan(&timedOut); err != nil || timedOut != 0 {
				return false, fmt.Sprintf("%s did not catch up to the primary (%v)", n, err)
			}
		}
		var table string
		var sum int64
		if err := l.root[n].QueryRowContext(qctx, "CHECKSUM TABLE app.writes").Scan(&table, &sum); err != nil {
			return false, n + ": " + err.Error()
		}
		sums[n] = sum
	}
	for _, n := range l.nodes {
		if sums[n] != sums[p] {
			return false, fmt.Sprintf("checksums differ: %v", sums)
		}
	}
	return true, fmt.Sprintf("app.writes checksum %d on all %d nodes", sums[p], len(l.nodes))
}

// clientView reads a window of writes back from the primary.
type clientView struct {
	Acked       int `json:"acked"`
	NotAcked    int `json:"not_acked"`
	LostAcked   int `json:"lost_acked"`   // acknowledged but missing on the primary: must be 0
	PhantomRows int `json:"phantom_rows"` // not acknowledged, yet committed
	// Acknowledged by the old primary after the new one had acknowledged a
	// write: the client saw two primaries. Must be 0.
	Interleaved int `json:"interleaved"`
	// Acknowledged by a node that was neither old nor new primary: must be 0.
	ServedByOther int `json:"served_by_other"`

	RecoveryMS *float64 `json:"recovery_ms,omitempty"` // t0 -> first ack served by the new primary
	GapMS      *float64 `json:"gap_ms,omitempty"`      // last old-primary ack -> first new-primary ack
}

func (l *lab) readBack(ctx context.Context, from, t0 time.Time, oldP, newP string) (clientView, error) {
	var cv clientView
	ws := l.w.since(from)
	if len(ws) == 0 {
		return cv, fmt.Errorf("no writes in the window")
	}
	served := map[int64]string{}
	rows, err := l.root[newP].QueryContext(ctx, "SELECT id, served_by FROM app.writes WHERE id BETWEEN ? AND ?", ws[0].ID, ws[len(ws)-1].ID)
	if err != nil {
		return cv, err
	}
	for rows.Next() {
		var id int64
		var by string
		if rows.Scan(&id, &by) == nil {
			served[id] = by
		}
	}
	rows.Close()

	var lastOld, firstNew *write
	for i := range ws {
		wr := &ws[i]
		by, present := served[wr.ID]
		switch {
		case wr.Acked && !present:
			cv.LostAcked++
		case !wr.Acked && present:
			cv.PhantomRows++
		}
		if !wr.Acked {
			cv.NotAcked++
			continue
		}
		cv.Acked++
		if !present {
			continue
		}
		switch by {
		case oldP:
			if firstNew != nil {
				cv.Interleaved++
			}
			lastOld = wr
		case newP:
			if firstNew == nil && wr.Done.After(t0) {
				firstNew = wr
			}
		default:
			cv.ServedByOther++
		}
	}
	if firstNew != nil {
		r := ms(firstNew.Done.Sub(t0))
		cv.RecoveryMS = &r
		if lastOld != nil {
			g := ms(firstNew.Done.Sub(lastOld.Done))
			cv.GapMS = &g
		}
	}
	return cv, nil
}

// lastAcked returns the most recent acknowledged write.
func (w *writer) lastAcked() (write, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.writes) - 1; i >= 0; i-- {
		if w.writes[i].Acked {
			return w.writes[i], true
		}
	}
	return write{}, false
}

// acksBetween counts acknowledged writes that completed in (from, to].
func (w *writer) acksBetween(from, to time.Time) int {
	n := 0
	for _, wr := range w.since(from.Add(-2 * time.Second)) {
		if wr.Acked && wr.Done.After(from) && !wr.Done.After(to) {
			n++
		}
	}
	return n
}

// routerStatus returns the router's current routing.
func (l *lab) routerStatus(ctx context.Context) (primary string, pool []string, err error) {
	var s struct {
		Primary  string   `json:"primary"`
		ReadPool []string `json:"read_pool"`
	}
	b := &bench{http: &http.Client{Timeout: time.Second}}
	err = b.getJSON(ctx, l.router+"/status", &s)
	return s.Primary, s.ReadPool, err
}

func (l *lab) epoch(ctx context.Context) uint64 { return l.topology(ctx).Committed.Epoch }
