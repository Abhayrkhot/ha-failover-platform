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
	base := time.Now().UnixMilli() * 1000
	l.w = &writer{db: app, every: writeEvery, base: base, next: base}
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
	base  int64 // IDs this run writes are all above base
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

// convergeResult is how a node came back after a failover.
type convergeResult struct {
	Outcome string `json:"outcome"` // "rejoined" or "rebuilt (errant)"
	// Only for a node that held transactions the primary lacked, captured
	// before the rebuild throws them away:
	ErrantGTIDs   string `json:"errant_gtids,omitempty"`
	DivergedRows  int    `json:"diverged_rows"`  // app.writes rows only this node had
	DivergedAcked int    `json:"diverged_acked"` // of those, acknowledged to the client: must be 0
}

// converge waits for node to be a healthy replica of primary. A node that
// holds errant GTIDs is first examined (divergence) and then rebuilt.
func (l *lab) converge(ctx context.Context, node, primary string, timeout time.Duration) (convergeResult, error) {
	res := convergeResult{Outcome: "rejoined"}
	ok, _ := until(ctx, timeout, 500*time.Millisecond, func() bool {
		rec, found := l.record(ctx, node)
		if found && rec.Healthy && rec.Role == health.RoleReplica && rec.Observation.SourceHost == primary {
			return true
		}
		if res.Outcome == "rejoined" {
			if errant, err := l.errant(ctx, node, primary); err == nil && errant {
				if d, err := l.divergence(ctx, node, primary); err == nil {
					res = d
				}
				res.Outcome = "rebuilt (errant)"
				if out, err := exec.CommandContext(ctx, "scripts/rebuild.sh", node).CombinedOutput(); err != nil {
					res.Outcome = "rebuild failed: " + strings.TrimSpace(string(out))
				}
			}
		}
		return false
	})
	if !ok {
		return res, fmt.Errorf("%s did not rejoin within %s", node, timeout)
	}
	return res, nil
}

// divergence describes what node has that primary lacks: the errant GTID
// set, the app.writes rows (from this run's writer) only node holds, and how
// many of those the client was told had committed. A fenced old primary may
// legitimately hold commits that reached its binlog but no replica; the
// invariant is that none of them was ever acknowledged.
func (l *lab) divergence(ctx context.Context, node, primary string) (convergeResult, error) {
	var res convergeResult
	qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var gtid string
	if err := l.root[node].QueryRowContext(qctx, `SELECT REPLACE(@@gtid_executed, '\n', '')`).Scan(&gtid); err != nil {
		return res, err
	}
	if err := l.root[primary].QueryRowContext(qctx, `SELECT GTID_SUBTRACT(?, REPLACE(@@gtid_executed, '\n', ''))`, gtid).Scan(&res.ErrantGTIDs); err != nil {
		return res, err
	}
	ids, err := queryIDs(qctx, l.root[node], "SELECT id FROM app.writes WHERE id > ?", l.w.base)
	if err != nil {
		return res, err
	}
	present := map[int64]bool{}
	for i := 0; i < len(ids); i += 500 {
		chunk := ids[i:min(i+500, len(ids))]
		args := make([]any, len(chunk))
		for j, id := range chunk {
			args[j] = id
		}
		got, err := queryIDs(qctx, l.root[primary],
			"SELECT id FROM app.writes WHERE id IN ("+strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")+")", args...)
		if err != nil {
			return res, err
		}
		for _, id := range got {
			present[id] = true
		}
	}
	acked := l.w.ackedIDs()
	for _, id := range ids {
		if !present[id] {
			res.DivergedRows++
			if acked[id] {
				res.DivergedAcked++
			}
		}
	}
	return res, nil
}

func queryIDs(ctx context.Context, db *sql.DB, query string, args ...any) ([]int64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
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
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var gtid string
	if err := retryConn(qctx, func() error {
		return l.root[p].QueryRowContext(qctx, `SELECT REPLACE(@@gtid_executed, '\n', '')`).Scan(&gtid)
	}); err != nil {
		return false, "read primary gtid: " + err.Error()
	}
	sums := map[string]int64{}
	for _, n := range l.nodes {
		if n != p {
			var timedOut int
			err := retryConn(qctx, func() error {
				return l.root[n].QueryRowContext(qctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 15)", gtid).Scan(&timedOut)
			})
			if err != nil || timedOut != 0 {
				return false, fmt.Sprintf("%s did not catch up to the primary (%v)", n, err)
			}
		}
		var table string
		var sum int64
		if err := retryConn(qctx, func() error {
			return l.root[n].QueryRowContext(qctx, "CHECKSUM TABLE app.writes").Scan(&table, &sum)
		}); err != nil {
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

// retryConn retries f when it fails on the connection rather than in MySQL:
// a pooled connection to a node that restarted (e.g. after a rebuild) fails
// once with "invalid connection" before the pool replaces it.
func retryConn(ctx context.Context, f func() error) error {
	var err error
	for range 3 {
		if err = f(); err == nil || mysqlx.Number(err) != 0 || ctx.Err() != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
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

// ackedIDs returns the set of acknowledged write IDs.
func (w *writer) ackedIDs() map[int64]bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[int64]bool, len(w.writes))
	for _, wr := range w.writes {
		if wr.Acked {
			out[wr.ID] = true
		}
	}
	return out
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
