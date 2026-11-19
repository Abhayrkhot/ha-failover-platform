// Package archiver streams the binary log of a healthy replica into an
// archive directory (one subdirectory per source server, named by its
// server_uuid) and follows the cluster through failovers. A restore replays
// the archive on top of a backup (point-in-time recovery).
//
// It archives a replica, never the primary. With semi-sync AFTER_SYNC a
// commit reaches the primary's binlog before any replica acknowledges it,
// so a primary that dies or is fenced at that moment holds a transaction
// that was never acknowledged and that the cluster then discards (an errant
// GTID). Archiving it would let a restore bring back data the cluster threw
// away. A replica's binlog (log_replica_updates) holds only what it applied.
// The cost is RPO: an acknowledged commit sitting in a replica's relay log,
// not yet applied, is not archived yet.
package archiver

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/mysqlx"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// Config is the archiver's tuning. Defaults live in cmd/hafo-archiver.
type Config struct {
	Nodes         []string
	RedisAddrs    []string
	Dir           string
	ReplUser      string // mysqlbinlog streams as the replication user
	ReplPassword  string
	MySQLUser     string // reads server_uuid and the binlog list
	MySQLPassword string
	ServerID      int // server_id of the dump connection; must not clash with a node's
	Poll          time.Duration
}

// Source describes one archive subdirectory (source.json).
type Source struct {
	Node            string    `json:"node"`
	ServerUUID      string    `json:"server_uuid"`
	FirstStreamedAt time.Time `json:"first_streamed_at"`
}

// Archiver supervises one mysqlbinlog process.
type Archiver struct {
	cfg   Config
	log   *slog.Logger
	store *store.Majority
	dbs   map[string]*sql.DB
	m     *metrics
	cnf   string // defaults file holding the replication password

	mu      sync.Mutex
	node    string
	uuid    string
	file    string
	since   time.Time
	cmd     *exec.Cmd
	done    chan struct{}
	lastErr string
}

// New prepares connections and the credentials file.
func New(cfg Config, log *slog.Logger, reg prometheus.Registerer) (*Archiver, error) {
	a := &Archiver{cfg: cfg, log: log, store: store.NewRedis(cfg.RedisAddrs, 200*time.Millisecond), dbs: map[string]*sql.DB{}}
	for _, n := range cfg.Nodes {
		db, err := mysqlx.Open(n+":3306", cfg.MySQLUser, cfg.MySQLPassword, time.Second, 2)
		if err != nil {
			return nil, err
		}
		a.dbs[n] = db
	}
	// A defaults file keeps the password off mysqlbinlog's command line.
	f, err := os.CreateTemp("", "hafo-archiver-*.cnf")
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(f, "[client]\nuser=%s\npassword=%s\n", cfg.ReplUser, cfg.ReplPassword); err != nil {
		return nil, err
	}
	_ = f.Close()
	a.cnf = f.Name()
	a.m = newMetrics(reg)
	return a, os.MkdirAll(cfg.Dir, 0o755)
}

// Run supervises until ctx is cancelled.
func (a *Archiver) Run(ctx context.Context) {
	t := time.NewTicker(a.cfg.Poll)
	defer t.Stop()
	for {
		a.tick(ctx)
		select {
		case <-ctx.Done():
			a.stop()
			return
		case <-t.C:
		}
	}
}

// Close releases connections.
func (a *Archiver) Close() {
	for _, db := range a.dbs {
		_ = db.Close()
	}
	_ = a.store.Close()
	_ = os.Remove(a.cnf)
}

func (a *Archiver) tick(ctx context.Context) {
	var healthy []string
	if v := cluster.ReadTopology(ctx, a.store); v.OK && v.Committed.Serving() {
		for _, n := range a.cfg.Nodes {
			rec, holders, ok := health.Latest(a.store.Get(ctx, health.Key(n)))
			if ok && holders >= a.store.Quorum() && rec.Healthy && rec.Role == health.RoleReplica &&
				rec.Observation.SourceHost == v.Committed.Primary {
				healthy = append(healthy, n)
			}
		}
	}
	a.mu.Lock()
	current, running := a.node, a.runningLocked()
	a.mu.Unlock()
	a.m.streaming.Set(b2f(running))
	a.m.bytes.Set(float64(dirSize(a.cfg.Dir)))

	want := Pick(current, healthy)
	switch {
	case want == "":
		if running {
			a.log.Warn("no healthy replica of the primary: archiving paused")
			a.stop()
		}
	case want != current || !running:
		if running {
			a.log.Info("switching archive source", "from", current, "to", want)
			a.stop()
		}
		if err := a.start(ctx, want); err != nil {
			a.log.Warn("could not start streaming", "node", want, "err", err)
			a.mu.Lock()
			a.lastErr = err.Error()
			a.mu.Unlock()
		}
	}
}

// Pick keeps the current source while it is still a healthy replica (so
// the archive does not hop between servers), else takes the first by name.
func Pick(current string, healthy []string) string {
	if current != "" && slices.Contains(healthy, current) {
		return current
	}
	if len(healthy) == 0 {
		return ""
	}
	s := slices.Clone(healthy)
	sort.Strings(s)
	return s[0]
}

func (a *Archiver) start(ctx context.Context, node string) error {
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	db := a.dbs[node]
	var uuid string
	if err := db.QueryRowContext(qctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return fmt.Errorf("server_uuid: %w", err)
	}
	first, err := firstBinlog(qctx, db)
	if err != nil {
		return err
	}
	dir := filepath.Join(a.cfg.Dir, uuid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	meta := filepath.Join(dir, "source.json")
	if _, err := os.Stat(meta); errors.Is(err, os.ErrNotExist) {
		b, _ := json.MarshalIndent(Source{Node: node, ServerUUID: uuid, FirstStreamedAt: time.Now().UTC()}, "", "  ")
		if err := os.WriteFile(meta, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	// Resume from the newest file already archived from this server: it may
	// be partial, and --raw rewrites it from the start.
	file := first
	if files := BinlogFiles(dir); len(files) > 0 {
		file = filepath.Base(files[len(files)-1])
	}

	cmd := exec.Command("mysqlbinlog", "--defaults-extra-file="+a.cnf,
		"--read-from-remote-server", "--host="+node, "--port=3306", "--get-server-public-key",
		"--raw", "--stop-never", "--connection-server-id="+strconv.Itoa(a.cfg.ServerID),
		"--result-file="+dir+"/", file)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			a.log.Warn("mysqlbinlog", "node", node, "msg", sc.Text())
		}
		err := cmd.Wait()
		a.log.Info("mysqlbinlog exited", "node", node, "err", err)
		close(done)
	}()

	a.mu.Lock()
	a.node, a.uuid, a.file, a.since, a.cmd, a.done, a.lastErr = node, uuid, file, time.Now(), cmd, done, ""
	a.mu.Unlock()
	a.m.starts.WithLabelValues(node).Inc()
	a.log.Info("streaming binlog", "node", node, "server_uuid", uuid, "from_file", file, "dir", dir)
	return nil
}

func (a *Archiver) stop() {
	a.mu.Lock()
	cmd, done := a.cmd, a.done
	a.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func (a *Archiver) runningLocked() bool {
	if a.done == nil {
		return false
	}
	select {
	case <-a.done:
		return false
	default:
		return true
	}
}

// StatusHandler serves what is being archived as JSON.
func (a *Archiver) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		a.mu.Lock()
		s := map[string]any{"source": a.node, "server_uuid": a.uuid, "from_file": a.file, "since": a.since,
			"streaming": a.runningLocked(), "last_error": a.lastErr, "archive_bytes": dirSize(a.cfg.Dir)}
		a.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	})
}

func firstBinlog(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return "", fmt.Errorf("show binary logs: %w", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	vals := make([]any, len(cols))
	for i := range vals {
		vals[i] = new(sql.RawBytes)
	}
	if !rows.Next() {
		return "", errors.New("no binary logs")
	}
	if err := rows.Scan(vals...); err != nil {
		return "", err
	}
	return string(*vals[0].(*sql.RawBytes)), nil
}

// BinlogFiles lists a source directory's archived binlog files in order.
func BinlogFiles(dir string) []string {
	entries, _ := os.ReadDir(dir)
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "binlog.") && !strings.HasSuffix(e.Name(), ".index") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

func dirSize(root string) int64 {
	var n int64
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

type metrics struct {
	streaming prometheus.Gauge
	bytes     prometheus.Gauge
	starts    *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		streaming: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_archiver_streaming", Help: "1 while a mysqlbinlog stream from a healthy replica is running.",
		}),
		bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hafo_archiver_archive_bytes", Help: "Bytes of binary log in the archive.",
		}),
		starts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hafo_archiver_stream_starts_total", Help: "Times a stream was started, by source node.",
		}, []string{"node"}),
	}
	reg.MustRegister(m.streaming, m.bytes, m.starts)
	return m
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
