package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// The restore drill destroys the whole database tier and rebuilds it from
// the newest backup plus the binlog archive, timing each phase:
//
//  1. take a backup (scripts/backup.sh: Clone of a healthy replica), then
//     keep writing through the router so the restore has to roll forward
//  2. disaster: kill all three MySQL nodes at once, delete their data
//     volumes, and wipe Redis (topology, leases) and the platform containers
//  3. restore (timed from here): copy the backup into three fresh volumes in
//     parallel, start MySQL, replay the archived binlog on one node, start
//     the platform. The coordinator finds no topology, rebuilds it from the
//     epoch history inside the restored data, promotes the most advanced
//     node, and the others catch up by replication.
//  4. verify: every write acknowledged before the disaster is back (RPO),
//     the seeded payload's checksum is unchanged, and all three nodes hold
//     identical data.
//
// The backups and binlogs volumes are the only things that survive step 2.

type drillPhase struct {
	Name   string  `json:"name"`
	MS     float64 `json:"ms"`
	Detail string  `json:"detail,omitempty"`
}

type drillReport struct {
	Kind       string    `json:"kind"`
	Commit     string    `json:"commit"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`

	DatasetMiB float64        `json:"dataset_mib"` // InnoDB data + index of the user schemas
	Backup     map[string]any `json:"backup"`
	WriteFor   string         `json:"writes_after_backup_for"`

	Phases    []drillPhase `json:"restore_phases"`
	RestoreMS float64      `json:"restore_ms"` // start of restore -> 3 healthy nodes serving writes

	AckedBeforeDisaster int               `json:"acked_before_disaster"`
	AckedLost           int               `json:"acked_lost"` // RPO in writes
	LostWindowMS        float64           `json:"lost_window_ms,omitempty"`
	ReplayedGTIDs       string            `json:"replayed_gtids,omitempty"`
	PayloadBefore       string            `json:"payload_checksum_before"`
	PayloadAfter        map[string]string `json:"payload_checksum_after"` // per node
	PayloadMatch        bool              `json:"payload_match"`
	Consistent          bool              `json:"consistent"`
	Checksum            string            `json:"checksum,omitempty"`
	Error               string            `json:"error,omitempty"`
}

const restoreTarget = "mysql-1" // the node the archive is replayed on

func runDrill(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("drill", flag.ExitOnError)
	redis := fs.String("redis", defaultRedis, "comma-separated Redis instances")
	router := fs.String("router", "127.0.0.1:23306", "router write port")
	writeFor := fs.Duration("write-for", 20*time.Second, "keep writing this long after the backup, before the disaster")
	out := fs.String("out", "results", "directory for the JSON report")
	_ = fs.Parse(args)

	l, closeLab, err := openLab(ctx, *redis, *router, 10*time.Millisecond)
	if err != nil {
		return err
	}
	defer closeLab()
	rep := drillReport{Kind: "drill", Commit: gitCommit(), StartedAt: time.Now(), WriteFor: writeFor.String()}
	err = drill(ctx, l, &rep, *writeFor)
	if err != nil {
		rep.Error = err.Error()
	}
	rep.FinishedAt = time.Now()
	if mkErr := os.MkdirAll(*out, 0o755); mkErr != nil {
		return mkErr
	}
	path := filepath.Join(*out, "drill-"+rep.StartedAt.Format("20060102-150405")+".json")
	if wErr := writeJSON(path, rep); wErr != nil {
		return wErr
	}
	printDrill(rep)
	fmt.Printf("\nreport: %s\n", path)
	return err
}

func drill(ctx context.Context, l *lab, rep *drillReport, writeFor time.Duration) error {
	if err := l.waitSteady(ctx, 3*time.Second, 3*time.Minute); err != nil {
		return err
	}
	p := l.topology(ctx).Committed.Primary
	rep.DatasetMiB, _ = dataMiB(ctx, l.root[p], "app", "ha_meta")
	var err error
	if rep.PayloadBefore, err = rootChecksum(ctx, p, "app.payload"); err != nil {
		return fmt.Errorf("checksum before: %w", err)
	}

	fmt.Println("taking a backup")
	outb, err := exec.CommandContext(ctx, "scripts/backup.sh").CombinedOutput()
	if err != nil {
		return fmt.Errorf("backup: %w: %s", err, strings.TrimSpace(string(outb)))
	}
	lines := strings.Split(strings.TrimSpace(string(outb)), "\n")
	backup := strings.TrimSpace(lines[len(lines)-1])
	if meta, err := exec.CommandContext(ctx, "docker", "exec", "hafo-"+p, "cat", "/backups/"+backup+".json").Output(); err == nil {
		_ = json.Unmarshal(meta, &rep.Backup)
	}
	fmt.Printf("backup %s; writing for %s more\n", backup, writeFor)
	time.Sleep(writeFor)

	// Disaster.
	fmt.Println("disaster: killing all MySQL nodes, deleting their data, wiping Redis and the platform")
	if err := docker(ctx, "kill", "hafo-mysql-1", "hafo-mysql-2", "hafo-mysql-3"); err != nil {
		return err
	}
	tDisaster := time.Now()
	acked := l.w.ackedIDs()
	rep.AckedBeforeDisaster = len(acked)
	l.w.pause()
	defer l.w.resume()
	platform := []string{"agent-1", "agent-2", "agent-3", "coordinator-1", "coordinator-2", "coordinator-3", "router", "archiver"}
	if err := compose(ctx, append([]string{"stop", "-t", "1"}, platform...)...); err != nil {
		return err
	}
	if err := compose(ctx, "rm", "-f", "-s", "mysql-1", "mysql-2", "mysql-3", "redis-1", "redis-2", "redis-3"); err != nil {
		return err
	}
	if err := docker(ctx, "volume", "rm", "hafo_mysql-1-data", "hafo_mysql-2-data", "hafo_mysql-3-data"); err != nil {
		return err
	}

	// Restore, timed.
	r0 := time.Now()
	step := func(name string, f func() (string, error)) error {
		start := time.Now()
		detail, err := f()
		ph := drillPhase{Name: name, MS: ms(time.Since(start)), Detail: detail}
		rep.Phases = append(rep.Phases, ph)
		fmt.Printf("  %-46s %7.0fms  %s\n", name, ph.MS, detail)
		return err
	}
	if err := step("copy the backup into 3 fresh volumes (parallel)", func() (string, error) {
		return "", parallel(l.nodes, func(n string) error { return restoreVolume(ctx, n, backup) })
	}); err != nil {
		return err
	}
	if err := step("start MySQL on the restored data", func() (string, error) {
		return "", compose(ctx, "up", "-d", "--no-deps", "--wait", "mysql-1", "mysql-2", "mysql-3")
	}); err != nil {
		return err
	}
	var restored string
	if err := step("clear replication state copied with the backup", func() (string, error) {
		for _, n := range l.nodes {
			ro := "SET GLOBAL super_read_only = ON"
			if n == restoreTarget {
				ro = "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = ON" // root may replay; clients may not
			}
			if err := rootSQL(ctx, n, "STOP REPLICA; RESET REPLICA ALL; "+ro); err != nil {
				return "", fmt.Errorf("%s: %w", n, err)
			}
		}
		var err error
		restored, err = rootQuery(ctx, restoreTarget, "SELECT REPLACE(@@GLOBAL.gtid_executed, '\\n', '')")
		return "backup holds " + restored, err
	}); err != nil {
		return err
	}
	if err := step("replay the binlog archive on "+restoreTarget, func() (string, error) {
		// Nobody is served from this node yet, and a crash here just means
		// restarting the restore: skip the per-commit fsyncs while replaying.
		if err := rootSQL(ctx, restoreTarget, "SET GLOBAL sync_binlog = 0; SET GLOBAL innodb_flush_log_at_trx_commit = 2"); err != nil {
			return "", err
		}
		files, err := replayArchive(ctx, restored)
		if err != nil {
			return files, err
		}
		if err := rootSQL(ctx, restoreTarget, "SET GLOBAL sync_binlog = 1; SET GLOBAL innodb_flush_log_at_trx_commit = 1"); err != nil {
			return files, err
		}
		after, err := rootQuery(ctx, restoreTarget, "SELECT REPLACE(@@GLOBAL.gtid_executed, '\\n', '')")
		if err != nil {
			return files, err
		}
		rep.ReplayedGTIDs, _ = rootQuery(ctx, restoreTarget, fmt.Sprintf("SELECT GTID_SUBTRACT('%s', '%s')", after, restored))
		return files + "; replayed " + rep.ReplayedGTIDs, rootSQL(ctx, restoreTarget, "SET GLOBAL super_read_only = ON")
	}); err != nil {
		return err
	}
	if err := step("start Redis, agents, coordinators, router", func() (string, error) {
		return "", compose(ctx, "up", "-d", "--wait")
	}); err != nil {
		return err
	}
	if err := step("coordinator rebuilds the topology and promotes", func() (string, error) {
		var p string
		ok, _ := until(ctx, 90*time.Second, 100*time.Millisecond, func() bool {
			v := l.topology(ctx)
			p = v.Committed.Primary
			return v.OK && v.Committed.Serving()
		})
		if !ok {
			return "", fmt.Errorf("no serving primary within 90s")
		}
		return fmt.Sprintf("primary %s, epoch %d", p, l.epoch(ctx)), nil
	}); err != nil {
		return err
	}
	l.w.resume()
	resumed := time.Now()
	if err := step("replicas catch up; writes acknowledged", func() (string, error) {
		ok, _ := until(ctx, 2*time.Minute, 100*time.Millisecond, func() bool {
			return l.allHealthy(ctx) && l.w.lastAck().After(resumed)
		})
		if !ok {
			return "", fmt.Errorf("cluster not healthy with writes flowing within 2m")
		}
		return "3/3 nodes healthy", nil
	}); err != nil {
		return err
	}
	rep.RestoreMS = ms(time.Since(r0))
	l.w.pause() // the deferred resume restarts it; checks below need a still table

	// Verify.
	p = l.topology(ctx).Committed.Primary
	present := map[int64]bool{}
	if err := retryConn(ctx, func() error {
		ids, err := queryIDs(ctx, l.root[p], "SELECT id FROM app.writes WHERE id > ?", l.w.base)
		for _, id := range ids {
			present[id] = true
		}
		return err
	}); err != nil {
		return err
	}
	var lastLost time.Time
	for _, wr := range l.w.since(time.Time{}) {
		if acked[wr.ID] && !present[wr.ID] {
			rep.AckedLost++
			if wr.Done.After(lastLost) {
				lastLost = wr.Done
			}
		}
	}
	if rep.AckedLost > 0 {
		var firstLost time.Time
		for _, wr := range l.w.since(time.Time{}) {
			if acked[wr.ID] && !present[wr.ID] && (firstLost.IsZero() || wr.Done.Before(firstLost)) {
				firstLost = wr.Done
			}
		}
		rep.LostWindowMS = ms(tDisaster.Sub(firstLost))
	}
	rep.PayloadAfter = map[string]string{}
	rep.PayloadMatch = true
	for _, n := range l.nodes {
		sum, err := rootChecksum(ctx, n, "app.payload")
		if err != nil {
			return fmt.Errorf("checksum after on %s: %w", n, err)
		}
		rep.PayloadAfter[n] = sum
		rep.PayloadMatch = rep.PayloadMatch && sum == rep.PayloadBefore
	}
	l.w.resume()
	rep.Consistent, rep.Checksum = l.consistent(ctx)
	l.w.pause()
	return nil
}

// restoreVolume creates node's data volume (labelled as compose would) and
// fills it with the backup. auto.cnf is dropped so each node generates its
// own server_uuid instead of all three inheriting the backup source's.
func restoreVolume(ctx context.Context, node, backup string) error {
	vol := "hafo_" + node + "-data"
	if err := docker(ctx, "volume", "create", "--label", "com.docker.compose.project=hafo",
		"--label", "com.docker.compose.volume="+node+"-data", vol); err != nil {
		return err
	}
	return docker(ctx, "run", "--rm", "-v", vol+":/var/lib/mysql", "-v", "hafo_backups:/backups:ro",
		"--entrypoint", "sh", "hafo-tools:local", "-c",
		"cp -a /backups/"+backup+"/. /var/lib/mysql/ && rm -f /var/lib/mysql/auto.cnf && chown -R 999:999 /var/lib/mysql")
}

type archivedFile struct {
	start string // when the archiver first streamed this file's source
	path  string
	prev  string // Previous-GTIDs: everything executed before this file
}

// replayArchive replays the binlog archive on the restore target, oldest
// source first, skipping whole files the backup already covers.
//
// Every binlog file starts with a Previous-GTIDs event: the GTID set
// executed before it. If the next file's Previous-GTIDs is contained in the
// backup, every transaction in this file is too, so it is not read at all.
// Without this, replay time grew with the size of the archive (every past
// source, the whole seed) rather than with what was written since the
// backup. Within the files that are read, --exclude-gtids skips what the
// backup holds without decoding it, and the server auto-skips anything else
// already applied. The newest file of a source may end mid-event; mysqlbinlog
// stops there with an error that is expected, and the verification decides.
func replayArchive(ctx context.Context, backup string) (string, error) {
	files, err := listArchive(ctx)
	if err != nil {
		return "", err
	}
	keep, err := neededFiles(ctx, files, backup)
	if err != nil {
		return "", err
	}
	detail := fmt.Sprintf("read %d of %d archived files (%d covered by the backup)", len(keep), len(files), len(files)-len(keep))
	if len(keep) == 0 {
		return detail, nil
	}
	script := `set -o pipefail
mysqlbinlog --force-if-open --exclude-gtids="$1" "${@:3}" 2>/tmp/mysqlbinlog.err \
  | MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h "$2" -uroot
rc=$?
grep -v -E 'Warning|^$' /tmp/mysqlbinlog.err | tail -2 >&2
exit $rc`
	args := append([]string{"compose", "run", "--rm", "-T", "tools", "bash", "-c", script, "_", backup, restoreTarget}, keep...)
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		detail += " (mysqlbinlog: " + err.Error() + " " + strings.TrimSpace(string(out)) + ")"
	}
	return detail, nil
}

// listArchive returns every archived binlog file with its Previous-GTIDs,
// ordered by when its source started being archived, then by name.
func listArchive(ctx context.Context) ([]archivedFile, error) {
	script := `for d in /archive/*/; do
  [ -f "$d/source.json" ] || continue
  t=$(grep -o '"first_streamed_at": *"[^"]*"' "$d/source.json" | cut -d'"' -f4)
  for f in "$d"binlog.[0-9]*; do
    [ -e "$f" ] || continue
    g=$(mysqlbinlog --stop-position=4096 "$f" 2>/dev/null | awk '/Previous-GTIDs/{p=1; next} p && /^# at/{exit} p {sub(/^# /, ""); if ($0 != "[empty]") printf "%s", $0}')
    printf '%s\t%s\t%s\n' "$t" "$f" "$g"
  done
done`
	out, err := exec.CommandContext(ctx, "docker", "compose", "run", "--rm", "-T", "tools", "bash", "-c", script).Output()
	if err != nil {
		return nil, fmt.Errorf("list archive: %w", err)
	}
	var files []archivedFile
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.SplitN(line, "\t", 3)
		if len(f) == 3 {
			files = append(files, archivedFile{start: f[0], path: f[1], prev: f[2]})
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].start != files[j].start {
			return files[i].start < files[j].start
		}
		return files[i].path < files[j].path
	})
	return files, nil
}

// neededFiles drops each file whose successor (same source) starts from a
// GTID set the backup already contains. One query answers every subset test.
func neededFiles(ctx context.Context, files []archivedFile, backup string) ([]string, error) {
	var tests []int
	var exprs []string
	for i := range files {
		if i+1 < len(files) && filepath.Dir(files[i+1].path) == filepath.Dir(files[i].path) {
			tests = append(tests, i)
			exprs = append(exprs, fmt.Sprintf("GTID_SUBSET('%s', '%s')", files[i+1].prev, backup))
		}
	}
	covered := map[int]bool{}
	if len(exprs) > 0 {
		out, err := rootQuery(ctx, restoreTarget, "SELECT "+strings.Join(exprs, ", "))
		if err != nil {
			return nil, fmt.Errorf("gtid subset: %w", err)
		}
		for k, v := range strings.Fields(out) {
			if k < len(tests) && v == "1" {
				covered[tests[k]] = true
			}
		}
	}
	var keep []string
	for i, f := range files {
		if !covered[i] {
			keep = append(keep, f.path)
		}
	}
	return keep, nil
}

// rootSQL runs statements as root inside the node's container (no host
// port needed, and no pooled connection that the restore would have broken).
func rootSQL(ctx context.Context, node, stmts string) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", "hafo-"+node, "sh", "-c",
		`MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --skip-column-names`)
	cmd.Stdin = strings.NewReader(stmts)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func rootQuery(ctx context.Context, node, query string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", "hafo-"+node, "sh", "-c",
		`MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --skip-column-names`)
	cmd.Stdin = strings.NewReader(query)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

// rootChecksum runs CHECKSUM TABLE through the mysql client inside the
// node's container: on a large table it outlasts the harness connections'
// read timeout.
func rootChecksum(ctx context.Context, node, table string) (string, error) {
	out, err := rootQuery(ctx, node, "CHECKSUM TABLE "+table)
	if err != nil {
		return "", err
	}
	f := strings.Fields(out)
	if len(f) != 2 || f[1] == "NULL" {
		return "", fmt.Errorf("unexpected CHECKSUM TABLE output %q", out)
	}
	return f[1], nil
}

func compose(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, lastLines(string(out), 3))
	}
	return nil
}

func parallel(items []string, f func(string) error) error {
	var wg sync.WaitGroup
	errs := make([]error, len(items))
	for i, it := range items {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = f(it)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], " | ")
}

func printDrill(r drillReport) {
	num := func(k string) float64 { f, _ := r.Backup[k].(float64); return f }
	fmt.Printf("\ndataset %.0f MiB; backup %v (%.0f MiB, %.0f ms); writes continued %s after it\n",
		r.DatasetMiB, r.Backup["backup"], num("bytes")/1048576, num("took_ms"), r.WriteFor)
	fmt.Printf("restore: %.1fs from an empty database tier to 3 healthy nodes serving writes\n", r.RestoreMS/1000)
	fmt.Printf("RPO: %d of %d acknowledged writes lost", r.AckedLost, r.AckedBeforeDisaster)
	if r.AckedLost > 0 {
		fmt.Printf(" (the last %.0fms before the disaster)", r.LostWindowMS)
	}
	fmt.Printf("\napp.payload checksum before %s, after %v (all match: %v); app.writes consistent %v (%s)\n",
		r.PayloadBefore, r.PayloadAfter, r.PayloadMatch, r.Consistent, r.Checksum)
	if r.Error != "" {
		fmt.Println("ERROR", r.Error)
	}
}
