package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/mysqlx"
)

// seed loads app.payload with random, incompressible rows through the
// router, so backup and restore timings refer to a stated amount of data.
func runSeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	mib := fs.Int("mib", 1024, "MiB of row data to add")
	rowBytes := fs.Int("row-bytes", 1024, "bytes of filler per row")
	batch := fs.Int("batch", 1000, "rows per INSERT")
	workers := fs.Int("workers", 4, "parallel writers")
	router := fs.String("router", "127.0.0.1:23306", "router write port")
	_ = fs.Parse(args)

	db, err := mysqlx.Open(*router, "root", os.Getenv("MYSQL_ROOT_PASSWORD"), 2*time.Second, *workers+1)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS app.payload (
		id     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		filler VARCHAR(4096) NOT NULL)`); err != nil {
		return err
	}

	rows := int64(*mib) * 1024 * 1024 / int64(*rowBytes)
	stmts := (rows + int64(*batch) - 1) / int64(*batch)
	var next, done atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, *workers)
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := seedWorker(ctx, db, &next, &done, stmts, *batch, *rowBytes); err != nil {
				errs <- err
			}
		}()
	}
	go func() {
		for t := time.NewTicker(5 * time.Second); ; <-t.C {
			if n := done.Load(); n >= stmts || ctx.Err() != nil {
				return
			} else {
				fmt.Printf("  %d/%d MiB in %s\n", n*int64(*batch)*int64(*rowBytes)>>20, *mib, time.Since(start).Round(time.Second))
			}
		}
	}()
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	size, _ := dataMiB(ctx, db, "app")
	fmt.Printf("seeded %d rows (%d MiB of filler) in %s; schema app is now %.0f MiB on disk\n",
		rows, *mib, time.Since(start).Round(time.Millisecond), size)
	return nil
}

func seedWorker(ctx context.Context, db *sql.DB, next, done *atomic.Int64, stmts int64, batch, rowBytes int) error {
	raw := make([]byte, rowBytes*3/4+3)
	for {
		if next.Add(1) > stmts {
			return nil
		}
		var b strings.Builder
		b.WriteString("INSERT INTO app.payload (filler) VALUES ")
		for i := range batch {
			if _, err := rand.Read(raw); err != nil {
				return err
			}
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString("('")
			b.WriteString(base64.RawStdEncoding.EncodeToString(raw)[:rowBytes])
			b.WriteString("')")
		}
		if _, err := db.ExecContext(ctx, b.String()); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		done.Add(1)
	}
}
