package wadjet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A cancelled query reclaims its scratch, and the assertion is a BYTE COUNT.
//
// Nothing in the tree asserted anything about disk after a query — the wire
// harness asserts SQLSTATE 57014 and stops there — so the whole of #625 was
// invisible to every gate. Round-0 measured, at a 4 MiB budget with the spill
// floors lowered:
//
//	ORDER BY   control 0 B   cancelled 1,339,590 B in 3 files
//	GROUP BY   control 0 B   cancelled 4,843,311 B in 117 files
//	ROW_NUMBER control 0 B   cancelled 26,791,800 B in 60 files
//
// and the file count went UP after the cancel (117 vs 116, 60 vs 59): a spill
// file mid-write finishes and is then orphaned. On a long-running server those
// bytes come back at process restart and not before.
//
// Two mechanisms, and the Workers arm is what separates them:
//
//   - M1 `defer pipeline.Close()` sat BELOW the `Run` error check, so a
//     cancelled Run returned before the defer statement ever executed.
//   - M2 `Pipeline.Close` held no reference to the morsel-parallel clones.
//     M1 alone fixes sort and window and leaves the aggregate leaking
//     6,164,505 B in 163 files, because those files belong to CLONE sinks.
//
// So the aggregate family runs with several morsel workers deliberately. A
// serial-only version of this gate passes with M2 unfixed.
func TestACancelledQueryLeavesNoSpillScratch(t *testing.T) {
	if testing.Short() {
		t.Skip("spills to disk and cancels; -short skips it")
	}
	// Lower the sort/window/raw-row run floors: they are 64 MiB constants a
	// 1.2 MB fixture cannot cross at any budget (ADR-0027 decision 5).
	defer exec.ForceSmallSpillRuns(4096)()

	cases := []struct {
		name    string
		sql     string
		workers string // WADJET_SCAN_WORKERS for this arm
		drain   int64  // exec.ForceAggDrainEvery, 0 = leave it alone
		engaged func() int64
		what    string
	}{
		{
			name:    "sort",
			sql:     "SELECT pad, id FROM " + cancelScratchTable + " ORDER BY pad, id",
			workers: "1",
			engaged: func() int64 { return exec.SortRunsWritten.Load() },
			what:    "sorted run written to disk",
		},
		{
			name:    "hash_aggregate_morsel_parallel",
			sql:     "SELECT pad, COUNT(*) AS n FROM " + cancelScratchTable + " GROUP BY pad",
			workers: "4",
			drain:   4,
			engaged: func() int64 { return exec.AggregatePartialDrains.Load() },
			what:    "aggregate partial-state drain",
		},
		{
			name:    "window",
			sql:     "SELECT id, ROW_NUMBER() OVER (ORDER BY pad, id) AS rn FROM " + cancelScratchTable,
			workers: "1",
			engaged: func() int64 { return exec.WindowRunsWritten.Load() },
			what:    "window run written to disk",
		},
		{
			name:    "spilling_join",
			sql:     "SELECT COUNT(*) AS n FROM " + cancelScratchTable + " x JOIN " + cancelScratchTable + " z ON x.id = z.id",
			workers: "1",
			engaged: func() int64 { return exec.JoinPartitionsEvicted.Load() },
			what:    "join partition evicted to disk",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WADJET_SCAN_WORKERS", tc.workers)
			if tc.drain > 0 {
				prev := exec.ForceAggDrainEvery(tc.drain)
				defer exec.ForceAggDrainEvery(prev)
			}

			// Control arm: the same query, run to completion. It establishes
			// both that the shape SPILLS (so the cancel arm is not vacuous)
			// and that a normal exit already reclaims everything.
			dir, db := cancelScratchOpen(t)
			before := tc.engaged()
			if _, err := db.Query(context.Background(), tc.sql); err != nil {
				t.Fatalf("control run: %v\n  SQL: %s", err, tc.sql)
			}
			if d := tc.engaged() - before; d == 0 {
				t.Fatalf("no %s in the control run — this cell would prove nothing about cancellation\n  SQL: %s",
					tc.what, tc.sql)
			}
			if n, bytes := scratchUsage(dir); bytes != 0 {
				t.Fatalf("a query that RAN TO COMPLETION left %d files / %d bytes under %s", n, bytes, dir)
			}

			// Cancel arm: identical query, cancelled the moment the spill
			// directory first holds a file. Deterministic on the spill, not
			// on a wall clock — a timer-based cancel either fires before the
			// operator has written anything (proving nothing) or after it
			// has finished (also proving nothing).
			dir2, db2 := cancelScratchOpen(t)
			ctx, cancel := context.WithCancel(context.Background())
			fired := make(chan int64, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					if _, b := scratchUsage(dir2); b > 0 {
						fired <- b
						cancel()
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Millisecond):
					}
				}
				fired <- 0
				cancel()
			}()

			_, qerr := db2.Query(ctx, tc.sql)
			<-done
			cancel()
			peak := <-fired
			if peak == 0 {
				t.Skipf("%s never wrote a spill file within the window; nothing to cancel over", tc.name)
			}
			if qerr == nil {
				t.Fatalf("the cancelled query returned a result; a cancel must be reported as an error")
			}

			// The assertion. AFTER Query returns — not during — the query's
			// scratch is gone. A file mid-write at cancel time finishes and
			// must still be removed by the operator's Close.
			if n, bytes := scratchUsage(dir2); bytes != 0 {
				t.Fatalf("a CANCELLED query left %d files / %d bytes under %s (peak during the run: %d bytes)%s\n"+
					"  every byte a query writes to scratch is reclaimed on every exit path (ADR-0028)\n"+
					"  SQL: %s", n, bytes, dir2, peak, scratchListing(dir2), tc.sql)
			}
		})
	}
}

// cancelScratchOpen opens a DB whose spill directory is a fresh temp dir,
// budgeted small enough that the fixture spills.
func cancelScratchOpen(t *testing.T) (string, *DB) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, Config{
		Store:        objstore.NewMemStore(),
		Bucket:       "test",
		MemoryBudget: 4 << 20,
		SpillDir:     dir,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, cancelScratchTable, schema, nil); err != nil {
		t.Fatalf("create %s: %v", cancelScratchTable, err)
	}
	rows := make([]map[string]any, cancelScratchRows)
	for i := range rows {
		rows[i] = map[string]any{
			"id":  int64(i),
			"pad": fmt.Sprintf("%06d-%s", i%cancelScratchGroups, strings.Repeat("x", 250)),
		}
	}
	ing := db.NewIngester(cancelScratchTable, schema, nil, ingest.Config{
		MaxBufferRows: cancelScratchRows + 1, RowGroupSize: 2048,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return dir, db
}

const (
	// A fixture sized to spill every family at a 4 MiB budget: 20 000 rows
	// of a 257-byte pad is ~5 MB of live column data, and the group count
	// keeps the aggregate's state from collapsing to a handful of slots.
	cancelScratchTable  = "leaky"
	cancelScratchRows   = 20000
	cancelScratchGroups = 4000
)

// scratchUsage counts the files and bytes under dir. Directories themselves
// are not counted — an empty wadjet-spill/ dir is not a leak, its contents
// are.
func scratchUsage(dir string) (int, int64) {
	var files int
	var bytes int64
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		files++
		bytes += info.Size()
		return nil
	})
	return files, bytes
}

// scratchListing is a debugging aid for a failure: names what survived.
func scratchListing(dir string) string {
	var b strings.Builder
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		fmt.Fprintf(&b, "\n    %s (%d B)", rel, info.Size())
		return nil
	})
	return b.String()
}
