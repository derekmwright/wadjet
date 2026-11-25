// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Cleanup after cancellation and failure on a SHARED pool (1685e): a statement
// that ends early must give back everything it charged, or every later
// statement of the DB inherits its bytes. The survivor S is held at its gate
// in its probe phase with its hash-join build retained; S's balance alone is
// snapshotted; the victim V then ends early at one lifecycle point; the pool
// and EVERY forced-purpose counter must be back at S's balance; after S ends,
// the pool is quiescent.

// poolBalance is a synchronized reading of the pool (no statement reserving).
type poolBalance struct {
	used      int64
	forced    [memory.NumForcePurposes]int64
	live      int
	peaks     int
	cooldowns int
	owners    int
}

func readPoolBalance(db *DB) poolBalance {
	tr := db.pool.Tracker()
	var b poolBalance
	b.used = tr.Used()
	for p := memory.ForcePurpose(0); p < memory.NumForcePurposes; p++ {
		b.forced[p] = tr.ForcedFor(p)
	}
	c := db.pool.SpillManager().Census()
	b.live, b.peaks, b.cooldowns = c.Live, c.Peaks, c.Cooldowns
	b.owners = len(tr.OwnedSnapshot())
	return b
}

// openSpillFiles lists the files beneath root that this process still holds
// open, by reading /proc/self/fd. A statement scope's Cleanup removes its
// directory whole, so a spill file an operator left open disappears from the
// directory listing all the same; only its open descriptor shows that the
// operator never closed it (a hash join's grace writers before 6cb01933).
// nil where /proc is not available.
func openSpillFiles(t *testing.T, root string) []string {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err == nil && strings.HasPrefix(target, root+string(filepath.Separator)) {
			out = append(out, target)
		}
	}
	sort.Strings(out)
	return out
}

// poolCleanupDB is wideJoinDB's table plus a small table `vt` the victim's
// scan-failure cell reads, over a store whose Get of one key can be made to
// fail (getFailStore, scan_load_error_test.go).
func poolCleanupDB(t *testing.T, budget int64) (*DB, *getFailStore) {
	t.Helper()
	ctx := context.Background()
	store := &getFailStore{Store: objstore.NewMemStore()}
	db, err := Open(ctx, Config{Store: store, Bucket: "test", MemoryBudget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	for _, tbl := range []string{"wide", "vt"} {
		if err := db.CreateTable(ctx, tbl, schema, nil); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, 4000)
		for i := range rows {
			rows[i] = map[string]any{"k": int64(i), "pad": fmt.Sprintf("%08d", i) + strings.Repeat("x", 504)}
		}
		ing := db.NewIngester(tbl, schema, nil, ingest.Config{MaxBufferRows: 4001, RowGroupSize: 512})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db, store
}

// TestPoolCancelOrFailOneStatementLeavesTheSurvivorsBalance is the cleanup
// matrix. Cells: V cancelled during its hash-join build, in its probe, in
// its spilled-partition replay, while its aggregate consumes; V failed by an
// object-store error on its scan. In every cell V must end with its
// cancellation or its named error, S must then answer PostgreSQL's answer,
// and the balances must hold.
func TestPoolCancelOrFailOneStatementLeavesTheSurvivorsBalance(t *testing.T) {
	const budget = 614268 // one join's build fits (peak 409512 alone), two do not
	cells := []string{"build", "probe", "replay", "aggregate-consume", "scan-load-error"}
	for _, cell := range cells {
		t.Run(cell, func(t *testing.T) {
			db, store := poolCleanupDB(t, budget)
			ctx := context.Background()
			gates := armPoolGates(2) // 0: survivor, 1: victim (probe / aggregate cells)

			sDone := make(chan struct{})
			var sAns string
			var sErr error
			go func() {
				defer close(sDone)
				res, err := db.Query(ctx, poolJoinSQL(0))
				if err != nil {
					sErr = err
					return
				}
				sAns = fmt.Sprint(res.Rows)
			}()
			select {
			case <-gates[0].reached:
			case <-sDone:
				t.Fatalf("the survivor ended before its gate: %v", sErr)
			case <-time.After(poolGateDeadline):
				t.Fatal("the survivor never reached its gate")
			}
			alone := readPoolBalance(db)
			root := db.pool.SpillManager().SpillDir()
			aloneFiles := spillRootEntries(t, root)
			aloneOpen := openSpillFiles(t, root)

			vctx, cancel := context.WithCancel(ctx)
			defer cancel()
			var fired atomic.Bool
			fire := func() {
				if fired.CompareAndSwap(false, true) {
					cancel()
				}
			}
			vSQL := poolJoinSQL(1)
			switch cell {
			case "build":
				// S's build is complete and S sits in its probe: every grace
				// arrival from here on is V's.
				defer exec.SetArrivalReserveHook(fire)()
			case "probe":
				go func() { <-gates[1].reached; fire(); gates[1].open() }()
			case "replay":
				// V's build spills against S's retained build, so V replays;
				// S built in memory and never does. V's gate (in its probe of
				// the in-memory partitions, before the replay) stays open.
				gates[1].open()
				defer exec.SetSpillReplayHook(fire)()
			case "aggregate-consume":
				vSQL = "SELECT k % 97 AS g, sum(pool_gate_1(k)) AS s FROM wide GROUP BY 1"
				go func() { <-gates[1].reached; fire(); gates[1].open() }()
			case "scan-load-error":
				m, err := db.Catalog().GetManifest(ctx, "vt")
				if err != nil {
					t.Fatal(err)
				}
				store.mu.Lock()
				store.target = m.Partitions[0].Files[0].Path
				store.mu.Unlock()
				vSQL = "SELECT count(*), sum(k) FROM vt"
			}
			_, vErr := db.Query(vctx, vSQL)
			gates[1].open()
			switch {
			case cell == "scan-load-error":
				if vErr == nil || !strings.Contains(vErr.Error(), "injected object-store failure") {
					t.Fatalf("victim: %v, want the injected store failure", vErr)
				}
			case !fired.Load():
				t.Fatalf("the victim never reached its %s point (err %v)", cell, vErr)
			case vErr == nil || !errors.Is(vErr, context.Canceled) && !strings.Contains(vErr.Error(), "context canceled"):
				t.Fatalf("victim cancelled at %s ended with %v", cell, vErr)
			}

			after := readPoolBalance(db)
			if after != alone {
				t.Errorf("after the victim ended at %s the pool reads %+v; the survivor alone held %+v", cell, after, alone)
			}
			// The victim's whole teardown, on every exit path (6cb01933 made the
			// hash join drop its files and state on each): no statement
			// directory or file of its own under the spill root, and no spill
			// file still held open — the second catches an operator that left
			// a writer open, which the scope's directory removal would hide.
			if files := spillRootEntries(t, root); !slices.Equal(files, aloneFiles) {
				t.Errorf("after the victim ended at %s the spill root holds %v; the survivor alone held %v", cell, files, aloneFiles)
			}
			if open := openSpillFiles(t, root); !slices.Equal(open, aloneOpen) {
				t.Errorf("after the victim ended at %s these spill files are still open: %v (the survivor alone held %v)", cell, open, aloneOpen)
			}

			gates[0].open()
			<-sDone
			if sErr != nil || sAns != poolJoinWant {
				t.Errorf("survivor: %s %v, PostgreSQL 17.11 %s", sAns, sErr, poolJoinWant)
			}
			assertDBPoolQuiescent(t, db)
			if open := openSpillFiles(t, root); len(open) != 0 {
				t.Errorf("after both statements ended these spill files are still open: %v", open)
			}
		})
	}
}
