// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// censusFixture is a table of one valid many-row-group file and, when
// withCorrupt is set, a second file whose first page header is overwritten,
// so a scan of the table decodes the valid file's row groups and FAILS on the
// corrupt one.
func censusFixture(t *testing.T, withCorrupt bool) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}}
	sets := make([][]map[string]any, 16)
	for i := range sets {
		sets[i] = testRows(64, i*64)
	}
	good := writeTestParquetMultiRG(t, schema, sets...)

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	put := func(path string, data []byte, rows int64) catalog.FileEntry {
		if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		return catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: rows}
	}
	entries := []catalog.FileEntry{put("tables/items/chunk_001.parquet", good, 16*64)}
	if withCorrupt {
		bad := writeTestParquetMultiRG(t, schema, testRows(64, 0), testRows(64, 64))
		bad = append([]byte(nil), bad...)
		for i := 4; i < 36 && i < len(bad); i++ {
			bad[i] = 0xFF // the first column chunk's page header
		}
		entries = append(entries, put("tables/items/chunk_002.parquet", bad, 128))
	}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
	return cat
}

// allForced is the forced census of a tracker, one figure per purpose.
func allForced(tr *memory.Tracker) map[memory.ForcePurpose]int64 {
	out := map[memory.ForcePurpose]int64{}
	for p := memory.ForceUnattributed; p <= memory.ForceSpillTracking; p++ {
		out[p] = tr.ForcedFor(p)
	}
	return out
}

func requireCensus(t *testing.T, what string, tr *memory.Tracker, used int64, forced map[memory.ForcePurpose]int64) {
	t.Helper()
	if got := tr.Used(); got != used {
		t.Errorf("%s: %s used = %d, want %d", what, tr.Name(), got, used)
	}
	for p, want := range forced {
		if got := tr.ForcedFor(p); got != want {
			t.Errorf("%s: %s forced[%q] = %d, want %d", what, tr.Name(), p.String(), got, want)
		}
	}
}

// waitFor polls cond for at most five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// runAbandonedScan runs one statement's scan on its own child of parent and
// tears it down with decoded batches still outstanding: cancelled (the
// consumer goes away) or failed (the scan's own decode error reaches the
// consumer first). It returns the statement tracker.
func runAbandonedScan(t *testing.T, cat *catalog.Catalog, parent *memory.Tracker, name string, fail bool) *memory.Tracker {
	t.Helper()
	stmt := parent.Child(name)
	src := newScannerSource(cat, "items", nil, []string{"id", "value"}, nil, nil).(*scannerExecSource)
	src.memTracker = stmt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := src.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	if fail {
		// The decode error is posted and the workers have filled the channel.
		waitFor(t, "the scan's decode error", func() bool { return len(src.scanner.errCh) == 1 })
		var err error
		for i := 0; i < 64 && err == nil; i++ {
			var b any
			b, err = src.Next(ctx)
			if err == nil && b == nil {
				t.Fatal("scan finished without surfacing its decode error")
			}
		}
		if err == nil || errors.Is(err, context.Canceled) {
			t.Fatalf("scan did not fail on the corrupt file: %v", err)
		}
	} else {
		if _, err := src.Next(ctx); err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	waitFor(t, "an outstanding decoded-batch charge", func() bool {
		return stmt.ForcedFor(memory.ForceScanDecodedBatch) > 0
	})
	cancel()
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return stmt
}

// TestScanTeardownReturnsForcedCensus (#1685a): a scan cancelled, or failed,
// with decoded batches outstanding gives back their forced charges AS forced
// charges, so total usage and every purpose's census return to their pre-scan
// values on the statement tracker and on the parent the statements share — and
// a later statement's refusal names its own forced bytes, not the phantom a
// torn-down scan left behind.
func TestScanTeardownReturnsForcedCensus(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "cancelled"
		if fail {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			cat := censusFixture(t, fail)
			const budget = 64 << 20
			parent := memory.NewTracker("worker", budget)
			before := allForced(parent)

			stmt1 := runAbandonedScan(t, cat, parent, "stmt-1", fail)
			zero := allForced(memory.NewTracker("zero", 0))
			requireCensus(t, "statement 1 after teardown", stmt1, 0, zero)
			requireCensus(t, "parent after statement 1", parent, 0, before)

			stmt2 := runAbandonedScan(t, cat, parent, "stmt-2", fail)
			requireCensus(t, "statement 2 after teardown", stmt2, 0, zero)
			requireCensus(t, "parent after statement 2", parent, 0, before)

			// A third statement forces a small charge of its own beside a
			// live neighbour's plain half of the budget, and asks for more than
			// the rest: its own tracker admits it, the shared parent refuses,
			// and the parent's refusal must name that purpose and that figure
			// alone.
			live := parent.Child("live")
			if err := live.Reserve(budget / 2); err != nil {
				t.Fatalf("live neighbour: %v", err)
			}
			stmt3 := parent.Child("stmt-3")
			stmt3.ForceReserveFor(1000, memory.ForceJoinIndex)
			err := stmt3.Reserve(budget / 2)
			if !errors.Is(err, memory.ErrMemoryExceeded) {
				t.Fatalf("statement 3 was not refused: %v", err)
			}
			if !strings.HasPrefix(err.Error(), "worker:") {
				t.Fatalf("statement 3 was refused by its own tracker, not the shared parent: %v", err)
			}
			if want := `used=33555432, requested=33554432, budget=67108864, of which forced=1000 by "hash join index")`; !strings.HasSuffix(err.Error(), want) {
				t.Errorf("statement 3's refusal = %q, want it to end %q", err, want)
			}
			stmt3.ReleaseForced(1000, memory.ForceJoinIndex)
			live.Release(budget / 2)
			requireCensus(t, "parent at the end", parent, 0, before)
		})
	}
}
