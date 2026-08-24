package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #491 follow-up. SubmitSQL's dispatch never stamps Task.DeleteMarkers on
// the wire — unlike executeStageDAG, it overwrites physStages with a
// synthetic single "pipeline" stage (carrying no ScanDeletes) before any
// stamp built from it would be read. See the comment above asyncCtx's
// construction in SubmitSQL (coordinator.go) for the full argument; this
// test is the end-to-end evidence for it: every TaskTypePipeline task
// re-plans its SQL on the worker against a live catalog and applies
// deletes exactly as the embedded single-process engine does, so the
// pipeline path is correct here independent of any wire-level marker.
//
// Both shapes SubmitSQL's createPipelineTasks can dispatch are covered:
// the plain whole-query pipeline (Tasks: 1, no file lists at all — the
// common case, since CanProbeSplit needs more files/bytes than this
// fixture has by default) and the probe-split pipeline (ScanFileFilter
// restricts each worker to a slice of the table's files — the shape the
// now-removed "a pre-scanned input CAN be base-table parquet" comment on
// executor.go's executePipeline was written for).
func TestDistributedScanHonorsDeleteMarkersOnThePipelinePath(t *testing.T) {
	ctx := context.Background()
	infra := tmdInfra(t, ctx)
	coord := tmdCoordinator(t, ctx, infra)
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
	})
	if err != nil {
		t.Fatalf("open DB over the coordinator's KV: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}}
	if err := infra.cat.CreateTable(ctx, "psd_a", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Six single-row files: enough for CanProbeSplit's workerCount*2 = 6
	// file-count gate once the probe-split phase below lowers
	// ProbeSplitMinBytes and inflates SizeBytes; too small in real bytes
	// to trip that gate on its own, which is what keeps the FIRST phase on
	// the plain (Tasks: 1) branch without any special-casing.
	//
	// One AddFiles call for all six entries, not six separate calls: the
	// embedded NATS/JetStream server's own internal stream-config
	// bookkeeping (vendored nats-server, unrelated to anything this test
	// exercises) has a data race under enough back-to-back stream-update
	// requests in a short window — batching keeps this fixture from being
	// the thing that trips it.
	const n = 6
	labels := []string{"x", "y"}
	entries := make([]catalog.FileEntry, n)
	for i := 1; i <= n; i++ {
		row := map[string]any{"k": int64(i), "g": labels[i%2], "v": int64(i * 10)}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows([]map[string]any{row}); err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer %d: %v", i, err)
		}
		data := buf.Bytes()
		path := fmt.Sprintf("tables/psd_a/chunk_%04d.parquet", i)
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put chunk %d: %v", i, err)
		}
		entries[i-1] = catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: 1, CreatedAt: time.Now()}
	}
	if err := infra.cat.AddFiles(ctx, "psd_a", map[string]string{}, "tables/psd_a/", entries); err != nil {
		t.Fatalf("add chunks: %v", err)
	}

	if _, err := db.Execute(ctx, "DELETE FROM psd_a WHERE k = 3"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	// Phase 1: plain pipeline path. The fixture's real file sizes are a
	// few hundred bytes each, far under the default 64 MB ProbeSplitMinBytes,
	// so CanProbeSplit returns false and SubmitSQL takes the "else" branch —
	// a single task with SQLText only, no ScanFileFilter/PreScannedInputs.
	psdCheckSubmit(t, ctx, coord, "SELECT k FROM psd_a", "k", []string{"1", "2", "4", "5", "6"}, false)
	psdCheckSubmit(t, ctx, coord, "SELECT COUNT(*) AS n FROM psd_a", "n", []string{"5"}, false)
	psdCheckSubmit(t, ctx, coord, "SELECT SUM(v) AS s FROM psd_a", "s", []string{"180"}, false)

	// Phase 2: probe-split pipeline path. Lowering ProbeSplitMinBytes and
	// re-declaring the same files at 32 MB each (the file CONTENT and the
	// catalog's manifest — including the DELETE marker already recorded
	// against chunk_0003.parquet — are untouched; only the declared
	// SizeBytes changes) makes CanProbeSplit's gates pass with 3 workers,
	// the same trick TestDistributedFusedAgg uses.
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })
	resized := make([]catalog.FileEntry, n)
	for i := 1; i <= n; i++ {
		path := fmt.Sprintf("tables/psd_a/chunk_%04d.parquet", i)
		resized[i-1] = catalog.FileEntry{Path: path, SizeBytes: 32 * 1024 * 1024, NumRows: 1, CreatedAt: time.Now()}
	}
	if err := infra.cat.AddFiles(ctx, "psd_a", map[string]string{}, "tables/psd_a/", resized); err != nil {
		t.Fatalf("re-declare chunk sizes: %v", err)
	}

	psdCheckSubmit(t, ctx, coord, "SELECT k FROM psd_a", "k", []string{"1", "2", "4", "5", "6"}, true)
	psdCheckSubmit(t, ctx, coord, "SELECT COUNT(*) AS n FROM psd_a", "n", []string{"5"}, true)
	psdCheckSubmit(t, ctx, coord, "SELECT SUM(v) AS s FROM psd_a", "s", []string{"180"}, true)
}

// psdCheckSubmit runs sql through SubmitSQL, waits for completion, and
// asserts the named column's values (sorted) match want. When
// wantProbeSplit is true it also asserts the query's stored plan actually
// took the probe-split branch (stage.ProbeSplitAlias set) — otherwise a
// fixture that silently fell back to the plain branch would make Phase 2
// indistinguishable from Phase 1 and prove nothing about ScanFileFilter.
func psdCheckSubmit(t *testing.T, ctx context.Context, coord *Coordinator, sql, col string, want []string, wantProbeSplit bool) {
	t.Helper()
	queryID, _, err := coord.SubmitSQL(ctx, sql)
	if err != nil {
		t.Fatalf("SubmitSQL(%q): %v", sql, err)
	}

	coord.mu.Lock()
	qm := coord.queryMetas[queryID]
	coord.mu.Unlock()
	gotProbeSplit := qm != nil && len(qm.stages) == 1 && qm.stages[0].ProbeSplitAlias != ""
	if gotProbeSplit != wantProbeSplit {
		t.Fatalf("SubmitSQL(%q): probe-split=%v, want %v", sql, gotProbeSplit, wantProbeSplit)
	}

	deadline := time.Now().Add(30 * time.Second)
	var status *QueryStatus
	for time.Now().Before(deadline) {
		status, err = coord.GetQueryStatus(queryID)
		if err != nil {
			t.Fatalf("GetQueryStatus(%q): %v", sql, err)
		}
		if status.State == QueryStateCompleted.String() || status.State == QueryStateFailed.String() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status == nil || (status.State != QueryStateCompleted.String() && status.State != QueryStateFailed.String()) {
		t.Fatalf("SubmitSQL(%q) did not complete within 30s (state=%v)", sql, status)
	}
	res, err := coord.GetQueryResults(ctx, queryID)
	if err != nil {
		t.Fatalf("GetQueryResults(%q): %v", sql, err)
	}
	if res.Error != "" {
		t.Fatalf("SubmitSQL(%q) failed: %s", sql, res.Error)
	}
	rows, err := res.Rows()
	if err != nil {
		t.Fatalf("materializing rows for %q: %v", sql, err)
	}
	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, fmt.Sprint(row[col]))
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	mfdWant(t, got, wantSorted, fmt.Sprintf("SubmitSQL %q column %q", sql, col))
}
