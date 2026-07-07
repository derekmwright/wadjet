package physical

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// setupJoinTables creates two joinable tables (l: id/val, r: rid/rval) with
// enough rows that any positive byte threshold sees both sides as "big".
func setupJoinTables(t *testing.T) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	writeTable := func(name string, cols []parquet.Column, rows []map[string]any) {
		schema := parquet.Schema{Columns: cols}
		if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		cfg := parquet.DefaultWriterConfig()
		cfg.Compression = parquet.CompressionNone
		w, err := parquet.NewWriter(&buf, schema, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		path := fmt.Sprintf("tables/%s/chunk_0000.parquet", name)
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), ""); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", []catalog.FileEntry{{
			Path:      path,
			SizeBytes: int64(len(data)),
			NumRows:   int64(len(rows)),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	lRows := make([]map[string]any, 300)
	for i := range lRows {
		lRows[i] = map[string]any{"id": int64(i), "val": fmt.Sprintf("l%d", i)}
	}
	writeTable("smj_l", []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}, lRows)

	rRows := make([]map[string]any, 200)
	for i := range rRows {
		rRows[i] = map[string]any{"rid": int64(i), "rval": fmt.Sprintf("r%d", i)}
	}
	writeTable("smj_r", []parquet.Column{
		{Name: "rid", Type: parquet.TypeInt64},
		{Name: "rval", Type: parquet.TypeString},
	}, rRows)

	return cat
}

// planSQL runs SQL through parse → logical → optimize → physical Plan with
// the given SMJ threshold and returns the physical plan.
func planSQL(t *testing.T, cat *catalog.Catalog, sql string, smjBytes int64) *PhysicalPlan {
	t.Helper()
	ctx := context.Background()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.SortMergeJoinBytes = smjBytes
	plan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return plan
}

// TestSortMergeJoinGate_RoutesBigInnerJoin: with the threshold forced to 1,
// an inner equi-join over two scan-rooted sides plans as a sort-merge join
// and produces the same rows the hash path would.
func TestSortMergeJoinGate_RoutesBigInnerJoin(t *testing.T) {
	cat := setupJoinTables(t)
	sql := "SELECT id, val, rval FROM smj_l JOIN smj_r ON smj_l.id = smj_r.rid"

	plan := planSQL(t, cat, sql, 1)
	if _, ok := plan.Pipeline.Source.(*smjSourceAdapter); !ok {
		t.Fatalf("expected smjSourceAdapter source under forced threshold, got %T", plan.Pipeline.Source)
	}
	if err := plan.Pipeline.Run(context.Background()); err != nil {
		t.Fatalf("running SMJ plan: %v", err)
	}
	sink := plan.Pipeline.Sink.(interface{ ToRows() []map[string]any })
	rows := sink.ToRows()
	if len(rows) != 200 { // ids 0..199 match; 200..299 unmatched
		t.Fatalf("expected 200 joined rows, got %d", len(rows))
	}
	for _, r := range rows {
		if r["val"] == nil || r["rval"] == nil {
			t.Fatalf("row missing side columns: %v", r)
		}
	}
}

// TestSortMergeJoinGate_DormantByDefault: the zero-value threshold leaves the
// join on the hash path — the gate must be unreachable without opt-in.
func TestSortMergeJoinGate_DormantByDefault(t *testing.T) {
	cat := setupJoinTables(t)
	sql := "SELECT id, val, rval FROM smj_l JOIN smj_r ON smj_l.id = smj_r.rid"

	before := SortMergeJoinsPlanned.Load()
	plan := planSQL(t, cat, sql, 0)
	if _, ok := plan.Pipeline.Source.(*smjSourceAdapter); ok {
		t.Fatal("SMJ planned with threshold 0 — the gate must be dormant by default")
	}
	if got := SortMergeJoinsPlanned.Load(); got != before {
		t.Fatalf("SortMergeJoinsPlanned moved %d→%d on the default config", before, got)
	}
	if err := plan.Pipeline.Run(context.Background()); err != nil {
		t.Fatalf("running hash plan: %v", err)
	}
}

// TestSortMergeJoinGate_SkipsNonInner: outer joins keep the hash path even
// under a forced threshold (v1 is inner-only).
func TestSortMergeJoinGate_SkipsNonInner(t *testing.T) {
	cat := setupJoinTables(t)
	sql := "SELECT id, val, rval FROM smj_l LEFT JOIN smj_r ON smj_l.id = smj_r.rid"

	plan := planSQL(t, cat, sql, 1)
	if _, ok := plan.Pipeline.Source.(*smjSourceAdapter); ok {
		t.Fatal("LEFT JOIN must not take the sort-merge path in v1")
	}
	if err := plan.Pipeline.Run(context.Background()); err != nil {
		t.Fatalf("running plan: %v", err)
	}
	sink := plan.Pipeline.Sink.(interface{ ToRows() []map[string]any })
	if rows := sink.ToRows(); len(rows) != 300 {
		t.Fatalf("LEFT JOIN expected 300 rows, got %d", len(rows))
	}
}

// TestSortMergeJoinGate_SkipsSmallSides: a threshold above both sides'
// estimated bytes keeps the hash path — SMJ is for provably-big sides only.
func TestSortMergeJoinGate_SkipsSmallSides(t *testing.T) {
	cat := setupJoinTables(t)
	sql := "SELECT id, val, rval FROM smj_l JOIN smj_r ON smj_l.id = smj_r.rid"

	plan := planSQL(t, cat, sql, 1<<40)
	if _, ok := plan.Pipeline.Source.(*smjSourceAdapter); ok {
		t.Fatal("tiny sides must not take the sort-merge path")
	}
}
