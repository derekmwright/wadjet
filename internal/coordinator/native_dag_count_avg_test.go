package coordinator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ingestMultiFile writes the given row chunks as separate parquet files
// in one table, so a downstream multi-task scan produces ≥len(chunks)
// partial outputs. Required for testing partial-aggregate merge correctness
// where the per-group merge must SUM (not COUNT) the partial counts.
func ingestMultiFile(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog, tableName string, schema []parquet.Column, chunks [][]map[string]any) {
	t.Helper()
	if err := cat.CreateTable(ctx, tableName, parquet.Schema{Columns: schema}, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	files := make([]catalog.FileEntry, 0, len(chunks))
	for i, rows := range chunks {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		path := "tables/" + tableName + "/chunk_" + string(rune('a'+i)) + ".parquet"
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		files = append(files, catalog.FileEntry{
			Path:      path,
			SizeBytes: int64(len(data)),
			NumRows:   int64(len(rows)),
			CreatedAt: time.Now(),
		})
	}
	if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", files); err != nil {
		t.Fatalf("adding files: %v", err)
	}
}

// TestNativeDAG_CountMultiFile_MergesAsSum is the explicit pre-SF10
// regression for COUNT-as-SUM merge. Multiple files → multiple scan-
// aggregate partials → final_aggregate must SUM (not re-COUNT) the
// partial counts. Pre-fix: merge applied COUNT to the partial count
// column and got the row count of partial rows (= number of partials,
// often 1 per group), not the input total.
func TestNativeDAG_CountMultiFile_MergesAsSum(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// Three files, each containing both groups (k=1 and k=2). Per-file
	// counts: {1: 2, 2: 2}. Across all three files: {1: 6, 2: 6}.
	chunks := [][]map[string]any{
		{
			{"k": int64(1), "v": int64(10)}, {"k": int64(1), "v": int64(11)},
			{"k": int64(2), "v": int64(20)}, {"k": int64(2), "v": int64(21)},
		},
		{
			{"k": int64(1), "v": int64(12)}, {"k": int64(1), "v": int64(13)},
			{"k": int64(2), "v": int64(22)}, {"k": int64(2), "v": int64(23)},
		},
		{
			{"k": int64(1), "v": int64(14)}, {"k": int64(1), "v": int64(15)},
			{"k": int64(2), "v": int64(24)}, {"k": int64(2), "v": int64(25)},
		},
	}
	ingestMultiFile(t, ctx, store, cat, "count_mf", schema, chunks)

	coord.UseNativeDAG = true
	defer func() { coord.UseNativeDAG = false }()
	res, err := coord.ExecuteSQL(ctx, "SELECT k, COUNT(*) AS n FROM count_mf GROUP BY k")
	if err != nil {
		t.Fatalf("native DAG: %v", err)
	}
	got := map[int64]int64{}
	for _, r := range res.Rows() {
		got[toInt64(r["k"])] = toInt64(r["n"])
	}
	want := map[int64]int64{1: 6, 2: 6}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("k=%d: got count=%d, want %d (got map=%v) — merge likely re-COUNT'd partial rows instead of SUMming partial counts",
				k, got[k], w, got)
		}
	}
}

// TestNativeDAG_SumMultiFile_MergesAsSum mirrors the COUNT test for SUM,
// catching merge bugs that mishandle SUM aggregation across multiple
// scan-aggregate partials.
func TestNativeDAG_SumMultiFile_MergesAsSum(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	chunks := [][]map[string]any{
		{
			{"k": int64(1), "v": int64(1)}, {"k": int64(1), "v": int64(2)},
			{"k": int64(2), "v": int64(10)},
		},
		{
			{"k": int64(1), "v": int64(3)}, {"k": int64(1), "v": int64(4)},
			{"k": int64(2), "v": int64(20)},
		},
	}
	ingestMultiFile(t, ctx, store, cat, "sum_mf", schema, chunks)

	coord.UseNativeDAG = true
	defer func() { coord.UseNativeDAG = false }()
	res, err := coord.ExecuteSQL(ctx, "SELECT k, SUM(v) AS s FROM sum_mf GROUP BY k")
	if err != nil {
		t.Fatalf("native DAG: %v", err)
	}
	got := map[int64]int64{}
	for _, r := range res.Rows() {
		got[toInt64(r["k"])] = toInt64(r["s"])
	}
	want := map[int64]int64{1: 10, 2: 30} // 1: 1+2+3+4=10; 2: 10+20=30
	for k, w := range want {
		if got[k] != w {
			t.Errorf("k=%d: got sum=%d, want %d (got map=%v)", k, got[k], w, got)
		}
	}
}

// TestNativeDAG_AvgMultiFile_FallbackOrCorrect verifies AVG over multiple
// files gives the right answer regardless of whether the dispatcher uses
// the single-task fallback (current implementation) or eventually grows
// a SUM+COUNT decomposition. The test asserts CORRECTNESS — not which
// code path is taken — so it'll keep passing if AVG decomposition lands.
func TestNativeDAG_AvgMultiFile_FallbackOrCorrect(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// Per-file averages would be: file0 {k=1: 1.5, k=2: 10}; file1 {k=1: 3.5, k=2: 20}.
	// True averages across all files: {k=1: (1+2+3+4)/4 = 2.5; k=2: (10+20)/2 = 15}.
	// If a buggy merge naively averaged the per-file averages, it'd compute
	// {k=1: (1.5+3.5)/2 = 2.5 (coincidentally correct); k=2: (10+20)/2 = 15 (coincidentally correct)}.
	// To distinguish, use UNEVEN row counts per file so naive avg-of-avgs differs from true avg.
	chunks := [][]map[string]any{
		{
			{"k": int64(1), "v": int64(10)},
			{"k": int64(2), "v": int64(100)},
		},
		{
			{"k": int64(1), "v": int64(20)}, {"k": int64(1), "v": int64(30)}, {"k": int64(1), "v": int64(40)},
			{"k": int64(2), "v": int64(200)}, {"k": int64(2), "v": int64(300)},
		},
	}
	// True averages: k=1: (10+20+30+40)/4 = 25; k=2: (100+200+300)/3 = 200.
	// Naive avg-of-per-file-avgs: k=1: (10 + 30)/2 = 20 (wrong, true is 25); k=2: (100 + 250)/2 = 175 (wrong, true is 200).
	ingestMultiFile(t, ctx, store, cat, "avg_mf", schema, chunks)

	coord.UseNativeDAG = true
	defer func() { coord.UseNativeDAG = false }()
	res, err := coord.ExecuteSQL(ctx, "SELECT k, AVG(v) AS a FROM avg_mf GROUP BY k")
	if err != nil {
		t.Fatalf("native DAG: %v", err)
	}
	got := map[int64]float64{}
	for _, r := range res.Rows() {
		switch x := r["a"].(type) {
		case float64:
			got[toInt64(r["k"])] = x
		case int64:
			got[toInt64(r["k"])] = float64(x)
		}
	}
	want := map[int64]float64{1: 25, 2: 200}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("k=%d: got avg=%v, want %v — naive avg-of-partial-avgs would give k=1:20 k=2:175 (got map=%v)",
				k, got[k], w, got)
		}
	}
}
