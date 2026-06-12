package exec

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// forceTinyRuns lowers the sorted-run floor so unit-scale budgets exercise
// the external-merge path; restores on cleanup.
func forceTinyRuns(tb testing.TB) {
	tb.Helper()
	oldRun := minSortRunBytes
	minSortRunBytes = 1
	tb.Cleanup(func() { minSortRunBytes = oldRun })
}

// newSortSpillHarness builds a Sort with a tracker budget small enough that
// SpillCheap pressure fires after a few batches.
func newSortSpillHarness(tb testing.TB, keys []SortKey, budget int64) *Sort {
	tb.Helper()
	tracker := memory.NewTracker("sort-ext-test", budget)
	sm, err := memory.NewSpillManager(tb.TempDir(), tracker)
	if err != nil {
		tb.Fatal(err)
	}
	s := NewSort(keys)
	s.Spill = sm
	if err := s.Init(context.Background()); err != nil {
		tb.Fatal(err)
	}
	return s
}

func drainSortRows(tb testing.TB, s *Sort) []map[string]any {
	tb.Helper()
	var rows []map[string]any
	for {
		b, err := s.Next(context.Background())
		if err != nil {
			tb.Fatalf("Next: %v", err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

// TestSortExternalMerge_StringsNullsDesc pushes string-keyed rows with nulls
// through the run-spill path and verifies the merged stream matches an
// in-memory reference sort: DESC with NULLS LAST plus an int tiebreaker.
func TestSortExternalMerge_StringsNullsDesc(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "id", Type: parquet.TypeInt64},
	}
	keys := []SortKey{
		{Column: "name", Order: Descending, NullsLast: true},
		{Column: "id", Order: Ascending},
	}

	rng := rand.New(rand.NewSource(42))
	var allRows []map[string]any
	const numBatches, rowsPerBatch = 12, 16
	for i := 0; i < numBatches*rowsPerBatch; i++ {
		var name any
		if rng.Intn(8) == 0 {
			name = nil
		} else {
			name = fmt.Sprintf("name-%02d", rng.Intn(20))
		}
		allRows = append(allRows, map[string]any{"name": name, "id": int64(i)})
	}

	// Reference: in-memory Sort (no Spill) over the same data.
	ref := NewSort(keys)
	if err := ref.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Spilling Sort with a budget tiny enough that every batch crosses
	// SpillCheap (40% of 512B).
	s := newSortSpillHarness(t, keys, 512)

	ctx := context.Background()
	for i := 0; i < numBatches; i++ {
		rows := allRows[i*rowsPerBatch : (i+1)*rowsPerBatch]
		if err := ref.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.runFiles) == 0 {
		t.Fatal("run-spill path was never exercised; budget/floor setup is wrong")
	}
	runPaths := append([]string(nil), s.runFiles...)

	if err := ref.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	want := drainSortRows(t, ref)
	got := drainSortRows(t, s)
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if fmt.Sprint(got[i]["name"]) != fmt.Sprint(want[i]["name"]) || got[i]["id"] != want[i]["id"] {
			t.Fatalf("row %d: got %v want %v", i, got[i], want[i])
		}
	}

	// Run scratch must be deleted once the merge drains.
	for _, p := range runPaths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("run file %s not cleaned up after drain", p)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSortExternalMerge_TopK verifies Limit enforcement across spilled runs:
// each run is truncated to Limit at write time and the merged stream stops at
// exactly Limit rows.
func TestSortExternalMerge_TopK(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	keys := []SortKey{{Column: "val", Order: Ascending}}

	s := newSortSpillHarness(t, keys, 512)
	s.Limit = 7

	ctx := context.Background()
	const total = 200
	perm := rand.New(rand.NewSource(7)).Perm(total)
	for i := 0; i < total; i += 10 {
		rows := make([]map[string]any, 0, 10)
		for j := i; j < i+10; j++ {
			rows = append(rows, map[string]any{"val": int64(perm[j])})
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.runFiles) == 0 {
		t.Fatal("run-spill path was never exercised")
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := drainSortRows(t, s)
	if len(got) != 7 {
		t.Fatalf("Top-K: got %d rows, want 7", len(got))
	}
	for i, r := range got {
		if r["val"] != int64(i) {
			t.Fatalf("Top-K row %d: got %v want %d", i, r["val"], i)
		}
	}
}

// TestSortExternalMerge_MultiLevel forces the multi-level pre-merge by
// capping fan-in at 2 with many runs, verifying correctness end-to-end.
func TestSortExternalMerge_MultiLevel(t *testing.T) {
	forceTinyRuns(t)
	oldFan := maxMergeFanIn
	maxMergeFanIn = 2
	t.Cleanup(func() { maxMergeFanIn = oldFan })

	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	s := newSortSpillHarness(t, []SortKey{{Column: "val", Order: Ascending}}, 256)

	ctx := context.Background()
	const total = 300
	perm := rand.New(rand.NewSource(11)).Perm(total)
	for i := 0; i < total; i += 5 {
		rows := make([]map[string]any, 0, 5)
		for j := i; j < i+5; j++ {
			rows = append(rows, map[string]any{"val": int64(perm[j])})
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.runFiles) < 3 {
		t.Fatalf("need ≥3 runs to exercise multi-level merge, got %d", len(s.runFiles))
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := drainSortRows(t, s)
	if len(got) != total {
		t.Fatalf("row count: got %d want %d", len(got), total)
	}
	for i, r := range got {
		if r["val"] != int64(i) {
			t.Fatalf("row %d: got %v want %d", i, r["val"], i)
		}
	}
}

// TestSortTruncate_ExternalMergePath covers the fragment-executor contract:
// Sort built WITHOUT Limit, spills, Finalize, then Truncate(n) runs once
// before the first Next — the merged stream must stop at n rows.
func TestSortTruncate_ExternalMergePath(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	s := newSortSpillHarness(t, []SortKey{{Column: "val", Order: Ascending}}, 512)

	ctx := context.Background()
	const total = 100
	perm := rand.New(rand.NewSource(3)).Perm(total)
	for i := 0; i < total; i += 10 {
		rows := make([]map[string]any, 0, 10)
		for j := i; j < i+10; j++ {
			rows = append(rows, map[string]any{"val": int64(perm[j])})
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.runFiles) == 0 {
		t.Fatal("run-spill path was never exercised")
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	s.Truncate(5)
	got := drainSortRows(t, s)
	if len(got) != 5 {
		t.Fatalf("Truncate(5) on merge path: got %d rows", len(got))
	}
	for i, r := range got {
		if r["val"] != int64(i) {
			t.Fatalf("row %d: got %v want %d", i, r["val"], i)
		}
	}
}

// TestSortSpillSome_ReliefContract verifies the AccountedOperator contract on
// the run path: EstimateRelief's claim is delivered by SpillSome, and the
// sort still produces correct output afterward.
func TestSortSpillSome_ReliefContract(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	// Large budget: no self-spill; SpillSome is the only spill trigger.
	s := newSortSpillHarness(t, []SortKey{{Column: "val", Order: Ascending}}, 1<<30)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		rows := make([]map[string]any, 0, 10)
		for j := 0; j < 10; j++ {
			rows = append(rows, map[string]any{"val": int64(100 - (i*10 + j))})
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	claimed := s.EstimateRelief(1 << 20)
	if claimed == 0 {
		t.Fatal("EstimateRelief claimed 0 with buffered batches")
	}
	freed, err := s.SpillSome(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if freed != claimed {
		t.Fatalf("SpillSome freed %d, EstimateRelief claimed %d", freed, claimed)
	}
	if len(s.runFiles) == 0 {
		t.Fatal("SpillSome did not produce a run file")
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := drainSortRows(t, s)
	if len(got) != 100 {
		t.Fatalf("row count: got %d want 100", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i]["val"].(int64) < got[i-1]["val"].(int64) {
			t.Fatalf("not sorted at %d", i)
		}
	}
}

// TestColumnarSpillVectorRoundTrip locks the new TypeVector encoding in the
// columnar spill format: dim + values + nulls must round-trip exactly.
func TestColumnarSpillVectorRoundTrip(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "emb", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
	}
	const n = 5
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
		for d := 0; d < 3; d++ {
			b.Columns[1].Float32Data[i*3+d] = float32(i*10 + d)
		}
	}
	b.Columns[1].Nulls.SetNull(2)

	path, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{b})
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	got, err := readSpillBatches(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Len != n {
		t.Fatalf("read back %d batches, first len %d", len(got), got[0].Len)
	}
	v := got[0].Columns[1]
	if v.VectorDim != 3 {
		t.Fatalf("VectorDim: got %d want 3", v.VectorDim)
	}
	for i := 0; i < n; i++ {
		if i == 2 {
			if !v.Nulls.IsNull(i) {
				t.Fatalf("row 2 should be null")
			}
			continue
		}
		for d := 0; d < 3; d++ {
			if v.Float32Data[i*3+d] != float32(i*10+d) {
				t.Fatalf("row %d dim %d: got %v want %v", i, d, v.Float32Data[i*3+d], float32(i*10+d))
			}
		}
	}
}

// TestSortExternalMerge_NestedSchema pushes ARRAY/MAP/ROW columns through
// the forced run-spill path and verifies the merged stream is value-identical
// to the in-memory reference sort. Locks the nested round-trip through:
// sortBatchesToRun gather -> columnar run encode/decode -> runMerger
// copyVectorValue reassembly.
func TestSortExternalMerge_NestedSchema(t *testing.T) {
	forceTinyRuns(t)

	elem := parquet.Column{Name: "element", Type: parquet.TypeString}
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "tags", Type: parquet.TypeArray, ElementType: &elem, Nullable: true},
		{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeString, Nullable: true},
		}},
	}

	makeRows := func() []map[string]any {
		rng := rand.New(rand.NewSource(7))
		perm := rng.Perm(300)
		rows := make([]map[string]any, 0, len(perm))
		for _, id := range perm {
			var tags any
			switch id % 4 {
			case 0:
				tags = nil // null array
			case 1:
				tags = []any{} // empty array
			default:
				tags = []any{fmt.Sprintf("t%d", id), fmt.Sprintf("u%d", id%7)}
			}
			var b any
			if id%5 == 0 {
				b = nil
			} else {
				b = fmt.Sprintf("b%d", id)
			}
			rows = append(rows, map[string]any{
				"id":   int64(id),
				"tags": tags,
				"rec":  map[string]any{"a": int64(id * 2), "b": b},
			})
		}
		return rows
	}

	feed := func(s *Sort, rows []map[string]any) {
		for pos := 0; pos < len(rows); pos += 32 {
			end := pos + 32
			if end > len(rows) {
				end = len(rows)
			}
			if err := s.Consume(context.Background(), batch.FromRows(schema, rows[pos:end])); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Finalize(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	keys := []SortKey{{Column: "id", Order: Ascending}}

	ref := NewSort(keys) // no spill manager: pure in-memory reference
	if err := ref.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(ref, makeRows())
	want := drainSortRows(t, ref)

	s := newSortSpillHarness(t, keys, 1) // 1-byte budget: every Consume is over-pressure
	feed(s, makeRows())
	got := drainSortRows(t, s)

	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("row %d differs:\n got  %#v\n want %#v", i, got[i], want[i])
		}
	}
}

// TestSortExternalMerge_Determinism: two identical spilling sorts must emit
// byte-identical row order (run-ordinal tie-break), the property distributed
// re-runs rely on.
func TestSortExternalMerge_Determinism(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "tag", Type: parquet.TypeString},
	}
	keys := []SortKey{{Column: "k", Order: Ascending}}
	run := func() []map[string]any {
		s := newSortSpillHarness(t, keys, 512)
		ctx := context.Background()
		// Many duplicate keys so the tie-break actually decides ordering.
		for i := 0; i < 10; i++ {
			rows := make([]map[string]any, 0, 10)
			for j := 0; j < 10; j++ {
				rows = append(rows, map[string]any{"k": int64(j % 3), "tag": fmt.Sprintf("b%02d-r%02d", i, j)})
			}
			if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		return drainSortRows(t, s)
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("row counts differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i]["k"] != b[i]["k"] || a[i]["tag"] != b[i]["tag"] {
			t.Fatalf("row %d differs between identical runs: %v vs %v", i, a[i], b[i])
		}
	}
	// Keys must still be sorted.
	if !sort.SliceIsSorted(a, func(i, j int) bool { return a[i]["k"].(int64) < a[j]["k"].(int64) }) {
		t.Fatal("output not sorted by key")
	}
}
