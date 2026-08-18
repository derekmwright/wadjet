package expr

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The per-batch memo (tryEvalMemoized) must produce byte-identical output
// to the plain per-row fallback for the regexp family: duplicated inputs,
// NULLs, non-matching rows, and empty strings all covered.
func TestMemoizedRegexpEvalMatchesPerRow(t *testing.T) {
	const n = 64
	vals := []any{
		"https://www.example.com/path/x", // matching, duplicated heavily
		"http://other.org/y/z",           // matching
		"not-a-url",                      // non-matching
		"",                               // empty
		nil,                              // NULL
	}
	col := batch.NewVector(batch.TypeString, n)
	for i := 0; i < n; i++ {
		v := vals[i%len(vals)]
		if v == nil {
			col.Nulls.SetNull(i)
			col.BytesData.Set(i, nil)
		} else {
			col.BytesData.Set(i, []byte(v.(string)))
		}
	}
	b := &batch.RecordBatch{
		Schema:  []parquet.Column{{Name: "s", Type: parquet.TypeString}},
		Columns: []*batch.Vector{col},
		Len:     n,
	}

	for _, tc := range []struct {
		name string
		fc   *FuncCall
	}{
		{"replace", &FuncCall{Name: "regexp_replace", Args: []Expr{
			&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}}},
		{"extract", &FuncCall{Name: "regexp_extract", Args: []Expr{
			&ColRef{Name: "s"}, &Lit{Val: `https?://([^/]+)`}, &Lit{Val: int64(1)}}}},
		{"like", &FuncCall{Name: "regexp_like", Args: []Expr{
			&ColRef{Name: "s"}, &Lit{Val: `^https`}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := tc.fc

			// Reference: plain per-row Eval.
			want := make([]any, n)
			for i := 0; i < n; i++ {
				want[i] = fc.Eval(b, i)
			}

			// Memoized batch path. regexp fns have no vec kernel, so
			// EvalVec routes through tryEvalMemoized; assert it engaged.
			out := batch.NewVector(batch.TypeString, n)
			if tc.name == "like" {
				out = batch.NewVector(batch.TypeBool, n)
			}
			if !fc.tryEvalMemoized(b, out, n) {
				t.Fatal("tryEvalMemoized did not engage")
			}
			for i := 0; i < n; i++ {
				var got any
				if out.Nulls.IsNull(i) {
					got = nil
				} else {
					got = out.GetValue(i)
				}
				if fmt.Sprint(got) != fmt.Sprint(want[i]) || (got == nil) != (want[i] == nil) {
					t.Fatalf("row %d: memo %v (%T), per-row %v (%T)", i, got, got, want[i], want[i])
				}
			}
		})
	}
}

// memoTestBatch builds a one-column string batch; nil entries are NULLs.
func memoTestBatch(tb testing.TB, vals []any) *batch.RecordBatch {
	tb.Helper()
	col := batch.NewVector(batch.TypeString, len(vals))
	for i, v := range vals {
		if v == nil {
			col.WriteNullAt(i)
			continue
		}
		col.BytesData.Set(i, []byte(v.(string)))
	}
	return &batch.RecordBatch{
		Schema:  []parquet.Column{{Name: "s", Type: parquet.TypeString}},
		Columns: []*batch.Vector{col},
		Len:     len(vals),
	}
}

// memoRandomInputs is the shared corpus for the typed-memo parity checks:
// repeats (the memo's whole point), empty strings, NULLs, multibyte, and
// values that differ only in a trailing byte so prefix relationships and
// near-collisions are exercised.
func memoRandomInputs(seed int64, n int) []any {
	r := rand.New(rand.NewSource(seed))
	pool := []any{
		nil, "", " ", "\n",
		"http://www.example.com/a/b",
		"http://www.example.com/a/c",
		"https://example.com/",
		"https://例え.jp/パス/ページ",
		"http://www.münchen.example.de/straße",
		"ünïcøde",
		"not-a-url",
		strings.Repeat("x", 300) + "://",
		"http://www.example.com/a/b\x00",
	}
	for c := byte('a'); c <= 'z'; c++ {
		pool = append(pool, "http://www.collide.example.org/p/"+string(c))
	}
	out := make([]any, n)
	for i := range out {
		out[i] = pool[r.Intn(len(pool))]
	}
	return out
}

// The typed string memo must be indistinguishable from plain per-row Eval
// — same values, same NULLs — over a randomized corpus, for every
// memoizable function that lands on a string output column.
func TestMemoizedStringsMatchPerRowRandomized(t *testing.T) {
	fns := []*FuncCall{
		{Name: "regexp_replace", Args: []Expr{ // ClickBench Q29, anchored
			&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}},
		{Name: "regexp_replace", Args: []Expr{ // unanchored, many matches
			&ColRef{Name: "s"}, &Lit{Val: `[aeiou]`}, &Lit{Val: `*`}}},
		{Name: "regexp_extract", Args: []Expr{ // NULL when nothing matches
			&ColRef{Name: "s"}, &Lit{Val: `https?://([^/]+)`}, &Lit{Val: int64(1)}}},
	}
	for seed := int64(0); seed < 8; seed++ {
		vals := memoRandomInputs(seed, 257)
		b := memoTestBatch(t, vals)
		for _, fc := range fns {
			want := make([]any, len(vals))
			for i := range vals {
				want[i] = fc.Eval(b, i)
			}
			out := batch.NewVector(batch.TypeString, len(vals))
			if !fc.tryEvalMemoized(b, out, len(vals)) {
				t.Fatalf("%s: tryEvalMemoized did not engage", fc.Name)
			}
			for i := range vals {
				var got any
				if !out.Nulls.IsNull(i) {
					got = out.BytesData.StringValue(i)
				}
				if (got == nil) != (want[i] == nil) || fmt.Sprint(got) != fmt.Sprint(want[i]) {
					t.Fatalf("seed %d %s row %d (input %v): memo %#v, per-row %#v",
						seed, fc.Name, i, vals[i], got, want[i])
				}
			}
		}
	}
}

// The memo's map storage is pooled across batches, and its keys are views
// into the input arena — which the engine also pools, refilling the same
// bytes with the next batch's values. Reproduce exactly that pairing: one
// reused input column, one reused output column, disjoint values per
// round. A memo entry surviving a round would be a key whose bytes have
// since been overwritten, mapping to a previous round's result.
func TestMemoizedStringsPooledMapIsNotStale(t *testing.T) {
	fc := &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}}
	const n = 128
	col := batch.NewVector(batch.TypeString, n)
	b := &batch.RecordBatch{
		Schema:  []parquet.Column{{Name: "s", Type: parquet.TypeString}},
		Columns: []*batch.Vector{col},
		Len:     n,
	}
	out := batch.NewVector(batch.TypeString, n)
	for round := range 50 {
		col.BytesData.Reset()
		for i := range n {
			// Fixed width, so round r's row i lands on exactly the bytes
			// round r-1's row i occupied.
			col.BytesData.Set(i, fmt.Appendf(nil, "http://www.r%02d-h%02d.example.com/p", round, i%7))
		}
		out.BytesData.Reset()
		if !fc.tryEvalMemoized(b, out, n) {
			t.Fatal("tryEvalMemoized did not engage")
		}
		for i := range n {
			want := fmt.Sprintf("r%02d-h%02d.example.com", round, i%7)
			if got := out.BytesData.StringValue(i); got != want {
				t.Fatalf("round %d row %d: got %q, want %q", round, i, got, want)
			}
		}
	}
}

// Direct check of the pool contract behind the test above: a map goes back
// empty, so it can never carry an entry — or a pointer into a retired
// batch's arena, which a recycled arena would silently rewrite — into the
// next batch. sync.Pool may hand back a fresh map instead of the one just
// returned, which only costs this check a chance to fire; it never makes
// it fail spuriously.
func TestMemoStrPoolReturnsClearedMaps(t *testing.T) {
	fc := &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}}
	vals := []any{
		"http://www.a.example.com/p", "http://www.b.example.com/p",
		"http://www.c.example.com/p", "http://www.d.example.com/p",
	}
	b := memoTestBatch(t, vals)
	out := batch.NewVector(batch.TypeString, len(vals))
	if !fc.tryEvalMemoized(b, out, len(vals)) {
		t.Fatal("tryEvalMemoized did not engage")
	}
	m := memoStrPool.Get().(map[string]memoStr)
	defer memoStrPool.Put(m)
	if len(m) != 0 {
		t.Fatalf("pooled memo came back with %d entries: %v", len(m), m)
	}
}

// One *FuncCall is shared by parallel pipeline clones, each evaluating its
// own batch. The memo is a local of tryEvalMemoized and its keys are
// zero-copy views into the caller's batch, so nothing may be shared across
// goroutines; run it under -race with each goroutine checking its own
// results, which also exercises the pooled map under concurrent checkout.
func TestMemoizedStringsConcurrentBatches(t *testing.T) {
	fc := &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for round := range 25 {
				vals := make([]any, 200)
				for i := range vals {
					vals[i] = fmt.Sprintf("http://www.g%d-r%d-h%d.example.com/path", g, round, i%11)
				}
				b := memoTestBatch(t, vals)
				out := batch.NewVector(batch.TypeString, len(vals))
				if !fc.tryEvalMemoized(b, out, len(vals)) {
					t.Errorf("goroutine %d: tryEvalMemoized did not engage", g)
					return
				}
				for i := range vals {
					want := fmt.Sprintf("g%d-r%d-h%d.example.com", g, round, i%11)
					if got := out.BytesData.StringValue(i); got != want {
						t.Errorf("goroutine %d round %d row %d: got %q, want %q", g, round, i, got, want)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// The point of the typed path: a distinct input no longer costs an
// interface box plus a cloned key, a row no longer costs a string→[]byte
// conversion inside SetValue, and the map storage is recycled.
func TestMemoizedStringsAllocationsScaleWithDistinctValues(t *testing.T) {
	fc := &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}}
	allocsFor := func(n int) float64 {
		vals := make([]any, n)
		for i := range vals {
			vals[i] = fmt.Sprintf("http://www.host%d.example.com/p/%d", i%8, i%8)
		}
		b := memoTestBatch(t, vals)
		out := batch.NewVector(batch.TypeString, n)
		return testing.AllocsPerRun(50, func() {
			out.BytesData.Reset()
			fc.tryEvalMemoized(b, out, n)
		})
	}
	// Same 8 distinct values either way, so the only thing that changes is
	// the row count. Stated as a ratio rather than an absolute (the race
	// detector adds a fixed overhead): 8x the rows must not cost more
	// allocations, because rows resolve out of the memo and write into the
	// output arena with neither a box nor a copy.
	small, large := allocsFor(64), allocsFor(512)
	if large > small+4 {
		t.Errorf("allocations grew with rows: %v at 64 rows, %v at 512 rows (8 distinct values in both)", small, large)
	}
}
