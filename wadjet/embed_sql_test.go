package wadjet

import (
	"context"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/embedding"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// countingProvider returns deterministic embeddings and records how many
// provider.Embed() calls were made, so the test can assert SQL-level batching.
type countingProvider struct {
	dim   int
	calls int
	rows  int
}

func (p *countingProvider) Embed(texts []string) ([][]float32, error) {
	p.calls++
	p.rows += len(texts)
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v := make([]float32, p.dim)
		seed := float32(0.1)
		if len(t) > 0 {
			seed = float32(t[0]) / 256.0
		}
		for j := range v {
			v[j] = seed + float32(j)*0.01
		}
		out[i] = v
	}
	return out, nil
}
func (p *countingProvider) Dimension() int { return p.dim }
func (p *countingProvider) Model() string  { return "counting-test" }

// TestEmbedSQLEndToEnd exercises embed() through the full SQL pipeline: it must
// (1) produce a real VECTOR column (not a stringified slice), (2) batch a whole
// record batch into a single provider call rather than one call per row,
// (3) pass NULL text through as NULL, and (4) compose with the VECTOR functions.
func TestEmbedSQLEndToEnd(t *testing.T) {
	ctx := context.Background()
	prov := &countingProvider{dim: 8}
	embedding.SetProvider(prov)
	defer embedding.SetProvider(nil)
	embedding.RegisterFunctions()

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "txt", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "docs", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "txt": "hello"},
		{"id": int64(2), "txt": "world"},
		{"id": int64(3), "txt": nil}, // NULL text → NULL embedding
	}
	ing := db.NewIngester("docs", schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, "SELECT id, embed(txt) AS v FROM docs ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(res.Rows))
	}

	// (1) VECTOR output: the value is a []float32 of the provider's dimension,
	//     not a string rendering of one.
	v1, ok := res.Rows[0]["v"].([]float32)
	if !ok {
		t.Fatalf("embed() output is %T, want []float32", res.Rows[0]["v"])
	}
	if len(v1) != 8 {
		t.Fatalf("embedding dim = %d, want 8", len(v1))
	}

	// Distinct inputs → distinct embeddings.
	v2 := res.Rows[1]["v"].([]float32)
	if v1[0] == v2[0] {
		t.Errorf("expected different embeddings for 'hello' vs 'world'")
	}

	// (3) NULL text → NULL embedding.
	if res.Rows[2]["v"] != nil {
		t.Errorf("embed(NULL) = %v, want nil", res.Rows[2]["v"])
	}

	// (2) SQL-level batching: the two non-null rows were embedded in a single
	//     provider call, not one per row.
	if prov.calls != 1 {
		t.Errorf("provider.Embed called %d times, want 1 (batched)", prov.calls)
	}
	if prov.rows != 2 {
		t.Errorf("provider embedded %d texts, want 2 (NULL excluded)", prov.rows)
	}

	// (4) Composition: embed() output feeds the VECTOR similarity functions.
	res2, err := db.Query(ctx, "SELECT cosine_similarity(embed(txt), embed(txt)) AS sim FROM docs WHERE id = 1")
	if err != nil {
		t.Fatalf("cosine query: %v", err)
	}
	if len(res2.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res2.Rows))
	}
	sim, ok := toFloat(res2.Rows[0]["sim"])
	if !ok || sim < 0.999 {
		t.Errorf("cosine_similarity of a vector with itself = %v, want ~1.0", res2.Rows[0]["sim"])
	}
}

// fixedDimProvider reports one dimension but returns vectors of a different
// width — simulating a misconfigured/unknown model. vecEmbed must NULL the
// mismatched rows rather than truncate or leave stale pooled data.
type fixedDimProvider struct {
	reportDim int
	returnDim int
}

func (p *fixedDimProvider) Embed(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		v := make([]float32, p.returnDim)
		for j := range v {
			v[j] = float32(j) + 1
		}
		out[i] = v
	}
	return out, nil
}
func (p *fixedDimProvider) Dimension() int { return p.reportDim }
func (p *fixedDimProvider) Model() string  { return "fixed-dim-test" }

// TestEmbedSQLBatchedUnderFilter is the regression for the review finding that
// a selection vector (from a WHERE filter) made embed() fall back to one
// provider call per row. With multiple non-null rows surviving the filter, a
// batched embed() makes exactly one call; the pre-fix per-row path made N.
func TestEmbedSQLBatchedUnderFilter(t *testing.T) {
	ctx := context.Background()
	prov := &countingProvider{dim: 8}
	embedding.SetProvider(prov)
	defer embedding.SetProvider(nil)
	embedding.RegisterFunctions()

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "txt", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "docs", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 6)
	for i := 0; i < 6; i++ {
		var txt any = "word" + strconv.Itoa(i)
		if i == 4 {
			txt = nil // a NULL survivor inside the filter
		}
		rows[i] = map[string]any{"id": int64(i), "txt": txt}
	}
	ing := db.NewIngester("docs", schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// WHERE forces a selection vector. Survivors: id 2,3,4,5 (4 rows; id4 is NULL).
	res, err := db.Query(ctx, "SELECT id, embed(txt) AS v FROM docs WHERE id >= 2 ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(res.Rows))
	}

	// The whole filtered batch must be embedded in ONE call (3 non-null texts),
	// not one call per row.
	if prov.calls != 1 {
		t.Errorf("provider.Embed called %d times under a filter, want 1 (batched)", prov.calls)
	}
	if prov.rows != 3 {
		t.Errorf("provider embedded %d texts, want 3 (NULL excluded)", prov.rows)
	}

	// Correct values + NULL passthrough survive the filtered/compacted path.
	for _, r := range res.Rows {
		if r["id"].(int64) == 4 {
			if r["v"] != nil {
				t.Errorf("embed(NULL) under filter = %v, want nil", r["v"])
			}
			continue
		}
		v, ok := r["v"].([]float32)
		if !ok || len(v) != 8 {
			t.Errorf("id=%v embed output %T len mismatch, want []float32 dim 8", r["id"], r["v"])
		}
	}
}

// TestEmbedSQLDimMismatch is the regression for the review finding that a
// provider returning a width different from its declared Dimension() silently
// corrupted output (truncation / stale pooled tail) while marking rows valid.
// The mismatched rows must come back NULL.
func TestEmbedSQLDimMismatch(t *testing.T) {
	ctx := context.Background()
	prov := &fixedDimProvider{reportDim: 8, returnDim: 4} // declares 8, returns 4
	embedding.SetProvider(prov)
	defer embedding.SetProvider(nil)
	embedding.RegisterFunctions()

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "txt", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "docs", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "txt": "alpha"},
		{"id": int64(2), "txt": "beta"},
	}
	ing := db.NewIngester("docs", schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, "SELECT id, embed(txt) AS v FROM docs ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for _, r := range res.Rows {
		if r["v"] != nil {
			t.Errorf("id=%v: width-mismatched embedding = %v, want nil (no corruption)", r["id"], r["v"])
		}
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
