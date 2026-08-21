package wadjet

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Algebraic const-arith aggregate rewrite (SUM(x+k) → SUM(x)+k*COUNT(x)
// etc., ClickBench Q30). Values verified against directly computed
// expectations, including NULL rows — the rewrite must preserve
// aggregate-over-non-null semantics via COUNT(x), not COUNT(*).
func TestConstArithAggRewrite(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
		{Name: "g", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	// v = 1..8 with two NULLs; sum(non-null)=1+2+4+5+7+8=27, count=6,
	// min=1, max=8, avg=4.5
	rows := []map[string]any{
		{"v": int64(1), "g": int64(0)}, {"v": int64(2), "g": int64(0)},
		{"g": int64(0)}, {"v": int64(4), "g": int64(0)},
		{"v": int64(5), "g": int64(0)}, {"g": int64(0)},
		{"v": int64(7), "g": int64(0)}, {"v": int64(8), "g": int64(0)},
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	q := `SELECT SUM(v + 10) AS a, SUM(v - 3) AS b, SUM(2 * v) AS c,
	             SUM(v / 2) AS d, SUM(v / 2.0) AS d2, AVG(v + 1) AS e, MIN(v + 5) AS f,
	             MAX(v - 1) AS g2, MIN(10 - v) AS h, SUM(10 - v) AS i,
	             COUNT(*) AS n FROM t`
	res, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows: %v", res.Rows)
	}
	r := res.Rows[0]
	want := map[string]float64{
		"a": 27 + 10*6, // 87
		"b": 27 - 3*6,  // 9
		"c": 54,
		// v / 2 is INTEGER division per row (#369, PostgreSQL semantics):
		// 0+1+2+2+3+4. The SUM(x/k)→SUM(x)/k rewrite must therefore NOT
		// fire for an integer divisor — 27/2 would answer 13 — which is
		// exactly what d2, the float-divisor control, still exercises.
		"d":  12,
		"d2": 13.5,
		"e":  5.5,
		"f":  6,
		"g2": 7,
		"h":  2,  // 10 - max(8)
		"i":  33, // 10*6 - 27
		"n":  8,
	}
	for k, w := range want {
		got, ok := toF(r[k])
		if !ok || math.Abs(got-w) > 1e-9 {
			t.Errorf("%s: got %v (%T), want %v (all: %v)", k, r[k], r[k], w, r)
		}
	}
}

func toF(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	}
	return 0, false
}
