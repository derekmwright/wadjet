package expr

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// q29Referers builds a ClickBench-shaped Referer corpus: whole URL strings
// drawn from a skewed pool, sized so a 2048-row batch holds roughly the 3x
// intra-batch duplication the real column shows (the property the per-batch
// memo exists to exploit), plus the empty and non-URL rows the column also
// carries.
func q29Referers(n int) []string {
	r := rand.New(rand.NewSource(29))
	paths := []string{
		"/search?query=analytics+engine&page=%d",
		"/catalog/item/%d/reviews",
		"/",
		"/a/rather/deeply/nested/path/segment/%d/index.html?utm_source=x&utm_medium=y",
	}
	pool := make([]string, 0, 896)
	for i := range 896 {
		host := fmt.Sprintf("www.host%d.example%d.com", i%320, i%17)
		pool = append(pool, fmt.Sprintf("http://"+host+paths[i%len(paths)], i))
	}
	out := make([]string, n)
	for i := range out {
		switch v := r.Intn(64); {
		case v == 0:
			out[i] = "" // filtered in the real query, still exercises the path
		case v == 1:
			out[i] = "not-a-url-at-all"
		default:
			// Squaring the uniform draw skews toward the head of the pool.
			f := r.Float64()
			out[i] = pool[int(f*f*float64(len(pool)))]
		}
	}
	return out
}

func q29Batch(vals []string) *batch.RecordBatch {
	schema := []parquet.Column{{Name: "Referer", Type: parquet.TypeString}}
	b := batch.NewRecordBatch(schema, len(vals))
	for i, v := range vals {
		b.Columns[0].BytesData.Set(i, []byte(v))
	}
	b.Len = len(vals)
	return b
}

func q29Expr() *FuncCall {
	return &FuncCall{Name: "regexp_replace", Args: []Expr{
		&ColRef{Name: "Referer"},
		&Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`},
		&Lit{Val: `\1`},
	}}
}

// BenchmarkQ29GroupKey1M evaluates ClickBench Q29's GROUP BY key
// expression over 1M rows in engine-sized (2048-row) batches — the
// per-batch memo path with the prepared regexp, exactly as the aggregate's
// pre-projection drives it. ns/op and B/op are therefore per million rows.
func BenchmarkQ29GroupKey1M(b *testing.B) {
	const rows = 1 << 20
	const batchSize = batch.DefaultBatchSize
	vals := q29Referers(rows)
	batches := make([]*batch.RecordBatch, 0, rows/batchSize)
	for off := 0; off < rows; off += batchSize {
		batches = append(batches, q29Batch(vals[off:off+batchSize]))
	}
	e := q29Expr()
	out := batch.NewVector(batch.TypeString, batchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, in := range batches {
			out.BytesData.Reset()
			e.EvalVec(in, out, in.Len)
		}
	}
}

// BenchmarkQ29GroupKeyBatch is the same work for a single batch, for
// profile runs that want a tight loop.
func BenchmarkQ29GroupKeyBatch(b *testing.B) {
	in := q29Batch(q29Referers(batch.DefaultBatchSize))
	e := q29Expr()
	out := batch.NewVector(batch.TypeString, batch.DefaultBatchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out.BytesData.Reset()
		e.EvalVec(in, out, in.Len)
	}
}
