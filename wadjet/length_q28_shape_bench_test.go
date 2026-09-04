package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkLengthOnTheQ28Shape is what #856 costs a QUERY, as opposed to what
// it costs the kernel.
//
// `expr.BenchmarkLengthCharactersVsOffsets` measures the kernel: counting
// characters reads the bytes where counting them from the offsets array did
// not, and the reviewer reproduced 6.2x there. A kernel ratio is not a query
// ratio — ClickBench Q28 also scans, filters, hashes a group key, aggregates
// and sorts — so the number that decides whether this is a problem is this one.
//
// The two arms are the same query over the same generated table:
//
//	AVG(LENGTH(url))         the character count LENGTH answers now
//	AVG(OCTET_LENGTH(url))   the byte count it answered before, which still
//	                         takes logical.shapeLenFuncs' offsets-only decode
//
// OCTET_LENGTH is the honest control: it is not "LENGTH with the fix removed",
// it IS the path LENGTH took before #856 — the same shape-only rewrite, the
// same ColShapeLen node — so the delta is the decode the character count adds
// and nothing else.
//
// The table is Q28's shape rather than its data: one int32 group key with
// enough cardinality to make the aggregate real, and a URL column whose length
// distribution is ClickBench-like (a fixed host plus a variable path, 30-120
// bytes). ASCII, as hits' URLs are — which is the case that pays the most,
// because a rune count over ASCII does the most work per character it does not
// need to.
//
// Measured 2026-09-04, 500k rows, -benchtime 10x -count 3, AMD Ryzen 9 5900X:
//
//	                 ns/op (3 runs)              B/op
//	LENGTH           197.4M  150.3M  200.9M      374.2M  374.7M  373.9M
//	OCTET_LENGTH     187.8M  197.2M  211.7M      318.6M  319.0M  318.6M
//
// The ALLOCATION delta is the signal and it is stable to three digits across
// all six runs: +17.5%, and it is the whole cost. It is not the counting —
// utf8.RuneCount over the offsets-addressed byte slice allocates nothing — it
// is the payload DECODE that counting forces, which the shape-only rewrite
// used to skip entirely for this column.
//
// The WALL samples overlap in both directions and no ratio can be read off
// them: this machine was running six concurrent arcs, and the LENGTH arm's
// fastest run (150.3M) is faster than every OCTET_LENGTH run. Whatever the
// query-level cost is, it is smaller than that contention and nowhere near the
// 6.2x the kernel benchmark shows — a kernel 6x slower inside a query that
// also scans, filters, hashes, aggregates and sorts moves the query by the
// fraction of it that kernel was.
//
// Run it with:
//
//	go test -run '^$' -bench BenchmarkLengthOnTheQ28Shape -benchtime 10x ./wadjet/
func BenchmarkLengthOnTheQ28Shape(b *testing.B) {
	const rows = 500_000
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "counterid", Type: parquet.TypeInt32},
		{Name: "url", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "q28shape", schema, nil); err != nil {
		b.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("q28shape", schema, nil,
		ingest.Config{MaxBufferRows: 100_000, RowGroupSize: 100_000})
	batchRows := make([]map[string]any, 0, 50_000)
	for i := 0; i < rows; i++ {
		// 30-120 byte URLs over 16 group keys: the HAVING COUNT(*) > … clause
		// in Q28 keeps only the heavy groups, so the aggregate must actually
		// run over every row.
		path := fmt.Sprintf("/track/%d/%s", i, urlFiller[:30+(i%91)])
		batchRows = append(batchRows, map[string]any{
			"counterid": int32(i % 16),
			"url":       "http://example.com" + path,
		})
		if len(batchRows) == cap(batchRows) {
			if err := ing.Ingest(ctx, batchRows); err != nil {
				b.Fatalf("ingest: %v", err)
			}
			batchRows = batchRows[:0]
		}
	}
	if len(batchRows) > 0 {
		if err := ing.Ingest(ctx, batchRows); err != nil {
			b.Fatalf("ingest: %v", err)
		}
	}
	if err := ing.FlushAll(ctx); err != nil {
		b.Fatalf("flush: %v", err)
	}

	const shape = `SELECT counterid, AVG(%s(url)) AS l, COUNT(*) AS c ` +
		`FROM q28shape WHERE url <> '' GROUP BY counterid ` +
		`HAVING COUNT(*) > 1000 ORDER BY l DESC LIMIT 25`
	for _, fn := range []string{"LENGTH", "OCTET_LENGTH"} {
		sql := fmt.Sprintf(shape, fn)
		b.Run(fn, func(b *testing.B) {
			// One warm run outside the timer: the first query pays the
			// manifest read and the page cache fill, which is not what this
			// measures.
			if _, err := db.Query(ctx, sql); err != nil {
				b.Fatalf("%v\n  SQL: %s", err, sql)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := db.Query(ctx, sql)
				if err != nil {
					b.Fatalf("%v", err)
				}
				if len(res.Rows) != 16 {
					b.Fatalf("%d groups, want 16", len(res.Rows))
				}
			}
		})
	}
}

// urlFiller is 120 bytes of ASCII path, sliced to vary the URL length.
const urlFiller = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz" +
	"0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmno"
