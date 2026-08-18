package test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// BenchmarkShapeOnlyQ28 measures the ClickBench Q28 shape end to end —
// AVG(LENGTH(url)) ... WHERE url <> ” GROUP BY k over 1M rows of ~90-byte
// URLs — with the lengths-only scan decode off and on. Off is the full
// decode: every URL is dictionary-gathered and memcpy'd into the vector
// arena to have its length subtracted back out.
func BenchmarkShapeOnlyQ28(b *testing.B) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "url", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		b.Fatal(err)
	}
	const n = 1000000
	r := rand.New(rand.NewSource(28))
	buf := make([]byte, 0, 128)
	rows := make([]map[string]any, 0, 100000)
	ing := db.NewIngester("hits", schema, nil, ingest.Config{MaxBufferRows: 200000, RowGroupSize: 65536})
	for i := 0; i < n; i++ {
		l := 60 + r.Intn(40)
		buf = append(buf[:0], "https://example.test/"...)
		for j := 0; j < l; j++ {
			buf = append(buf, byte('a'+r.Intn(26)))
		}
		url := any(string(buf))
		if i%50 == 0 {
			url = "" // the `<> ''` conjunct has something to reject
		}
		rows = append(rows, map[string]any{"k": int64(i % 1000), "url": url})
		if len(rows) == cap(rows) {
			if err := ing.Ingest(ctx, rows); err != nil {
				b.Fatal(err)
			}
			rows = rows[:0]
		}
	}
	if len(rows) > 0 {
		if err := ing.Ingest(ctx, rows); err != nil {
			b.Fatal(err)
		}
	}
	if err := ing.FlushAll(ctx); err != nil {
		b.Fatal(err)
	}

	const q = "SELECT k, AVG(LENGTH(url)) AS l, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY k ORDER BY l DESC LIMIT 25"
	run := func(b *testing.B, on bool) {
		prev := scan.SetLengthsOnlyDecodeForTest(on)
		defer scan.SetLengthsOnlyDecodeForTest(prev)
		if _, err := db.Query(ctx, q); err != nil { // warm caches identically
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := db.Query(ctx, q)
			if err != nil {
				b.Fatal(err)
			}
			if len(res.Rows) != 25 {
				b.Fatalf("got %d rows, want 25", len(res.Rows))
			}
		}
	}
	b.Run("full-decode", func(b *testing.B) { run(b, false) })
	b.Run("lengths-only", func(b *testing.B) { run(b, true) })
}
