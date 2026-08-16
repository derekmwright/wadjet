package clickbench

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// loadHitsQueries reads the 43 ClickBench queries (one per line).
func loadHitsQueries(tb testing.TB) []string {
	tb.Helper()
	f, err := os.Open("queries.sql")
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	var queries []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if q := sc.Text(); q != "" {
			queries = append(queries, q)
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatal(err)
	}
	return queries
}

// openHitsDB stands up an embedded wadjet.DB with the hits table registered
// over a real ClickBench part (WADJET_HITS_PART) in a MemStore. The catalog
// schema is the file's probed schema with BYTE_ARRAY columns registered as
// String (the athena_partitioned parquet carries no UTF8 annotation, but
// every byte-array column in hits is text and the queries compare them to
// string literals).
func openHitsDB(tb testing.TB, ctx context.Context) (*wadjet.DB, int64) {
	tb.Helper()
	path := os.Getenv("WADJET_HITS_PART")
	if path == "" {
		tb.Skip("WADJET_HITS_PART not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	r, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatalf("probe schema: %v", err)
	}
	schema := r.Schema()
	for i := range schema.Columns {
		if schema.Columns[i].Type == parquet.TypeBytes {
			schema.Columns[i].Type = parquet.TypeString
		}
		// EventDate is INT16 days-since-epoch on disk; register it as DATE
		// (int32-days storage, same storage class) so date-literal predicates
		// and MIN/MAX behave like the official DuckDB view's make_date()
		// rewrite. EventTime stays INT64 epoch-seconds: wadjet's temporal
		// scalar functions take int64 seconds directly (the toDateTime macro
		// equivalent), see queries.sql vs queries-duckdb.sql.
		if schema.Columns[i].Name == "EventDate" {
			schema.Columns[i].Type = parquet.TypeDate
		}
	}

	store := objstore.NewMemStore()
	const bucket = "clickbench"
	key := "tables/hits/hits_0.parquet"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		tb.Fatalf("make bucket: %v", err)
	}
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		tb.Fatalf("stage part: %v", err)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: bucket})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { db.Close() })

	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		tb.Fatalf("create table: %v", err)
	}
	if err := db.Catalog().AddFiles(ctx, "hits", nil, "", []catalog.FileEntry{{
		Path:      key,
		SizeBytes: int64(len(data)),
		NumRows:   r.NumRows(),
		CreatedAt: time.Now(),
	}}); err != nil {
		tb.Fatalf("register file: %v", err)
	}
	return db, r.NumRows()
}

// TestHitsQueryExecProbe is the execution-coverage probe for the ClickBench
// arc: run all 43 queries end-to-end over a real hits part and enumerate
// which execute. Like the parse gate before it, this is the map of what to
// fix — it fails only on the floor (COUNT(*) must work), and logs the rest.
func TestHitsQueryExecProbe(t *testing.T) {
	ctx := context.Background()
	db, wantRows := openHitsDB(t, ctx)

	res, err := db.Query(ctx, "SELECT COUNT(*) FROM hits")
	if err != nil {
		t.Fatalf("COUNT(*): %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("COUNT(*): %d rows", len(res.Rows))
	}
	var got int64
	for _, v := range res.Rows[0] {
		if n, ok := v.(int64); ok {
			got = n
		}
	}
	if got != wantRows {
		t.Fatalf("COUNT(*) = %d, footer says %d", got, wantRows)
	}
	t.Logf("COUNT(*) = %d ✓", got)

	queries := loadHitsQueries(t)
	ok := 0
	for i, q := range queries {
		qctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		res, err := db.Query(qctx, q)
		cancel()
		if err != nil {
			short := q
			if len(short) > 90 {
				short = short[:90] + "…"
			}
			t.Logf("Q%02d FAIL: %v\n      %s", i+1, err, short)
			continue
		}
		ok++
		t.Logf("Q%02d ok (%d rows)", i+1, len(res.Rows))
	}
	t.Logf("execution coverage: %d/%d", ok, len(queries))
	if ok == 0 {
		t.Fatal("no queries executed")
	}
	_ = fmt.Sprint()
}
