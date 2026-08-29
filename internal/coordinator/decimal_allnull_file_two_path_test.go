package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #685's review finding F1, end to end: a FILE whose DECIMAL column is entirely
// NULL must not change the scale of the aggregate over the other files.
//
// No filter, no selectivity, no identity row — this is a plain two-file table
// and `SELECT SUM(a) FROM t`. What makes it a morsel-parallel question is
// scanParallelism(): with two or more scan workers, exec runs one CLONE of the
// scalar aggregate per morsel and merges them. The clone that read the all-NULL
// file carried no DECIMAL value at all, so it had no scale to give — but
// kernel.Accumulator.Merge adopted its DecScale unconditionally and the
// primary's right Int128 rendered under it: 400.00 for 4.00, on the
// single-process engine and on the coordinator's in-process fast path (the
// PRODUCTION default, LocalFastPathBytes = DefaultLocalFastPathBytes), while
// the same query's stage DAG answered 4.00.
//
// Three arms, three worker counts. One worker never clones, which is why
// WADJET_SCAN_WORKERS=1 was correct and ≥2 was not — a shape no single-arm test
// and no default-parallelism test could separate.
const anfTable = "decnullfile"

func anfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
}

// anfFiles is two files' worth of rows: four 1.00 / 1.0000 rows, then four rows
// that are NULL in both DECIMAL columns.
func anfFiles() [][]map[string]any {
	valued := make([]map[string]any, 0, 4)
	for i := 1; i <= 4; i++ {
		valued = append(valued, map[string]any{
			"id": int64(i),
			"a":  parquet.Decimal128{Lo: 100},   // 1.00
			"b":  parquet.Decimal128{Lo: 10000}, // 1.0000
		})
	}
	nulls := make([]map[string]any, 0, 4)
	for i := 5; i <= 8; i++ {
		nulls = append(nulls, map[string]any{"id": int64(i), "a": nil, "b": nil})
	}
	return [][]map[string]any{valued, nulls}
}

// anfWant is what PostgreSQL 17.11 answers over the identical eight rows,
// verified live before the fix (`numeric(9,2) a`, `numeric(18,4) b`):
//
//	SUM(a) 4.00     MIN(a) 1.00     MAX(a) 1.00     COUNT(a) 4   SUM(DISTINCT a) 1.00
//	SUM(b) 4.0000   MIN(b) 1.0000   MAX(b) 1.0000   COUNT(b) 4   SUM(DISTINCT b) 1.0000
//
// AVG is left out of the exact table for ADR-0012 item 9's reason (PostgreSQL
// keeps more digits) and asserted at wadjet's own contract instead.
func anfWant(col string) map[string]string {
	if col == "a" {
		return map[string]string{"s": "4.00", "lo": "1.00", "hi": "1.00", "av": "1.000000"}
	}
	return map[string]string{"s": "4.0000", "lo": "1.0000", "hi": "1.0000", "av": "1.00000000"}
}

// anfScaleOf is the number of fractional digits a rendered DECIMAL carries.
// -1 for a value that is not decimal text at all.
func anfScaleOf(v any) int {
	s, ok := v.(string)
	if !ok {
		return -1
	}
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		return 0
	}
	return len(s) - dot - 1
}

func TestAllNullFileDoesNotRescaleTheAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	anfWriteTable(t, ctx, infra)
	// The DAG arm: LocalFastPathBytes = 0 forces every query onto the stages.
	dagCoord := tmdCoordinator(t, ctx, infra)
	// The FAST-PATH arm: the production default, which is what a real
	// deployment runs and what answered 400.00.
	fastCoord := New(Config{
		NATSUrl: infra.clientURL, ResultBucket: "test",
		LocalFastPathBytes: DefaultLocalFastPathBytes,
	}, infra.cat, infra.nc, infra.js, infra.logger)
	single := anfStandalone(t, ctx)

	for _, workers := range []string{"1", "2", "4"} {
		for _, col := range []string{"a", "b"} {
			want := anfWant(col)
			// SUM(DISTINCT) rides along for the SCALE question only — see the
			// assertion below.
			sql := fmt.Sprintf(
				"SELECT SUM(%[1]s) AS s, MIN(%[1]s) AS lo, MAX(%[1]s) AS hi, AVG(%[1]s) AS av, "+
					"SUM(DISTINCT %[1]s) AS sd, COUNT(%[1]s) AS n FROM %[2]s", col, anfTable)
			colScale := 2
			if col == "b" {
				colScale = 4
			}
			for _, arm := range []struct {
				name string
				run  func() ([]map[string]any, error)
			}{
				{"single", func() ([]map[string]any, error) {
					r, err := tmdRunSingle(ctx, single, sql)
					if err != nil {
						return nil, err
					}
					return r.Rows, nil
				}},
				{"coordinator fast path", func() ([]map[string]any, error) {
					r, err := tmdRunDAG(ctx, fastCoord, sql)
					if err != nil {
						return nil, err
					}
					return r.Rows, nil
				}},
				{"stage DAG", func() ([]map[string]any, error) {
					r, err := tmdRunDAG(ctx, dagCoord, sql)
					if err != nil {
						return nil, err
					}
					return r.Rows, nil
				}},
			} {
				t.Run(fmt.Sprintf("%s_workers%s_%s", col, workers, arm.name), func(t *testing.T) {
					t.Setenv("WADJET_SCAN_WORKERS", workers)
					// Repeated, because the defect is ORDER-DEPENDENT: which
					// morsel the all-NULL file lands in and which clone the
					// primary merges first decide whether the wrong scale is
					// the one that survives. Measured on the pre-fix tree, the
					// coordinator fast path failed 3 runs out of 3 at two or
					// more workers while the embedded arm failed 1 in 9 — a
					// single attempt per arm would be a gate that passes for
					// the wrong reason most of the time it is green.
					const attempts = 8
					for attempt := 0; attempt < attempts; attempt++ {
						anfCheckOnce(t, arm.name, sql, want, colScale, workers, arm.run)
						if t.Failed() {
							return
						}
					}
				})
			}
		}
	}
}

// anfCheckOnce runs the query on one arm and holds every cell to its answer.
func anfCheckOnce(t *testing.T, arm, sql string, want map[string]string, colScale int,
	workers string, run func() ([]map[string]any, error),
) {
	t.Helper()
	rows, err := run()
	if err != nil {
		t.Fatalf("%s refused %q: %v", arm, sql, err)
	}
	if len(rows) != 1 {
		t.Fatalf("%s: %d rows, want 1", arm, len(rows))
	}
	r := rows[0]
	for _, k := range []string{"s", "lo", "hi", "av"} {
		got, _ := r[k].(string)
		if got != want[k] {
			t.Errorf("%s %s = %#v, want %q — a file whose DECIMAL column is all NULL changed "+
				"the scale of the aggregate over the other file (#685 review, F1; scan workers = %s)",
				arm, k, r[k], want[k], workers)
		}
	}
	// SUM(DISTINCT) is asserted on its SCALE and not its VALUE. DISTINCT
	// reaches only COUNT in this engine — physical's walkStages and
	// buildAggregate both rewrite the function for `count` alone — so
	// SUM(DISTINCT a) is planned as SUM(a) and answers 4.00 where PostgreSQL
	// answers 1.00. That is a pre-existing defect of its own, present at this
	// commit's parent and untouched by it; pinning the wrong value here would
	// make this gate a record of it, and dropping the column would lose the arm
	// F1's reach actually includes. The SCALE is the part that belongs to #685,
	// and a 10^scale factor moves it.
	if got := anfScaleOf(r["sd"]); got != colScale {
		t.Errorf("%s SUM(DISTINCT) = %#v renders %d fractional digits, want %d (scan workers = %s)",
			arm, r["sd"], got, colScale, workers)
	}
	if n := toInt64(r["n"]); n != 4 {
		t.Errorf("%s COUNT = %v, want 4", arm, r["n"])
	}
}

// anfWriteTable writes the fixture as exactly two parquet files.
func anfWriteTable(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	schema := anfSchema()
	if err := infra.cat.CreateTable(ctx, anfTable, schema, nil); err != nil {
		t.Fatalf("create %s: %v", anfTable, err)
	}
	var entries []catalog.FileEntry
	for c, rows := range anfFiles() {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", anfTable, c)
		payload := buf.Bytes()
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(payload)),
			NumRows: int64(len(rows)), CreatedAt: time.Now(),
		})
	}
	if len(entries) != 2 {
		t.Fatalf("the fixture wrote %d files, not 2", len(entries))
	}
	if err := infra.cat.AddFiles(ctx, anfTable, map[string]string{}, "tables/"+anfTable+"/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}

// anfStandalone is the embedded single-process arm over the same two files.
// The ingester is flushed BETWEEN the two row sets so the all-NULL rows land in
// a file of their own — one file would give the scan one morsel and hide the
// merge this gate is about.
func anfStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := anfSchema()
	if err := db.CreateTable(ctx, anfTable, schema, nil); err != nil {
		t.Fatalf("create %s: %v", anfTable, err)
	}
	for _, rows := range anfFiles() {
		ing := db.NewIngester(anfTable, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1, RowGroupSize: 2,
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	return db
}
