package test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// metaMinMaxToggle returns the registered kill switch for the
// statistics-answered MIN/MAX optimization (WADJET_META_MINMAX).
func metaMinMaxToggle(tb testing.TB) *optswitch.Toggle {
	tb.Helper()
	for _, t := range optswitch.All() {
		if t.Name == "meta-minmax" {
			return t
		}
	}
	tb.Fatal("meta-minmax toggle is not registered")
	return nil
}

// mmFixture builds the tables the MIN/MAX metadata tests share:
//
//	events — 2 files x 3 row groups, every supported type plus a string
//	         column, an all-NULL column, and a column whose first row group
//	         is entirely NULL
//	empty  — created, never written
func mmFixture(tb testing.TB) (*wadjet.DB, context.Context) {
	tb.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "i32", Type: parquet.TypeInt32},
		{Name: "i64", Type: parquet.TypeInt64},
		{Name: "f64", Type: parquet.TypeFloat64},
		{Name: "f32", Type: parquet.TypeFloat32},
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "s", Type: parquet.TypeString},
		{Name: "allnull", Type: parquet.TypeInt64, Nullable: true},
		{Name: "sparse", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		tb.Fatalf("create events: %v", err)
	}
	if err := db.CreateTable(ctx, "empty", schema, nil); err != nil {
		tb.Fatalf("create empty: %v", err)
	}

	// Two files of six rows each, row groups of two: file 0 carries values
	// 1..6, file 1 carries 11..16. "sparse" is NULL for the whole first row
	// group of each file (values 1,2 and 11,12) so the fold has to skip
	// all-NULL row groups and still find 3 as the minimum.
	for f := 0; f < 2; f++ {
		rows := make([]map[string]any, 0, 6)
		for i := 1; i <= 6; i++ {
			n := int64(i + f*10)
			row := map[string]any{
				"i32":     int32(n),
				"i64":     n,
				"f64":     float64(n) + 0.5,
				"f32":     float32(n) + 0.25,
				"d":       fmt.Sprintf("2026-03-%02d", n),
				"ts":      n * 1000,
				"s":       fmt.Sprintf("v%02d", n),
				"allnull": nil,
				"sparse":  n,
			}
			if i <= 2 {
				row["sparse"] = nil
			}
			rows = append(rows, row)
		}
		ing := db.NewIngester("events", schema, nil, ingest.Config{MaxBufferRows: 1000, RowGroupSize: 2})
		if err := ing.Ingest(ctx, rows); err != nil {
			tb.Fatalf("ingest file %d: %v", f, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			tb.Fatalf("flush file %d: %v", f, err)
		}
	}
	return db, ctx
}

// mmRun executes sql with the optimization forced to `on` and returns the
// result together with how many plans took the metadata path.
func mmRun(tb testing.TB, db *wadjet.DB, ctx context.Context, sql string, on bool) (*wadjet.QueryResult, int64) {
	tb.Helper()
	tg := metaMinMaxToggle(tb)
	prev := tg.Set(on)
	defer tg.Set(prev)

	before := physical.MetadataMinMaxPlanned.Load()
	res, err := db.Query(ctx, sql)
	if err != nil {
		tb.Fatalf("query %q (meta-minmax=%v): %v", sql, on, err)
	}
	return res, physical.MetadataMinMaxPlanned.Load() - before
}

// TestMetadataMinMax is the invariance gate for the statistics-answered
// MIN/MAX path: every query must return exactly what the scan returns, and
// the optimization must engage only on the shapes it is proven safe for.
func TestMetadataMinMax(t *testing.T) {
	db, ctx := mmFixture(t)

	tests := []struct {
		name     string
		sql      string
		wantFire bool
		wantRows []map[string]any
	}{
		{
			name: "int64 min/max across files and row groups", wantFire: true,
			sql:      "SELECT MIN(i64) AS lo, MAX(i64) AS hi FROM events",
			wantRows: []map[string]any{{"lo": int64(1), "hi": int64(16)}},
		},
		{
			name: "int32 widens to int64", wantFire: true,
			sql:      "SELECT MIN(i32) AS lo, MAX(i32) AS hi FROM events",
			wantRows: []map[string]any{{"lo": int64(1), "hi": int64(16)}},
		},
		{
			name: "date renders as a date, not epoch days", wantFire: true,
			sql:      "SELECT MIN(d) AS lo, MAX(d) AS hi FROM events",
			wantRows: []map[string]any{{"lo": "2026-03-01", "hi": "2026-03-16"}},
		},
		{
			name: "timestamp", wantFire: true,
			sql:      "SELECT MIN(ts) AS lo, MAX(ts) AS hi FROM events",
			wantRows: []map[string]any{{"lo": int64(1000), "hi": int64(16000)}},
		},
		{
			name: "float64", wantFire: true,
			sql:      "SELECT MIN(f64) AS lo, MAX(f64) AS hi FROM events",
			wantRows: []map[string]any{{"lo": 1.5, "hi": 16.5}},
		},
		{
			// REAL in, REAL out -- `pg_typeof(min(real))` is real (#760). The
			// statistics path and the SCAN path have to agree, which is what
			// physical.mmTypeFor's own doc says and what this cell is here to
			// hold: with only one of them moved, the same MIN answered
			// `double precision` or `real` depending on whether a WHERE
			// clause sent the query down the other one.
			name: "float32 stays float32", wantFire: true,
			sql:      "SELECT MIN(f32) AS lo, MAX(f32) AS hi FROM events",
			wantRows: []map[string]any{{"lo": float32(1.25), "hi": float32(16.25)}},
		},
		{
			name: "mixed columns plus COUNT(*)", wantFire: true,
			sql:      "SELECT MIN(i64) AS lo, MAX(d) AS hi, MIN(f64) AS flo, COUNT(*) AS n FROM events",
			wantRows: []map[string]any{{"lo": int64(1), "hi": "2026-03-16", "flo": 1.5, "n": int64(12)}},
		},
		{
			name: "repeated column reuses one accumulator", wantFire: true,
			sql:      "SELECT MIN(i64) AS lo, MAX(i64) AS hi, MIN(i64) AS lo2 FROM events",
			wantRows: []map[string]any{{"lo": int64(1), "hi": int64(16), "lo2": int64(1)}},
		},
		{
			name: "unaliased output keeps the scan's column names", wantFire: true,
			sql:      "SELECT MIN(i64), MAX(d) FROM events",
			wantRows: []map[string]any{{"min(i64)": int64(1), "max(d)": "2026-03-16"}},
		},
		{
			name: "table-qualified column reference", wantFire: true,
			sql:      "SELECT MIN(e.i64) AS lo FROM events e",
			wantRows: []map[string]any{{"lo": int64(1)}},
		},
		{
			name: "all-NULL column is NULL, not zero", wantFire: true,
			sql:      "SELECT MIN(allnull) AS lo, MAX(allnull) AS hi FROM events",
			wantRows: []map[string]any{{"lo": nil, "hi": nil}},
		},
		{
			name: "all-NULL row groups are skipped", wantFire: true,
			sql:      "SELECT MIN(sparse) AS lo, MAX(sparse) AS hi FROM events",
			wantRows: []map[string]any{{"lo": int64(3), "hi": int64(16)}},
		},
		{
			name: "empty table yields NULL", wantFire: true,
			sql:      "SELECT MIN(i64) AS lo, MAX(d) AS hi FROM empty",
			wantRows: []map[string]any{{"lo": nil, "hi": nil}},
		},
		{
			name: "empty table with COUNT(*)", wantFire: true,
			sql:      "SELECT MIN(i64) AS lo, COUNT(*) AS n FROM empty",
			wantRows: []map[string]any{{"lo": nil, "n": int64(0)}},
		},
		// --- shapes that must NOT engage ---
		{
			name: "string statistics may be truncated: scan", wantFire: false,
			sql:      "SELECT MIN(s) AS lo, MAX(s) AS hi FROM events",
			wantRows: []map[string]any{{"lo": "v01", "hi": "v16"}},
		},
		{
			name: "one unsupported column declines the whole query", wantFire: false,
			sql:      "SELECT MIN(i64) AS lo, MIN(s) AS slo FROM events",
			wantRows: []map[string]any{{"lo": int64(1), "slo": "v01"}},
		},
		{
			name: "WHERE present", wantFire: false,
			sql:      "SELECT MIN(i64) AS lo, MAX(i64) AS hi FROM events WHERE i64 > 3",
			wantRows: []map[string]any{{"lo": int64(4), "hi": int64(16)}},
		},
		{
			name: "GROUP BY present", wantFire: false,
			sql: "SELECT i32 AS k, MIN(i64) AS lo FROM events WHERE i32 < 3 GROUP BY i32 ORDER BY k",
			wantRows: []map[string]any{
				{"k": int32(1), "lo": int64(1)},
				{"k": int32(2), "lo": int64(2)},
			},
		},
		{
			name: "aggregate over a non-liftable expression", wantFire: false,
			sql:      "SELECT MIN(i64 * f64) AS lo FROM events",
			wantRows: []map[string]any{{"lo": 1.5}},
		},
		{
			// The metadata path fires here AGAIN, and the rows are unchanged.
			//
			// This cell recorded a SECOND cost of #841's decline: the
			// syntactic `rewriteConstArithAggs` used to turn MIN(x + k) into
			// MIN(x) + k before the physical planner saw it, so the aggregate
			// WAS a bare column by then and the manifest statistics answered
			// it without a scan. #841 declined the lift for an integer literal
			// — the per-row form can raise 22003 and the lifted form cannot —
			// so the argument stopped being a bare column and the query
			// scanned.
			//
			// #850 restored the lift on the terms this cell's own text
			// predicted: the TYPED pass runs after annotation, reads the
			// column's declared type and its statistics, and lifts
			// `agg(col op const)` only when the extremes prove the per-row
			// form cannot refuse (|max| + |k| inside the carrier). The
			// argument is a bare column by the time the physical planner
			// looks, so the statistics answer it, exactly as before #841.
			//
			// wantFire is therefore TRUE, and it is the "answered from
			// metadata" question it always was — the counter is
			// physical.MetadataMinMaxPlanned, which counts the path being
			// PLANNED for the query, not a statistics READ. The lift's own
			// bound reads the manifest in the LOGICAL planner and never
			// touches this counter. The two arms below are what makes the
			// claim safe: the kill-switch-off run must produce the identical
			// rows from a real scan.
			//
			// PostgreSQL 17.11: `SELECT pg_typeof(MIN(v + 1)) FROM t` with v
			// bigint is bigint, and MIN(i64 + 1) over this fixture is 2.
			name: "constant arithmetic is lifted out of the aggregate again", wantFire: true,
			// bigint on PostgreSQL 17: `SELECT pg_typeof(MIN(v + 1)) FROM t`
			// with v bigint answers bigint. This wanted float64 while the
			// integer rule reached only the outermost node of a projection,
			// so the identical expression declared INT64 as `SELECT i64 + 1`
			// and FLOAT64 as this aggregate's input.
			sql:      "SELECT MIN(i64 + 1) AS lo FROM events",
			wantRows: []map[string]any{{"lo": int64(2)}},
		},
		{
			name: "COUNT(col) alongside MIN", wantFire: false,
			sql:      "SELECT MIN(i64) AS lo, COUNT(sparse) AS n FROM events",
			wantRows: []map[string]any{{"lo": int64(1), "n": int64(8)}},
		},
		{
			name: "bare COUNT(*) stays on the manifest path", wantFire: false,
			sql:      "SELECT COUNT(*) AS n FROM events",
			wantRows: []map[string]any{{"n": int64(12)}},
		},
		{
			name: "SUM is not a statistics aggregate", wantFire: false,
			sql: "SELECT SUM(i64) AS s, MIN(i64) AS lo FROM events",
			// SUM over an INT64 column is PostgreSQL's `numeric` since #784, so
			// it boxes as the DECIMAL text "102". What this entry asserts is
			// that the metadata path does NOT answer a SUM from statistics;
			// which carrier the scan's answer arrives in is a different gate's
			// question.
			wantRows: []map[string]any{{"s": "102", "lo": int64(1)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fired := mmRun(t, db, ctx, tt.sql, true)
			if (fired > 0) != tt.wantFire {
				t.Fatalf("metadata path fired %d times, want fired=%v", fired, tt.wantFire)
			}
			if !reflect.DeepEqual(got.Rows, tt.wantRows) {
				t.Fatalf("rows = %#v, want %#v", got.Rows, tt.wantRows)
			}

			// Kill switch off: the scan must produce the identical answer.
			scanned, firedOff := mmRun(t, db, ctx, tt.sql, false)
			if firedOff != 0 {
				t.Fatalf("metadata path fired %d times with the kill switch off", firedOff)
			}
			if !reflect.DeepEqual(got.Columns, scanned.Columns) {
				t.Errorf("columns diverge: metadata %v, scan %v", got.Columns, scanned.Columns)
			}
			if !reflect.DeepEqual(got.Rows, scanned.Rows) {
				t.Errorf("rows diverge:\n  metadata %#v\n  scan     %#v", got.Rows, scanned.Rows)
			}
		})
	}
}

// TestMetadataMinMaxDeleteMarkers: a merge-on-read delete can remove the very
// row holding an extreme, and the footer cannot know that — the optimization
// must stand down while delete markers exist.
func TestMetadataMinMaxDeleteMarkers(t *testing.T) {
	db, ctx := mmFixture(t)

	if _, fired := mmRun(t, db, ctx, "SELECT MIN(i64) AS lo FROM events", true); fired == 0 {
		t.Fatal("expected the metadata path before any delete")
	}
	if _, err := db.Query(ctx, "DELETE FROM events WHERE i64 = 1"); err != nil {
		t.Skipf("DELETE unsupported in this build: %v", err)
	}
	res, fired := mmRun(t, db, ctx, "SELECT MIN(i64) AS lo FROM events", true)
	if fired != 0 {
		t.Error("metadata path engaged despite delete markers")
	}
	if got := res.Rows[0]["lo"]; got != int64(2) {
		t.Errorf("min after delete = %#v, want 2", got)
	}
}
