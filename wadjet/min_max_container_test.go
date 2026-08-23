package wadjet

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #426: MIN() and MAX() over ARRAY, ROW, MAP and VECTOR answered NULL, on
// every input, silently — the six resolvers in kernel/agg.go fill an
// Accumulator slot and a container fits none of them, so the row updater was
// never called and HasMin/HasMax stayed false.
//
// The engine already had the order (#415: kernel.CompareValuesAt, which is
// what ORDER BY, PARTITION BY and a sort-merge join on a container key use),
// and PostgreSQL does order arrays — min(anyarray)/max(anyarray) exist and
// use the same lexicographic array_cmp. So this was a gap, not a position.
//
// Two assertions per case, deliberately:
//
//   - the ABSOLUTE value the documented order produces. A differential test
//     against ORDER BY alone would pass if the comparator and the aggregate
//     broke together, which is exactly how #415 hid.
//   - agreement with the engine's own ORDER BY, because "MIN(col) is the
//     first row of ORDER BY col" is the property the fix claims, and it is
//     what keeps the aggregate and the sort from drifting apart later.

// mmcQueryOne runs a single-row query and returns the named column.
func mmcQueryOne(t *testing.T, db *DB, sql, col string) any {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s: %d rows, want 1", sql, len(res.Rows))
	}
	return res.Rows[0][col]
}

func TestMinMaxOverContainers(t *testing.T) {
	db := coOpen(t)

	cases := []struct {
		col     string
		wantMin any
		wantMax any
	}{
		// ARRAY: lexicographic, shorter-is-less on a prefix tie. The EMPTY
		// array is a VALUE and the least one — it must not be confused with
		// the NULL row, which MIN/MAX ignore.
		{"c_arr", []any{}, []any{"b"}},
		// ROW: field-wise in declaration order, a NULL field after a
		// non-NULL one. a="x" beats a="y"; among the three a="x" rows,
		// b=1 < b=2 < b=NULL.
		{"c_row",
			map[string]any{"a": "x", "b": int64(1)},
			map[string]any{"a": "y", "b": int64(1)}},
		// MAP: entry-wise over key-sorted entries, so the EMPTY map is least
		// and {b:1} — whose first key is the largest — is greatest.
		{"c_map",
			[]any{},
			[]any{map[string]any{"key": "b", "value": int64(1)}}},
		// VECTOR: the ARRAY rule over the fixed-width layout.
		{"c_vec", []float32{-1, 9}, []float32{1, 0}},
	}

	for _, tc := range cases {
		t.Run(tc.col, func(t *testing.T) {
			gotMin := mmcQueryOne(t,
				db, fmt.Sprintf("SELECT MIN(%s) AS v FROM %s", tc.col, coTable), "v")
			if !reflect.DeepEqual(gotMin, tc.wantMin) {
				t.Errorf("MIN(%s) = %#v, want %#v", tc.col, gotMin, tc.wantMin)
			}
			gotMax := mmcQueryOne(t,
				db, fmt.Sprintf("SELECT MAX(%s) AS v FROM %s", tc.col, coTable), "v")
			if !reflect.DeepEqual(gotMax, tc.wantMax) {
				t.Errorf("MAX(%s) = %#v, want %#v", tc.col, gotMax, tc.wantMax)
			}

			// The engine's own ORDER BY is the second opinion. NULLs are
			// excluded explicitly: MIN/MAX ignore them, and DESC puts them
			// first here.
			orderMin := mmcQueryOne(t, db, fmt.Sprintf(
				"SELECT %s AS v FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT 1",
				tc.col, coTable, tc.col, tc.col), "v")
			if !reflect.DeepEqual(gotMin, orderMin) {
				t.Errorf("MIN(%s) = %#v but ORDER BY %s LIMIT 1 = %#v",
					tc.col, gotMin, tc.col, orderMin)
			}
			orderMax := mmcQueryOne(t, db, fmt.Sprintf(
				"SELECT %s AS v FROM %s WHERE %s IS NOT NULL ORDER BY %s DESC LIMIT 1",
				tc.col, coTable, tc.col, tc.col), "v")
			if !reflect.DeepEqual(gotMax, orderMax) {
				t.Errorf("MAX(%s) = %#v but ORDER BY %s DESC LIMIT 1 = %#v",
					tc.col, gotMax, tc.col, orderMax)
			}
		})
	}
}

// TestMinMaxOverContainersEmptyInput: an ungrouped aggregate over zero rows
// still emits one row, and MIN/MAX of nothing is NULL. The retained-value
// state has no accumulator identity to fall back on, so this is the arm that
// would surface an empty container instead.
func TestMinMaxOverContainersEmptyInput(t *testing.T) {
	db := coOpen(t)
	for _, col := range []string{"c_arr", "c_row", "c_map", "c_vec"} {
		sql := fmt.Sprintf("SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s WHERE id < 0",
			col, col, coTable)
		res, err := db.Query(context.Background(), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s: %d rows, want 1", sql, len(res.Rows))
		}
		if got := res.Rows[0]["lo"]; got != nil {
			t.Errorf("MIN(%s) over no rows = %#v, want nil", col, got)
		}
		if got := res.Rows[0]["hi"]; got != nil {
			t.Errorf("MAX(%s) over no rows = %#v, want nil", col, got)
		}
	}
}

// mmcGroupTable is a grouped fixture with three groups the aggregate has to
// answer differently: one ordinary, one whose container is NULL in EVERY row
// (the answer is NULL, and NOT the empty container), and one holding only
// the EMPTY container (the answer is that empty value, and NOT NULL).
const mmcGroupTable = "mmc_grouped"

func mmcOpenGrouped(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
	}}
	if err := db.CreateTable(ctx, mmcGroupTable, schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"g": int32(1), "c_arr": []any{"m"}, "c_row": map[string]any{"a": "m", "b": int64(2)}},
		{"g": int32(1), "c_arr": []any{"a", "z"}, "c_row": map[string]any{"a": "a", "b": nil}},
		{"g": int32(1), "c_arr": nil, "c_row": nil},
		{"g": int32(2), "c_arr": nil, "c_row": nil},
		{"g": int32(2), "c_arr": nil, "c_row": nil},
		{"g": int32(3), "c_arr": []any{}, "c_row": map[string]any{"a": nil, "b": nil}},
		{"g": int32(3), "c_arr": []any{}, "c_row": map[string]any{"a": nil, "b": nil}},
	}
	ing := db.NewIngester(mmcGroupTable, schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 2})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestMinMaxOverContainersGrouped(t *testing.T) {
	db := mmcOpenGrouped(t)
	res, err := db.Query(context.Background(),
		"SELECT g, MIN(c_arr) AS lo, MAX(c_arr) AS hi, MIN(c_row) AS rlo, MAX(c_row) AS rhi "+
			"FROM "+mmcGroupTable+" GROUP BY g ORDER BY g")
	if err != nil {
		t.Fatalf("grouped min/max: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(res.Rows))
	}
	want := []map[string]any{
		{
			"g": int32(1),
			// NULL rows are ignored, so the group's extremes come from the
			// two live rows.
			"lo":  []any{"a", "z"},
			"hi":  []any{"m"},
			"rlo": map[string]any{"a": "a", "b": nil},
			"rhi": map[string]any{"a": "m", "b": int64(2)},
		},
		{
			// Every row NULL: MIN/MAX are NULL, not an empty container.
			"g": int32(2), "lo": nil, "hi": nil, "rlo": nil, "rhi": nil,
		},
		{
			// Only EMPTY / all-null-field containers: a real value, not NULL.
			"g":   int32(3),
			"lo":  []any{},
			"hi":  []any{},
			"rlo": map[string]any{"a": nil, "b": nil},
			"rhi": map[string]any{"a": nil, "b": nil},
		},
	}
	for i, w := range want {
		for col, wv := range w {
			if gv := res.Rows[i][col]; !reflect.DeepEqual(gv, wv) {
				t.Errorf("group row %d column %s: got %#v, want %#v", i, col, gv, wv)
			}
		}
	}
}
