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

// #415's end-to-end gate. The three sort resolvers ended in a default that
// reported every pair EQUAL, and ARRAY, ROW, MAP and VECTOR all landed there:
// `ORDER BY arr_col` ran, compared everything equal and handed back INPUT
// ORDER — no error, no warning — and a window PARTITION BY on one of those
// columns put every row in one partition.
//
// The assertions are absolute, not differential: each query names the exact
// sequence the documented total order produces (ARRAY/VECTOR lexicographic,
// ROW field-wise, MAP entry-wise over key-sorted entries; column NULLs where
// the query asked, element NULLs after non-NULLs per PostgreSQL's array_cmp).
// A second arm could not have caught this — both arms took the same broken
// comparator.

const coTable = "container_order"

func coSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "c_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
		{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 2},
	}}
}

// coRows: id is the row's IDENTITY, deliberately anti-correlated with every
// container's order, so a comparator that returns input order fails.
func coRows() []map[string]any {
	return []map[string]any{
		{"id": int64(0), "c_arr": []any{"b"}, "c_row": map[string]any{"a": "y", "b": int64(1)},
			"c_map": map[string]any{"b": int64(1)}, "c_vec": []float32{1, 0}},
		{"id": int64(1), "c_arr": []any{"a", "b"}, "c_row": map[string]any{"a": "x", "b": int64(2)},
			"c_map": map[string]any{"a": int64(1), "b": int64(2)}, "c_vec": []float32{0, 1}},
		{"id": int64(2), "c_arr": nil, "c_row": nil, "c_map": nil, "c_vec": nil},
		{"id": int64(3), "c_arr": []any{"a"}, "c_row": map[string]any{"a": "x", "b": int64(1)},
			"c_map": map[string]any{"a": int64(1)}, "c_vec": []float32{0, 0}},
		{"id": int64(4), "c_arr": []any{}, "c_row": map[string]any{"a": "x", "b": nil},
			"c_map": map[string]any{}, "c_vec": []float32{-1, 9}},
	}
}

func coOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := coSchema()
	if err := db.CreateTable(ctx, coTable, schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester(coTable, schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	rows := coRows()
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func coIDs(t *testing.T, db *DB, sql string) []int64 {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([]int64, 0, len(res.Rows))
	for _, r := range res.Rows {
		v, ok := r["id"].(int64)
		if !ok {
			t.Fatalf("%s: id came back as %T (%v)", sql, r["id"], r["id"])
		}
		out = append(out, v)
	}
	return out
}

func TestOrderByContainerTypes(t *testing.T) {
	db := coOpen(t)
	cases := []struct {
		name string
		sql  string
		want []int64
	}{
		// ARRAY: [] < ["a"] < ["a","b"] < ["b"] — the shorter array loses the
		// tie on its own prefix (PostgreSQL's array_cmp). Column NULLs land
		// where PostgreSQL's defaults put them: last under ASC, first under
		// DESC.
		{"array_asc", "SELECT id FROM " + coTable + " ORDER BY c_arr", []int64{4, 3, 1, 0, 2}},
		{"array_desc", "SELECT id FROM " + coTable + " ORDER BY c_arr DESC", []int64{2, 0, 1, 3, 4}},
		{"array_asc_nulls_first", "SELECT id FROM " + coTable + " ORDER BY c_arr NULLS FIRST", []int64{2, 4, 3, 1, 0}},
		{"array_asc_nulls_last", "SELECT id FROM " + coTable + " ORDER BY c_arr NULLS LAST", []int64{4, 3, 1, 0, 2}},
		{"array_desc_nulls_last", "SELECT id FROM " + coTable + " ORDER BY c_arr DESC NULLS LAST", []int64{0, 1, 3, 4, 2}},
		{"array_desc_nulls_first", "SELECT id FROM " + coTable + " ORDER BY c_arr DESC NULLS FIRST", []int64{2, 0, 1, 3, 4}},

		// ROW: field-wise. {x,1} < {x,2} < {x,NULL} (a NULL FIELD sorts after
		// a non-NULL one, per PostgreSQL's record_cmp, whatever the column's
		// own null placement is) < {y,1}.
		{"row_asc", "SELECT id FROM " + coTable + " ORDER BY c_row", []int64{3, 1, 4, 0, 2}},
		{"row_desc", "SELECT id FROM " + coTable + " ORDER BY c_row DESC", []int64{2, 0, 4, 1, 3}},

		// MAP: entry-wise over KEY-SORTED entries. {} < {a:1} < {a:1,b:2} < {b:1}.
		{"map_asc", "SELECT id FROM " + coTable + " ORDER BY c_map", []int64{4, 3, 1, 0, 2}},
		{"map_desc", "SELECT id FROM " + coTable + " ORDER BY c_map DESC", []int64{2, 0, 1, 3, 4}},

		// VECTOR: lexicographic float32. [-1,9] < [0,0] < [0,1] < [1,0].
		{"vector_asc", "SELECT id FROM " + coTable + " ORDER BY c_vec", []int64{4, 3, 1, 0, 2}},
		{"vector_desc", "SELECT id FROM " + coTable + " ORDER BY c_vec DESC", []int64{2, 0, 1, 3, 4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := coIDs(t, db, tc.sql)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s\n got %v\nwant %v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestOrderByContainerIsNotInputOrder is the shape of the defect stated
// directly: a comparator that reports every value equal returns a STABLE sort,
// i.e. input order. Every ORDER BY above must differ from it.
func TestOrderByContainerIsNotInputOrder(t *testing.T) {
	db := coOpen(t)
	input := coIDs(t, db, "SELECT id FROM "+coTable)
	for _, col := range []string{"c_arr", "c_row", "c_map", "c_vec"} {
		got := coIDs(t, db, fmt.Sprintf("SELECT id FROM %s ORDER BY %s", coTable, col))
		if reflect.DeepEqual(got, input) {
			t.Errorf("ORDER BY %s returned input order %v — the comparator is a no-op again", col, got)
		}
	}
}

// TestWindowPartitionByContainer: the window's partition boundary walk uses
// compareVectorValues, whose default boxed both sides through GetValue. For a
// container that answered through compareAny's `default: return 0`, so every
// row landed in ONE partition and COUNT(*) OVER (PARTITION BY arr) returned
// the whole table's count on every row.
func TestWindowPartitionByContainer(t *testing.T) {
	ctx := context.Background()
	db := coOpen(t)
	// Two rows share ["a"] / {a:1} / [0,0], the rest are singletons.
	extra := []map[string]any{
		{"id": int64(5), "c_arr": []any{"a"}, "c_row": map[string]any{"a": "x", "b": int64(1)},
			"c_map": map[string]any{"a": int64(1)}, "c_vec": []float32{0, 0}},
	}
	ing := db.NewIngester(coTable, coSchema(), nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	if err := ing.Ingest(ctx, extra); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	for _, col := range []string{"c_arr", "c_row", "c_map", "c_vec"} {
		t.Run(col, func(t *testing.T) {
			sql := fmt.Sprintf(
				"SELECT id, COUNT(*) OVER (PARTITION BY %s) AS n FROM %s ORDER BY id", col, coTable)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			if len(res.Rows) != 6 {
				t.Fatalf("%s returned %d rows, want 6", sql, len(res.Rows))
			}
			// ids 3 and 5 share a value; the other four are alone. A
			// single-partition answer is 6 on every row.
			want := map[int64]int64{0: 1, 1: 1, 2: 1, 3: 2, 4: 1, 5: 2}
			for _, r := range res.Rows {
				id, _ := r["id"].(int64)
				n, ok := r["n"].(int64)
				if !ok {
					t.Fatalf("count came back as %T (%v)", r["n"], r["n"])
				}
				if n != want[id] {
					t.Errorf("PARTITION BY %s: id %d in a partition of %d, want %d", col, id, n, want[id])
				}
			}
		})
	}
}
