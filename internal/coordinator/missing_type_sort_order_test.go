package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSortBatchesOrdersMissingTypes gates the probe-split gather's ORDER BY
// (Coordinator.sortBatches via compareBatchRows) for the five types #548
// named: IPv6, UUID, BYTES, BOOL and DECIMAL. Before the fix extractFloat64
// answered 0 for each of these carrier/text-backed columns, so every pair of
// rows tied in the default arm and slices.SortFunc (not a stable sort) left
// them in an ARBITRARY order — a silent wrong answer to the client.
//
// The expected orders encode the single-process engine's ordering for each
// type (sortCompareStringNoNulls' byte order for IPv6/UUID/BYTES, false<true
// for BOOL, CompareDecimalValues' numeric order for DECIMAL), so a passing
// case is two-path agreement. Each case also asserts NULL placement, which is
// absolute (NULLS LAST for ASC, NULLS FIRST for DESC by default, an explicit
// clause winning) and must NOT flip with ASC/DESC.
func TestSortBatchesOrdersMissingTypes(t *testing.T) {
	ptrTrue := func() *bool { b := true; return &b }
	ptrFalse := func() *bool { b := false; return &b }

	cases := []struct {
		name    string
		keyType parquet.TypeID
		scale   int // DECIMAL only
		// rows: each has an int64 "id" and a "k" of keyType (nil = NULL).
		rows    []map[string]any
		orderBy logical.OrderExpr
		want    []int64 // expected id order
	}{
		// ---- BOOL: false < true ----
		{
			name:    "bool_asc",
			keyType: parquet.TypeBool,
			rows: []map[string]any{
				{"id": int64(1), "k": true},
				{"id": int64(2), "k": false},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3}, // false, true, NULL(last)
		},
		{
			name:    "bool_desc",
			keyType: parquet.TypeBool,
			rows: []map[string]any{
				{"id": int64(1), "k": true},
				{"id": int64(2), "k": false},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 1, 2}, // NULL(first), true, false
		},

		// ---- BYTES: raw byte order ----
		{
			name:    "bytes_asc",
			keyType: parquet.TypeBytes,
			rows: []map[string]any{
				{"id": int64(1), "k": []byte("banana")},
				{"id": int64(2), "k": []byte("apple")},
				{"id": int64(3), "k": nil},
				{"id": int64(4), "k": []byte("cherry")},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 4, 3}, // apple, banana, cherry, NULL(last)
		},
		{
			name:    "bytes_desc",
			keyType: parquet.TypeBytes,
			rows: []map[string]any{
				{"id": int64(1), "k": []byte("banana")},
				{"id": int64(2), "k": []byte("apple")},
				{"id": int64(3), "k": nil},
				{"id": int64(4), "k": []byte("cherry")},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 4, 1, 2}, // NULL(first), cherry, banana, apple
		},

		// ---- UUID: 16-byte canonical byte order ----
		{
			name:    "uuid_asc",
			keyType: parquet.TypeUUID,
			rows: []map[string]any{
				{"id": int64(1), "k": "00000000-0000-0000-0000-000000000002"},
				{"id": int64(2), "k": "00000000-0000-0000-0000-000000000001"},
				{"id": int64(3), "k": "ffffffff-ffff-ffff-ffff-ffffffffffff"},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3},
		},
		{
			name:    "uuid_desc",
			keyType: parquet.TypeUUID,
			rows: []map[string]any{
				{"id": int64(1), "k": "00000000-0000-0000-0000-000000000002"},
				{"id": int64(2), "k": "00000000-0000-0000-0000-000000000001"},
				{"id": int64(3), "k": "ffffffff-ffff-ffff-ffff-ffffffffffff"},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 1, 2},
		},

		// ---- IPv6: 16-byte big-endian order ----
		{
			name:    "ipv6_asc",
			keyType: parquet.TypeIPv6,
			rows: []map[string]any{
				{"id": int64(1), "k": "::2"},
				{"id": int64(2), "k": "::1"},
				{"id": int64(3), "k": "ff::"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3, 4}, // ::1, ::2, ff::, NULL(last)
		},
		{
			name:    "ipv6_desc",
			keyType: parquet.TypeIPv6,
			rows: []map[string]any{
				{"id": int64(1), "k": "::2"},
				{"id": int64(2), "k": "::1"},
				{"id": int64(3), "k": "ff::"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{4, 3, 1, 2}, // NULL(first), ff::, ::2, ::1
		},

		// ---- DECIMAL: numeric order (NOT text order) ----
		{
			// Text order would be 10.20 < 10.25 < 2.05 -> [3,1,2]; numeric
			// order is 2.05 < 10.20 < 10.25 -> [2,3,1]. The two disagree, so
			// this case fails on a string comparison and passes only on the
			// Int128 numeric compare.
			name:    "decimal_asc",
			keyType: parquet.TypeDecimal,
			scale:   2,
			rows: []map[string]any{
				{"id": int64(1), "k": "10.25"},
				{"id": int64(2), "k": "2.05"},
				{"id": int64(3), "k": "10.20"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 3, 1, 4}, // 2.05, 10.20, 10.25, NULL(last)
		},
		{
			name:    "decimal_desc",
			keyType: parquet.TypeDecimal,
			scale:   2,
			rows: []map[string]any{
				{"id": int64(1), "k": "10.25"},
				{"id": int64(2), "k": "2.05"},
				{"id": int64(3), "k": "10.20"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{4, 1, 3, 2}, // NULL(first), 10.25, 10.20, 2.05
		},

		// ---- Explicit NULLS clause overrides the ASC/DESC default ----
		{
			name:    "decimal_asc_nulls_first",
			keyType: parquet.TypeDecimal,
			scale:   2,
			rows: []map[string]any{
				{"id": int64(1), "k": "10.25"},
				{"id": int64(2), "k": "2.05"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", NullsFirst: ptrTrue()},
			want:    []int64{4, 2, 1}, // NULL(first), 2.05, 10.25
		},
		{
			name:    "bytes_desc_nulls_last",
			keyType: parquet.TypeBytes,
			rows: []map[string]any{
				{"id": int64(1), "k": []byte("banana")},
				{"id": int64(2), "k": []byte("apple")},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true, NullsFirst: ptrFalse()},
			want:    []int64{1, 2, 3}, // banana, apple, NULL(last)
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "k", Type: tc.keyType, Nullable: true},
			}
			if tc.keyType == parquet.TypeDecimal {
				schema[1].Precision = 18
				schema[1].Scale = tc.scale
			}
			b := batch.FromRows(schema, tc.rows)
			colIdx := map[string]int{"id": 0, "k": 1}

			c := &Coordinator{}
			c.sortBatches([]*batch.RecordBatch{b}, []string{"id", "k"}, colIdx, []logical.OrderExpr{tc.orderBy})

			var got []int64
			n := b.ActiveLen()
			for i := 0; i < n; i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				got = append(got, b.Columns[0].Int64Data[row])
			}
			if len(got) != len(tc.want) {
				t.Fatalf("sortBatches produced %d rows, want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("id order = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
