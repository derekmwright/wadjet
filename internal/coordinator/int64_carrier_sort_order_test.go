package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSortBatchesOrdersInt64CarrierTypes gates the probe-split gather's
// ORDER BY (Coordinator.sortBatches via compareBatchRows) for the types
// backed by an exact int64 carrier: plain bigint (Int64), Timestamp,
// Duration, IPv4 and MAC (#642, the whole int64-carrier remainder #548
// left — bigint is the most common ORDER BY type of all). Before the fix
// these routed through extractFloat64 -> float64(Int64Data), which loses
// precision above float64's 2^53 exact-integer range. A nanosecond timestamp
// is ~1.7e18, so two DISTINCT keys 1ns apart collapse to the SAME float64 and
// TIE in the numeric arm; slices.SortFunc is not stable, so the merged result
// came back in an arbitrary order — a silent wrong answer, while the
// single-process sortCompareInt64NoNulls orders the exact int64 correctly.
//
// The Int64 (bigint), Timestamp and Duration cases use two keys that DIFFER
// BY 1 at values above 2^53 (near int64 max / ~1.7e18 / 2^61), below float64
// resolution there (float64(k)==float64(k+1)), so they fail before the fix
// and pass after — two-path agreement with the exact int64 order. Each case's
// input rows are ordered opposite to the expected output for the colliding
// pair, so the old code's tie (which insertion sort leaves in input order for
// small inputs) is demonstrably the WRONG order.
//
// IPv4 (32-bit) and MAC (48-bit) values are always below 2^53, so they CANNOT
// construct a float64 collision — their cases assert basic ordering (with NULL
// placement) and guard that the exact-int64 arm keeps ordering them right.
func TestSortBatchesOrdersInt64CarrierTypes(t *testing.T) {
	ptrTrue := func() *bool { b := true; return &b }

	// Two nanosecond timestamps 1ns apart at ~1.7e18: distinct int64 keys that
	// round to the SAME float64 (float64(tsLo) == float64(tsHi)).
	const tsLo = int64(1700000000000000000)
	const tsHi = tsLo + 1
	// Two durations 1ns apart at 2^61: same sub-float64-resolution collision.
	const durLo = int64(1) << 61
	const durHi = durLo + 1
	// Two plain bigints 1 apart near int64 max (large counters / hashes-as-
	// bigint): distinct int64 keys that round to the SAME float64. Values
	// just above 2^53 do NOT collide (ulp is still small there); the float64
	// gap only opens wide enough to swallow adjacent integers far higher —
	// e.g. near 2^63, where ulp is 1024 (verified float64(bigLo)==float64(bigHi)).
	const bigLo = int64(9223372036854775806) // math.MaxInt64 - 1
	const bigHi = bigLo + 1                  // math.MaxInt64

	cases := []struct {
		name    string
		keyType parquet.TypeID
		rows    []map[string]any
		orderBy logical.OrderExpr
		want    []int64
	}{
		// ---- Timestamp: exact-int64 order below float64 resolution ----
		{
			// Input pair is [hi, lo]; ASC must reorder to [lo, hi]. Old code
			// ties (float collision) and keeps input order -> [hi, lo] -> FAIL.
			name:    "timestamp_asc_subfloat",
			keyType: parquet.TypeTimestamp,
			rows: []map[string]any{
				{"id": int64(1), "k": tsHi},
				{"id": int64(2), "k": tsLo},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3}, // tsLo, tsHi, NULL(last)
		},
		{
			// Input non-null pair is [lo, hi]; DESC must reorder to [hi, lo].
			// Old code ties and keeps input -> [lo, hi] -> FAIL.
			name:    "timestamp_desc_subfloat",
			keyType: parquet.TypeTimestamp,
			rows: []map[string]any{
				{"id": int64(2), "k": tsLo},
				{"id": int64(1), "k": tsHi},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 1, 2}, // NULL(first), tsHi, tsLo
		},
		{
			name:    "timestamp_asc_nulls_first",
			keyType: parquet.TypeTimestamp,
			rows: []map[string]any{
				{"id": int64(1), "k": tsHi},
				{"id": int64(2), "k": tsLo},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", NullsFirst: ptrTrue()},
			want:    []int64{3, 2, 1}, // NULL(first), tsLo, tsHi
		},

		// ---- Int64 (plain bigint): exact order below float64 resolution ----
		{
			// The most common ORDER BY type. Input pair is [hi, lo]; ASC must
			// reorder to [lo, hi]. Old code ties (float collision at >2^53)
			// and keeps input order -> [hi, lo] -> FAIL.
			name:    "bigint_asc_subfloat",
			keyType: parquet.TypeInt64,
			rows: []map[string]any{
				{"id": int64(1), "k": bigHi},
				{"id": int64(2), "k": bigLo},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3}, // bigLo, bigHi, NULL(last)
		},
		{
			name:    "bigint_desc_subfloat",
			keyType: parquet.TypeInt64,
			rows: []map[string]any{
				{"id": int64(2), "k": bigLo},
				{"id": int64(1), "k": bigHi},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 1, 2}, // NULL(first), bigHi, bigLo
		},

		// ---- Duration: exact-int64 order below float64 resolution ----
		{
			name:    "duration_asc_subfloat",
			keyType: parquet.TypeDuration,
			rows: []map[string]any{
				{"id": int64(1), "k": durHi},
				{"id": int64(2), "k": durLo},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3}, // durLo, durHi, NULL(last)
		},
		{
			name:    "duration_desc_subfloat",
			keyType: parquet.TypeDuration,
			rows: []map[string]any{
				{"id": int64(2), "k": durLo},
				{"id": int64(1), "k": durHi},
				{"id": int64(3), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 1, 2}, // NULL(first), durHi, durLo
		},

		// ---- IPv4: basic ordering (32-bit, no float64 collision possible) ----
		{
			name:    "ipv4_asc",
			keyType: parquet.TypeIPv4,
			rows: []map[string]any{
				{"id": int64(1), "k": "10.0.0.2"},
				{"id": int64(2), "k": "10.0.0.1"},
				{"id": int64(3), "k": "255.255.255.255"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3, 4}, // .1, .2, 255.., NULL(last)
		},
		{
			name:    "ipv4_desc",
			keyType: parquet.TypeIPv4,
			rows: []map[string]any{
				{"id": int64(1), "k": "10.0.0.2"},
				{"id": int64(2), "k": "10.0.0.1"},
				{"id": int64(3), "k": "255.255.255.255"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{4, 3, 1, 2}, // NULL(first), 255.., .2, .1
		},

		// ---- MAC: basic ordering (48-bit, no float64 collision possible) ----
		{
			name:    "mac_asc",
			keyType: parquet.TypeMAC,
			rows: []map[string]any{
				{"id": int64(1), "k": "00:00:00:00:00:02"},
				{"id": int64(2), "k": "00:00:00:00:00:01"},
				{"id": int64(3), "k": "ff:ff:ff:ff:ff:ff"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{2, 1, 3, 4},
		},
		{
			name:    "mac_desc",
			keyType: parquet.TypeMAC,
			rows: []map[string]any{
				{"id": int64(1), "k": "00:00:00:00:00:02"},
				{"id": int64(2), "k": "00:00:00:00:00:01"},
				{"id": int64(3), "k": "ff:ff:ff:ff:ff:ff"},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{4, 3, 1, 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "k", Type: tc.keyType, Nullable: true},
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
