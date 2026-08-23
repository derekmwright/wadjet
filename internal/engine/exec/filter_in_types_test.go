package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// inTypesBatch carries one row per type value, three rows each:
// row 0 matches the IN list, row 1 does not, row 2 is NULL.
func inTypesBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	cols := []parquet.Column{
		{Name: "c_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "c_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "c_bytes", Type: parquet.TypeBytes, Nullable: true},
		{Name: "c_dec", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_dur", Type: parquet.TypeDuration, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
	}
	rows := []map[string]any{
		{
			"c_bool": true, "c_f32": float32(1.5), "c_bytes": []byte("hit"),
			"c_dec": "5.0005", "c_ipv4": "10.0.0.5", "c_ipv6": "2001:db8::5",
			"c_mac": "aa:bb:cc:00:00:05", "c_dur": int64(5000000),
			"c_uuid": "00000000-0000-4000-8000-000000000005",
		},
		{
			"c_bool": false, "c_f32": float32(2.5), "c_bytes": []byte("miss"),
			"c_dec": "6.0006", "c_ipv4": "10.0.0.6", "c_ipv6": "2001:db8::6",
			"c_mac": "aa:bb:cc:00:00:06", "c_dur": int64(6000000),
			"c_uuid": "00000000-0000-4000-8000-000000000006",
		},
		{},
	}
	return batch.FromRows(cols, rows)
}

// TestInFilterCoversEveryScalarType is the #411 regression.
//
// ResolveInFilterKernel had no arm for BOOL, FLOAT32, BYTES, DECIMAL, IPV4,
// MAC or DURATION, and InFilter.Execute turned a nil kernel into an empty
// selection — so `WHERE bool_col IN (true)` returned zero rows with no error,
// indistinguishable from genuinely empty data (the failure mode #147 was filed
// for). IPV6 had an arm but built its set from the literal TEXT while the
// column stores 16 raw bytes, so it could not match either.
func TestInFilterCoversEveryScalarType(t *testing.T) {
	cases := []struct {
		col  string
		vals []any
	}{
		{"c_bool", []any{true}},
		{"c_f32", []any{1.5}},
		{"c_bytes", []any{"hit"}},
		{"c_dec", []any{5.0005}},
		{"c_ipv4", []any{"10.0.0.5"}},
		{"c_ipv6", []any{"2001:db8::5"}},
		{"c_mac", []any{"aa:bb:cc:00:00:05"}},
		{"c_dur", []any{int64(5000000)}},
		{"c_uuid", []any{"00000000-0000-4000-8000-000000000005"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.col, func(t *testing.T) {
			out, err := NewInFilter(tc.col, tc.vals, false).
				Execute(context.Background(), inTypesBatch(t))
			if err != nil {
				t.Fatalf("IN: %v", err)
			}
			if got := selOf(out); len(got) != 1 || got[0] != 0 {
				t.Fatalf("IN selected %v, want [0] — row 0 is the matching value", got)
			}

			// NOT IN keeps the non-matching row and drops the NULL: a NULL
			// probe is UNKNOWN, not true, which is PostgreSQL's rule.
			out, err = NewInFilter(tc.col, tc.vals, true).
				Execute(context.Background(), inTypesBatch(t))
			if err != nil {
				t.Fatalf("NOT IN: %v", err)
			}
			if got := selOf(out); len(got) != 1 || got[0] != 1 {
				t.Fatalf("NOT IN selected %v, want [1] — row 1 is the non-matching value and row 2 is NULL", got)
			}
		})
	}
}

// TestUUIDEqualityMatchesTheStoredBytes is #411's second face: a UUID column
// stores 16 raw bytes, and both comparison paths compared them against the
// 36-character literal, so equality could never be true.
func TestUUIDEqualityMatchesTheStoredBytes(t *testing.T) {
	const lit = "00000000-0000-4000-8000-000000000005"

	out, err := NewKernelFilter("c_uuid", OpEq, lit).
		Execute(context.Background(), inTypesBatch(t))
	if err != nil {
		t.Fatalf("kernel filter: %v", err)
	}
	if got := selOf(out); len(got) != 1 || got[0] != 0 {
		t.Fatalf("vectorized kernel selected %v, want [0]", got)
	}

	// The row-at-a-time predicate has to agree — two spellings of one
	// comparison is how a path-dependent answer starts.
	pred := ColumnCompare("c_uuid", OpEq, lit)
	b := inTypesBatch(t)
	var hits []int
	for i := 0; i < b.Len; i++ {
		if pred(b, i) {
			hits = append(hits, i)
		}
	}
	if len(hits) != 1 || hits[0] != 0 {
		t.Fatalf("row predicate selected %v, want [0]", hits)
	}
}

// TestInFilterUnkernelledTypeErrors: a container column cannot be probed for
// set membership, and saying so is the point — the old code answered "no rows".
func TestInFilterUnkernelledTypeErrors(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{
		Name: "c_arr", Type: parquet.TypeArray,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString},
	}}, 1)
	if _, err := NewInFilter("c_arr", []any{"x"}, false).Execute(context.Background(), b); err == nil {
		t.Fatal("want an error for a type with no set-membership kernel")
	} else if !strings.Contains(err.Error(), "no set-membership kernel") {
		t.Fatalf("error %q does not name the missing kernel", err)
	}
	if _, err := NewInFilter("nope", []any{"x"}, false).Execute(context.Background(), b); err == nil {
		t.Fatal("want an error for an unresolvable column")
	} else if !strings.Contains(err.Error(), "does not exist in the input schema") {
		t.Fatalf("error %q does not report the missing column", err)
	}
}
