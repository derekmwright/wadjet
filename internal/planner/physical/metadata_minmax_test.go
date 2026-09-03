package physical

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// intCols builds a one-column int accumulator set for column "c".
func intCols(t *testing.T, colType parquet.TypeID) []mmColumn {
	t.Helper()
	kind, out, ok := mmTypeFor(colType)
	if !ok {
		t.Fatalf("mmTypeFor(%v) unexpectedly unsupported", colType)
	}
	return []mmColumn{{name: "c", kind: kind, colType: colType, outType: out}}
}

func rg(numRows int64, cs parquet.ColumnStats) parquet.RowGroupStats {
	return parquet.RowGroupStats{NumRows: numRows, Columns: map[string]parquet.ColumnStats{"c": cs}}
}

func TestMMTypeFor(t *testing.T) {
	tests := []struct {
		name    string
		in      parquet.TypeID
		wantOut parquet.TypeID
		wantOK  bool
	}{
		{"int32 widens to int64", parquet.TypeInt32, parquet.TypeInt64, true},
		{"int64", parquet.TypeInt64, parquet.TypeInt64, true},
		{"date stays date", parquet.TypeDate, parquet.TypeDate, true},
		{"timestamp stays timestamp", parquet.TypeTimestamp, parquet.TypeTimestamp, true},
		{"float32 stays float32", parquet.TypeFloat32, parquet.TypeFloat32, true},
		{"float64", parquet.TypeFloat64, parquet.TypeFloat64, true},
		// Declined: truncatable, unordered, or unrepresentable statistics.
		{"string declined", parquet.TypeString, 0, false},
		{"bytes declined", parquet.TypeBytes, 0, false},
		{"decimal declined", parquet.TypeDecimal, 0, false},
		{"bool declined", parquet.TypeBool, 0, false},
		{"uuid declined", parquet.TypeUUID, 0, false},
		{"ipv4 declined", parquet.TypeIPv4, 0, false},
		{"array declined", parquet.TypeArray, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, out, ok := mmTypeFor(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("mmTypeFor(%v) ok = %v, want %v", tt.in, ok, tt.wantOK)
			}
			if ok && out != tt.wantOut {
				t.Errorf("mmTypeFor(%v) out = %v, want %v", tt.in, out, tt.wantOut)
			}
		})
	}
}

func TestMMFoldRowGroupInt(t *testing.T) {
	tests := []struct {
		name    string
		colType parquet.TypeID
		groups  []parquet.RowGroupStats
		wantOK  bool
		wantHas bool
		wantMin int64
		wantMax int64
	}{
		{
			name:    "single row group",
			colType: parquet.TypeInt64,
			groups:  []parquet.RowGroupStats{rg(10, parquet.ColumnStats{HasStats: true, MinValue: int64(3), MaxValue: int64(9)})},
			wantOK:  true, wantHas: true, wantMin: 3, wantMax: 9,
		},
		{
			name:    "extremes span row groups",
			colType: parquet.TypeInt64,
			groups: []parquet.RowGroupStats{
				rg(10, parquet.ColumnStats{HasStats: true, MinValue: int64(5), MaxValue: int64(40)}),
				rg(10, parquet.ColumnStats{HasStats: true, MinValue: int64(-2), MaxValue: int64(7)}),
				rg(10, parquet.ColumnStats{HasStats: true, MinValue: int64(6), MaxValue: int64(99)}),
			},
			wantOK: true, wantHas: true, wantMin: -2, wantMax: 99,
		},
		{
			name:    "all-null row group contributes nothing",
			colType: parquet.TypeInt64,
			groups: []parquet.RowGroupStats{
				rg(10, parquet.ColumnStats{HasStats: true, NullCount: 10}),
				rg(10, parquet.ColumnStats{HasStats: true, MinValue: int64(4), MaxValue: int64(8), NullCount: 3}),
			},
			wantOK: true, wantHas: true, wantMin: 4, wantMax: 8,
		},
		{
			name:    "every row group all-null yields no value",
			colType: parquet.TypeInt64,
			groups: []parquet.RowGroupStats{
				rg(10, parquet.ColumnStats{HasStats: true, NullCount: 10}),
				rg(7, parquet.ColumnStats{HasStats: true, NullCount: 7}),
			},
			wantOK: true, wantHas: false,
		},
		{
			name:    "empty row group ignored",
			colType: parquet.TypeInt64,
			groups: []parquet.RowGroupStats{
				rg(0, parquet.ColumnStats{}),
				rg(4, parquet.ColumnStats{HasStats: true, MinValue: int64(1), MaxValue: int64(2)}),
			},
			wantOK: true, wantHas: true, wantMin: 1, wantMax: 2,
		},
		{
			name:    "date within int32 range",
			colType: parquet.TypeDate,
			groups:  []parquet.RowGroupStats{rg(5, parquet.ColumnStats{HasStats: true, MinValue: int64(19000), MaxValue: int64(20000)})},
			wantOK:  true, wantHas: true, wantMin: 19000, wantMax: 20000,
		},
		// --- decline cases: the caller must fall back to the scan ---
		{
			name:    "no statistics at all",
			colType: parquet.TypeInt64,
			groups:  []parquet.RowGroupStats{rg(10, parquet.ColumnStats{HasStats: false})},
			wantOK:  false,
		},
		{
			name:    "statistics present but min/max absent and rows not all null",
			colType: parquet.TypeInt64,
			groups:  []parquet.RowGroupStats{rg(10, parquet.ColumnStats{HasStats: true, NullCount: 3})},
			wantOK:  false,
		},
		{
			name:    "column missing from the row group",
			colType: parquet.TypeInt64,
			groups:  []parquet.RowGroupStats{{NumRows: 10, Columns: map[string]parquet.ColumnStats{"other": {HasStats: true, MinValue: int64(1), MaxValue: int64(2)}}}},
			wantOK:  false,
		},
		{
			name:    "one good row group then a statless one",
			colType: parquet.TypeInt64,
			groups: []parquet.RowGroupStats{
				rg(10, parquet.ColumnStats{HasStats: true, MinValue: int64(1), MaxValue: int64(2)}),
				rg(10, parquet.ColumnStats{HasStats: false}),
			},
			wantOK: false,
		},
		{
			name:    "stat is not the native form the type decodes to",
			colType: parquet.TypeInt64,
			groups:  []parquet.RowGroupStats{rg(10, parquet.ColumnStats{HasStats: true, MinValue: "a", MaxValue: "z"})},
			wantOK:  false,
		},
		{
			name:    "date beyond the int32 day vector",
			colType: parquet.TypeDate,
			groups:  []parquet.RowGroupStats{rg(5, parquet.ColumnStats{HasStats: true, MinValue: int64(1), MaxValue: int64(math.MaxInt32) + 1})},
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols := intCols(t, tt.colType)
			ok := true
			for _, g := range tt.groups {
				if !mmFoldRowGroup(cols, g) {
					ok = false
					break
				}
			}
			if ok != tt.wantOK {
				t.Fatalf("fold ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if cols[0].found != tt.wantHas {
				t.Fatalf("found = %v, want %v", cols[0].found, tt.wantHas)
			}
			if tt.wantHas && (cols[0].minI != tt.wantMin || cols[0].maxI != tt.wantMax) {
				t.Errorf("min/max = %d/%d, want %d/%d", cols[0].minI, cols[0].maxI, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestMMFoldRowGroupFloat(t *testing.T) {
	tests := []struct {
		name    string
		groups  []parquet.RowGroupStats
		wantOK  bool
		wantHas bool
		wantMin float64
		wantMax float64
	}{
		{
			name: "extremes span row groups",
			groups: []parquet.RowGroupStats{
				rg(4, parquet.ColumnStats{HasStats: true, MinValue: 1.5, MaxValue: 9.25}),
				rg(4, parquet.ColumnStats{HasStats: true, MinValue: -3.5, MaxValue: 4.0}),
			},
			wantOK: true, wantHas: true, wantMin: -3.5, wantMax: 9.25,
		},
		{
			name:   "NaN-poisoned statistics decline",
			groups: []parquet.RowGroupStats{rg(4, parquet.ColumnStats{HasStats: true, MinValue: math.NaN(), MaxValue: math.NaN()})},
			wantOK: false,
		},
		{
			name:   "int stat on a float column declines",
			groups: []parquet.RowGroupStats{rg(4, parquet.ColumnStats{HasStats: true, MinValue: int64(1), MaxValue: int64(2)})},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols := intCols(t, parquet.TypeFloat64)
			ok := true
			for _, g := range tt.groups {
				if !mmFoldRowGroup(cols, g) {
					ok = false
					break
				}
			}
			if ok != tt.wantOK {
				t.Fatalf("fold ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if cols[0].found != tt.wantHas {
				t.Fatalf("found = %v, want %v", cols[0].found, tt.wantHas)
			}
			if tt.wantHas && (cols[0].minF != tt.wantMin || cols[0].maxF != tt.wantMax) {
				t.Errorf("min/max = %v/%v, want %v/%v", cols[0].minF, cols[0].maxF, tt.wantMin, tt.wantMax)
			}
		})
	}
}

// TestMMMerge covers the per-file accumulator join: workers fold their own
// files, the parent merges, and an untouched (all-null-file) accumulator must
// not drag the answer to zero.
func TestMMMerge(t *testing.T) {
	dst := intCols(t, parquet.TypeInt64)
	a := intCols(t, parquet.TypeInt64)
	b := intCols(t, parquet.TypeInt64)
	empty := intCols(t, parquet.TypeInt64)

	if !mmFoldRowGroup(a, rg(4, parquet.ColumnStats{HasStats: true, MinValue: int64(10), MaxValue: int64(20)})) {
		t.Fatal("fold a")
	}
	if !mmFoldRowGroup(b, rg(4, parquet.ColumnStats{HasStats: true, MinValue: int64(-5), MaxValue: int64(3)})) {
		t.Fatal("fold b")
	}

	mmMerge(dst, empty)
	if dst[0].found {
		t.Fatal("merging an empty accumulator must not mark a value found")
	}
	mmMerge(dst, a)
	mmMerge(dst, empty)
	mmMerge(dst, b)
	if !dst[0].found || dst[0].minI != -5 || dst[0].maxI != 20 {
		t.Fatalf("merged min/max = %d/%d (found=%v), want -5/20", dst[0].minI, dst[0].maxI, dst[0].found)
	}
}
