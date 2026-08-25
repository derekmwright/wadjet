package kernel

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestParseDateToDaysDoesNotClampAtTheDurationBoundary pins #451: every
// value below is PostgreSQL-verified (`date '<lit>' - date '1970-01-01'`),
// and every one past 1677-09-22..2262-04-11 used to answer the window's
// edge instead — a time.Duration nanosecond saturation in the old
// t.Sub(epoch) arithmetic.
func TestParseDateToDaysDoesNotClampAtTheDurationBoundary(t *testing.T) {
	tests := []struct {
		lit  string
		days int32
	}{
		{"2262-04-11", 106751}, // last date the old code got right
		{"2262-04-12", 106752}, // first date the old code clamped
		{"2300-01-01", 120530},
		{"9999-12-31", 2932896}, // the SCD-2 "end of time" sentinel
		{"1677-09-22", -106751}, // last date the old code got right, downward
		{"1677-09-21", -106752}, // first date the old code clamped, downward
		{"1600-01-01", -135140},
		{"0001-01-01", -719162},
		{"1970-01-01", 0},
	}
	for _, tc := range tests {
		t.Run(tc.lit, func(t *testing.T) {
			got, err := parseDateToDays(tc.lit)
			if err != nil {
				t.Fatalf("parseDateToDays(%q) error = %v, want nil", tc.lit, err)
			}
			if got != tc.days {
				t.Errorf("parseDateToDays(%q) = %d, want %d", tc.lit, got, tc.days)
			}
		})
	}
}

// TestParseDateToDaysFloorsPreEpochTimeOfDay pins the adjacent defect named
// in #451: the old expression's Hours()/24 truncated toward zero, so a
// pre-epoch timestamp WITH a time-of-day rounded a day toward the epoch.
func TestParseDateToDaysFloorsPreEpochTimeOfDay(t *testing.T) {
	withTime, err := parseDateToDays("1969-12-31 12:00:00")
	if err != nil {
		t.Fatalf("parseDateToDays with time-of-day: %v", err)
	}
	dateOnly, err := parseDateToDays("1969-12-31")
	if err != nil {
		t.Fatalf("parseDateToDays date-only: %v", err)
	}
	if dateOnly != -1 {
		t.Fatalf("parseDateToDays(%q) = %d, want -1", "1969-12-31", dateOnly)
	}
	if withTime != dateOnly {
		t.Errorf("parseDateToDays(%q) = %d, want %d (same day as %q)",
			"1969-12-31 12:00:00", withTime, dateOnly, "1969-12-31")
	}
}

// TestToDateInt32RefusesOutOfRangeIntegers pins the numeric half of #451:
// an int64/int day count that does not fit the DATE column's int32 storage
// must be refused, not silently truncated by int32(days).
func TestToDateInt32RefusesOutOfRangeIntegers(t *testing.T) {
	for _, v := range []any{
		int64(math.MaxInt32) + 1,
		int64(math.MinInt32) - 1,
		int(math.MaxInt32) + 1,
	} {
		if _, err := toDateInt32(v); err == nil {
			t.Errorf("toDateInt32(%v) = no error, want errDateDaysOutOfRange", v)
		}
	}
	// In range: no error, exact passthrough.
	for _, v := range []any{int64(math.MaxInt32), int64(math.MinInt32), 0, nil} {
		got, err := toDateInt32(v)
		if err != nil {
			t.Errorf("toDateInt32(%v) unexpected error: %v", v, err)
		}
		if v != nil && int64(got) != toInt64(v) {
			t.Errorf("toDateInt32(%v) = %d, want %d", v, got, toInt64(v))
		}
	}
}

// TestFilterKernelDateBeyondOldClampMatchesTheRealRow is the filter-level
// #451 regression: before the fix, `d > '2300-01-01'` and `d = '9999-12-31'`
// both used the CLAMPED bound (2262-04-11, day 106751) instead of the real
// one, so a query matched the wrong rows in both directions.
func TestFilterKernelDateBeyondOldClampMatchesTheRealRow(t *testing.T) {
	schema := []parquet.Column{{Name: "d", Type: parquet.TypeDate}}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, int32(106751))  // 2262-04-11
	b.Columns[0].SetValue(1, int32(107151))  // 2263-05-16
	b.Columns[0].SetValue(2, int32(2932896)) // 9999-12-31

	// `d > '2300-01-01'` (day 120530): only the 9999-12-31 row is really
	// past it. Pre-fix, the bound clamped to 106751 and wrongly admitted
	// row 1 (2263-05-16) too.
	gt := ResolveFilterKernel(batch.TypeDate, OpGt, "2300-01-01")
	if gt == nil {
		t.Fatal("ResolveFilterKernel(TypeDate, OpGt, '2300-01-01') = nil")
	}
	sel := gt(b.Columns[0], nil, 3, make([]uint32, 0, 3))
	if len(sel) != 1 || sel[0] != 2 {
		t.Errorf("d > '2300-01-01': got %v, want [2]", sel)
	}

	// `d = '9999-12-31'` (day 2932896): must match row 2, not row 0
	// (2262-04-11, the pre-fix clamp target).
	eq := ResolveFilterKernel(batch.TypeDate, OpEq, "9999-12-31")
	if eq == nil {
		t.Fatal("ResolveFilterKernel(TypeDate, OpEq, '9999-12-31') = nil")
	}
	sel = eq(b.Columns[0], nil, 3, make([]uint32, 0, 3))
	if len(sel) != 1 || sel[0] != 2 {
		t.Errorf("d = '9999-12-31': got %v, want [2]", sel)
	}
}

// TestInFilterKernelDateBeyondOldClamp is the IN-list counterpart —
// ResolveInFilterKernel builds its set through the same toDateInt32 helper
// (#451).
func TestInFilterKernelDateBeyondOldClamp(t *testing.T) {
	schema := []parquet.Column{{Name: "d", Type: parquet.TypeDate}}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, int32(106751))  // 2262-04-11 — the old clamp target
	b.Columns[0].SetValue(1, int32(2932896)) // 9999-12-31 — the real value

	kern := ResolveInFilterKernel(batch.TypeDate, []any{"9999-12-31"}, false)
	if kern == nil {
		t.Fatal("ResolveInFilterKernel(TypeDate, ['9999-12-31']) = nil")
	}
	sel := kern(b.Columns[0], nil, 2, make([]uint32, 0, 2))
	if len(sel) != 1 || sel[0] != 1 {
		t.Errorf("d IN ('9999-12-31'): got %v, want [1]", sel)
	}
}

// TestResolveFilterKernelDateRefusesUnrepresentableLiteral pins the "no
// kernel, caller raises" convention (#451): a DATE constant whose day count
// overflows int32 must not silently compare against a truncated value.
func TestResolveFilterKernelDateRefusesUnrepresentableLiteral(t *testing.T) {
	if kern := ResolveFilterKernel(batch.TypeDate, OpEq, int64(math.MaxInt32)+1); kern != nil {
		t.Error("ResolveFilterKernel(TypeDate, out-of-range int64) = non-nil kernel, want nil")
	}
	if kern := ResolveInFilterKernel(batch.TypeDate, []any{int64(math.MaxInt32) + 1}, false); kern != nil {
		t.Error("ResolveInFilterKernel(TypeDate, out-of-range int64) = non-nil kernel, want nil")
	}
}

// TestStatsDomainValueDateBeyondOldClamp pins the pruning half of #451:
// StatsDomainValue must hand the prune layer the REAL day count for a date
// like '9999-12-31', not the clamped one, and must WITHHOLD (not clamp) a
// literal that genuinely overflows int32.
func TestStatsDomainValueDateBeyondOldClamp(t *testing.T) {
	got, ok := StatsDomainValue(batch.TypeDate, 0, "9999-12-31")
	if !ok {
		t.Fatal("StatsDomainValue(TypeDate, '9999-12-31') ok = false, want true")
	}
	if got != int64(2932896) {
		t.Errorf("StatsDomainValue(TypeDate, '9999-12-31') = %v, want 2932896", got)
	}

	got, ok = StatsDomainValue(batch.TypeDate, 0, "1677-09-21")
	if !ok {
		t.Fatal("StatsDomainValue(TypeDate, '1677-09-21') ok = false, want true")
	}
	if got != int64(-106752) {
		t.Errorf("StatsDomainValue(TypeDate, '1677-09-21') = %v, want -106752", got)
	}
}
