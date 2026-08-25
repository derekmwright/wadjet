package expr

import "testing"

// TestParseDateToEpochDaysOKDoesNotClampFarDates is the expr-package half of
// #451: parseDateToEpochDaysOK computed the day count via t.Sub(epoch), a
// time.Duration that saturates at ±math.MaxInt64 ns (~292 years) rather than
// reporting an overflow, so any 4-digit-year date outside roughly
// 1677-09-22..2262-04-11 silently answered the window's edge instead of its
// real value. Values below are PostgreSQL-verified (`date '<lit>' - date
// '1970-01-01'`).
func TestParseDateToEpochDaysOKDoesNotClampFarDates(t *testing.T) {
	tests := []struct {
		lit  string
		days int64
	}{
		{"2262-04-11", 106751},  // last date the old code got right
		{"2262-04-12", 106752},  // first date the old code clamped
		{"9999-12-31", 2932896}, // the SCD-2 "end of time" sentinel
		{"1677-09-22", -106751}, // last date the old code got right, downward
		{"1677-09-21", -106752}, // first date the old code clamped, downward
		{"0001-01-01", -719162},
		{"1970-01-01", 0},
	}
	for _, tc := range tests {
		got, ok := parseDateToEpochDaysOK(tc.lit)
		if !ok {
			t.Errorf("parseDateToEpochDaysOK(%q) ok = false, want true", tc.lit)
			continue
		}
		if got != tc.days {
			t.Errorf("parseDateToEpochDaysOK(%q) = %d, want %d", tc.lit, got, tc.days)
		}
	}
}

// TestParseDateToEpochDaysFloorsPreEpochTimeOfDay pins the adjacent defect:
// the old expression's Hours()/24 truncated toward zero, so a pre-epoch
// timestamp WITH a time-of-day rounded a day toward the epoch.
func TestParseDateToEpochDaysFloorsPreEpochTimeOfDay(t *testing.T) {
	withTime, ok := parseDateToEpochDaysOK("1969-12-31 12:00:00")
	if !ok {
		t.Fatal("parseDateToEpochDaysOK with time-of-day: ok = false")
	}
	if withTime != -1 {
		t.Errorf("parseDateToEpochDaysOK(%q) = %d, want -1", "1969-12-31 12:00:00", withTime)
	}
}
