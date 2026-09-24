// SPDX-License-Identifier: MIT

package parquet

import (
	"errors"
	"testing"
	"time"
)

// TestDateTextHoldsPostgreSQLRange pins the DATE input function's range to
// PostgreSQL 17.11's (4714-11-24 BC … 5874897-12-31), not the int32
// carrier's: `'5874898-01-01'::date` is 22008 `date out of range` there, and
// it used to be stored here as day 2145043270 (arc VL round 4). Both ends and
// one step past each, through the text reader and the time.Time box the DML
// doors hand the writer.
func TestDateTextHoldsPostgreSQLRange(t *testing.T) {
	for _, tc := range []struct {
		in   string
		days int64
		ok   bool
	}{
		{"5874897-12-31", MaxDateDay, true},
		{"5874898-01-01", 0, false},
		{"5881580-07-11", 0, false}, // the int32 carrier's own last day
		{"1970-01-01", 0, true},
		{"2026-03-03", 20515, true},
	} {
		got, err := ParseDateDays(tc.in)
		if tc.ok {
			if err != nil || int64(got) != tc.days {
				t.Errorf("ParseDateDays(%q) = %d, %v; want %d", tc.in, got, err, tc.days)
			}
			continue
		}
		var de *DateParseError
		if !errors.As(err, &de) || de.SQLState() != "22008" || de.Error() != `date out of range: "`+tc.in+`"` {
			t.Errorf("ParseDateDays(%q) = %d, %v; want 22008 date out of range", tc.in, got, err)
		}
	}
	for _, tc := range []struct {
		t  time.Time
		ok bool
	}{
		{time.Date(5874897, 12, 31, 0, 0, 0, 0, time.UTC), true},
		{time.Date(5874898, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC), true},
		{time.Date(-4713, 11, 23, 0, 0, 0, 0, time.UTC), false},
	} {
		v, _, err := normalizeTemporalBox(TypeDate, tc.t)
		if tc.ok != (err == nil) {
			t.Errorf("normalizeTemporalBox(DATE, %v) = %v, %v; want ok=%v", tc.t, v, err, tc.ok)
		}
	}
}
