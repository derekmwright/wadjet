// This file holds expr interval; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"time"
)

// --- Interval type ---

// IntervalValue represents a SQL INTERVAL (e.g., INTERVAL '30' DAY).
type IntervalValue struct {
	Years   int
	Months  int
	Days    int
	Hours   int
	Minutes int
	Seconds int
}

// addInterval applies an interval to an instant.
func addInterval(t time.Time, iv IntervalValue, subtract bool) time.Time {
	sign := 1
	if subtract {
		sign = -1
	}
	t = t.AddDate(sign*iv.Years, sign*iv.Months, sign*iv.Days)
	return t.Add(time.Duration(sign) * (time.Duration(iv.Hours)*time.Hour +
		time.Duration(iv.Minutes)*time.Minute +
		time.Duration(iv.Seconds)*time.Second))
}

// dateAddInterval applies an interval to a date/timestamp string.
func dateAddInterval(dateStr string, iv IntervalValue, subtract bool) string {
	t := parseDateValue(dateStr)
	if t.IsZero() {
		return ""
	}
	t = addInterval(t, iv, subtract)
	// A whole day is a calendar date; anything with a clock is an instant and
	// renders the one way this engine renders instants (formatInstant).
	if iv.Hours == 0 && iv.Minutes == 0 && iv.Seconds == 0 &&
		t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 {
		return t.Format("2006-01-02")
	}
	return formatInstant(t)
}
