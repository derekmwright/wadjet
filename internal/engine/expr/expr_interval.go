// SPDX-License-Identifier: MIT

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
