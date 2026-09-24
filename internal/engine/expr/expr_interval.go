// SPDX-License-Identifier: MIT

// This file holds expr interval; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
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

// String renders an INTERVAL as PostgreSQL's default (`postgres` style)
// output does: `1 year 2 mons 3 days 04:05:06`, each calendar field with its
// own sign and unit (singular at 1), the clock part as a signed HH:MM:SS
// total, and `00:00:00` for zero. It is the text any consumer that prints the
// value reads — an INTERVAL literal in a select list, and since arc VL round
// 4 a text CAST to INTERVAL, which used to hand back its operand text and now
// carries the interval itself.
func (iv IntervalValue) String() string {
	var parts []string
	unit := func(n int, one, many string) {
		if n == 0 {
			return
		}
		name := many
		if n == 1 || n == -1 {
			name = one
		}
		parts = append(parts, fmt.Sprintf("%d %s", n, name))
	}
	unit(iv.Years, "year", "years")
	unit(iv.Months, "mon", "mons")
	unit(iv.Days, "day", "days")
	secs := int64(iv.Hours)*3600 + int64(iv.Minutes)*60 + int64(iv.Seconds)
	if secs != 0 || len(parts) == 0 {
		sign := ""
		if secs < 0 {
			sign, secs = "-", -secs
		}
		parts = append(parts, fmt.Sprintf("%s%02d:%02d:%02d", sign, secs/3600, secs/60%60, secs%60))
	}
	return strings.Join(parts, " ")
}
