// SPDX-License-Identifier: MIT

// This file holds expr date conversion; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// --- Date/time conversion functions ---

func fnFromUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// A fractional epoch keeps its fraction to the carrier's millisecond:
	// exactIntArg truncated it first, so FROM_UNIXTIME(1718454645.5) answered
	// 12:30:45 where PostgreSQL's to_timestamp(1718454645.5) is 12:30:45.5
	// (#1266 review B6). An integer is still read exactly (#1031).
	if _, isInt := toInt64Safe(args[0]); !isInt {
		if ms, ok := epochSecondsTextMillis(numberText(args[0])); ok {
			return formatInstant(time.UnixMilli(ms))
		}
	}
	epoch := exactIntArg(args[0]) // exactly, not through a double (#1031)
	return formatInstant(time.Unix(epoch, 0))
}

// numberText is a number argument's decimal spelling: the shortest text that
// round-trips a float64, so 1718454645.123 is those digits and not the
// binary expansion a multiply by 1000 would floor to …122.
func numberText(v any) string {
	switch n := v.(type) {
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(n), 'f', -1, 32)
	case string:
		return strings.TrimSpace(n)
	}
	return fmt.Sprint(v)
}

// epochSecondsTextMillis reads a plain decimal count of seconds as epoch
// milliseconds, digits past the millisecond FLOORED toward the past (the
// carrier's rule everywhere else, #1266). ok is false for anything that is not
// `[-+]digits[.digits]` or that overflows.
func epochSecondsTextMillis(s string) (int64, bool) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimLeft(s, "+-")
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" && frac == "" {
		return 0, false
	}
	if whole == "" {
		whole = "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, false
	}
	for _, c := range frac {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	ms3 := (frac + "000")[:3]
	m, _ := strconv.ParseInt(ms3, 10, 64)
	rest := strings.Trim(frac[min(3, len(frac)):], "0") != ""
	if w > (math.MaxInt64-999)/1000 {
		return 0, false
	}
	total := w*1000 + m
	if neg {
		total = -total
		if rest {
			total-- // floor toward the past
		}
	}
	return total, true
}

func fnToUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t, ok := parseTimeOK(args[0])
	if !ok {
		return nil
	}
	return epochSeconds(t)
}

func fnDateFormat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t, ok := parseTimeOK(args[0])
	if !ok {
		return nil
	}
	return t.Format(sqlFormatToGo(toString(args[1])))
}

func fnDateParse(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t, err := time.Parse(sqlFormatToGo(toString(args[1])), toString(args[0]))
	if err != nil {
		return nil
	}
	return formatInstant(t)
}

// sqlFormatToGo converts SQL date format specifiers to Go time layout.
func sqlFormatToGo(format string) string {
	r := strings.NewReplacer(
		"%Y", "2006",
		"%m", "01",
		"%d", "02",
		"%H", "15",
		"%i", "04",
		"%s", "05",
		"%S", "05",
		"%M", "January",
		"%b", "Jan",
		"%W", "Monday",
		"%a", "Mon",
		"%p", "PM",
		"%T", "15:04:05",
	)
	return r.Replace(format)
}
