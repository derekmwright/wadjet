// This file holds expr date conversion; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strings"
	"time"
)

// --- Date/time conversion functions ---

func fnFromUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	epoch := int64(ToFloat64(args[0]))
	return formatInstant(time.Unix(epoch, 0))
}

func fnToUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return float64(t.Unix())
}

func fnDateFormat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
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
