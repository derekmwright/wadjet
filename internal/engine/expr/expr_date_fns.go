// This file holds expr date fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
	"time"
)

// --- Date/time functions ---

func fnNow(args []any) any {
	return formatInstant(clockNow())
}

func fnYear(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Year())
}

func fnMonth(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Month())
}

func fnDay(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Day())
}

func fnHour(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Hour())
}

func fnMinute(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Minute())
}

func fnSecond(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Second())
}

func fnDateTrunc(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	unit := strings.ToLower(fmt.Sprint(args[0]))
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	switch unit {
	case "year":
		return formatInstant(time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()))
	case "quarter":
		q1 := time.Month((int(t.Month())-1)/3*3 + 1)
		return formatInstant(time.Date(t.Year(), q1, 1, 0, 0, 0, 0, t.Location()))
	case "month":
		return formatInstant(time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()))
	case "week":
		// ISO convention (DuckDB/Postgres): truncate to Monday.
		d := t
		for d.Weekday() != time.Monday {
			d = d.AddDate(0, 0, -1)
		}
		return formatInstant(time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, t.Location()))
	case "day":
		return formatInstant(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
	case "hour":
		return formatInstant(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()))
	case "minute":
		return formatInstant(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()))
	case "second":
		return formatInstant(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()))
	case "decade":
		return formatInstant(time.Date(t.Year()/10*10, 1, 1, 0, 0, 0, 0, t.Location()))
	case "century":
		// PostgreSQL's centuries START at year 1: date_trunc('century',
		// '2023-05-17') is 2001-01-01, not 2000-01-01. Measured live.
		return formatInstant(time.Date((t.Year()-1)/100*100+1, 1, 1, 0, 0, 0, 0, t.Location()))
	case "millennium":
		return formatInstant(time.Date((t.Year()-1)/1000*1000+1, 1, 1, 0, 0, 0, 0, t.Location()))
	case "milliseconds", "microseconds":
		// This engine's instants are epoch MILLISECONDS, so both of these are
		// the identity here. PostgreSQL truncates a microsecond value to the
		// millisecond for the first and answers the value itself for the
		// second; over a millisecond-resolution instant those coincide.
		return formatInstant(t)
	default:
		// PostgreSQL refuses a unit its timestamp functions do not know rather
		// than answering NULL, and the accepted set is exactly the thirteen
		// above — `epoch`, `doy` and `dow` are EXTRACT's fields and are refused
		// here on the server too, measured live (#855).
		raiseUnitNotRecognized(unit)
		return nil
	}
}

func fnExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	unit := strings.ToLower(fmt.Sprint(args[0]))
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	// Unit set kept identical to vecExtract's, so the two paths answer the
	// same question — quarter/week/epoch used to exist only in the kernel.
	switch unit {
	case "year":
		return float64(t.Year())
	case "quarter":
		return float64((int(t.Month())-1)/3 + 1)
	case "month":
		return float64(t.Month())
	case "week":
		_, week := t.ISOWeek()
		return float64(week)
	case "day":
		return float64(t.Day())
	case "hour":
		return float64(t.Hour())
	case "minute":
		return float64(t.Minute())
	case "second":
		return float64(t.Second())
	case "dow", "dayofweek":
		return float64(t.Weekday())
	case "doy", "dayofyear":
		return float64(t.YearDay())
	case "epoch":
		return float64(t.Unix())
	default:
		return nil
	}
}

func fnCurrentDate(args []any) any {
	// UTC, the zone every other clock function renders in (clock.go). It read
	// the machine's LOCAL date, so `CURRENT_DATE` and `CAST(NOW() AS DATE)`
	// named two different days for the hours between local midnight and UTC
	// midnight — #870.
	return clockNow().UTC().Format("2006-01-02")
}
