// This file holds expr date accessors; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
	"time"
)

// --- Date: additional accessors ---

func fnQuarter(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64((int(t.Month())-1)/3 + 1)
}

func fnWeek(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	_, week := t.ISOWeek()
	return float64(week)
}

func fnDayOfWeek(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Weekday())
}

func fnDayOfYear(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.YearDay())
}

func fnLastDayOfMonth(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	firstOfNext := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
	last := firstOfNext.AddDate(0, 0, -1)
	return last.Format("2006-01-02")
}

func fnCurrentTimestamp(args []any) any {
	return formatInstant(clockNow())
}

func fnAtTimezone(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	tz := toString(args[1])
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil
	}
	// NOT formatInstant. Every other timestamp-valued function renders through
	// it (#544), and this one must not: the result is a wall clock in `loc`,
	// and the engine's one rendering has no zone, so printing it bare would
	// publish 07:00 New York as 07:00 UTC — five hours wrong the moment
	// anything parses it back. Keeping the offset is the same position
	// fnTimezone takes when it declines a non-UTC zone rather than convert
	// with a guessed sign; both wait on a naive-timestamp type.
	return t.In(loc).Format(time.RFC3339)
}

// ProcessStart returns the instant this process began — the value
// pg_postmaster_start_time() reports. Exported so a gate can assert that the
// wire carries THIS process's start EXACTLY, instead of bounding it against
// wall-clock time at assertion time. That bound is a statement about how long
// the rest of a test binary ran, not about the server: at 300 seconds it
// failed permanently once the -race suite crossed five minutes (#563), and
// with the bound removed it passes for a server reporting 1970 (#518).
func ProcessStart() time.Time { return processStart }

// fnPgPostmasterStartTime implements pg_postmaster_start_time(). DataGrip asks
// for it while opening a connection (`select round(extract(epoch from
// pg_postmaster_start_time() at time zone 'UTC')) as startup_time`) to label
// the session with the server's uptime.
//
// PostgreSQL reports when the postmaster — the process that owns the cluster —
// started. `wadjet serve` is that process, so process start is the honest
// answer. The value is a timestamp in the representation now() and
// current_timestamp use — formatInstant text, the engine's one instant
// rendering: the scalar registry is func([]any) any with no type channel, and
// every temporal function downstream (parseTime, epoch, timezone) reads that
// form. It carries the MILLISECOND now, where RFC3339 second-truncated it.
func fnPgPostmasterStartTime(args []any) any {
	return formatInstant(processStart)
}

// fnEpoch implements EXTRACT(EPOCH FROM ts), which the parser rewrites to
// epoch(ts): seconds since 1970-01-01T00:00:00Z. It is derived from the
// resolved instant, never from the column's raw stored number — a DATE column
// holds days and a TIMESTAMP column holds milliseconds, so passing the stored
// value through unchanged answered 9568 for a 1996 date (issue #319).
// resolveTemporalArgs converts those columns to a time.Time before this runs;
// text timestamps are parsed by parseTime as they always were.
func fnEpoch(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return float64(t.Unix())
}

// fnTimezone implements PostgreSQL's timezone(zone, timestamp), the canonical
// form of the `<timestamp> AT TIME ZONE <zone>` operator that the parser
// rewrites to this call.
//
// PostgreSQL gives the operator two directions, chosen by the input type:
//
//	timestamptz AT TIME ZONE zone → timestamp   (absolute instant → wall clock in zone)
//	timestamp   AT TIME ZONE zone → timestamptz (wall clock in zone → absolute instant)
//
// Wadjet has one timestamp type and its values are absolute instants
// (vectors hold epoch seconds, the scalar layer passes RFC3339 text), so only
// the first direction has a meaning here. But PostgreSQL's result for that
// direction is a *naive* timestamp, which this type system cannot represent.
// Rendering the instant in the zone instead — keeping the offset, so the
// instant is preserved — disagrees with PostgreSQL for everything downstream
// that reads the naive result as UTC: PostgreSQL's EXTRACT(EPOCH FROM ts AT
// TIME ZONE 'America/New_York') is the zone's offset away from EXTRACT(EPOCH
// FROM ts), while an instant-preserving conversion leaves it equal.
//
// So the zone is restricted to UTC, the case where the two readings coincide:
// an instant and its UTC wall clock are the same count of seconds since the
// epoch, and the EXTRACT(EPOCH FROM …) round trip is exact. Every other zone
// is rejected rather than converted with a wrong sign — as a compile-time
// error when the zone is a literal (see compileFuncCallNode) and as NULL here
// when it is not. Widening this means giving the type system a naive-timestamp
// type first.
func fnTimezone(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if !isUTCZone(toString(args[0])) {
		return nil
	}
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	return formatInstant(t)
}

// isUTCZone reports whether a zone name is one of the spellings of UTC that
// AT TIME ZONE accepts. All of these are zero offset with no DST rule, so the
// conversion fnTimezone declines to guess at does not arise for them.
func isUTCZone(zone string) bool {
	switch strings.ToUpper(strings.TrimSpace(zone)) {
	case "UTC", "GMT", "Z", "ETC/UTC", "ETC/GMT":
		return true
	}
	return false
}

func fnHumanReadableSeconds(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	total := int64(ToFloat64(args[0]))
	if total < 0 {
		total = -total
	}
	days := total / 86400
	total %= 86400
	hours := total / 3600
	total %= 3600
	minutes := total / 60
	seconds := total % 60
	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", days, plural(days)))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hour%s", hours, plural(hours)))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minute%s", minutes, plural(minutes)))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d second%s", seconds, plural(seconds)))
	}
	return strings.Join(parts, ", ")
}

func plural(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}
