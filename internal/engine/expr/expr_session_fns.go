// This file holds expr session fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/hex"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// --- Session / catalog information functions ---
//
// PostgreSQL clients (pgJDBC, DataGrip, psql, Superset) open a connection by
// asking who and where they are: current_user, current_schema,
// current_database. These are answered here rather than only in the pgwire
// introspection layer so that a query mixing them with real columns — or
// selecting three of them at once — executes as an ordinary query with an
// ordinary result shape.
//
// The values are server constants. ScalarFunc is func([]any) any and
// DefaultRegistry is process-global, so a scalar function cannot see the
// calling connection's identity; a per-session answer would need a
// context-carrying evaluation path that does not exist. The constants match
// what pgwire reports for an unauthenticated session.
const (
	// SessionUser is the user name reported by current_user / session_user /
	// user / current_role.
	SessionUser = "wadjet"
	// SessionCatalog is the database name reported by current_catalog /
	// current_database().
	SessionCatalog = "wadjet"
	// SessionSchema is the schema reported by current_schema.
	SessionSchema = "public"
	// ServerVersion is the answer to version(). PostgreSQL drivers parse the
	// leading "PostgreSQL <major>" to decide which protocol features and
	// catalog queries they may use, so the string keeps that prefix.
	ServerVersion = "PostgreSQL 15.0 (Wadjet analytical query engine)"
)

func fnVersion(args []any) any { return ServerVersion }

func fnCurrentUser(args []any) any { return SessionUser }

func fnCurrentCatalog(args []any) any { return SessionCatalog }

func fnCurrentSchema(args []any) any { return SessionSchema }

// fnCurrentSchemas mirrors PostgreSQL's current_schemas(include_implicit),
// which returns the search path as a text array. pgJDBC calls it during
// connection setup; the value is rendered in PostgreSQL's array text format.
func fnCurrentSchemas(args []any) any {
	includeImplicit := false
	if len(args) > 0 {
		switch v := args[0].(type) {
		case bool:
			includeImplicit = v
		case string:
			includeImplicit = strings.EqualFold(v, "true") || strings.EqualFold(v, "t")
		}
	}
	if includeImplicit {
		return "{pg_catalog," + SessionSchema + "}"
	}
	return "{" + SessionSchema + "}"
}

// fnDateDiff returns the number of whole days between two instants.
// Usage: date_diff(date1, date2) → integer
//
// The difference is taken in integer milliseconds, so no day count rounds
// through a float64, and a partial day truncates toward the PAST rather than
// toward zero — otherwise a negative difference would round the other way
// from a positive one, and the same pair of instants would answer differently
// on either side of the epoch.
func fnDateDiff(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t1, _, ok1 := parseDateArg(args[0])
	t2, _, ok2 := parseDateArg(args[1])
	if !ok1 || !ok2 {
		return nil
	}
	const msPerDay = 24 * 60 * 60 * 1000
	diff := t1.UnixMilli() - t2.UnixMilli()
	days := diff / msPerDay
	if diff < 0 && diff%msPerDay != 0 {
		days--
	}
	return float64(days)
}

// fnDateAdd adds days (or an interval) to a date or timestamp.
// Usage: date_add(date, days) → date string
//
//	date_add(date, interval) → date string
func fnDateAdd(args []any) any { return dateShift(args, false) }

// fnDateSub subtracts days (or an interval) from a date or timestamp.
// Usage: date_sub(date, days) → date string
//
//	date_sub(date, interval) → date string
func fnDateSub(args []any) any { return dateShift(args, true) }

// dateShift is the shared body of date_add / date_sub.
//
// A numeric second argument counts DAYS — what a DATE argument has always
// meant here, and what Spark/Hive date_add means; an INTERVAL keeps its own
// unit. The result preserves the input's time-of-day: before issue #322 both
// functions formatted the result "2006-01-02" unconditionally, so a TIMESTAMP
// argument silently lost its clock on the way out. A whole-day argument still
// renders as a calendar date.
//
// The interval branch is intervalShift, shared verbatim with `date ± INTERVAL`
// in BinOp.Eval so date_sub(d, INTERVAL '90' DAY) and d - INTERVAL '90' DAY
// cannot disagree (issue #332).
func dateShift(args []any, subtract bool) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if iv, ok := args[1].(IntervalValue); ok {
		return intervalShift(args[0], iv, subtract)
	}
	t, dateOnly, ok := parseDateArg(args[0])
	if !ok {
		return nil
	}
	days := int(ToFloat64(args[1]))
	if subtract {
		days = -days
	}
	return formatDateResult(t.AddDate(0, 0, days), dateOnly)
}

// intervalShift applies an INTERVAL to a date-valued operand. It is the shared
// body of `date ± INTERVAL` (BinOp.Eval) and date_add / date_sub with an
// interval argument (dateShift) — one function, so the operator and the
// function family cannot answer the same question differently (issue #332).
//
// There is ONE instant renderer now. A TEXT operand goes through
// dateAddInterval and a resolved temporal COLUMN through formatDateResult, and
// both end at formatInstant — so the output format no longer depends on how
// the value reached the operator, which is what #322 settled for date_add over
// a TIMESTAMP column and what #544's second pass finished for the rest. A
// whole DAY still renders as a calendar date on both paths, which is what
// TPC-H Q1's pinned `DATE '1998-12-01' - INTERVAL '90' DAY` reads.
func intervalShift(v any, iv IntervalValue, subtract bool) any {
	if ds, ok := v.(string); ok {
		return dateAddInterval(ds, iv, subtract)
	}
	t, dateOnly, ok := parseDateArg(v)
	if !ok {
		return nil
	}
	// An interval carrying a time component makes the result an instant even
	// when the input was a whole day.
	dateOnly = dateOnly && iv.Hours == 0 && iv.Minutes == 0 && iv.Seconds == 0
	return formatDateResult(addInterval(t, iv, subtract), dateOnly)
}

// parseDateArg resolves a date-arithmetic argument to the instant it denotes,
// and reports whether that instant is a whole DAY — which is what decides how
// the result renders (see formatDateResult).
//
// A temporal COLUMN arrives here already resolved, as a time.Time (TIMESTAMP)
// or a civilDate (DATE), because resolveTemporalArgs converts it while the
// declared column type is still in hand. So a bare number reaching this point
// is genuinely a bare number, and keeps the days-since-epoch reading it has
// always had in this family — unchanged by #319, and unchanged here.
func parseDateArg(v any) (time.Time, bool, bool) {
	switch tv := v.(type) {
	case civilDate:
		return tv.t, true, true
	case time.Time:
		return tv, false, true
	case string:
		// A bare "2006-01-02" is a whole day; any layout with a clock is
		// an instant whose clock the result keeps.
		if t, err := time.Parse("2006-01-02", tv); err == nil {
			return t, true, true
		}
		t := parseDateValue(tv)
		if t.IsZero() {
			return time.Time{}, false, false
		}
		return t, false, true
	}
	t := parseDateValue(v)
	if t.IsZero() {
		return time.Time{}, false, false
	}
	return t, true, true
}

// formatInstant renders an instant the ONE way this engine renders one:
// batch.FormatTimestamp over epoch milliseconds, which is what pgwire's send
// path, the TIMESTAMP-to-text cast, the sort/compare key and formatDateResult
// already use — `2023-05-17 13:24:35`, PostgreSQL's own text for a timestamp,
// with a fractional second only when there is one.
//
// Every function whose RESULT is an instant goes through here. Before #544's
// second pass, twelve arms of date_trunc plus now(), current_timestamp(),
// from_unixtime(), date_parse(), timezone() and date-string arithmetic each
// called `Format(time.RFC3339)` directly, so `date_trunc('day', ts)` answered
// `2023-05-17T00:00:00Z` where the column it came from answered
// `2023-05-17 00:00:00` and where PostgreSQL answers the same. One value, two
// spellings, decided by which expression the value passed through — the exact
// shape #544 closed for the cast and the wire.
//
// The instant is normalized to UTC because that is what the engine's TIMESTAMP
// is: epoch milliseconds, no zone. Callers that truncate in the parsed value's
// own location keep doing so — this changes how the result PRINTS, never which
// instant it is.
//
// Two functions deliberately do NOT come through here, and both are about the
// value rather than the dialect:
//
//   - to_iso8601(), whose NAME is its format contract.
//   - at_timezone(), whose result is a wall clock in ANOTHER zone. Printed
//     bare it would be read back as UTC, which is the misreading fnTimezone
//     refuses a non-UTC zone rather than commit; its offset is load-bearing.
//
// The resolution limit is the millisecond, because that is the engine's
// instant. A caller holding finer precision loses it here, as it does on the
// wire (docs/data-types.md, "One rendering").
func formatInstant(t time.Time) string {
	return batch.FormatTimestamp(t.UTC().UnixMilli())
}

// formatDateResult renders a date-arithmetic result. A whole day stays a
// calendar date; an instant goes through formatInstant, so date_add over a
// TIMESTAMP column reads exactly the way the column itself does.
func formatDateResult(t time.Time, dateOnly bool) string {
	if dateOnly {
		return t.UTC().Format("2006-01-02")
	}
	return formatInstant(t)
}

// fnToDate converts a date, a timestamp or a string to a calendar date.
func fnToDate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseDateValue(args[0])
	if t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02")
}

// parseDateValue parses a date from various formats.
//
// A temporal column argument arrives already resolved to its instant (see
// resolveTemporalArgs), as a time.Time or — for a DATE column — a civilDate;
// a bare number is a bare number, and still reads as days since the epoch.
func parseDateValue(v any) time.Time {
	switch tv := v.(type) {
	case time.Time:
		return tv
	case civilDate:
		return tv.t
	case string:
		// THE accept-set, offset discarded — the same function the writer and
		// the comparison kernels read, so a CAST answers the instant the
		// literal SPELLS rather than the instant its offset names (B2).
		if t, ok := parquet.ParseTimestampWallClock(tv); ok {
			return t
		}
	case int32:
		// Days since epoch
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	case int64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	case float64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	}
	return time.Time{}
}

// fnUUIDVersion extracts the version from a UUID string.
func fnUUIDVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	raw := parseUUIDHex(s)
	if raw == nil {
		return nil
	}
	return float64(raw[6] >> 4)
}

// fnUUIDToString formats raw UUID bytes as a standard string.
func fnUUIDToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// Already formatted?
	if len(s) == 36 && s[8] == '-' {
		return s
	}
	raw := parseUUIDHex(s)
	if raw == nil {
		return nil
	}
	var buf [36]byte
	hex.Encode(buf[0:8], raw[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], raw[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], raw[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], raw[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], raw[10:16])
	return string(buf[:])
}

// parseUUIDHex parses a UUID string (with or without dashes) into 16 bytes.
// parseUUIDHex is the boxed-pair path's UUID literal, delegating to the
// kernel's one parser for the same reason macLitToInt64 does — the braced
// spelling PostgreSQL accepts reached the vectorized kernel and not this copy
// (#627, protocol method 6).
func parseUUIDHex(s string) []byte {
	raw, ok := kernel.UUIDLiteralToRaw(s)
	if !ok || len(raw) != 16 {
		return nil
	}
	return []byte(raw)
}

// ipv4LitToInt64 parses a dotted-quad IPv4 literal into the int64 encoding
// TypeIPv4 columns use — batch.Vector.SetValue's TypeIPv4 case and
// writer.go's prepareRows agree on this: the address's big-endian uint32
// widened to int64. ok is false for anything that isn't a valid IPv4 text
// form (including an IPv6 literal — To4 refuses it).
// ipv4LitToInt64 DELEGATES to the kernel's one parser, for the reason
// macLitToInt64 below already does: a third copy of the grammar is a third
// accept-set, and this one is the copy the COMPILED comparison node reads —
// which is the evaluator the DAG reaches. `c_ipv4 = '10.0.0.1/32'` is the
// address itself on the server and on the vectorized arm, and this copy read
// it as no address at all, so the DAG answered 0 rows where the single arm
// answered 1 (#627 round 2, B1).
func ipv4LitToInt64(s string) (int64, bool) {
	return kernel.IPv4LitKey(s)
}

// macLitToInt64 parses a MAC literal into the int64 encoding TypeMAC columns
// use — batch.Vector.SetValue's TypeMAC case: the six bytes packed
// big-endian into the low 48 bits.
// macLitToInt64 is the BOXED-PAIR path's MAC literal, delegating to the
// kernel's one parser for the reason exec.parseMACFilterVal does: a second
// copy of the grammar is a second answer to "is this a MAC", and #627's
// widening reached the vectorized kernel while this copy still refused the
// same spelling — so `c_mac = '08002b:010203'` answered on one comparison path
// and raised 22P02 on another (protocol method 6).
func macLitToInt64(s string) (int64, bool) {
	return kernel.MACLitKey(s)
}
