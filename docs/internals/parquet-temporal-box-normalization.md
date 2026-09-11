# Parquet temporal box normalization

Source: internal/storage/parquet/date_parse.go — func normalizeTemporalBox(t TypeID, val any) (any, bool, error) {, moved 2026-09-11 (#1026)
Superseded: TIMESTAMP time.Time is converted by int64LeafValue, not normalizeTemporalBox; DATE and DURATION conversions remain here.

normalizeTemporalBox converts a box handed to a DATE, TIMESTAMP or DURATION
column into the integer that column's leaf stores — days, milliseconds and
nanoseconds respectively — and reports whether it converted anything.

It exists because "which boxes are acceptable" was answered in two places
that disagreed. ingest.checkType admits, per type:

	DATE       time.Time, int32, int64, string
	TIMESTAMP  time.Time, int64, string
	DURATION   time.Duration, int64, string

and of those the writer converted exactly one — a DATE string. Every other
non-integer box fell through toInt32/toInt64's `default: return 0` and was
stored as ZERO, silently: a time.Time DATE (the box the SQL literal path
produces, #673), a string TIMESTAMP and a string or time.Duration DURATION
(time.Duration is a NAMED type, so `case int64` in a Go type switch does not
match it). An accepted box that stores a wrong value is worse than a
rejected one, so this is the single conversion both boundaries use, and a
box it cannot convert is an error naming the column and the row rather than
a zero.

A DATE takes the CALENDAR DATE as written in the time's own location, not
its UTC instant — a DATE is a date, and this is also the rule
ingest.formatPartitionValue already formats a DATE partition key by
(t.Format("2006-01-02")), so a partition's directory name and its stored
value cannot disagree.
