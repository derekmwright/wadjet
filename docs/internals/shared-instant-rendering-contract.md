# Shared instant rendering contract

Source: internal/engine/expr/expr_session_fns.go — func formatInstant(t time.Time) string {, moved 2026-09-11 (#1026)

formatInstant renders an instant the ONE way this engine renders one:
batch.FormatTimestamp over epoch milliseconds, which is what pgwire's send
path, the TIMESTAMP-to-text cast, the sort/compare key and formatDateResult
already use — `2023-05-17 13:24:35`, PostgreSQL's own text for a timestamp,
with a fractional second only when there is one.

Every function whose RESULT is an instant goes through here. Before #544's
second pass, twelve arms of date_trunc plus now(), current_timestamp(),
from_unixtime(), date_parse(), timezone() and date-string arithmetic each
called `Format(time.RFC3339)` directly, so `date_trunc('day', ts)` answered
`2023-05-17T00:00:00Z` where the column it came from answered
`2023-05-17 00:00:00` and where PostgreSQL answers the same. One value, two
spellings, decided by which expression the value passed through — the exact
shape #544 closed for the cast and the wire.

The instant is normalized to UTC because that is what the engine's TIMESTAMP
is: epoch milliseconds, no zone. Callers that truncate in the parsed value's
own location keep doing so — this changes how the result PRINTS, never which
instant it is.

Two functions deliberately do NOT come through here, and both are about the
value rather than the dialect:

  - to_iso8601(), whose NAME is its format contract.
  - at_timezone(), whose result is a wall clock in ANOTHER zone. Printed
    bare it would be read back as UTC, which is the misreading fnTimezone
    refuses a non-UTC zone rather than commit; its offset is load-bearing.

The resolution limit is the millisecond, because that is the engine's
instant. A caller holding finer precision loses it here, as it does on the
wire (docs/data-types.md, "One rendering").
