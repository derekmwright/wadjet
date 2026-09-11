# Timezone utc only boundary

Source: internal/engine/expr/expr_date_accessors.go — func fnTimezone(args []any) any {, moved 2026-09-11 (#1026)
Superseded: TIMESTAMP vectors store epoch milliseconds, not seconds; scalar instant rendering uses formatInstant rather than RFC3339.

fnTimezone implements PostgreSQL's timezone(zone, timestamp), the canonical
form of the `<timestamp> AT TIME ZONE <zone>` operator that the parser
rewrites to this call.

PostgreSQL gives the operator two directions, chosen by the input type:

	timestamptz AT TIME ZONE zone → timestamp   (absolute instant → wall clock in zone)
	timestamp   AT TIME ZONE zone → timestamptz (wall clock in zone → absolute instant)

Wadjet has one timestamp type and its values are absolute instants
(vectors hold epoch seconds, the scalar layer passes RFC3339 text), so only
the first direction has a meaning here. But PostgreSQL's result for that
direction is a *naive* timestamp, which this type system cannot represent.
Rendering the instant in the zone instead — keeping the offset, so the
instant is preserved — disagrees with PostgreSQL for everything downstream
that reads the naive result as UTC: PostgreSQL's EXTRACT(EPOCH FROM ts AT
TIME ZONE 'America/New_York') is the zone's offset away from EXTRACT(EPOCH
FROM ts), while an instant-preserving conversion leaves it equal.

So the zone is restricted to UTC, the case where the two readings coincide:
an instant and its UTC wall clock are the same count of seconds since the
epoch, and the EXTRACT(EPOCH FROM …) round trip is exact. Every other zone
is rejected rather than converted with a wrong sign — as a compile-time
error when the zone is a literal (see compileFuncCallNode) and as NULL here
when it is not. Widening this means giving the type system a naive-timestamp
type first.
