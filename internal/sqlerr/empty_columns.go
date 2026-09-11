package sqlerr

// A result-producing statement must declare columns even when it returns no rows.
// Zero columns without error must fail, not masquerade as an empty answer
// (#1008, #1010); comparing two empty schemas cannot catch that shared failure.
// Use XX000: the engine failed to describe output, not client-invalid SQL or
// an intentionally unsupported 0A000 feature (ADR-0012).
// Every result door must carry this refusal with its own path in the message.
// See docs/internals/sqlerr-empty-result-column-refusal.md for the design.
const EmptyResultSQLState = "XX000"

// EmptyResultColumns is the refusal a door makes when a statement that
// produces a result set produced one with no columns at all.
//
// door names the entry point ("embedded query", "coordinator query") the way
// every other refusal in this family does, so the message says which path
// failed to describe its output.
func EmptyResultColumns(door string) error {
	return New(EmptyResultSQLState,
		"%s: the result has no columns at all, which is never an answer — "+
			"a statement that produces a result set declares its columns or fails", door)
}
