package expr

import "time"

// The engine's CLOCK, and its one ZONE.
//
// Every clock function reads this and renders in UTC. `NOW()` and
// `CURRENT_TIMESTAMP` already did — expr.formatInstant normalizes to UTC,
// which is what "one rendering" requires of a zoneless TIMESTAMP — and
// `CURRENT_DATE` did NOT: it formatted `time.Now()` in the machine's LOCAL
// zone, so on a host west of Greenwich the two disagreed by a DAY for the
// hours between local midnight-minus-offset and UTC midnight. Found by a
// landing battery run at 20:00 ET, where `CURRENT_DATE` said 2026-09-04 and
// `NOW()` said 2026-09-05 (#870).
//
// PostgreSQL keeps them consistent through the session's TimeZone —
// `current_date = now()::date` is true there, measured on 17.11 under
// `TimeZone = UTC` — and this engine has no session zone to consult. UTC is
// the zone its rendering already commits to (ADR-0012's clock entry: a
// zoneless instant, no offset to print), so it is the zone the clock reads in
// too. If a session TimeZone is ever implemented, both move together.
var clockNow = time.Now

// SetClockForTest replaces the clock every clock function reads and returns a
// function restoring it. TEST ONLY, and NOT safe for parallel tests in the
// same process — it is a package var, like exec's spill knobs.
//
// It exists because the defect it gates is a CONDITION, not a query shape: the
// two functions agree for most of the day and disagree only inside the UTC
// offset, so a gate that reads the real clock passes on the machine that has
// the bug for sixteen hours out of twenty-four.
func SetClockForTest(f func() time.Time) func() {
	prev := clockNow
	clockNow = f
	return func() { clockNow = prev }
}
