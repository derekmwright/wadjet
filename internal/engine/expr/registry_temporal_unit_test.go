// SPDX-License-Identifier: MIT

package expr

import (
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestTemporalRegistryEntriesProduceTheirDeclaredUnit is the UNIT half of the
// census (round-3 review P1): TestRegistryDeclaredTypeIsTheProducedType checks
// that a DATE- or TIMESTAMP-declared kernel boxes an int64, and an int64 is
// what BOTH units are carried in — a DATE kernel boxing epoch MILLISECONDS
// passed it (the reviewer's to_date / from_iso8601_date mutations). Here every
// temporal-declared registry entry is evaluated on a KNOWN instant and its
// value must equal that instant's encoding in the declared unit: epoch days
// for DATE, epoch milliseconds for TIMESTAMP. An entry this table does not
// name fails, so a new temporal function cannot join the registry unchecked.
func TestTemporalRegistryEntriesProduceTheirDeclaredUnit(t *testing.T) {
	clock := time.Date(2026, 3, 3, 10, 20, 30, 123e6, time.UTC)
	defer SetClockForTest(func() time.Time { return clock })()
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
	cases := map[string]struct {
		args []any
		want time.Time
	}{
		"current_date":             {nil, day(2026, 3, 3)},
		"current_timestamp":        {nil, clock},
		"now":                      {nil, clock},
		"localtimestamp":           {nil, clock},
		"pg_postmaster_start_time": {nil, processStart},
		"pg_conf_load_time":        {nil, processStart},
		"date_add":                 {[]any{"2026-03-03", int64(1)}, day(2026, 3, 4)},
		"date_sub":                 {[]any{"2026-03-03", int64(1)}, day(2026, 3, 2)},
		"date_parse":               {[]any{"2026-03-03", "%Y-%m-%d"}, day(2026, 3, 3)},
		"date_trunc":               {[]any{"hour", "2026-03-03 10:20:30"}, time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC)},
		"from_iso8601_date":        {[]any{"2026-03-03"}, day(2026, 3, 3)},
		"from_unixtime":            {[]any{int64(1772533230)}, time.Unix(1772533230, 0).UTC()},
		"last_day_of_month":        {[]any{"2026-03-03"}, day(2026, 3, 31)},
		"time_bucket":              {[]any{IntervalValue{Hours: 1}, "2026-03-03 10:20:30"}, time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC)},
		"timezone":                 {[]any{"UTC", "2026-03-03 10:20:30"}, time.Date(2026, 3, 3, 10, 20, 30, 0, time.UTC)},
		"to_date":                  {[]any{"2026-03-03 10:20:30"}, day(2026, 3, 3)},
	}
	names := DefaultRegistry.Names()
	sort.Strings(names)
	checked := 0
	for _, n := range names {
		decl, conf := DefaultRegistry.ReturnType(n).Resolve(0, nil)
		if conf != Decided || (decl.ID != batch.TypeDate && decl.ID != batch.TypeTimestamp) {
			continue
		}
		tc, ok := cases[n]
		if !ok {
			t.Errorf("temporal entry %s (declared %s) has no known-instant case here — add one", n, decl.ID)
			continue
		}
		checked++
		got := DefaultRegistry.Lookup(n)(tc.args)
		want := tc.want.UnixMilli()
		unit := "epoch milliseconds"
		if decl.ID == batch.TypeDate {
			want, unit = epochDaysOf(tc.want), "epoch days"
		}
		if got != any(want) {
			t.Errorf("%s(%v) declared %s = %#v, want %d (%s of %s)", n, tc.args, decl.ID, got, want, unit, tc.want)
		}
	}
	// The per-call declarations the planner narrows from the registry's:
	// date_add / date_sub over a DATE argument are a DATE (epoch days).
	for _, n := range []string{"date_add", "date_sub"} {
		shift := int64(1)
		want := epochDaysOf(day(2026, 3, 4))
		if n == "date_sub" {
			want = epochDaysOf(day(2026, 3, 2))
		}
		if got := DefaultRegistry.Lookup(n)([]any{civilDate{t: day(2026, 3, 3)}, shift}); got != any(want) {
			t.Errorf("%s(DATE '2026-03-03', 1) = %#v, want %d epoch days", n, got, want)
		}
	}
	if checked != len(cases) {
		t.Errorf("checked %d temporal entries, the table names %d — a named entry is no longer temporal-declared", checked, len(cases))
	}
}
