package coordinator

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/sqlgen"
)

// #487 gave sqlgen two shapes — `LIMIT 0` and `OFFSET n` — and the consumer
// that compares its output was not extended with them. `diffOneQuery` asked
// `q.Limit > 0` in two places: once to choose a COUNT-only comparison, and
// once to decide whether to re-run the stripped query. A bare `OFFSET n` has
// `Limit == 0`, so it took the EXACT-multiset path, and
//
//	SELECT o_custkey, o_shippriority FROM orders OFFSET 3
//
// compared 14997 rows against 14997 rows and reported the first differing
// one — a query whose row set SQL does not pin down without a total ORDER BY
// (ADR-0013's legal nondeterminism). It was also flaky rather than
// deterministic: WHICH rows the distributed arm skips depends on scheduling,
// so the failing seed moved between runs.
//
// These two tests hold the fix without running a query, so they cost nothing
// and cannot themselves flake.

// TestARowWindowIsComparedByCountNotByRows is the disposition, per shape.
func TestARowWindowIsComparedByCountNotByRows(t *testing.T) {
	base := func() *sqlgen.Query {
		return &sqlgen.Query{
			Select:  []string{"t1_key"},
			Tables:  []string{"t1"},
			OrderBy: []string{"t1_key"},
		}
	}
	for _, tc := range []struct {
		name    string
		mutate  func(*sqlgen.Query)
		windows bool
	}{
		{"no_window", func(*sqlgen.Query) {}, false},
		{"limit", func(q *sqlgen.Query) { q.Limit = 5 }, true},
		{"offset", func(q *sqlgen.Query) { q.Offset = 3 }, true},
		{"limit_and_offset", func(q *sqlgen.Query) { q.Limit = 5; q.Offset = 3 }, true},
		{"limit_zero", func(q *sqlgen.Query) { q.LimitZero = true }, true},
		{"limit_zero_offset", func(q *sqlgen.Query) { q.LimitZero = true; q.Offset = 2 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := base()
			tc.mutate(q)
			if got := windowsTheRowSet(q); got != tc.windows {
				t.Fatalf("windowsTheRowSet(%q) = %v, want %v — a query that keeps only "+
					"part of its input must compare by COUNT, because which rows it keeps "+
					"is not determined without a total order", q.SQL(), got, tc.windows)
			}
			if !tc.windows {
				return
			}
			// The stripped form is what carries the multiset comparison, so it
			// must have no window left at all — otherwise the exact compare
			// happens there instead, which is where the bare OFFSET landed.
			s := strippedOfRowWindow(q)
			if windowsTheRowSet(s) {
				t.Errorf("the stripped form of %q is %q, which still windows its row set",
					q.SQL(), s.SQL())
			}
			if len(s.OrderBy) != 0 {
				t.Errorf("the stripped form of %q keeps an ORDER BY (%v); the multiset "+
					"comparison does not need one and a partial order cannot earn one",
					q.SQL(), s.OrderBy)
			}
			// And stripping does not mutate the original.
			if !windowsTheRowSet(q) {
				t.Errorf("strippedOfRowWindow mutated its argument")
			}
		})
	}
}

// TestEveryQueryFieldIsClassifiedForRowWindowing is the completeness half: a
// field added to sqlgen.Query that keeps only part of the rows must be
// classified, or the next #487 repeats this one.
//
// It walks the struct rather than listing the fields a second time, so a new
// field fails until somebody decides which list it belongs in.
func TestEveryQueryFieldIsClassifiedForRowWindowing(t *testing.T) {
	// Fields that decide WHICH of its input's rows a query returns. Every one
	// of these must make windowsTheRowSet true and must be cleared by
	// strippedOfRowWindow.
	windowing := map[string]bool{
		"Limit":     true,
		"Offset":    true,
		"LimitZero": true,
	}
	// Fields that shape the rows or their order but not which of them survive
	// a window. OrderBy is here and is stripped anyway: it cannot make the
	// comparison exact on its own, and dropping it is what makes the stripped
	// multiset comparison legal.
	notWindowing := map[string]bool{
		"Distinct": true, "Select": true, "Tables": true, "On": true,
		"Where": true, "GroupBy": true, "Having": true, "OrderBy": true,
	}

	typ := reflect.TypeOf(sqlgen.Query{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if windowing[name] == notWindowing[name] {
			t.Errorf("sqlgen.Query.%s is in neither list (or both): decide whether it "+
				"decides WHICH rows the query returns. If it does, windowsTheRowSet must "+
				"see it and strippedOfRowWindow must clear it; #487 is what happens when "+
				"one is added and neither is told", name)
		}
	}
	for name := range windowing {
		if _, ok := typ.FieldByName(name); !ok {
			t.Errorf("sqlgen.Query has no field %s; the classification is stale", name)
		}
	}
	for name := range notWindowing {
		if _, ok := typ.FieldByName(name); !ok {
			t.Errorf("sqlgen.Query has no field %s; the classification is stale", name)
		}
	}
}
