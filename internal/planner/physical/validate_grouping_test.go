package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// PostgreSQL's grouping rule does not relax when a GROUP BY is present, it
// NARROWS: every non-aggregated SELECT / HAVING / ORDER BY expression must be
// one of the grouped expressions. checkUngrouped used to return early on
// `len(info.GroupBy) > 0` — its own comment said the legal forms were "richer,
// so those stay unchecked" — and three silent wrong answers fell out of the
// gap (#590):
//
//   - an ungrouped SELECT-list column was replaced IN PLACE by the grouping
//     key, so `SELECT count(*), b FROM t GROUP BY a` answered under the
//     headers `a | count(*)`;
//   - a bare ungrouped column in HAVING excluded EVERY group, so TLP's
//     `p` / `NOT p` / `p IS NULL` partition summed to zero rows;
//   - a comparison over one reached the executor and failed there with a
//     42703-shaped "column does not exist in the input schema".
//
// The refusals are the first half of this table. The second half is the more
// expensive mistake to make: the legal grouped shapes that must keep working,
// because a false positive breaks SQL that has an answer.
func TestValidateGroupedQueryRefusesUngroupedReferences(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{
		"events": {"id", "ts", "attrs", "region", "amount", "flag"},
		"other":  {"eid", "val"},
	}}

	tests := []struct {
		name      string
		sql       string
		wantState string // "" = must validate
		wantIn    string
	}{
		// --- #590 repro 1: the SELECT list -------------------------------
		{"select ungrouped column", "SELECT region, flag FROM events GROUP BY region", "42803", `"flag"`},
		{"select ungrouped beside aggregate", "SELECT count(*), flag FROM events GROUP BY region", "42803", `"flag"`},
		{"select ungrouped inside expression", "SELECT region, UPPER(flag) FROM events GROUP BY region", "42803", `"flag"`},
		{"select ungrouped inside arithmetic", "SELECT region, amount + 1 FROM events GROUP BY region", "42803", `"amount"`},
		{"select ungrouped qualified", "SELECT e.region, e.flag FROM events e GROUP BY e.region", "42803", `"e.flag"`},
		{"select ungrouped under a cast", "SELECT region, CAST(amount AS BIGINT) FROM events GROUP BY region", "42803", `"amount"`},
		{"select ungrouped inside a case", "SELECT region, CASE WHEN flag THEN 1 ELSE 0 END FROM events GROUP BY region", "42803", `"flag"`},
		{"grouped expression does not license its column",
			"SELECT SUBSTR(region, 1, 2), region FROM events GROUP BY SUBSTR(region, 1, 2)", "42803", `"region"`},

		// --- #590 repro 2: HAVING ----------------------------------------
		{"having bare ungrouped column", "SELECT region FROM events GROUP BY region HAVING flag", "42803", `"flag"`},
		{"having negated ungrouped column", "SELECT region FROM events GROUP BY region HAVING NOT flag", "42803", `"flag"`},
		{"having ungrouped column is null", "SELECT region FROM events GROUP BY region HAVING flag IS NULL", "42803", `"flag"`},
		{"having comparison over ungrouped column", "SELECT region FROM events GROUP BY region HAVING amount > 100", "42803", `"amount"`},
		{"having ungrouped column in a conjunction",
			"SELECT region FROM events GROUP BY region HAVING count(*) > 1 AND amount > 100", "42803", `"amount"`},
		{"having ungrouped column in an IN list",
			"SELECT region FROM events GROUP BY region HAVING amount IN (1, 2)", "42803", `"amount"`},
		{"having ungrouped column BETWEEN",
			"SELECT region FROM events GROUP BY region HAVING amount BETWEEN 1 AND 2", "42803", `"amount"`},

		// --- ORDER BY -----------------------------------------------------
		{"order by ungrouped column", "SELECT region FROM events GROUP BY region ORDER BY amount", "42803", `"amount"`},
		{"order by ungrouped expression", "SELECT region FROM events GROUP BY region ORDER BY amount + 1", "42803", `"amount"`},

		// --- A HAVING with no GROUP BY makes the table one group ----------
		{"having without group by, ungrouped select item",
			"SELECT region FROM events HAVING count(*) > 1", "42803", `"region"`},
		{"having without group by, ungrouped having column",
			"SELECT count(*) FROM events HAVING amount > 1", "42803", `"amount"`},

		// --- An unresolvable NAME is settled before grouping coverage -----
		//
		// PostgreSQL 17 resolves the function during parse analysis
		// (transformFuncCall → 42883) and runs parseCheckAggregates only
		// afterwards, so every one of these is "function … does not exist"
		// there, verbatim, and never a complaint about the argument. This
		// walk reached the argument first and reported 42803 on it, which
		// names the wrong problem and hides the only actionable half of the
		// message. Each of these reads an UNGROUPED column, so the 42803 arm
		// is genuinely reachable and the order is what decides.
		{"unknown name in aggregate position",
			"SELECT no_such_agg(amount) FROM events GROUP BY region", "42883", "no_such_agg"},
		// Inside an aggregate there is no conflict to resolve: the aggregate
		// licenses its arguments, so the 42803 arm never fires and the walk
		// stops at the aggregate name. The compiler answers this one, with
		// the same 42883 — `TestUnknownFunctionErrors` in ./test pins that
		// end to end. Recorded here so the division of labour is deliberate
		// rather than an oversight.
		{"unknown scalar inside an aggregate is left to the compiler",
			"SELECT sum(no_such_fn(amount)) FROM events GROUP BY region", "", ""},
		{"unknown scalar beside a grouped column",
			"SELECT region, no_such_fn(amount) FROM events GROUP BY region", "42883", "no_such_fn"},
		{"unknown name in having",
			"SELECT region FROM events GROUP BY region HAVING no_such_agg(amount) > 1", "42883", "no_such_agg"},
		{"unknown name in order by",
			"SELECT region FROM events GROUP BY region ORDER BY no_such_agg(amount)", "42883", "no_such_agg"},
		{"unimplemented aggregate under a group by",
			"SELECT array_agg(amount) FROM events GROUP BY region", "42883",
			"aggregate function that Wadjet does not implement"},
		// A name that DOES resolve must still be judged on its arguments —
		// the deference above is to the unresolvable name only, and this is
		// the entry that fails if it becomes a blanket skip.
		{"known scalar over an ungrouped column still refuses",
			"SELECT region, UPPER(flag) FROM events GROUP BY region", "42803", `"flag"`},

		// --- The legal shapes: these must keep validating -----------------
		{"grouped column selected", "SELECT region, count(*) FROM events GROUP BY region", "", ""},
		{"expression over the grouped column", "SELECT region || 'x', count(*) FROM events GROUP BY region", "", ""},
		{"grouped column qualified in select", "SELECT e.region FROM events e GROUP BY region", "", ""},
		{"grouped column qualified in group by", "SELECT region FROM events e GROUP BY e.region", "", ""},
		{"group by ordinal", "SELECT region, count(*) FROM events GROUP BY 1", "", ""},
		{"group by output alias", "SELECT region AS r, count(*) FROM events GROUP BY r", "", ""},
		{"group by alias of an expression", "SELECT UPPER(region) AS r, count(*) FROM events GROUP BY r", "", ""},
		{"group by ordinal of an expression", "SELECT UPPER(region) AS r, count(*) FROM events GROUP BY 1", "", ""},
		{"grouping expression repeated verbatim",
			"SELECT SUBSTR(region, 1, 2), count(*) FROM events GROUP BY SUBSTR(region, 1, 2)", "", ""},
		{"grouping expression repeated in parentheses",
			"SELECT (SUBSTR(region, 1, 2)), count(*) FROM events GROUP BY SUBSTR(region, 1, 2)", "", ""},
		{"grouped key not selected", "SELECT count(*) FROM events GROUP BY region", "", ""},
		{"extra grouped key not selected", "SELECT region FROM events GROUP BY region, amount", "", ""},
		{"aggregate consumes an ungrouped column", "SELECT region, max(amount) FROM events GROUP BY region", "", ""},
		{"aggregate over an expression of ungrouped columns",
			"SELECT region, sum(amount * 2) FROM events GROUP BY region", "", ""},
		{"having over an aggregate", "SELECT region FROM events GROUP BY region HAVING count(*) > 1", "", ""},
		{"having over a select alias", "SELECT region, count(*) AS c FROM events GROUP BY region HAVING c > 1", "", ""},
		{"having is null over an aggregate",
			"SELECT region FROM events GROUP BY region HAVING max(amount) IS NULL", "", ""},
		{"order by the grouped column", "SELECT region FROM events GROUP BY region ORDER BY region", "", ""},
		{"order by an aggregate", "SELECT region FROM events GROUP BY region ORDER BY max(amount)", "", ""},
		{"order by a select alias", "SELECT region, count(*) AS c FROM events GROUP BY region ORDER BY c", "", ""},
		{"order by an ordinal", "SELECT region FROM events GROUP BY region ORDER BY 1", "", ""},
		{"window function beside a grouped column",
			"SELECT region, row_number() OVER (ORDER BY region) FROM events GROUP BY region", "", ""},
		{"literal select item", "SELECT 1, region FROM events GROUP BY region", "", ""},
		// A correlated outer reference is constant within the inner group and
		// is not one of the inner block's own columns, so the check must not
		// judge it.
		{"correlated outer column inside a grouped subquery",
			"SELECT id FROM events e WHERE amount > (SELECT max(o.val) FROM other o GROUP BY o.eid HAVING max(o.val) > e.amount)", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := mustExtract(t, tt.sql)
			err := validateColumns(context.Background(), cat, info)
			if tt.wantState == "" {
				if err != nil {
					t.Fatalf("expected %q to validate, got: %v", tt.sql, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected a %s rejection for %q, got nil — the statement would be answered silently",
					tt.wantState, tt.sql)
			}
			if got := sqlerr.StateOf(err); got != tt.wantState {
				t.Errorf("SQLSTATE = %q, want %s (err: %v)", got, tt.wantState, err)
			}
			if tt.wantIn != "" && !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error must name the offender %s, got: %v", tt.wantIn, err)
			}
		})
	}
}

// The binder's stance — a false positive breaks a working query, a false
// negative merely lets one through — decides the uncertain cases, and the
// grouping check has to keep it. An unenumerable source means no reference in
// the block can be proven ungrouped.
func TestValidateGroupedQuerySkipsWhenAScopeIsOpen(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{"events": {"id", "region", "amount"}}}
	for _, sql := range []string{
		// A table function's columns are not enumerable.
		"SELECT region, nope FROM read_json('x.json') GROUP BY region",
		// A table the catalog cannot answer for at all: unreachable, not
		// missing (a missing one is 42P01 before this check runs).
		"SELECT region, amount FROM events GROUP BY region",
	} {
		unreachable := &fakeCatalog{tables: cat.tables, unreachable: true}
		if err := validateColumns(context.Background(), unreachable, mustExtract(t, sql)); err != nil {
			t.Errorf("an unenumerable scope must not be judged: %q rejected with %v", sql, err)
		}
	}
}

// #620: the grouping check matched a reference against grouped terms by BARE
// column NAME, so `GROUP BY t3.c1` licensed ANY column named `c1` — including
// an ungrouped `t1.c1` from a JOINed (or comma-joined) second table. The
// SELECT list then returned another column's values under `t1.c1`'s name and a
// HAVING over it excluded every group, both silently. The offending table
// reached the query the same way the fixed comma-join case did; the true
// defect was the qualifier-insensitive match, not the FROM shape, so the gate
// exercises both the collision (refused) and the spelling differences a
// grouped column is still allowed (admitted).
func TestValidateGroupedQueryResolvesColumnIdentityAcrossJoins(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{
		"t3": {"c0", "c1", "c2", "c4"},
		"t0": {"c0"},
		"t1": {"c1", "c2"},
	}}

	tests := []struct {
		name      string
		sql       string
		wantState string // "" = must validate
		wantIn    string
	}{
		// --- #620 repro: an ungrouped column from the JOINed table whose
		//     NAME collides with a grouped column of the other table -------
		{"join select ungrouped name-collision",
			"SELECT t3.c4, t1.c1 FROM t3 LEFT OUTER JOIN t1 ON t1.c2 GROUP BY t3.c1, t3.c2, t3.c4",
			"42803", `"t1.c1"`},
		{"plain join having ungrouped name-collision",
			"SELECT t3.c4 FROM t3 JOIN t1 ON t1.c2 GROUP BY t3.c1, t3.c2, t3.c4 HAVING t1.c1",
			"42803", `"t1.c1"`},
		{"left join having ungrouped name-collision",
			"SELECT t3.c4 FROM t3, t0 LEFT OUTER JOIN t1 ON t1.c2 GROUP BY t3.c1, t3.c2, t3.c4 HAVING t1.c1",
			"42803", `"t1.c1"`},
		{"right join having ungrouped name-collision",
			"SELECT t3.c4 FROM t3 RIGHT OUTER JOIN t1 ON t1.c2 GROUP BY t3.c1, t3.c2, t3.c4 HAVING t1.c1",
			"42803", `"t1.c1"`},
		{"full join having ungrouped name-collision",
			"SELECT t3.c4 FROM t3 FULL OUTER JOIN t1 ON t1.c2 GROUP BY t3.c1, t3.c2, t3.c4 HAVING t1.c1",
			"42803", `"t1.c1"`},
		// The gap was never JOIN-specific: a comma join with the same NAME
		// collision was equally silently wrong.
		{"comma join having ungrouped name-collision",
			"SELECT t3.c4 FROM t3, t1 GROUP BY t3.c1, t3.c2, t3.c4 HAVING t1.c1",
			"42803", `"t1.c1"`},

		// --- The legal shapes that identity resolution must keep admitting.
		//     A grouped column licenses every spelling of ITSELF, and an
		//     aggregate over the joined table's column is always fine. -----
		{"grouped join column selected qualified",
			"SELECT t3.c1, COUNT(*) FROM t3 JOIN t1 ON t1.c2 = t3.c2 GROUP BY t3.c1", "", ""},
		{"grouped join column selected bare",
			"SELECT c4, COUNT(*) FROM t3 JOIN t1 ON t1.c2 = t3.c2 GROUP BY t3.c4", "", ""},
		{"aggregate over the joined table's column",
			"SELECT t3.c1, MAX(t1.c1) FROM t3 JOIN t1 ON t1.c2 = t3.c2 GROUP BY t3.c1", "", ""},
		{"having aggregate over the joined table's column",
			"SELECT t3.c1 FROM t3 JOIN t1 ON t1.c2 = t3.c2 GROUP BY t3.c1 HAVING COUNT(t1.c1) > 0", "", ""},
		{"grouped column of the joined table selected",
			"SELECT t1.c1, COUNT(*) FROM t3 JOIN t1 ON t1.c2 = t3.c2 GROUP BY t1.c1", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := mustExtract(t, tt.sql)
			err := validateColumns(context.Background(), cat, info)
			if tt.wantState == "" {
				if err != nil {
					t.Fatalf("expected %q to validate, got: %v", tt.sql, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected a %s rejection for %q, got nil — the statement would be answered silently",
					tt.wantState, tt.sql)
			}
			if got := sqlerr.StateOf(err); got != tt.wantState {
				t.Errorf("SQLSTATE = %q, want %s (err: %v)", got, tt.wantState, err)
			}
			if tt.wantIn != "" && !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error must name the offender %s, got: %v", tt.wantIn, err)
			}
		})
	}
}
