package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ORDER BY AN AGGREGATE THE SELECT LIST DOES NOT CARRY (#597).
//
// `SELECT n_regionkey FROM nation GROUP BY n_regionkey ORDER BY MAX(n_nationkey)`
// was refused outright — "an aggregate expression that is not itself a select
// item cannot be sorted on" — on every arm, for a statement PostgreSQL 17 and
// DuckDB both answer. It is the safe direction of failure and it is still a
// gap a BI client meets.
//
// The value was already there. `BuildFromSelect` hoists every aggregate named
// only in ORDER BY onto the Aggregate node under a slot of its own (#811,
// because an aggregate that RAISES has to raise here as PostgreSQL's does), so
// the term needs no evaluation, only a name: the hidden projection is a plain
// reference to that output column. The single-process Project copies it, and
// `physical.resolveHiddenSortKeys` maps it straight onto the column the
// aggregate stage emits — nothing is materialized on either engine, which is
// why this is not the shape the refusal was really guarding.
//
// The boundary is a claim and `computed_over_an_aggregate_is_still_refused`
// attempts it: `ORDER BY COUNT(*) * 2` is a value COMPUTED from an aggregate's
// output, the DAG's sort runs between the aggregate and the gather with
// nothing in between to evaluate it, and honouring it on small inputs and
// losing it on large ones is the routing-dependent answer this whole suite
// exists to prevent. It stays refused, with PostgreSQL's own class for a shape
// PostgreSQL answers: 0A000.
//
// Every cell asserts the SEQUENCE, not the row set. The failure this fix could
// have — a hidden key that resolves to nothing and orders by NULL on every
// row — returns exactly the right rows, and only an ordered comparison sees
// it. Each ORDER BY here is TOTAL over the fixture: `MAX(id)` and `MIN(id)`
// are distinct per group, and the COUNT spellings carry a tiebreak, because an
// ORDER BY with ties has no defined order among peers (ADR-0013's legal
// nondeterminism) and the two engines really do return different peer orders.
type obuaCell struct {
	name string
	sql  string
	// want is PostgreSQL 17's answer IN ORDER.
	want []string
	// wantErrLike, when set, is the refusal every arm must give, and
	// wantState its class.
	wantErrLike string
	wantState   string
}

func obuaCells() []obuaCell {
	return []obuaCell{
		{name: "order_by_unselected_max",
			sql:  `SELECT g FROM typemx GROUP BY g ORDER BY MAX(id)`,
			want: []string{"g=<nil>", "g=2", "g=3", "g=4", "g=5", "g=6", "g=0", "g=1"}},
		{name: "order_by_unselected_min_desc",
			sql:  `SELECT g FROM typemx WHERE id < 50 GROUP BY g ORDER BY MIN(id) DESC`,
			want: []string{"g=<nil>", "g=6", "g=5", "g=4", "g=3", "g=2", "g=1", "g=0"}},
		{name: "order_by_two_terms_one_unselected",
			sql:  `SELECT g FROM typemx WHERE id < 50 GROUP BY g ORDER BY COUNT(*) DESC, MIN(id)`,
			want: []string{"g=0", "g=1", "g=2", "g=6", "g=3", "g=4", "g=5", "g=<nil>"}},
		{name: "order_by_unselected_count_with_tiebreak",
			sql:  `SELECT g FROM typemx WHERE id < 50 GROUP BY g ORDER BY COUNT(*), MIN(id)`,
			want: []string{"g=<nil>", "g=3", "g=4", "g=5", "g=1", "g=2", "g=6", "g=0"}},
		{name: "order_by_unselected_sum_over_an_expression",
			sql:  `SELECT g FROM typemx WHERE id < 50 GROUP BY g ORDER BY SUM(id * 2)`,
			want: []string{"g=<nil>", "g=3", "g=4", "g=1", "g=2", "g=5", "g=6", "g=0"}},
		{name: "order_by_unselected_count_distinct",
			sql:  `SELECT g FROM typemx WHERE id < 50 GROUP BY g ORDER BY COUNT(DISTINCT id), MIN(id)`,
			want: []string{"g=<nil>", "g=3", "g=4", "g=5", "g=1", "g=2", "g=6", "g=0"}},
		// Beside a SELECTED aggregate: the hoisted one must not disturb it,
		// and the values are asserted so a mis-paired output column shows.
		{name: "order_by_unselected_beside_a_selected_aggregate",
			sql: `SELECT g, COUNT(*) AS c FROM typemx WHERE id < 50 GROUP BY g ORDER BY MAX(id)`,
			want: []string{"g=<nil>|c=3", "g=1|c=7", "g=2|c=7", "g=3|c=6", "g=4|c=6",
				"g=5|c=6", "g=6|c=7", "g=0|c=8"}},
		// The REUSE arm: `ORDER BY COUNT(*)` beside `COUNT(*) AS c` must key
		// on the aggregate the SELECT list already computes rather than
		// hoisting a second copy — the same rule HAVING's reuse test states,
		// and a second copy is visible as a wrong VALUE under `c`.
		{name: "order_by_reuses_the_selected_aggregate",
			sql: `SELECT g, COUNT(*) AS c FROM typemx WHERE id < 50 GROUP BY g ` +
				`ORDER BY COUNT(*), MIN(id)`,
			want: []string{"g=<nil>|c=3", "g=3|c=6", "g=4|c=6", "g=5|c=6", "g=1|c=7",
				"g=2|c=7", "g=6|c=7", "g=0|c=8"}},
		// The UNGROUPED spelling, which A2's #811 settled: the aggregate
		// returns one row, so the ORDER BY is provably a no-op and the term is
		// dropped rather than materialized.
		{name: "ungrouped_order_by_an_aggregate",
			sql:  `SELECT MAX(id) AS m FROM typemx ORDER BY MIN(id)`,
			want: []string{"m=4999"}},
		// The BOUNDARY, attempted from the other side.
		{name: "computed_over_an_aggregate_is_still_refused",
			sql:         `SELECT g FROM typemx WHERE id < 50 GROUP BY g ORDER BY COUNT(*) * 2`,
			wantErrLike: "an aggregate expression that is not itself a select item cannot be sorted on",
			wantState:   "0A000"},
		// Controls that were right before and must stay right.
		{name: "ctl_order_by_a_selected_aggregates_alias",
			sql: `SELECT g, MAX(id) AS m FROM typemx GROUP BY g ORDER BY m`,
			want: []string{"g=<nil>|m=4991", "g=2|m=4993", "g=3|m=4994", "g=4|m=4995",
				"g=5|m=4996", "g=6|m=4997", "g=0|m=4998", "g=1|m=4999"}},
		{name: "ctl_order_by_the_grouped_column",
			sql: `SELECT g, MAX(id) AS m FROM typemx GROUP BY g ORDER BY g`,
			want: []string{"g=0|m=4998", "g=1|m=4999", "g=2|m=4993", "g=3|m=4994",
				"g=4|m=4995", "g=5|m=4996", "g=6|m=4997", "g=<nil>|m=4991"}},
	}
}

func TestOrderByAnUnselectedAggregateMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range obuaCells() {
		t.Run(tc.name, func(t *testing.T) {
			arms := []struct {
				name string
				run  func() ([]string, error)
			}{
				{"single", func() ([]string, error) { return obuaRows(tmdRunSingle(ctx, single, tc.sql)) }},
				{"spilled", func() ([]string, error) { return obuaRows(tmdRunSingle(ctx, spilled, tc.sql)) }},
				{"dag", func() ([]string, error) { return obuaRows(tmdRunDAG(ctx, coord, tc.sql)) }},
				{"dag-shuffled", func() ([]string, error) { return obuaRows(tmdRunDAG(ctx, coordB, tc.sql)) }},
			}
			for _, arm := range arms {
				got, err := arm.run()
				if tc.wantErrLike != "" {
					if err == nil {
						t.Errorf("%s arm: ANSWERED %v — the refusal this cell records is lifted. "+
							"That is a change to what the engine implements, not a bug fix: "+
							"assert the ORDER, and say where the term is evaluated on the DAG.\n  SQL: %s",
							arm.name, got, tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), tc.wantErrLike) {
						t.Errorf("%s arm: %v\n  want one containing %q\n  SQL: %s",
							arm.name, err, tc.wantErrLike, tc.sql)
					}
					if st := sqlerr.StateOf(err); st != tc.wantState {
						t.Errorf("%s arm: SQLSTATE %q, want %q\n  SQL: %s",
							arm.name, st, tc.wantState, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
					t.Errorf("%s arm, IN ORDER\n  got  %v\n  want %v (live PostgreSQL 17)\n"+
						"  The row SET is not the assertion: a hidden sort key that resolves to\n"+
						"  nothing orders by NULL on every row and returns exactly these rows in\n"+
						"  an arbitrary sequence.\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// obuaRows renders a result IN ORDER, one string per row.
func obuaRows(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			parts = append(parts, c+"="+obuaValue(r[c]))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, nil
}

func obuaValue(v any) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", v)
}
