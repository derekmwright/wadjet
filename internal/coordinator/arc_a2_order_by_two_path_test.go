package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// a2OrderRun is na2Run without the unconditional sort.
//
// na2Run sorts its rendered rows so an unordered multiset compares totally,
// which is right for every census that does not care about sequence. Half the
// cells here DO: the whole point of a sort key is the order it produces, and a
// sorted comparison cannot tell `ORDER BY 1` from `ORDER BY 1 DESC` — both
// return the same three rows.
func a2OrderRun(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			switch t := r[c].(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			case float64:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, t))
			case float32:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, float64(t)))
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, r[c], r[c]))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, nil
}

// ORDER BY, four arms, every answer anchored to live PostgreSQL 17.
//
// The cells here share one theme: a sort term the planner could not name, and
// what it did about that. `SELECT * ... ORDER BY 1` was REFUSED on every arm
// (#810) — the most ordinary SQL in the cluster, and what psql, DataGrip,
// Superset and every "preview this table" button emit.
//
// `want` is PostgreSQL's answer rendered by na2Run and sorted, so ROW ORDER is
// not what these cells assert — the ordered digest lives in benchmarks/tpch.
// What they assert is that the query ANSWERS at all, with the right rows, and
// that the shapes still refused are refused identically on both engines with
// the class PostgreSQL uses.
type a2OrderCell struct {
	issue, name, sql string
	want             []string
	// wantErrLike is a substring every arm's error must carry.
	wantErrLike string
	// wantState is the SQLSTATE every arm's error must carry, asserted
	// beside the message so a right refusal under a missing class fails.
	wantState string
	// pgSays records PostgreSQL 17's own disposition in prose, measured live.
	pgSays string
	// ordered, when set, asserts the row SEQUENCE rather than the multiset —
	// the point of a sort key is the order, and a multiset comparison cannot
	// tell `ORDER BY 1` from `ORDER BY 1 DESC`.
	ordered bool
}

func a2OrderCells() []a2OrderCell {
	return []a2OrderCell{
		// ------------------------------------------------------------------
		// #810 — a select-list POSITION over a list that carries a `*`.
		//
		// The parser resolves an ordinal against info.Columns, where a star
		// is ONE entry; position 1 therefore landed on the star, the term was
		// rewritten to the literal text `*`, and the logical layer refused it
		// with "`SELECT *` is expanded too late for the planner to count its
		// positions here". Every arm, PostgreSQL answers all of them.
		{issue: "#810", name: "star_only_order_by_1", ordered: true,
			sql:    `SELECT * FROM zzp WHERE id < 3 ORDER BY 1`,
			want:   []string{"id=int64:1|d92=-3.50", "id=int64:2|d92=0.00"},
			pgSays: "2 rows ordered by id"},
		{issue: "#810", name: "star_only_order_by_1_desc", ordered: true,
			sql: `SELECT * FROM zzp ORDER BY 1 DESC`,
			want: []string{
				"id=int64:3|d92=12.75", "id=int64:2|d92=0.00", "id=int64:1|d92=-3.50"},
			pgSays: "3 rows, id descending"},
		// Position 2 names the star's SECOND column, which is what makes the
		// count matter: a fix that resolved only position 1 would pass the
		// cell above and answer this one wrong.
		{issue: "#810", name: "star_only_order_by_2", ordered: true,
			sql: `SELECT * FROM zzp ORDER BY 2`,
			want: []string{
				"id=int64:1|d92=-3.50", "id=int64:2|d92=0.00", "id=int64:3|d92=12.75"},
			pgSays: "3 rows ordered by d92: -3.50, 0.00, 12.75"},
		// A star BESIDE another select item. Position 1 is still the star's
		// first column, not `seven` — the star occupies as many positions as
		// it has columns, and counting it as one item is the defect.
		//
		// The literal is 7 and not 1 on purpose (protocol method 2). With
		// `1 AS one` the sibling item's own Expr TEXT is "1", which is also
		// the ORDER BY term's text, so `resolveOrderByColumn` matched it by
		// accident and the cell passed on a tree with no ordinal resolution
		// at all — a fixture that cannot tell the two rules apart.
		{issue: "#810", name: "star_plus_item_order_by_1", ordered: true,
			sql: `SELECT *, 7 AS seven FROM zzp ORDER BY 1`,
			want: []string{
				"id=int64:1|d92=-3.50|seven=int64:7",
				"id=int64:2|d92=0.00|seven=int64:7",
				"id=int64:3|d92=12.75|seven=int64:7"},
			pgSays: "3 rows ordered by id"},
		// And position 3, which names the sibling ITEM rather than a star
		// column: the count has to cross the star, not stop at it.
		{issue: "#810", name: "star_plus_item_order_by_3", ordered: true,
			sql: `SELECT *, 7 AS seven FROM zzp WHERE id < 3 ORDER BY 3, 1 DESC`,
			want: []string{
				"id=int64:2|d92=0.00|seven=int64:7",
				"id=int64:1|d92=-3.50|seven=int64:7"},
			pgSays: "2 rows: seven is constant, so the tiebreak `1 DESC` decides — id 2 then 1"},
		// A star over a DERIVED TABLE: the star expands against the
		// subquery's own projection, which the logical layer can enumerate.
		{issue: "#810", name: "star_over_derived_order_by_1",
			sql:    `SELECT * FROM (SELECT g + 1 AS k FROM typemx WHERE id < 3) s ORDER BY 1`,
			want:   []string{"k=int64:1", "k=int64:2", "k=int64:3"},
			pgSays: "3 rows: 1, 2, 3"},
		// The control that never went through the star path and must not
		// move: an ordinal over a list with no star at all, which the parser
		// has always resolved.
		{issue: "#810", name: "ctl_named_columns_order_by_1", ordered: true,
			sql: `SELECT id, g FROM typemx WHERE id < 3 ORDER BY 1`,
			want: []string{
				"id=int64:0|g=int32:0", "id=int64:1|g=int32:1", "id=int64:2|g=int32:2"},
			pgSays: "3 rows ordered by id"},
		// An ordinal over a star that DOES expand, out of range. PostgreSQL
		// says 42P10 and so does wadjet now — before, every ordinal over a
		// star got the same "cannot count" message whether or not it was in
		// range, which is a wrong reason attached to a right refusal.
		{issue: "#810", name: "star_order_by_out_of_range",
			sql:         `SELECT * FROM zzp ORDER BY 99`,
			wantErrLike: "ORDER BY position 99 is not in select list",
			wantState:   "42P10",
			pgSays:      `42P10: ORDER BY position 99 is not in select list`},
		// THE BOUNDARY, and it is a real divergence rather than a fix's edge:
		// a star over a JOIN. PostgreSQL answers this (4 columns, ordered by
		// the first). Wadjet refuses, because ExpandStarProjections declines
		// a star whose source is not a lone scan — guessing a join's column
		// set would silently change which columns a query returns, which is
		// that pass's own stated reason and predates this fix. The cell is
		// here so the boundary has a fixture that attempts it (protocol rule
		// 11) and so a later change that starts answering it is noticed.
		{issue: "#810", name: "boundary_star_over_join",
			sql:         `SELECT * FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY 1`,
			wantErrLike: "expands to a column list the planner cannot count",
			wantState:   "42P10",
			pgSays:      "PostgreSQL ANSWERS: 3 rows, 4 columns, ordered by a.id"},
	}
}

func TestOrderByResolvesAPositionAfterTheStarExpands(t *testing.T) {
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

	for _, tc := range a2OrderCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			if !tc.ordered {
				sort.Strings(want)
			}
			check := func(arm string, got []string, err error) {
				t.Helper()
				if !tc.ordered {
					sort.Strings(got)
				}
				if tc.wantErrLike != "" {
					if err == nil {
						t.Errorf("%s arm: answered %v, but this shape is LOUD here\n"+
							"  want an error containing %q\n  PostgreSQL 17: %s\n  SQL: %s",
							arm, got, tc.wantErrLike, tc.pgSays, tc.sql)
						return
					}
					if !strings.Contains(err.Error(), tc.wantErrLike) {
						t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
							arm, err, tc.wantErrLike, tc.sql)
					}
					if s := sqlerr.StateOf(err); s != tc.wantState {
						t.Errorf("%s arm: SQLSTATE %q, want %q — a refusal a client cannot "+
							"classify is half a refusal\n  SQL: %s", arm, s, tc.wantState, tc.sql)
					}
					return
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", arm, err, tc.pgSays, tc.sql)
					return
				}
				if len(got) != len(want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm, len(got), len(want), got, want, tc.sql)
					return
				}
				for i := range got {
					if got[i] != want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm, i, got[i], want[i], tc.sql)
						return
					}
				}
			}

			sgot, serr := a2OrderRun(tmdRunSingle(ctx, single, tc.sql))
			check("single", sgot, serr)
			for i := 0; i < 5; i++ {
				got, err := a2OrderRun(tmdRunSingle(ctx, spilled, tc.sql))
				check("spilled", got, err)
				if t.Failed() {
					break
				}
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				got, err := a2OrderRun(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
			}
		})
	}
}
