// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// wdLocalRoutes is every count of a query the coordinator answered on its
// single-process pipeline instead of the stage DAG.
func wdLocalRoutes(c *Coordinator) int64 {
	return c.NullAwareAntiLocalRoutes() + c.TableLessLocalRoutes() + c.UnbuildableStageLocalRoutes() +
		c.UnreachableOutputLocalRoutes() + c.LateralProjectionLocalRoutes() + c.ScalarProjectionLocalRoutes() +
		c.InSubqueryLocalRoutes() + c.CorrelatedLocalRoutes() + c.DistinctLocalRoutes() +
		c.GroupingSetsLocalRoutes() + c.GroupKeyLocalRoutes() + c.ResidualSidesLocalRoutes() +
		c.PolicedWindowLocalRoutes() + c.WindowOverLateralLocalRoutes() + c.LateralIdentityLocalRoutes()
}

// wdRunCells runs every cell of a name<TAB>sql table on the five arms against
// its PostgreSQL answer. onDAG names the cells the DAG arms must plan as a
// stage DAG — no route to the coordinator-local pipeline. The arms run one
// after another so each coordinator's route count is the cell's own.
func wdRunCells(t *testing.T, cellsPath, answersPath string, onDAG func(string) bool) {
	t.Helper()
	cells := wdConsumerTable(t, cellsPath)
	answers := map[string]string{}
	for _, a := range wdConsumerTable(t, answersPath) {
		answers[a[0]] = a[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := wdArms(t, ctx)
	for _, c := range cells {
		name, sql := c[0], c[1]
		want, ok := answers[name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", name)
		}
		t.Run(name, func(t *testing.T) {
			for _, arm := range arms {
				var before int64
				if arm.coord != nil {
					before = wdLocalRoutes(arm.coord)
				}
				res, err := arm.run(sql)
				if arm.coord != nil && onDAG(name) {
					if n := wdLocalRoutes(arm.coord) - before; n != 0 {
						t.Errorf("%s\n  arm  %s\n  answered on the coordinator-local pipeline (%d routes), want the stage DAG", sql, arm.name, n)
					}
				}
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q", sql, arm.name, wdRender(res), state, msg)
					} else if st := sqlerr.StateOf(err); st != state || !waErrorAgrees(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q", sql, arm.name, st, err, state, msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s", sql, arm.name, err, want)
					continue
				}
				if got := wdRender(res); wdNormalize(got) != wdNormalize(want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", sql, arm.name, got, want)
				}
			}
		})
	}
}

// A WINDOW OVER A CHAIN OF DERIVED TABLES WHOSE LOWER TABLE SHADOWS A COLUMN
// OF ITS OWN INPUT (#1435 round 6): `(SELECT id, g, b FROM (SELECT id, g, b *
// 2 AS b FROM wd_t) s0) s`, three levels, a qualified or `*` pass-through, a
// filter between the tables, a CTE chain. The DAG's producer emits the upper
// table's declared columns, each composed down through the chain to the scan
// — a bare pass-through item (`SELECT id, g, b`) included — so the stage DAG
// plans the query, as 9420d256 did. At 4e6592c7 the composition matched
// aliased items only, refused every chain whose upper table forwards a column
// bare, and the query ran on the coordinator-local pipeline (and refused a
// result over its budget, TestArcWDTwoLevelShadowBigResultOnTheDAG). fm/:
// the window arm as the join's BUILD side (a filter inside the table makes
// the planner swap the sides) is qualified by its relation's name like any
// one-relation arm, where the gather read the probe's `b` for the arm's
// (`b AS w3` 20 where PostgreSQL answers 40) at 9420d256 and 4e6592c7.
// fm/ctl_* are non-shadowing controls, which route as at base.
func TestArcWDTwoLevelShadowPlansOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over a window on a chain of derived tables")
	}
	wdRunCells(t, "testdata/arc_wd_two_level_shadow_cells.tsv", "testdata/arc_wd_two_level_shadow_pg17.tsv",
		func(name string) bool { return !strings.HasPrefix(name, "fm/ctl_") })
}

// A result larger than the coordinator-local budget over a two-level shadowing
// table, on a DAG coordinator with a 64 KiB fast path: 4e6592c7 routed it local
// and refused `… result exceeded the local budget`; 9420d256 and the round-6
// tip answer every row on the stage DAG.
func TestArcWDTwoLevelShadowBigResultOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: 300 000 rows on a DAG coordinator")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const n = 300000
	tbl := tmdTable{name: "wdbig", schema: parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true}, {Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true}}}}
	for i := int64(1); i <= n; i++ {
		tbl.rows = append(tbl.rows, map[string]any{"id": i, "g": i % 10, "b": i})
	}
	infra := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infra, nil, []tmdTable{tbl})
	c := tmdCoordinator(t, ctx, infra, func(cfg *Config) { cfg.LocalFastPathBytes = 64 << 10 })
	const body = "(SELECT id, g, SUM(b) OVER (ORDER BY id) AS w, b AS w3 FROM " +
		"(SELECT id, g, b FROM (SELECT id, g, b * 2 AS b FROM wdbig) s0) s) x"
	before := wdLocalRoutes(c)
	out, err := c.ExecuteSQL(ctx, "SELECT x.id, SUM(x.w3) AS s FROM "+body+" GROUP BY x.id")
	if err == nil && out.Error != "" {
		t.Fatalf("refused: %s", out.Error)
	}
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	rows, err := out.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != n {
		t.Fatalf("%d rows, want %d", len(rows), n)
	}
	for _, r := range rows {
		id, _ := r["id"].(int64)
		if s := fmt.Sprint(r["s"]); s != strconv.FormatInt(2*id, 10) {
			t.Fatalf("id %d: s = %s, want %d", id, s, 2*id)
		}
	}
	if routes := wdLocalRoutes(c) - before; routes != 0 {
		t.Fatalf("answered on the coordinator-local pipeline (%d routes), want the stage DAG", routes)
	}
}

// A TYPED-NULL LAG / LEAD DEFAULT (#1436): a default that is a NULL literal —
// bare, under a CAST, or an expression whose every leaf is one — contributes
// its type FAMILY and no width (physical.lagLeadDefaultDecl), so
// `LAG(b * 1000000000000000 + 1, 1, CAST(NULL AS NUMERIC))` is numeric and
// answers 10000000000000001 where 8b00b112 failed the query on every arm.
// b2/ covers every default type over a value past 2^53 (a typed NULL of each
// type, NULL, omitted, integer / decimal / wide / exponent literals, CASTs, a
// quoted literal, columns, expressions) for LAG, LEAD and a SUM above; b2t/ a
// typed NULL over each value type; ar/lag_*, ar/cmp and ar/text a typed NULL
// beside the window. The rule is the default's alone: a typed NULL in
// COALESCE, CASE, UNION ALL, NULLIF, GREATEST or arithmetic keeps its
// planner-wide declaration (double precision, as at 8b00b112), and those
// cells left this table in round 8 (ADR-0024, candidate U).
//
// Left out, base-identical and recorded as filing candidates: `1e300` (no
// DECIMAL(38,s) holds it, so the literal is a double here and numeric on
// PostgreSQL), SUM over `LAG(v, 1, NULL)` of a computed value (declared
// double with or without the default), and `/` and SQRT over the typed NULL
// (double precision by ADR-0024's quotient rung).
func TestArcWDTypedNullDefaultEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over typed-NULL defaults")
	}
	wdRunCells(t, "testdata/arc_wd_typed_null_default_cells.tsv", "testdata/arc_wd_typed_null_default_pg17.tsv",
		func(string) bool { return false })
}

// A CONSUMER ABOVE A JOIN OF ANY KIND OVER A WINDOW ON A SHADOWING DERIVED
// TABLE (#1435 round 7): `wd_t y JOIN (SELECT id, g, SUM(b) OVER (…) AS w, b
// AS w3, b FROM (SELECT id, g, b * 2 AS b FROM wd_t) s) x` read by the SELECT
// list (qualified and bare), an aggregate, HAVING, GROUP BY, a window, WHERE,
// ORDER BY + LIMIT, DISTINCT, a second join keyed on the arm's column, a UNION
// ALL arm and a scalar subquery; INNER, LEFT, RIGHT, FULL, CROSS and a keyless
// LEFT join, the window arm second or first, one or two levels of derived
// tables, with and without a filter inside them (nowin/: the same table
// without a window, the control). The block the window is read through
// publishes its own list onto the window stage and the join names that arm by
// the block's alias, so `x.w3` and `x.b` are the block's columns, never the
// probe's bare `b`. At 9655ef07 an aggregate read the probe's `b` for `x.w3`
// (180 where PostgreSQL answers 360; 8b00b112 refused), and DISTINCT, GROUP
// BY, the SELECT list's `x.b`, a second join and a window read it on the
// three DAG arms. Cells that route to the coordinator-local pipeline at base
// (ORDER BY … LIMIT, a scalar subquery, every nowin/ cell) keep their route.
func TestArcWDJoinConsumersEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over a join above a window on a shadowing derived table")
	}
	wdRunCells(t, "testdata/arc_wd_join_consumers_cells.tsv", "testdata/arc_wd_join_consumers_pg17.tsv",
		func(name string) bool {
			return !strings.HasPrefix(name, "j7/nowin") && !strings.HasSuffix(name, "/scal") &&
				!strings.HasSuffix(name, "/sort")
		})
}
