package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/wadjet"
)

// The value gate for the producer-emits-no-stage class (#656) and its loud
// siblings (#558, #658, #659, #660, #672, #681).
//
// One mechanism, seven silent shapes plus the loud ones: walkStages attached
// a Filter to `len(*stages)-1` and a Project only to a scan or a join, so when
// the producer was a Sort, a LIMIT, a Window, a deduped `cte-alias` or an
// aggregate's output, the predicate or the projection landed on a stage that
// a later pass deleted or that never reads the field — and the query answered
// WITHOUT it. Nothing failed: no operator ever saw a name it could not
// resolve.
//
// Every expectation below is computed in Go from the fixture generator
// (internal/oracle/typematrix), never read off either engine, and every one
// matches the PostgreSQL 17 answers recorded in the umbrella's shape table.
// Both arms are asserted, because the single-process path was already right
// for all seven silent shapes: a gate that only compared the two arms would
// have passed the day the class was fixed AND the day it regresses in the
// other direction.

// sfcRow is one fixture row's fields, typed.
type sfcRow struct {
	id  int64
	c   any // c_i64, nil when NULL
	str any // c_str
	g   any // g
}

func sfcRows() []sfcRow {
	data := typematrix.Data(typematrix.Rows)
	out := make([]sfcRow, len(data))
	for i, r := range data {
		out[i] = sfcRow{id: r["id"].(int64), c: r["c_i64"], str: r["c_str"], g: r["g"]}
	}
	return out
}

// sfcPositiveIDs is the id set of rows whose c_i64 is non-NULL and > 0,
// restricted to the first n ids by id order (n < 0 = all).
func sfcPositiveIDs(rows []sfcRow, n int) []int64 {
	var out []int64
	for i, r := range rows {
		if n >= 0 && i >= n {
			break
		}
		if v, ok := r.c.(int64); ok && v > 0 {
			out = append(out, r.id)
		}
	}
	return out
}

func TestStageCarriesFilterAndProjectionTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the #656 gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	rows := sfcRows()
	tbl := typematrix.Table

	// --- expectations, from the generator ---------------------------------

	// a, b: the first 10 rows by id, then v > 0. c_i64 = i*1_000_003 and is
	// NULL only where i%31 == 30, so row 0 is the one the predicate rejects
	// (its value is 0, not NULL) and the answer is 9 — the DAG's 10 was the
	// WHERE never running.
	wantAB := int64(len(sfcPositiveIDs(rows, 10)))
	// c: the same predicate over the whole fixture, with no LIMIT to hide
	// behind. 4838 = 5000 rows − 161 NULLs − the one zero.
	wantC := int64(len(sfcPositiveIDs(rows, -1)))
	// d: the top 5 by v DESC among id < 100, then v > 96_000_000.
	// PostgreSQL orders NULLs FIRST for DESC, so the five rows are the three
	// NULL-v rows (i = 30, 61, 92) and then ids 99 and 98 — of which only the
	// last two survive the predicate. The DAG returned all five, NULLs
	// included, which is the shape that shows the filter must run ABOVE the
	// LIMIT rather than under it.
	wantD := []int64{99, 98}
	// f: `g + 1` grouped, then gk > 3. g = i%7 with NULL every 13th row, so
	// gk ∈ {1..7} plus a NULL group, and gk > 3 keeps four of them.
	wantF := map[int64]int64{}
	for _, r := range rows {
		g, ok := r.g.(int32)
		if !ok {
			continue
		}
		if gk := int64(g) + 1; gk > 3 {
			wantF[gk]++
		}
	}
	// g: the SELECT list above a window. The DAG used to return the window's
	// raw input plus its output column — {id, c_str, w} — for a query that
	// asked for {id, v}.
	wantG := make([]map[string]any, 0, 3)
	for _, r := range rows[:3] {
		wantG = append(wantG, map[string]any{"id": r.id, "v": strings.ToUpper(r.str.(string))})
	}
	// 660: UNION ALL over a CTE referenced twice.
	want660 := int64(2 * typematrix.Rows)

	// --- the shapes --------------------------------------------------------

	t.Run("a/CountAboveCTEOrderByLimit", func(t *testing.T) {
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s ORDER BY id LIMIT 10) `+
				`SELECT COUNT(*) AS n FROM c WHERE v > 0`, tbl), "n", wantAB)
	})
	t.Run("b/CountAboveDerivedOrderByLimit", func(t *testing.T) {
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id, c_i64 AS v FROM %s ORDER BY id LIMIT 10) s `+
				`WHERE s.v > 0`, tbl), "n", wantAB)
	})
	t.Run("c/CountAboveCTEOrderBy", func(t *testing.T) {
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s ORDER BY id) `+
				`SELECT COUNT(*) AS n FROM c WHERE v > 0`, tbl), "n", wantC)
	})
	t.Run("d/FilterRunsAboveTheLimit", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s WHERE id < 100 ORDER BY v DESC LIMIT 5) `+
				`SELECT id, v FROM c WHERE v > 96000000`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if diff := diffIDs(wantD, predIDs(t, res)); diff != "" {
				t.Errorf("%s arm: %s\n  SQL: %s", arm.name, diff, sql)
			}
		}
	})
	t.Run("e/FilterAboveTheInnerOfTwoCTERefs", func(t *testing.T) {
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %[1]s) `+
				`SELECT COUNT(*) AS n FROM c JOIN (SELECT id AS j FROM c WHERE v > 0) x ON c.id = x.j`,
			tbl), "n", wantC)
	})
	t.Run("f/FilterOnAnAggregateOutputAlias", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH c AS (SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1) `+
				`SELECT gk, n FROM c WHERE gk > 3 ORDER BY gk`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			got := map[int64]int64{}
			for _, r := range res.Rows {
				gk, ok1 := numAsInt(r["gk"])
				n, ok2 := numAsInt(r["n"])
				if !ok1 || !ok2 {
					t.Fatalf("%s arm: row %v is not (int, int)", arm.name, r)
				}
				got[gk] = n
			}
			if fmt.Sprint(got) != fmt.Sprint(wantF) {
				t.Errorf("%s arm answered %v, want %v — the predicate names the aggregate's "+
					"OUTPUT column and must not be re-spelled into its input expression\n  SQL: %s",
					arm.name, got, wantF, sql)
			}
			// And the column NAME: the alias, not the group-key expression
			// text the worker computes the key under.
			if !sameNames(res.Columns, []string{"gk", "n"}) {
				t.Errorf("%s arm returned columns %v, want [gk n]", arm.name, res.Columns)
			}
		}
	})
	t.Run("f/TheSameAliasWithNoWhereAboveIt", func(t *testing.T) {
		// The NAME half on its own: with no consumer to force it, the DAG
		// returned the group key's expression text as the column name for a
		// query whose SELECT list says `gk`. Values were right; the client
		// saw `g + 1`.
		sql := fmt.Sprintf(
			`WITH c AS (SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1) `+
				`SELECT gk, n FROM c ORDER BY gk`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if !sameNames(res.Columns, []string{"gk", "n"}) {
				t.Errorf("%s arm returned columns %v, want [gk n]\n  SQL: %s",
					arm.name, res.Columns, sql)
			}
		}
	})
	t.Run("f/TheSameAliasThroughADerivedTable", func(t *testing.T) {
		// No CTE fence, so the logical optimizer's Filter-Project SWAP
		// applies instead of the DAG's re-spelling — and substituted `gk`
		// into `(g + 1)`, an expression over `g`, which the aggregate's
		// OUTPUT rows do not carry. BOTH paths answered zero rows for a
		// query PostgreSQL answers: the same defect one layer up, in the
		// shared optimizer rather than in stage emission.
		sql := fmt.Sprintf(
			`SELECT gk, n FROM (SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1) s `+
				`WHERE gk > 3 ORDER BY gk`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			got := map[int64]int64{}
			for _, r := range res.Rows {
				gk, _ := numAsInt(r["gk"])
				n, _ := numAsInt(r["n"])
				got[gk] = n
			}
			if fmt.Sprint(got) != fmt.Sprint(wantF) {
				t.Errorf("%s arm answered %v, want %v\n  SQL: %s", arm.name, got, wantF, sql)
			}
		}
	})
	t.Run("g/ProjectionAboveAWindow", func(t *testing.T) {
		sql := fmt.Sprintf(
			`SELECT id, UPPER(c_str) AS v FROM `+
				`(SELECT id, c_str, ROW_NUMBER() OVER (ORDER BY id) AS w FROM %s) x `+
				`ORDER BY id LIMIT 3`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if !sameNames(res.Columns, []string{"id", "v"}) {
				t.Errorf("%s arm returned columns %v, want [id v] — the SELECT list above the "+
					"window was never applied\n  SQL: %s", arm.name, res.Columns, sql)
			}
			if len(res.Rows) != len(wantG) {
				t.Fatalf("%s arm returned %d rows, want %d", arm.name, len(res.Rows), len(wantG))
			}
			for i, w := range wantG {
				got := res.Rows[i]
				gid, _ := numAsInt(got["id"])
				if gid != w["id"].(int64) || fmt.Sprint(got["v"]) != w["v"].(string) {
					t.Errorf("%s arm row %d = %v, want %v\n  SQL: %s", arm.name, i, got, w, sql)
				}
			}
		}
	})

	// --- the loud siblings, same mechanism ---------------------------------

	t.Run("558/HiddenOrderByAboveAWindow", func(t *testing.T) {
		// The ORDER BY term is not in the SELECT list, so the plan
		// materializes it as __sortkey_0 — a column only a projection can
		// create, and the window fragment had none, so the task failed:
		// `sort: key column "__sortkey_0" does not exist in the input
		// schema`. DESC on purpose: scan order is id order, so an
		// unapplied ordering would pass an ASC assertion by luck.
		sql := fmt.Sprintf(
			`SELECT c_str, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM %s WHERE id < 25 `+
				`ORDER BY c_i64 DESC`, tbl)
		var want []string
		for i := 24; i >= 0; i-- {
			// c_i64 = i*1_000_003 and none of the first 25 rows is NULL
			// (the stride is 31), so DESC is exactly reverse id order.
			want = append(want, rows[i].str.(string))
		}
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			var got []string
			for _, r := range res.Rows {
				if s, ok := r["c_str"].(string); ok {
					got = append(got, s)
				}
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s arm ordered by the hidden c_i64 DESC as %v, want %v\n  SQL: %s",
					arm.name, got, want, sql)
			}
		}
	})
	t.Run("658/WindowPartitionByADerivedAlias", func(t *testing.T) {
		// PARTITION BY names the CTE's alias, which the window's input does
		// not carry on the DAG: `PARTITION BY "gk" is not a column of its
		// input`. The answer is one row per id with rn = its rank inside its
		// own g partition; for the first five ids, all in distinct
		// partitions, that is 1.
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id, g AS gk, c_i64 AS v FROM %s WHERE id < 200) `+
				`SELECT id, ROW_NUMBER() OVER (PARTITION BY gk ORDER BY id) AS rn FROM c `+
				`ORDER BY id LIMIT 5`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if len(res.Rows) != 5 {
				t.Fatalf("%s arm returned %d rows, want 5\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			for i, r := range res.Rows {
				id, _ := numAsInt(r["id"])
				rn, _ := numAsInt(r["rn"])
				if id != int64(i) || rn != 1 {
					t.Errorf("%s arm row %d = (id=%d, rn=%d), want (id=%d, rn=1)",
						arm.name, i, id, rn, i)
				}
			}
		}
	})
	t.Run("658/WindowArgumentAndOrderByOverADerivedAlias", func(t *testing.T) {
		// The other two names exec.Window reads off its input batch. An
		// ARGUMENT naming a CTE alias found no vector and wrote NULL in
		// every row — the SILENT half of #658, where the PARTITION BY key
		// fails loud; the window ORDER BY is the third.
		var sum int64
		for _, r := range rows[:5] {
			sum += r.c.(int64)
		}
		t.Run("Argument", func(t *testing.T) {
			sql := fmt.Sprintf(
				`WITH c AS (SELECT id, c_i64 AS v FROM %s WHERE id < 5) `+
					`SELECT id, SUM(v) OVER () AS s FROM c ORDER BY id`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				for _, r := range res.Rows {
					got, ok := numAsInt(r["s"])
					if !ok || got != sum {
						t.Errorf("%s arm answered %v, want %d\n  SQL: %s",
							arm.name, r["s"], sum, sql)
					}
				}
			}
		})
		t.Run("OrderBy", func(t *testing.T) {
			sql := fmt.Sprintf(
				`WITH c AS (SELECT id AS i, c_i64 AS v FROM %s WHERE id < 5) `+
					`SELECT i, ROW_NUMBER() OVER (ORDER BY i DESC) AS rn FROM c ORDER BY i`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != 5 {
					t.Fatalf("%s arm returned %d rows, want 5", arm.name, len(res.Rows))
				}
				for k, r := range res.Rows {
					i, _ := numAsInt(r["i"])
					rn, _ := numAsInt(r["rn"])
					if i != int64(k) || rn != int64(5-k) {
						t.Errorf("%s arm row %d = (i=%d, rn=%d), want (i=%d, rn=%d)",
							arm.name, k, i, rn, k, 5-k)
					}
				}
			}
		})
	})
	t.Run("660/UnionAllOverATwiceReferencedCTE", func(t *testing.T) {
		// The union's arms both name the deduped `cte-alias` stage while
		// Dependencies had already been rewritten to its target, and the
		// plan was refused: `arm 1 names producer "cte-alias-1" but
		// Dependencies[1] is "scan-0"`.
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %[1]s) `+
				`SELECT COUNT(*) AS n FROM (SELECT id FROM c UNION ALL SELECT id FROM c) u`,
			tbl), "n", want660)
	})
	t.Run("681/ComputedAggregateAliasAsAJoinKey", func(t *testing.T) {
		// The shuffle key is a COMPUTED alias over an aggregate's output — a
		// column no stage emitted, so the DAG refused the plan outright:
		// `partitioned shuffle: key "b.k" not in schema`.
		//
		// The group counts, including the NULL group, are the k values; each
		// is also an id, so every group joins exactly once.
		counts := map[any]int64{}
		for _, r := range rows {
			counts[r.g]++
		}
		ids := map[int64]bool{}
		for _, r := range rows {
			ids[r.id] = true
		}
		var want int64
		for _, c := range counts {
			if ids[c+1] {
				want++
			}
		}
		// The key is INT64 on one side and the widened type of `COUNT(*) + 1`
		// on the other, so the answer also depends on the join key being
		// built at the pair's common type (#615) — without which both arms
		// matched nothing. Both halves are asserted here: the query must be
		// ANSWERED (that is #681, which refused the plan) and answer 8.
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %[1]s a JOIN `+
				`(SELECT g, COUNT(*) + 1 AS k FROM %[1]s GROUP BY g) b ON a.id = b.k`,
			tbl), "n", want)
	})

	t.Run("659/ScalarSubqueryInTheSelectList", func(t *testing.T) {
		// The DAG has no lowering for a subquery in the SELECT list — the
		// scalar-producer machinery covers predicates only — so every task
		// failed with `subqueries require a SubqueryRunner`. The planner now
		// refuses the shape and the coordinator routes it to its local
		// engine, which is what turns the failure into the answer.
		var wantMax int64
		for _, r := range rows {
			if v, ok := r.c.(int64); ok && v > wantMax {
				wantMax = v
			}
		}
		before := coord.ScalarProjectionLocalRoutes()
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %[1]s) `+
				`SELECT id, (SELECT MAX(v) FROM c) AS mx FROM c WHERE id < 3 ORDER BY id`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if len(res.Rows) != 3 {
				t.Fatalf("%s arm returned %d rows, want 3\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			for i, r := range res.Rows {
				id, _ := numAsInt(r["id"])
				// Compared as text: the two arms box a MAX(BIGINT) in
				// different Go types and which one is right is another
				// gate's question, not this one's.
				if id != int64(i) || fmt.Sprint(r["mx"]) != fmt.Sprint(wantMax) {
					t.Errorf("%s arm row %d = (id=%d, mx=%v %T), want (id=%d, mx=%d)",
						arm.name, i, id, r["mx"], r["mx"], i, wantMax)
				}
			}
		}
		if got := coord.ScalarProjectionLocalRoutes(); got <= before {
			t.Errorf("the coordinator answered without taking the #659 refusal route "+
				"(%d → %d): the gate would pass on a DAG that never saw the shape", before, got)
		}
		// A subquery over a BASE TABLE takes the same route: the defect was
		// never CTE-specific, and a refusal narrowed to CTEs would leave the
		// commoner spelling failing.
		sfcRun(t, sfcArms(ctx, single, coord)[1], fmt.Sprintf(
			`SELECT id, (SELECT MAX(k) FROM %s) AS mx FROM %s WHERE id < 3 ORDER BY id`,
			typematrix.Dim, tbl))
	})
	t.Run("672/WindowArgumentExpression", func(t *testing.T) {
		// A window function's ARGUMENT expression was never materialized, so
		// WindowColumn.InputCol named a column the batch did not carry and
		// the operator wrote NULL in every row — on BOTH paths, for every
		// input type. resolveWindowKeys now materializes it exactly as it
		// materializes a computed PARTITION BY term.
		var sum, max int64
		for _, r := range rows[:5] {
			v := r.c.(int64) * 2
			sum += v
			if v > max {
				max = v
			}
		}
		first := rows[0].c.(int64) * 2
		for _, c := range []struct {
			name string
			sql  string
			want int64
		}{
			{"Sum", `SELECT id, SUM(c_i64 * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`, sum},
			{"Max", `SELECT id, MAX(c_i64 * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`, max},
			{"FirstValue", `SELECT id, FIRST_VALUE(c_i64 * 2) OVER (ORDER BY id) AS s FROM %s WHERE id < 5 ORDER BY id`, first},
			{"Coalesce", `SELECT id, SUM(COALESCE(c_i64, 0) * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`, sum},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != 5 {
						t.Fatalf("%s arm returned %d rows, want 5\n  SQL: %s",
							arm.name, len(res.Rows), sql)
					}
					for _, r := range res.Rows {
						got, ok := numAsInt(r["s"])
						if !ok {
							t.Fatalf("%s arm answered %v (%T) — a window over an expression "+
								"must not be NULL\n  SQL: %s", arm.name, r["s"], r["s"], sql)
						}
						if got != c.want {
							t.Errorf("%s arm answered %d, want %d\n  SQL: %s",
								arm.name, got, c.want, sql)
						}
					}
				}
			})
		}
	})

	// --- the adversarial-review round -------------------------------------
	//
	// Every shape below was silent or loud on a plan the first round's gates
	// accepted. They are here because the gates could not see them, not
	// because the shapes are exotic: two of them are a CTE referenced twice.

	t.Run("A1/WhereOnTheFirstOfTwoCTERefs", func(t *testing.T) {
		// The FIRST reference walked emits the CTE body's real producer, so
		// its WHERE landed on the very stage the OTHER reference reads —
		// every reference saw the filtered stream. 109 became 18, silently,
		// and 209 became 27 with three references. The mirror of the
		// deduped-alias case, which round one closed while opening this.
		const lit = 90000000
		var filtered, all int64
		for _, r := range rows {
			if r.id >= 100 {
				continue
			}
			all++
			if v, ok := r.c.(int64); ok && v > lit {
				filtered++
			}
		}
		for _, c := range []struct {
			name string
			sql  string
			want int64
		}{
			{"TwoRefs", `WITH c AS (SELECT id, c_i64 AS v FROM %[1]s WHERE id < 100) ` +
				`SELECT COUNT(*) AS n FROM (SELECT id FROM c WHERE v > %[2]d UNION ALL ` +
				`SELECT id FROM c) u`, filtered + all},
			{"ThreeRefs", `WITH c AS (SELECT id, c_i64 AS v FROM %[1]s WHERE id < 100) ` +
				`SELECT COUNT(*) AS n FROM (SELECT id FROM c WHERE v > %[2]d UNION ALL ` +
				`SELECT id FROM c UNION ALL SELECT id FROM c) u`, filtered + 2*all},
			// The same through a JOIN rather than a union (#656 B3).
			{"ThroughAJoin", `WITH c AS (SELECT id, c_i64 AS v FROM %[1]s WHERE id < 100) ` +
				`SELECT COUNT(*) AS n FROM (SELECT id FROM c WHERE v > %[2]d) x ` +
				`JOIN (SELECT id AS j FROM c) y ON x.id = y.j`, filtered},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sfcScalar(t, ctx, single, coord, fmt.Sprintf(c.sql, tbl, lit), "n", c.want)
			})
		}
	})
	t.Run("A2/WhereAboveAWindowNamingAnAlias", func(t *testing.T) {
		// resolveFilterInSubtree walked through Distinct, Sort and LIMIT but
		// not a WINDOW, so `gk` reached the window stage's filter naming a
		// column its input does not carry: UNKNOWN on every row, zero rows.
		var want int64
		for _, r := range rows {
			if r.id >= 50 {
				continue
			}
			if g, ok := r.g.(int32); ok && g == 1 {
				want++
			}
		}
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`WITH c AS (SELECT id, g AS gk, c_i64 AS v FROM %s WHERE id < 50) `+
				`SELECT COUNT(*) AS n FROM (SELECT id, gk, `+
				`ROW_NUMBER() OVER (PARTITION BY gk ORDER BY id) AS rn FROM c) x WHERE gk = 1`,
			tbl), "n", want)
	})
	t.Run("A3/WindowArgumentExpressionOverAnAlias", func(t *testing.T) {
		// The window ARGUMENT is materialized as __winkey_N, and its
		// EXPRESSION TEXT still named the CTE's alias — round one re-spelled
		// only the bare InputCol. NULL on every row.
		var sum int64
		for _, r := range rows[:5] {
			sum += r.c.(int64) * 2
		}
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s WHERE id < 5) `+
				`SELECT id, SUM(v * 2) OVER () AS s FROM c ORDER BY id`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			for _, r := range res.Rows {
				got, ok := numAsInt(r["s"])
				if !ok || got != sum {
					t.Errorf("%s arm answered %v, want %d\n  SQL: %s", arm.name, r["s"], sum, sql)
				}
			}
		}
	})
	t.Run("B1/AggregateOutputAliasWithAHaving", func(t *testing.T) {
		// A HAVING is a Filter between the SELECT list and the aggregate, and
		// both the projection carrier and the logical substitution guard
		// stopped at it. The CTE form answered 0 on the DAG, the derived form
		// 0 on BOTH paths, and the join form failed loud.
		want := map[int64]int64{}
		for _, r := range rows {
			if g, ok := r.g.(int32); ok {
				want[int64(g)+1]++
			}
		}
		for k, v := range want {
			if k <= 3 || v <= 5 {
				delete(want, k)
			}
		}
		for _, c := range []struct{ name, sql string }{
			{"CTE", `WITH c AS (SELECT g+1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g+1 ` +
				`HAVING COUNT(*) > 5) SELECT gk, n FROM c WHERE gk > 3 ORDER BY gk`},
			{"Derived", `SELECT gk, n FROM (SELECT g+1 AS gk, COUNT(*) AS n FROM %[1]s ` +
				`GROUP BY g+1 HAVING COUNT(*) > 5) s WHERE gk > 3 ORDER BY gk`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					got := map[int64]int64{}
					for _, r := range res.Rows {
						gk, _ := numAsInt(r["gk"])
						n, _ := numAsInt(r["n"])
						got[gk] = n
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s arm answered %v, want %v\n  SQL: %s", arm.name, got, want, sql)
					}
				}
			})
		}
		t.Run("Join", func(t *testing.T) {
			counts := map[any]int64{}
			for _, r := range rows {
				counts[r.g]++
			}
			ids := map[int64]bool{}
			for _, r := range rows {
				ids[r.id] = true
			}
			var want int64
			for _, cnt := range counts {
				if cnt > 5 && ids[cnt+1] {
					want++
				}
			}
			sfcScalar(t, ctx, single, coord, fmt.Sprintf(
				`SELECT COUNT(*) AS n FROM %[1]s a JOIN (SELECT g, COUNT(*)+1 AS k FROM %[1]s `+
					`GROUP BY g HAVING COUNT(*) > 5) b ON a.id = b.k`, tbl), "n", want)
		})
	})
	t.Run("B2/SelectListAboveASortAndLimit", func(t *testing.T) {
		// Nothing ever populated ProjectExprs on a sort or a limit stage
		// despite the slots, so the SELECT list above one was never applied:
		// the DAG returned the producer's raw column, under its own name.
		want := []int64{0, 2, 4, 6, 8}
		for _, c := range []struct{ name, sql string }{
			{"Unordered", `SELECT id * 2 AS d FROM (SELECT id FROM %s ORDER BY id LIMIT 5) s`},
			{"Ordered", `SELECT id * 2 AS d FROM (SELECT id FROM %s ORDER BY id LIMIT 5) s ORDER BY d`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if !sameNames(res.Columns, []string{"d"}) {
						t.Errorf("%s arm returned columns %v, want [d]\n  SQL: %s",
							arm.name, res.Columns, sql)
					}
					var got []int64
					for _, r := range res.Rows {
						v, _ := numAsInt(r["d"])
						got = append(got, v)
					}
					if c.name == "Ordered" && fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s arm answered %v, want %v\n  SQL: %s", arm.name, got, want, sql)
					}
					if len(got) != 5 {
						t.Errorf("%s arm returned %d rows, want 5", arm.name, len(got))
					}
				}
			})
		}
	})
	t.Run("D/ComputedAliasOverAnAggregateAliasOrdered", func(t *testing.T) {
		// A SELECT list computed over an aggregate-output alias, ordered by
		// its own output: the sort keyed on a name nothing emitted.
		var want []int64
		seen := map[int64]bool{}
		for _, r := range rows {
			g, ok := r.g.(int32)
			if !ok {
				continue
			}
			if gk := int64(g) + 1; gk > 3 && !seen[gk] {
				seen[gk] = true
				want = append(want, gk*10)
			}
		}
		for i := range want {
			for j := i + 1; j < len(want); j++ {
				if want[j] < want[i] {
					want[i], want[j] = want[j], want[i]
				}
			}
		}
		sql := fmt.Sprintf(
			`WITH c AS (SELECT g+1 AS gk, COUNT(*) AS n FROM %s GROUP BY g+1) `+
				`SELECT gk*10 AS gk10 FROM c WHERE gk > 3 ORDER BY gk10`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			var got []int64
			for _, r := range res.Rows {
				v, _ := numAsInt(r["gk10"])
				got = append(got, v)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s arm answered %v, want %v\n  SQL: %s", arm.name, got, want, sql)
			}
		}
	})
	t.Run("C/WindowOverADecimalExpressionIsExact", func(t *testing.T) {
		// A window over a DECIMAL EXPRESSION. Two things must hold for this
		// to answer PostgreSQL's 20.0020: the argument has to be
		// MATERIALIZED at all (#672 — without it, NULL in every row), and
		// the materialized column has to be DECLARED DECIMAL, which needs
		// the exact DECIMAL arithmetic underneath it (#555). With the
		// declaration missing the evaluator hands an exact value to a
		// FLOAT64 vector and the query fails outright.
		//
		// The GROUPED spelling is the control: the two are the same question
		// written twice (ADR-0024 item 2) and must agree digit for digit.
		const want = "20.0020"
		for _, c := range []struct{ name, sql string }{
			{"WindowSum", `SELECT id, SUM(c_dec * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`},
			{"WindowMin", `SELECT id, MIN(c_dec * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`},
			// The argument one level down, naming a derived table's alias:
			// the key's declarations live BELOW that rename, and reading
			// them off the Project answered nothing at all.
			{"WindowSumOverAnAlias", `SELECT id, SUM(v * 2) OVER () AS s FROM ` +
				`(SELECT id, c_dec AS v FROM %s WHERE id < 5) x ORDER BY id`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != 5 {
						t.Fatalf("%s arm returned %d rows, want 5", arm.name, len(res.Rows))
					}
					exp := want
					if c.name == "WindowMin" {
						exp = "0.0000"
					}
					for _, r := range res.Rows {
						if fmt.Sprint(r["s"]) != exp {
							t.Errorf("%s arm answered %v (%T), want %s exactly — PostgreSQL's "+
								"numeric is exact and so is wadjet's\n  SQL: %s",
								arm.name, r["s"], r["s"], exp, sql)
						}
					}
				}
			})
		}
	})
	t.Run("C/CoalesceOverADecimal", func(t *testing.T) {
		// COALESCE's return-type resolution used to pick the INTEGER
		// literal's type over the DECIMAL column's, so `COALESCE(c_dec, 0)`
		// failed outright on BOTH paths with `cannot store string into INT64
		// vector`. #695 decided it, and this half of the pin is now the
		// ASSERTION it asked for: PostgreSQL's numeric, digit for digit.
		bare := fmt.Sprintf(`SELECT id, COALESCE(c_dec, 0) AS d FROM %s WHERE id < 5 ORDER BY id`, tbl)
		wantBare := []string{"0.0000", "1.0001", "2.0002", "3.0003", "4.0004"}
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, bare)
			if len(res.Rows) != len(wantBare) {
				t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
					arm.name, len(res.Rows), len(wantBare), bare)
			}
			for i, r := range res.Rows {
				if got := fmt.Sprint(r["d"]); got != wantBare[i] {
					t.Errorf("%s arm answered %q for id=%v, want %q exactly — PostgreSQL's "+
						"numeric is exact and so is wadjet's\n  SQL: %s",
						arm.name, got, r["id"], wantBare[i], bare)
				}
			}
		}
		// The half that is still open, and NOT the window's doing:
		// ARITHMETIC over COALESCE's output is float on both paths, so
		// `SUM(COALESCE(c_dec, 0) * 2)` answers 20.002000000000002 where
		// PostgreSQL answers 20.0020. The windowed spelling agrees with the
		// GROUPED one exactly, which is the evidence that the window
		// materialization is not the defect — the same argument the sibling
		// entry above makes for the plain `SUM(c_dec * 2) OVER ()`, which IS
		// exact.
		//
		// TODO(#555): both spellings become 20.0020 when the choice
		// constructs carry their DECLARED type into the arithmetic. Assert
		// it here and delete this pin.
		const wantExact = "20.0020"
		win := fmt.Sprintf(
			`SELECT id, SUM(COALESCE(c_dec, 0) * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`, tbl)
		grouped := fmt.Sprintf(
			`SELECT SUM(COALESCE(c_dec, 0) * 2) AS s FROM %s WHERE id < 5`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			w := sfcRun(t, arm, win)
			g := sfcRun(t, arm, grouped)
			if len(g.Rows) > 0 && fmt.Sprint(g.Rows[0]["s"]) == wantExact {
				t.Fatalf("%s arm now answers %s exactly for %q — the arithmetic over COALESCE "+
					"carries its declared type. Assert it on both spellings and delete this pin.",
					arm.name, wantExact, grouped)
			}
			if len(w.Rows) == 0 || len(g.Rows) == 0 {
				t.Fatalf("%s arm returned no rows", arm.name)
			}
			if fmt.Sprint(w.Rows[0]["s"]) != fmt.Sprint(g.Rows[0]["s"]) {
				t.Errorf("%s arm: the windowed spelling answered %v and the grouped one %v; "+
					"they are the same question written twice",
					arm.name, w.Rows[0]["s"], g.Rows[0]["s"])
			}
		}
	})

	// --- the second adversarial round --------------------------------------

	t.Run("F1/NarrowedSelectOverAnAggregateAlias", func(t *testing.T) {
		// A round-1 regression, and a silent wrong COLUMN SET rather than a
		// wrong value: absorbAggregateOutputProjection renamed the group key
		// on the aggregate stage while the gather's rename still named the
		// OLD spelling, so the gather could not narrow and the client got
		// the stage's full output.
		sql := fmt.Sprintf(
			`SELECT s.id FROM (SELECT g + 1 AS id, COUNT(*) AS v FROM %s GROUP BY g + 1) s `+
				`WHERE s.v > 0 ORDER BY s.id`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if !sameNames(res.Columns, []string{"id"}) {
				t.Errorf("%s arm returned columns %v, want [id] — the gather could not narrow\n  SQL: %s",
					arm.name, res.Columns, sql)
			}
		}
	})
	t.Run("F2/FilterAboveANarrowedSortProducer", func(t *testing.T) {
		// The SELECT list was attached to the scan BELOW the sort, narrowing
		// away the column the filter above the sort names. 3956 rows became
		// 0, silently.
		var want int64
		for _, r := range rows {
			if g, ok := r.g.(int32); ok && g > 0 {
				want++
			}
		}
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id AS k, g AS v FROM %s ORDER BY id) s WHERE s.v > 0`,
			tbl), "n", want)
	})
	t.Run("F2/SelectListOverACollapsingProducer", func(t *testing.T) {
		// An aggregate, a DISTINCT and a union all COLLAPSE their input, so
		// the SELECT list can neither fuse into them nor be evaluated below.
		// Each failed loud with `sort: key column "d" does not exist`, or
		// silently returned the producer's raw columns.
		groups := map[any]bool{}
		for _, r := range rows {
			groups[r.g] = true
		}
		for _, c := range []struct {
			name string
			sql  string
			want int
		}{
			{"Aggregate", `SELECT k * 2 AS d FROM (SELECT g + 1 AS k, COUNT(*) AS v FROM %[1]s ` +
				`GROUP BY g + 1) s ORDER BY d`, len(groups)},
			{"Distinct", `SELECT k * 2 AS d FROM (SELECT DISTINCT id AS k, g AS v FROM %[1]s ` +
				`WHERE id < 5) s ORDER BY d`, 5},
			{"Union", `SELECT k * 2 AS d FROM (SELECT id AS k FROM %[1]s WHERE id < 3 UNION ALL ` +
				`SELECT id FROM %[1]s WHERE id < 2) s ORDER BY d`, 5},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if !sameNames(res.Columns, []string{"d"}) {
						t.Errorf("%s arm returned columns %v, want [d]\n  SQL: %s",
							arm.name, res.Columns, sql)
					}
					if len(res.Rows) != c.want {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), c.want, sql)
					}
				}
			})
		}
	})
	t.Run("F2/WindowOverAnAggregateAlias", func(t *testing.T) {
		// The sort above the window keyed on the alias, which nothing
		// between the aggregate and the gather emitted: loud. Its filter
		// sibling was silent — `t.k` named a column the window's input does
		// not carry, so every row was UNKNOWN and the answer was zero rows.
		groups := map[any]int64{}
		for _, r := range rows {
			groups[r.g]++
		}
		var total int64
		for _, c := range groups {
			total += c
		}
		sql := fmt.Sprintf(
			`SELECT k, SUM(v) OVER () AS w FROM (SELECT g + 1 AS k, COUNT(*) AS v FROM %s `+
				`GROUP BY g + 1) s ORDER BY k`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if len(res.Rows) != len(groups) {
				t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
					arm.name, len(res.Rows), len(groups), sql)
			}
			for _, r := range res.Rows {
				if w, _ := numAsInt(r["w"]); w != total {
					t.Errorf("%s arm answered w=%v, want %d\n  SQL: %s", arm.name, r["w"], total, sql)
				}
			}
		}
		var wantFiltered int64
		for g := range groups {
			if g != nil {
				wantFiltered++ // the NULL group's key is NULL, which `>= 0` rejects
			}
		}
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT k, SUM(v) OVER () AS w FROM `+
				`(SELECT g + 1 AS k, COUNT(*) AS v FROM %s GROUP BY g + 1) s) t WHERE t.k >= 0`,
			tbl), "n", wantFiltered)
	})
	t.Run("F2/ProjThenFilterOverEveryProducer", func(t *testing.T) {
		// The consumer shape the live checks flagged most often, run against
		// every producer class as a VALUE gate: a projection AND a filter
		// above a producer that emits no stage of its own. Every count is
		// computed from the fixture generator.
		groups := map[int32]int64{}
		var nullGroup bool
		for _, r := range rows {
			if g, ok := r.g.(int32); ok {
				groups[g]++
			} else {
				nullGroup = true
			}
		}
		countIf := func(pred func(sfcRow) bool) int64 {
			var n int64
			for _, r := range rows {
				if pred(r) {
					n++
				}
			}
			return n
		}
		posUnder := func(limit int64) int64 {
			return countIf(func(r sfcRow) bool {
				g, ok := r.g.(int32)
				return ok && g > 0 && r.id < limit
			})
		}
		// The ids the ORDER-BY-LIMIT producers keep, which is what makes
		// them assertable at all: id is unique, so the first n by id are
		// ids 0..n-1.
		var idRange = func(lo, hi int64) int64 {
			return countIf(func(r sfcRow) bool {
				g, ok := r.g.(int32)
				return ok && g > 0 && r.id >= lo && r.id < hi
			})
		}
		nGroups := int64(len(groups))
		if nullGroup {
			nGroups++ // g+1 over a NULL g is a NULL key, still a group
		}
		var bigGroups int64
		for _, c := range groups {
			if c > 500 {
				bigGroups++
			}
		}
		for _, c := range []struct {
			name string
			body string
			want int64
		}{
			{"scan", `SELECT id AS k, g AS v FROM %[1]s WHERE id < 200`, posUnder(200)},
			{"sort", `SELECT id AS k, g AS v FROM %[1]s WHERE id < 200 ORDER BY id`, posUnder(200)},
			{"sortlimit", `SELECT id AS k, g AS v FROM %[1]s ORDER BY id LIMIT 50`, idRange(0, 50)},
			{"offset", `SELECT id AS k, g AS v FROM %[1]s ORDER BY id LIMIT 50 OFFSET 10`, idRange(10, 60)},
			{"window", `SELECT id AS k, ROW_NUMBER() OVER (ORDER BY id) AS v FROM %[1]s WHERE id < 200`, 200},
			{"winpart", `SELECT id AS k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS v ` +
				`FROM %[1]s WHERE id < 200`, 200},
			{"sortwin", `SELECT id AS k, ROW_NUMBER() OVER (ORDER BY id) AS v FROM %[1]s ` +
				`WHERE id < 200 ORDER BY id`, 200},
			{"agg", `SELECT g + 1 AS k, COUNT(*) AS v FROM %[1]s GROUP BY g + 1`, nGroups},
			{"agghaving", `SELECT g + 1 AS k, COUNT(*) AS v FROM %[1]s GROUP BY g + 1 ` +
				`HAVING COUNT(*) > 500`, bigGroups},
			{"distinct", `SELECT DISTINCT id AS k, g AS v FROM %[1]s WHERE id < 200`, posUnder(200)},
			{"union", `SELECT id AS k, g AS v FROM %[1]s WHERE id < 3 UNION ALL ` +
				`SELECT id, g FROM %[1]s WHERE id < 2`, posUnder(3) + posUnder(2)},
			{"cte", `WITH_CTE`, posUnder(200)},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				var sql string
				if c.body == "WITH_CTE" {
					sql = fmt.Sprintf(`WITH c AS (SELECT id AS k, g AS v FROM %[1]s WHERE id < 200) `+
						`SELECT k * 2 AS d FROM c WHERE v > 0 ORDER BY d`, tbl)
				} else {
					sql = fmt.Sprintf(`SELECT k * 2 AS d FROM (`+c.body+`) s WHERE s.v > 0 ORDER BY d`, tbl)
				}
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if !sameNames(res.Columns, []string{"d"}) {
						t.Errorf("%s arm returned columns %v, want [d]\n  SQL: %s",
							arm.name, res.Columns, sql)
					}
					if int64(len(res.Rows)) != c.want {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), c.want, sql)
					}
				}
			})
		}
	})
	t.Run("F2/OrderedProjectionUnderAnOuterAggregate", func(t *testing.T) {
		// An ORDER BY on an alias INSIDE a derived table whose consumer is
		// an aggregate. The outer COUNT(*) needs no columns, so the
		// projection that computes the key is pruned and the sort keys on a
		// name nothing emits: `sort: key column "d" does not exist in the
		// input schema` at DISPATCH, for a query PostgreSQL answers. Loud
		// rather than silent, and pre-existing — the sort-key half of the
		// same placement question. Refused at plan time now, which routes it
		// local and answers it.
		for _, c := range []struct {
			name string
			body string
		}{
			{"scan", `SELECT id * 2 AS d FROM %[1]s WHERE id < 5 ORDER BY d`},
			{"aggregate", `SELECT k * 2 AS d FROM (SELECT g + 1 AS k, COUNT(*) AS v FROM %[1]s ` +
				`GROUP BY g + 1) s ORDER BY d`},
			{"distinct", `SELECT k * 2 AS d FROM (SELECT DISTINCT id AS k, g AS v FROM %[1]s ` +
				`WHERE id < 5) s ORDER BY d`},
			{"union", `SELECT k * 2 AS d FROM (SELECT id AS k, g AS v FROM %[1]s WHERE id < 3 ` +
				`UNION ALL SELECT id, g FROM %[1]s WHERE id < 2) s ORDER BY d`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				inner := fmt.Sprintf(c.body, tbl)
				// The bare spelling must still plan on the DAG and answer.
				bare := sfcRun(t, sfcArms(ctx, single, coord)[1], inner)
				sfcScalar(t, ctx, single, coord,
					`SELECT count(*) AS n FROM (`+inner+`) x`, "n", int64(len(bare.Rows)))
			})
		}
	})
	t.Run("F2/NestedWrappedWindowIsRoutedLocal", func(t *testing.T) {
		// The DAG cannot compute this SELECT list — a window wrapped in an
		// expression one level down, whose defining AST extractOutputRenames
		// never sees. It used to answer with the window stage's raw output,
		// `__win_0` included; it is refused at plan time now and answered on
		// the local engine, so the client gets its one column.
		before := coord.UnreachableOutputLocalRoutes()
		sql := fmt.Sprintf(
			`SELECT x FROM (SELECT SUM(id) OVER () + 1 AS x FROM %s WHERE id < 5) s`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			if !sameNames(res.Columns, []string{"x"}) {
				t.Errorf("%s arm returned columns %v, want [x]\n  SQL: %s",
					arm.name, res.Columns, sql)
			}
		}
		if got := coord.UnreachableOutputLocalRoutes(); got <= before {
			t.Errorf("the coordinator answered without taking the reachability route (%d → %d)",
				before, got)
		}
	})

	// --- the third adversarial round ---------------------------------------

	t.Run("N1/SharedCTEAggregateWithAComputedGroupAlias", func(t *testing.T) {
		// The structural shared-producer rule refused any Filter or Project
		// on a stage with two consumers, which is wrong in one direction:
		// absorbAggregateOutputProjection's projection belongs to the CTE
		// BODY, and every reference reads it equally. Three shapes with no
		// consumer-scoped anything reached the client as a hard error.
		//
		// The values are PostgreSQL 17's over the same fixture, and the
		// NULL group's key is NULL, which every comparison rejects.
		groups := map[int32]int64{}
		var nullGroup bool
		for _, r := range rows {
			if g, ok := r.g.(int32); ok {
				groups[g]++
			} else {
				nullGroup = true
			}
		}
		nGroups := int64(len(groups))
		if nullGroup {
			nGroups++
		}
		var above3, below3 int64
		for g := range groups {
			if int64(g)+1 > 3 {
				above3++
			}
			if int64(g)+1 < 3 {
				below3++
			}
		}
		body := fmt.Sprintf(`WITH a AS (SELECT g + 1 AS gk, COUNT(*) AS n FROM %s `+
			`GROUP BY g + 1) `, tbl)
		t.Run("UnionAllNoFilter", func(t *testing.T) {
			sql := body + `SELECT gk FROM a UNION ALL SELECT gk FROM a ORDER BY gk`
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if int64(len(res.Rows)) != 2*nGroups {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), 2*nGroups, sql)
				}
				// PostgreSQL orders ASC with NULLs LAST, so the last two
				// rows are the NULL group and no earlier row is NULL.
				for i, r := range res.Rows {
					isNull := r["gk"] == nil
					if want := i >= len(res.Rows)-2; isNull != want {
						t.Fatalf("%s arm: row %d gk=%v; ASC orders NULLs LAST\n  SQL: %s",
							arm.name, i, r["gk"], sql)
					}
				}
			}
		})
		t.Run("UnionAllWithAFilterOnEachRef", func(t *testing.T) {
			sfcScalar(t, ctx, single, coord, body+
				`SELECT COUNT(*) AS n FROM (SELECT gk FROM a WHERE gk > 3 `+
				`UNION ALL SELECT gk FROM a WHERE gk < 3) u`, "n", above3+below3)
		})
		t.Run("SelfJoinOnTheAlias", func(t *testing.T) {
			// The NULL key joins to nothing, so the count is the non-NULL
			// group count.
			sfcScalar(t, ctx, single, coord, body+
				`SELECT COUNT(*) AS n FROM a JOIN a b ON a.gk = b.gk`, "n", int64(len(groups)))
		})
		t.Run("ctl/APlainGroupKeyStillAnswers", func(t *testing.T) {
			sql := fmt.Sprintf(`WITH a AS (SELECT g AS gk, COUNT(*) AS n FROM %s GROUP BY g) `+
				`SELECT gk FROM a UNION ALL SELECT gk FROM a ORDER BY gk`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if int64(len(res.Rows)) != 2*nGroups {
					t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), 2*nGroups, sql)
				}
			}
		})
	})
	t.Run("N1/AComputedGroupKeyIsAColumnNameAboveItsAggregate", func(t *testing.T) {
		// The silent half of the same finding, and a wrong VALUE rather than
		// a refusal: an aggregate emits its group key under the TEXT of the
		// GROUP BY expression, so above that stage `g + 1` names ONE column.
		// A projection that rebuilds it as arithmetic reads `g`, which the
		// aggregate does not emit, and every row answers NULL. Both the
		// inserted StageProject and a union arm's projection did that.
		var wantKeys []int64
		seen := map[int32]bool{}
		for _, r := range rows {
			if g, ok := r.g.(int32); ok && !seen[g] {
				seen[g] = true
				wantKeys = append(wantKeys, int64(g)+1)
			}
		}
		for _, c := range []struct{ name, sql string }{
			{"bare", `SELECT g + 1 AS gk FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"cte", `WITH a AS (SELECT g + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1) ` +
				`SELECT gk FROM a ORDER BY gk`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					var got int
					for _, r := range res.Rows {
						if r["gk"] != nil {
							got++
						}
					}
					if got != len(wantKeys) {
						t.Errorf("%s arm returned %d non-NULL keys, want %d — a computed group "+
							"key rebuilt as arithmetic answers NULL\n  SQL: %s",
							arm.name, got, len(wantKeys), sql)
					}
				}
			})
		}
	})
	t.Run("N2/CarrierSchemaRefusalsThatWereNotDefects", func(t *testing.T) {
		// assertCarrierSchemaResolves ran live and returned a plain error, so
		// nothing routed it local and it reached the client. Both shapes are
		// answerable, and both are fixed at the CARRIER rather than by
		// routing: the HAVING resolves because `g + 1` is the emitted column
		// NAME, and the window projection is no longer attached to a stage
		// that cannot compute it.
		t.Run("HavingOnAComputedGroupKeyReachesTheClient", func(t *testing.T) {
			// The refusal is gone. The ANSWER is still wrong, on BOTH paths,
			// and that is #720's pin below — this subtest owns only the
			// no-hard-error half.
			sql := fmt.Sprintf(`WITH a AS (SELECT g + 1 AS gk, COUNT(*) AS n FROM %s `+
				`GROUP BY g + 1 HAVING g + 1 > 2) SELECT gk, n FROM a WHERE gk > 3 ORDER BY gk`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				if _, err := arm.run(sql); err != nil {
					t.Errorf("%s arm refused a query PostgreSQL answers: %v\n  SQL: %s",
						arm.name, err, sql)
				}
			}
		})
		t.Run("WindowAliasForwardedThroughARename", func(t *testing.T) {
			// `id AS k` is what fires it: the outer list is two bare
			// forwards, one of them a derived alias whose definition lives
			// in `__win_0`. PostgreSQL answers 2 rows.
			sql := fmt.Sprintf(`SELECT k, s FROM (SELECT id AS k, SUM(c_i64) OVER () + 1 AS s `+
				`FROM %s WHERE id < 3) x WHERE k > 0 ORDER BY k`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if !sameNames(res.Columns, []string{"k", "s"}) {
					t.Errorf("%s arm returned columns %v, want [k s]", arm.name, res.Columns)
				}
				if len(res.Rows) != 2 {
					t.Errorf("%s arm returned %d rows, want 2\n  SQL: %s",
						arm.name, len(res.Rows), sql)
				}
				for _, r := range res.Rows {
					if r["s"] == nil {
						t.Errorf("%s arm answered s=NULL; the projection named a column its "+
							"carrier does not emit\n  SQL: %s", arm.name, sql)
					}
				}
			}
		})
	})
	t.Run("N3/HavingOnAComputedGroupKeyIsWrongOnBothPaths", func(t *testing.T) {
		// #720, pinned. PostgreSQL 17 answers 5 rows for the first and
		// REFUSES the second (a SELECT alias is not visible in HAVING); we
		// answer 0 for the first on both paths, and the two paths disagree
		// on the second. Pre-existing and byte-identical on the parent
		// commit — outside this class's mechanism, since it reproduces
		// single-process.
		//
		// TODO(#720): both halves flip when the HAVING predicate is spelled
		// against the aggregate's OUTPUT.
		expr := fmt.Sprintf(`SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1 `+
			`HAVING g + 1 > 2 ORDER BY gk`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, expr)
			if len(res.Rows) != 0 {
				t.Fatalf("%s arm now returns %d rows for a HAVING on a computed group key; "+
					"PostgreSQL answers 5. #720 is fixed — assert the five rows and delete "+
					"this pin\n  SQL: %s", arm.name, len(res.Rows), expr)
			}
		}
		// The control that must stay right.
		var want int64
		counts := map[int64]int64{}
		for _, r := range rows {
			if g, ok := r.g.(int32); ok {
				counts[int64(g)]++
			}
		}
		for g := range counts {
			if g > 2 {
				want++
			}
		}
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT g AS gk, COUNT(*) AS c FROM %s GROUP BY g `+
				`HAVING g > 2) s`, tbl), "n", want)
	})
	t.Run("N3/UnionOverTwoIdenticalSortedProducersIsRefused", func(t *testing.T) {
		// #715, pinned. Both arms lower to the same sorted producer and
		// UnionArm.DepStage names the merge_sort while Dependencies[0] names
		// the sort; the shape check refuses with a PLAIN error, so nothing
		// routes it local. Pre-existing and byte-identical on the parent
		// commit.
		//
		// TODO(#715): delete this pin when the two records agree.
		sql := fmt.Sprintf(`SELECT k FROM (SELECT id AS k FROM %[1]s WHERE id < 5 `+
			`ORDER BY id) a UNION ALL SELECT k FROM (SELECT id AS k FROM %[1]s WHERE id < 5 `+
			`ORDER BY id) b`, tbl)
		arms := sfcArms(ctx, single, coord)
		if res := sfcRun(t, arms[0], sql); len(res.Rows) != 10 {
			t.Errorf("the single-process arm returned %d rows, want 10", len(res.Rows))
		}
		if _, err := arms[1].run(sql); err == nil {
			t.Fatalf("the DAG now plans a union over two identical sorted producers — #715 " +
				"is fixed, assert the ten rows and delete this pin")
		} else if !strings.Contains(err.Error(), "names producer") {
			t.Errorf("the DAG failed for a different reason than #715: %v", err)
		}
	})

	// --- the fourth adversarial round --------------------------------------

	t.Run("R4/MixedArmUnionOverAComputedGroupKey", func(t *testing.T) {
		// A union whose arms are an AGGREGATE on a computed key and a plain
		// BASE TABLE. The aggregate arm was untyped — a walk above the
		// aggregate read `g + 1` as arithmetic over a column it does not
		// emit and fell to the float rule — so the set operation reconciled
		// to double and CAST the other arm. Once the aggregate arm stopped
		// answering NULL, the two arms disagreed about what `gk` IS: the
		// worker copies a bare reference and ignores the declaration, so the
		// sort above read an INT64 column as float, got an empty typed slice
		// and INDEXED it. A recovered panic, reported as an internal error,
		// on a query PostgreSQL answers.
		//
		// PostgreSQL 17: 0 1 1 2 2 3 4 5 6 7 NULL — eleven rows, and the
		// union of bigint and int resolves BIGINT, not double.
		wantKeys := map[int64]bool{}
		var nullKey bool
		for _, r := range rows {
			if g, ok := r.g.(int32); ok {
				wantKeys[int64(g)+1] = true
			} else {
				nullKey = true
			}
		}
		want := int64(len(wantKeys))
		if nullKey {
			want++
		}
		want += 3 // the base-table arm's id < 3
		agg := fmt.Sprintf(`SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1`, tbl)
		for _, c := range []struct{ name, sql string }{
			{"Ordered", `WITH a AS (` + agg + `) SELECT gk FROM a UNION ALL ` +
				`SELECT g FROM %[1]s WHERE id < 3 ORDER BY gk`},
			{"Unordered", `WITH a AS (` + agg + `) SELECT gk FROM a UNION ALL ` +
				`SELECT g FROM %[1]s WHERE id < 3`},
			{"ArmsSwapped", `SELECT g AS gk FROM %[1]s WHERE id < 3 UNION ALL ` +
				`SELECT gk FROM (` + agg + `) a ORDER BY gk`},
			{"ExplicitCast", `WITH a AS (` + agg + `) SELECT gk FROM a UNION ALL ` +
				`SELECT CAST(g AS BIGINT) FROM %[1]s WHERE id < 3 ORDER BY gk`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if int64(len(res.Rows)) != want {
						t.Errorf("%s arm returned %d rows, want %d — the base-table arm's rows "+
							"vanish when the arms disagree\n  SQL: %s",
							arm.name, len(res.Rows), want, sql)
					}
					var nonNull int
					for _, r := range res.Rows {
						if r["gk"] != nil {
							nonNull++
						}
					}
					if int64(nonNull) != want-1 {
						t.Errorf("%s arm returned %d non-NULL keys, want %d\n  SQL: %s",
							arm.name, nonNull, want-1, sql)
					}
				}
			})
		}
		t.Run("ctl/APlainGroupKeyStillAnswers", func(t *testing.T) {
			sfcScalar(t, ctx, single, coord, fmt.Sprintf(
				`WITH a AS (SELECT g AS gk, COUNT(*) AS c FROM %[1]s GROUP BY g) `+
					`SELECT COUNT(*) AS n FROM (SELECT gk FROM a UNION ALL `+
					`SELECT g FROM %[1]s WHERE id < 3) u`, tbl), "n", want)
		})
	})
	t.Run("R4/GroupKeyTextMatchingIsSpellingSensitive", func(t *testing.T) {
		// #723, pinned on BOTH arms. A SELECT item is matched to its GROUP BY
		// key by the TEXT of the expression, and the normalisation differs by
		// path and by site: whitespace is normalised, parentheses and
		// identifier case are not, and which side carries the parenthesis
		// decides which path gets it wrong. PostgreSQL answers every spelling
		// identically because it matches by expression EQUIVALENCE.
		//
		// TODO(#723): every "0" below becomes 7 when the comparison goes
		// through the AST. Delete this pin then and assert PostgreSQL's
		// answer for all six.
		var wantKeys int
		seen := map[int32]bool{}
		for _, r := range rows {
			if g, ok := r.g.(int32); ok && !seen[g] {
				seen[g] = true
				wantKeys++
			}
		}
		for _, c := range []struct {
			name            string
			sql             string
			singleNN, dagNN int
		}{
			{"Plain", `SELECT g + 1 AS gk FROM %[1]s GROUP BY g + 1 ORDER BY gk`, wantKeys, wantKeys},
			{"Whitespace", `SELECT g+1 AS gk FROM %[1]s GROUP BY g + 1 ORDER BY gk`, wantKeys, wantKeys},
			{"ParenOnBoth", `SELECT (g + 1) AS gk FROM %[1]s GROUP BY (g + 1) ORDER BY gk`, wantKeys, wantKeys},
			{"ParenOnTheSelect", `SELECT (g + 1) AS gk FROM %[1]s GROUP BY g + 1 ORDER BY gk`, 0, wantKeys},
			{"IdentifierCase", `SELECT G + 1 AS gk FROM %[1]s GROUP BY g + 1 ORDER BY gk`, 0, wantKeys},
			{"ParenOnTheGroupBy", `SELECT g + 1 AS gk FROM %[1]s GROUP BY (g + 1) ORDER BY gk`, 0, 0},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				sql := fmt.Sprintf(c.sql, tbl)
				for i, arm := range sfcArms(ctx, single, coord) {
					want := c.singleNN
					if i == 1 {
						want = c.dagNN
					}
					res := sfcRun(t, arm, sql)
					var nonNull int
					for _, r := range res.Rows {
						if r["gk"] != nil {
							nonNull++
						}
					}
					if nonNull == want {
						continue
					}
					if want == 0 {
						t.Fatalf("%s arm now answers %d non-NULL keys where this pin records 0; "+
							"PostgreSQL answers %d. #723 is fixed for this spelling — assert it "+
							"and delete the pin\n  SQL: %s", arm.name, nonNull, wantKeys, sql)
					}
					t.Errorf("%s arm answered %d non-NULL keys, want %d\n  SQL: %s",
						arm.name, nonNull, want, sql)
				}
			})
		}
	})

	// --- the fifth adversarial round ---------------------------------------

	t.Run("R5/SelfJoinOrderedOnAliasesOfQualifiedColumns", func(t *testing.T) {
		// The sharpest kind of silent: the right ROWS in the wrong SEQUENCE.
		//
		// `SELECT a.k AS lo, b.k AS hi FROM t a JOIN t b ON … ORDER BY lo, hi`
		// fuses its ordering into the JOIN stage. Round 3's StageProject was
		// then inserted between that join and the gather to compute the
		// SELECT list — and a stage's ordering is read off the DIRECT
		// dependency, so the gather saw a `project` stage that declares no
		// SortKeys and merged the join's ordered files in arrival order. The
		// multiset stayed correct and the ORDER BY silently did nothing.
		//
		// Asserted as a SEQUENCE, computed in Go from the fixture generator:
		// a multiset assertion is exactly the one that cannot see this.
		type pair struct{ lo, hi int64 }
		build := func(limit int64, desc bool) []pair {
			byGroup := map[int32][]int64{}
			for _, r := range rows {
				if g, ok := r.g.(int32); ok {
					byGroup[g] = append(byGroup[g], r.id)
				}
			}
			var out []pair
			for _, ids := range byGroup {
				for _, a := range ids {
					if a >= limit {
						continue
					}
					for _, b := range ids {
						if a < b {
							out = append(out, pair{a, b})
						}
					}
				}
			}
			sort.Slice(out, func(i, j int) bool {
				if out[i].lo != out[j].lo {
					if desc {
						return out[i].lo > out[j].lo
					}
					return out[i].lo < out[j].lo
				}
				if desc {
					return out[i].hi > out[j].hi
				}
				return out[i].hi < out[j].hi
			})
			return out
		}
		join := fmt.Sprintf(`FROM %[1]s a JOIN %[1]s b ON a.g = b.g AND a.id < b.id `+
			`WHERE a.id < 30`, tbl)
		assertSeq := func(t *testing.T, sql string, want []pair) {
			t.Helper()
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != len(want) {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), len(want), sql)
				}
				for i, r := range res.Rows {
					lo, _ := numAsInt(r["lo"])
					hi, _ := numAsInt(r["hi"])
					if lo != want[i].lo || hi != want[i].hi {
						t.Fatalf("%s arm: row %d is (%d,%d), want (%d,%d) — the ORDER BY did "+
							"not run\n  SQL: %s", arm.name, i, lo, hi, want[i].lo, want[i].hi, sql)
					}
				}
			}
		}
		asc, desc := build(30, false), build(30, true)
		t.Run("OnAliases", func(t *testing.T) {
			assertSeq(t, `SELECT a.id AS lo, b.id AS hi `+join+` ORDER BY lo, hi`, asc)
		})
		t.Run("OnTheQualifiedColumns", func(t *testing.T) {
			assertSeq(t, `SELECT a.id AS lo, b.id AS hi `+join+` ORDER BY a.id, b.id`, asc)
		})
		t.Run("OneAliasAndOneQualifiedColumn", func(t *testing.T) {
			assertSeq(t, `SELECT a.id AS lo, b.id AS hi `+join+` ORDER BY lo, b.id`, asc)
		})
		t.Run("Descending", func(t *testing.T) {
			assertSeq(t, `SELECT a.id AS lo, b.id AS hi `+join+` ORDER BY lo DESC, hi DESC`, desc)
		})
		t.Run("WithALimit", func(t *testing.T) {
			assertSeq(t, `SELECT a.id AS lo, b.id AS hi `+join+` ORDER BY lo, hi LIMIT 7`, asc[:7])
		})
		t.Run("AComputedSelectItem", func(t *testing.T) {
			// `a.id * 10` cannot be a passthrough, so this is the spelling
			// that genuinely needs the projection — and it must still not
			// cost the join's ordering.
			sql := `SELECT a.id * 10 AS lo, b.id AS hi ` + join + ` ORDER BY lo, hi`
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != len(asc) {
					t.Fatalf("%s arm returned %d rows, want %d", arm.name, len(res.Rows), len(asc))
				}
				for i, r := range res.Rows {
					lo, _ := numAsInt(r["lo"])
					hi, _ := numAsInt(r["hi"])
					if lo != asc[i].lo*10 || hi != asc[i].hi {
						t.Fatalf("%s arm: row %d is (%d,%d), want (%d,%d)\n  SQL: %s",
							arm.name, i, lo, hi, asc[i].lo*10, asc[i].hi, sql)
					}
				}
			}
		})
		t.Run("AnOrderedAggregateThatDoesTakeTheProjectStage", func(t *testing.T) {
			// The other side of the same rule, and the one that keeps it
			// from being a blanket ban. An aggregate is a SINGLE ordered
			// stream, so a projection above it can carry the ordering with
			// it — the keys survive the projection under their own names and
			// concatenating one file is still that file. The stage IS
			// inserted here, and the ORDER BY must still run.
			var keys []int64
			seen := map[int32]bool{}
			for _, r := range rows {
				if g, ok := r.g.(int32); ok && !seen[g] {
					seen[g] = true
					keys = append(keys, int64(g))
				}
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
			sql := fmt.Sprintf(`SELECT k FROM (SELECT g AS k, COUNT(*) AS c FROM %s `+
				`GROUP BY g) t WHERE k >= 0 ORDER BY k`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != len(keys) {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), len(keys), sql)
				}
				if !sameNames(res.Columns, []string{"k"}) {
					t.Errorf("%s arm returned columns %v, want [k]", arm.name, res.Columns)
				}
				for i, r := range res.Rows {
					got, _ := numAsInt(r["k"])
					if got != keys[i] {
						t.Fatalf("%s arm: row %d is %d, want %d — the ORDER BY did not survive "+
							"the inserted projection\n  SQL: %s", arm.name, i, got, keys[i], sql)
					}
				}
			}
		})
		t.Run("AThreeWaySelfJoin", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT a.id AS lo, b.id AS mid, c.id AS hi FROM %[1]s a `+
				`JOIN %[1]s b ON a.g = b.g AND a.id < b.id `+
				`JOIN %[1]s c ON b.g = c.g AND b.id < c.id `+
				`WHERE a.id < 8 AND c.id < 40 ORDER BY lo, mid, hi`, tbl)
			var last [3]int64
			first := true
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) == 0 {
					t.Fatalf("%s arm returned no rows", arm.name)
				}
				var prev [3]int64
				for i, r := range res.Rows {
					lo, _ := numAsInt(r["lo"])
					mid, _ := numAsInt(r["mid"])
					hi, _ := numAsInt(r["hi"])
					cur := [3]int64{lo, mid, hi}
					if i > 0 && !tripleLess(prev, cur) {
						t.Fatalf("%s arm: row %d %v does not follow %v\n  SQL: %s",
							arm.name, i, cur, prev, sql)
					}
					prev = cur
				}
				if first {
					last, first = prev, false
				} else if last != prev {
					t.Errorf("the two arms end on different rows: %v vs %v", last, prev)
				}
			}
		})
	})

	// --- controls: shapes that were already right --------------------------

	t.Run("ctl/HavingStillRunsBelowTheSelectList", func(t *testing.T) {
		var want int64
		counts := map[int64]int64{}
		for _, r := range rows {
			if g, ok := r.g.(int32); ok {
				counts[int64(g)]++
			}
		}
		for _, c := range counts {
			if c > 500 {
				want++
			}
		}
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT g, COUNT(*) AS c FROM %s GROUP BY g `+
				`HAVING COUNT(*) > 500) s`, tbl), "n", want)
	})
	t.Run("ctl/QualifyStillRunsAboveTheWindow", func(t *testing.T) {
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn `+
				`FROM %s) x WHERE rn <= 3`, tbl), "n", 3)
	})
	t.Run("ctl/FilterStillRunsAboveABareLimit", func(t *testing.T) {
		// LIMIT with no ORDER BY: the rows are whichever ten the scan
		// produced, so only the BOUND is assertable — the predicate must
		// still cut the answer to at most ten, never the whole table.
		sql := fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id FROM %s LIMIT 10) s WHERE id >= 0`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, sql)
			n, _ := numAsInt(res.Rows[0]["n"])
			if n != 10 {
				t.Errorf("%s arm answered %d, want 10\n  SQL: %s", arm.name, n, sql)
			}
		}
	})
}

// sfcArm is one execution path.
type sfcArm struct {
	name string
	run  func(sql string) (*oracle.Result, error)
}

func sfcArms(ctx context.Context, single *wadjet.DB, coord *Coordinator) []sfcArm {
	return []sfcArm{
		{"single", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
	}
}

func sfcRun(t *testing.T, arm sfcArm, sql string) *oracle.Result {
	t.Helper()
	res, err := arm.run(sql)
	if err != nil {
		t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, sql)
	}
	return res
}

// sfcScalar asserts a one-row, one-column integer answer on both arms.
func sfcScalar(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator,
	sql, col string, want int64) {
	t.Helper()
	for _, arm := range sfcArms(ctx, single, coord) {
		res := sfcRun(t, arm, sql)
		if len(res.Rows) != 1 {
			t.Fatalf("%s arm returned %d rows, want 1\n  SQL: %s", arm.name, len(res.Rows), sql)
		}
		got, ok := numAsInt(res.Rows[0][col])
		if !ok {
			t.Fatalf("%s arm returned %T for %q\n  SQL: %s", arm.name, res.Rows[0][col], col, sql)
		}
		if got != want {
			t.Errorf("%s arm answered %d, want %d\n  SQL: %s", arm.name, got, want, sql)
		}
	}
}

// tripleLess is strict lexicographic order over three sort keys.
func tripleLess(a, b [3]int64) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
