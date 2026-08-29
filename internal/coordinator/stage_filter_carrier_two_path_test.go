package coordinator

import (
	"context"
	"fmt"
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
	t.Run("C/WindowOverADecimalExpressionIsFloat", func(t *testing.T) {
		// NOT a placement defect, and pinned here so the claim is checkable:
		// DECIMAL arithmetic itself lands in float64 today (the TODO(#555)
		// in nodeDeclaredType — expr.BinOpNumeric has no Int128 Mul), so
		// `SELECT c_dec * 2` is float on both paths and a window over it
		// declares what the value really is. PostgreSQL answers 20.0020.
		// When #555 lands this pin fails, which is the signal to re-declare
		// the materialized window key DECIMAL.
		for _, sql := range []string{
			fmt.Sprintf(`SELECT id, c_dec * 2 AS d FROM %s WHERE id < 5 ORDER BY id`, tbl),
			fmt.Sprintf(`SELECT id, SUM(c_dec * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`, tbl),
		} {
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				for _, r := range res.Rows {
					for _, col := range []string{"d", "s"} {
						v, present := r[col]
						if !present {
							continue
						}
						if _, isFloat := v.(float64); !isFloat {
							t.Errorf("%s arm returned %T for %q; the #555 pin expected float64. "+
								"If DECIMAL arithmetic is exact now, re-declare the materialized "+
								"window key DECIMAL and delete this pin.\n  SQL: %s",
								arm.name, v, col, sql)
						}
					}
				}
			}
		}
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
			if c > 700 {
				want++
			}
		}
		sfcScalar(t, ctx, single, coord, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT g, COUNT(*) AS c FROM %s GROUP BY g `+
				`HAVING COUNT(*) > 700) s`, tbl), "n", want)
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
