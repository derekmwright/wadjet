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
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
			// TWO windows over DIFFERENT expressions of ONE column, read
			// through the SECOND one. resolveWindowKeys keys its map by the
			// term's TEXT so that two clauses sharing a term compute it once;
			// the mirror risk is two clauses that do NOT share one collapsing
			// into a single slot, which would give both the same value.
			{"TwoExpressionsOfOneColumn",
				`SELECT id, SUM(c_i64 * 3) OVER () AS other, SUM(c_i64 * 2) OVER () AS s ` +
					`FROM %s WHERE id < 5 ORDER BY id`, sum},
			// A window over an expression INSIDE a derived table, consumed
			// above it: the outer reference reads a stage the window produced,
			// not the scan the argument was written against.
			{"ThroughADerivedTable",
				`SELECT id, s FROM (SELECT id, SUM(c_i64 * 2) OVER () AS s FROM %s WHERE id < 5) x ` +
					`ORDER BY id`, sum},
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
		// ARITHMETIC over COALESCE's output was float on both paths until
		// #724's sibling commit, so `SUM(COALESCE(c_dec, 0) * 2)` answered
		// 20.002000000000002 where PostgreSQL answers 20.0020. The runtime
		// knew the COALESCE produced a DECIMAL (expr.DecimalResultOf) and the
		// PLAN did not (physical.decimalArithOperand had no arm for a choice),
		// so the multiplication was declared and run in float64.
		//
		// Both spellings are exact now. Asserting the WINDOWED one beside the
		// GROUPED one is what keeps the two questions one question: they were
		// equal while both were wrong, which is how the window
		// materialization was exonerated in the first place.
		const wantExact = "20.0020"
		win := fmt.Sprintf(
			`SELECT id, SUM(COALESCE(c_dec, 0) * 2) OVER () AS s FROM %s WHERE id < 5 ORDER BY id`, tbl)
		grouped := fmt.Sprintf(
			`SELECT SUM(COALESCE(c_dec, 0) * 2) AS s FROM %s WHERE id < 5`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			w := sfcRun(t, arm, win)
			g := sfcRun(t, arm, grouped)
			if len(w.Rows) == 0 || len(g.Rows) == 0 {
				t.Fatalf("%s arm returned no rows", arm.name)
			}
			if got := fmt.Sprint(g.Rows[0]["s"]); got != wantExact {
				t.Errorf("%s arm answered %q for %q, want %s exactly — PostgreSQL's numeric "+
					"is exact and so is wadjet's", arm.name, got, grouped, wantExact)
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
	t.Run("N3/HavingOnAComputedGroupKey", func(t *testing.T) {
		// #720, fixed. A HAVING over a computed group key returned ZERO rows
		// on both paths: above the aggregate `g + 1` is the NAME of one
		// column and `g` is gone, so a predicate left as arithmetic answered
		// UNKNOWN on every row and the filter admitted nothing.
		//
		// The expectation is computed from the fixture generator and equals
		// the five rows PostgreSQL 17 answers: (3,659) (4,659) (5,659)
		// (6,659) (7,660).
		wantHaving := map[int64]int64{}
		for _, r := range rows {
			g, ok := r.g.(int32)
			if !ok {
				continue
			}
			if gk := int64(g) + 1; gk > 2 {
				wantHaving[gk]++
			}
		}
		expr := fmt.Sprintf(`SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1 `+
			`HAVING g + 1 > 2 ORDER BY gk`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			res := sfcRun(t, arm, expr)
			if len(res.Rows) != len(wantHaving) {
				t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
					arm.name, len(res.Rows), len(wantHaving), expr)
			}
			var last int64
			for i, r := range res.Rows {
				gk, ok := numAsInt(r["gk"])
				if !ok {
					t.Fatalf("%s arm returned %T for gk\n  SQL: %s", arm.name, r["gk"], expr)
				}
				if i > 0 && gk <= last {
					t.Errorf("%s arm returned gk out of order at row %d: %d after %d\n  SQL: %s",
						arm.name, i, gk, last, expr)
				}
				last = gk
				n, _ := numAsInt(r["n"])
				if n != wantHaving[gk] {
					t.Errorf("%s arm answered n=%d for gk=%d, want %d\n  SQL: %s",
						arm.name, n, gk, wantHaving[gk], expr)
				}
			}
		}
		// A select-list ALIAS is not visible in HAVING. PostgreSQL refuses
		// this with `column "gk" does not exist`; both paths must refuse it
		// too, and the DAG answering it with zero rows was the other half of
		// #720.
		aliasSQL := fmt.Sprintf(`SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY g + 1 `+
			`HAVING gk > 2 ORDER BY gk`, tbl)
		for _, arm := range sfcArms(ctx, single, coord) {
			if _, err := arm.run(aliasSQL); err == nil {
				t.Errorf("%s arm ANSWERED a HAVING on a select-list alias; PostgreSQL refuses it "+
					"with 42703\n  SQL: %s", arm.name, aliasSQL)
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
	t.Run("R4/AGroupKeyIsResolvedByIdentityNotBySpelling", func(t *testing.T) {
		// #723 and #725, fixed. A SELECT item used to be matched to its
		// GROUP BY key by the TEXT of the expression, and the normalisation
		// differed by path and by site: whitespace was normalised,
		// parentheses and identifier case were not, and which side carried
		// the parenthesis decided which path came back with a key column
		// that was NULL on every row.
		//
		// Every expectation below is computed from the fixture generator and
		// equals what PostgreSQL 17 answers for the same SQL: eight rows,
		// keys 1..7 and one NULL group.
		wantN := map[int64]int64{}
		var nullRows int64
		for _, r := range rows {
			g, ok := r.g.(int32)
			if !ok {
				nullRows++
				continue
			}
			wantN[int64(g)+1]++
		}
		wantKeys := len(wantN)

		// assertKeyed checks the FULL answer of a `gk, n` query: the row
		// count, the ascending key sequence, the per-key counts, and that
		// the NULL group is present exactly once and last. A count of
		// non-NULL keys alone cannot see a key column that carries the
		// wrong VALUE, only one that carries none.
		assertKeyed := func(t *testing.T, sql string, keyOf func(int64) int64) {
			t.Helper()
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != wantKeys+1 {
					t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), wantKeys+1, sql)
					continue
				}
				seenNull := 0
				var last int64
				for i, r := range res.Rows {
					n, _ := numAsInt(r["n"])
					if r["gk"] == nil {
						seenNull++
						if i != len(res.Rows)-1 {
							t.Errorf("%s arm put the NULL key at row %d of %d; PostgreSQL sorts "+
								"NULLS LAST for ASC\n  SQL: %s", arm.name, i, len(res.Rows), sql)
						}
						if n != nullRows {
							t.Errorf("%s arm answered n=%d for the NULL key, want %d\n  SQL: %s",
								arm.name, n, nullRows, sql)
						}
						continue
					}
					gk, ok := numAsInt(r["gk"])
					if !ok {
						t.Errorf("%s arm returned %T for gk\n  SQL: %s", arm.name, r["gk"], sql)
						continue
					}
					if i > 0 && gk <= last {
						t.Errorf("%s arm returned gk out of order at row %d: %d after %d\n  SQL: %s",
							arm.name, i, gk, last, sql)
					}
					last = gk
					want, known := wantN[keyOf(gk)]
					if !known {
						t.Errorf("%s arm answered an unknown key %d\n  SQL: %s", arm.name, gk, sql)
						continue
					}
					if n != want {
						t.Errorf("%s arm answered n=%d for gk=%d, want %d\n  SQL: %s",
							arm.name, n, gk, want, sql)
					}
				}
				if seenNull != 1 {
					t.Errorf("%s arm returned %d NULL-key rows, want exactly 1\n  SQL: %s",
						arm.name, seenNull, sql)
				}
			}
		}
		self := func(k int64) int64 { return k }

		// The SELECT item, spelled every way SQL allows for one expression.
		for _, c := range []struct{ name, sql string }{
			{"Plain", `SELECT g + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"Whitespace", `SELECT g+1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"ParenOnBoth", `SELECT (g + 1) AS gk, COUNT(*) AS n FROM %[1]s GROUP BY (g + 1) ORDER BY gk`},
			{"ParenOnTheSelect", `SELECT (g + 1) AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"ParenOnTheGroupBy", `SELECT g + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY (g + 1) ORDER BY gk`},
			{"ParenNested", `SELECT ((g) + 1) AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"IdentifierCase", `SELECT G + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"OrderByTheExpression", `SELECT g + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY g + 1`},
			{"OrderByTheParenthesisedExpression", `SELECT g + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY (g + 1)`},
			{"OrderByOrdinal", `SELECT g + 1 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g + 1 ORDER BY 1`},
			{"KeyAlsoInsideAnAggregate", `SELECT g + 1 AS gk, COUNT(*) AS n, SUM(g + 1) AS s ` +
				`FROM %[1]s GROUP BY g + 1 ORDER BY gk`},
			{"DerivedTableOuterReference", `SELECT s.gk, s.n FROM (SELECT (g + 1) AS gk, ` +
				`COUNT(*) AS n FROM %[1]s GROUP BY g + 1) s ORDER BY s.gk`},
			{"CTEOuterReference", `WITH a AS (SELECT (g + 1) AS gk, COUNT(*) AS n FROM %[1]s ` +
				`GROUP BY g + 1) SELECT gk, n FROM a ORDER BY gk`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				assertKeyed(t, fmt.Sprintf(c.sql, tbl), self)
			})
		}

		// An EXPRESSION OVER the key rather than the key itself: not the key,
		// so nothing resolved it as one, and the aggregate's output has no
		// `g` to rebuild it from.
		t.Run("ExpressionOverTheKey", func(t *testing.T) {
			assertKeyed(t, fmt.Sprintf(`SELECT (g + 1) * 2 AS gk, COUNT(*) AS n FROM %s `+
				`GROUP BY g + 1 ORDER BY gk`, tbl), func(k int64) int64 { return k / 2 })
		})
		t.Run("FunctionOverTheKey", func(t *testing.T) {
			assertKeyed(t, fmt.Sprintf(`SELECT ABS(g + 1) AS gk, COUNT(*) AS n FROM %s `+
				`GROUP BY g + 1 ORDER BY gk`, tbl), self)
		})

		// An expression over a DECIMAL key, asserted DIGIT FOR DIGIT. The
		// key's own (p,s) has to reach the term written above it: without it
		// the term falls to the float rule and renders exact fixed point
		// through a float64, so `2.0000` comes back as `2` — the right value
		// with the wrong number of digits, which a float comparison cannot
		// see (ADR-0024 item 2). PostgreSQL 17 answers 2.0000, 4.0002,
		// 6.0004 … 40.0038.
		t.Run("DecimalExpressionOverTheKey", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT (c_dec + 1) * 2 AS x, COUNT(*) AS n FROM %s `+
				`WHERE id < 20 GROUP BY c_dec + 1 ORDER BY x`, tbl)
			want := make([]string, 0, 20)
			for i := 0; i < 20; i++ {
				// c_dec = i + 0.0001*(i%9973), so (c_dec + 1) * 2 has scale 4.
				want = append(want, fmt.Sprintf("%d.%04d", 2*(i+1), 2*i))
			}
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != len(want) {
					t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), len(want), sql)
					continue
				}
				for i, r := range res.Rows {
					got := fmt.Sprintf("%v", r["x"])
					if got != want[i] {
						t.Errorf("%s arm answered %q at row %d, want %q — an exact DECIMAL "+
							"rendered through a float declaration\n  SQL: %s",
							arm.name, got, i, want[i], sql)
					}
				}
			}
		})

		// TWO computed keys, and the two SELECT items spelled differently
		// from both of them.
		t.Run("TwoComputedKeys", func(t *testing.T) {
			type ab struct{ a, b int64 }
			want := map[ab]int64{}
			var nulls int64
			for _, r := range rows {
				g, ok := r.g.(int32)
				if !ok {
					nulls++
					continue
				}
				want[ab{int64(g) + 1, int64(g) * 2}]++
			}
			for _, sql := range []string{
				`SELECT g + 1 AS a, g * 2 AS b, COUNT(*) AS n FROM %[1]s GROUP BY g + 1, g * 2 ORDER BY a, b`,
				`SELECT (g + 1) AS a, (g * 2) AS b, COUNT(*) AS n FROM %[1]s GROUP BY g + 1, g * 2 ORDER BY a, b`,
				`SELECT g + 1 AS a, g * 2 AS b, COUNT(*) AS n FROM %[1]s GROUP BY g * 2, g + 1 ORDER BY a, b`,
			} {
				q := fmt.Sprintf(sql, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, q)
					if len(res.Rows) != len(want)+1 {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), len(want)+1, q)
						continue
					}
					for _, r := range res.Rows {
						if r["a"] == nil && r["b"] == nil {
							if n, _ := numAsInt(r["n"]); n != nulls {
								t.Errorf("%s arm answered n=%d for the NULL group, want %d\n  SQL: %s",
									arm.name, n, nulls, q)
							}
							continue
						}
						a, aok := numAsInt(r["a"])
						b, bok := numAsInt(r["b"])
						n, _ := numAsInt(r["n"])
						if !aok || !bok {
							t.Errorf("%s arm returned a NULL half of a non-NULL group\n  SQL: %s",
								arm.name, q)
							continue
						}
						if n != want[ab{a, b}] {
							t.Errorf("%s arm answered n=%d for (a=%d, b=%d), want %d\n  SQL: %s",
								arm.name, n, a, b, want[ab{a, b}], q)
						}
					}
				}
			}
		})

		// Associativity is NOT a spelling difference: `g - 1 - 2` and
		// `g - (1 - 2)` are two expressions and must stay two group keys.
		// An identity that erased parentheses by printing without them
		// would collapse these, which is the wrong answer in the more
		// dangerous direction.
		t.Run("ctl/AssociativityIsNotSpelling", func(t *testing.T) {
			for _, c := range []struct {
				sql   string
				keyOf func(int64) int64
			}{
				{`SELECT g - 1 - 2 AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g - 1 - 2 ORDER BY gk`,
					func(k int64) int64 { return k + 4 }},
				{`SELECT g - (1 - 2) AS gk, COUNT(*) AS n FROM %[1]s GROUP BY g - (1 - 2) ORDER BY gk`,
					func(k int64) int64 { return k }},
			} {
				assertKeyed(t, fmt.Sprintf(c.sql, tbl), c.keyOf)
			}
		})

		// A key over a DECIMAL, a DATE and a STRING expression: the identity
		// is type-blind, and the value it publishes must not be.
		t.Run("KeyOverOtherTypes", func(t *testing.T) {
			for _, c := range []struct {
				name, sql, col        string
				wantRows, wantNonNull int
			}{
				// c_dec nulls every 101st row and c_str every 43rd, so
				// neither has a NULL inside these id windows; the SUBSTR
				// entry spans the whole fixture and PostgreSQL answers two
				// rows, one of them the NULL group.
				{"Decimal", `SELECT (c_dec + 1) AS dk, COUNT(*) AS n FROM %[1]s WHERE id < 20 ` +
					`GROUP BY c_dec + 1 ORDER BY dk`, "dk", 20, 20},
				{"String", `SELECT (c_str || 'x') AS sk, COUNT(*) AS n FROM %[1]s WHERE id < 4 ` +
					`GROUP BY c_str || 'x' ORDER BY sk`, "sk", 4, 4},
				{"Substr", `SELECT (SUBSTR(c_str, 1, 4)) AS p, COUNT(*) AS n FROM %[1]s ` +
					`GROUP BY SUBSTR(c_str, 1, 4) ORDER BY p`, "p", 2, 1},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					sql := fmt.Sprintf(c.sql, tbl)
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, sql)
						if len(res.Rows) != c.wantRows {
							t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
								arm.name, len(res.Rows), c.wantRows, sql)
						}
						nonNull := 0
						for _, r := range res.Rows {
							if r[c.col] != nil {
								nonNull++
							}
						}
						if nonNull != c.wantNonNull {
							t.Errorf("%s arm answered %d non-NULL keys, want %d — the item was "+
								"rebuilt as an expression over columns the aggregate does not "+
								"emit\n  SQL: %s", arm.name, nonNull, c.wantNonNull, sql)
						}
					}
				})
			}
		})

		// #725: a DELIMITED identifier is a NAME, not the expression its
		// characters spell. The DAG shipped it to the worker WITH its quotes
		// (`hash aggregate: GROUP BY key "\"g + 1\"" is not a column of its
		// input`) and named the result column with them too.
		t.Run("DelimitedIdentifierKey", func(t *testing.T) {
			for _, c := range []struct {
				name, sql, col string
				wantRows       int
			}{
				{"Bare", `SELECT "g + 1", COUNT(*) AS n FROM (SELECT g + 1 AS "g + 1", g FROM %[1]s) s ` +
					`GROUP BY "g + 1" ORDER BY 1`, "g + 1", wantKeys + 1},
				{"NoSpaces", `SELECT "g+1", COUNT(*) AS n FROM (SELECT g + 1 AS "g+1", g FROM %[1]s) s ` +
					`GROUP BY "g+1" ORDER BY 1`, "g+1", wantKeys + 1},
				{"Aliased", `SELECT "g + 1" AS gk, COUNT(*) AS n FROM (SELECT g + 1 AS "g + 1", g ` +
					`FROM %[1]s) s GROUP BY "g + 1" ORDER BY gk`, "gk", wantKeys + 1},
				{"Having", `SELECT "g + 1", COUNT(*) AS n FROM (SELECT g + 1 AS "g + 1", g FROM %[1]s) s ` +
					`GROUP BY "g + 1" HAVING "g + 1" > 2 ORDER BY 1`, "g + 1", wantKeys - 2},
				{"OverAnotherTypesColumn", `SELECT c_str AS "g + 1", COUNT(*) AS n FROM %[1]s ` +
					`WHERE id < 5 GROUP BY "g + 1" ORDER BY 1`, "g + 1", 5},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					sql := fmt.Sprintf(c.sql, tbl)
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, sql)
						if len(res.Rows) != c.wantRows {
							t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
								arm.name, len(res.Rows), c.wantRows, sql)
						}
						if !sameNames(res.Columns[:1], []string{c.col}) {
							t.Errorf("%s arm named the first column %q, want %q — a delimited "+
								"identifier is a name and its quotes are not part of it\n  SQL: %s",
								arm.name, res.Columns[0], c.col, sql)
						}
					}
				})
			}
			// The wire-visible half: a set operation takes its result column
			// names from the first arm, and the DAG published `"g + 1"` with
			// the quotes where PostgreSQL says `g + 1`.
			sql := fmt.Sprintf(`WITH a AS (SELECT g + 1 AS "g + 1", COUNT(*) AS n FROM %s `+
				`GROUP BY g + 1) SELECT "g + 1" FROM a UNION ALL SELECT "g + 1" FROM a ORDER BY 1`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if !sameNames(res.Columns, []string{"g + 1"}) {
					t.Errorf("%s arm returned columns %v, want [g + 1]\n  SQL: %s",
						arm.name, res.Columns, sql)
				}
				if len(res.Rows) != 2*(wantKeys+1) {
					t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), 2*(wantKeys+1), sql)
				}
			}
		})

		// The COLLISION matrix. A relation that carries a column SPELLED
		// like the group key is two values wanting one name, and the answer
		// used to depend on which operator resolved it: the single-process
		// pre-aggregate projection APPENDS and resolves by first exact
		// match, so the INPUT column won and the query grouped by it; the
		// worker's projection NARROWS, so the KEY won and shadowed a column
		// an aggregate needed. The key is materialized into a reserved slot
		// now and published by a rename, so neither can see the other
		// (ADR-0026).
		//
		// PostgreSQL 17 answers the same eight rows for every entry as the
		// plain shape — the extra column is not the key and never was.
		t.Run("KeyNameCollidesWithAnInputColumn", func(t *testing.T) {
			// `c_i32 = i*3` with a NULL every 29th row, renamed to a name
			// that reads back as the key's own arithmetic.
			inner := fmt.Sprintf(`(SELECT g, c_i32 AS "g + 1" FROM %s) s`, tbl)
			for _, c := range []struct{ name, sql string }{
				{"Bare", `SELECT g + 1 AS k, COUNT(*) AS n FROM ` + inner +
					` GROUP BY g + 1 ORDER BY k`},
				{"NoSpaces", fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS n FROM `+
					`(SELECT g, c_i32 AS "g+1" FROM %s) s GROUP BY g+1 ORDER BY k`, tbl)},
				{"KeyOnly", `SELECT g + 1 AS k FROM ` + inner + ` GROUP BY g + 1 ORDER BY k`},
				{"Having", `SELECT g + 1 AS k, COUNT(*) AS n FROM ` + inner +
					` GROUP BY g + 1 HAVING g + 1 > 2 ORDER BY k`},
				{"CTE", fmt.Sprintf(`WITH a AS (SELECT g, c_i32 AS "g + 1" FROM %s) `+
					`SELECT g + 1 AS k, COUNT(*) AS n FROM a GROUP BY g + 1 ORDER BY k`, tbl)},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					want := wantKeys + 1
					if c.name == "Having" {
						want = wantKeys - 2
					}
					// BOTH arms now. The DAG used to resolve these names
					// against a scan whose rename Project it flattened and
					// group by the COLUMN; gating the name resolution on the
					// key being a column REFERENCE closed that half too.
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, c.sql)
						if len(res.Rows) != want {
							t.Errorf("%s arm returned %d rows, want %d — the key was resolved "+
								"to the input COLUMN spelled like it\n  SQL: %s",
								arm.name, len(res.Rows), want, c.sql)
						}
					}
				})
			}
			// The other direction: an AGGREGATE whose alias is spelled like
			// the key. PostgreSQL answers the counts; reading the key
			// instead is the same collision one column over.
			//
			// Single-process only: on the DAG a group key and an aggregate
			// output that share a NAME are one column, and the gather answers
			// with the first — pinned as #736 below.
			for _, alias := range []string{`"g + 1"`, `"G + 1"`} {
				sql := fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS %s FROM %s `+
					`GROUP BY g + 1 ORDER BY k`, alias, tbl)
				res := sfcRun(t, sfcArms(ctx, single, coord)[0], sql)
				if len(res.Rows) != wantKeys+1 {
					t.Errorf("single arm returned %d rows, want %d\n  SQL: %s",
						len(res.Rows), wantKeys+1, sql)
					continue
				}
				col := res.Columns[1]
				for _, r := range res.Rows {
					k, kok := numAsInt(r["k"])
					v, vok := numAsInt(r[col])
					if kok && vok && k == v {
						t.Errorf("single arm answered the KEY under the aggregate's alias %s "+
							"(k=%d, %s=%d)\n  SQL: %s", alias, k, col, v, sql)
						break
					}
				}
			}
		})

		// A derived key over a RENAMED column. The DAG emits no stage for a
		// rename Project, so the key reached the worker spelled over a name
		// the scan does not emit and every row landed in ONE NULL group;
		// aggStageDispatchKey re-spells it into source columns for dispatch.
		t.Run("KeyOverARenamedColumn", func(t *testing.T) {
			for _, c := range []struct{ name, sql string }{
				{"DerivedTable", `SELECT a_b + 1 AS k, COUNT(*) AS n FROM ` +
					`(SELECT c_i32 AS a_b, g FROM %[1]s) s WHERE g = 1 GROUP BY a_b + 1 ORDER BY k`},
				{"CTE", `WITH s AS (SELECT c_i32 AS a_b, g FROM %[1]s) ` +
					`SELECT a_b + 1 AS k, COUNT(*) AS n FROM s WHERE g = 1 GROUP BY a_b + 1 ORDER BY k`},
				{"Delimited", `SELECT "a_b" + 1 AS k, COUNT(*) AS n FROM ` +
					`(SELECT c_i32 AS "a_b", g FROM %[1]s) s WHERE g = 1 GROUP BY "a_b" + 1 ORDER BY k`},
				{"DelimitedWithSpace", `SELECT "a b" + 1 AS k, COUNT(*) AS n FROM ` +
					`(SELECT c_i32 AS "a b", g FROM %[1]s) s WHERE g = 1 GROUP BY "a b" + 1 ORDER BY k`},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					sql := fmt.Sprintf(c.sql, tbl)
					// Computed from the generator: c_i32 = id*3, NULL every
					// 29th row, restricted to g = 1 (id % 7 == 1, and g is
					// NULL every 13th row).
					want, nulls := 0, 0
					for _, r := range rows {
						if g, ok := r.g.(int32); !ok || g != 1 {
							continue
						}
						if r.id%29 == 28 {
							nulls++
							continue
						}
						want++
					}
					total := want
					if nulls > 0 {
						total++
					}
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, sql)
						if len(res.Rows) != total {
							t.Errorf("%s arm returned %d rows, want %d — the key was computed "+
								"from a name the scan does not emit\n  SQL: %s",
								arm.name, len(res.Rows), total, sql)
						}
					}
				})
			}
		})

		// The SLOT itself, over a table that STORES a column of that name.
		// A stored name in the reserved namespace is never refused at read,
		// so the planner's slot has to move past it — and the allocator has
		// to exclude the slots it has ALREADY ISSUED as well as the names in
		// scope. Skipping only the second put two derived keys in one
		// column: key 0 stepped off the stored `__gb_expr_0` onto
		// `__gb_expr_1`, key 1 started there and took it, and a twelve-group
		// query answered three with the second key carrying the first one's
		// value.
		//
		// g and h partition the rows DIFFERENTLY (three values against four,
		// twelve non-empty pairs), so a key that lands in the wrong slot is a
		// wrong ROW COUNT and not a coincidence. Every expectation is
		// PostgreSQL 17's.
		t.Run("SlotCollidesWithAStoredColumn", func(t *testing.T) {
			ct := collSlotTable
			for _, c := range []struct {
				name, sql string
				wantRows  int
			}{
				{"TwoDerivedKeys", `SELECT g + 1 AS a, h * 2 AS b, COUNT(*) AS n FROM %[1]s ` +
					`GROUP BY g + 1, h * 2 ORDER BY a, b`, 12},
				{"ReversedKeyOrder", `SELECT g + 1 AS a, h * 2 AS b, COUNT(*) AS n FROM %[1]s ` +
					`GROUP BY h * 2, g + 1 ORDER BY a, b`, 12},
				{"ThreeDerivedKeys", `SELECT g + 1 AS a, h * 2 AS b, id %% 5 AS c, COUNT(*) AS n ` +
					`FROM %[1]s GROUP BY g + 1, h * 2, id %% 5 ORDER BY a, b, c`, 60},
				{"WithHaving", `SELECT g + 1 AS a, h * 2 AS b, COUNT(*) AS n FROM %[1]s ` +
					`GROUP BY g + 1, h * 2 HAVING g + 1 > 1 ORDER BY a, b`, 8},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					sql := fmt.Sprintf(c.sql, ct)
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, sql)
						if len(res.Rows) != c.wantRows {
							t.Errorf("%s arm returned %d rows, want %d — two derived keys "+
								"share one slot\n  SQL: %s", arm.name, len(res.Rows), c.wantRows, sql)
						}
					}
				})
			}
			// The same collision MINTED by the query itself is not allocated
			// around at all — it is refused at the door, which is the
			// stronger guarantee and the one #694's reservation exists to
			// give. A stored column of that name is still admitted and still
			// read (the entries above), because a table holding one can only
			// have been written before the namespace was reserved.
			t.Run("MintedByADerivedAliasIsRefused", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT g + 1 AS a, h * 2 AS b, COUNT(*) AS n FROM `+
					`(SELECT g, h, 1 AS "__gb_expr_0" FROM %s) s GROUP BY g + 1, h * 2 `+
					`ORDER BY a, b`, ct)
				for _, arm := range sfcArms(ctx, single, coord) {
					if _, err := arm.run(sql); err == nil {
						t.Errorf("%s arm ANSWERED a query that mints a reserved slot name; "+
							"the alias door must refuse it\n  SQL: %s", arm.name, sql)
					}
				}
			})
			// TWO SIBLING AGGREGATES in one query, each minting a derived
			// key's slot (#759).
			//
			// The allocator is created PER AGGREGATE — seeded with that
			// aggregate's own input and emitted names — so two sibling
			// aggregates each mint `__gb_expr_0` (here `__gb_expr_2`, past
			// the fixture's two stored ones). That is structurally what #747
			// was for WINDOW slots, and it is METHOD 10's shape exactly: the
			// design claims slots are unique within a query and no fixture
			// attempted it.
			//
			// These entries are the attempt, and they PASS on every arm —
			// the claim holds here for a reason the window family does not
			// have. A group-key slot is a column of its aggregate's OWN
			// INPUT, materialized by the pre-aggregate projection and
			// consumed by that operator; the aggregate publishes under
			// `plansql.GroupKeyName` and the slot never leaves. Two sibling
			// aggregates have disjoint input streams, so one name in both is
			// two different columns that never meet. `__win_N` is the
			// opposite — an OUTPUT column that travels up into the join,
			// which is why the SAME shape collapses there.
			//
			// Every entry names BOTH siblings' aggregate values as well as
			// their keys, and the two are far apart (a count of 80 beside a
			// sum in the thousands), because equal values make a collapse
			// invisible. The day the boundary above stops holding, these
			// fail rather than the class being rediscovered.
			t.Run("SiblingAggregatesEachMintingASlot", func(t *testing.T) {
				const p = `(SELECT g + 1 AS a, COUNT(*) AS n FROM %[1]s GROUP BY g + 1) p`
				const q = `(SELECT h + 1 AS a, SUM(id) AS m FROM %[1]s GROUP BY h + 1) q`
				for _, c := range []struct {
					name, sql string
					rows      int
					// want is the per-row (key, p's count, q's sum) triple,
					// PostgreSQL 17's over the 240 collslot rows.
					want [][3]int64
				}{
					{
						name: "joined",
						sql: `SELECT p.a AS pa, p.n AS pn, q.m AS qm FROM ` + p + ` JOIN ` + q +
							` ON p.a = q.a ORDER BY p.a`,
						rows: 3,
						want: [][3]int64{{1, 80, 7080}, {2, 80, 7140}, {3, 80, 7200}},
					},
					{
						// A sibling NESTED in a sibling, the shape that broke
						// the window family on the single path.
						name: "nested",
						sql: `SELECT p.a AS pa, p.n AS pn, x.m AS qm FROM ` + p +
							` JOIN (SELECT q.a AS a, q.m AS m FROM ` + q + `) x ` +
							`ON p.a = x.a ORDER BY p.a`,
						rows: 3,
						want: [][3]int64{{1, 80, 7080}, {2, 80, 7140}, {3, 80, 7200}},
					},
					{
						// Each sibling minting TWO slots, so the second key
						// of one must not land on the first key of the other.
						name: "two-keys-each",
						sql: `SELECT p.a AS pa, p.n AS pn, q.m AS qm FROM ` +
							`(SELECT g + 1 AS a, h + 2 AS b, COUNT(*) AS n FROM %[1]s ` +
							`GROUP BY g + 1, h + 2) p JOIN ` +
							`(SELECT h + 1 AS a, g + 2 AS b, SUM(id) AS m FROM %[1]s ` +
							`GROUP BY h + 1, g + 2) q ON p.a = q.a AND p.b = q.b ` +
							`ORDER BY p.a, p.b`,
						rows: 9,
						want: nil, // asserted as a row count and a key sequence below
					},
					{
						// The STORED slot columns read as AGGREGATE ARGUMENTS
						// on either side at once: the allocator has to step
						// past both, in both aggregates.
						name: "over-the-stored-slots",
						sql: `SELECT p.a AS pa, p.n AS pn, q.m AS qm FROM ` +
							`(SELECT g + 1 AS a, MAX("__gb_expr_0") AS n FROM %[1]s GROUP BY g + 1) p ` +
							`JOIN (SELECT h + 1 AS a, MAX("__gb_expr_1") AS m FROM %[1]s ` +
							`GROUP BY h + 1) q ON p.a = q.a ORDER BY p.a`,
						rows: 3,
						want: [][3]int64{{1, 1237, 2236}, {2, 1238, 2237}, {3, 1239, 2238}},
					},
					{
						// UNION ALL rather than a join: the two aggregates
						// meet in a set operation instead.
						name: "union-all",
						sql: `SELECT a, n FROM ` + p + ` UNION ALL SELECT a, m FROM ` + q +
							` ORDER BY a, n`,
						rows: 7,
						want: nil,
					},
				} {
					c := c
					t.Run(c.name, func(t *testing.T) {
						sql := fmt.Sprintf(c.sql, ct)
						for _, arm := range sfcArms(ctx, single, coord) {
							res := sfcRun(t, arm, sql)
							if len(res.Rows) != c.rows {
								t.Errorf("%s arm returned %d rows, want %d — two sibling "+
									"aggregates shared a slot\n  SQL: %s",
									arm.name, len(res.Rows), c.rows, sql)
								continue
							}
							for i, w := range c.want {
								r := res.Rows[i]
								ka, _ := numAsInt(r["pa"])
								pn, _ := numAsInt(r["pn"])
								qm, _ := numAsInt(r["qm"])
								if ka != w[0] || pn != w[1] || qm != w[2] {
									t.Errorf("%s arm row %d: (%d, %d, %d), PostgreSQL 17 "+
										"answers (%d, %d, %d)\n  SQL: %s",
										arm.name, i, ka, pn, qm, w[0], w[1], w[2], sql)
								}
							}
						}
					})
				}
			})
			// The other binding: an AGGREGATE over the STORED column. Keys
			// and row count can be right while the argument is bound to the
			// planner's slot, so the VALUE is what this asserts —
			// SUM(__gb_expr_0) over 20 rows of ~1000 is five figures, and
			// SUM of a group key is one or two.
			t.Run("AggregateOverTheStoredColumn", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT g + 1 AS a, h * 2 AS b, SUM(%s) AS s FROM %s `+
					`GROUP BY g + 1, h * 2 ORDER BY a, b`, collSlotCol, ct)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != 12 {
						t.Fatalf("%s arm returned %d rows, want 12\n  SQL: %s",
							arm.name, len(res.Rows), sql)
					}
					for _, r := range res.Rows {
						v, ok := numAsInt(r["s"])
						if !ok || v < 20000 {
							t.Errorf("%s arm answered s=%v; SUM over the stored column is five "+
								"figures, so this is the group key's sum\n  SQL: %s",
								arm.name, r["s"], sql)
							break
						}
					}
				}
				// MAX, where the two are furthest apart: the stored column's
				// maximum is four digits and a group key's is one.
				mx := fmt.Sprintf(`SELECT g + 1 AS a, MAX(%s) AS m FROM %s GROUP BY g + 1 ORDER BY a`,
					collSlotCol, ct)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, mx)
					for _, r := range res.Rows {
						if v, ok := numAsInt(r["m"]); !ok || v < 1000 {
							t.Errorf("%s arm answered m=%v, want the stored column's maximum"+
								"\n  SQL: %s", arm.name, r["m"], mx)
							break
						}
					}
				}
			})
			// The control: the stored column as a PLAIN group key of its own
			// still answers, because a stored name in the reserved namespace
			// is read like any other column.
			t.Run("ctl/TheStoredColumnIsStillGroupable", func(t *testing.T) {
				sfcScalar(t, ctx, single, coord, fmt.Sprintf(
					`SELECT COUNT(*) AS n FROM (SELECT %s AS k, COUNT(*) AS c FROM %s `+
						`WHERE id < 4 GROUP BY %s) s`, collSlotCol, ct, collSlotCol), "n", 4)
			})
		})

		// An ARITHMETIC key beside a DELIMITED COLUMN spelled the same way.
		// The recorded key text cannot tell them apart — a delimited
		// identifier's quotes are not part of its name — so the arithmetic
		// key was resolved as a NAME and bound to the column: PostgreSQL
		// answers five groups of the sum and both DAG arms answered nine
		// groups of the column, silently. The two must answer DIFFERENTLY
		// and each must match PostgreSQL.
		t.Run("ArithmeticKeyBesideADelimitedColumnOfThatText", func(t *testing.T) {
			inner := fmt.Sprintf(`(SELECT a AS g, id AS "g + 1" FROM %s) s`, dbpTable)
			for _, c := range []struct {
				name, sql string
				wantRows  int
			}{
				// The ARITHMETIC: five distinct values of a + 1 over nine
				// rows, one of them the NULL group.
				{"Arithmetic", `SELECT g + 1 AS k, COUNT(*) AS n FROM ` + inner +
					` GROUP BY g + 1 ORDER BY k`, 5},
				{"ArithmeticHaving", `SELECT g + 1 AS k, COUNT(*) AS n FROM ` + inner +
					` GROUP BY g + 1 HAVING g + 1 > 1 ORDER BY k`, 2},
				{"ArithmeticBesideAnAggregateOfTheColumn", `SELECT g + 1 AS k, MAX("g + 1") AS m ` +
					`FROM ` + inner + ` GROUP BY g + 1 ORDER BY k`, 5},
				// The DELIMITED NAME: nine distinct ids, one per row.
				{"DelimitedName", `SELECT "g + 1" AS k, COUNT(*) AS n FROM ` + inner +
					` GROUP BY "g + 1" ORDER BY k`, 9},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, c.sql)
						if len(res.Rows) != c.wantRows {
							t.Errorf("%s arm returned %d rows, want %d — the two spellings must "+
								"answer differently\n  SQL: %s",
								arm.name, len(res.Rows), c.wantRows, c.sql)
						}
					}
				})
			}
		})

		// A DELIMITED ALIAS whose case or spacing survives to the client,
		// under a positional ORDER BY. Two causes met in one message: the
		// alias was lower-cased where the projection emits it while every
		// consumer still asked for the written spelling, and `ORDER BY 1`
		// was rewritten to the alias's TEXT and re-parsed — so `AS "G + 1"`
		// came back as arithmetic over a column `G` that does not exist.
		t.Run("DelimitedAliasUnderPositionalOrderBy", func(t *testing.T) {
			for _, c := range []struct{ name, sql, col string }{
				{"ArithmeticLooking", `SELECT id AS "G + 1" FROM %[1]s ORDER BY 1`, "G + 1"},
				{"MixedCase", `SELECT id AS "Kk" FROM %[1]s ORDER BY 1`, "Kk"},
				{"WithASpace", `SELECT id AS "A B" FROM %[1]s ORDER BY 1`, "A B"},
				{"AKeyword", `SELECT id AS "select" FROM %[1]s ORDER BY 1`, "select"},
				{"Descending", `SELECT id AS "Kk" FROM %[1]s ORDER BY 1 DESC`, "Kk"},
				{"SecondOrdinal", `SELECT id AS "Kk", a FROM %[1]s ORDER BY 2, 1`, "Kk"},
				{"NoOrderBy", `SELECT id AS "G + 1" FROM %[1]s`, "G + 1"},
				{"ByTheAliasItself", `SELECT id AS "Kk" FROM %[1]s ORDER BY "Kk"`, "Kk"},
				{"BesideAGroupBy", `SELECT a AS "Kk", COUNT(*) AS n FROM %[1]s GROUP BY a ORDER BY 1`, "Kk"},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					sql := fmt.Sprintf(c.sql, dbpTable)
					want := 9
					if c.name == "BesideAGroupBy" {
						want = 5
					}
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, sql)
						if len(res.Rows) != want {
							t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
								arm.name, len(res.Rows), want, sql)
						}
						if res.Columns[0] != c.col {
							t.Errorf("%s arm named the column %q, want %q — a delimited alias "+
								"keeps the case it was written in\n  SQL: %s",
								arm.name, res.Columns[0], c.col, sql)
						}
					}
				})
			}
		})

		// GROUPING COVERAGE when the GROUP BY term is a DELIMITED
		// IDENTIFIER. `GROUP BY "g + 1"` groups by ONE COLUMN and says
		// nothing about `g`, so `SELECT g + 1` beside it reads an ungrouped
		// column and PostgreSQL 17 refuses it with 42803. Reading the term's
		// recorded TEXT as an expression made the walk mark `g` grouped, so
		// the query answered — 60 rows with a NULL key on the single-process
		// path and 3 rows on the DAG, one engine disagreeing with the other
		// about a query neither should answer.
		//
		// Both directions are asserted, because a fix that simply refuses
		// more would pass the first half and break the second.
		t.Run("GroupingCoverageUnderADelimitedTerm", func(t *testing.T) {
			gt := gcovTable
			// (i) the delimited term does NOT group `g`, so an expression
			// over `g` is 42803 — with COUNT, with SUM of a third column,
			// with HAVING, and beside a reference to the column itself.
			t.Run("ExpressionOverAnUngroupedColumnIsRefused", func(t *testing.T) {
				for _, sql := range []string{
					`SELECT g + 1 AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY k`,
					`SELECT g + 1 AS k, SUM(h) AS s FROM %[1]s GROUP BY "g + 1" ORDER BY k`,
					`SELECT g + 1 AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" HAVING g + 1 > 1 ORDER BY k`,
					`SELECT "g + 1" AS a, g + 1 AS b, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY a`,
					// (iv) the same trap with no arithmetic in the name at
					// all, so a fix that special-cases operators is visibly
					// not enough.
					`SELECT g + 1 AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g plus 1" ORDER BY k`,
					`SELECT g + 1 AS k, SUM(h) AS s FROM %[1]s GROUP BY "g plus 1" ORDER BY k`,
				} {
					q := fmt.Sprintf(sql, gt)
					for _, arm := range sfcArms(ctx, single, coord) {
						if _, err := arm.run(q); err == nil {
							t.Errorf("%s arm ANSWERED a query PostgreSQL refuses with 42803 - "+
								"a delimited group term does not group the columns its NAME "+
								"happens to spell\n  SQL: %s", arm.name, q)
						}
					}
				}
			})
			// (ii) the delimited term DOES group its own column, so the same
			// query written against that column answers: 60 distinct values,
			// one row each.
			t.Run("TheDelimitedColumnItselfAnswers", func(t *testing.T) {
				for _, sql := range []string{
					`SELECT "g + 1" AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY k`,
					`SELECT "g + 1", COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY 1`,
					`SELECT "g plus 1" AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g plus 1" ORDER BY k`,
				} {
					q := fmt.Sprintf(sql, gt)
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, q)
						if len(res.Rows) != 60 {
							t.Errorf("%s arm returned %d rows, want 60\n  SQL: %s",
								arm.name, len(res.Rows), q)
						}
					}
				}
			})
			// (iii) the mirror: an ARITHMETIC term does not group the column
			// spelled like it, so selecting that column is 42803.
			t.Run("TheDelimitedColumnUnderAnArithmeticTermIsRefused", func(t *testing.T) {
				q := fmt.Sprintf(`SELECT "g + 1" AS k, COUNT(*) AS n FROM %s `+
					`GROUP BY g + 1 ORDER BY k`, gt)
				for _, arm := range sfcArms(ctx, single, coord) {
					if _, err := arm.run(q); err == nil {
						t.Errorf("%s arm ANSWERED a query PostgreSQL refuses with 42803 - the "+
							"arithmetic term does not group the COLUMN spelled like it"+
							"\n  SQL: %s", arm.name, q)
					}
				}
			})
			// The controls. Three that must keep refusing - they were right
			// before and a coarser fix could leave them right by accident -
			// and one that must keep ANSWERING, which is what a fix that
			// simply refused more would break.
			t.Run("ctl/OtherUngroupedColumnsStillRefuse", func(t *testing.T) {
				for _, sql := range []string{
					`SELECT g AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY k`,
					`SELECT h * 2 AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY k`,
					`SELECT j AS k, COUNT(*) AS n FROM %[1]s GROUP BY "g + 1" ORDER BY k`,
				} {
					q := fmt.Sprintf(sql, gt)
					for _, arm := range sfcArms(ctx, single, coord) {
						if _, err := arm.run(q); err == nil {
							t.Errorf("%s arm ANSWERED an ungrouped reference PostgreSQL "+
								"refuses\n  SQL: %s", arm.name, q)
						}
					}
				}
			})
			t.Run("ctl/TheArithmeticKeyStillAnswers", func(t *testing.T) {
				q := fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS n FROM %s `+
					`GROUP BY g + 1 ORDER BY k`, gt)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, q)
					if len(res.Rows) != 3 {
						t.Errorf("%s arm returned %d rows, want 3 - the ordinary shape must "+
							"not be refused by a coverage rule aimed at delimited terms"+
							"\n  SQL: %s", arm.name, len(res.Rows), q)
					}
				}
			})
		})

		// DECIMAL keys, and the two residuals the arc's own typing fix
		// EXPOSED rather than caused. Both are louder than they were — they
		// used to answer NULL or collapse — and both are still wrong.
		t.Run("DecimalKeyOnTheDAG", func(t *testing.T) {
			// The control first: an INTEGER literal in a key over a renamed
			// DECIMAL answers on both paths, which is what says the key's
			// leaves are resolved and typed correctly now.
			t.Run("ctl/IntegerLiteralAnswersOnBothPaths", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT r1 + 1 AS k, COUNT(*) AS n FROM `+
					`(SELECT a AS r1 FROM %s) s GROUP BY r1 + 1 ORDER BY k`, dbpTable)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != 5 {
						t.Errorf("%s arm returned %d rows, want 5\n  SQL: %s",
							arm.name, len(res.Rows), sql)
					}
				}
			})
			// #729's last DAG residual, CLOSED 2026-09-02: the DAG declared
			// FLOAT64 for arithmetic whose value is an exact DECIMAL, so a
			// FRACTIONAL literal in a key over a renamed DECIMAL failed at
			// dispatch where the single-process path answered PostgreSQL's
			// rows. `derivedGroupKeyDecl` types the key in the scope that can
			// NAME its columns now (ADR-0026 §5): `sourceColDeclsThroughRenames`
			// stopped at a computed projection item and answered NOTHING, and
			// the emitted scope's FLOAT rule was accepted because
			// `nodeDeclaredType` answers Decided for arithmetic it cannot type.
			//
			// Asserted digit for digit rather than by row count, because the
			// row count was right while the values were the thing at risk.
			t.Run("FractionalLiteralInTheKey", func(t *testing.T) {
				sql := fmt.Sprintf(
					`SELECT r1 + 0.5 AS k, COUNT(*) AS n FROM (SELECT a AS r1 FROM %s) s `+
						`GROUP BY r1 + 0.5 ORDER BY k`, dbpTable)
				// PostgreSQL 17 over decpair.
				want := []string{"0.49|1", "0.50|1", "2.50|1", "13.25|4", "|2"}
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != len(want) {
						t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), len(want), sql)
					}
					for i, w := range want {
						k, n := res.Rows[i]["k"], res.Rows[i]["n"]
						got := fmt.Sprintf("%v|%v", nz(k), nz(n))
						if got != w {
							t.Errorf("%s arm row %d = %s, PostgreSQL 17 says %s\n  SQL: %s",
								arm.name, i, got, w, sql)
						}
					}
				}
			})
			// An outer aggregate over a renamed-DECIMAL key: pinned as #729
			// before the reserved-slot work landed, and answering on BOTH
			// paths now. Asserted rather than re-pinned — PostgreSQL says
			// 18.74 and both arms say 18.74.
			t.Run("ctl/OuterAggregateOverTheKeyAnswersOnBothPaths", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT SUM(k) AS s FROM (SELECT r1 + 1 AS k, `+
					`COUNT(*) AS c FROM (SELECT a AS r1 FROM %s) s GROUP BY r1 + 1) t`, dbpTable)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != 1 {
						t.Fatalf("%s arm returned %d rows, want 1\n  SQL: %s",
							arm.name, len(res.Rows), sql)
					}
					if got := fmt.Sprintf("%v", res.Rows[0]["s"]); got != "18.74" {
						t.Errorf("%s arm answered s=%q, want \"18.74\"\n  SQL: %s",
							arm.name, got, sql)
					}
				}
			})
			// #749, and this is the pin's own ratchet having fired: the key
			// used to come back at scale 9 for `+ 1` and 8 for `* 2` —
			// correctly ROUNDED values of the wrong type, which is the
			// hardest kind of wrong to notice — and it answers PostgreSQL's
			// digits now. The pin said "assert it and delete the pin"; this
			// is that assertion.
			//
			// An EXACT operator caps its PRECISION at 38 and keeps its SCALE
			// (batch.DecimalResultType). Item 3's reduction spent fraction
			// digits the answer has to buy integer digits it may not need;
			// item 4's 22003 is the honest answer for a value that then does
			// not fit.
			t.Run("WideDecimalKeyKeepsItsScale", func(t *testing.T) {
				// The EXACT strings, not a scale test. Asserting only that
				// the scale IS 10 would pass just as happily on a value that
				// had lost a digit elsewhere, and this is exact fixed-point:
				// every digit is part of the answer.
				//
				// dw = -1339555555706255.5281706149 (scale 10), so
				// PostgreSQL's numeric rule — max of the operand scales for
				// `+`, their sum for `*` — gives these two, digit for digit,
				// on both arms.
				for _, c := range []struct{ expr, want string }{
					{"dw + 1", "-1339555555706254.5281706149"},
					{"dw * 2", "-2679111111412511.0563412298"},
				} {
					sql := fmt.Sprintf(`SELECT %[1]s AS k, COUNT(*) AS n FROM decagg `+
						`WHERE dw IS NOT NULL GROUP BY %[1]s ORDER BY k LIMIT 1`, c.expr)
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, sql)
						if len(res.Rows) != 1 {
							t.Fatalf("%s arm returned %d rows, want 1\n  SQL: %s",
								arm.name, len(res.Rows), sql)
						}
						if got := fmt.Sprintf("%v", res.Rows[0]["k"]); got != c.want {
							t.Errorf("%s arm answered %q, want %q (live PostgreSQL 17)\n  SQL: %s",
								arm.name, got, c.want, sql)
						}
					}
				}
			})
		})

		// The DAG residuals of this family. Each was pre-existing on ff7c3f19
		// and each is #736's mechanism 3: a computed key's RESOLUTION name and
		// its PUBLISHED name were one `Stage.GroupByCols` entry, and these
		// shapes need them to differ.
		//
		// Two of them were answered by a REFUSAL that routed the query onto the
		// coordinator-local single-process pipeline — the engine that carried
		// the two names separately. Since ADR-0026 §2 the STAGE carries both
		// (`Stage.GroupByResolve` beside `Stage.GroupByCols`), the refusal is
		// deleted, and both run ON the DAG: the key an aggregate below already
		// publishes is resolved as the COLUMN it is instead of being re-parsed
		// as arithmetic, and the derived key that shares its published name
		// with an aggregate output is resolved by its hidden slot while the
		// projection above it is pinned to the physical slot its provenance
		// names (#575's mechanism, now on both engines). The third was fixed
		// outright: the argument's delimiters are stripped the way the
		// single-process path has always stripped them.
		t.Run("DAGResolvesAComputedKeyAgainstTheScan", func(t *testing.T) {
			dag := sfcArms(ctx, single, coord)[1]
			// A key an aggregate DIRECTLY BELOW already publishes — the
			// DISTINCT lowering. It used to be ONE NULL group on the DAG.
			// Asserted on the KEYS and not the row count: the COUNT twin
			// answered 1 either way, which is why it is the second entry.
			t.Run("DistinctOverTheKey", func(t *testing.T) {
				sql := fmt.Sprintf(
					`SELECT DISTINCT g + 1 AS k FROM %s GROUP BY g + 1 ORDER BY k`, tbl)
				before := coord.GroupKeyLocalRoutes()
				res := sfcRun(t, dag, sql)
				if len(res.Rows) != wantKeys+1 {
					t.Fatalf("the DAG arm returned %d rows, PostgreSQL 17 answers %d\n  SQL: %s",
						len(res.Rows), wantKeys+1, sql)
				}
				for i := 0; i < wantKeys; i++ {
					got, ok := numAsInt(res.Rows[i]["k"])
					if !ok || got != int64(i+1) {
						t.Fatalf("the DAG arm row %d: k = %v, PostgreSQL 17 answers %d\n  SQL: %s",
							i, res.Rows[i]["k"], i+1, sql)
					}
				}
				if res.Rows[wantKeys]["k"] != nil {
					t.Errorf("the DAG arm put %v where PostgreSQL has the NULL group\n  SQL: %s",
						res.Rows[wantKeys]["k"], sql)
				}
				// The DISPOSITION beside the rows, and it moved with the fix:
				// this shape used to be REFUSED and answered by the local
				// pipeline, and the DAG executes it now. Asserting that the
				// route did NOT fire is what keeps a silent return to routing
				// from passing on rows alone.
				if coord.GroupKeyLocalRoutes() != before {
					t.Errorf("the DAG arm ROUTED this to the coordinator-local pipeline. The "+
						"answer is right either way, which is why the disposition is asserted: "+
						"a key an aggregate DIRECTLY BELOW publishes is resolved as the COLUMN "+
						"it is (Stage.GroupByResolve, Computed=false), and a refusal that starts "+
						"firing on it is a right-to-routed regression in kind\n  SQL: %s", sql)
				}
			})
			t.Run("CountOverDistinctOverTheKey", func(t *testing.T) {
				sfcScalar(t, ctx, single, coord, fmt.Sprintf(
					`SELECT COUNT(*) AS n FROM (SELECT DISTINCT g + 1 AS k FROM %s `+
						`GROUP BY g + 1) s`, tbl), "n", int64(wantKeys+1))
			})
			// An aggregate whose ARGUMENT is spelled like the key. This one is
			// FIXED rather than routed: the DAG carried `agg.InputCol` with its
			// delimiters (`"g + 1"`, quotes included), the alias lookup missed,
			// and the worker asked for a column literally named `"g + 1"` —
			// `column "\"g + 1\"" does not exist`, three attempts, a hard
			// failure for a query PostgreSQL answers.
			t.Run("AggregateArgumentSpelledLikeTheKey", func(t *testing.T) {
				// MAX, whose answer is an exact INT64 on both engines. SUM over
				// the same column is #784 (an integer SUM declares FLOAT64 on
				// every arm), which would make this entry about a different
				// defect.
				sql := fmt.Sprintf(`SELECT g + 1 AS k, MAX("g + 1") AS m FROM `+
					`(SELECT g, c_i32 AS "g + 1" FROM %s) s GROUP BY g + 1 ORDER BY k`, tbl)
				// PostgreSQL 17: the largest c_i32 (= id * 3) in each g + 1
				// group, computed from the fixture rather than transcribed.
				wantMax := map[int64]int64{}
				var nullMax int64
				var haveNull bool
				for _, r := range typematrix.Data(typematrix.Rows) {
					v, ok := r["c_i32"].(int32)
					if !ok {
						continue // c_i32 is NULL every 29th row; MAX skips it
					}
					g, gok := r["g"].(int32)
					if !gok {
						if !haveNull || int64(v) > nullMax {
							nullMax, haveNull = int64(v), true
						}
						continue
					}
					k := int64(g) + 1
					if cur, seen := wantMax[k]; !seen || int64(v) > cur {
						wantMax[k] = int64(v)
					}
				}
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != wantKeys+1 {
						t.Fatalf("%s arm returned %d rows, PostgreSQL 17 answers %d\n  SQL: %s",
							arm.name, len(res.Rows), wantKeys+1, sql)
					}
					for i, r := range res.Rows {
						want, wantK := nullMax, int64(0)
						if i < wantKeys {
							wantK = int64(i + 1)
							want = wantMax[wantK]
							got, ok := numAsInt(r["k"])
							if !ok || got != wantK {
								t.Errorf("%s arm row %d: k = %v, PostgreSQL 17 answers %d\n  SQL: %s",
									arm.name, i, r["k"], wantK, sql)
								continue
							}
						} else if r["k"] != nil {
							t.Errorf("%s arm put %v where PostgreSQL has the NULL group\n  SQL: %s",
								arm.name, r["k"], sql)
						}
						got, ok := numAsInt(r["m"])
						if !ok || got != want {
							t.Errorf("%s arm row %d: m = %v, PostgreSQL 17 answers %d — the "+
								"aggregate's DELIMITED argument did not resolve to its source "+
								"column\n  SQL: %s", arm.name, i, r["m"], want, sql)
						}
					}
				}
			})
			// An aggregate ALIASED like the key: one published name, two
			// columns of the batch, and every by-name lookup answers with the
			// FIRST — the key's. It used to be routed local like the DISTINCT
			// above, and it runs on the DAG now.
			//
			// Not because anything learned to tell the two apart by NAME —
			// nothing can. `absorbAggregateOutputProjection` DECLINES over an
			// aggregate that publishes one name twice, because carrying that
			// projection renames exactly one of the two and every reference
			// spelled before it then means the other. The consumers that CAN
			// tell them apart do it by CLASS and by POSITION: the aggregate
			// emits keys before outputs, the gather's `OutputRename.IsAgg`
			// pairs each rename with the column of its own class (#575), and a
			// merge addresses its aggregates by ordinal (`mergeByPosition`).
			t.Run("AggregateAliasedLikeTheKey", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS "g + 1" FROM %s `+
					`GROUP BY g + 1 ORDER BY k`, tbl)
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != wantKeys+1 {
						t.Fatalf("%s arm returned %d rows, PostgreSQL 17 answers %d\n  SQL: %s",
							arm.name, len(res.Rows), wantKeys+1, sql)
					}
					for i, r := range res.Rows {
						want := nullRows
						if i < wantKeys {
							want = wantN[int64(i+1)]
						}
						got, ok := numAsInt(r["g + 1"])
						if !ok || got != want {
							t.Errorf("%s arm row %d: the aggregate's column = %v, PostgreSQL 17 "+
								"answers the COUNT %d — the KEY's value is under the aggregate's "+
								"alias\n  SQL: %s", arm.name, i, r["g + 1"], want, sql)
						}
					}
				}
			})
			// #785's PLAIN-COLUMN twin, pinned. An aggregate ALIASED like the
			// key is #736's condition (2) — but that condition fires only for a
			// DERIVED key, and here the key is a plain column, so nothing
			// refuses and BOTH engines are wrong the same way.
			//
			// `SELECT COUNT(*) AS g, g AS x … GROUP BY g HAVING COUNT(*) > 0`
			// drops the group `g = 0`: the HAVING's `COUNT(*)` is respelled
			// against what the aggregate publishes and binds the KEY's column
			// (0, 1, 2) instead of the count, so `0 > 0` is false. Every count
			// is 80, so no surviving row looks wrong — only the missing one
			// does, which is why this asserts the whole ordered result.
			//
			// NOT the fifth reader: `aggregateOutputNames` reading
			// `AggScopePreservingWrapper` is what fixed the same collision
			// under a WINDOW, and this shape is unchanged by it. Measured.
			//
			// THE MECHANISM, and why the obvious fix was WITHDRAWN rather than
			// landed (2026-09-02, ADR-0026 §3a — read it before trying again).
			// The Aggregate emits its keys and its aggregate outputs into ONE
			// schema, and `batch.RecordBatch.ColumnIndex` returns the FIRST
			// match, so the HAVING binds the key. Giving the colliding aggregate
			// a HIDDEN SLOT and letting the SELECT-list projection rename it —
			// the nested-aggregate rewrite's existing machinery — fixes the
			// single-process ladder (`> 0 / > 1 / > 79` goes from 2/1/0 rows to
			// PostgreSQL's 3/3/3) and BREAKS the DAG: the projection becomes
			// `[g + 1 AS k, __agg_0 AS "g + 1"]`, whose output name is another
			// item's SOURCE name — a PERMUTATION — and
			// `absorbAggregateOutputProjection` carries the SELECT list onto the
			// producer as a rename MAP, which has no order. Measured: the
			// `HAVING g + 1 > 2` control, right on all four arms today, came back
			// with the COUNT under `k` and NULL under `g + 1`.
			// `aggregatePublishesADuplicateName` is what declines that projection
			// today, and giving the aggregate a slot is exactly what stops it
			// firing. So the fix is either "do not absorb a projection whose
			// outputs permute its inputs" or "the GROUP KEY takes the slot" —
			// §2's two names in the other direction.
			//
			// A duplicate OUTPUT name is legal SQL and must stay accepted:
			// PostgreSQL answers `SELECT COUNT(*) AS g, g AS x`. What must not
			// stand is first-match deciding which column it meant.
			//
			// TODO(#785): delete when the HAVING binds the aggregate.
			t.Run("pin785-AnAggregateAliasedLikeAPlainKeyBesideAHaving", func(t *testing.T) {
				const pinned = `SELECT COUNT(*) AS g, g AS x FROM collslot ` +
					`GROUP BY g HAVING COUNT(*) > 0 ORDER BY x`
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, pinned)
					if len(res.Rows) == 3 {
						t.Fatalf("the %s arm now answers three groups — PostgreSQL's. #785 is "+
							"fixed for this shape; assert it and delete this pin\n  SQL: %s",
							arm.name, pinned)
					}
					if len(res.Rows) != 2 {
						t.Errorf("the %s arm answered %d rows; this pin records 2 and PostgreSQL "+
							"answers 3. The answer MOVED without becoming right, which is a "+
							"change #785 has to account for\n  SQL: %s",
							arm.name, len(res.Rows), pinned)
						continue
					}
					// The two that survive are x = 1 and x = 2; the DROPPED one
					// is x = 0, and naming it is what makes the pin readable.
					for i, want := range []int64{1, 2} {
						if got, ok := numAsInt(res.Rows[i]["x"]); !ok || got != want {
							t.Errorf("the %s arm row %d: x = %v, this pin records %d\n  SQL: %s",
								arm.name, i, res.Rows[i]["x"], want, pinned)
						}
					}
				}
				// The three controls that make the cell exactly this one. Each
				// is correct on both arms and must stay so.
				for _, c := range []struct{ name, sql string }{
					{"ctl/no-having", `SELECT COUNT(*) AS g, g AS x FROM collslot ` +
						`GROUP BY g ORDER BY x`},
					{"ctl/no-alias-collision", `SELECT COUNT(*) AS c, g AS x FROM collslot ` +
						`GROUP BY g HAVING COUNT(*) > 0 ORDER BY x`},
					{"ctl/collision-without-having-under-a-window",
						`SELECT COUNT(*) AS g, g AS x, ROW_NUMBER() OVER (ORDER BY g) AS rn ` +
							`FROM collslot GROUP BY g ORDER BY x`},
				} {
					c := c
					t.Run(c.name, func(t *testing.T) {
						for _, arm := range sfcArms(ctx, single, coord) {
							res := sfcRun(t, arm, c.sql)
							if len(res.Rows) != 3 {
								t.Errorf("%s arm returned %d rows, PostgreSQL 17 answers 3\n  SQL: %s",
									arm.name, len(res.Rows), c.sql)
								continue
							}
							for i, want := range []int64{0, 1, 2} {
								if got, ok := numAsInt(res.Rows[i]["x"]); !ok || got != want {
									t.Errorf("%s arm row %d: x = %v, PostgreSQL 17 answers %d\n  SQL: %s",
										arm.name, i, res.Rows[i]["x"], want, c.sql)
								}
							}
							// And the aggregate's own column, which is the one
							// the collision moves: every group has 80 rows.
							for i := range res.Rows {
								col := "g"
								if _, ok := res.Rows[i]["c"]; ok {
									col = "c"
								}
								if got, ok := numAsInt(res.Rows[i][col]); !ok || got != 80 {
									t.Errorf("%s arm row %d: %s = %v, PostgreSQL 17 answers 80 — "+
										"the KEY's value is under the aggregate's alias\n  SQL: %s",
										arm.name, i, col, res.Rows[i][col], c.sql)
								}
							}
						}
					})
				}
			})

			// The CONTROL the refusal must not swallow: an ordinary computed
			// key needs no second name and stays on the DAG. Without this, a
			// refusal widened by accident — every grouped query routed local —
			// would pass every assertion above.
			t.Run("ctl/an-ordinary-computed-key-still-runs-on-the-dag", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS n FROM %s `+
					`GROUP BY g + 1 ORDER BY k`, tbl)
				before := coord.GroupKeyLocalRoutes()
				res := sfcRun(t, dag, sql)
				if len(res.Rows) != wantKeys+1 {
					t.Errorf("the DAG arm returned %d rows, PostgreSQL 17 answers %d\n  SQL: %s",
						len(res.Rows), wantKeys+1, sql)
				}
				if coord.GroupKeyLocalRoutes() != before {
					t.Errorf("the DAG arm routed an ordinary computed key to the local "+
						"pipeline — refuseUnstageableGroupKey is firing on shapes the DAG "+
						"executes correctly\n  SQL: %s", sql)
				}
			})
		})

		// A WINDOW above the aggregate (#737).
		//
		// A window APPENDS its outputs to its input and renames nothing, so
		// every column the aggregate published is still there under its own
		// name — and the two walks that re-point a SELECT item at its key's
		// column both stopped at a NodeWindow anyway, so `g + 1` was compiled
		// as ARITHMETIC over a `g` the aggregate does not emit and the key was
		// NULL on every row, on all five arms.
		//
		// The window's OWN spec is the second half and needs the same rule:
		// its ORDER BY / PARTITION BY key and its argument are evaluated over
		// the aggregate's OUTPUT, where a computed key is a NAME and an
		// aggregate call is the name of the column that computes it. Left as
		// text they named nothing and the window ordered by NULL — the right
		// rows in an arbitrary sequence, which is why every entry here asserts
		// the SEQUENCE and not only the key set.
		//
		// PostgreSQL 17 answers keys 1..7 then the NULL group, with
		// ROW_NUMBER 1..8 in that order.
		for _, tc := range []struct {
			name, sql string
			// rn is the window column's value for the i-th row (0-based) of
			// PostgreSQL's ordering; nil means "not asserted".
			rn func(i int) int64
			// bare marks the entry whose key is `g` itself rather than
			// `g + 1`, so its published values are 0..6.
			bare bool
		}{
			{
				name: "WindowAboveTheAggregate",
				sql: fmt.Sprintf(`SELECT g + 1 AS k, ROW_NUMBER() OVER (ORDER BY g + 1) AS rn `+
					`FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
				rn: func(i int) int64 { return int64(i + 1) },
			},
			{
				// ORDER BY the window's own output, so a key that orders
				// wrongly reorders the whole answer rather than one column.
				name: "WindowAboveTheAggregateOrderedByTheWindow",
				sql: fmt.Sprintf(`SELECT g + 1 AS k, ROW_NUMBER() OVER (ORDER BY g + 1) AS rn `+
					`FROM %s GROUP BY g + 1 ORDER BY rn`, tbl),
				rn: func(i int) int64 { return int64(i + 1) },
			},
			{
				// PARTITION BY the key: a key resolved to nothing puts every
				// row in ONE partition, so each row's ROW_NUMBER is 1 here
				// and 1..8 there.
				name: "WindowPartitionedByTheKey",
				sql: fmt.Sprintf(`SELECT g + 1 AS k, ROW_NUMBER() OVER (PARTITION BY g + 1) AS rn `+
					`FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
				rn: func(int) int64 { return 1 },
			},
			{
				// An expression OVER the key beside the window.
				name: "WindowBesideAnExpressionOverTheKey",
				sql: fmt.Sprintf(`SELECT g + 1 AS k, ROW_NUMBER() OVER (ORDER BY g + 1) AS rn `+
					`FROM %s GROUP BY g + 1 HAVING COUNT(*) > 0 ORDER BY k`, tbl),
				rn: func(i int) int64 { return int64(i + 1) },
			},
			{
				// A BARE column key, which needs no materialization at all
				// and must keep answering.
				name: "WindowAboveABareKey",
				sql: fmt.Sprintf(`SELECT g AS k, ROW_NUMBER() OVER (ORDER BY g) AS rn `+
					`FROM %s GROUP BY g ORDER BY k`, tbl),
				rn:   func(i int) int64 { return int64(i + 1) },
				bare: true,
			},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				// A bare key publishes g itself, so its values are 0..6; the
				// derived key publishes g + 1.
				bare := tc.bare
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, tc.sql)
					if len(res.Rows) != wantKeys+1 {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), wantKeys+1, tc.sql)
						continue
					}
					for i, r := range res.Rows {
						if i == len(res.Rows)-1 {
							if r["k"] != nil {
								t.Errorf("%s arm put %v where PostgreSQL has the NULL group\n  SQL: %s",
									arm.name, r["k"], tc.sql)
							}
						} else {
							want := int64(i + 1)
							if bare {
								want = int64(i)
							}
							got, ok := numAsInt(r["k"])
							if !ok || got != want {
								t.Errorf("%s arm row %d: k = %v, PostgreSQL 17 answers %d — a "+
									"window above the aggregate lost the computed key\n  SQL: %s",
									arm.name, i, r["k"], want, tc.sql)
								break
							}
						}
						if tc.rn == nil {
							continue
						}
						got, ok := numAsInt(r["rn"])
						if !ok || got != tc.rn(i) {
							t.Errorf("%s arm row %d: rn = %v, PostgreSQL 17 answers %d — the "+
								"window's own key was evaluated over a schema the aggregate "+
								"does not emit\n  SQL: %s", arm.name, i, r["rn"], tc.rn(i), tc.sql)
							break
						}
					}
				}
			})
		}

		// DISTINCT above a computed key with a WINDOW between them, which is
		// the THIRD walk asking §4's question (#737 round 2).
		//
		// `groupKeysPublishedBelow` decides whether an aggregate's key is
		// already published by an aggregate DIRECTLY BELOW it — the
		// `SELECT DISTINCT g + 1 … GROUP BY g + 1` lowering, two aggregates
		// keyed alike — and it read its own hardcoded Filter/Sort/Limit list
		// while §4 said "both walks read one list, so they cannot disagree".
		// There were THREE. With a window between the two aggregates the
		// outer one did not see the inner's key, materialized it again over a
		// schema with no `g`, and collapsed the table into ONE NULL group.
		//
		// The DAG arms were PINNED at one NULL group for #736's own mechanism
		// (`Stage.GroupByCols` is both the resolution and the published name),
		// which is a different site from the walk. They now answer, through the
		// refusal that routes the shape onto the coordinator-local pipeline —
		// so the pins are gone and every arm is asserted on the KEYS.
		t.Run("DistinctOverAComputedKeyUnderAWindow", func(t *testing.T) {
			for _, c := range []struct {
				name, sql string
				rows      int // PostgreSQL 17
			}{
				{
					name: "distinct-with-a-window",
					sql: fmt.Sprintf(`SELECT DISTINCT g + 1 AS k, `+
						`ROW_NUMBER() OVER (ORDER BY g + 1) AS rn FROM %s GROUP BY g + 1 `+
						`ORDER BY k`, tbl),
					rows: wantKeys + 1,
				},
				{
					name: "the-key-alone-through-a-derived-table",
					sql: fmt.Sprintf(`SELECT k FROM (SELECT DISTINCT g + 1 AS k, `+
						`ROW_NUMBER() OVER (ORDER BY g + 1) AS rn FROM %s GROUP BY g + 1) s `+
						`ORDER BY k`, tbl),
					rows: wantKeys + 1,
				},
				{
					// The COUNT twin, which was already right on every arm —
					// a row count cannot see a NULL key, and that is exactly
					// why the two entries above assert the KEYS.
					name: "ctl/count-over-the-same-shape",
					sql: fmt.Sprintf(`SELECT COUNT(*) AS n FROM (SELECT DISTINCT g + 1 AS k, `+
						`ROW_NUMBER() OVER (ORDER BY g + 1) AS rn FROM %s GROUP BY g + 1) s`, tbl),
					rows: 1,
				},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, c.sql)
						if len(res.Rows) != c.rows {
							t.Errorf("%s arm returned %d rows, PostgreSQL 17 answers %d\n  SQL: %s",
								arm.name, len(res.Rows), c.rows, c.sql)
							continue
						}
						if _, isKeyed := res.Rows[0]["k"]; !isKeyed {
							continue // the COUNT control
						}
						nonNull := 0
						for _, r := range res.Rows {
							if r["k"] != nil {
								nonNull++
							}
						}
						if nonNull != wantKeys {
							t.Errorf("%s arm answered %d non-NULL keys, PostgreSQL 17 answers "+
								"%d — a DISTINCT over a computed key re-materialized it above "+
								"the aggregate that already published it\n  SQL: %s",
								arm.name, nonNull, wantKeys, c.sql)
						}
					}
				})
			}
		})

		// A WHERE on the key, ABOVE the window (#774).
		//
		// It used to admit no row at all where PostgreSQL answers four, on
		// every arm: the outer predicate is pushed below the derived table's
		// Project and `k` substituted away to `(g + 1)`, which above the
		// aggregate is the NAME of one column rather than arithmetic, so the
		// filter was UNKNOWN on every row and a filter admits only TRUE.
		//
		// The site was a FOURTH walk asking ADR-0026 §4's question with its own
		// Filter-only list — `readsAnAggregate` through
		// `logical.AggregateBelowProject`. It now reads
		// `logical.AggScopePreservingWrapper`, which has the WINDOW on it, so
		// the substitution is declined and the predicate stays where the query
		// wrote it. `TestAggScopePreservingWrapperIsReadByEveryWalk` drives it
		// as the fourth named reader.
		//
		// Every entry asserts the ORDERED keys, not a row count: the failure
		// mode this replaces produced the empty set, which a count sees, but
		// the neighbouring one (#737) produced the right count with NULL keys,
		// which it does not.
		t.Run("WhereOnTheKeyAboveAWindow", func(t *testing.T) {
			const win = `ROW_NUMBER() OVER (ORDER BY g + 1) AS rn`
			// PostgreSQL 17 over typemx: keys 4..7 with ROW_NUMBER 4..7.
			for _, c := range []struct {
				name, sql string
				keys      []int64
			}{
				{"the-repro", fmt.Sprintf(`SELECT k, rn FROM (SELECT g + 1 AS k, %s `+
					`FROM %s GROUP BY g + 1) s WHERE k > 3 ORDER BY k`, win, tbl),
					[]int64{4, 5, 6, 7}},
				{"two-keys", fmt.Sprintf(`SELECT k, k2, rn FROM (SELECT g + 1 AS k, `+
					`g + 2 AS k2, %s FROM %s GROUP BY g + 1, g + 2) s WHERE k > 3 ORDER BY k`,
					win, tbl), []int64{4, 5, 6, 7}},
				{"filter-on-the-key-AND-the-window", fmt.Sprintf(`SELECT k, rn FROM `+
					`(SELECT g + 1 AS k, %s FROM %s GROUP BY g + 1) s WHERE k > 3 AND rn > 2 `+
					`ORDER BY k`, win, tbl), []int64{4, 5, 6, 7}},
				{"beside-a-having", fmt.Sprintf(`SELECT k, rn FROM (SELECT g + 1 AS k, %s `+
					`FROM %s GROUP BY g + 1 HAVING COUNT(*) > 100) s WHERE k > 3 ORDER BY k DESC`,
					win, tbl), []int64{7, 6, 5, 4}},
				{"an-expression-over-the-key", fmt.Sprintf(`SELECT k, rn FROM `+
					`(SELECT g + 1 AS k, %s FROM %s GROUP BY g + 1) s WHERE k * 2 > 6 ORDER BY k`,
					win, tbl), []int64{4, 5, 6, 7}},
				{"is-not-null-drops-only-the-null-group", fmt.Sprintf(`SELECT k, rn FROM `+
					`(SELECT g + 1 AS k, %s FROM %s GROUP BY g + 1) s WHERE k IS NOT NULL `+
					`ORDER BY k`, win, tbl), []int64{1, 2, 3, 4, 5, 6, 7}},
				{"partitioned-window", fmt.Sprintf(`SELECT k FROM (SELECT g + 1 AS k, `+
					`ROW_NUMBER() OVER (PARTITION BY g + 1) AS rn FROM %s GROUP BY g + 1) s `+
					`WHERE k > 3 ORDER BY k`, tbl), []int64{4, 5, 6, 7}},
				{"through-a-cte", fmt.Sprintf(`WITH s AS (SELECT g + 1 AS k, %s FROM %s `+
					`GROUP BY g + 1) SELECT k, rn FROM s WHERE k > 3 ORDER BY k`, win, tbl),
					[]int64{4, 5, 6, 7}},
				{"through-two-derived-levels", fmt.Sprintf(`SELECT k, rn FROM (SELECT k, rn `+
					`FROM (SELECT g + 1 AS k, %s FROM %s GROUP BY g + 1) t) s WHERE k > 3 `+
					`ORDER BY k`, win, tbl), []int64{4, 5, 6, 7}},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, c.sql)
						if len(res.Rows) != len(c.keys) {
							t.Errorf("%s arm returned %d rows, PostgreSQL 17 answers %d — a "+
								"filter on a computed key above a WINDOW was substituted into "+
								"arithmetic over a column the aggregate does not emit\n  SQL: %s",
								arm.name, len(res.Rows), len(c.keys), c.sql)
							continue
						}
						for i, want := range c.keys {
							got, ok := numAsInt(res.Rows[i]["k"])
							if !ok || got != want {
								t.Errorf("%s arm row %d: k = %v, PostgreSQL 17 answers %d\n  SQL: %s",
									arm.name, i, res.Rows[i]["k"], want, c.sql)
								break
							}
						}
					}
				})
			}
			// The AGGREGATE face of the same cell. No row COUNT can see this
			// one — it is one row either way — so it is asserted on the VALUE:
			// SUM used to be NULL over the empty set where PostgreSQL answers
			// 22 (4+5+6+7).
			t.Run("sum-over-the-filtered-key", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT SUM(k) AS s FROM (SELECT g + 1 AS k, %s `+
					`FROM %s GROUP BY g + 1) s WHERE k > 3`, win, tbl)
				sfcScalar(t, ctx, single, coord, sql, "s", 22)
			})
			t.Run("count-over-the-filtered-key", func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT COUNT(*) AS n FROM (SELECT g + 1 AS k, %s `+
					`FROM %s GROUP BY g + 1) s WHERE k > 3`, win, tbl)
				sfcScalar(t, ctx, single, coord, sql, "n", 4)
			})
			// The four CONTROLS that made the cell exactly this one, and that
			// have to keep answering: the same filter with no window, the same
			// query with no filter, a BARE key instead of a computed one, and
			// the filter on the WINDOW's output instead of the key. The last
			// two are the ones a too-wide decline would break — the predicate
			// stays above the Project there, and that must not become "the
			// predicate is never pushed".
			for _, c := range []struct {
				name, sql string
				rows      int
			}{
				{"ctl/no-window", fmt.Sprintf(`SELECT k FROM (SELECT g + 1 AS k, `+
					`COUNT(*) AS c FROM %s GROUP BY g + 1) s WHERE k > 3 ORDER BY k`, tbl), 4},
				{"ctl/no-filter", fmt.Sprintf(`SELECT k, rn FROM (SELECT g + 1 AS k, %s `+
					`FROM %s GROUP BY g + 1) s ORDER BY k`, win, tbl), wantKeys + 1},
				{"ctl/bare-key", fmt.Sprintf(`SELECT k, rn FROM (SELECT g AS k, `+
					`ROW_NUMBER() OVER (ORDER BY g) AS rn FROM %s GROUP BY g) s `+
					`WHERE k > 3 ORDER BY k`, tbl), 3},
				{"ctl/filter-on-the-window", fmt.Sprintf(`SELECT s.k, s.rn FROM `+
					`(SELECT g + 1 AS k, %s FROM %s GROUP BY g + 1) s WHERE s.rn > 3 `+
					`ORDER BY s.k`, win, tbl), 5},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, c.sql)
						if len(res.Rows) != c.rows {
							t.Errorf("%s arm returned %d rows, PostgreSQL 17 answers %d\n  SQL: %s",
								arm.name, len(res.Rows), c.rows, c.sql)
						}
					}
				})
			}
		})

		// The window's ORDER BY is an AGGREGATE. RANK rather than ROW_NUMBER
		// on purpose: the counts TIE — six groups share 659 or 660 — and
		// PostgreSQL does not specify which peer a ROW_NUMBER gives which
		// number to, so a ROW_NUMBER entry here would assert an answer the
		// server is free to change (ADR-0013's legal nondeterminism). RANK is
		// tie-immune and still sees the whole defect: with the key naming
		// nothing the ordering was NULL on every row and every rank was 1.
		t.Run("WindowOrderedByAnAggregate", func(t *testing.T) {
			// PostgreSQL's rank: one more than the number of groups whose
			// count is strictly smaller, computed from the fixture.
			counts := make([]int64, 0, len(wantN)+1)
			for _, n := range wantN {
				counts = append(counts, n)
			}
			counts = append(counts, nullRows)
			rankOf := func(n int64) int64 {
				var less int64
				for _, c := range counts {
					if c < n {
						less++
					}
				}
				return less + 1
			}
			// The second entry wraps the aggregate in a larger term, which is
			// the shape the RENDERING has to survive: the respell replaces one
			// SUBTERM and the rest of the expression is re-rendered around it.
			for _, sql := range []string{
				fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS n, `+
					`RANK() OVER (ORDER BY COUNT(*)) AS rk FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
				fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS n, `+
					`RANK() OVER (ORDER BY COUNT(*) + 1) AS rk FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
			} {
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != wantKeys+1 {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), wantKeys+1, sql)
						continue
					}
					for i, r := range res.Rows {
						n, _ := numAsInt(r["n"])
						got, ok := numAsInt(r["rk"])
						if want := rankOf(n); !ok || got != want {
							t.Errorf("%s arm row %d: rk = %v for a group of %d, PostgreSQL 17 "+
								"answers %d — the window ordered by a name the aggregate does "+
								"not publish\n  SQL: %s", arm.name, i, r["rk"], n, want, sql)
							break
						}
					}
				}
			}
		})

		// The KEY nested inside a larger window term. The respell renders the
		// key as a DELIMITED identifier, and here it has to survive being one
		// operand of an expression the operator then MATERIALIZES — `"g + 1" *
		// 2` — rather than being the whole term, which is the only shape the
		// entries above exercise.
		t.Run("WindowTermWrappingTheKey", func(t *testing.T) {
			for _, c := range []struct {
				name, sql string
				rk        func(i int) int64
			}{
				{
					// RANK over `(g + 1) * 2` is the key's own order: 1..7
					// then the NULL group last.
					name: "order-by-an-expression-over-the-key",
					sql: fmt.Sprintf(`SELECT g + 1 AS k, RANK() OVER (ORDER BY (g + 1) * 2) AS rk `+
						`FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
					rk: func(i int) int64 { return int64(i + 1) },
				},
				{
					// PARTITION BY an expression over the key: the keys split
					// by parity, so the rank restarts. 1,3,5,7 are odd →
					// ranks 1..4; 2,4,6 even → 1..3; and the NULL key is a
					// partition of its own → 1. A key resolving to nothing
					// puts every row in ONE partition and ranks them 1..8,
					// which is what this entry can see and the ORDER BY one
					// cannot.
					name: "partition-by-an-expression-over-the-key",
					sql: fmt.Sprintf(`SELECT g + 1 AS k, `+
						`RANK() OVER (PARTITION BY (g + 1) %% 2 ORDER BY g + 1) AS rk `+
						`FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
					rk: func(i int) int64 {
						if i == wantKeys {
							return 1 // the NULL key, alone in its partition
						}
						return int64(i/2 + 1)
					},
				},
			} {
				c := c
				t.Run(c.name, func(t *testing.T) {
					for _, arm := range sfcArms(ctx, single, coord) {
						res := sfcRun(t, arm, c.sql)
						if len(res.Rows) != wantKeys+1 {
							t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
								arm.name, len(res.Rows), wantKeys+1, c.sql)
							continue
						}
						for i, r := range res.Rows {
							got, ok := numAsInt(r["rk"])
							if want := c.rk(i); !ok || got != want {
								t.Errorf("%s arm row %d: rk = %v, PostgreSQL 17 answers %d — "+
									"the key's delimited spelling did not survive being one "+
									"operand of a larger term\n  SQL: %s",
									arm.name, i, r["rk"], want, c.sql)
								break
							}
						}
					}
				})
			}
		})

		// The window's ARGUMENT is an AGGREGATE, which the aggregate below
		// publishes under its own output name. It used to reach the operator
		// as the text `count(*)`, which names nothing there.
		t.Run("WindowOverAnAggregateOutput", func(t *testing.T) {
			var total int64
			for _, n := range wantN {
				total += n
			}
			total += nullRows
			for _, sql := range []string{
				fmt.Sprintf(`SELECT g + 1 AS k, COUNT(*) AS n, SUM(COUNT(*)) OVER () AS s `+
					`FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
				// The same with the aggregate NOT in the SELECT list, so it
				// has to be hoisted rather than reused.
				fmt.Sprintf(`SELECT g + 1 AS k, SUM(COUNT(*)) OVER () AS s `+
					`FROM %s GROUP BY g + 1 ORDER BY k`, tbl),
			} {
				for _, arm := range sfcArms(ctx, single, coord) {
					res := sfcRun(t, arm, sql)
					if len(res.Rows) != wantKeys+1 {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm.name, len(res.Rows), wantKeys+1, sql)
						continue
					}
					for i, r := range res.Rows {
						got, ok := numAsInt(r["s"])
						if !ok || got != total {
							t.Errorf("%s arm row %d: s = %v, PostgreSQL 17 answers %d — a window "+
								"over an aggregate's output read a name the aggregate does not "+
								"publish\n  SQL: %s", arm.name, i, r["s"], total, sql)
							break
						}
					}
				}
			}
		})

		// The two spellings this pin recorded as 42803 — a table QUALIFIER on
		// the select item, and a CAST type SYNONYM — are ANSWERED now (#738,
		// 2026-09-03), and asserted with their rows by
		// TestTheIdentityErasesAQualifierAndATypeSynonym. The pin is gone
		// because it failed on the change, which is what it was for.

		// GROUP BY a name that is BOTH a select alias and an input column.
		// PostgreSQL resolves it to the input COLUMN and then refuses the
		// query, because the select list's own `c_i32` is then ungrouped.
		//
		// This was pinned as `GroupByAliasShadowsAnInputColumn` — 20 rows on
		// both arms — until #739 moved the precedence to the scope layer. It
		// is a GATE now: the refusal is the answer, and it is asserted rather
		// than recorded.
		t.Run("GroupByAliasLosesToAnInputColumn", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT c_i32 AS g, COUNT(*) AS n FROM %s WHERE id < 20 `+
				`GROUP BY g ORDER BY g`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res, err := arm.run(sql)
				if err == nil {
					t.Fatalf("%s arm answered %d rows; PostgreSQL 17 raises 42803 here, "+
						"because GROUP BY g binds to the INPUT column and c_i32 is then "+
						"ungrouped (#739)\n  SQL: %s", arm.name, len(res.Rows), sql)
				}
				if !strings.Contains(err.Error(), "must appear in the GROUP BY clause") {
					t.Errorf("%s arm: %v\n  want the 42803 grouping refusal\n  SQL: %s",
						arm.name, err, sql)
				}
			}
		})

		// A delimited key containing a DOT is published under its part after
		// the dot. The VALUES agree with PostgreSQL on every arm; the column
		// NAME does not (`b` where PostgreSQL says `a.b`).
		//
		// TODO(#740): delete this pin when the whole name survives.
		t.Run("DelimitedKeyWithADotLosesItsQualifier", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT "a.b", COUNT(*) AS n FROM `+
				`(SELECT c_i32 AS "a.b" FROM %s WHERE id < 5) s GROUP BY "a.b" ORDER BY 1`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != 5 {
					t.Errorf("%s arm returned %d rows, want 5\n  SQL: %s",
						arm.name, len(res.Rows), sql)
				}
				if res.Columns[0] == "a.b" {
					t.Fatalf("%s arm now publishes the whole delimited name, which is "+
						"PostgreSQL's answer. #740 is fixed — assert it and delete this "+
						"pin\n  SQL: %s", arm.name, sql)
				}
			}
		})

		// An unquoted identifier FOLDS at the lexer, so a GROUP BY term
		// written in a different case from the column is the same term
		// (#731). This was the pin that recorded one NULL group over the whole
		// 5000-row table; PostgreSQL answers one row per key, and so does
		// every arm now. The DELIMITED spelling beside it is the boundary:
		// `"G"` names no column of this table and is 42703, which is
		// PostgreSQL's answer too.
		t.Run("IdentifierCaseInTheGroupByTerm", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT g + 1 AS gk, COUNT(*) AS n FROM %s GROUP BY G + 1 ORDER BY gk`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != wantKeys+1 {
					t.Fatalf("%s arm returned %d rows, want %d (PostgreSQL's answer)\n  SQL: %s",
						arm.name, len(res.Rows), wantKeys+1, sql)
				}
			}
		})

		t.Run("IdentifierCaseInTheSelectListAndTheFilter", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT ID, G FROM %s WHERE ID < 3 ORDER BY ID`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res := sfcRun(t, arm, sql)
				if len(res.Rows) != 3 {
					t.Fatalf("%s arm returned %d rows, want 3\n  SQL: %s", arm.name, len(res.Rows), sql)
				}
				if got := res.Columns; len(got) != 2 || got[0] != "id" || got[1] != "g" {
					t.Fatalf("%s arm published %v, want [id g] — PostgreSQL publishes the FOLDED "+
						"spelling of an unquoted reference\n  SQL: %s", arm.name, got, sql)
				}
				for _, r := range res.Rows {
					if r["id"] == nil || r["g"] == nil {
						t.Fatalf("%s arm answered NULL for a resolvable column: %v\n  SQL: %s",
							arm.name, r, sql)
					}
				}
			}
		})

		t.Run("ADelimitedIdentifierIsByteExact", func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT "G" FROM %s WHERE id < 3`, tbl)
			for _, arm := range sfcArms(ctx, single, coord) {
				res, err := arm.run(sql)
				if err == nil {
					t.Fatalf("%s arm answered %d rows for a delimited name no column carries; "+
						"PostgreSQL is 42703\n  SQL: %s", arm.name, len(res.Rows), sql)
				}
				if got := sqlerr.StateOf(err); got != "42703" {
					t.Fatalf("%s arm: SQLSTATE %q, want 42703 (err: %v)\n  SQL: %s",
						arm.name, got, err, sql)
				}
			}
		})
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

// nz renders a result value the way this file's assertions read it: a NULL is
// the empty string, so a row whose key is NULL is distinguishable from one
// whose key is the text "<nil>".
func nz(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
