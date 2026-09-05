package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A PREDICATE over exact arithmetic whose operand is a CHOICE, a CAST, a
// scalar function or a negation, on four arms (#849 round-3 residual, #555).
//
// `expr.BinOp` — the generic arithmetic node every operand with no typed
// protocol compiles to — boxes an exact fixed-point result as a DECIMAL
// COLUMN's value is boxed, its rendered TEXT. `expr.classifyOperand` had an
// arm for its TYPED sibling and none for it, so a comparison above such a node
// fell through the declaration-driven rules to `compare()`, which orders two
// strings by BYTES: `"1.00" > "1"` is TRUE as bytes and FALSE as a number.
//
// The headline shape is `TestCTEComputedColumnAboveAJoinChainThreeArms`'s
// `coalesce/derived/2join`, which answered 8 where PostgreSQL answers 5. It is
// one cell of a class: every producer below reaches the same node, and the
// four contexts (a bare predicate, a derived table, a CTE, and a derived table
// above a two-join chain) are here because the CTE spelling answered
// CORRECTLY on the single arm — it materializes the value into a DECIMAL
// column, and a column comparison never took the boxed path — while both DAG
// arms inlined it and did not. A gate that measured one context would have
// called the defect a DAG defect.
//
// Every expected count is live PostgreSQL 17.11's over the decpair rows
// loaded into a `--locale=C` database, measured on the oracle server before
// the fix was written. PostgreSQL answers the same number in all four
// contexts, which is itself the claim.
func TestGenericBinOpBoxKindMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
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

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"single+budget", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, spilled, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	const tbl = dbpTable
	chain := " JOIN " + tbl + " t ON c.id = t.id JOIN " + tbl + " u ON c.id = u.id "

	// decpair.a is DECIMAL(9,2) = 12.75 12.75 12.75 -0.01 2.00 0.00 NULL
	// 12.75 NULL; b is DECIMAL(18,4) = 12.7500 12.7501 12.7499 -0.0100
	// 10.0000 0.0000 1.0000 NULL NULL. The literals sit INSIDE those ranges
	// on purpose: a predicate selecting every row or no row would pass on an
	// engine that dropped it.
	for _, fold := range []struct {
		name, expr string
		// PostgreSQL 17.11's COUNT(*) for `> 1`, `>= 1`, `< 1` and
		// `BETWEEN 1 AND 2` over the nine rows.
		gt, ge, lt, between int64
	}{
		{"coalesce", "COALESCE(a, 0) + 1", 5, 8, 1, 3},
		{"case", "CASE WHEN id < 5 THEN a ELSE 0 END + 1", 3, 8, 1, 5},
		{"nullif", "NULLIF(a, b) + 1", 4, 4, 0, 0},
		{"greatest", "GREATEST(a, 0) + 1", 5, 9, 0, 4},
		{"coalesce_wide_scale", "COALESCE(b, 0) + 1", 5, 8, 1, 4},
		{"unary_minus", "-a + 1", 1, 2, 5, 2},
		{"cast", "CAST(a AS DECIMAL(9,2)) + 1", 5, 6, 1, 1},
		{"scalar_fn", "ABS(a) + 1", 6, 7, 0, 2},
		{"times", "COALESCE(a, 0) * 2", 5, 5, 4, 0},
		// The CONTROLS, which must not move. A choice with no arithmetic over
		// it was already classified (joinOperandKinds); plain column
		// arithmetic compiles to the TYPED node, which was already
		// classified; and an all-INTEGER choice keeps the integer domain and
		// never reaches the decimal arm at all.
		{"ctl_choice_without_arithmetic", "COALESCE(a, 0)", 5, 5, 4, 1},
		{"ctl_plain_column_arithmetic", "a + 1", 5, 6, 1, 1},
		{"ctl_integer_choice", "COALESCE(id, 1.5) + 1", 9, 9, 0, 1},
	} {
		fold := fold
		for _, pred := range []struct {
			name, op string
			want     int64
		}{
			{"gt1", "%s > 1", fold.gt},
			{"ge1", "%s >= 1", fold.ge},
			{"lt1", "%s < 1", fold.lt},
			{"between12", "%s BETWEEN 1 AND 2", fold.between},
		} {
			pred := pred
			for _, ctxShape := range []struct{ name, tmpl string }{
				{"plain", "SELECT COUNT(*) AS n FROM " + tbl + " WHERE " +
					fmt.Sprintf(pred.op, "("+fold.expr+")")},
				{"derived", "SELECT COUNT(*) AS n FROM (SELECT id, " + fold.expr +
					" AS dv FROM " + tbl + ") c WHERE " + fmt.Sprintf(pred.op, "c.dv")},
				{"cte", "WITH c AS (SELECT id, " + fold.expr + " AS dv FROM " + tbl +
					") SELECT COUNT(*) AS n FROM c WHERE " + fmt.Sprintf(pred.op, "c.dv")},
				// The headline: a derived table above a TWO-JOIN chain, which
				// is `TestCTEComputedColumnAboveAJoinChainThreeArms`'s own
				// shape. The joins are on the key, so they neither add nor
				// drop a row and the count is the predicate's alone.
				{"derived/2join", "SELECT COUNT(*) AS n FROM (SELECT id, " + fold.expr +
					" AS dv FROM " + tbl + ") c" + chain + "WHERE " +
					fmt.Sprintf(pred.op, "c.dv")},
			} {
				sql := ctxShape.tmpl
				t.Run(fold.name+"/"+pred.name+"/"+ctxShape.name, func(t *testing.T) {
					for _, arm := range arms {
						res, err := arm.run(sql)
						if err != nil {
							t.Fatalf("%s arm refused a query PostgreSQL answers %d: %v\n  SQL: %s",
								arm.name, pred.want, err, sql)
						}
						got := ctrCounts(t, res)
						if len(got) != 1 || got[0] != pred.want {
							t.Errorf("%s arm answered %v, PostgreSQL 17.11 answers %d — a "+
								"comparison above exact arithmetic read the value's rendered "+
								"TEXT by bytes\n  SQL: %s", arm.name, got, pred.want, sql)
						}
					}
				})
			}
		}
	}

	// The boxed sites that are NOT a comparison operator read the same rule,
	// and each was wrong in its own way: GREATEST picked 2 over 13.75 because
	// "13.75" sorts below "2"; IN found nothing; IS DISTINCT FROM found
	// everything. Values, not counts, where the value is what shows it.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"in_list", "SELECT COUNT(*) AS n FROM " + tbl +
			" WHERE (COALESCE(a, 0) + 1) IN (1, 3)", []string{"n=4"}},
		{"is_distinct_from", "SELECT COUNT(*) AS n FROM " + tbl +
			" WHERE (COALESCE(a, 0) + 1) IS DISTINCT FROM 1", []string{"n=6"}},
		{"against_a_decimal_column", "SELECT COUNT(*) AS n FROM " + tbl +
			" WHERE (COALESCE(a, 0) + 1) > b", []string{"n=5"}},
		{"against_a_float_column", "SELECT COUNT(*) AS n FROM " + tbl +
			" WHERE (COALESCE(a, 0) + 1) > f", []string{"n=3"}},
		{"against_a_decimal_literal", "SELECT COUNT(*) AS n FROM " + tbl +
			" WHERE (COALESCE(a, 0) + 1) > 1.0", []string{"n=5"}},
		// The two below carry ADR-0024's recorded per-value scale (#764):
		// PostgreSQL prints the chosen literal `2`, because its numeric keeps
		// each VALUE's own dscale, and a single-scale vector renders every row
		// at the fold's — `2.00`. The DIGITS are the server's, which is what
		// these cells are for: before the fix GREATEST answered 2 for
		// PostgreSQL's 13.75, because "13.75" sorts below "2" as bytes.
		{"greatest_over_the_node", "SELECT GREATEST(COALESCE(a, 0) + 1, 2) AS v FROM " +
			tbl + " WHERE id IN (1, 4, 6) ORDER BY id",
			[]string{"v=13.75", "v=2.00", "v=2.00"}},
		{"least_over_the_node", "SELECT LEAST(COALESCE(a, 0) + 1, 2) AS v FROM " +
			tbl + " WHERE id IN (1, 4, 6) ORDER BY id",
			[]string{"v=2.00", "v=0.99", "v=1.00"}},
		{"case_when_over_the_node", "SELECT CASE WHEN (COALESCE(a, 0) + 1) > 1 " +
			"THEN 'y' ELSE 'n' END AS v FROM " + tbl + " WHERE id IN (1, 4, 6) ORDER BY id",
			[]string{"v=y", "v=n", "v=n"}},
	} {
		tc := tc
		t.Run("boxed-site/"+tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				got := make([]string, 0, len(res.Rows))
				for _, r := range res.Rows {
					for _, c := range res.Columns {
						got = append(got, fmt.Sprintf("%s=%v", c, r[c]))
					}
				}
				if len(got) != len(tc.want) {
					t.Fatalf("%s arm returned %d values, PostgreSQL 17.11 returns %d: %v\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.sql)
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm value %d\n  got  %s\n  want %s (live PostgreSQL 17.11)\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
					}
				}
			}
		})
	}

	// And the DECLARATION the value travels under, on the DAG: PostgreSQL
	// types every one of these `numeric` with no modifier. A right value under
	// a wrong type is the half a value oracle cannot see (ADR-0024 item 2).
	for _, tc := range []struct {
		name, sql, col string
	}{
		{"coalesce", "SELECT COALESCE(a, 0) + 1 AS v FROM " + tbl, "v"},
		{"unary_minus", "SELECT -a + 1 AS v FROM " + tbl, "v"},
		{"cast", "SELECT CAST(a AS DECIMAL(9,2)) + 1 AS v FROM " + tbl, "v"},
		{"scalar_fn", "SELECT ABS(a) + 1 AS v FROM " + tbl, "v"},
	} {
		tc := tc
		t.Run("declares-numeric/"+tc.name, func(t *testing.T) {
			out, err := coord.ExecuteSQL(ctx, tc.sql)
			if err != nil {
				t.Fatalf("stage DAG refused %q: %v", tc.sql, err)
			}
			if out.Error != "" {
				t.Fatalf("stage DAG refused %q: %s", tc.sql, out.Error)
			}
			if !out.WireUnconstrainedDecimal[tc.col] {
				t.Errorf("%s\n  WireUnconstrainedDecimal[%q] = false, want true — "+
					"PostgreSQL types this numeric with no modifier", tc.sql, tc.col)
			}
			for _, c := range out.OutputSchema() {
				if c.Name == tc.col && c.Type != parquet.TypeDecimal {
					t.Errorf("%s\n  column %q declared %s, want DECIMAL", tc.sql, tc.col, c.Type)
				}
			}
		})
	}
}
