package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDecimalChoiceExpressionTwoPath holds the single-process engine and the
// stage DAG to the same answer for a PROJECTED GREATEST/LEAST/COALESCE/CASE
// over two DECIMAL columns of different scale — #529 and the COALESCE half of
// #555.
//
// Neither path could run these before ADR-0024: the registry declared the
// return type FLOAT64, the value arrived as the column's rendered text, and
// the #361 silent-write guard refused it. The two paths reach the projection
// by different routes — the single-process engine compiles exec.Project from
// the AST, the DAG ships a ProjectExprSpec the worker re-parses and compiles
// against a schema it learns from the stage — so a declaration that carried
// the DECIMAL's (p,s) on only one of them would show up here as an arm
// disagreement rather than as a wrong answer on both.
//
// The values are the exact ones dbpData holds: row 3 (a=12.75, b=12.7499) is
// the proof the pick is numeric and not textual, since "12.75" < "12.7499" as
// text, and the fixture's ±1-ulp neighbours are what a rounded comparison
// would collapse.
func TestDecimalChoiceExpressionTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		expr string
		want []string
	}{
		// a is DECIMAL(9,2), b is DECIMAL(18,4): the common type is
		// DECIMAL(18,4) — the maximum scale with the widest integer part —
		// so every row renders with four fraction digits.
		{"greatest", "GREATEST(a, b)",
			[]string{"12.7500", "12.7501", "12.7500", "-0.0100", "10.0000", "0.0000", "1.0000", "12.7500", ""}},
		{"least", "LEAST(a, b)",
			[]string{"12.7500", "12.7500", "12.7499", "-0.0100", "2.0000", "0.0000", "1.0000", "12.7500", ""}},
		{"coalesce", "COALESCE(a, b)",
			[]string{"12.7500", "12.7500", "12.7500", "-0.0100", "2.0000", "0.0000", "1.0000", "12.7500", ""}},
		{"case", "CASE WHEN id < 5 THEN a ELSE b END",
			[]string{"12.7500", "12.7500", "12.7500", "-0.0100", "10.0000", "0.0000", "1.0000", "", ""}},
		// NULLIF mirrors argument 0 alone, so the output keeps a's (9,2).
		// Rows 1, 4 and 6 are NULL because the EQUALITY is exact: 12.75 at
		// scale 2 and 12.7500 at scale 4 are the same number, though not the
		// same text (evalNullIf, the boxed-comparison site #506 did not
		// reach — invisible until a projected NULLIF over a DECIMAL could
		// run at all).
		{"nullif", "NULLIF(a, b)",
			[]string{"", "12.75", "12.75", "", "2.00", "", "", "12.75", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s ORDER BY id", tc.expr, dbpTable)
			var single9, dag9 string
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				got := make([]string, 0, len(rows))
				for _, r := range rows {
					if r["v"] == nil {
						got = append(got, "")
						continue
					}
					s, ok := r["v"].(string)
					if !ok {
						t.Fatalf("%s: v = %#v (%T), want the DECIMAL text — a non-string box "+
							"means the output vector is not a DECIMAL one", arm.name, r["v"], r["v"])
					}
					got = append(got, s)
				}
				joined := strings.Join(got, ",")
				if arm.dag {
					dag9 = joined
				} else {
					single9 = joined
				}
				if joined != strings.Join(tc.want, ",") {
					t.Errorf("%s: %s\n  got  %v\n  want %v", arm.name, sql, got, tc.want)
				}
			}
			if single9 != dag9 {
				t.Errorf("the two paths disagree:\n  single %s\n  dag    %s", single9, dag9)
			}
		})
	}
}

// TestDecimalComputedWireTypmodTwoPath is ADR-0024 item 5 on the DISTRIBUTED
// path: the wire's "unconstrained numeric" (typmod -1) answer for a window
// function (#587), a computed expression, and a set operation whose arms
// disagree (#542) is a PLAN property, so both entry points must carry it —
// the single-process pipeline through CollectSink.SchemaHintWireUnconstrained
// Decimal and the DAG through the terminal gather's
// OutputWireUnconstrainedDecimal.
//
// The DAG half is the one nothing else covers: the pg-oracle wire arm runs
// against a standalone server.
func TestDecimalComputedWireTypmodTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		col  string
		want bool
	}{
		{"bare column keeps its typmod", "SELECT a FROM " + dbpTable, "a", false},
		// select_common_typmod KEEPS the modifier when every input agrees —
		// the direction a "computed means unconstrained" rule gets wrong.
		{"greatest over one column keeps it",
			"SELECT GREATEST(a, a) AS v FROM " + dbpTable, "v", false},
		{"nullif keeps its first argument's",
			"SELECT NULLIF(a, b) AS v FROM " + dbpTable, "v", false},
		{"windowed min is unconstrained",
			"SELECT MIN(a) OVER () AS v FROM " + dbpTable, "v", true},
		{"windowed min over zero rows is unconstrained",
			"SELECT MIN(a) OVER () AS v FROM " + dbpTable + " WHERE id < 0", "v", true},
		{"greatest is unconstrained", "SELECT GREATEST(a, b) AS v FROM " + dbpTable, "v", true},
		{"coalesce is unconstrained", "SELECT COALESCE(a, b) AS v FROM " + dbpTable, "v", true},
		{"a set operation whose arms agree keeps the typmod",
			"SELECT a AS v FROM " + dbpTable + " UNION ALL SELECT a FROM " + dbpTable, "v", false},
		{"a set operation whose arms disagree is unconstrained",
			"SELECT a AS v FROM " + dbpTable + " UNION ALL SELECT b FROM " + dbpTable, "v", true},
		// An arm carrying NO modifier makes the result unconstrained
		// however well the widths line up. A COMPUTED arm is the shape the
		// DAG can run — an AGGREGATE arm is refused there outright (#346),
		// and wadjet/decimal_declared_result_test.go covers that one on the
		// single-process path.
		{"a set operation over a computed arm is unconstrained",
			"SELECT COALESCE(a, b) AS v FROM " + dbpTable +
				" UNION ALL SELECT b FROM " + dbpTable, "v", true},
		// #697 on the DAG. Both entry points read
		// declaredWireUnconstrainedDecimal off the SAME optimized root, so
		// one fix covers both — but "same function" is not evidence, and the
		// shapes below are the ones whose column the walk could not resolve
		// at all: a JOIN whose side is an Aggregate (a decorrelated
		// correlated subquery, a semi-join to a GROUP BY) or a Project (a
		// derived table). Every column of the query lost its declaration
		// there, so a BARE reference came out unconstrained.
		{"a bare column behind a correlated subquery keeps its typmod",
			"SELECT a FROM " + dbpTable + " t WHERE t.id = " +
				"(SELECT MIN(u.id) FROM " + dbpTable + " u WHERE u.s = t.s)", "a", false},
		{"a bare column behind a grouped IN subquery keeps its typmod",
			"SELECT a FROM " + dbpTable + " WHERE id IN (SELECT id FROM " + dbpTable +
				" GROUP BY id)", "a", false},
		{"a bare rename through a derived-table join keeps its typmod",
			"SELECT x.aa AS aa FROM (SELECT id, a AS aa FROM " + dbpTable + ") x JOIN " +
				dbpTable + " u ON x.id = u.id", "aa", false},
		// The other direction, which the join arm of emittedComputedCols is
		// what preserves: with the type map resolving under a join, a value
		// something below it COMPUTED must still lose its modifier.
		{"arithmetic below a derived-table join is unconstrained",
			"SELECT x.d AS d FROM (SELECT id, a * 2 AS d FROM " + dbpTable + ") x JOIN " +
				dbpTable + " u ON x.id = u.id", "d", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := coord.ExecuteSQL(ctx, tc.sql)
			if err != nil {
				t.Fatalf("stage DAG refused %q: %v", tc.sql, err)
			}
			if out.Error != "" {
				t.Fatalf("stage DAG refused %q: %s", tc.sql, out.Error)
			}
			if got := out.WireUnconstrainedDecimal[tc.col]; got != tc.want {
				t.Errorf("%s\n  WireUnconstrainedDecimal[%q] = %v, want %v",
					tc.sql, tc.col, got, tc.want)
			}
			// And the declared TYPE agrees with it: a window over a DECIMAL
			// column describes itself numeric, never float8 (#587's second
			// half, which only the zero-row arm can show).
			if tc.want {
				schema := out.OutputSchema()
				for _, c := range schema {
					if c.Name == tc.col && c.Type != parquet.TypeDecimal {
						t.Errorf("%s\n  column %q declared %s, want DECIMAL", tc.sql, tc.col, c.Type)
					}
				}
			}
		})
	}
}

// TestDecimalComputedKeyTwoPath is TestDecimalChoiceExpressionTwoPath for the
// sites a computed DECIMAL reaches as a MATERIALIZED KEY rather than as an
// output column: a GROUP BY expression, a DISTINCT, an aggregate's derived
// input, an ORDER BY term, a window PARTITION BY / ORDER BY term, and a join
// condition.
//
// Each of those allocates its own vector from a declaration, and on the DAG
// that declaration crosses the wire — as ProjectExprSpec, WindowKeyExprs or
// SortKeySpec — so a (p,s) that rode along only in-process would truncate
// every key on the distributed arm alone: 12.75 and 12.7501 collapsing into
// one group holding 12. That is a wrong ANSWER, not a wrong type, and it is
// the failure mode ADR-0024's declaration work could otherwise have
// introduced where a loud refusal stood before (#529, #555).
func TestDecimalComputedKeyTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// The two 12.75-family values are what a scale-0 key vector
		// collapses: 12.7500 and 12.7501 both truncate to 12.
		{"group by a computed decimal",
			"SELECT COALESCE(a, b) AS k, COUNT(*) AS n FROM " + dbpTable + " GROUP BY COALESCE(a, b) ORDER BY 1",
			"[map[k:-0.0100 n:1] map[k:0.0000 n:1] map[k:1.0000 n:1] map[k:2.0000 n:1] " +
				"map[k:12.7500 n:4] map[k:<nil> n:1]]"},
		{"distinct over a computed decimal",
			"SELECT DISTINCT GREATEST(a, b) AS g FROM " + dbpTable + " ORDER BY 1",
			"[map[g:-0.0100] map[g:0.0000] map[g:1.0000] map[g:10.0000] map[g:12.7500] " +
				"map[g:12.7501] map[g:<nil>]]"},
		{"an aggregate over a computed decimal",
			"SELECT MAX(COALESCE(a, b)) AS m, MIN(GREATEST(a, b)) AS l FROM " + dbpTable,
			"[map[l:-0.0100 m:12.7500]]"},
		{"order by a computed decimal",
			"SELECT id FROM " + dbpTable + " ORDER BY GREATEST(a, b) DESC, id", ""},
		{"a window partitioned by a computed decimal",
			"SELECT id, MIN(a) OVER (PARTITION BY COALESCE(a, b)) AS w FROM " + dbpTable + " ORDER BY id", ""},
		{"a window ordered by a computed decimal",
			"SELECT id, ROW_NUMBER() OVER (ORDER BY GREATEST(a, b), id) AS r FROM " + dbpTable + " ORDER BY id", ""},
		{"a join on a computed decimal",
			"SELECT x.id FROM " + dbpTable + " x JOIN " + dbpTable + " y " +
				"ON COALESCE(x.a, x.b) = COALESCE(y.a, y.b) AND x.id < y.id ORDER BY x.id", ""},
		{"a computed decimal in a filter",
			"SELECT COUNT(*) AS n FROM " + dbpTable + " WHERE GREATEST(a, b) = 12.7501", "[map[n:1]]"},
		// The two-path split the review found: the single-process engine
		// keys an IN subquery's membership set by the RENDERED text, and
		// COALESCE's winner renders at the NARROW column's scale while the
		// set holds the reconciled wide one — "12.75" against "12.7500", one
		// number under two keys, zero rows. The stage DAG lowers the same
		// predicate to a semi join keyed through the columnar encoding and
		// was right all along, so this shape is a disagreement before it is
		// a wrong answer (#474's row-at-a-time twin, ADR-0012 item 8).
		{"an IN subquery whose two sides render at different scales",
			"SELECT id FROM " + dbpTable + " WHERE COALESCE(a, b) IN " +
				"(SELECT COALESCE(a, b) FROM " + dbpTable + " WHERE id = 2) ORDER BY id",
			"[map[id:1] map[id:2] map[id:3] map[id:8]]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotSingle := fmt.Sprintf("%v", dtpRun(t, ctx, single, coord, tc.sql, false))
			gotDAG := fmt.Sprintf("%v", dtpRun(t, ctx, single, coord, tc.sql, true))
			if gotSingle != gotDAG {
				t.Errorf("the two paths disagree on %s\n  single %s\n  dag    %s",
					tc.sql, gotSingle, gotDAG)
			}
			if tc.want != "" && gotSingle != tc.want {
				// A declaration lost in a place BOTH paths share agrees with
				// itself, so the sharpest cases pin the answer as well.
				t.Errorf("%s\n  got  %s\n  want %s", tc.sql, gotSingle, tc.want)
			}
		})
	}
}

// TestDecimalChoiceOverAnIntegerTwoPath is #695 on both engines: a choice
// construct over a DECIMAL branch and an INTEGER one — a literal or a column
// — is numeric on PostgreSQL and DECIMAL here, and the INTEGER branch's value
// materializes at the fold's scale.
//
// The two paths reach it by different routes and can fail differently. The
// single-process engine compiles exec.Project from the AST; the DAG ships a
// ProjectExprSpec the worker re-parses and compiles against a schema it learns
// from the stage, so a box that stayed an INTEGER on one of them would be a
// 22003 there and a value here — and before #695 the DECLARATION itself was
// INT64/FLOAT64, which made the failure depend on which branch each row took.
//
// Values verified live on postgres:17-alpine over the same nine rows. The one
// rendering difference is a finite carrier's: PostgreSQL prints the integer
// branch as `100` because its numeric carries a per-VALUE scale, and a DECIMAL
// column has one scale for the whole column.
func TestDecimalChoiceOverAnIntegerTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		expr string
		want []string
	}{
		// The literal's own spelling is its (p,s), so the fold keeps the
		// column's DECIMAL(9,2) rather than widening to an INT64 range.
		{"greatest over a literal the decimal never beats", "GREATEST(a, 100)",
			[]string{"100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00"}},
		{"coalesce with zero", "COALESCE(a, 0)",
			[]string{"12.75", "12.75", "12.75", "-0.01", "2.00", "0.00", "0.00", "12.75", "0.00"}},
		{"case with an integer else", "CASE WHEN id < 5 THEN a ELSE 0 END",
			[]string{"12.75", "12.75", "12.75", "-0.01", "0.00", "0.00", "0.00", "0.00", "0.00"}},
		// An integer COLUMN contributes its whole RANGE at scale 0, so this
		// one is DECIMAL(21,2).
		{"least over an integer column", "LEAST(a, id)",
			[]string{"1.00", "2.00", "3.00", "-0.01", "2.00", "0.00", "7.00", "8.00", "9.00"}},
		// NULLIF mirrors argument 0 alone, so the fold is over `a` and the
		// output keeps its (9,2). Row 6 is NULL because a IS 0 there.
		{"nullif against an integer literal", "NULLIF(a, 0)",
			[]string{"12.75", "12.75", "12.75", "-0.01", "2.00", "", "", "12.75", ""}},
		// TPC-H Q14's expression: exact arithmetic in the THEN branch and an
		// integer literal in the ELSE. (9,2) x (18,4) is DECIMAL(28,6).
		{"the Q14 shape", "CASE WHEN id < 5 THEN a * b ELSE 0 END",
			[]string{"162.562500", "162.563775", "162.561225", "0.000100",
				"0.000000", "0.000000", "0.000000", "0.000000", "0.000000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s ORDER BY id", tc.expr, dbpTable)
			var singleJoined, dagJoined string
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				got := make([]string, 0, len(rows))
				for _, r := range rows {
					if r["v"] == nil {
						got = append(got, "")
						continue
					}
					s, ok := r["v"].(string)
					if !ok {
						t.Fatalf("%s: v = %#v (%T), want the DECIMAL text — a non-string box "+
							"means the value took the integer carrier reading (#695)",
							arm.name, r["v"], r["v"])
					}
					got = append(got, s)
				}
				joined := strings.Join(got, ",")
				if arm.dag {
					dagJoined = joined
				} else {
					singleJoined = joined
				}
				if joined != strings.Join(tc.want, ",") {
					t.Errorf("%s: %s\n  got  %v\n  want %v", arm.name, sql, got, tc.want)
				}
			}
			if singleJoined != dagJoined {
				t.Errorf("the two paths disagree:\n  single %s\n  dag    %s", singleJoined, dagJoined)
			}
		})
	}
}

// TestDecimalChoiceOverAnIntegerAggregatedTwoPath puts the same expression
// UNDER an aggregate and BEHIND a shuffle, which is where TPC-H Q14 and Q08
// met #695: the pre-aggregate projection materializes the CASE into a DECIMAL
// vector of its own (physical.aggPreProject) and the GROUP BY key hashes it,
// and on the DAG both happen in the worker rather than in this process.
func TestDecimalChoiceOverAnIntegerAggregatedTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// Q14's numerator, exactly: SUM over a CASE whose ELSE is the
		// integer 0. SUM(DECIMAL(28,6)) is DECIMAL(38,6).
		{"sum over the Q14 shape",
			"SELECT SUM(CASE WHEN id < 5 THEN a * b ELSE 0 END) AS v FROM " + dbpTable,
			"487.687600"},
		// Q08's shape: the branch is a bare DECIMAL column and the ELSE an
		// integer literal, so the whole answer used to be declared FLOAT64
		// and was right only while no row took the decimal branch.
		{"sum over the Q08 shape",
			"SELECT SUM(CASE WHEN id < 5 THEN a ELSE 0 END) AS v FROM " + dbpTable,
			"38.24"},
		{"sum over a coalesced column",
			"SELECT SUM(COALESCE(a, 0)) AS v FROM " + dbpTable,
			"52.99"},
		// The choice ABOVE the aggregate rather than below it, which is a
		// different site on the DAG: the gather re-evaluates a WRAPPED
		// aggregate's expression against the merged batch
		// (coordinator.evalExprColumn), and it decides whether to build a
		// DECIMAL column from expr.DecimalResultOf — the same fold. Before
		// #695 the pair `__agg_0` ⊕ `0` was not a DECIMAL result there, so
		// the column was built float64.
		{"coalesce over a sum",
			"SELECT COALESCE(SUM(a), 0) AS v FROM " + dbpTable, "52.99"},
		{"greatest over a sum",
			"SELECT GREATEST(SUM(a), 0) AS v FROM " + dbpTable, "52.99"},
		{"coalesce over a wrapped sum",
			"SELECT COALESCE(SUM(a) * 2, 0) AS v FROM " + dbpTable, "105.98"},
		// The review's P1: the gather re-evaluates a WRAPPED aggregate's
		// expression and decides whether to build a DECIMAL column from
		// expr.DecimalResultOf — the same arm classification. A CAST to an
		// integer and a nested choice of integers were not arms it knew, so
		// the fold declined, the column was built float64, and the decimal
		// text box was NULLED: the DAG answered NULL where PostgreSQL and the
		// single-process path answer 52.99.
		{"greatest over a sum and a CAST",
			"SELECT GREATEST(SUM(a), CAST(0 AS BIGINT)) AS v FROM " + dbpTable, "52.99"},
		{"coalesce over a sum and a nested choice",
			"SELECT COALESCE(SUM(a), CASE WHEN 1=1 THEN 0 ELSE 1 END) AS v FROM " + dbpTable,
			"52.99"},
		// The EMPTY aggregate, where the LITERAL branch is the answer on
		// every row: SUM over no rows is NULL, so the integer arm is what
		// the value comes from. PostgreSQL answers 0.
		//
		// This entry carried a wantDAG pin while the DAG printed "0" and the
		// single-process path printed "0.00". The cause was never this fold:
		// it was #685's EMPTY PARTIAL aggregate emitting its DECIMAL column at
		// scale 0, so every consumer above it read the wrong scale — the same
		// defect that made `SUM(d92) WHERE id = 1` over two files answer
		// 1275.00. #685 is beneath this branch now (7d9d7d79, c77a8a16) and
		// the pin is gone: both paths assert one value, which is what deleting
		// it proves.
		{"coalesce over an empty sum",
			"SELECT COALESCE(SUM(a), 0) AS v FROM " + dbpTable + " WHERE id < 0", "0.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, tc.sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				dtpCell(t, arm.name+" "+tc.sql, rows[0]["v"], tc.want)
			}
		})
	}

	// GROUP BY the expression, which makes the choice a SHUFFLE KEY on the
	// DAG: the key is encoded from the materialized DECIMAL vector, so an
	// integer box that reached it would hash a different number than the
	// single-process path did.
	t.Run("group by the choice expression", func(t *testing.T) {
		sql := "SELECT CASE WHEN id < 5 THEN a ELSE 0 END AS k, COUNT(*) AS n FROM " +
			dbpTable + " GROUP BY 1 ORDER BY 1"
		want := []string{"-0.01=1", "0.00=5", "12.75=3"}
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			got := make([]string, 0, len(rows))
			for _, r := range rows {
				got = append(got, fmt.Sprintf("%v=%v", r["k"], r["n"]))
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s: %s\n  got  %v\n  want %v", arm.name, sql, got, want)
			}
		}
	})
}

// TestAggregateOverADerivedColumnTwoPath is TPC-H Q08's exact shape: an
// aggregate whose ARGUMENT is an EXPRESSION over a column a DERIVED TABLE
// names.
//
// walkStages emits no stage for an ordinary Project (ADR-0025), so the derived
// table's SELECT list never happens on the DAG. The aggregate's argument
// travels to the worker as TEXT and is compiled there against the batch the
// stage hands it — which carries the SCAN's columns, not the derived table's —
// so `volume` resolved to nothing, `expr.ColRef.Eval` answered nil on every
// row, and `SUM(CASE … THEN volume ELSE 0 END)` came back as the total of its
// ELSE branch alone: 0 where PostgreSQL 17 answers 162.562500.
//
// It was TYPE-INDEPENDENT — a plain rename triggered it, no expression and no
// DECIMAL — and every arm was a SILENT wrong number. The worst of them is the
// SHADOWING pair below: where the derived alias spells a base column's name,
// the worker read the BASE column and answered a plausible different number
// rather than a zero, which no "is it suspiciously round" eyeball catches.
//
// respellAggInputExpr closes it by giving every reference INSIDE the argument
// the resolution resolveAggInputName already gave an argument that IS a name:
// a rename becomes its source column, a computed alias becomes its defining
// expression in parentheses. The single-process path runs that Project as a
// real operator and was already right, which is why both arms are asserted —
// a gate comparing only the two arms would pass on the day this regresses in
// the other direction.
//
// Every `want` below is PostgreSQL 17's answer over the same nine rows.
func TestAggregateOverADerivedColumnTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		// want is PostgreSQL 17's answer, and both paths must give it.
		want string
	}{
		{
			name: "a DECIMAL derived column, computed",
			sql: "SELECT SUM(CASE WHEN s = '1.50' THEN volume ELSE 0 END) AS v FROM " +
				"(SELECT s, a * b AS volume FROM " + dbpTable + ") x",
			want: "162.562500",
		},
		{
			name: "a DECIMAL derived column, renamed",
			sql: "SELECT SUM(CASE WHEN s = '1.50' THEN v ELSE 0 END) AS v FROM " +
				"(SELECT s, a AS v FROM " + dbpTable + ") x",
			want: "12.75",
		},
		{
			// No DECIMAL anywhere: this is the entry that says the defect is
			// about the derived NAME, not about the type.
			name: "an INTEGER derived column, computed",
			sql: "SELECT SUM(CASE WHEN s = '1.50' THEN twice ELSE 0 END) AS v FROM " +
				"(SELECT s, id * 2 AS twice FROM " + dbpTable + ") x",
			want: "2",
		},
		{
			name: "an INTEGER derived column, renamed",
			sql: "SELECT SUM(CASE WHEN s = '1.50' THEN idr ELSE 0 END) AS v FROM " +
				"(SELECT s, id AS idr FROM " + dbpTable + ") x",
			want: "1",
		},
		{
			// The SHADOWING arm, and the reason its values were chosen: the
			// derived alias `a` names decpair.b, and the base table HAS a
			// column called `a`. Reading the base column answered 2.0000
			// where the right answer is 10.0000 — a different number, not a
			// zero. Row 5 is the only one where a and b differ (2.00 against
			// 10.0000), so the two readings are distinguishable at all.
			name: "a derived alias SHADOWING a base column",
			sql: "SELECT SUM(CASE WHEN s = '9' THEN a ELSE 0 END) AS v FROM " +
				"(SELECT s, b AS a FROM " + dbpTable + ") x",
			want: "10.0000",
		},
		{
			// Every reference in the CASE is a rename, including the one in
			// the CONDITION: a walk that respelled only the branches would
			// answer the ELSE total here.
			name: "a CASE whose condition and both branches are renames",
			sql: "SELECT SUM(CASE WHEN t = 'x' THEN p ELSE q END) AS v FROM " +
				"(SELECT s AS t, a AS p, b AS q FROM " + dbpTable + ") x",
			want: "49.2400",
		},
		{
			name: "a CAST of a derived column",
			sql:  "SELECT SUM(CAST(v AS BIGINT)) AS v FROM (SELECT a AS v FROM " + dbpTable + ") x",
			want: "54",
		},
		{
			name: "a COALESCE over a derived column",
			sql:  "SELECT SUM(COALESCE(v, 0)) AS v FROM (SELECT a AS v FROM " + dbpTable + ") x",
			want: "52.99",
		},
		{
			// Two derived tables: the walk has to resolve level by level,
			// `w` to `twice` to `id * 2`.
			name: "a derived alias through TWO derived tables",
			sql: "SELECT SUM(CASE WHEN s = '1.50' THEN w ELSE 0 END) AS v FROM " +
				"(SELECT s, twice AS w FROM (SELECT s, id * 2 AS twice FROM " + dbpTable + ") d1) d2",
			want: "2",
		},
		{
			// A computed alias substituted into ARITHMETIC. The definition is
			// `id * 2` and it lands inside `… * 3`, so it has to arrive
			// parenthesized — spliced bare it re-associates.
			name: "arithmetic over a computed derived alias",
			sql:  "SELECT SUM(twice * 3) AS v FROM (SELECT s, id * 2 AS twice FROM " + dbpTable + ") x",
			want: "270",
		},
		{
			// The CONTROL: a bare aggregate over the same derived column,
			// which resolveAggInputName has handled since #355.
			name: "control, a bare aggregate over the derived column",
			sql:  "SELECT SUM(volume) AS v FROM (SELECT a * b AS volume FROM " + dbpTable + ") x",
			want: "507.687600",
		},
		{
			// The other control: the same CASE with no derived table under it.
			name: "control, the same CASE over the base table",
			sql:  "SELECT SUM(CASE WHEN s = '1.50' THEN a ELSE 0 END) AS v FROM " + dbpTable,
			want: "12.75",
		},
		{
			// And the shadowing control: the same alias with no CASE around
			// it, which took resolveAggInputName's name route and was right
			// all along. It is here so a failure localizes to the EXPRESSION
			// walk rather than to the alias resolution under it.
			name: "control, a bare aggregate over the shadowing alias",
			sql:  "SELECT SUM(a) AS v FROM (SELECT s, b AS a FROM " + dbpTable + ") x",
			want: "49.2400",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := dtpRun(t, ctx, single, coord, tc.sql, false)
			if len(rows) != 1 {
				t.Fatalf("single: %d rows, want 1", len(rows))
			}
			if got := fmt.Sprintf("%v", rows[0]["v"]); got != tc.want {
				t.Errorf("single %s = %q, want %q", tc.sql, got, tc.want)
			}

			res, err := tmdRunDAG(ctx, coord, tc.sql)
			if err != nil {
				t.Fatalf("stage DAG refused %q: %v", tc.sql, err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("dag: %d rows, want 1", len(res.Rows))
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != tc.want {
				t.Errorf("dag %s = %q, want %q — PostgreSQL 17 answers %q and the "+
					"single-process path agrees, so the derived name resolved to "+
					"nothing or to the wrong column (#702)", tc.sql, got, tc.want, tc.want)
			}
		})
	}
}

// TestAggregateOverADerivedRowFieldPathTwoPath is the arm the schema assert
// found, and the reason it exists.
//
// `rw.b` over `SELECT c_row AS rw` is a ROW FIELD PATH whose CONTAINER is the
// rename. The whole spelling names no column and neither does the field, so
// the reference reached the worker as `rw.b`, which nothing in the batch can be
// looked up under: the DAG answered 0 where PostgreSQL and the single-process
// path answer 55. It survived the first round of #702's fix — the reference
// resolution asked only about the whole spelling — and
// assertAggregateInputsResolve is what turned it from a silent 0 into
// `its input carries no [rw.b]`, which is how it was found at all.
//
// The qualifier is now resolved when the whole spelling is not, in ADR-0022
// §1's order, and the shape answers on both paths.
func TestAggregateOverADerivedRowFieldPathTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct{ name, sql, want string }{
		{
			name: "a ROW field path whose container is a derived rename",
			sql: "SELECT SUM(CASE WHEN id < 5 THEN rw.b ELSE 0 END) AS v FROM " +
				"(SELECT id, c_row AS rw FROM " + typematrix.Nested + ") x",
			want: "55",
		},
		{
			// The control: the same field path with no rename between the
			// aggregate and the scan, which never needed resolving.
			name: "control, the same field path with no rename",
			sql: "SELECT SUM(CASE WHEN id < 5 THEN c_row.b ELSE 0 END) AS v FROM " +
				typematrix.Nested,
			want: "55",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range sfcArms(ctx, single, coord) {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused %q: %v", arm.name, tc.sql, err)
				}
				if len(res.Rows) != 1 {
					t.Fatalf("%s arm returned %d rows, want 1", arm.name, len(res.Rows))
				}
				if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != tc.want {
					t.Errorf("%s arm answered %q, want %q — a ROW field path through a "+
						"derived rename names nothing the stage carries (#702)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// TestAggregateOverADerivedColumnGroupedTwoPath is the same defect under a
// GROUP BY, where the aggregate stage is a partial/final pair rather than a
// single collapsing stage — a different lowering, and the one TPC-H Q08
// actually takes.
func TestAggregateOverADerivedColumnGroupedTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// PostgreSQL 17 over the nine decpair rows: only s='1.50' matches, and
	// its `twice` is 2. Every other group totals its ELSE branch, 0.
	want := map[string]string{
		"-1": "0", "0": "0", "1.5": "0", "1.50": "2",
		"1.500": "0", "10": "0", "9": "0", "abc": "0",
	}
	sql := "SELECT s, SUM(CASE WHEN s = '1.50' THEN twice ELSE 0 END) AS v FROM " +
		"(SELECT s, id * 2 AS twice FROM " + dbpTable + ") x GROUP BY s ORDER BY s"

	for _, arm := range sfcArms(ctx, single, coord) {
		res, err := arm.run(sql)
		if err != nil {
			t.Fatalf("%s arm refused %q: %v", arm.name, sql, err)
		}
		if len(res.Rows) != len(want) {
			t.Fatalf("%s arm returned %d rows, want %d", arm.name, len(res.Rows), len(want))
		}
		for _, r := range res.Rows {
			k := fmt.Sprintf("%v", r["s"])
			if got := fmt.Sprintf("%v", r["v"]); got != want[k] {
				t.Errorf("%s arm answered %q for s=%q, want %q — the grouped lowering "+
					"reads the same derived name (#702)", arm.name, got, k, want[k])
			}
		}
	}
}
