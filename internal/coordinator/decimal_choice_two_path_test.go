package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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
