package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// The two wadjet paths must DESCRIBE one query identically.
//
// Nothing in this repository compared the two paths' output column NAMES
// before this gate: the semantics corpus is deliberately name-blind
// (benchmarks/tpch/postgres_compare_test.go, "which diverges for a reason of
// its own (#732)"), the wire arm drives the single-process engine only, and
// the two-path suites compare rows keyed by name — which cannot see a name
// that differs, because each arm reads its own.
//
// #744: `extractOutputRenames` (internal/planner/physical/plan.go) built the
// client-visible target with `strings.ToLower(p.Expr)` while `deriveColumns`
// (wadjet/wadjet.go) sends `col.Expr` verbatim. An unaliased window inside a
// larger expression therefore arrived as `sum(a) over (...) + 1` from the DAG
// and `sum(a) OVER (...) + 1` from the single-process engine — one query, two
// RowDescriptions, and a BI client that keys a result set by column name sees
// two different result sets.
//
// `pgName` records PostgreSQL 17's own name for the same statement, measured
// live. Wadjet does not send it (that is #732, a naming RULE and a product
// decision — PostgreSQL names an unaliased expression `?column?`), and this
// gate deliberately does not assert it: what it asserts is that the two
// wadjet paths agree, which is true today and independent of whichever rule
// #732 settles on.
type a2NameCell struct {
	issue, name, sql string
	// want is the full ordered column-name list BOTH paths must send.
	want []string
	// pgName is PostgreSQL 17's name list for the same statement, recorded so
	// the #732 decision has its measurement beside the shape rather than in a
	// scratch file.
	pgName []string
	// wantRoutes is the routing delta each DAG arm must show. The zero value
	// means the DAG EXECUTED this shape; anything else names the refusal it
	// took, and says that the two DAG arms are the coordinator-local pipeline
	// for this cell (rule 11).
	wantRoutes a2Routes
}

func a2NameCells() []a2NameCell {
	return []a2NameCell{
		// ------------------------------------------------------------------
		// #744 — an unaliased WINDOW inside a larger expression. The three
		// shapes the DAG lowercased, and the bare-window control that already
		// agreed (#694's `WindowOutputName`) and must keep agreeing.
		{issue: "#744", name: "window_plus_one",
			sql:    `SELECT id, SUM(a) OVER () + 1 FROM decpair ORDER BY id`,
			want:   []string{"id", "sum(a) OVER (...) + 1"},
			pgName: []string{"id", "?column?"}},
		{issue: "#744", name: "window_partition_times_two",
			sql:    `SELECT id, SUM(a) OVER (PARTITION BY id) * 2 FROM decpair ORDER BY id`,
			want:   []string{"id", "sum(a) OVER (...) * 2"},
			pgName: []string{"id", "?column?"}},
		{issue: "#744", name: "cast_over_window",
			sql:    `SELECT id, CAST(SUM(a) OVER () AS BIGINT) FROM decpair ORDER BY id`,
			want:   []string{"id", "cast(sum(a) OVER (...) as bigint)"},
			pgName: []string{"id", "sum"}},
		{issue: "#744", name: "ctl_bare_window",
			sql:    `SELECT id, SUM(a) OVER () FROM decpair ORDER BY id`,
			want:   []string{"id", "sum"},
			pgName: []string{"id", "sum"}},

		// ------------------------------------------------------------------
		// The neighbouring namers the same switch reaches. They agreed before
		// #744's fix and the fix must not move them: an aggregate's own text,
		// an aggregate inside an expression, an aliased window, a bare column
		// and a computed column with no aggregate anywhere.
		{issue: "#744", name: "ctl_bare_aggregate",
			sql:    `SELECT g, COUNT(*) FROM typemx GROUP BY g ORDER BY g`,
			want:   []string{"g", "count(*)"},
			pgName: []string{"g", "count"}},
		{issue: "#744", name: "ctl_aggregate_in_expression",
			sql:    `SELECT g, SUM(id) + 1 FROM typemx GROUP BY g ORDER BY g`,
			want:   []string{"g", "sum(id) + 1"},
			pgName: []string{"g", "?column?"}},
		// An UNQUOTED alias folds, so both paths now send PostgreSQL's own
		// name for it; the delimited spelling beside it keeps its bytes
		// (#731). Before the fold, wadjet sent `Wsum` where PostgreSQL sends
		// `wsum` — a name a BI client keying by column name reads as a
		// different column.
		{issue: "#744", name: "ctl_aliased_window",
			sql:    `SELECT id, SUM(a) OVER () + 1 AS Wsum FROM decpair ORDER BY id`,
			want:   []string{"id", "wsum"},
			pgName: []string{"id", "wsum"}},
		{issue: "#731", name: "ctl_aliased_window_delimited",
			sql:    `SELECT id, SUM(a) OVER () + 1 AS "Wsum" FROM decpair ORDER BY id`,
			want:   []string{"id", "Wsum"},
			pgName: []string{"id", "Wsum"}},
		{issue: "#744", name: "ctl_aliased_aggregate",
			sql:    `SELECT g, COUNT(*) AS Ntotal FROM typemx GROUP BY g ORDER BY g`,
			want:   []string{"g", "ntotal"},
			pgName: []string{"g", "ntotal"}},
		{issue: "#731", name: "ctl_aliased_aggregate_delimited",
			sql:    `SELECT g, COUNT(*) AS "Ntotal" FROM typemx GROUP BY g ORDER BY g`,
			want:   []string{"g", "Ntotal"},
			pgName: []string{"g", "Ntotal"}},
		{issue: "#731", name: "identifier_case_in_the_select_list",
			sql:    `SELECT G AS gk, COUNT(*) AS n FROM typemx GROUP BY g ORDER BY gk`,
			want:   []string{"gk", "n"},
			pgName: []string{"gk", "n"}},
		{issue: "#731", name: "identifier_case_in_a_qualified_reference",
			sql:    `SELECT T.G FROM typemx T WHERE id < 3 ORDER BY 1`,
			want:   []string{"g"},
			pgName: []string{"g"}},
		{issue: "#744", name: "ctl_computed_no_aggregate",
			sql:    `SELECT g + 1 FROM typemx WHERE id < 3 ORDER BY 1`,
			want:   []string{"g + 1"},
			pgName: []string{"?column?"}},
		{issue: "#744", name: "ctl_bare_column",
			sql:    `SELECT g FROM typemx WHERE id < 3 ORDER BY g`,
			want:   []string{"g"},
			pgName: []string{"g"}},
	}
}

// The two paths must also DECLARE one query identically (#813).
//
// `CAST(SUM(a) OVER () AS BIGINT)` came back as an int64 from the
// single-process engine and as a float64 from the DAG. The declaration was the
// same on both — `inferCastType("bigint")` is INT64 — and the DAG's VALUE was
// not: `expr.Int64ResultOf`, which decides whether the gather materializes an
// expression into an INT64 vector or a float64 one, had no CAST arm, so the
// integer box the cast produces went into a float64 column. One query, two
// wire OIDs, decided by which engine ran it.
//
// This gate is the type twin of the name gate above, and it lives beside it
// for the reason that one exists: nothing in the tree compared the two paths'
// declared TYPES either, because every two-path row comparison reads each
// arm's values through that arm's own boxes.
type a2DeclCell struct {
	issue, name, sql string
	// want is the Go box each row's value column must arrive in, on BOTH
	// paths. It is the box and not the number: the numbers agreed all along.
	want   string
	pgSays string
	// wantRoutes is the routing delta each DAG arm must show. The zero value
	// means the DAG EXECUTED this shape; anything else names the refusal it
	// took, and says that the two DAG arms are the coordinator-local pipeline
	// for this cell (rule 11).
	wantRoutes a2Routes
}

func a2DeclCells() []a2DeclCell {
	return []a2DeclCell{
		{issue: "#813", name: "cast_window_to_bigint",
			sql:  `SELECT CAST(SUM(a) OVER () AS BIGINT) AS v FROM decpair ORDER BY id LIMIT 1`,
			want: "int64", pgSays: "bigint"},
		{issue: "#813", name: "cast_window_to_integer",
			sql:  `SELECT CAST(SUM(a) OVER () AS INTEGER) AS v FROM decpair ORDER BY id LIMIT 1`,
			want: "int64", pgSays: "integer — wadjet's standing int4/int8 width divergence, " +
				"and the DOMAIN is what this cell is about"},
		{issue: "#813", name: "cast_window_to_smallint",
			sql:  `SELECT CAST(SUM(a) OVER () AS SMALLINT) AS v FROM decpair ORDER BY id LIMIT 1`,
			want: "int64", pgSays: "smallint"},
		// The controls: a non-integer destination must stay float, and a cast
		// over a GROUPED aggregate — which already agreed — must not move.
		{issue: "#813", name: "ctl_cast_window_to_double",
			sql:  `SELECT CAST(SUM(a) OVER () AS DOUBLE) AS v FROM decpair ORDER BY id LIMIT 1`,
			want: "float64", pgSays: "double precision"},
		{issue: "#813", name: "ctl_cast_grouped_aggregate_to_bigint",
			sql:  `SELECT CAST(SUM(id) AS BIGINT) AS v FROM typemx GROUP BY g ORDER BY g LIMIT 1`,
			want: "int64", pgSays: "bigint"},
	}
}

func TestBothPathsDeclareACastOverAWindowTheSameWay(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range a2DeclCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			box := func(arm string, res *oracle.Result, err error) string {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
				}
				if len(res.Rows) == 0 {
					t.Fatalf("%s arm: no rows\n  SQL: %s", arm, tc.sql)
				}
				got := fmt.Sprintf("%T", res.Rows[0]["v"])
				if got != tc.want {
					t.Errorf("%s arm: v arrives as %s, want %s\n  PostgreSQL 17 declares %s\n  SQL: %s",
						arm, got, tc.want, tc.pgSays, tc.sql)
				}
				return got
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			s := box("single", sres, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				if d := box(arm.name, dres, derr); d != s {
					t.Errorf("the two wadjet paths declare one query differently\n"+
						"  single       %s\n  %-12s %s\n  SQL: %s", s, arm.name, d, tc.sql)
				}
			}
		})
	}
}

func TestBothPathsNameAnUnaliasedExpressionTheSameWay(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range a2NameCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			names := func(arm string, run func() ([]string, error)) []string {
				t.Helper()
				got, err := run()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
				}
				if strings.Join(got, "\x00") != strings.Join(tc.want, "\x00") {
					t.Errorf("%s arm names\n  got  %q\n  want %q\n  PostgreSQL 17 sends %q (#732: wadjet "+
						"does not adopt that rule; what this gate holds is that the two wadjet paths agree)\n  SQL: %s",
						arm, got, tc.want, tc.pgName, tc.sql)
				}
				return got
			}

			s := names("single", func() ([]string, error) {
				res, err := tmdRunSingle(ctx, single, tc.sql)
				if err != nil {
					return nil, err
				}
				return res.Columns, nil
			})
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				d := names(arm.name, func() ([]string, error) {
					res, err := tmdRunDAG(ctx, arm.c, tc.sql)
					if err != nil {
						return nil, err
					}
					return res.Columns, nil
				})
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				// Asserted against the SINGLE arm as well as against `want`,
				// so a future change that moves BOTH paths in step is caught
				// by `want` and one that moves only one is caught here even
				// if `want` were updated carelessly.
				if strings.Join(d, "\x00") != strings.Join(s, "\x00") {
					t.Errorf("the two wadjet paths describe one query differently\n"+
						"  single       %q\n  %-12s %q\n  SQL: %s", s, arm.name, d, tc.sql)
				}
			}
		})
	}
}
