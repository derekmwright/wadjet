package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
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
		{issue: "#744", name: "ctl_aliased_window",
			sql:    `SELECT id, SUM(a) OVER () + 1 AS Wsum FROM decpair ORDER BY id`,
			want:   []string{"id", "Wsum"},
			pgName: []string{"id", "wsum"}},
		{issue: "#744", name: "ctl_aliased_aggregate",
			sql:    `SELECT g, COUNT(*) AS Ntotal FROM typemx GROUP BY g ORDER BY g`,
			want:   []string{"g", "Ntotal"},
			pgName: []string{"g", "ntotal"}},
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
				d := names(arm.name, func() ([]string, error) {
					res, err := tmdRunDAG(ctx, arm.c, tc.sql)
					if err != nil {
						return nil, err
					}
					return res.Columns, nil
				})
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
