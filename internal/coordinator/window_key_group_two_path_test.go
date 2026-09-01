package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// A WINDOW's output used as a GROUP BY key, on three arms against live
// PostgreSQL 17 (#777).
//
// #741's family — a window inside a derived table under a join — is all correct
// and gated next door. The composition with GROUP BY was the broken cell:
//
//	SELECT x.id, x.w, COUNT(*) FROM (SELECT id, SUM(a) OVER () + 0 AS w
//	  FROM decpair) x LEFT JOIN decpair z ON x.id = z.id GROUP BY x.id, x.w
//
// PostgreSQL 17 and the single-process path answer `w = 52.99` on all nine
// rows. Both DAG arms answered NULL on all nine, silently.
//
// `aggStageGroupKey` answers a key that names a derived table's COMPUTED alias
// with the alias's DEFINING EXPRESSION, unconditionally — so the key was
// dispatched as `__win_0 + 0`, a window SLOT the join does not carry because the
// window arm's own projection already renamed it away to `w`. The worker's
// pre-aggregate projection compiled that text against a batch with no `__win_0`,
// `expr.ColRef.Eval` answered nil, and every row landed in ONE NULL key.
//
// The aggregate's ARGUMENT path has asked the right question since #742 —
// `aggInputAliasIsMaterializedUnderItsName`: where a JOIN, a window, a sort, a
// LIMIT or a DISTINCT stands between the aggregate and the source, the producing
// fragment materializes the alias under its own NAME and the expression is what
// does not resolve. `aggKeyAliasMaterializedByProducer` asks it for the KEY.
//
// Every entry asserts the KEY's value per row and not a row count: the failure
// this replaces returned exactly PostgreSQL's nine rows with a NULL in one
// column, which no count and no key-set assertion can see.
func TestWindowOutputAsAGroupKeyMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
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
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	const tbl = dbpTable // decpair, nine rows; SUM(a) OVER () is 52.99
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17, whole result, ordered
	}{
		{
			name: "the-repro/left-join",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
		},
		{
			// The INNER join twin. #777 named the LEFT join; the mechanism is
			// the key's spelling and has nothing to do with null-padding, so
			// both belong here — and if only one were gated, a fix that keyed on
			// the join type would pass.
			name: "inner-join",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
		},
		{
			// The window output as the ONLY key: nine rows collapse to one, so a
			// NULL key here is the difference between one group of 9 and one
			// group of 9 — the row count is IDENTICAL either way, and only the
			// key's value says which happened.
			name: "the-window-output-as-the-only-key",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " +
				tbl + ") x LEFT JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "1 rows: 52.99|9;",
		},
		{
			// The window arm on the NULL-SUPPLYING side, so the join type is
			// exercised in both directions.
			name: "the-window-arm-is-the-inner-side-of-the-left-join",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM " + tbl + " z LEFT JOIN (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x ON x.id = z.id " +
				"GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "1 rows: 52.99|9;",
		},
		{
			name: "beside-a-having",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w HAVING COUNT(*) > 0 ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
		},
		{
			// A PARTITIONED window, whose per-row values DIFFER — so a key that
			// resolved to one shared column rather than to nothing would still
			// fail here, which the constant-valued entries above cannot see.
			name: "a-partitioned-window-output-as-the-key",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER (PARTITION BY id) + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|12.75|1;2|12.75|1;3|12.75|1;4|-0.01|1;5|2.00|1;6|0.00|1;" +
				"7||1;8|12.75|1;9||1;",
		},
		{
			// A RANKING function, and an aggregate over the OTHER arm beside it,
			// so the answer is not carried entirely by the key column.
			name: "a-ranking-window-output-as-the-key",
			sql: "SELECT x.id AS id, x.w AS w, SUM(z.a) AS s FROM (SELECT id, " +
				"ROW_NUMBER() OVER (ORDER BY id) + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "s"},
			want: "9 rows: 1|1|12.75;2|2|12.75;3|3|12.75;4|4|-0.01;5|5|2.00;6|6|0.00;" +
				"7|7|;8|8|12.75;9|9|;",
		},
		{
			// Over typemx, grouping by a column of the OTHER arm beside the
			// window output — the shape where a NULL key would merge groups that
			// PostgreSQL keeps apart.
			name: "a-window-output-beside-a-key-from-the-other-arm",
			sql: "SELECT t.g AS g, x.w AS w, COUNT(*) AS n FROM (SELECT id, g, " +
				"SUM(c_i32) OVER () + 0 AS w FROM typemx WHERE id < 40) x LEFT JOIN typemx t " +
				"ON x.id = t.id GROUP BY t.g, x.w ORDER BY g",
			cols: []string{"g", "w", "n"},
			want: "8 rows: 0|2256|6;1|2256|6;2|2256|6;3|2256|5;4|2256|5;5|2256|4;6|2256|5;" +
				"|2256|3;",
		},
		// --- controls, all correct at base and all breakable by a fix that
		// answers the ALIAS where nothing materializes it.
		{
			name: "ctl/no-group-by",
			sql: "SELECT x.id AS id, x.w AS w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " +
				tbl + ") x LEFT JOIN " + tbl + " z ON x.id = z.id ORDER BY x.id",
			cols: []string{"id", "w"},
			want: "9 rows: 1|52.99;2|52.99;3|52.99;4|52.99;5|52.99;6|52.99;7|52.99;8|52.99;9|52.99;",
		},
		{
			// The UNWRAPPED window, which is a plain rename of the slot and
			// therefore takes resolveAggInputName's rename arm, not this one.
			name: "ctl/unwrapped-window-output",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// THE BOUNDARY of the answer above, pinned rather than described.
	//
	// `aggKeyAliasMaterializedByProducer` asks whether the producer DIRECTLY
	// below the Project that defines the alias materializes it. It deliberately
	// does not ask the wider question the ARGUMENT path asks — whether ANY
	// join, sort, LIMIT, window or DISTINCT stands between the aggregate and the
	// source — because widening it was measured and it turned two CORRECT DAG
	// answers into `stage scan-0: column "w" does not exist` while fixing none
	// of the shapes below.
	//
	// So an ordinary computed alias over a BARE SCAN, used as a group key
	// through a join or a DISTINCT, is still wrong on the DAG: one NULL group
	// over the whole table. It is not #777's mechanism — it has no window in it,
	// and it is byte-identical with this file's change reverted AND with the
	// whole arc reverted. It is the #736 family's one-field problem
	// (`Stage.GroupByCols` is both the RESOLUTION name and the PUBLISHED name)
	// in a shape that arc's refusal deliberately does not cover: the key IS a
	// column of the aggregate's input by every logical-plan test, and only the
	// STAGE spelling is wrong. It is filed as #781.
	//
	// Pinned here rather than elsewhere because these are the queries a fix to
	// the WIDER question would move, and a pin that starts agreeing FAILS.
	//
	// TODO(#781): delete these when a computed alias over a bare scan resolves.
	for _, c := range []struct {
		name, sql string
		cols      []string
		pg        string // PostgreSQL 17
		dagPinned string // what both DAG arms answer instead; "" = they FAIL
	}{
		{
			name: "pin781/a-computed-alias-over-a-bare-scan-under-a-join",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				") x JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols:      []string{"w", "n"},
			pg:        "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			dagPinned: "1 rows: |9;",
		},
		{
			name: "pin781/a-computed-alias-under-a-distinct",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT g * 3 AS w FROM typemx" +
				") x GROUP BY x.w ORDER BY w",
			cols:      []string{"w", "n"},
			pg:        "8 rows: 0|1;3|1;6|1;9|1;12|1;15|1;18|1;|1;",
			dagPinned: "1 rows: |8;",
		},
		{
			// The no-join spelling, which fails LOUDLY instead — and only
			// because the key is DECIMAL: `derivedGroupKeyDecl` cannot type it
			// through the rename, so the slot is FLOAT64 and the #361 store
			// guard fires. Same site, a different symptom, and it is here so a
			// change that moves either one is visible.
			name:      "pin781/a-computed-decimal-alias-over-a-bare-scan-is-loud",
			sql:       "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl + ") x GROUP BY x.w ORDER BY w",
			cols:      []string{"w", "n"},
			pg:        "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			dagPinned: "",
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(c.sql)
				if arm.name == "single" {
					if err != nil {
						t.Fatalf("single arm refused the query: %v\n  SQL: %s", err, c.sql)
					}
					if got := dajDigest(res, c.cols); got != c.pg {
						t.Errorf("single arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
							got, c.pg, c.sql)
					}
					continue
				}
				if c.dagPinned == "" {
					if err == nil {
						t.Fatalf("the %s arm now ANSWERS this, where this pin records a hard "+
							"failure and PostgreSQL answers %q. Re-measure #781 and assert or "+
							"update the pin\n  SQL: %s", arm.name, c.pg, c.sql)
					}
					continue
				}
				if err != nil {
					t.Fatalf("the %s arm now FAILS where this pin records %q; PostgreSQL answers "+
						"%q. Re-measure #781 and update or delete this pin: %v\n  SQL: %s",
						arm.name, c.dagPinned, c.pg, err, c.sql)
				}
				got := dajDigest(res, c.cols)
				if got == c.pg {
					t.Fatalf("the %s arm now answers PostgreSQL's rows — #781 is fixed for this "+
						"shape. Assert it and delete this pin\n  SQL: %s", arm.name, c.sql)
				}
				if got != c.dagPinned {
					t.Errorf("the %s arm answered\n  %s\nthis pin records\n  %s\nand PostgreSQL "+
						"answers\n  %s\nThe answer MOVED without becoming right, which is a "+
						"change #781 has to account for\n  SQL: %s", arm.name, got, c.dagPinned,
						c.pg, c.sql)
				}
			}
		})
	}
}
