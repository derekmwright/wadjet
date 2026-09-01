package coordinator

import (
	"context"
	"testing"
	"time"
)

// Arithmetic OVER a DECIMAL window output, on three arms against PostgreSQL 17
// (#729).
//
// `SELECT id, w * 2 FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x` is a
// query PostgreSQL answers and the single-process path answers, and both DAG
// arms failed inside the fragment:
//
//	stage window-1 (window): post-breaker exec:
//	batch: cannot store string into FLOAT64 vector
//
// `decpair.a` is DECIMAL(9,2), so `SUM(a) OVER ()` is exact and its rendering
// is a string; the projection above the window declared `w * 2` FLOAT64 and
// the #361 store guard refused the exact value.
//
// The declaration was there to be read. `windowSpecOutputType` is the window
// stage's own answer for its output slot, DECIMAL (p,s) included, and
// `emittedColDecimal` has read it since #586 — but `inputColTypes` and
// `inputColDecimal`, which are what a consumer ABOVE the window resolves a
// reference through, stopped at a NodeWindow entirely. A window APPENDS its
// outputs and rebinds nothing, so stopping there was never about a rebound
// name; it left the slot with no declared type at all and the float rule stood.
//
// The INTEGER control is the localization: the same shape over `SUM(id)
// OVER ()` was correct on both paths, which says the defect is the DECLARATION
// and not arithmetic over a window output.
//
// Every value below is PostgreSQL 17's over the nine decpair rows.
func TestArithmeticOverADecimalWindowOutputThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string
	}{
		{
			name: "times-two-over-a-decimal-window",
			sql: "SELECT id, w * 2 AS w2 FROM (SELECT id, SUM(a) OVER () AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|105.98;2|105.98;3|105.98;4|105.98;5|105.98;6|105.98;7|105.98;" +
				"8|105.98;9|105.98;",
		},
		{
			// The INTEGER control, correct on both paths before this fix: it
			// is the DECIMAL declaration that was lost, not the arithmetic.
			name: "control-times-two-over-an-integer-window",
			sql: "SELECT id, w * 2 AS w2 FROM (SELECT id, SUM(id) OVER () AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|90;2|90;3|90;4|90;5|90;6|90;7|90;8|90;9|90;",
		},
		{
			// Addition rather than multiplication, so the (p,s) the fold
			// computes is a different one.
			name: "plus-one-over-a-decimal-window",
			sql: "SELECT id, w + 1 AS w2 FROM (SELECT id, SUM(a) OVER () AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|53.99;2|53.99;3|53.99;4|53.99;5|53.99;6|53.99;7|53.99;" +
				"8|53.99;9|53.99;",
		},
		{
			// MIN takes the VALUE-function branch of windowSpecOutputType,
			// which carries the argument's own (p,s) rather than the
			// accumulator's.
			name: "times-two-over-a-decimal-min-window",
			sql: "SELECT id, w * 2 AS w2 FROM (SELECT id, MIN(a) OVER () AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|-0.02;2|-0.02;3|-0.02;4|-0.02;5|-0.02;6|-0.02;7|-0.02;" +
				"8|-0.02;9|-0.02;",
		},
		{
			// The other DECIMAL column, at scale 4: a fixture where both
			// operands are at one scale cannot tell a carried scale from a
			// defaulted one.
			name: "times-two-over-a-scale-4-decimal-window",
			sql: "SELECT id, w * 2 AS w2 FROM (SELECT id, SUM(b) OVER () AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|98.4800;2|98.4800;3|98.4800;4|98.4800;5|98.4800;6|98.4800;" +
				"7|98.4800;8|98.4800;9|98.4800;",
		},
		{
			// A PARTITIONED window, so the value VARIES per row and a
			// whole-column constant cannot pass by accident. NULL rows stay
			// NULL through the arithmetic.
			name: "times-two-over-a-partitioned-decimal-window",
			sql: "SELECT id, w * 2 AS w2 FROM (SELECT id, SUM(a) OVER (PARTITION BY id) AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|25.50;2|25.50;3|25.50;4|-0.02;5|4.00;6|0.00;7|;8|25.50;9|;",
		},
		{
			// A function call rather than an operator, which reaches the
			// declaration through a different branch.
			name: "coalesce-then-times-two",
			sql: "SELECT id, COALESCE(w, 0) * 2 AS w2 FROM (SELECT id, SUM(a) OVER () AS w FROM " +
				tbl + ") x ORDER BY id",
			cols: []string{"id", "w2"},
			want: "9 rows: 1|105.98;2|105.98;3|105.98;4|105.98;5|105.98;6|105.98;7|105.98;" +
				"8|105.98;9|105.98;",
		},
		{
			// The control with NO arithmetic: the window's own output, which
			// was right on every arm and must stay right.
			name: "control-the-window-output-alone",
			sql: "SELECT id, w FROM (SELECT id, SUM(a) OVER () AS w FROM " + tbl +
				") x ORDER BY id",
			cols: []string{"id", "w"},
			want: "9 rows: 1|52.99;2|52.99;3|52.99;4|52.99;5|52.99;6|52.99;7|52.99;" +
				"8|52.99;9|52.99;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n"+
						" — the window's DECIMAL output type never reached the consumer "+
						"above it\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
