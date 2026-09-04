package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #850: the const-arith aggregate lift recovers its speed WITHOUT recovering
// the defect #841 closed.
//
// The two claims are asserted together on purpose. The lifted and the per-row
// forms must answer the SAME value where both can answer, and the shapes where
// the per-row form must RAISE must still raise — a lift that came back for
// those would be #841 restored.
//
// Every expectation is live PostgreSQL 17.11 over the same rows; the switch-off
// arm is the engine's own per-row form, which is what makes each pair a
// statement about the lift rather than about the fixture.
func TestConstArithLiftAnswersTheSameAsThePerRowForm(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table
	for _, sql := range []string{
		`SELECT SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT SUM(c_i32 - 3) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT SUM(3 - c_i32) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT SUM(c_i32 * 2) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT AVG(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT AVG(c_i32 * 2) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT MIN(c_i32 + 3) AS a, MAX(c_i32 + 3) AS b FROM ` + tbl + ` WHERE id < 100`,
		`SELECT MIN(c_i32 * 2) AS a, MAX(c_i32 * 2) AS b FROM ` + tbl + ` WHERE id < 100`,
		`SELECT MIN(3 - c_i32) AS a, MAX(3 - c_i32) AS b FROM ` + tbl + ` WHERE id < 100`,
		`SELECT SUM(c_i64 + 3) AS s FROM ` + tbl + ` WHERE id < 100`,
		// The float columns are here as shapes the lift must LEAVE ALONE, not
		// as evidence about it: `c_f64 = i/3` spans 0…333, so the lifted and
		// per-row forms are bit identical whatever the rewrite does and these
		// cells cannot fail. TestConstArithLiftIsNotAppliedToFloatColumns
		// carries the fixture that separates them (round-1 review, B1).
		`SELECT SUM(c_f64 + 3) AS s FROM ` + tbl + ` WHERE id < 100`,
		`SELECT SUM(c_f32 * 2) AS s FROM ` + tbl + ` WHERE id < 100`,
		// A DECIMAL column, which still declines — the pair must agree anyway.
		`SELECT SUM(c_dec + 3) AS s FROM ` + tbl + ` WHERE id < 100`,
		// NULL semantics: SUM(x+k) sums over the rows where x is non-null, and
		// the lifted SUM(x) + k*COUNT(x) has to count the same rows. c_i32 is
		// non-null in this fixture, so the shape that proves it is the one
		// where the FILTER removes rows.
		`SELECT SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE c_i32 > 100`,
		// Grouped, so the lift is applied under a shuffle key.
		`SELECT g AS k, SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 40 GROUP BY g ORDER BY k`,
		`SELECT g AS k, AVG(c_i32 * 2) AS s FROM ` + tbl + ` WHERE id < 40 GROUP BY g ORDER BY k`,
		// Several aggregates over one column — Q30's shape, where the dedup
		// makes them share a SUM and a COUNT.
		`SELECT SUM(c_i32 + 3) AS a, SUM(c_i32 + 4) AS b, SUM(c_i32 * 2) AS c, ` +
			`COUNT(*) AS n FROM ` + tbl + ` WHERE id < 100`,
		// Two different columns, so the per-column cache cannot cross them.
		`SELECT SUM(c_i32 + 3) AS a, SUM(c_i64 + 3) AS b FROM ` + tbl + ` WHERE id < 100`,
		// An aggregate the lift must leave alone, beside ones it takes.
		`SELECT SUM(c_i32 + 3) AS a, SUM(DISTINCT c_i32) AS b, SUM(c_i32) AS c ` +
			`FROM ` + tbl + ` WHERE id < 100`,
		// A derived table below the aggregate: the walk stops at the Project,
		// so this declines — and must still answer the same thing.
		`SELECT SUM(v + 3) AS s FROM (SELECT c_i32 AS v FROM ` + tbl + ` WHERE id < 100) x`,
		// The aggregate's output read by something OTHER than the projection
		// this pass rewrites. The pass RETIRES the slot the projection used and
		// mints new ones, so any sibling reader is where a rewrite goes wrong —
		// and these are the four readers there are.
		`SELECT SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 100 HAVING SUM(c_i32 + 3) > 0`,
		`SELECT g AS k, SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 40 GROUP BY g ` +
			`HAVING SUM(c_i32 + 3) > 20 ORDER BY k`,
		`SELECT g AS k, SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 40 GROUP BY g ` +
			`ORDER BY SUM(c_i32 + 3) DESC, k`,
		`SELECT SUM(c_i32 + 3) AS s FROM ` + tbl + ` WHERE id < 100 ORDER BY 1`,
		// TWO projections reading one aggregate: the retire-then-sweep has to
		// leave the second one reading something.
		`SELECT SUM(c_i32 + 3) AS x, SUM(c_i32 + 3) AS y FROM ` + tbl + ` WHERE id < 100`,
		// An expression AROUND the aggregate, which this pass does not rewrite
		// (the projection is not IsAgg) and must not disturb.
		`SELECT SUM(c_i32 + 3) + 1 AS s FROM ` + tbl + ` WHERE id < 100`,
		// The aggregate read through a derived table by an outer filter.
		`SELECT COUNT(*) AS n FROM (SELECT g, SUM(c_i32 + 3) AS s FROM ` + tbl +
			` WHERE id < 40 GROUP BY g) x WHERE s > 20`,
	} {
		t.Run(sql, func(t *testing.T) {
			lifted, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("lifted: %v", err)
			}
			tog := caaToggle(t)
			prev := tog.Set(false)
			perRow, err := db.Query(ctx, sql)
			tog.Set(prev)
			if err != nil {
				t.Fatalf("per-row: %v", err)
			}
			if len(lifted.Rows) != len(perRow.Rows) {
				t.Fatalf("%d rows lifted, %d per-row", len(lifted.Rows), len(perRow.Rows))
			}
			for i := range lifted.Rows {
				for _, col := range lifted.Columns {
					if lifted.Rows[i][col] != perRow.Rows[i][col] {
						t.Errorf("row %d column %q: lifted %#v, per-row %#v — the lift is an "+
							"identity over VALUES (#850)",
							i, col, lifted.Rows[i][col], perRow.Rows[i][col])
					}
				}
			}
			// The DECLARED type moves with the value: a lift that changed
			// bigint to double would be right to six digits and wrong on the
			// wire, which is the half only the metadata sees.
			if len(lifted.ColumnMetas) != len(perRow.ColumnMetas) {
				t.Fatalf("%d metas lifted, %d per-row", len(lifted.ColumnMetas), len(perRow.ColumnMetas))
			}
			for i := range lifted.ColumnMetas {
				l, p := lifted.ColumnMetas[i], perRow.ColumnMetas[i]
				if l.TypeID != p.TypeID || l.Precision != p.Precision || l.Scale != p.Scale {
					t.Errorf("column %q declared %s(%d,%d) lifted and %s(%d,%d) per-row",
						l.Name, l.TypeName, l.Precision, l.Scale, p.TypeName, p.Precision, p.Scale)
				}
			}
		})
	}
}

// #841 has NOT come back. The lift may not move a per-row refusal, and every
// one of these overflows int64 on the row it reaches: PostgreSQL 17.11 raises
// `bigint out of range` for the input expression in every position, and the
// bound that lets #850 lift is exactly the proof that these cannot be lifted.
func TestConstArithLiftStillRefusesWhatThePerRowFormRefuses(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table
	for _, sql := range []string{
		`SELECT SUM(c_i64 * 9223372036854775807) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT SUM(c_i64 + 9223372036854775807) AS v FROM ` + tbl + ` WHERE id = 3`,
		`SELECT SUM(-9223372036854775807 - c_i64) AS v FROM ` + tbl + ` WHERE id = 3`,
		`SELECT AVG(c_i64 * 9223372036854775807) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT MIN(c_i64 * 9223372036854775807) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT MAX(c_i64 + 9223372036854775807) AS v FROM ` + tbl + ` WHERE id = 3`,
		// An INT32 column times int8's maximum: the column bounds itself, and
		// the bound says this one overflows.
		`SELECT SUM(c_i32 * 9223372036854775807) AS v FROM ` + tbl + ` WHERE id = 1`,
	} {
		t.Run(sql, func(t *testing.T) {
			got, err := db.Query(ctx, sql)
			if err == nil {
				t.Fatalf("ANSWERED %v; PostgreSQL 17.11 raises 22003 `bigint out of range` for "+
					"this expression in every position, and the lift may not move that "+
					"refusal out of the row (#841, #850)", got.Rows)
			}
			if state := sqlerr.StateOf(err); state != "22003" {
				t.Errorf("SQLSTATE %s, want 22003: %v", state, err)
			}
		})
	}
	// The BOUNDARY from the other side: a NON-INTEGER literal keeps the
	// syntactic lift, and it must — PostgreSQL types the pair numeric there
	// and never refuses it. This is the cell #841's own census carries, kept
	// here so a bound that swallowed it would be visible from #850's side too.
	if _, err := db.Query(ctx,
		`SELECT SUM(c_i64 * 2.0) AS v FROM `+tbl+` WHERE id = 1`); err != nil {
		t.Errorf("a non-integer literal must ANSWER: %v", err)
	}
}

// caaToggle finds the const-arith lift's kill switch in the registry. The
// registry is the seam the invariance oracle reads, so asking it here is what
// makes the per-row arm this test compares against the SAME arm the oracle
// runs.
func caaToggle(t *testing.T) *optswitch.Toggle {
	t.Helper()
	for _, tog := range optswitch.All() {
		if tog.Name == "const-arith-agg" {
			return tog
		}
	}
	t.Fatalf("no const-arith-agg toggle registered")
	return nil
}
