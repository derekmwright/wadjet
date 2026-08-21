package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Regression tests for #367 and #380: statements PostgreSQL refuses must be
// refused here too — with the SQLSTATE a client branches on and an error that
// names the offender — never answered silently. Before the fix every one of
// these produced rows: an unknown table read as "no matching rows", 1/0
// answered 0, CAST('abc' AS integer) answered 0, a bare column beside an
// aggregate answered NULL, an ambiguous column silently picked a side, and a
// reference to an undefined table alias returned either no rows (stage DAG)
// or an operator-level error naming a batch (fast path).
func TestRefusedStatementsError(t *testing.T) {
	ctx := context.Background()
	db, _ := ndOpen(t)

	cases := []struct {
		name      string
		sql       string
		wantState string
		wantIn    string // substring naming the offender
	}{
		{"unknown table", `SELECT * FROM no_such_table_here`, "42P01", `"no_such_table_here"`},
		{"undefined table alias (#380)", `SELECT t0.id FROM events t0 WHERE t1.amount BETWEEN 10 AND 150`, "42P01", `"t1"`},
		{"division by zero literal", `SELECT 1/0`, "22012", "division by zero"},
		{"division by zero over column", `SELECT id / (id - id) FROM events WHERE id = 1`, "22012", "division by zero"},
		{"invalid text cast", `SELECT CAST('abc' AS integer)`, "22P02", `"abc"`},
		{"bare column beside aggregate", `SELECT grp, COUNT(*) FROM events`, "42803", `"grp"`},
		{"ambiguous column", `SELECT id FROM events a JOIN events b ON a.id = b.id`, "42702", `"id"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err == nil {
				t.Fatalf("statement was ANSWERED (%d rows) instead of refused\n  SQL: %s", len(res.Rows), c.sql)
			}
			if got := sqlerr.StateOf(err); got != c.wantState {
				t.Errorf("SQLSTATE = %q, want %s\n  err: %v", got, c.wantState, err)
			}
			if !strings.Contains(err.Error(), c.wantIn) {
				t.Errorf("error must contain %q, got: %v", c.wantIn, err)
			}
		})
	}

	// The refusals' boundaries: the legitimate lookalikes still answer.
	t.Run("lookalikes still answer", func(t *testing.T) {
		for _, sql := range []string{
			`SELECT attrs.score FROM events WHERE id = 1`,                                     // ROW field path
			`SELECT a.id, b.id FROM events a JOIN events b ON a.id = b.id WHERE a.id = 1`,     // qualified disambiguation
			`SELECT grp, COUNT(*) FROM events GROUP BY grp`,                                   // grouped
			`SELECT id / 2 FROM events WHERE id = 4`,                                          // ordinary division
			`SELECT CAST('12' AS integer)`,                                                    // numeric text cast
			`SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM events o WHERE o.id = e.id)`, // correlated outer alias
		} {
			if _, err := db.Query(ctx, sql); err != nil {
				t.Errorf("legitimate statement refused: %v\n  SQL: %s", err, sql)
			}
		}
	})
}
