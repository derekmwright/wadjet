package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// This file gates #522: LIKE against a container column (ARRAY/ROW/MAP/
// VECTOR) used to match Go's own fmt.Sprint of the boxed value ("[1 2 3]",
// "map[k0:0]") — a text form nothing else in the engine produces and
// nothing ever committed to. PostgreSQL has no `~~` operator for any
// composite or array type (verified live against PostgreSQL 17:
// `ARRAY[1,2,3] LIKE '1'` and a composite-typed value both raise
// "operator does not exist: <type> ~~ unknown", SQLSTATE 42883), and that
// is the answer wadjet now gives too, on both the WHERE-clause kernel path
// (kernel.ResolveLikeFilterKernel) and the row-at-a-time SELECT-list path
// (expr.Like.EvalBoolNull) — ADR-0012 item 11's decision, closed rather
// than left open by silence.
func TestLikeAgainstContainerRefusesWithPostgresErrorCode(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	for _, col := range []string{"c_arr", "c_row", "c_map", "c_vec"} {
		c, ok := findTypeMatrixColumn(t, col)
		if !ok {
			t.Fatalf("typematrix has no column %q", col)
		}
		tbl := c.TableOf()

		t.Run(col+"_where", func(t *testing.T) {
			_, err := tmRun(ctx, db, "SELECT COUNT(*) AS n FROM "+tbl+" WHERE "+col+" LIKE '%1%'")
			assertLikeContainerRefusal(t, err)
		})
		t.Run(col+"_select", func(t *testing.T) {
			_, err := tmRun(ctx, db, "SELECT "+col+" LIKE '%1%' AS m FROM "+tbl)
			assertLikeContainerRefusal(t, err)
		})
	}
}

func assertLikeContainerRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("answered instead of refusing")
	}
	if !strings.Contains(err.Error(), "operator does not exist") || !strings.Contains(err.Error(), "~~") {
		t.Errorf("error = %v, want PostgreSQL's operator-does-not-exist wording", err)
	}
	if got := sqlerr.StateOf(err); got != "42883" {
		t.Errorf("SQLSTATE = %q, want 42883 (undefined_function, PostgreSQL's own code for this) — a client branches on this", got)
	}
}

func findTypeMatrixColumn(t *testing.T, name string) (typematrix.Col, bool) {
	t.Helper()
	for _, c := range typematrix.Columns() {
		if c.Name == name {
			return c, true
		}
	}
	return typematrix.Col{}, false
}
