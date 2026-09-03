package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// #531: an uncorrelated IN-subquery's membership set is the query's memory, so
// it is charged to the query's budget.
//
// `id IN (SELECT id + 0 FROM typemx)` declines decorrelation — a COMPUTED inner
// select item is not a semi-join key — so it does not become the budgeted,
// spillable semi join its plain twin does. It builds a hash set of every inner
// row instead and probes it per row, and that set was invisible: measured at
// 120,000 bytes, the query ANSWERED at an 8 KiB budget, 14.6× the whole
// allowance, while the same run logged the scan forcing its file load past that
// same budget — so the tracker was live and simply never told.
//
// The pair is the gate. The declining shape must REFUSE and name what it
// refused; the decorrelating twin must still answer, because charging it would
// mean the semi join's build side is being counted twice.
func TestAnInSubquerysMembershipSetIsChargedToTheQueryBudget(t *testing.T) {
	ctx := context.Background()
	const budget = 64 * 1024 // the measured set is 120,000 bytes: nearly 2x this

	t.Run("computed_inner_item_refuses", func(t *testing.T) {
		db := spillMxOpen(t, budget)
		_, err := tmRun(ctx, db, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %[1]s WHERE id IN (SELECT id + 0 FROM %[1]s)`, typematrix.Table))
		if err == nil {
			t.Fatalf("a membership set of every row of %s was built and probed at a %d-byte budget "+
				"without the tracker moving — the set is ~120 KB, nearly 2x the whole allowance (#531)",
				typematrix.Table, budget)
		}
		if !strings.Contains(err.Error(), "IN subquery membership set") {
			t.Fatalf("the query refused, but not for the set: %v\n"+
				"a refusal that does not NAME the membership set leaves an operator no way to tell "+
				"this apart from any other over-budget query", err)
		}
	})

	// The twin that must keep answering: the same predicate with a bare inner
	// column decorrelates into a semi join, whose build side is already
	// budgeted and spillable. If this one starts refusing, the charge has been
	// applied to a set that no longer exists.
	t.Run("decorrelating_twin_answers", func(t *testing.T) {
		db := spillMxOpen(t, budget)
		got, err := tmRun(ctx, db, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %[1]s WHERE id IN (SELECT id FROM %[1]s)`, typematrix.Table))
		if err != nil {
			t.Fatalf("the decorrelating shape must still answer — its semi join is budgeted and "+
				"spillable and reaches no membership set at all: %v", err)
		}
		if n := fmt.Sprint(got.Rows[0]["n"]); n != fmt.Sprint(typematrix.Rows) {
			t.Fatalf("COUNT(*)=%s, want %d", n, typematrix.Rows)
		}
	})
}
