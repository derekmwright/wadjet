package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// THE ASYNC DOOR DESCRIBES ITS RESULT LIKE EVERY OTHER DOOR — #1008 / #1010
// round 2.
//
// `SubmitSQL` + `GetQueryResults` is a FOURTH place a result set is assembled,
// and it is the one place that reads its columns off the GATHERED BATCHES —
// of which a zero-row query has none. The DAG's own fallback,
// `physical.GatherOutputSchema`, describes a GATHER stage, and a one-stage plan
// has none, so EVERY zero-row SELECT came back from this door with
// `"columns": null` and HTTP 200 — including the named select list that the
// embedded, coordinator and HTTP-sync doors have described from the plan since
// #416.
//
// The fix is not a fourth rule: `SubmitSQL` records
// `physical.Planner.DeclaredOutputSchema` — the same walk the embedded door's
// `Plan.OutputSchema` carries — and `GetQueryResults` falls back to it. The
// four doors then describe one statement with one list, and the ONE shape none
// of them can declare (a zero-row star over a bushy join) is refused here with
// the same `sqlerr.EmptyResultColumns` (XX000) it is refused with everywhere
// else, on this door's `SQLResult.Error` channel — the one
// `internal/server.handleGetQueryResults` already answers a failed query on,
// and the one #1002's merge refusal took.
func TestN1AnAsyncResultDeclaresItsColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	c := tmdCoordinator(t, ctx, infra)

	for _, tc := range []struct {
		name, sql, wantCols string
		wantRefusal         bool
	}{
		{
			// The shape no door can declare, refused on all four.
			name: "a zero-row star over a bushy join is refused",
			sql: "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"JOIN lat_item j ON j.order_id = o.id WHERE o.id > 99",
			wantRefusal: true,
		},
		{
			// THE CELL THE OTHER DOORS USE AS A CONTROL, and the one this
			// door answered with no columns at all.
			name:     "a zero-row named select list declares its columns",
			sql:      "SELECT o.id, o.customer FROM lat_ord o WHERE o.id > 99",
			wantCols: "id,customer",
		},
		{
			name:     "a zero-row star over one relation declares its columns",
			sql:      "SELECT * FROM lat_ord WHERE id > 99",
			wantCols: "id,customer,total",
		},
		{
			name: "a zero-row star over one join declares its columns",
			sql: "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"WHERE o.id > 99",
			wantCols: "id,order_id,product,amount,o.id,customer,total",
		},
		{
			// CONTROL: the same statement WITH rows, whose columns come off
			// the batches as they always did.
			name:     "control: the same named list with rows",
			sql:      "SELECT o.id, o.customer FROM lat_ord o",
			wantCols: "id,customer",
		},
		{
			// CONTROL: an aggregate always produced its row and its column.
			name:     "control: a zero-row aggregate still returns its one row",
			sql:      "SELECT COUNT(*) AS n FROM lat_ord WHERE id > 99",
			wantCols: "n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := n1AsyncRun(t, ctx, c, tc.sql)
			got := strings.Join(res.Columns, ",")
			if tc.wantRefusal {
				if !strings.Contains(res.Error, "the result has no columns at all") {
					t.Fatalf("answered cols=[%s] error=%q, want the empty-column refusal\n  SQL: %s",
						got, res.Error, tc.sql)
				}
				return
			}
			if res.Error != "" {
				t.Fatalf("error %q for a statement that declares its columns\n  SQL: %s",
					res.Error, tc.sql)
			}
			if got != tc.wantCols {
				t.Errorf("cols=[%s], want [%s]\n  SQL: %s", got, tc.wantCols, tc.sql)
			}
		})
	}
}

// n1AsyncRun submits a statement on the async door and reads its result back,
// which is the pair `POST /v1/queries/async` and
// `GET /v1/queries/{id}/results` render.
func n1AsyncRun(t *testing.T, ctx context.Context, c *Coordinator, sql string) *SQLResult {
	t.Helper()
	qid, _, err := c.SubmitSQL(ctx, sql)
	if err != nil {
		t.Fatalf("submit: %v\n  SQL: %s", err, sql)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		info := c.tracker.Get(qid)
		if info != nil && (info.State == QueryStateCompleted || info.State == QueryStateFailed) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	res, err := c.GetQueryResults(ctx, qid)
	if err != nil {
		t.Fatalf("results: %v\n  SQL: %s", err, sql)
	}
	return res
}
