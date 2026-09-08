package coordinator

import (
	"context"
	"testing"
	"time"
)

// AN EMPTY COLUMN LIST IS NEVER AN ANSWER — the door rule, four arms.
//
// A statement that produces a result set produces COLUMNS: PostgreSQL sends a
// RowDescription with fields even when it returns no rows, and every client
// depends on it. A result carrying zero columns and no error is not a small
// answer — it is the engine failing to describe its own output, and at the
// client it is INDISTINGUISHABLE from a query that legitimately found nothing.
//
// That is how #1008 and #1010 reached a client, and it is why neither was
// caught by a value comparison: two empty column lists compare equal. Both
// have root-cause fixes in this arc; this refusal is what stops the CLASS from
// being silent again, at the two places a result set is assembled — the
// embedded `wadjet.DB.Query` and `Coordinator.ExecuteSQL` — which every other
// door (pgwire, the HTTP sync and async doors, gRPC) renders.
//
// THE FIXTURE THAT REACHES IT is a zero-row `SELECT *` over a BUSHY join.
// `starJoinDeclaredOutputSchema` declares a star over ONE join by calling the
// join operator's own namer (#978), and declines where a side contains a join
// of its own, because concatenating a nested join's sides is not the rule the
// operator applies. With no rows to read a schema off and no declaration, the
// result had no columns at all. PostgreSQL ANSWERS this query with a header
// and zero rows, so the refusal is a wadjet-side bound and goes in ADR-0012's
// divergence list; what it replaces is not PostgreSQL's answer either.
//
// THE CONTROLS ARE THE BOUNDARY: a zero-row star over one relation and over
// one join still DECLARE their columns and must keep answering, or this
// refusal would have eaten #846's and #978's fixes.
func TestN1AResultWithNoColumnsIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const refusal = "ERR embedded query: the result has no columns at all"
	const refusalDAG = "ERR coordinator query: the result has no columns at all"

	f1Run(t, arms, []f1Case{
		{
			// A zero-row star over a THREE-relation join: nothing declares
			// it, and at base it came back as `cols=[] rows=0`.
			name: "a zero-row star over a bushy join is refused, not empty",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN lat_item i ON i.order_id = o.id " +
				"JOIN lat_item j ON j.order_id = o.id WHERE o.id > 99",
			want:        refusal,
			wantDag:     refusalDAG,
			wantDagshuf: refusalDAG,
		},
		{
			// CONTROL: a zero-row star over ONE relation declares the table's
			// own columns (#846) and must still answer.
			name: "control: a zero-row star over one relation declares its columns",
			sql:  "SELECT * FROM lat_ord WHERE id > 99",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=0",
		},
		{
			// CONTROL: a zero-row star over ONE join declares what the join
			// operator publishes (#978) and must still answer.
			name: "control: a zero-row star over one join declares its columns",
			sql: "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"WHERE o.id > 99",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 " +
				"o.id:INT64 customer:STRING total:FLOAT64] rows=0",
		},
		{
			// CONTROL: a zero-row NAMED select list has declared its columns
			// since #416 and is the shape this refusal must never reach.
			name: "control: a zero-row named select list declares its columns",
			sql:  "SELECT o.id, o.customer FROM lat_ord o WHERE o.id > 99",
			want: "cols=[id:INT64 customer:STRING] rows=0",
		},
	})
}
