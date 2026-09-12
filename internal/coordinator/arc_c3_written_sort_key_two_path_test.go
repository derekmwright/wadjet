package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A WRITTEN QUALIFIED ORDER BY TERM BINDS THE COLUMN ITS QUALIFIER NAMES, ON
// THE DAG TOO — #1014, five arms, every answer measured on live
// postgres:17-alpine.
//
// A sort key binds by IDENTITY: an ordinal binds the slot at that position
// (#1003), an alias binds the output slot, and a WRITTEN qualified term binds
// the column of the relation its qualifier names (#989, L1). The first of
// those was settled on the DAG by `f98dac81`, which PINNED this cell one
// spelling over:
//
//	SELECT DISTINCT a.order_id AS amount, b.amount FROM lat_item a
//	JOIN lat_item b ON b.order_id = a.order_id ORDER BY 1, b.amount DESC
//
// The output list carries `amount` TWICE — the alias and `b.amount`'s own bare
// name — and only the single-process engines resolved the written term to a
// POSITION (`sortKeyLocalSlotPos`). On the DAG the key kept the name, and
// `ColumnIndexFallback` answers a duplicate with the FIRST match, so the
// second key sorted on the first column: `1,50 | 1,100 | …` for PostgreSQL's
// `1,100 | 1,50 | …`. A total order is not one of ADR-0013's nondeterminism
// classes, and at 5000 rows under a LIMIT it is a wrong ROW SET as well as a
// wrong sequence.
//
// Both engines now resolve a written term's slot through ONE function,
// `sortKeyWrittenSlotPos`. What they do NOT share is the proof that the
// position addresses THEIR stream: the DAG's is `producerPublishesSelectList`,
// measured against the stage that produces the sort's input (ADR-0026 §8), and
// a written term takes a position ONLY under it — never under the shape bound
// an ordinal may also use — because a written term is resolvable on far more
// queries than an ordinal is, and a position handed out where this layer has
// not looked at the producer is the defect rather than the fix.
//
// THE BOUNDARY IS A CLAIM and the controls attempt it from both sides: a query
// with no duplicate output name, where resolution by name was right all along;
// and a sort over a bare CROSS join, whose stage materializes no projection,
// where the measurement must still DECLINE and the name must still answer.
func TestC3AWrittenSortKeyBindsItsOwnColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c3Arms(t, ctx)

	const selfJoin = "FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id "
	const dup = "SELECT DISTINCT a.order_id AS amount, b.amount " + selfJoin
	const wide = "SELECT DISTINCT a.g AS id, b.id FROM typemx a JOIN typemx b ON b.id = a.id "

	c3RunDecl(t, arms, []c3Case{
		{
			// #1014's own shape: the ordinal half was right after #1003 and
			// the written half was pinned. DESC on the written key, so a run
			// that binds the other column cannot pass by coincidence.
			name: "1014 an ordinal beside a written qualified term",
			sql:  dup + "ORDER BY 1, b.amount DESC",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | 1,100 | 1,50 | 2,125 | 2,75",
		},
		{
			// The written term LEADING, which is a different sequence
			// entirely rather than a re-ordering inside tie groups — so this
			// cell cannot be passed by applying key 1 alone.
			name: "1014 the written term leading",
			sql:  dup + "ORDER BY b.amount, 1",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | 1,50 | 2,75 | 1,100 | 2,125",
		},
		{
			// The written term ALONE. No ordinal in the list at all, so
			// nothing here is inherited from #1003.
			name: "1014 the written term alone, DESC",
			sql:  dup + "ORDER BY b.amount DESC",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | 2,125 | 1,100 | 2,75 | 1,50",
		},
		{
			// BOTH keys written, the second qualified by the other arm of the
			// self-join: `a.order_id` and `b.amount` are two relations' two
			// columns that publish one name between them.
			name: "1014 both keys written, one per join arm",
			sql:  dup + "ORDER BY a.order_id, b.amount DESC",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | 1,100 | 1,50 | 2,125 | 2,75",
		},
		{
			// WITHOUT DISTINCT: eight rows, the written key leading and DESC.
			name: "1014 without DISTINCT, the written key leading",
			sql: "SELECT a.order_id AS amount, b.amount " + selfJoin +
				"ORDER BY b.amount DESC, 1",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=8 | " +
				"2,125 | 2,125 | 1,100 | 1,100 | 2,75 | 2,75 | 1,50 | 1,50",
		},
		{
			// AT SCALE: 5000 rows across four files, so the coordinator's
			// merge coalesces more than one batch before it can order them,
			// and the LIMIT makes a mis-bound key a wrong ROW SET.
			name: "1014 at 5000 rows, an ordinal beside a written term",
			sql:  wide + "ORDER BY 1, b.id DESC LIMIT 6",
			want: "cols=[id:INT32 id:INT64] rows=6 | " +
				"0,4998 | 0,4984 | 0,4977 | 0,4970 | 0,4963 | 0,4956",
		},
		{
			// The same at scale with BOTH keys written.
			name: "1014 at 5000 rows, both keys written",
			sql:  wide + "ORDER BY a.g, b.id DESC LIMIT 6",
			want: "cols=[id:INT32 id:INT64] rows=6 | " +
				"0,4998 | 0,4984 | 0,4977 | 0,4970 | 0,4963 | 0,4956",
		},
		{
			// THE DIVERGENCE, recorded rather than moved. PostgreSQL 17.11
			// refuses `ORDER BY amount` here — 42702 `ORDER BY "amount" is
			// ambiguous`, measured — because two output columns answer to the
			// name. Wadjet answers it, binding the FIRST, which is ADR-0012's
			// "`ORDER BY <name>` over two output columns of that name is
			// answered, not refused" (#557). The second key is an ordinal, so
			// the order is TOTAL and this cell records WHICH column the bare
			// name binds: a change there is a diff rather than a surprise.
			//
			// #1014's filing expects "PG: the output column" for this
			// spelling. That is not what PostgreSQL does, and the superset is
			// already settled, so this arc records the measurement instead of
			// widening the issue.
			name: "1014 divergence: the bare ambiguous name PostgreSQL refuses",
			sql:  dup + "ORDER BY amount, 2",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | 1,50 | 1,100 | 2,75 | 2,125",
			why: "PostgreSQL 17.11 raises 42702 `ORDER BY \"amount\" is ambiguous`; " +
				"wadjet binds the first output column (ADR-0012's divergence list)",
		},
		{
			// THE CTE SPELLING of the headline: one relation referenced twice
			// through a CTE rather than a table, so the duplicate name comes
			// from a derived block's output list rather than a catalog
			// schema. Same answer.
			name: "1014 the CTE spelling of the headline",
			sql: "WITH q AS (SELECT order_id, amount FROM lat_item) " +
				"SELECT DISTINCT a.order_id AS amount, b.amount FROM q a " +
				"JOIN q b ON b.order_id = a.order_id ORDER BY 1, b.amount DESC",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | 1,100 | 1,50 | 2,125 | 2,75",
		},
		{
			// THE GROUP BY TWIN. The producing stage here is a real
			// `final_aggregate` over a GROUP BY rather than the DISTINCT's,
			// and the duplicated name is the aggregate's published key
			// spelling beside an alias. N1's reviewer named this spelling as
			// the one its fix reached beyond its own gate; it is asserted
			// here in the written form too.
			name: "1014 the GROUP BY twin, written key",
			sql: "SELECT b.product AS amount, a.amount " + selfJoin +
				"GROUP BY b.product, a.amount ORDER BY 1, a.amount DESC",
			want: "cols=[amount:STRING amount:FLOAT64] rows=8 | " +
				"Doohickey,125 | Doohickey,75 | Gadget,100 | Gadget,50 | " +
				"Widget,125 | Widget,100 | Widget,75 | Widget,50",
		},
		{
			// The same GROUP BY with both keys ORDINALS, so the two spellings
			// of one list are asserted side by side.
			name: "1014 the GROUP BY twin, ordinals",
			sql: "SELECT b.product AS amount, a.amount " + selfJoin +
				"GROUP BY b.product, a.amount ORDER BY 1, 2 DESC",
			want: "cols=[amount:STRING amount:FLOAT64] rows=8 | " +
				"Doohickey,125 | Doohickey,75 | Gadget,100 | Gadget,50 | " +
				"Widget,125 | Widget,100 | Widget,75 | Widget,50",
		},
		{
			// CONTROL: the same query with the second item ALIASED apart, so
			// no output name is duplicated and resolution by name was right
			// all along. It must not move.
			name: "control: no duplicate output name",
			sql: "SELECT DISTINCT a.order_id AS oid, b.amount AS amt " + selfJoin +
				"ORDER BY amt, 1",
			want: "cols=[oid:INT64 amt:FLOAT64] rows=4 | 1,50 | 2,75 | 1,100 | 2,125",
		},
		{
			// CONTROL: a WRITTEN key over a bare CROSS join, whose stage
			// carries no ProjectExprs — the shape the measurement exists to
			// decline (the same control `f98dac81` uses for the ordinal, one
			// spelling over). The position is not provable here, the name is
			// the address it always was, and PostgreSQL's order is answered.
			name: "control: a written key over a bare cross join",
			sql: "SELECT o.customer, i.amount FROM lat_ord o, lat_item i " +
				"ORDER BY i.amount, o.customer",
			want: "cols=[customer:STRING amount:FLOAT64] rows=12 | " +
				"Alice,50 | Bob,50 | Carol,50 | Alice,75 | Bob,75 | Carol,75 | " +
				"Alice,100 | Bob,100 | Carol,100 | Alice,125 | Bob,125 | Carol,125",
		},
	})
}

// c3RunDecl is c3Run over the DECLARING renderer: the column list and its
// declared types beside the rows, in the query's own order.
func c3RunDecl(t *testing.T, arms []c3Arm, cases []c3Case) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := f1Counters(arm.coord)
				got, err := arm.decl(tc.sql)
				if err != nil {
					got = "ERR " + err.Error()
				}
				want := tc.want
				if p, ok := tc.pin[arm.name]; ok {
					want = p
				}
				if got != want {
					why := ""
					if tc.why != "" {
						why = "\n  recorded: " + tc.why
					}
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s",
						tc.sql, arm.name, got, want, why)
				}
				if strings.HasPrefix(got, "cols=[] ") {
					t.Errorf("%s\n  arm %s answered with NO COLUMNS, which is never an "+
						"answer (arc N1)", tc.sql, arm.name)
				}
				if arm.coord != nil {
					moved := f1CounterDelta(before, f1Counters(arm.coord))
					if moved != tc.routed[arm.name] {
						t.Errorf("%s\n  arm %s disposition: got %q, want %q",
							tc.sql, arm.name, moved, tc.routed[arm.name])
					}
				}
			}
		})
	}
}
