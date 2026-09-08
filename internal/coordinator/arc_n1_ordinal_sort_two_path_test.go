package coordinator

import (
	"context"
	"testing"
	"time"
)

// AN ORDINAL SORT KEY BINDS THE SLOT AT THAT POSITION — #1003, four arms,
// every answer measured on live postgres:17-alpine.
//
// An output slot's identity is its POSITION (#557): two output columns may
// legally carry ONE name, and `ORDER BY 1, 2` addresses the slots rather than
// the names. `sortKeySlotPosStage` dropped the position for any sort whose
// subtree contains a join — because on the DAG a Project emits no stage, so
// the sort's input is the PRODUCING stage's stream and a select-list position
// need not address it. The key then resolved by NAME, and
// `ColumnIndexFallback` answers a duplicate with the FIRST match:
//
//	SELECT DISTINCT a.order_id AS amount, b.amount FROM lat_item a
//	JOIN lat_item b ON b.order_id = a.order_id ORDER BY 1, 2 DESC
//
// published `amount` twice, so BOTH keys bound column one and the two DAG arms
// returned `1,50 | 1,100 | …` where PostgreSQL 17 and both single-process arms
// return `1,100 | 1,50 | …`. A total order is not one of ADR-0013's
// nondeterminism classes: the rows are right, the sequence is not, and no
// unordered comparison can see it.
//
// The bound is now MEASURED rather than assumed (ADR-0026 §8, K3's rule): the
// position is used when the stage that produces the sort's input PUBLISHES the
// select list as the ordered prefix of its own output — which the
// `final_aggregate` stage under that query does, materializing
// `[a.order_id→amount, b.amount→amount, a.order_id, b.amount]`. Where the
// projection is not materialized the key still resolves by name, exactly as
// before.
//
// THE BOUNDARY IS A CLAIM, and the cells attempt it from both sides: DESC on
// either key, the ordinals swapped, ties on the leading key, a 5000-row cell
// where the merge has to coalesce, and a control whose sort input is a bare
// join with no materialized projection — the shape the bound exists for.
func TestN1AnOrdinalSortKeyBindsItsSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const selfJoin = "FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id "

	f1Run(t, arms, []f1Case{
		{
			// #1003's shape, one spelling over: two output columns of ONE
			// name, ordered by POSITION, with the second key DESC so a run
			// that drops it cannot pass by coincidence.
			name: "1003 DISTINCT over a self-join, ORDER BY 1, 2 DESC",
			sql: "SELECT DISTINCT a.order_id AS amount, b.amount " + selfJoin +
				"ORDER BY 1, 2 DESC",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"1,100 | 1,50 | 2,125 | 2,75",
		},
		{
			// The same ASC. Its answer is the arrival order too, so it is
			// here as the pair to the cell above rather than as evidence of
			// its own: together they say the key LIST is applied.
			name: "1003 the same ORDER BY 1, 2",
			sql: "SELECT DISTINCT a.order_id AS amount, b.amount " + selfJoin +
				"ORDER BY 1, 2",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"1,50 | 1,100 | 2,75 | 2,125",
		},
		{
			// The ordinals SWAPPED, so a run that binds "the first column" for
			// every key cannot pass in this direction either.
			name: "1003 the same ORDER BY 2, 1",
			sql: "SELECT DISTINCT a.order_id AS amount, b.amount " + selfJoin +
				"ORDER BY 2, 1",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"1,50 | 2,75 | 1,100 | 2,125",
		},
		{
			// The LEADING key DESC, which moves the tie groups rather than
			// the rows inside them.
			name: "1003 the same ORDER BY 1 DESC, 2",
			sql: "SELECT DISTINCT a.order_id AS amount, b.amount " + selfJoin +
				"ORDER BY 1 DESC, 2",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"2,75 | 2,125 | 1,50 | 1,100",
		},
		{
			// An ordinal MIXED with a WRITTEN term. The ordinal is right on
			// every arm now; the written `b.amount` is not, and the two DAG
			// arms are PINNED at the divergence with the mechanism.
			//
			// `resolveSortKeyColumn` maps a written term onto the select-list
			// item that carries it, so this key reaches the stage spelled
			// `amount` — the name the producer publishes TWICE — and binds
			// the first of them. The stream does carry `b.amount` (the
			// aggregate publishes the group key under its own spelling too),
			// so the value is reachable and the rewrite is what loses it.
			// That is a WRITTEN key's resolution, not an ordinal's: the same
			// family as #968's pinned consumers, one spelling over, and it is
			// recorded here rather than widened into this arc.
			name: "1003 an ordinal beside a written key",
			sql: "SELECT DISTINCT a.order_id AS amount, b.amount " + selfJoin +
				"ORDER BY 1, b.amount DESC",
			want: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"1,100 | 1,50 | 2,125 | 2,75",
			wantDag: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"1,50 | 1,100 | 2,125 | 2,75",
			wantDagshuf: "cols=[amount:INT64 amount:FLOAT64] rows=4 | " +
				"1,50 | 1,100 | 2,125 | 2,75",
			why: "a WRITTEN ORDER BY term is rewritten to the select-list " +
				"alias, which two output columns carry; the ordinal half of " +
				"this key list is right on every arm",
		},
		{
			// AT SCALE: 5000 rows, so the coordinator's merge has to coalesce
			// more than one batch before it can apply the ordering, and the
			// ties on the leading key are hundreds deep.
			name: "1003 at 5000 rows, ties on the leading key",
			sql: "SELECT DISTINCT a.g AS id, b.id FROM typemx a " +
				"JOIN typemx b ON b.id = a.id ORDER BY 1, 2 DESC LIMIT 6",
			want: "cols=[id:INT32 id:INT64] rows=6 | " +
				"0,4998 | 0,4984 | 0,4977 | 0,4970 | 0,4963 | 0,4956",
		},
		{
			// CONTROL: the same query with the second item ALIASED apart, so
			// no name is duplicated and the by-name resolution was right all
			// along. It must not move.
			name: "control: no duplicate name, ordinals",
			sql: "SELECT DISTINCT a.order_id AS oid, b.amount AS amt " + selfJoin +
				"ORDER BY 1, 2 DESC",
			want: "cols=[oid:INT64 amt:FLOAT64] rows=4 | " +
				"1,100 | 1,50 | 2,125 | 2,75",
		},
		{
			// CONTROL: a sort over a bare CROSS join with no materialized
			// projection — the shape sortKeySlotPosStage's bound exists for
			// (`SELECT clt1.c2, clt2.c1 FROM clt1, clt2 ORDER BY 2`, held by
			// TestArcA2OrderBy and benchmarks/tpch/duplicate_name_dag_test.go).
			// The measurement must still DECLINE there, and the by-name
			// resolution must still answer PostgreSQL's order.
			name: "control: an ordinal over a bare cross join",
			sql:  "SELECT o.customer, i.amount FROM lat_ord o, lat_item i ORDER BY 2, 1",
			want: "cols=[customer:STRING amount:FLOAT64] rows=12 | " +
				"Alice,50 | Bob,50 | Carol,50 | Alice,75 | Bob,75 | Carol,75 | " +
				"Alice,100 | Bob,100 | Carol,100 | Alice,125 | Bob,125 | Carol,125",
		},
	})
}
