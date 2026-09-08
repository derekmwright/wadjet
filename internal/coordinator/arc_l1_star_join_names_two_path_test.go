package coordinator

import (
	"context"
	"testing"
	"time"
)

// A STAR OVER A JOIN PUBLISHES THE PLAN, NOT THE QUERY — #997, DEFERRED with
// its mechanism (ADR-0026 §6a's "NOT settled" paragraph). This is the census
// the structural fix DELETES; every `want` below is what this tree answers and
// NOT what PostgreSQL answers, and `why` carries PostgreSQL's list so the
// divergence is recorded rather than exempted.
//
// PostgreSQL publishes a join's arms in the FROM clause's written order and
// keeps DUPLICATE names by POSITION:
//
//	SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id
//	  →  id, order_id, product, amount, id, order_id, product, amount
//
// under every predicate, because a name is a property of the query. wadjet
// publishes the PROBE side's columns bare and the BUILD side's qualified, and
// which side builds is a cost decision: `logical.reorderJoins` expresses it by
// SWAPPING the join node's two children ("use cardinality estimates to decide
// probe vs build side") and `physical.buildJoin` reads `Children[0]` as probe
// and `Children[1]` as build. One position carries two meanings — the query's
// arm order, which is the star's column order AND the qualification tie-break,
// and a cost estimate — so the SAME STATEMENT's RowDescription changes when a
// predicate moves that estimate.
//
// The three cells are one statement under no predicate, a selective predicate
// and a zero-row predicate. They differ, on all four arms, and that is the
// defect: a client that keys on column labels (JDBC by label, DataGrip,
// Superset) sees a different schema for one query depending on the data.
//
// Why it is not fixed here (protocol rule 11): both dispositions require the
// published list to be independent of the swap, and after `reorderJoins` there
// is no written arm order left on the node to be independent OF. Recording it
// is not enough either — the EXECUTED stream must then be reordered to match,
// or the declaration (`starJoinDeclaredOutputSchema`, which assembles from the
// same swapped children) and the answer disagree, which is the failure
// ADR-0026 exists to prevent. The fix is to make the build side a PROPERTY of
// the join node and leave the children in written order, with
// `repairDecorrelatedSpelling`, `inner_key_spelling.go`,
// `dedupSemiAntiBuildSide`, `physical.buildJoin`, `walkStages`, the worker
// fragment builder and `exec.joinOutputSchemaWithMapping` all reading that
// property. That is an arc, not a hunk.
//
// A pin that starts agreeing FAILS: when the arc lands these three cells fail,
// and deleting them is its proof.
func TestL1AStarOverAJoinPublishesThePlanNotTheQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const rows = " rows=4 | 1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | " +
		"3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125"
	const qualB = "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 b.id:INT64 " +
		"b.order_id:INT64 b.product:STRING b.amount:FLOAT64]"
	const qualA = "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 a.id:INT64 " +
		"a.order_id:INT64 a.product:STRING a.amount:FLOAT64]"
	// #993's two cells: the three-relation shape, with and without the derived
	// block, stated PER ARM. `single` and `spilled` take `want`; the two DAG
	// arms take their own, and they differ from each other, which is the
	// divergence being pinned.
	const rows993 = " rows=4 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | " +
		"2,1,Gadget,100,1,Alice,150,1,Alice,150 | " +
		"3,2,Widget,75,2,Bob,200,2,Bob,200 | " +
		"4,2,Doohickey,125,2,Bob,200,2,Bob,200"
	const bare993 = "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o.id:INT64 " +
		"customer:STRING total:FLOAT64 o2.id:INT64 o2.customer:STRING o2.total:FLOAT64]" + rows993
	const qual993 = "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o.id:INT64 " +
		"o.customer:STRING o.total:FLOAT64 o2.id:INT64 o2.customer:STRING o2.total:FLOAT64]" + rows993
	const why = "#997 (deferred): PostgreSQL publishes `id, order_id, product, amount, id, " +
		"order_id, product, amount` for all three cells — the FROM clause's arm order, " +
		"duplicates kept by POSITION. wadjet qualifies the BUILD side, and reorderJoins " +
		"picks it by cardinality, so a predicate changes the column list."

	f1Run(t, arms, []f1Case{
		{
			name: "997 no predicate: the star qualifies b",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id ORDER BY a.id",
			want: qualB + rows,
			why:  why,
		},
		{
			// The SAME statement with a predicate that cannot change the rows
			// — every id is under 100 — publishes a DIFFERENT column list.
			name: "997 a selective predicate: the same statement qualifies a",
			sql: "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id " +
				"WHERE a.id < 100 ORDER BY a.id",
			want: qualA + rows,
			why:  why,
		},
		{
			// The zero-row declaration takes the operator's own namer
			// (`exec.JoinOutputSchema`, #978), so it agrees with the executed
			// answer and diverges from PostgreSQL in the same way. That
			// consistency is by construction and is not the defect; the
			// plan-dependence they share is.
			name: "997 a zero-row predicate declares the same plan-dependent list",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id WHERE a.id < 0",
			want: qualA + " rows=0",
			why:  why,
		},
		// #993's SURVIVING half, measured by arc M1 and pinned per DAG ARM.
		//
		// The filing's column SET no longer reproduces — arc K3's v0.18.62
		// made all four arms publish PostgreSQL's ten — and what is left is a
		// NAME divergence between the two DAG arms: `dag` publishes the outer
		// relation's non-key columns bare, `dagshuf` qualifies all of them.
		//
		// Its producer is `physical.markCoPathingSelfJoinBuilds`, which sets
		// `Stage.QualifyAllBuildCols` when two joins in one chain BUILD over
		// the same table (Q07's rule). Disabling that pass in place makes all
		// four arms agree on the bare spelling, which is what localizes it.
		// The two DAG arms differ because the walk reads each join's BUILD
		// dependency and the arms' stage DAGs put different sides on the build:
		// over this statement the broadcast arm finds ONE lat_ord build and the
		// shuffle arm finds TWO, so only the shuffle arm marks. `WADJET_STAGE_
		// FUSION=0` does not change it, so the fusion passes are not the cause.
		//
		// Same producer as #997 and the same rule broken — which side builds is
		// a cost decision and must not decide a NAME — so this rides that arc.
		// The second cell is the same statement with NO derived block, which is
		// what says the block K3's publishing rule stops at is not the
		// condition: the divergence is the join's names, not the block's.
		{
			name: "993 a star over a derived block whose body is a join",
			sql: "SELECT * FROM lat_ord o JOIN (SELECT * FROM lat_item i " +
				"JOIN lat_ord o2 ON o2.id = i.order_id) s ON s.order_id = o.id ORDER BY s.id",
			want:        bare993,
			wantDag:     bare993,
			wantDagshuf: qual993,
			why: "#993 (deferred, rides #997): PostgreSQL publishes the three FROM arms in " +
				"written order with every name bare — `id, customer, total, id, order_id, " +
				"product, amount, id, customer, total`. This tree publishes the join " +
				"operator's order on all four arms (#997), and `dagshuf` additionally " +
				"qualifies the outer relation's non-key columns because " +
				"markCoPathingSelfJoinBuilds marks a second lat_ord build there.",
		},
		{
			name: "993 the same three relations with NO derived block",
			sql: "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"JOIN lat_ord o2 ON o2.id = i.order_id ORDER BY i.id",
			want:        bare993,
			wantDag:     bare993,
			wantDagshuf: qual993,
			why: "#993 (deferred): the SAME divergence with no derived block at all, which " +
				"is what says K3's `a block whose body is a JOIN is never marked` boundary " +
				"is not the condition.",
		},
	})
}
