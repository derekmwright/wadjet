package coordinator

// r2Pin is the RESIDUE of the join-arm table: every cell that does not agree
// with PostgreSQL 17.11 on some arm, pinned PER ARM with the mechanism that
// keeps it open. Every value here was measured at base 2d819c95 with this same
// table, before any code in this arc changed, so a pin is what the engine
// ANSWERED and never what this arc made of it. A pin that starts agreeing
// FAILS: deleting one is the proof its fix landed.
var r2Pin = map[string]map[string]string{
	// #1102 IS CLOSED and its 19 cells are deleted, which is the proof: a
	// set operation composes a NEW relation out of what its arms emit, so the
	// stage's stream is the operation's own list and the name the enclosing
	// query writes describes all of it. The arm is a MATERIALIZED arm now
	// (physical.setOpArmPublishesItsOwnList), qualified by that name, and the
	// star states its list (logical.blockOwnProjection) instead of declining
	// it — 16 star cells on five arms and 3 value cells on the DAG arms.

	// #1099 IS CLOSED and its 35 cells are deleted, which is the proof: a
	// reference into a block resolves to the spelling the producing stream
	// really carries, so `o2.id AS k` is chased to `o2.id` and not to the
	// bare `id` that BOTH relations inside the block answer to
	// (physical.resolveShuffleKey). Every consumer of a join-bodied arm moves
	// with it.

	// TWO DERIVED ARMS OF ONE SHAPE, on the three DAG arms: `a.k` and `b.k`
	// each resolve through their block's projection to the same bare source
	// name, and a bare name binds the PROBE side's copy — so both items read
	// one relation and every row is paired with itself.
	//
	// EIGHT of the ten cells are closed and deleted. Six by the gather's
	// rename putting the BUILD arm's own name back on a source column the
	// walk resolved to a bare one (physical.buildArmQualified); two more by
	// an arm whose SELECT list the aggregate absorption MATERIALIZED being a
	// materialized arm, qualified by the name the enclosing query writes
	// rather than by the scan below it — #1102's rule one producer over, and
	// it moves nothing in the corpus (round 2, P1: the TPC-H stage-dump
	// golden, the distribution snapshot, the optimization-invariance and
	// two-path arms are byte-identical with it).
	//
	// THE TWO THAT REMAIN ARE THE NESTED GROUPED ARM: the block that renames
	// is TWO blocks above the aggregate, with the inner block's own Sort
	// between them, so `aggregateProjectionTarget` — which reads a Project
	// whose INPUT is the aggregate's output, through a HAVING Filter and
	// nothing else — does not reach it and no list is materialized. Marking
	// such an arm materialized anyway was MEASURED in round 2 and is a
	// different wrong answer, not a fix: the arm is then named `b` while its
	// stage still emits the inner block's `p`/`n`, and the outer block's
	// rename is lost — `cols=[p n b.product b.n]` where PostgreSQL publishes
	// `k p k p`. Closing it needs the nested block's list materialized onto
	// the aggregate stage (the #991/#1076 machinery, one level deeper), which
	// is a second question and not a name.
	"nested-rename-grouped/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
	},
	"nested-rename-grouped-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
	},
}
