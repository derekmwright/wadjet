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
	// SIX of the ten cells are closed and deleted, which is the proof: the
	// gather's rename puts the BUILD arm's own name back on a source column
	// the walk resolved to a bare one, wherever the arm holds exactly one
	// relation and computes no relation of its own
	// (physical.buildArmQualified).
	//
	// THE FOUR THAT REMAIN ARE THE AGGREGATE-ROOTED ARMS, and their mechanism
	// is one step past this arc's fix: an aggregate publishes a relation of
	// its OWN — its keys and its outputs, under the names IT decided — so the
	// identity of those columns is the ARM's name, while the DAG qualifies
	// them by the scan below it (`stageBuildTableAlias`). Closing it is the
	// same move #1102 made for a set operation: treat an aggregate-terminated
	// arm as a MATERIALIZED arm so the join qualifies it by `joinArmAlias`,
	// and re-qualify here under that name. It is deferred rather than done
	// because that alias decides the spelling of every grouped join arm in
	// the corpus — TPC-H Q13, Q15, Q17 and Q18 each have one — and the change
	// belongs with its own measurement of them.
	"grouped-alias-src-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
	},
	"grouped-alias-src/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
	},
	"nested-rename-grouped-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
	},
	"nested-rename-grouped/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Doohickey,1,Doohickey | 1,Doohickey,1,Doohickey | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 2,Widget,2,Widget",
	},

	// #1095 IS CLOSED and its 2 cells are deleted, which is the proof: an
	// ordering fused onto a JOIN whose probe is the aggregate that publishes
	// one name twice now addresses the SLOT its class names, exactly as the
	// gather's rename does (physical.joinProbeAggregateSlots, and the class
	// through the block from physical.sortTermNamesAggregateItem).

}
