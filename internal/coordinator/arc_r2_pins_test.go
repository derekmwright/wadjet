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
	// name (`order_id`), and a bare name binds the PROBE side's copy — so
	// both items read one relation and every row is paired with itself.
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
	"nested-rename-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
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
	"nested-rename/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
	},
	"sortkey-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
	},
	"sortkey-limit-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=5 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Widget,2,Widget",
	},
	"sortkey-limit/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=5 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Widget,2,Widget",
	},
	"sortkey/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=8 | 1,Gadget,1,Gadget | 1,Gadget,1,Gadget | 1,Widget,1,Widget | 1,Widget,1,Widget | 2,Doohickey,2,Doohickey | 2,Doohickey,2,Doohickey | 2,Widget,2,Widget | 2,Widget,2,Widget",
	},

	// #1095's ORDER half, on the three DAG arms: the statement's ORDER BY
	// above the JOIN sorts by the group KEY's value where the projection
	// reads the aggregate that was aliased with the key's source name, so the
	// gather merges the stage's runs on the wrong column (ADR-0026 §8a).
	"issue/1095": {
		"dag":          "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
		"dag-shuffled": "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
		"dag-morsel4":  "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
	},
	"issue/1095-desc": {
		"dag":          "cols=[product:INT64 id:INT64] rows=9 | 2,1 | 2,2 | 2,3 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3",
		"dag-shuffled": "cols=[product:INT64 id:INT64] rows=9 | 2,1 | 2,2 | 2,3 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3",
		"dag-morsel4":  "cols=[product:INT64 id:INT64] rows=9 | 2,1 | 2,2 | 2,3 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3",
	},
}
