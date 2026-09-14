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

	// #1099, on the three DAG arms: a JOIN-BODIED arm's key resolved through
	// the block's own projection to a BARE source name (`o2.id AS k` chased
	// to `id`), and the block's stream carries TWO relations' `id` — so the
	// outer join keyed on `lat_item`'s id instead of `lat_ord`'s and paired
	// rows that violate the join condition. Every consumer of the arm reads
	// the same wrong rows, which is why the whole `joinbody` column of the
	// table is here.
	"issue/1099": {
		"dag":          "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
		"dag-shuffled": "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
	},
	"issue/1099-block-first": {
		"dag":          "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
		"dag-shuffled": "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
	},
	"issue/1099-no-distinct": {
		"dag":          "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
		"dag-shuffled": "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING c:STRING k:INT64] rows=3 | 1,Alice,Alice,1 | 2,Bob,Alice,1 | 3,Carol,Bob,2",
	},
	"joinbody-collide/both/group": {
		"dag":          "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,2",
		"dag-shuffled": "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,2",
		"dag-morsel4":  "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,2",
	},
	"joinbody-collide/both/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
	},
	"joinbody-collide/both/ord": {
		"dag":          "cols=[customer:STRING id:INT64 id:INT64] rows=4 | Alice,1,1 | Alice,1,1 | Bob,2,2 | Bob,2,2",
		"dag-shuffled": "cols=[customer:STRING id:INT64 id:INT64] rows=4 | Alice,1,1 | Alice,1,1 | Bob,2,2 | Bob,2,2",
		"dag-morsel4":  "cols=[customer:STRING id:INT64 id:INT64] rows=4 | Alice,1,1 | Alice,1,1 | Bob,2,2 | Bob,2,2",
	},
	"joinbody-collide/both/qual": {
		"dag":          "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
		"dag-shuffled": "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
		"dag-morsel4":  "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
	},
	"joinbody-collide/both/star": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
	},
	"joinbody-collide/left/distinct": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
	},
	"joinbody-collide/left/group": {
		"dag":          "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-shuffled": "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-morsel4":  "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,1",
	},
	"joinbody-collide/left/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
	},
	"joinbody-collide/left/ord": {
		"dag":          "cols=[customer:STRING id:INT64 id:INT64] rows=3 | Alice,1,1 | Alice,1,2 | Bob,2,3",
		"dag-shuffled": "cols=[customer:STRING id:INT64 id:INT64] rows=3 | Alice,1,1 | Alice,1,2 | Bob,2,3",
		"dag-morsel4":  "cols=[customer:STRING id:INT64 id:INT64] rows=3 | Alice,1,1 | Alice,1,2 | Bob,2,3",
	},
	"joinbody-collide/left/qual": {
		"dag":          "cols=[id:INT64] rows=3 | 1 | 1 | 2",
		"dag-shuffled": "cols=[id:INT64] rows=3 | 1 | 1 | 2",
		"dag-morsel4":  "cols=[id:INT64] rows=3 | 1 | 1 | 2",
	},
	"joinbody-collide/left/star": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,1,Alice,150 | 1,Alice,2,Bob,200 | 2,Bob,3,Carol,0",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,1,Alice,150 | 1,Alice,2,Bob,200 | 2,Bob,3,Carol,0",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,1,Alice,150 | 1,Alice,2,Bob,200 | 2,Bob,3,Carol,0",
	},
	"joinbody-collide/right/distinct": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
	},
	"joinbody-collide/right/group": {
		"dag":          "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-shuffled": "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-morsel4":  "cols=[id:INT64 n:INT64] rows=2 | 1,2 | 2,1",
	},
	"joinbody-collide/right/list": {
		"dag":          "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-shuffled": "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
	},
	"joinbody-collide/right/ord": {
		"dag":          "cols=[customer:STRING id:INT64 id:INT64] rows=3 | Alice,1,1 | Alice,1,2 | Bob,2,3",
		"dag-shuffled": "cols=[customer:STRING id:INT64 id:INT64] rows=3 | Alice,1,1 | Alice,1,2 | Bob,2,3",
		"dag-morsel4":  "cols=[customer:STRING id:INT64 id:INT64] rows=3 | Alice,1,1 | Alice,1,2 | Bob,2,3",
	},
	"joinbody-collide/right/qual": {
		"dag":          "cols=[id:INT64] rows=3 | 1 | 1 | 2",
		"dag-shuffled": "cols=[id:INT64] rows=3 | 1 | 1 | 2",
		"dag-morsel4":  "cols=[id:INT64] rows=3 | 1 | 1 | 2",
	},
	"joinbody-collide/right/star": {
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 customer:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,1,Alice | 3,Carol,0,2,Bob",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 customer:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,1,Alice | 3,Carol,0,2,Bob",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 customer:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,1,Alice | 3,Carol,0,2,Bob",
	},
	"joinbody/both/group": {
		"dag":          "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,2",
		"dag-shuffled": "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,2",
		"dag-morsel4":  "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,2",
	},
	"joinbody/both/list": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
	},
	"joinbody/both/ord": {
		"dag":          "cols=[p:STRING k:INT64 k:INT64] rows=4 | Alice,1,1 | Alice,1,1 | Bob,2,2 | Bob,2,2",
		"dag-shuffled": "cols=[p:STRING k:INT64 k:INT64] rows=4 | Alice,1,1 | Alice,1,1 | Bob,2,2 | Bob,2,2",
		"dag-morsel4":  "cols=[p:STRING k:INT64 k:INT64] rows=4 | Alice,1,1 | Alice,1,1 | Bob,2,2 | Bob,2,2",
	},
	"joinbody/both/qual": {
		"dag":          "cols=[k:INT64] rows=4 | 1 | 1 | 2 | 2",
		"dag-shuffled": "cols=[k:INT64] rows=4 | 1 | 1 | 2 | 2",
		"dag-morsel4":  "cols=[k:INT64] rows=4 | 1 | 1 | 2 | 2",
	},
	"joinbody/both/star": {
		"dag":          "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-shuffled": "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
		"dag-morsel4":  "cols=[k:INT64 p:STRING k:INT64 p:STRING] rows=4 | 1,Alice,1,Alice | 1,Alice,1,Alice | 2,Bob,2,Bob | 2,Bob,2,Bob",
	},
	"joinbody/left/distinct": {
		"dag":          "cols=[k:INT64 p:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-shuffled": "cols=[k:INT64 p:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-morsel4":  "cols=[k:INT64 p:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
	},
	"joinbody/left/group": {
		"dag":          "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-shuffled": "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-morsel4":  "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,1",
	},
	"joinbody/left/list": {
		"dag":          "cols=[k:INT64 p:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-shuffled": "cols=[k:INT64 p:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-morsel4":  "cols=[k:INT64 p:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
	},
	"joinbody/left/qual": {
		"dag":          "cols=[k:INT64] rows=3 | 1 | 1 | 2",
		"dag-shuffled": "cols=[k:INT64] rows=3 | 1 | 1 | 2",
		"dag-morsel4":  "cols=[k:INT64] rows=3 | 1 | 1 | 2",
	},
	"joinbody/left/star": {
		"dag":          "cols=[k:INT64 p:STRING id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,1,Alice,150 | 1,Alice,2,Bob,200 | 2,Bob,3,Carol,0",
		"dag-shuffled": "cols=[k:INT64 p:STRING id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,1,Alice,150 | 1,Alice,2,Bob,200 | 2,Bob,3,Carol,0",
		"dag-morsel4":  "cols=[k:INT64 p:STRING id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,1,Alice,150 | 1,Alice,2,Bob,200 | 2,Bob,3,Carol,0",
	},
	"joinbody/right/distinct": {
		"dag":          "cols=[k:INT64 p:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-shuffled": "cols=[k:INT64 p:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
		"dag-morsel4":  "cols=[k:INT64 p:STRING id:INT64] rows=3 | 1,Alice,1 | 1,Alice,2 | 2,Bob,3",
	},
	"joinbody/right/group": {
		"dag":          "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-shuffled": "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,1",
		"dag-morsel4":  "cols=[k:INT64 n:INT64] rows=2 | 1,2 | 2,1",
	},
	"joinbody/right/list": {
		"dag":          "cols=[k:INT64 p:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-shuffled": "cols=[k:INT64 p:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
		"dag-morsel4":  "cols=[k:INT64 p:STRING id:INT64 customer:STRING] rows=3 | 1,Alice,1,Alice | 1,Alice,2,Bob | 2,Bob,3,Carol",
	},
	"joinbody/right/qual": {
		"dag":          "cols=[k:INT64] rows=3 | 1 | 1 | 2",
		"dag-shuffled": "cols=[k:INT64] rows=3 | 1 | 1 | 2",
		"dag-morsel4":  "cols=[k:INT64] rows=3 | 1 | 1 | 2",
	},
	"joinbody/right/star": {
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 p:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,1,Alice | 3,Carol,0,2,Bob",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 p:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,1,Alice | 3,Carol,0,2,Bob",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 p:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,1,Alice | 3,Carol,0,2,Bob",
	},

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
