package coordinator

// o2Pin is the RESIDUE: every cell this table measures that does not agree
// with PostgreSQL 17.11 on some arm, pinned per arm with the mechanism that
// keeps it open. A pin that starts agreeing FAILS, and deleting it is the
// proof of the fix. Nothing is exempted: a position with no pin and no
// PostgreSQL answer cannot be reached at all.
var o2Pin = map[string]map[string]string{
	// THE PUBLISHED NAME OF AN UNALIASED ITEM inside a block a JOIN reads.
	// PostgreSQL calls it `?column?`; wadjet publishes the spelling the block's
	// own Project emits it under — its expression text — because the star over
	// the join reads the STREAM, and the published name is applied only where
	// the block IS the statement's output projection.
	//
	// NOT FIXED HERE, and the mechanism is why: making the block's stream carry
	// `?column?` gives two unaliased items ONE stream name, and every by-name
	// lookup between the block and the client — the join's own output naming, a
	// shuffle key, a gather rename — then reads the first of them. Closing it
	// means the block's published names travelling BESIDE its stream, addressed
	// by POSITION, which is `ProjectExprSpec.SourceSlot` one relation out.
	// Values and positions agree on every arm; the NAME is the divergence.
	"lateral/unaliased/star": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
	},
	"lateral/unaliased/ordstar": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.amount + 1:FLOAT64] rows=4 | 1,Alice,150,1,101 | 1,Alice,150,1,51 | 2,Bob,200,2,126 | 2,Bob,200,2,76",
	},
	"lateral/unaliased-string/star": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
	},
	"lateral/unaliased-string/ordstar": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 i.product || 'y':STRING] rows=4 | 1,Alice,150,1,Gadgety | 1,Alice,150,1,Widgety | 2,Bob,200,2,Doohickeyy | 2,Bob,200,2,Widgety",
	},
	"lateral/group-unaliased/star": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
	},
	"lateral/group-unaliased/ordstar": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 product:STRING count(*) + 1:INT64] rows=4 | 1,Alice,150,Gadget,2 | 1,Alice,150,Widget,2 | 2,Bob,200,Doohickey,2 | 2,Bob,200,Widget,2",
	},
	"joined/unaliased/star": {
		"single":       "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"spilled512k":  "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"dag":          "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"dag-shuffled": "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"dag-morsel4":  "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
	},
	"joined/unaliased/ordstar": {
		"single":       "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"spilled512k":  "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"dag":          "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"dag-shuffled": "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
		"dag-morsel4":  "cols=[order_id:INT64 amount + 1:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,101,1,Alice,150 | 1,101,2,Bob,200 | 1,101,3,Carol,0 | 1,51,1,Alice,150 | 1,51,2,Bob,200 | 1,51,3,Carol,0 | 2,126,1,Alice,150 | 2,126,2,Bob,200 | 2,126,3,Carol,0 | 2,76,1,Alice,150 | 2,76,2,Bob,200 | 2,76,3,Carol,0",
	},
	"joined/unaliased-string/star": {
		"single":       "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"spilled512k":  "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"dag":          "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"dag-shuffled": "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"dag-morsel4":  "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
	},
	"joined/unaliased-string/ordstar": {
		"single":       "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"spilled512k":  "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"dag":          "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"dag-shuffled": "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
		"dag-morsel4":  "cols=[order_id:INT64 product || 'y':STRING id:INT64 customer:STRING total:FLOAT64] rows=12 | 1,Gadgety,1,Alice,150 | 1,Gadgety,2,Bob,200 | 1,Gadgety,3,Carol,0 | 1,Widgety,1,Alice,150 | 1,Widgety,2,Bob,200 | 1,Widgety,3,Carol,0 | 2,Doohickeyy,1,Alice,150 | 2,Doohickeyy,2,Bob,200 | 2,Doohickeyy,3,Carol,0 | 2,Widgety,1,Alice,150 | 2,Widgety,2,Bob,200 | 2,Widgety,3,Carol,0",
	},
	"joined/group-unaliased/star": {
		"single":       "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"spilled512k":  "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"dag":          "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"dag-shuffled": "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"dag-morsel4":  "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
	},
	"joined/group-unaliased/ordstar": {
		"single":       "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"spilled512k":  "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"dag":          "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"dag-shuffled": "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
		"dag-morsel4":  "cols=[product:STRING count(*) + 1:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,2,1,Alice,150 | Doohickey,2,2,Bob,200 | Doohickey,2,3,Carol,0 | Gadget,2,1,Alice,150 | Gadget,2,2,Bob,200 | Gadget,2,3,Carol,0 | Widget,3,1,Alice,150 | Widget,3,2,Bob,200 | Widget,3,3,Carol,0",
	},

	// A LATERAL'S OWN `LIMIT` IS NOT PER OUTER ROW. PostgreSQL evaluates a
	// LATERAL once per outer row, so its LIMIT bounds each evaluation; the
	// decorrelation makes the body ONE relation joined once, and the bound
	// applies to the whole of it. Pre-existing, on every arm, DEFERRED with its
	// mechanism by arc N1 (`1008 boundary: a grouped lateral's own LIMIT is not
	// per outer row`): the bound has to travel with the correlation key as a
	// per-key top-N, which is ADR-0021's territory.
	"lateral/inner-order-pub-limit/star": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
	},
	"lateral/inner-order-pub-limit/qstar": {
		"single":       "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"spilled512k":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"dag":          "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"dag-shuffled": "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"dag-morsel4":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
	},
	"lateral/inner-order-pub-limit/list": {
		"single":       "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"spilled512k":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"dag":          "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"dag-shuffled": "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
		"dag-morsel4":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Doohickey",
	},
	"lateral/inner-order-hidden-limit/star": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
	},
	"lateral/inner-order-hidden-limit/qstar": {
		"single":       "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"spilled512k":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag":          "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag-shuffled": "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag-morsel4":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
	},
	"lateral/inner-order-hidden-limit/list": {
		"single":       "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"spilled512k":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag":          "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag-shuffled": "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag-morsel4":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
	},

	// AN AGGREGATE ALIASED LIKE ITS OWN GROUP KEY'S SOURCE COLUMN, on the DAG
	// arms. `SELECT COUNT(*) AS product … GROUP BY i.product` makes the
	// aggregate publish TWO columns called `product`: the key, whose qualifier
	// `exec.PublishedGroupKeyNames` strips because no other KEY collides with
	// it, and the aggregate's own output. Every consumer above resolves the name
	// through `batch.RecordBatch.ColumnIndex`, which answers the FIRST — the
	// key — so the gather's rename reads product NAMES where PostgreSQL and the
	// single-process arms answer a count.
	//
	// PRE-EXISTING: identical at v0.19.0 before this arc's first commit, and
	// this arc neither creates nor closes it. DEFERRED with the mechanism: the
	// ambiguity rule that keeps a qualifier counts only KEYS, and it has to
	// count the aggregate's OUTPUT names too — one rule, read by
	// `exec.PublishedGroupKeyNames`, mirrored at seven `stageEmittedKeyNames`
	// call sites and in the worker's fragment plan, so the two engines cannot
	// drift (ADR-0026 §2b). Recorded as a filing candidate in
	// o2_landing_notes.md.
	"lateral/group-alias-src/qstar": {
		"dag":          "cols=[product:STRING] rows=4 | Doohickey | Gadget | Widget | Widget",
		"dag-shuffled": "cols=[product:STRING] rows=4 | Doohickey | Gadget | Widget | Widget",
		"dag-morsel4":  "cols=[product:STRING] rows=4 | Doohickey | Gadget | Widget | Widget",
	},
	"lateral/group-alias-src/list": {
		"dag":          "cols=[product:STRING] rows=4 | Doohickey | Gadget | Widget | Widget",
		"dag-shuffled": "cols=[product:STRING] rows=4 | Doohickey | Gadget | Widget | Widget",
		"dag-morsel4":  "cols=[product:STRING] rows=4 | Doohickey | Gadget | Widget | Widget",
	},
	"lateral/group-alias-src/ordkeys": {
		"dag":          "cols=[product:STRING id:INT64] rows=4 | Doohickey,2 | Gadget,1 | Widget,1 | Widget,2",
		"dag-shuffled": "cols=[product:STRING id:INT64] rows=4 | Doohickey,2 | Gadget,1 | Widget,1 | Widget,2",
		"dag-morsel4":  "cols=[product:STRING id:INT64] rows=4 | Doohickey,2 | Gadget,1 | Widget,1 | Widget,2",
	},
	"joined/group-alias-src/ordkeys": {
		"dag":          "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
		"dag-shuffled": "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
		"dag-morsel4":  "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
	},
	"issue/968-lateral-alias-equals-the-key-source": {
		"dag":          "cols=[id:INT64 product:STRING] rows=4 | 1,Gadget | 1,Widget | 2,Doohickey | 2,Widget",
		"dag-shuffled": "cols=[id:INT64 product:STRING] rows=4 | 1,Gadget | 1,Widget | 2,Doohickey | 2,Widget",
		"dag-morsel4":  "cols=[id:INT64 product:STRING] rows=4 | 1,Gadget | 1,Widget | 2,Doohickey | 2,Widget",
	},

	// THE COLUMN ORDER OF A STAR OVER A JOIN follows the side the planner
	// BUILDS, not the FROM clause. Here the single-process arms put the outer
	// relation first and the DAG arms put the block first, which is
	// PostgreSQL's order for this spelling. ADR-0026 §7's own note: which side
	// builds is a cost decision and must not decide a name or a position — it
	// rides arc O1 (#997), whose lane `exec.JoinOutputSchema` is.
	"joined/group/star": {
		"single":      "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
		"spilled512k": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
	},
	"joined/group/ordstar": {
		"single":      "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
		"spilled512k": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
	},

	// SUM OVER A `numeric(18,4)` IS DECLARED `DECIMAL(38,4)` where PostgreSQL
	// declares an UNCONSTRAINED `numeric`. The ROWS and the ORDER agree on every
	// arm, digit for digit; only the declaration differs, and it differs because
	// a 128-bit carrier has to state a precision (ADR-0024). ADR-0012 records
	// it. These cells are here because #968 was REPORTED over this statement:
	// they are its measurement, and they say the report no longer reproduces.
	"outputslot/968-alias-swap-order-limit": {
		"single":       "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"spilled512k":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag":          "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag-shuffled": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag-morsel4":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
	},
	"outputslot/968-alias-swap-order": {
		"single":       "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
		"spilled512k":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
		"dag":          "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
		"dag-shuffled": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
		"dag-morsel4":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
	},
	"outputslot/968-alias-swap-no-order": {
		"single":       "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 12.75,38.2500 | 2.00,10.0000 | NULL,1.0000",
		"spilled512k":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 12.75,38.2500 | 2.00,10.0000 | NULL,1.0000",
		"dag":          "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 12.75,38.2500 | 2.00,10.0000 | NULL,1.0000",
		"dag-shuffled": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 12.75,38.2500 | 2.00,10.0000 | NULL,1.0000",
		"dag-morsel4":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 12.75,38.2500 | 2.00,10.0000 | NULL,1.0000",
	},
	"outputslot/968-order-by-the-other": {
		"single":       "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
		"spilled512k":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
		"dag":          "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
		"dag-shuffled": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
		"dag-morsel4":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
	},
	"outputslot/968-ctl-no-collision": {
		"single":       "cols=[ka:DECIMAL(9,2) sb:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"spilled512k":  "cols=[ka:DECIMAL(9,2) sb:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag":          "cols=[ka:DECIMAL(9,2) sb:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag-shuffled": "cols=[ka:DECIMAL(9,2) sb:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag-morsel4":  "cols=[ka:DECIMAL(9,2) sb:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
	},
	"outputslot/968-ordinal": {
		"single":       "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"spilled512k":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag":          "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag-shuffled": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		"dag-morsel4":  "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)] rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
	},
}
