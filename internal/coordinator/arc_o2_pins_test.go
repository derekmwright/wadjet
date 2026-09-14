package coordinator

// o2Pin is the RESIDUE: every cell this table measures that does not agree
// with PostgreSQL 17.11 on some arm and is not a recorded REFUSAL
// (o2Refuses), pinned per arm with the mechanism that keeps it open.
//
// EVERY ONE was re-measured at base 0193c4e9 through a source overlay of this
// same table: 26 of 29 are byte-identical there on all five arms, and the
// three that are not — `lateral/inner-order-hidden-limit/star` and the two
// `class/*/then-join` cells — changed by LOSING `__sortkey_0`, which is this
// arc closing half of each. No pin records a value this arc made worse: a
// pin that starts agreeing FAILS, and deleting it is the proof of the fix.
var o2Pin = map[string]map[string]string{
	// THE PUBLISHED NAME OF AN UNALIASED ITEM inside a block a JOIN reads.
	// PostgreSQL calls it `?column?`; wadjet publishes the spelling the block's
	// own Project emits it under — its expression text — because the star over
	// the join reads the STREAM, and the published name is applied only where
	// the block IS the statement's output projection. Closing it means the
	// block's published names travelling BESIDE its stream, addressed by
	// POSITION (`ProjectExprSpec.SourceSlot` one relation out); renaming the
	// stream itself gives two unaliased items ONE name. Values and positions
	// agree on every arm; the NAME is the divergence. Recorded in ADR-0012.
	// PRE-EXISTING: byte-identical at base 0193c4e9 on all five arms.
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

	// A CORRELATED LATERAL'S OWN BOUND IS NOT APPLIED PER OUTER ROW (#1019).
	// PostgreSQL evaluates the body once per outer row, so its `LIMIT` bounds
	// each row's own result; the decorrelation makes the body ONE relation
	// joined once and the bound applies to the whole of it — three rows for
	// PostgreSQL's four. Honouring it means the bound travelling WITH the
	// correlation key as a per-key top-N (a `ROW_NUMBER() OVER (PARTITION BY
	// <key> …)` filter in place of the LIMIT), which is ADR-0021's territory.
	//
	// IT IS PINNED AND NOT REFUSED, and round 2's refusal was wrong for a
	// measured reason: whether a bound BINDS is a property of the DATA, so a
	// plan-time refusal on the bound's EXISTENCE turned `LIMIT 10` over a body
	// that never yields ten rows for one key — right on five arms at base —
	// into an error. Only the QUALIFIED star declines now, because it is the
	// one consumer that would publish this body as a relation whose ROW COUNT
	// is not the one the query wrote (o2Refuses).
	//
	// PRE-EXISTING: byte-identical at base on all five arms, except
	// `inner-order-hidden-limit/star`, where base ALSO published `__sortkey_0`
	// — that half is closed (#991) and the row count is what is left.
	"lateral/inner-order-pub-limit/star": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey",
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
	"lateral/inner-order-hidden-limit/list": {
		"single":       "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"spilled512k":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag":          "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag-shuffled": "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
		"dag-morsel4":  "cols=[order_id:INT64 product:STRING] rows=3 | 1,Gadget | 1,Widget | 2,Widget",
	},

	// A NESTED BLOCK'S RENAME is lost on the DAG: the inner block publishes
	// `product AS p` and the outer republishes `z.p`, and the three DAG arms
	// publish the SOURCE name `product` where the single-process arms and
	// PostgreSQL publish `p`. Values, positions and row count agree; only the
	// first column's NAME differs, and only where the inner block also
	// materialized its own ORDER BY key. PRE-EXISTING: byte-identical at base
	// on all five arms, and independently with this arc's depth walk reverted.
	"nested/grouped-hidden-key/depth2/join-star": {
		"dag":          "cols=[product:STRING n:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,1,1,Alice,150 | Doohickey,1,2,Bob,200 | Doohickey,1,3,Carol,0 | Gadget,1,1,Alice,150 | Gadget,1,2,Bob,200 | Gadget,1,3,Carol,0 | Widget,2,1,Alice,150 | Widget,2,2,Bob,200 | Widget,2,3,Carol,0",
		"dag-shuffled": "cols=[product:STRING n:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,1,1,Alice,150 | Doohickey,1,2,Bob,200 | Doohickey,1,3,Carol,0 | Gadget,1,1,Alice,150 | Gadget,1,2,Bob,200 | Gadget,1,3,Carol,0 | Widget,2,1,Alice,150 | Widget,2,2,Bob,200 | Widget,2,3,Carol,0",
		"dag-morsel4":  "cols=[product:STRING n:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,1,1,Alice,150 | Doohickey,1,2,Bob,200 | Doohickey,1,3,Carol,0 | Gadget,1,1,Alice,150 | Gadget,1,2,Bob,200 | Gadget,1,3,Carol,0 | Widget,2,1,Alice,150 | Widget,2,2,Bob,200 | Widget,2,3,Carol,0",
	},
	"nested/grouped-hidden-key/depth3/join-star": {
		"dag":          "cols=[product:STRING n:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,1,1,Alice,150 | Doohickey,1,2,Bob,200 | Doohickey,1,3,Carol,0 | Gadget,1,1,Alice,150 | Gadget,1,2,Bob,200 | Gadget,1,3,Carol,0 | Widget,2,1,Alice,150 | Widget,2,2,Bob,200 | Widget,2,3,Carol,0",
		"dag-shuffled": "cols=[product:STRING n:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,1,1,Alice,150 | Doohickey,1,2,Bob,200 | Doohickey,1,3,Carol,0 | Gadget,1,1,Alice,150 | Gadget,1,2,Bob,200 | Gadget,1,3,Carol,0 | Widget,2,1,Alice,150 | Widget,2,2,Bob,200 | Widget,2,3,Carol,0",
		"dag-morsel4":  "cols=[product:STRING n:INT64 id:INT64 customer:STRING total:FLOAT64] rows=9 | Doohickey,1,1,Alice,150 | Doohickey,1,2,Bob,200 | Doohickey,1,3,Carol,0 | Gadget,1,1,Alice,150 | Gadget,1,2,Bob,200 | Gadget,1,3,Carol,0 | Widget,2,1,Alice,150 | Widget,2,2,Bob,200 | Widget,2,3,Carol,0",
	},

	// AN AGGREGATE ALIASED LIKE ITS OWN GROUP KEY'S SOURCE COLUMN — the VALUE
	// half is closed (#1078) and what remains is an ORDER on the three DAG
	// arms: the statement's `ORDER BY x.product, o.id` over the JOIN above the
	// block sorts by the group KEY's value while the projection reads the
	// count, so the key sequence comes back as two concatenated runs where
	// PostgreSQL's is non-decreasing. The binder that still reaches the key is
	// not the one #1078 moved — the sort sits above a JOIN, so the gather's
	// merge orders on what the stage emitted (ADR-0026 §8a). PRE-EXISTING:
	// byte-identical at base on all five arms.
	"joined/group-alias-src/ordkeys": {
		"dag":          "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
		"dag-shuffled": "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
		"dag-morsel4":  "cols=[product:INT64 id:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3",
	},

	// THE COLUMN ORDER OF A STAR OVER A JOIN follows the side the planner
	// BUILDS, not the FROM clause — ADR-0026 §7's own note: which side builds
	// is a cost decision and must not decide a name or a position, and it
	// rides arc O1 (#997). The `then-join` cells are the ANTI spellings, where
	// the decorrelated join makes the other side the build even though the
	// block is written first. Values and row counts agree on every arm.
	// PRE-EXISTING: byte-identical at base on all five arms.
	"joined/group/star": {
		"single":      "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
		"spilled512k": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
	},
	"joined/group/ordstar": {
		"single":      "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
		"spilled512k": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=6 | 1,Alice,150,1,2 | 1,Alice,150,2,2 | 2,Bob,200,1,2 | 2,Bob,200,2,2 | 3,Carol,0,1,2 | 3,Carol,0,2,2",
	},
	"class/not-in/then-join": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
	},
	"class/not-exists/then-join": {
		"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
		"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | 2,Bob,200,2,Widget",
	},

	// SUM OVER A `numeric(18,4)` IS DECLARED `DECIMAL(38,4)` where PostgreSQL
	// declares an UNCONSTRAINED `numeric`. The ROWS and the ORDER agree on
	// every arm, digit for digit; only the declaration differs, because a
	// 128-bit carrier has to state a precision (ADR-0024, ADR-0012). These
	// cells are here because #968 was REPORTED over this statement: they are
	// its measurement, and they say the report no longer reproduces.
	// PRE-EXISTING: byte-identical at base on all five arms.
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
