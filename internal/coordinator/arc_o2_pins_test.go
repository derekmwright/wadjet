// SPDX-License-Identifier: AGPL-3.0-only

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
	// THE PUBLISHED NAME OF AN UNALIASED ITEM inside a block a LATERAL reads
	// is CLOSED (arc JP round 4): a star over a LATERAL join is expanded to the
	// FROM arms' own lists, the lateral's read as `s.*` reads it, so each item
	// is ADR-0026 §2's pair like the `joined/*` half arc O1 closed — it
	// resolves by the block's spelling (`i.amount + 1`) and publishes
	// PostgreSQL's `?column?`. The six `lateral/{unaliased,unaliased-string,
	// group-unaliased}/{star,ordstar}` pins that stood here agree now and are
	// deleted as the proof.

	// A CORRELATED LATERAL'S OWN BOUND IS APPLIED PER OUTER ROW since arc LT
	// (#1019, ADR-0021 §1s): the four `lateral/inner-order-*-limit/{star,list}`
	// pins that stood here — three rows for PostgreSQL's four, byte-identical
	// on five arms from base to 51addfb6 — are deleted, and the cells assert
	// PostgreSQL's row set in arc_o2_postgres_answers_test.go. The deletion is
	// the proof.

	// A NESTED BLOCK'S RENAME WAS LOST ON THE DAG — the inner block publishes
	// `product AS p`, the outer republishes `z.p`, and the three DAG arms
	// published the SOURCE name `product` — and arc O1 closed it for the star
	// that reads such a block over a JOIN: the star publishes each arm's own
	// VISIBLE list, so `p` is what the expansion spells and what the gather
	// declares (#997/#1012). The two `nested/grouped-hidden-key/*/join-star`
	// pins are deleted, which is the proof.

	// AN AGGREGATE ALIASED LIKE ITS OWN GROUP KEY'S SOURCE COLUMN is CLOSED
	// in both halves and the `joined/group-alias-src/ordkeys` pin is deleted,
	// which is the proof. The VALUE half was #1078 (arc O2); the ORDER half
	// was #1095 (arc R2): the statement's `ORDER BY x.product, o.id` over the
	// JOIN above the block sorted by the group KEY while the projection read
	// the count, because the ordering fused onto the join bound its key by
	// NAME and the aggregate publishes that name twice. It addresses the SLOT
	// its class names now — the class read through the block, the position
	// from the probe's model measured against the stage's own output stream
	// (dagplan.joinProbeAggregateSlots, ADR-0026 §8i).

	// THE COLUMN ORDER OF A STAR OVER A JOIN followed the side the planner
	// BUILDS, not the FROM clause — ADR-0026 §7's own note: which side builds
	// is a cost decision and must not decide a name or a position. Arc O1 rode
	// it (#997/#1012): the order is the LOGICAL join's, left arm then right in
	// written order, on all five arms. The four pins this comment carried —
	// `joined/group/{star,ordstar}` and the two `class/*/then-join` ANTI
	// spellings, where the decorrelated join makes the other side the build
	// even though the block is written first — are deleted, which is the proof.

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
