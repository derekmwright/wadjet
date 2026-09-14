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

// r2Refuse is the second half of the residue: a cell an arm REFUSES, recorded
// per arm with the STABLE part of the message, because the text carries a
// query id and a file name that differ on every run.
//
// A refusal is a disposition and not a value: the query is right, the
// single-process arms and PostgreSQL 17.11 answer it, and the distributed arms
// say — loudly, at the shuffle — that one stage's files do not describe one
// relation (ADR-0010). Both shapes below are IDENTICAL at base `2d819c95`, so
// neither is this arc's doing; they are here because ADR-0026 §8i item 5
// states a rule and these are the two shapes measured not to honour it, and a
// rule with an unmeasured neighbour is how a class hides (arc R2 round 2's
// closure review, N2 and N3).
//
// A cell that starts ANSWERING fails the gate: it then needs PostgreSQL's
// answer, not a refusal.
var r2Refuse = map[string]map[string]string{
	// N2 — A SET OPERATION whose arms are FILTERED, so one shuffle partition
	// of the operation's own output is empty and another is not. The stage's
	// files then disagree about a column's NAME, about the WIDTH of a star,
	// and the DISTINCT spelling's GROUP BY key resolves against an input that
	// no longer carries it. The 36-cell outer dimension carries a set-op arm
	// too, but only with a FULL build, so the empty-partition condition is
	// never reached there.
	"emptybuild/setop/left/list": {
		"dag":          `names column 1 "s.id" where an earlier file of the same stage input named it "k"`,
		"dag-shuffled": `names column 1 "s.id" where an earlier file of the same stage input named it "k"`,
		"dag-morsel4":  `names column 1 "s.id" where an earlier file of the same stage input named it "k"`,
	},
	"emptybuild/setop/full/list": {
		"dag":          `names column 1 "s.id" where an earlier file of the same stage input named it "k"`,
		"dag-shuffled": `names column 1 "s.id" where an earlier file of the same stage input named it "k"`,
		"dag-morsel4":  `names column 1 "s.id" where an earlier file of the same stage input named it "k"`,
	},
	"emptybuild/setop/right/list": {
		"dag":          `names column 0 "id" where an earlier file of the same stage input named it "k"`,
		"dag-shuffled": `names column 0 "id" where an earlier file of the same stage input named it "k"`,
		"dag-morsel4":  `names column 0 "id" where an earlier file of the same stage input named it "k"`,
	},
	"emptybuild/setop/left/star": {
		"dag":          "declares 9 columns where an earlier file of the same stage input declared 5",
		"dag-shuffled": "declares 9 columns where an earlier file of the same stage input declared 5",
		"dag-morsel4":  "declares 9 columns where an earlier file of the same stage input declared 5",
	},
	"emptybuild/setop/right/star": {
		"dag":          "declares 9 columns where an earlier file of the same stage input declared 5",
		"dag-shuffled": "declares 9 columns where an earlier file of the same stage input declared 5",
		"dag-morsel4":  "declares 9 columns where an earlier file of the same stage input declared 5",
	},
	"emptybuild/setop/full/star": {
		"dag":          "declares 9 columns where an earlier file of the same stage input declared 5",
		"dag-shuffled": "declares 9 columns where an earlier file of the same stage input declared 5",
		"dag-morsel4":  "declares 9 columns where an earlier file of the same stage input declared 5",
	},
	"emptybuild/setop/left/distinct": {
		"dag":          `GROUP BY key "s.k" is not a column of its input`,
		"dag-shuffled": `GROUP BY key "s.k" is not a column of its input`,
		"dag-morsel4":  `GROUP BY key "s.k" is not a column of its input`,
	},
	"emptybuild/setop/right/distinct": {
		"dag":          `GROUP BY key "s.k" is not a column of its input`,
		"dag-shuffled": `GROUP BY key "s.k" is not a column of its input`,
		"dag-morsel4":  `GROUP BY key "s.k" is not a column of its input`,
	},
	"emptybuild/setop/full/distinct": {
		"dag":          `GROUP BY key "s.k" is not a column of its input`,
		"dag-shuffled": `GROUP BY key "s.k" is not a column of its input`,
		"dag-morsel4":  `GROUP BY key "s.k" is not a column of its input`,
	},

	// N3 — a block whose body is a CO-PATHING SELF-JOIN under an outer join:
	// `markCoPathingSelfJoinBuilds` qualifies every build column of BOTH
	// joins, over the finished stage list, and the declared side schema is
	// then one column narrower than the file its siblings write. The
	// dag-shuffled arm answers PostgreSQL's rows — round 2 improved it there
	// and did not close it — and the RIGHT spelling, where the block is the
	// preserved side, answers on every arm.
	"emptybuild/selfjoin/left/list": {
		"dag":         "declares 5 columns where an earlier file of the same stage input declared 4",
		"dag-morsel4": "declares 5 columns where an earlier file of the same stage input declared 4",
	},
	"emptybuild/selfjoin/full/list": {
		"dag":         "declares 5 columns where an earlier file of the same stage input declared 4",
		"dag-morsel4": "declares 5 columns where an earlier file of the same stage input declared 4",
	},
}
