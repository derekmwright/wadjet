// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// A WINDOW KEY AND ITS JOIN ARM — #1028, on FIVE ARMS, with the repair that
// was tried and MEASURED BACK OUT.
//
// `PARTITION BY o.id` over `lat_ord o JOIN lat_item i` reaches the operator as
// the BARE `id`: `bindWindowColRef` falls back to the bare name when the
// qualified spelling is not in the input's type map, and a map keyed by name
// folds two arms' `id` into one entry. The join emits one arm's duplicate bare
// and the other's qualified, so the key binds whichever arm the reorderer put
// bare — every row lands in its own partition and the window answers 1 where
// PostgreSQL 17.11 answers 2, on all five arms, in silence. `SUM(o.total) OVER
// (PARTITION BY o.id)` and `ORDER BY o.id` are the same fact through the
// window's other two positions. Seven cells below are PINNED on it.
//
// **The repair was written, measured and taken out, and that measurement is
// the finding.** `PARTITION BY o.id + 0` — one character away, and an
// EXPRESSION, so it is MATERIALIZED into a slot the projection below the
// window computes — is right, which says the loss is in the NAME. Routing a
// qualified reference the input cannot settle down that same route fixes these
// seven cells and breaks three gates that were green, and THAT is the whole of
// the deferral's reason:
//
//   - `coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975): over two
//     DERIVED arms that both publish `w`, `PARTITION BY x.w` is right as a
//     NAME — the join qualifies the build arm's copy by the alias the query
//     wrote — and MATERIALIZING it answers each row its own partition. The
//     base-table and derived-table resolutions are DIFFERENT mechanisms, and
//     narrowing the route to a base-scan arm still left
//     `TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG` (#658)
//     and `TestJ2AJoinConsumerBindsThePublishedIdentity` (#770) failing.
//
// An earlier version of this header added a second reason — that materializing
// an ORDER BY term INVERTS the window's direction, "a prerequisite either way"
// — and it is FALSE. Measured two ways by the round-2 review and re-measured
// here: `ORDER BY i.amount + 0 DESC` over the same join, an EXPRESSION and so
// already on the materialization route, answers PostgreSQL's DESCENDING
// numbering on all five arms (`orderDescExprOverJoin`, with its
// single-relation control), and re-applying the repair verbatim makes
// `orderDescOverJoin` and `orderAscOverJoin` ANSWER PostgreSQL rather than
// invert. Those two are pinned on the SAME bare-name bind as the rest of the
// seven, and the two expression cells are here so the claim cannot drift back.
//
// So the seam is one question — which arm owns a window key — answered today
// by three mechanisms that disagree (the bare-name bind, the qualified name
// arc K1 settled for derived arms, and the materialized slot), and closing it
// is one resolution for all three, not a fourth. Pinned here, with
// PostgreSQL's answer beside every cell.
//
// #1028 was filed for the DERIVED-ALIAS spelling — a window argument naming a
// computed alias published by one ARM — and that family answers on all five
// arms at this arc's base (`derivedAlias*` below); it is gated here because an
// unreproduced issue with no gate is one nobody re-checks.
//
// `windowUnderGroupKeySubset` is a third shape and a different mechanism: a
// window partitioned on a SUBSET of the GROUP BY keys below it was REFUSED on
// the three DAG arms by AssertExchangeConsistency, because EnsureDistribution
// read the aggregate's distribution before its own exchange relabelled it.
// Every want here is live PostgreSQL 17.11 over this package's lat_ord /
// lat_item rows.

func TestArcL1AWindowKeyBindsItsOwnJoinArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the window-key table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := r1Arms(t, ctx)
	for _, tc := range []l1Case{
		{"partOuterArm", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"partInnerArm", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY i.id) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"partArmsSwapped", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_item i JOIN lat_ord o ON i.order_id = o.id ORDER BY a, b"},
		{"partUncontested", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.customer) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"partBothArms", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.id, i.id) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"partExpression", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.id + 0) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"argOverArm", "SELECT o.id AS a, i.id AS b, SUM(o.total) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"orderOverArm", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (ORDER BY o.id) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"leftJoinArm", "SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o LEFT JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"threeWay", "SELECT o.id AS a, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_item j ON j.order_id = o.id ORDER BY a, n"},
		{"singleRelation", "SELECT p.id AS a, COUNT(*) OVER (PARTITION BY p.order_id) AS n FROM lat_item p ORDER BY a"},
		{"derivedAliasArm", "SELECT SUM(x.v) OVER () AS s FROM (SELECT id, id * 2 AS v FROM lat_ord) x JOIN lat_ord y ON x.id = y.id ORDER BY s"},
		{"derivedAliasPart", "SELECT SUM(y.total) OVER (PARTITION BY x.v) AS s FROM (SELECT id, id * 2 AS v FROM lat_ord) x JOIN lat_ord y ON x.id = y.id ORDER BY s"},
		{"derivedAliasBuild", "SELECT SUM(y.total) OVER (PARTITION BY x.v) AS s FROM lat_ord y JOIN (SELECT id, id * 2 AS v FROM lat_ord) x ON x.id = y.id ORDER BY s"},
		{"derivedAliasLeft", "SELECT SUM(y.total) OVER (PARTITION BY x.v) AS s FROM (SELECT id, id * 2 AS v FROM lat_ord) x LEFT JOIN lat_ord y ON x.id = y.id ORDER BY s"},
		{"derivedAliasArg", "SELECT SUM(x.v * 2) OVER () AS s FROM (SELECT id, id * 2 AS v FROM lat_ord) x JOIN lat_ord y ON x.id = y.id ORDER BY s"},
		{"derivedAliasNested", "SELECT SUM(x.v) OVER () + 1 AS s FROM (SELECT id, id * 2 AS v FROM lat_ord) x JOIN lat_ord y ON x.id = y.id ORDER BY s"},
		{"windowUnderGroupKeySubset", "SELECT i.order_id AS g, i.product AS p, SUM(i.amount) AS m, ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.product) AS rn FROM lat_item i GROUP BY i.order_id, i.product ORDER BY g, p"},
		{"orderDescExprOverJoin", "SELECT i.id AS a, ROW_NUMBER() OVER (ORDER BY i.amount + 0 DESC) AS rn FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a"},
		{"orderDescSingleRel", "SELECT i.id AS a, ROW_NUMBER() OVER (ORDER BY i.amount DESC) AS rn FROM lat_item i ORDER BY a"},
		{"orderDescOverJoin", "SELECT o.id AS a, i.id AS b, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY i.amount DESC) AS rn FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"orderAscOverJoin", "SELECT o.id AS a, i.id AS b, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY i.amount) AS rn FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
		{"orderDescNoPartition", "SELECT o.id AS a, i.id AS b, ROW_NUMBER() OVER (ORDER BY i.amount DESC) AS rn FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY a, b"},
	} {
		want := l1WindowKeyPostgres[tc.name]
		if want == "" {
			t.Fatalf("%s: no PostgreSQL row set recorded", tc.name)
		}
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(tc.sql)
				if pin, pinned := l1WindowKeyPins[tc.name]; pinned {
					if got == want {
						t.Errorf("%s\n  arm  %s\n  the pinned divergence is GONE and the cell "+
							"answers PostgreSQL's %s: delete this cell's pin", tc.sql, arm.name, want)
						continue
					}
					if got != pin {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pinned %s (PostgreSQL answers %s)",
							tc.sql, arm.name, got, pin, want)
					}
					continue
				}
				if got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						tc.sql, arm.name, got, want)
				}
			}
		})
	}
}

// l1WindowKeyPins are the seven cells a window key's bare-name bind gets
// wrong, with the mechanism in the header above. A pin that starts agreeing
// FAILS.
var l1WindowKeyPins = map[string]string{
	"partOuterArm":      "rows=4 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
	"partArmsSwapped":   "rows=4 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
	"argOverArm":        "rows=4 1,1,150 | 1,2,150 | 2,3,200 | 2,4,200",
	"orderOverArm":      "rows=4 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
	"threeWay":          "rows=8 1,2 | 1,2 | 1,2 | 1,2 | 2,2 | 2,2 | 2,2 | 2,2",
	"orderDescOverJoin": "rows=4 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
	"orderAscOverJoin":  "rows=4 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
}

var l1WindowKeyPostgres = map[string]string{
	"orderDescExprOverJoin":     "rows=4 1,4 | 2,2 | 3,3 | 4,1",
	"orderDescSingleRel":        "rows=4 1,4 | 2,2 | 3,3 | 4,1",
	"partOuterArm":              "rows=4 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
	"partInnerArm":              "rows=4 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
	"partArmsSwapped":           "rows=4 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
	"partUncontested":           "rows=4 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
	"partBothArms":              "rows=4 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
	"partExpression":            "rows=4 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
	"argOverArm":                "rows=4 1,1,300 | 1,2,300 | 2,3,400 | 2,4,400",
	"orderOverArm":              "rows=4 1,1,2 | 1,2,2 | 2,3,4 | 2,4,4",
	"leftJoinArm":               "rows=5 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2 | 3,NULL,1",
	"threeWay":                  "rows=8 1,4 | 1,4 | 1,4 | 1,4 | 2,4 | 2,4 | 2,4 | 2,4",
	"singleRelation":            "rows=4 1,2 | 2,2 | 3,2 | 4,2",
	"derivedAliasArm":           "rows=3 12 | 12 | 12",
	"derivedAliasPart":          "rows=3 0 | 150 | 200",
	"derivedAliasBuild":         "rows=3 0 | 150 | 200",
	"derivedAliasLeft":          "rows=3 0 | 150 | 200",
	"derivedAliasArg":           "rows=3 24 | 24 | 24",
	"derivedAliasNested":        "rows=3 13 | 13 | 13",
	"windowUnderGroupKeySubset": "rows=4 1,Gadget,100,1 | 1,Widget,50,2 | 2,Doohickey,125,1 | 2,Widget,75,2",
	"orderDescOverJoin":         "rows=4 1,1,2 | 1,2,1 | 2,3,2 | 2,4,1",
	"orderAscOverJoin":          "rows=4 1,1,1 | 1,2,2 | 2,3,1 | 2,4,2",
	"orderDescNoPartition":      "rows=4 1,1,4 | 1,2,2 | 2,3,3 | 2,4,1",
}
