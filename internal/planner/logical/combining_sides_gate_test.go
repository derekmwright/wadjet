// SPDX-License-Identifier: MIT

package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// EVERY SIDE OF A RELATION-COMBINING NODE GOES THROUGH ONE DOOR.
//
// §9's rule is that a relation-combining operator builds its output from its
// sides' STREAMS, so each side publishes its VISIBLE list there. Round 2 put
// the call in the four constructors and claimed they were the whole class,
// because there is no fifth binary node TYPE. That was true of the type and
// false of the wiring: `IN`, `NOT IN`, `EXISTS` and a correlated scalar
// subquery each build a `NodeJoin` LITERALLY and fill the probe child in
// afterwards, so the constructor never saw the side and a sorted derived block
// under any of them still published `__sortkey_0` on five arms and on the wire
// (#1080).
//
// A source-text gate would have to decide which `Children[i] =` targets a
// combining node, which it cannot. This asserts the PROPERTY instead, over
// built plans: for every join and set-operation node in the tree, applying the
// rule to a side must be a NO-OP — `dropBlockHiddenSlots(side) == side`,
// pointer-identical — because the door already applied it. A new construction
// site that skips `setCombinedChild` fails here, whatever it is called and
// wherever it lives.
//
// The corpus is every way this planner builds one: the four written binary
// nodes, the four decorrelations, a lateral, and the nesting that puts one
// above another.
func TestEveryRelationCombiningSideIsWiredThroughOneDoor(t *testing.T) {
	const blk = "SELECT order_id, product FROM lat_item ORDER BY amount LIMIT 3"
	for _, sql := range []string{
		// Written binary nodes.
		"SELECT * FROM lat_ord o JOIN (" + blk + ") s ON s.order_id = o.id",
		"SELECT * FROM (" + blk + ") x UNION ALL SELECT id, customer FROM lat_ord",
		"SELECT * FROM (" + blk + ") x UNION SELECT id, customer FROM lat_ord",
		"SELECT * FROM (" + blk + ") x INTERSECT SELECT order_id, product FROM lat_item",
		"SELECT * FROM (" + blk + ") x EXCEPT SELECT id, customer FROM lat_ord",
		// The decorrelations, which build a NodeJoin literally.
		"SELECT * FROM (" + blk + ") x WHERE x.order_id IN (SELECT id FROM lat_ord)",
		"SELECT * FROM (" + blk + ") x WHERE x.order_id NOT IN (SELECT id FROM lat_ord)",
		"SELECT * FROM (" + blk + ") x WHERE EXISTS (SELECT 1 FROM lat_ord o WHERE o.id = x.order_id)",
		"SELECT * FROM (" + blk + ") x WHERE NOT EXISTS (SELECT 1 FROM lat_ord o WHERE o.id = x.order_id)",
		"SELECT * FROM (" + blk + ") x WHERE x.order_id > (SELECT MIN(i.order_id) FROM lat_item i WHERE i.product = x.product)",
		// A LATERAL, whose join the lowering builds, and the nestings that put
		// one combining node above another.
		"SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 3) s ON true",
		"SELECT * FROM (SELECT * FROM (" + blk + ") x WHERE x.order_id IN (SELECT id FROM lat_ord)) y JOIN lat_ord o ON o.id = y.order_id",
		"SELECT * FROM (" + blk + ") x WHERE x.order_id IN (SELECT id FROM lat_ord) UNION ALL SELECT id, customer FROM lat_ord",
		// CONTROLS: the same shapes over a block that materialized nothing,
		// so a pass that wired every side would still be exercised.
		"SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item) s ON s.order_id = o.id",
		"SELECT * FROM (SELECT order_id, product FROM lat_item) x WHERE x.order_id IN (SELECT id FROM lat_ord)",
	} {
		sql := sql
		t.Run(strings.Join(strings.Fields(sql)[:6], "_"), func(t *testing.T) {
			parsed, err := plansql.Parse(sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			plan, err := BuildFromSelect(parsed.SelectInfo)
			if err != nil {
				// A shape this planner refuses builds no plan to walk, and a
				// refusal is not what this gate is about.
				t.Skipf("refused: %v", err)
			}
			// THROUGH THE OPTIMIZER, because three of the four literal
			// constructions are decorrelation rules and none of them runs in
			// the builder: a gate over the built plan alone would have passed
			// with #1080 unfixed.
			plan = Optimize(plan)
			var walk func(*Node)
			walk = func(n *Node) {
				if n == nil {
					return
				}
				switch n.Type {
				case NodeJoin, NodeUnion, NodeIntersect, NodeExcept:
					for i, side := range n.Children {
						if side == nil {
							continue
						}
						if dropBlockHiddenSlots(side) != side {
							t.Errorf("%v side %d was wired WITHOUT the door: it still carries a "+
								"key its own sort materialized, so a star above this node would "+
								"publish a reserved name (ADR-0026 §9). Wire it with "+
								"setCombinedChild.\n  SQL: %s", n.Type, i, sql)
						}
					}
				}
				for _, c := range n.Children {
					walk(c)
				}
			}
			walk(plan)
		})
	}
}
