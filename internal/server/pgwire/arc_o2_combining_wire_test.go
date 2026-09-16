// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// WHAT THE WIRE DECLARES when the consumer above a derived block that
// materialized its own ORDER BY key is a relation-combining operator OTHER
// than a written join: a SET OPERATION, or the semi/anti join a decorrelated
// `IN` / `NOT IN` / `EXISTS` builds.
//
// #991 made the block publish its visible list to a JOIN and ADR-0026 §9 gave
// the reason as a rule — "a JOIN is the one consumer that reads a side's
// STREAM" — which was false: a set-operation arm reads the same stream, so
// `__sortkey_0` reached `RowDescription` here for every operation and for the
// CTE spelling (#1075). A relation-COMBINING operator builds its output from
// its sides' streams — and the class is every WIRING of such a node, not the
// four constructors: three of these shapes build a `NodeJoin` literally and
// fill its probe child in afterwards (#1080).
//
// The door matters: this is the single-process server, where the leak was
// silent. On the distributed arms the same statement was REFUSED, because the
// extra column made the two arms disagree on count — which the coordinator's
// own `setop/*` cells hold.
func TestArcO2TheWireUnderACombiningOperator(t *testing.T) {
	srv := setupJ1LateralDB(t)
	const block = `SELECT order_id, product FROM j1item ORDER BY amount LIMIT 3`
	for _, c := range []struct {
		name, sql string
		want      []string // PostgreSQL 17.11's RowDescription
	}{
		{"union_all_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x UNION ALL SELECT id, customer FROM j1ord`,
			[]string{"order_id", "product"}},
		{"union_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x UNION SELECT id, customer FROM j1ord`,
			[]string{"order_id", "product"}},
		{"except_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x EXCEPT SELECT id, customer FROM j1ord`,
			[]string{"order_id", "product"}},
		{"intersect_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x INTERSECT SELECT order_id, product FROM j1item`,
			[]string{"order_id", "product"}},
		{"cte_sorted_block_union",
			`WITH s AS (` + block + `) SELECT * FROM s UNION ALL SELECT id, customer FROM j1ord`,
			[]string{"order_id", "product"}},
		{"both_arms_are_sorted_blocks",
			`SELECT * FROM (` + block + `) x UNION ALL ` +
				`SELECT * FROM (SELECT order_id, product FROM j1item ORDER BY id LIMIT 2) y`,
			[]string{"order_id", "product"}},
		// The CONTROL, and #991's own shape: the JOIN consumer, which has been
		// right since that fix and may not move.
		{"ctl_join_over_a_sorted_block",
			`SELECT * FROM j1ord o JOIN (` + block + `) s ON s.order_id = o.id`,
			[]string{"id", "customer", "total", "order_id", "product"}},
		// THE REST OF THE CLASS: the joins the DECORRELATION builds. `IN`,
		// `NOT IN` and `EXISTS` each construct a `NodeJoin` LITERALLY and fill
		// its probe child in afterwards, so a rule that lived in the four
		// constructors never saw them and the block's sort key reached this
		// door under every one of them (#1080).
		{"in_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x WHERE x.order_id IN (SELECT id FROM j1ord)`,
			[]string{"order_id", "product"}},
		{"exists_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x ` +
				`WHERE EXISTS (SELECT 1 FROM j1ord o WHERE o.id = x.order_id)`,
			[]string{"order_id", "product"}},
		{"not_in_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x ` +
				`WHERE x.order_id NOT IN (SELECT id FROM j1ord WHERE id > 2)`,
			[]string{"order_id", "product"}},
		{"not_exists_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x ` +
				`WHERE NOT EXISTS (SELECT 1 FROM j1ord o WHERE o.id = x.order_id AND o.id > 2)`,
			[]string{"order_id", "product"}},
		{"in_and_exists_stacked_over_a_sorted_block",
			`SELECT * FROM (` + block + `) x WHERE x.order_id IN (SELECT id FROM j1ord) ` +
				`AND EXISTS (SELECT 1 FROM j1ord o WHERE o.id = x.order_id)`,
			[]string{"order_id", "product"}},
		// The CONTROL from the other side: a set operation over a block that
		// materialized NOTHING is untouched by the re-projection.
		{"ctl_setop_over_an_unsorted_block",
			`SELECT * FROM (SELECT order_id, product FROM j1item) x ` +
				`UNION ALL SELECT id, customer FROM j1ord`,
			[]string{"order_id", "product"}},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%s\n  REFUSED: %v — PostgreSQL 17.11 answers it", c.sql, res.Err)
			}
			got := make([]string, len(res.FieldDescriptions))
			for i, f := range res.FieldDescriptions {
				got[i] = f.Name
			}
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("%s\n  RowDescription %v, want %v (PostgreSQL 17.11)", c.sql, got, c.want)
			}
			// The property behind every list: a name no query can spell is a
			// name no client can use.
			for _, name := range got {
				if fam := plansql.ReservedSlotFamily(name); fam != "" {
					t.Errorf("%s\n  RowDescription carries %q, which is in the reserved "+
						"%s namespace and no query can spell", c.sql, name, fam)
				}
			}
		})
	}
}
