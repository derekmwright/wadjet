// SPDX-License-Identifier: AGPL-3.0-only

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// THE SEAM ON THE WIRE — arc WK.
//
// `coordinator.TestWKASeamConsumerBindsItsOwnOccurrence` compares the seam's
// VALUES on five arms. A value oracle cannot see a right value under a wrong
// OID or a wrong field NAME, which is the half only this door can answer
// (CLAUDE.md, the wire arm). So this table asserts, for the same consumers over
// the same producers, what `RowDescription` carries: every field's NAME and
// every field's type OID.
//
// The window's own output is the cell that matters most here: `COUNT(*) OVER
// (PARTITION BY o.id)` is `bigint` (OID 20) and `SUM(o.id) OVER ()` is
// `numeric` (OID 1700), exactly as PostgreSQL 17.11 declares them, and neither
// declaration may move when the key underneath it is re-bound. The star cells
// carry the other half: a join publishes each arm's own list, duplicate names
// kept BY POSITION and never qualified (ADR-0026 §9), so a key whose qualifier
// now survives to the operator must not surface as a qualified FIELD NAME.
//
// And the property behind every list: nothing the planner minted for itself is
// on the wire. A re-bound key is still resolved through `__winkey_N` when it
// is an expression, and a name no query can spell is a name no client can use.
func TestWKTheWireDeclaresTheSeamsOwnColumns(t *testing.T) {
	srv := setupJ1LateralDB(t)
	// The OIDs this table names: 20 bigint, 25 text, 701 float8, 1700 numeric
	// — PostgreSQL 17.11's own, for the same expressions.
	for _, c := range []struct {
		name, sql string
		want      []string // "field:oid", in order
		// refuses is the cell's disposition where this engine declines a
		// statement PostgreSQL answers. The refusal's own sentence is
		// asserted, and PostgreSQL's list is recorded beside it, so the day
		// the shape starts answering this cell fails rather than drifting.
		refuses string
		pgSays  string
	}{
		// The WINDOW's three positions over two BASE-SCAN arms — the shape
		// arc WK re-bound. The key is `o.id` and both arms publish `id`.
		{name: "winpart_base", sql: `SELECT o.id AS a, i.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n ` +
			`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"a:20", "b:20", "n:20"}},
		{name: "winorder_base", sql: `SELECT o.id AS a, i.id AS b, COUNT(*) OVER (ORDER BY o.id) AS n ` +
			`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"a:20", "b:20", "n:20"}},
		{name: "winarg_base", sql: `SELECT o.id AS a, i.id AS b, SUM(o.id) OVER () AS n ` +
			`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"a:20", "b:20", "n:1700"}},
		{name: "winarg_float_base", sql: `SELECT o.id AS a, SUM(o.total) OVER (PARTITION BY o.id) AS n ` +
			`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"a:20", "n:701"}},
		// The same key over a DERIVED arm whose duplicate name the join
		// qualifies — #975's route, which this arc leaves on its own rule.
		{name: "winpart_derived", sql: `SELECT x.id AS a, y.id AS b, COUNT(*) OVER (PARTITION BY x.id) AS n ` +
			`FROM (SELECT id, order_id FROM j1item) x JOIN (SELECT id, order_id FROM j1item) y ` +
			`ON y.id = x.id`,
			want: []string{"a:20", "b:20", "n:20"}},
		// An EXPRESSION key takes the MATERIALIZED route and its slot must not
		// reach the wire.
		{name: "winpart_expression", sql: `SELECT o.id AS a, COUNT(*) OVER (PARTITION BY o.id + 0) AS n ` +
			`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"a:20", "n:20"}},
		// A SORT key over the same contested name: the sort publishes nothing
		// of its own, so the list is the SELECT list's.
		{name: "sortkey_base", sql: `SELECT o.id AS a, i.id AS b FROM j1ord o JOIN j1item i ` +
			`ON i.order_id = o.id ORDER BY i.id DESC, a`,
			want: []string{"a:20", "b:20"}},
		// A STAR over the join: each arm's own list, duplicates kept BY
		// POSITION and NEVER qualified — a surviving key qualifier must not
		// become a field name (ADR-0026 §9).
		{name: "star_base", sql: `SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"id:20", "customer:25", "total:701", "id:20", "order_id:20",
				"product:25", "amount:701"}},
		// A star BESIDE another item over a join is REFUSED here and answered
		// by PostgreSQL. Pre-existing and not this arc's: the expansion mints
		// its projection on SHAPE alone and takes it back out when the arms
		// cannot be stated (ADR-0026 §9's `ElideUnstatedJoinStar`), and a `*`
		// standing beside an item is left for the physical planner, which has
		// no relation to expand it from. Recorded as a filing candidate, with
		// PostgreSQL's own list beside it.
		{name: "star_beside_an_item_over_a_join",
			sql: `SELECT *, COUNT(*) OVER (PARTITION BY o.id) AS n ` +
				`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			refuses: `column "*" does not exist in the input schema`,
			pgSays:  "id, customer, total, id, order_id, product, amount, n (8 fields)"},
		// CONTROLS that were right before this arc and may not move: an
		// UNCONTESTED qualified key, and a single-relation one.
		{name: "ctl_uncontested_key", sql: `SELECT o.id AS a, COUNT(*) OVER (PARTITION BY o.customer) AS n ` +
			`FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			want: []string{"a:20", "n:20"}},
		{name: "ctl_single_relation", sql: `SELECT p.id AS a, COUNT(*) OVER (PARTITION BY p.order_id) AS n ` +
			`FROM j1item p`,
			want: []string{"a:20", "n:20"}},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if c.refuses != "" {
				if res.Err == nil {
					t.Fatalf("%s\n  ANSWERED where this cell pins a refusal; PostgreSQL 17.11 "+
						"declares %s — assert that list and delete the pin", c.sql, c.pgSays)
				}
				if !strings.Contains(res.Err.Error(), c.refuses) {
					t.Errorf("%s\n  got refusal %v\n  want one containing %q", c.sql, res.Err, c.refuses)
				}
				return
			}
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			got := make([]string, len(res.FieldDescriptions))
			for i, f := range res.FieldDescriptions {
				got[i] = fmt.Sprintf("%s:%d", f.Name, f.DataTypeOID)
			}
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("%s\n  RowDescription %v\n  want           %v",
					c.sql, got, c.want)
			}
			for _, f := range res.FieldDescriptions {
				if fam := plansql.ReservedSlotFamily(f.Name); fam != "" {
					t.Errorf("%s\n  RowDescription carries %q, which is in the reserved "+
						"%s namespace and no query can spell", c.sql, f.Name, fam)
				}
			}
		})
	}
}
