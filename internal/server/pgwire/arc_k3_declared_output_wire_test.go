package pgwire

// THE WIRE CARRIES THE BLOCK'S OWN PROJECTION — arc K3, #984.
//
// A client reads the WIRE. `RowDescription` is where psql gets its header,
// where pgx and pgJDBC build their column metadata and where Superset and
// DataGrip read the shape of a result, so a query that answers the right
// values under the wrong names is broken for every one of them and invisible
// to a gate that compares values (ADR-0012: the wire arm is the one a value
// oracle cannot provide).
//
// A derived block a star reads publishes its OWN projection now, so the names
// on the wire are the names the query wrote: a duplicated source column
// appears twice, a rename appears under its alias, and no `__agg_N` reaches a
// client.
//
// The ZERO-ROW half of this arc (#978) is gated where #846's is, in
// zero_row_rowdescription_test.go: a Describe of the STATEMENT is what a
// pgJDBC client sends and the only path that asks the PLAN rather than the
// executed batch.
//
// Every "PostgreSQL 17" list below is measured live over the same rows.

import (
	"context"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// pinned reports whether this field name is part of the cell's own expected
// list — the only way a reserved name is tolerated here.
func pinned(want []string, name string) bool {
	for _, w := range want {
		if w == name {
			return true
		}
	}
	return false
}

func TestArcK3TheWireDeclaresTheBlocksProjection(t *testing.T) {
	srv := setupJ1LateralDB(t)
	for _, c := range []struct {
		name, sql string
		// want is the RowDescription's field names, in order.
		want []string
		// pgSays is what PostgreSQL 17 sends when it differs — a divergence
		// this arc does not close is stated, never left to be discovered.
		pgSays string
	}{
		{"dup_source_column", `SELECT * FROM j1ord o JOIN (SELECT order_id, ` +
			`order_id AS oid FROM j1item) s ON s.order_id = o.id`,
			[]string{"order_id", "oid", "id", "customer", "total"},
			"(id, customer, total, order_id, oid) — the same five, in FROM order; " +
				"this engine publishes the join's probe side first on every arm"},
		{"derived_rename", `SELECT * FROM j1ord o JOIN (SELECT order_id AS k, ` +
			`amount FROM j1item) d ON d.k = o.id`,
			[]string{"k", "amount", "id", "customer", "total"},
			"(id, customer, total, k, amount) — same five, FROM order"},
		{"alias_over_an_aggregate", `SELECT * FROM j1ord o JOIN (SELECT order_id, ` +
			`CAST(COUNT(*) AS VARCHAR) AS n FROM j1item GROUP BY order_id) s ` +
			`ON s.order_id = o.id`,
			[]string{"id", "customer", "total", "order_id", "n"}, ""},
		{"computed_item", `SELECT * FROM j1ord o JOIN (SELECT order_id, ` +
			`amount * 2 AS d FROM j1item) s ON s.order_id = o.id`,
			[]string{"order_id", "d", "id", "customer", "total"},
			"(id, customer, total, order_id, d) — same five, FROM order"},
		{"lateral_with_a_computed_default", `SELECT * FROM j1ord o LEFT JOIN LATERAL (` +
			`SELECT COUNT(*) + 1 AS n FROM j1item WHERE order_id = o.id) s ON true`,
			[]string{"id", "customer", "total", "n"}, ""},
		{"two_laterals_publish_bare_names", `SELECT * FROM j1ord o JOIN LATERAL (` +
			`SELECT MAX(amount) AS mx FROM j1item WHERE order_id = o.id) s ON true ` +
			`JOIN LATERAL (SELECT MIN(amount) AS mn FROM j1item WHERE amount >= s.mx) s2 ` +
			`ON true`,
			[]string{"id", "customer", "total", "mx", "mn"}, ""},

		// CONTROLS — a NAMED list over each block, and a plain join's star,
		// were right before this arc and may not move.
		{"ctl_named_over_the_dup_block",
			`SELECT s.order_id AS a, s.oid AS b FROM j1ord o JOIN (SELECT order_id, ` +
				`order_id AS oid FROM j1item) s ON s.order_id = o.id`,
			[]string{"a", "b"}, ""},
		{"ctl_a_plain_join_star",
			`SELECT * FROM j1ord o JOIN j1item li ON li.order_id = o.id`,
			[]string{"id", "order_id", "product", "amount", "o.id", "customer", "total"},
			"(id, customer, total, id, order_id, product, amount) — the join's own " +
				"duplicate-name qualification, pre-existing and unrelated"},
		// A PRE-EXISTING LEAK, PINNED ON THE WIRE. A block with its own
		// `ORDER BY … LIMIT` carries a materialized `__sortkey_N`, and this
		// door — a single-process server — has always published it.
		// PostgreSQL sends five fields. Held here so the day it stops leaking
		// this cell fails; the property assertion below exempts nothing that
		// the cell's own list does not already name. The DAG's answer for the
		// same shapes is pinned per arm in
		// `coordinator.TestArcK3ADerivedBlockPublishesItsOwnProjection`.
		{"pinned_a_materialized_sort_key_reaches_the_wire",
			`SELECT * FROM j1ord o JOIN (SELECT order_id, product FROM j1item ` +
				`ORDER BY amount LIMIT 3) s ON s.order_id = o.id`,
			[]string{"id", "customer", "total", "order_id", "product", "__sortkey_0"},
			"(id, customer, total, order_id, product) — five; this engine publishes " +
				"the block's own ORDER BY term as a sixth column"},
		{"pinned_an_introducing_block_with_a_sort_key",
			`SELECT * FROM j1ord o JOIN (SELECT order_id, order_id AS oid FROM j1item ` +
				`ORDER BY amount LIMIT 3) s ON s.order_id = o.id`,
			[]string{"id", "customer", "total", "order_id", "oid", "__sortkey_0"},
			"(id, customer, total, order_id, oid) — five; the sixth is this engine's " +
				"materialized ORDER BY term, and at v0.18.60 this statement FAILED"},

		{"ctl_star_over_a_block_that_is_its_stream",
			`SELECT * FROM j1ord o JOIN (SELECT order_id FROM j1item) s ` +
				`ON s.order_id = o.id`,
			[]string{"order_id", "id", "customer", "total"}, ""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			got := make([]string, len(res.FieldDescriptions))
			for i, f := range res.FieldDescriptions {
				got[i] = f.Name
			}
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("%s\n  RowDescription %v, want %v\n  PostgreSQL 17: %s",
					c.sql, got, c.want, c.pgSays)
			}
			// The property behind every list: nothing the planner minted for
			// itself is on the wire. A name no query can spell is a name no
			// client can use.
			for _, name := range got {
				// A cell whose own expectation NAMES a reserved column is
				// pinning a pre-existing leak, not exempting one: the day it
				// stops leaking the list above changes and the cell fails.
				if pinned(c.want, name) {
					continue
				}
				if fam := plansql.ReservedSlotFamily(name); fam != "" {
					t.Errorf("%s\n  RowDescription carries %q, which is in the reserved "+
						"%s namespace and no query can spell", c.sql, name, fam)
				}
			}
		})
	}
}
