// SPDX-License-Identifier: MIT

package pgwire

// THE WIRE DECLARES A STAR JOIN'S OWN ARMS — arc O1, #997 / #1012 / #993.
//
// `SELECT *` over a join publishes every FROM arm's column list, LEFT ARM
// FIRST in the clause's written order, duplicate names kept BY POSITION and
// never qualified. The engine used to publish the join OPERATOR's stream —
// probe columns then build columns, the build's duplicates qualified by its
// alias — and which side probes is a COST decision, so the RowDescription for
// ONE statement changed with the data: `SELECT * FROM t a JOIN t b ON a.id =
// b.id` declared `b.id` with no predicate and `a.id` under `WHERE a.id < 100`,
// a predicate that changes no row.
//
// This is the door that sees it. A client keys on column LABELS — JDBC by
// label, DataGrip, Superset — so a list that moves with an estimate is a
// different relation to them, and a value oracle cannot see a right value
// under a wrong label or a wrong OID (ADR-0012).
//
// Every want below is PostgreSQL 17.11's RowDescription for the same
// statement over the same rows, measured live. `coordinator.
// TestO1AStarOverAJoinPublishesTheQueryNotThePlan` is the same rule on five
// execution arms.

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestO1TheWireDeclaresAStarJoinsOwnArms(t *testing.T) {
	srv := setupJ1LateralDB(t)
	// The two relations' own declarations, as PostgreSQL sends them:
	// bigint 20, text 25, float8 701.
	const ord = "id:20,customer:25,total:701"
	const item = "id:20,order_id:20,product:25,amount:701"

	for _, c := range []struct {
		name, sql string
		// want is the RowDescription as "name:OID" per field, in order.
		want string
		why  string
	}{
		{"inner", `SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			ord + "," + item, ""},
		{"inner_written_the_other_way", `SELECT * FROM j1item i JOIN j1ord o ON i.order_id = o.id`,
			item + "," + ord, ""},
		// THE THREE CELLS #997 WAS FILED FOR: one statement, three
		// predicates, one RowDescription. The predicates change no row.
		{"self_join_no_predicate", `SELECT * FROM j1item a JOIN j1item b ON a.id = b.id`,
			item + "," + item, ""},
		{"self_join_selective_predicate",
			`SELECT * FROM j1item a JOIN j1item b ON a.id = b.id WHERE a.id < 100`,
			item + "," + item, ""},
		{"self_join_zero_row_predicate",
			`SELECT * FROM j1item a JOIN j1item b ON a.id = b.id WHERE a.id < 0`,
			item + "," + item,
			"the ZERO-ROW declaration is the same list: a client is told about the " +
				"relation the engine would have produced (#978)"},
		{"left_join", `SELECT * FROM j1ord o LEFT JOIN j1item i ON i.order_id = o.id`,
			ord + "," + item, ""},
		{"right_join", `SELECT * FROM j1item i RIGHT JOIN j1ord o ON i.order_id = o.id`,
			item + "," + ord, ""},
		{"full_join", `SELECT * FROM j1ord o FULL JOIN j1item i ON i.order_id = o.id`,
			ord + "," + item, ""},
		{"cross_join", `SELECT * FROM j1ord o CROSS JOIN j1item i`, ord + "," + item, ""},
		{"comma_join", `SELECT * FROM j1ord o, j1item i WHERE i.order_id = o.id`,
			ord + "," + item, ""},
		{"three_way", `SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id ` +
			`JOIN j1ord o2 ON o2.id = i.order_id`, ord + "," + item + "," + ord, ""},
		{"a_star_beside_an_item", `SELECT *, o.id FROM j1ord o JOIN j1item i ON i.order_id = o.id`,
			ord + "," + item + ",id:20",
			"REFUSED before this arc — `column \"*\" does not exist in the input schema` " +
				"— because the star over a join was left unexpanded"},
		{"a_derived_block_whose_body_is_a_join",
			`SELECT * FROM (SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id) s`,
			ord + "," + item, ""},
		{"a_cte_whose_body_is_a_join",
			`WITH c AS (SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id) SELECT * FROM c`,
			ord + "," + item, ""},
		{"a_cte_as_an_arm",
			`WITH c AS (SELECT * FROM j1item) SELECT * FROM j1ord o JOIN c ON c.order_id = o.id`,
			ord + "," + item, ""},
		{"a_positional_sort_key_over_the_stars_list",
			`SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id ORDER BY 4`,
			ord + "," + item,
			"REFUSED before this arc — `ORDER BY position 4: SELECT * over this FROM " +
				"clause expands to a column list the planner cannot count` — for a " +
				"statement PostgreSQL answers"},
		{"a_written_sort_key_naming_the_second_arm",
			`SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id ORDER BY i.id`,
			ord + "," + item, ""},
		{"a_qualified_star_still_names_one_relation",
			`SELECT i.* FROM j1ord o JOIN j1item i ON i.order_id = o.id`, item, ""},

		// AN ARM WITH AN UNALIASED ITEM, on the door that sees both halves of
		// the pair: the NAME PostgreSQL publishes and the OID its type gets.
		// At 3842eaba every one of these read NULL under OID 25 (text),
		// because the item was referenced by its PUBLISHED name while the
		// producer emits it under its own expression text; a star item
		// carries BOTH names now (ADR-0026 §9's pair, `StarColumn`).
		{"an_arm_with_an_unaliased_aggregate",
			`SELECT * FROM j1ord o JOIN (SELECT order_id, COUNT(*) FROM j1item ` +
				`GROUP BY order_id) s ON s.order_id = o.id`,
			ord + ",order_id:20,count:20",
			"`count` is PostgreSQL's name for an unaliased COUNT(*), and bigint its type"},
		{"an_arm_with_an_unaliased_expression",
			`SELECT * FROM j1ord o JOIN (SELECT id AS k, amount * 2 FROM j1item) s ` +
				`ON s.k = o.id`,
			ord + ",k:20,?column?:701", ""},
		{"an_arm_with_an_unaliased_CAST",
			`SELECT * FROM j1ord o JOIN (SELECT id AS k, CAST(amount AS BIGINT) FROM j1item) s ` +
				`ON s.k = o.id`,
			ord + ",k:20,amount:20",
			"PostgreSQL names a CAST after the column it casts"},
		// The divergence note this cell carried is GONE: an integer literal
		// that fits declares int4 since #1070, which is PostgreSQL's own
		// literal rule, so the OID is the server's as well as the name.
		{"an_arm_with_an_unaliased_literal",
			`SELECT * FROM j1ord o JOIN (SELECT id AS k, 1 FROM j1item) s ON s.k = o.id`,
			ord + ",k:20,?column?:23", ""},
		{"a_CTE_arm_with_an_unaliased_aggregate",
			`WITH c AS (SELECT id AS k, SUM(amount) FROM j1item GROUP BY id) ` +
				`SELECT * FROM j1ord o JOIN c ON c.k = o.id`,
			ord + ",k:20,sum:701", ""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			// The description is read off the READER, not off `Read()`'s
			// Result: pgconn fills that struct's FieldDescriptions from the
			// rows it accumulated, so a ZERO-ROW result reports none there
			// even when the server sent a full RowDescription (the same
			// client-side artefact `n1WireExec` records).
			rr := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0})
			var got []string
			for _, f := range rr.FieldDescriptions() {
				got = append(got, fmt.Sprintf("%s:%d", f.Name, f.DataTypeOID))
			}
			for rr.NextRow() {
			}
			if _, err := rr.Close(); err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if strings.Join(got, ",") != c.want {
				why := ""
				if c.why != "" {
					why = "\n  note: " + c.why
				}
				t.Errorf("%s\n  RowDescription %s\n  PostgreSQL 17  %s%s",
					c.sql, strings.Join(got, ","), c.want, why)
			}
		})
	}
}
