// SPDX-License-Identifier: MIT

package pgwire

// THE WIRE DECLARES A STAR'S OWN ARMS — arc SR, over a `USING` merge, over a
// zero-row join and over a set operation.
//
// A value oracle cannot see a right value under a wrong label or a wrong OID
// (ADR-0012), and a client keys on LABELS — JDBC by label, DataGrip, Superset.
// This is the door that sees both halves of ADR-0026 §2's pair.
//
// Every want below is PostgreSQL 17.11's own RowDescription for the same
// statement over the same rows, read off the READER with pgconn against a
// live server (the log names the command). What this arc moved:
//
//   - the `using_*` cells over a SELF JOIN were REFUSED at 563aa517 (0A000,
//     "the arms' own column lists could not both be read here"), because the
//     USING merge declined whenever the two arms shared a column name outside
//     the USING list. The `ctl_on_*` cells are the same pair spelled with
//     `ON`, which already answered — which is what says the decline's premise
//     was false (#1177).
//   - the set-operation cells declared the leftmost arm's RESOLUTION spelling
//     (`total + 1`, `cast(total as varchar)`) where PostgreSQL declares
//     `?column?` and `total` (#1079).
//   - `a_reference_into_a_block_publishing_one_name_twice` ANSWERED at base
//     where PostgreSQL raises 42702 (#1094).
//
// `coordinator.TestSRAStarPublishesItsArmsOwnColumns` is the same rule on five
// execution arms; this file is the declaration, in BOTH result formats.

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestSRTheWireDeclaresAStarsOwnArms(t *testing.T) {
	srv := setupJ1LateralDB(t)

	for _, c := range []struct {
		name, sql string
		// want is the RowDescription as "name:OID" per field, in order, or
		// "ERR <SQLSTATE>" for a statement PostgreSQL refuses.
		want string
		why  string
	}{
		{"using_over_a_self_join_shares_every_tail_name",
			"SELECT * FROM j1item a JOIN j1item b USING (id)",
			"id:20,order_id:20,product:25,amount:701,order_id:20,product:25,amount:701", ""},
		{"using_left_over_a_self_join",
			"SELECT * FROM j1item a LEFT JOIN j1item b USING (id)",
			"id:20,order_id:20,product:25,amount:701,order_id:20,product:25,amount:701", ""},
		{"using_right_over_a_self_join",
			"SELECT * FROM j1item a RIGHT JOIN j1item b USING (id)",
			"id:20,order_id:20,product:25,amount:701,order_id:20,product:25,amount:701", ""},
		{"using_two_keys_over_a_self_join",
			"SELECT * FROM j1item a JOIN j1item b USING (id, order_id)",
			"id:20,order_id:20,product:25,amount:701,product:25,amount:701", ""},
		{"using_over_arms_that_share_only_the_key",
			"SELECT * FROM j1ord o JOIN j1item i USING (id)",
			"id:20,customer:25,total:701,order_id:20,product:25,amount:701", ""},
		{"using_beside_a_qualified_item",
			"SELECT *, a.amount FROM j1item a JOIN j1item b USING (id)",
			"id:20,order_id:20,product:25,amount:701,order_id:20,product:25,amount:701,amount:701", ""},
		{"using_a_qualified_star_left",
			"SELECT a.* FROM j1item a JOIN j1item b USING (id)",
			"id:20,order_id:20,product:25,amount:701", ""},
		{"using_a_qualified_star_right",
			"SELECT b.* FROM j1item a JOIN j1item b USING (id)",
			"id:20,order_id:20,product:25,amount:701", ""},
		{"using_zero_row_declares_the_same_list",
			"SELECT * FROM j1item a JOIN j1item b USING (id) WHERE a.id < 0",
			"id:20,order_id:20,product:25,amount:701,order_id:20,product:25,amount:701", ""},
		{"using_over_two_derived_arms",
			"SELECT * FROM (SELECT id, amount FROM j1item) a JOIN (SELECT id, amount FROM j1item) b USING (id)",
			"id:20,amount:701,amount:701", ""},
		{"ctl_on_over_a_self_join",
			"SELECT * FROM j1item a JOIN j1item b ON a.id = b.id",
			"id:20,order_id:20,product:25,amount:701,id:20,order_id:20,product:25,amount:701", ""},
		{"ctl_on_zero_row_over_a_self_join",
			"SELECT * FROM j1item a JOIN j1item b ON a.id = b.id WHERE a.id < 0",
			"id:20,order_id:20,product:25,amount:701,id:20,order_id:20,product:25,amount:701", ""},
		{"zero_row_three_way",
			"SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id JOIN j1ord p ON p.id = o.id WHERE o.id < 0",
			"id:20,customer:25,total:701,id:20,order_id:20,product:25,amount:701,id:20,customer:25,total:701", ""},
		{"zero_row_four_way",
			"SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id JOIN j1ord p ON p.id = o.id JOIN j1item j ON j.order_id = o.id WHERE o.id < 0",
			"id:20,customer:25,total:701,id:20,order_id:20,product:25,amount:701,id:20,customer:25,total:701,id:20,order_id:20,product:25,amount:701", ""},
		{"zero_row_mixed_outer",
			"SELECT * FROM j1ord o LEFT JOIN j1item i ON i.order_id = o.id JOIN j1ord p ON p.id = o.id WHERE o.id < 0",
			"id:20,customer:25,total:701,id:20,order_id:20,product:25,amount:701,id:20,customer:25,total:701", ""},
		{"zero_row_with_a_derived_side",
			"SELECT * FROM j1ord o JOIN (SELECT id, order_id FROM j1item) i ON i.order_id = o.id JOIN j1ord p ON p.id = o.id WHERE o.id < 0",
			"id:20,customer:25,total:701,id:20,order_id:20,id:20,customer:25,total:701", ""},
		{"a_top_level_set_operation",
			"SELECT id, total + 1 FROM j1ord UNION ALL SELECT id, total + 2 FROM j1ord",
			"id:20,?column?:701", ""},
		{"a_set_operation_in_a_cte",
			"WITH c AS (SELECT id, total + 1 FROM j1ord UNION ALL SELECT id, total + 2 FROM j1ord) SELECT * FROM c",
			"id:20,?column?:701", ""},
		{"a_qualified_star_over_a_set_operation_cte",
			"WITH c AS (SELECT id, total + 1 FROM j1ord UNION ALL SELECT id, total + 2 FROM j1ord) SELECT c.* FROM c",
			"id:20,?column?:701", ""},
		// PINNED, and the pin is the TYPE half only. The NAME is this arc's
		// and it agrees: PostgreSQL names a CAST after the column it casts,
		// so the set operation publishes `total` rather than the arm's
		// resolution spelling `cast(total as varchar)`. The OID does not: a
		// CAST to VARCHAR declares text (25) here and varchar (1043) there,
		// which is the declared-type territory of ADR-0024 and ADR-0012's
		// list, reachable by `SELECT CAST(x AS VARCHAR)` with no set
		// operation anywhere. A pin that starts agreeing FAILS.
		{"a_cast_in_a_set_operation_cte",
			"WITH c AS (SELECT id, CAST(total AS VARCHAR) FROM j1ord UNION ALL SELECT id, customer FROM j1ord) SELECT * FROM c",
			"id:20,total:25",
			"PostgreSQL 17.11 declares id:20,total:1043 — the NAME agrees, the varchar OID is ADR-0012's list"},
		{"a_set_operation_block_as_a_join_arm",
			"SELECT * FROM (SELECT id, total + 1 FROM j1ord UNION ALL SELECT id, total + 2 FROM j1ord) a JOIN j1ord b ON a.id = b.id",
			"id:20,?column?:701,id:20,customer:25,total:701", ""},
		{"a_join_of_two_blocks_with_unaliased_items",
			"SELECT * FROM (SELECT id, amount + 1 FROM j1item) a JOIN (SELECT id, amount + 2 FROM j1item) b ON b.id = a.id",
			"id:20,?column?:701,id:20,?column?:701", ""},
		{"a_reference_into_a_block_publishing_one_name_twice",
			"SELECT x.id FROM (SELECT a.id, b.id FROM j1item a JOIN j1item b ON a.id = b.id) x",
			"ERR 42702", ""},
		{"a_bare_star_over_a_block_publishing_one_name_twice",
			"SELECT * FROM (SELECT a.id, b.id FROM j1item a JOIN j1item b ON a.id = b.id) x",
			"id:20,id:20", ""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, format := range []struct {
				name string
				code int16
			}{{"text", 0}, {"binary", 1}} {
				t.Run(format.name, func(t *testing.T) {
					conn := connectPgconn(t, srv.Addr())
					// Off the READER, not off Read()'s Result: pgconn fills
					// that struct's FieldDescriptions from the rows it
					// accumulated, so a ZERO-ROW result reports none there
					// even when the server sent a full RowDescription.
					rr := conn.ExecParams(context.Background(), c.sql, nil, nil, nil,
						[]int16{format.code})
					var got []string
					for _, f := range rr.FieldDescriptions() {
						got = append(got, fmt.Sprintf("%s:%d", f.Name, f.DataTypeOID))
					}
					for rr.NextRow() {
					}
					_, err := rr.Close()
					if strings.HasPrefix(c.want, "ERR ") {
						state := strings.TrimPrefix(c.want, "ERR ")
						if err == nil {
							t.Fatalf("ANSWERED where PostgreSQL raises %s\n  SQL: %s\n  got %s",
								state, c.sql, strings.Join(got, ","))
						}
						if !strings.Contains(err.Error(), state) {
							t.Errorf("%s\n  refused with %v\n  want SQLSTATE %s", c.sql, err, state)
						}
						return
					}
					if err != nil {
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
		})
	}
}
