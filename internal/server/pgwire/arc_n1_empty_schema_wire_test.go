// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// AN EMPTY COLUMN LIST IS NEVER AN ANSWER — ON THE WIRE (arc N1 round 2).
//
// A statement that produces a result set sends a RowDescription with fields,
// and a client depends on it: psql prints a header, pgJDBC's `executeQuery`
// reads metadata off it. A result carrying none and no error is the engine
// failing to describe its own output, and #1008 and #1010 both reached a
// client that way.
//
// Three doors ask one decision (`sqlerr.EmptyResultColumns`, XX000) and this
// is the one that shows what CROSSES THE WIRE: the SQLSTATE a client switches
// on, and the one sentence it is given. It is also the cell that holds the
// other half — a statement with NO fields and no error, which every session
// command is, must not trip the rule. `INSERT`, `SET`, `BEGIN` and `COMMIT`
// answer zero fields on this door and are dispatched above the check, so a
// rule written one layer too high would break every one of them.
func TestN1AnEmptyColumnListIsRefusedOnTheWire(t *testing.T) {
	srv := setupJ1LateralDB(t)

	for _, c := range []struct {
		name, sql string
		// want is the RowDescription's field names, in order; wantSQLState is
		// set instead for the statement this door refuses.
		want         []string
		wantSQLState string
	}{
		{
			// A zero-row star over a LATERAL whose subquery is an UNGROUPED
			// AGGREGATE was the shape no door could declare (its join carries
			// the pad marker) and was refused XX000 here. Arc JP round 4
			// expands a star over a LATERAL join to the FROM arms' own lists,
			// the lateral's read as `s.*` reads it, so it declares PostgreSQL's
			// columns like any other star.
			name: "a_zero_row_star_over_an_ungrouped_lateral_declares",
			sql: `SELECT * FROM j1ord o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM j1item WHERE order_id = o.id) s ON true WHERE o.id > 99`,
			want: []string{"id", "customer", "total", "mx"},
		},
		{
			// THE SHAPE NO DOOR CAN DECLARE now: the same zero-row star where
			// the lateral publishes one name twice, so its list cannot be
			// enumerated by name and the star stays on the join's stream,
			// which carries the pad marker the declaration will not publish.
			name: "a_zero_row_star_over_an_ungrouped_lateral_naming_one_column_twice_is_XX000",
			sql: `SELECT * FROM j1ord o JOIN LATERAL (SELECT MAX(amount) AS mx, MIN(amount) AS mx ` +
				`FROM j1item WHERE order_id = o.id) s ON true WHERE o.id > 99`,
			wantSQLState: sqlerr.EmptyResultSQLState,
		},
		{
			// A PLAIN lateral list naming one column twice carries no pad
			// marker: the block's list is declared as written, both columns
			// (arc JP round 5, B3 — the duplicate was declared once). The
			// second is named `s.mx` where PostgreSQL says `mx`, as on the
			// non-empty result (the join qualifies a duplicate name; FC-JP-13).
			name: "a_zero_row_star_over_a_plain_lateral_naming_one_column_twice_declares_both",
			sql: `SELECT * FROM j1ord o JOIN LATERAL (SELECT amount AS mx, id AS mx ` +
				`FROM j1item WHERE order_id = o.id) s ON true WHERE o.id > 99`,
			want: []string{"id", "customer", "total", "mx", "s.mx"},
		},
		{
			// The shape no door COULD declare — a zero-row star over a BUSHY
			// join, where `starJoinDeclaredOutputSchema` declined because a
			// side contains a join of its own (#978's stated bound). Arc O1
			// declares it (#997/#1012): the star is expanded into the FROM
			// clause's arms before any declaration walk runs, per ARM rather
			// than per operator, so the depth of the join stopped mattering
			// and the ordinary projection walk answers. The refusal below is
			// unchanged for everything else.
			name: "a_zero_row_star_over_a_bushy_join_declares",
			sql: `SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id ` +
				`JOIN j1item j ON j.order_id = o.id WHERE o.id > 99`,
			want: []string{"id", "customer", "total", "id", "order_id", "product",
				"amount", "id", "order_id", "product", "amount"},
		},
		// The three zero-row shapes that DO declare, which is what says the
		// refusal is narrow rather than "no rows means no columns".
		{"a_zero_row_named_list_declares", `SELECT o.id, o.customer FROM j1ord o WHERE o.id > 99`,
			[]string{"id", "customer"}, ""},
		{"a_zero_row_star_over_one_relation_declares", `SELECT * FROM j1ord WHERE id > 99`,
			[]string{"id", "customer", "total"}, ""},
		{"a_zero_row_star_over_one_join_declares",
			`SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id WHERE o.id > 99`,
			[]string{"id", "customer", "total", "id", "order_id", "product", "amount"}, ""},
		// THE OTHER HALF. A statement that produces no result set sends no
		// fields, and the rule must not reach it.
		{"ctl_a_session_command_sends_no_fields_and_is_not_refused", `SET search_path = public`, nil, ""},
		{"ctl_BEGIN_sends_no_fields_and_is_not_refused", `BEGIN`, nil, ""},
		{"ctl_COMMIT_sends_no_fields_and_is_not_refused", `COMMIT`, nil, ""},
		{"ctl_EXPLAIN_has_its_own_column", `EXPLAIN SELECT id FROM j1ord`, []string{"plan"}, ""},
		{"ctl_SHOW_TABLES_has_its_own_column", `SHOW TABLES`, []string{"table_name"}, ""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			res := n1WireExec(t, srv.Addr(), c.sql)
			if c.wantSQLState != "" {
				if res.err == nil {
					t.Fatalf("ANSWERED where the rule refuses\n  SQL: %s", c.sql)
				}
				if !strings.Contains(res.err.Error(), "SQLSTATE "+c.wantSQLState) {
					t.Fatalf("refused with %v\n  want SQLSTATE %s on the wire\n  SQL: %s",
						res.err, c.wantSQLState, c.sql)
				}
				// The sentence, not just the class: a client is given one.
				if !strings.Contains(res.err.Error(), "the result has no columns at all") {
					t.Errorf("the refusal's sentence did not cross the wire: %v", res.err)
				}
				return
			}
			if res.err != nil {
				t.Fatalf("%v\n  SQL: %s", res.err, c.sql)
			}
			if strings.Join(res.fields, ",") != strings.Join(c.want, ",") {
				t.Errorf("%s\n  RowDescription %v, want %v", c.sql, res.fields, c.want)
			}
		})
	}
}

// n1WireResult is what one statement put on the wire: the RowDescription's
// field names and the error, if any.
type n1WireResult struct {
	fields []string
	err    error
}

// n1WireExec runs one statement through the extended protocol and reads the
// RowDescription off the READER.
//
// Not off `ResultReader.Read()`'s `Result`: pgconn fills that struct's
// FieldDescriptions from the rows it accumulated, so a ZERO-ROW result reports
// none there even when the server sent a full RowDescription — a client-side
// artefact that would make this gate assert the opposite of what crossed the
// wire. `ResultReader.FieldDescriptions()` is the description itself.
func n1WireExec(t *testing.T, addr, sql string) n1WireResult {
	t.Helper()
	conn := connectPgconn(t, addr)
	rr := conn.ExecParams(context.Background(), sql, nil, nil, nil, []int16{0})
	for rr.NextRow() {
	}
	var fields []string
	for _, f := range rr.FieldDescriptions() {
		fields = append(fields, string(f.Name))
	}
	_, err := rr.Close()
	return n1WireResult{fields: fields, err: err}
}
