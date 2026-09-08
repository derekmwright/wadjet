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
			// The shape no door can declare: a zero-row star over a BUSHY
			// join (`starJoinDeclaredOutputSchema` declines where a side
			// contains a join of its own, #978's stated bound).
			name: "a_zero_row_star_over_a_bushy_join_is_XX000",
			sql: `SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id ` +
				`JOIN j1item j ON j.order_id = o.id WHERE o.id > 99`,
			wantSQLState: sqlerr.EmptyResultSQLState,
		},
		// The three zero-row shapes that DO declare, which is what says the
		// refusal is narrow rather than "no rows means no columns".
		{"a_zero_row_named_list_declares", `SELECT o.id, o.customer FROM j1ord o WHERE o.id > 99`,
			[]string{"id", "customer"}, ""},
		{"a_zero_row_star_over_one_relation_declares", `SELECT * FROM j1ord WHERE id > 99`,
			[]string{"id", "customer", "total"}, ""},
		{"a_zero_row_star_over_one_join_declares",
			`SELECT * FROM j1ord o JOIN j1item i ON i.order_id = o.id WHERE o.id > 99`,
			[]string{"id", "order_id", "product", "amount", "o.id", "customer", "total"}, ""},
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
