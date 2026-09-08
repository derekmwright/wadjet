package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// DISTINCT INSIDE A WINDOW CALL IS REFUSED, AS PostgreSQL REFUSES IT
// (#987 review, P4).
//
// PostgreSQL 17.11, measured live:
//
//	SELECT SUM(DISTINCT n) OVER () FROM (…) t
//	  ERROR:  0A000: DISTINCT is not implemented for window functions
//
// Wadjet dropped the keyword. Both places that turn the parsed call into a
// plan build the argument list from the argument NODES' text and never look at
// `FuncCallNode.Distinct`, so `SUM(DISTINCT c_proto) OVER ()` answered 621435
// — the raw total — where the GROUPED spelling of the same aggregate answers
// 32640. A plausible wrong number, on all four arms, at bb8635a4 and at every
// commit of this arc before this one.
//
// It is asserted as a SQLSTATE rather than through the census's rendered `ERR
// …` string on purpose: the four arms wrap a parse failure under different
// prefixes (`parsing SQL: parsing SQL: …` single-process, `parse: parsing SQL:
// …` on the DAG), so a prefix match would pin the WRAPPING and say nothing
// about the refusal. What a client sees is the code and the message, and those
// are what this asserts.
func TestAWindowFunctionRefusesDISTINCT(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	// The message PostgreSQL uses, verbatim; the parenthesised function name
	// is wadjet's addition and is not asserted.
	const want = "DISTINCT is not implemented for window functions"

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"sum", "SELECT SUM(DISTINCT c_proto) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1"},
		{"count", "SELECT COUNT(DISTINCT c_proto) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1"},
		{"avg", "SELECT AVG(DISTINCT c_proto) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1"},
		{"min", "SELECT MIN(DISTINCT c_proto) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1"},
		// Partitioned, framed, and nested inside a larger expression: the
		// refusal is at the one site where a call becomes a window call, so
		// every spelling reaches it.
		{"partitioned", "SELECT SUM(DISTINCT c_proto) OVER (PARTITION BY g) AS v FROM typemx ORDER BY 1 LIMIT 1"},
		{"framed", "SELECT SUM(DISTINCT c_proto) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING " +
			"AND CURRENT ROW) AS v FROM typemx ORDER BY 1 LIMIT 1"},
		{"nested in an expression", "SELECT SUM(DISTINCT c_proto) OVER () + 1 AS v FROM typemx ORDER BY 1 LIMIT 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if err == nil {
					t.Errorf("%s\n  arm %s answered %s — PostgreSQL refuses this shape 0A000, "+
						"and answering it means silently ignoring DISTINCT", tc.sql, arm.name, got)
					continue
				}
				if st := sqlerr.StateOf(err); st != "0A000" {
					t.Errorf("%s\n  arm %s: SQLSTATE %q, want 0A000: %v", tc.sql, arm.name, st, err)
				}
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s\n  arm %s: %v\n  want a message containing %q", tc.sql, arm.name, err, want)
				}
			}
		})
	}

	// The CONTROLS, on the same arms: DISTINCT outside a window still works,
	// and a window without DISTINCT still answers. Both are what says the
	// refusal is keyed on the pair and not on either keyword.
	for _, tc := range []struct{ name, sql, want string }{
		{"ctl grouped DISTINCT still answers",
			"SELECT SUM(DISTINCT c_proto) AS v FROM typemx",
			"cols=[v:INT64] rows=1 | 32640"},
		{"ctl the window without DISTINCT still answers",
			"SELECT SUM(c_proto) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1",
			"cols=[v:INT64] rows=1 | 621435"},
		{"ctl DISTINCT in the SELECT list beside a window still answers",
			"SELECT DISTINCT SUM(c_proto) OVER () AS v FROM typemx",
			"cols=[v:INT64] rows=1 | 621435"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s\n  arm %s: %v", tc.sql, arm.name, err)
				}
				if got != tc.want {
					t.Errorf("%s\n  arm %s\n  got  %s\n  want %s", tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
}
