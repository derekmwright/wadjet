// SPDX-License-Identifier: MIT

package pgwire

// A NUMERIC VALUE IS EXACT WHEREVER A NUMERIC OPERAND MEETS AN INTEGER OR A
// CONSTANT (#1386, #1392, #1450), as a client reads it: the declared OID, the
// text encoder over the simple and the extended protocol, and the binary
// encoder (extended, binary results). The cells are the three issues'
// operand kinds over arc SS's six-row ss_t fixture (setupSSAuditWireDB):
// an integer CAST beside a numeric (`CAST(t.i AS INTEGER) * 0.1`, `%`, past
// 2^53, and stored by INSERT … SELECT), a numeric constant a double cannot
// carry in a choice / unary minus / scalar subquery / bare NUMERIC cast and
// stored, and the explicit integer CAST of a float-carried numeric.
//
// Every want is PostgreSQL 17.11's, measured with the same client in the
// same three modes (testdata/arc_nx_numeric_carrier_wire_pg17.tsv); the
// cells in testdata/arc_nx_numeric_carrier_wire_kept.tsv are recorded
// divergences, asserted as they stand.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestArcNXNumericCarrierOnTheWire(t *testing.T) {
	srv := setupSSAuditWireDB(t)
	ctx := context.Background()
	modes := map[string]string{"s": "simple_protocol", "e": "exec", "b": "describe_exec"}
	conns := map[string]*pgx.Conn{}
	for m, qm := range modes {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=%s", srv.Addr(), qm))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		conns[m] = conn
	}
	kept := map[string][2]string{}
	for _, k := range ssAuditWireTSV(t, "testdata/arc_nx_numeric_carrier_wire_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	cells := ssAuditWireTSV(t, "testdata/arc_nx_numeric_carrier_wire_pg17.tsv", 5)
	seen := map[string]bool{}
	for _, c := range cells {
		seen[c[0]+"/"+c[1]] = true
	}
	for k := range kept {
		if !seen[k] {
			t.Fatalf("kept cell %s is not in the table", k)
		}
	}
	for _, c := range cells {
		name, mode, ordered, sql, pg := c[0], c[1], c[2] == "true", c[3], c[4]
		t.Run(name+"/"+mode, func(t *testing.T) {
			conn := conns[mode]
			var got, firstErr string
			for _, st := range strings.Split(sql, " ;; ") {
				r := ssAuditWireRun(ctx, conn, st, ordered, mode == "b")
				if strings.HasPrefix(r, "ERR") && firstErr == "" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(st)), "DROP") {
					firstErr = r
				}
				got = r
			}
			if firstErr != "" {
				got = firstErr
			}
			want, why := pg, "PostgreSQL 17.11 sends"
			if k, ok := kept[name+"/"+mode]; ok {
				want, why = k[0], "kept: "+k[1]
			}
			if got != want {
				t.Errorf("%s\n  got  %s\n  want %s (%s)", sql, got, want, why)
			}
		})
	}
}
