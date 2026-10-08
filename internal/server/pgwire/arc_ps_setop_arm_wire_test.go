// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestArcPSComputedArmKeepsItsDisplayScaleOnTheWire: a set operation whose
// arm is a computed numeric over a NUMERIC(10,2) column prints each value at
// its own display scale on the pgwire door, as the embedded query door and
// PostgreSQL 17.11 do (ADR-0024 §11). The door once trimmed an unconstrained
// column's text a second time after the one printer had written it, so
// `2.50` reached a psql client as `2.5` on this door alone (v0.25.5
// housekeeping, seam B item 1). PostgreSQL's text was measured on 17.11 over
// the same rows.
func TestArcPSComputedArmKeepsItsDisplayScaleOnTheWire(t *testing.T) {
	ctx := context.Background()
	srv := setupSSAuditWireDB(t)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=simple_protocol", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	// un_x.v is a bare NUMERIC: the set operation's result is marked
	// unconstrained by that arm (ADR-0024 §10), which is what the door
	// used to re-trim.
	for _, ddl := range []string{"CREATE TABLE un_x (id BIGINT, v NUMERIC)", "INSERT INTO un_x VALUES (1, 1), (2, 1.5)"} {
		if _, err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	for _, c := range [][2]string{
		{"SELECT v FROM un_x UNION ALL SELECT n + 0 FROM ss_t", "1 1.5 2.25 -3.50 10.00 0.00 0.01 NULL"},
		{"SELECT v FROM un_x UNION ALL SELECT n * 2 FROM ss_t WHERE id < 3", "1 1.5 4.50 -7.00"},
		{"SELECT n + 0 FROM ss_t UNION ALL SELECT 1.5 FROM ss_i WHERE id = 1", "2.25 -3.50 10.00 0.00 0.01 NULL 1.5"},
		{"SELECT n + 0 FROM ss_t", "2.25 -3.50 10.00 0.00 0.01 NULL"},
	} {
		rows, err := conn.Query(ctx, c[0])
		if err != nil {
			t.Fatalf("%s: %v", c[0], err)
		}
		var got []string
		for rows.Next() {
			var v *string
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("%s: %v", c[0], err)
			}
			if v == nil {
				got = append(got, "NULL")
			} else {
				got = append(got, *v)
			}
		}
		rows.Close()
		if s := strings.Join(got, " "); s != c[1] {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c[0], s, c[1])
		}
	}
}
