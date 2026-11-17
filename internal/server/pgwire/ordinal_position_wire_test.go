// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"testing"
)

// AN ORDER BY POSITION THAT NAMES NO SELECT ITEM IS 42P10 ON THE WIRE (#1700).
// It reached a client as 42601: the refusal was uncoded and took the default.
// Each statement goes through the simple protocol and through Parse+Describe;
// every want is PostgreSQL 17.11's code over the same calls (measured with
// WADJET_PG_DSN against the arc PW fixture, the table q (k, v)).
func TestOrderByPositionOutOfRangeIs42P10OnTheWire(t *testing.T) {
	ctx := context.Background()
	side := pwEngine(t, ctx)
	for _, q := range []string{
		"SELECT k, v FROM q ORDER BY 9",
		"SELECT k, v FROM q ORDER BY 0",
		"SELECT k, v FROM q ORDER BY -1",
		"SELECT * FROM (SELECT k, v FROM q ORDER BY 3) s",
		"SELECT (SELECT k FROM q ORDER BY 2 LIMIT 1)",
		"SELECT o.k, (SELECT i.v FROM q i WHERE i.k = o.k ORDER BY 2 LIMIT 1) FROM q o",
		"SELECT k FROM q UNION SELECT k FROM q ORDER BY 2",
		// Controls already coded at the base: a position past a star, and
		// GROUP BY.
		"SELECT * FROM q ORDER BY 9",
		"SELECT k, count(*) FROM q GROUP BY 9",
	} {
		_, err := side.conn.Exec(ctx, q).ReadAll()
		if got := pwErr(err); got != "ERR 42P10" {
			t.Errorf("simple %s: got %s, want ERR 42P10 (PostgreSQL 17.11)", q, got)
		}
		_, err = side.conn.Prepare(ctx, "", q, nil)
		if got := pwErr(err); got != "ERR 42P10" {
			t.Errorf("prepare %s: got %s, want ERR 42P10 (PostgreSQL 17.11)", q, got)
		}
	}
}
