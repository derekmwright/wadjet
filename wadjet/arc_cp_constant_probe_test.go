// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"
)

// The embedded arm (`DB.Query`, raw positional rows) of the constant-probe
// rule, for the two issues' own statements over `st_pair` (#1382 #1418). At
// v0.25.2 the body's scan of st_pair replayed the outer self-join's
// duplicate-scan cache, which held only the columns the join reads, so every
// matching membership below answered no rows, `99 NOT IN` over the
// NULL-free body answered no rows, the membership as a SELECT-list value
// answered NULL on every row, and the EXISTS spellings failed with
// `filter column "q.v_i64" does not exist in the input schema`. Every want is
// PostgreSQL 17.11 over the same fixture.
func TestArcCPEmbeddedConstantProbeOverAJoin(t *testing.T) {
	db := stEmbeddedDB(t)
	ctx := context.Background()
	const (
		join = "SELECT a.id, b.id FROM st_pair a JOIN st_pair b ON a.id = b.id "
		all  = "[[1 1] [2 2] [3 3] [4 4]]"
	)
	for _, c := range []struct{ name, sql, want string }{
		{"1382/cast", join + "WHERE CAST('12' AS BIGINT) IN (SELECT q.v_i64 FROM st_pair q) ORDER BY 1", all},
		{"1382/int", join + "WHERE 12 IN (SELECT q.v_i64 FROM st_pair q) ORDER BY 1", all},
		{"1382/quoted", join + "WHERE '12' IN (SELECT q.v_i64 FROM st_pair q) ORDER BY 1", all},
		{"1382/notInNull", join + "WHERE 99 NOT IN (SELECT q.v_i64 FROM st_pair q) ORDER BY 1", "[]"},
		{"1382/notIn", join + "WHERE 99 NOT IN (SELECT q.v_i64 FROM st_pair q WHERE q.v_i64 IS NOT NULL) ORDER BY 1", all},
		{"1382/on", "SELECT a.id, b.id FROM st_pair a JOIN st_pair b ON a.id = b.id AND 12 IN (SELECT q.v_i64 FROM st_pair q) ORDER BY 1", all},
		{"1382/comma", "SELECT a.id, b.id FROM st_pair a, st_pair b WHERE a.id = b.id AND 12 IN (SELECT q.v_i64 FROM st_pair q) ORDER BY 1", all},
		{"1382/selectList", "SELECT a.id, 12 IN (SELECT q.v_i64 FROM st_pair q) FROM st_pair a JOIN st_pair b ON a.id = b.id ORDER BY 1", "[[1 true] [2 true] [3 true] [4 true]]"},
		{"1418/exists", join + "WHERE EXISTS (SELECT 1 FROM st_pair q WHERE q.v_i64 = 12) ORDER BY 1", all},
		{"1418/notExists", join + "WHERE NOT EXISTS (SELECT 1 FROM st_pair q WHERE q.v_i64 = 12) ORDER BY 1", "[]"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", c.name, c.sql, err, c.want)
			continue
		}
		if got := stCells(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
}
