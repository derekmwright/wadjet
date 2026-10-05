// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// The embedded door under an ADVANCING clock (#1566, round 2): the one
// accessor's clock moves on one second at every read, so any read but the
// statement's stamp shows — in the count and in the answer. Every statement
// below, a write included, reads the clock exactly once and answers
// PostgreSQL 17.11's value.
func TestArcSCEmbeddedAdvancingClockOneRead(t *testing.T) {
	ctx := context.Background()
	db := scOpen(t, ctx)
	scExec(t, ctx, db, "CREATE TABLE sa (id BIGINT, v BIGINT, ts TIMESTAMP)")
	scExec(t, ctx, db, "INSERT INTO sa SELECT g, g % 7, TIMESTAMP '2024-01-01 00:00:00' FROM generate_series(1, 4096) g")
	if _, err := db.Query(ctx, "CREATE OR REPLACE FUNCTION sa_u(x) AS now()"); err != nil {
		t.Fatal(err)
	}

	var reads atomic.Int64
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	defer expr.SetClockForTest(func() time.Time {
		return base.Add(time.Duration(reads.Add(1)) * time.Second)
	})()

	const k = "(CAST(CAST(extract(epoch FROM now()) AS BIGINT) % 7 AS INT) + 1)"
	const ms = "CAST(extract(epoch FROM now()) * 1000 AS BIGINT)"
	for _, c := range []struct {
		write bool
		sql   string
		want  string // PostgreSQL 17.11's answer (a write: the check query's)
		check string
	}{
		{false, "SELECT count(DISTINCT now()), count(*) FROM sa WHERE now() = CURRENT_TIMESTAMP AND CURRENT_DATE = CAST(now() AS DATE)", "1 4096", ""},
		{false, "SELECT count(*) = 4096 - " + k + " FROM (SELECT id, lag(id, " + k + ") OVER (ORDER BY id) l FROM sa) s WHERE l = id - " + k, "true", ""},
		{false, "SELECT max(n) = " + k + " FROM (SELECT ntile(" + k + ") OVER (ORDER BY id) n FROM sa) s", "true", ""},
		{false, "SELECT count(*) = 4096 * (" + k + " % 2) FROM sa TABLESAMPLE BERNOULLI (100 * (" + k + " % 2))", "true", ""},
		{false, "SELECT count(*) FROM generate_series(" + ms + ", " + ms + " + 5) g WHERE g = " + ms, "1", ""},
		{false, "SELECT count(*) FROM sa WHERE sa_u(id) = now()", "4096", ""},
		{false, "SELECT count(*) FROM sa WHERE now() = (SELECT now()) AND CAST(now() AS DATE) IN (CURRENT_DATE)", "4096", ""},
		{true, "INSERT INTO sa SELECT id + 10000, v, now() FROM sa WHERE id <= 2048", "1 2048", "SELECT count(DISTINCT ts), count(*) FROM sa WHERE id > 10000"},
		{true, "UPDATE sa SET ts = now() WHERE id > 10000", "1 2048", "SELECT count(DISTINCT ts), count(*) FROM sa WHERE id > 10000"},
		{true, "INSERT INTO sa VALUES (20001, 0, now()), (20002, 0, now()), (20003, 0, now())", "1 3", "SELECT count(DISTINCT ts), count(*) FROM sa WHERE id > 20000"},
		{true, "CREATE TABLE sb AS SELECT id, now() AS t, sa_u(id) AS u FROM sa WHERE id <= 4096", "1 4096", "SELECT count(DISTINCT t), count(*) FROM sb WHERE t = u"},
	} {
		before := reads.Load()
		var got string
		if c.write {
			if _, err := db.Execute(ctx, c.sql); err != nil {
				t.Errorf("%s: %v", c.sql, err)
				continue
			}
		} else {
			got = scQuery1(t, ctx, db, c.sql)
		}
		if n := reads.Load() - before; n != 1 {
			t.Errorf("%s read the clock %d times; a statement reads it once", c.sql, n)
		}
		if c.write {
			got = scQuery1(t, ctx, db, c.check)
		}
		if got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, strings.TrimSpace(got), c.want)
		}
	}
}
