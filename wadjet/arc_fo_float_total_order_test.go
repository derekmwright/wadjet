// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"
)

// The embedded arm of arc FO (#1488, #1489): `DB.Query` over a DataDir-backed
// database, unbudgeted and under a 512 KiB budget. PostgreSQL 17.11 orders
// NaN above every value, Infinity included (max over {Infinity, 1.5, NaN,
// -Infinity} is NaN), and publishes a group's key as a member's own value (a
// group whose only member is -0 publishes -0). Go renders the float cells:
// -0 prints "-0", Infinity "+Inf".
func TestArcFOEmbeddedFloatTotalOrder(t *testing.T) {
	ctx := context.Background()
	for _, arm := range []struct {
		name   string
		budget int64
	}{{"unbudgeted", 0}, {"budget512k", 512 * 1024}} {
		cfg := Config{DataDir: t.TempDir(), MemoryBudget: arm.budget}
		if arm.budget > 0 {
			cfg.SpillDir = t.TempDir()
		}
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, ty := range []struct{ key, spell string }{{"f8", "DOUBLE PRECISION"}, {"f4", "REAL"}} {
			tbl := "fo_" + ty.key
			for _, sql := range []string{
				"CREATE TABLE " + tbl + " (id BIGINT, c " + ty.spell + ")",
				"INSERT INTO " + tbl + " VALUES (1, CAST('Infinity' AS " + ty.spell + ")), (2, 1.5), (3, CAST('-0' AS " + ty.spell + ")), " +
					"(4, CAST('NaN' AS " + ty.spell + ")), (5, CAST('-Infinity' AS " + ty.spell + ")), (6, -CAST('NaN' AS " + ty.spell + ")), (7, NULL)",
			} {
				if _, err := db.Query(ctx, sql); err != nil {
					t.Fatalf("%s: %s: %v", arm.name, sql, err)
				}
			}
			cases := []struct{ name, sql, want string }{
				{"minMax", "SELECT min(c) AS lo, max(c) AS hi FROM " + tbl, "[[-Inf NaN]]"},
				{"groupBy", "SELECT c, count(*) AS n FROM " + tbl + " GROUP BY c ORDER BY c", "[[-Inf 1] [-0 1] [1.5 1] [+Inf 1] [NaN 2] [<nil> 1]]"},
				{"distinct", "SELECT DISTINCT c FROM " + tbl + " ORDER BY c DESC", "[[<nil>] [NaN] [+Inf] [1.5] [-0] [-Inf]]"},
				{"union", "SELECT c FROM " + tbl + " UNION SELECT c FROM " + tbl + " ORDER BY c", "[[-Inf] [-0] [1.5] [+Inf] [NaN] [<nil>]]"},
				{"eqMax", "SELECT id FROM " + tbl + " WHERE c = (SELECT max(c) FROM " + tbl + ") ORDER BY id", "[[4] [6]]"},
				{"orderDesc", "SELECT id FROM " + tbl + " ORDER BY c DESC NULLS LAST, id", "[[4] [6] [1] [2] [3] [5] [7]]"},
			}
			for _, c := range cases {
				res, err := db.Query(ctx, c.sql)
				if err != nil {
					t.Errorf("%s/%s/%s: %s\n  refused: %v", arm.name, ty.key, c.name, c.sql, err)
					continue
				}
				if got := stCells(res); got != c.want {
					t.Errorf("%s/%s/%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", arm.name, ty.key, c.name, c.sql, got, c.want)
				}
			}
		}
		db.Close()
	}
}
