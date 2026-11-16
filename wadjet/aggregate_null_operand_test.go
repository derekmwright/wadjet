// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// AN AGGREGATE OVER `x op NULL` (#1558). The aggregate's computed input is
// materialized by the planner's pre-projection, which wrote the bulk float64
// kernel's buffer and dropped its "some row is NULL" report: every NULL row
// kept the kernel's placeholder, so SUM(t.i + NULL) answered the sum of t.i,
// SUM(t.i / NULL) answered 0, and COUNT(t.i + NULL) counted every row — while
// the same expression PROJECTED answered NULL, because exec.Project honors the
// report. Every want is PostgreSQL 17.11's answer (NULL for SUM/AVG/MIN/MAX,
// 0 for COUNT), on the plain engine and under a memory budget with spill
// engaged, with and without a filter (a selection vector on the input) and
// grouped.
func TestAggregateOverNullOperand(t *testing.T) {
	ctx := context.Background()
	arms := map[string]Config{
		"single":  {Store: objstore.NewMemStore(), Bucket: "test"},
		"spilled": {Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 1 << 16},
	}
	cells := []struct{ sql, want string }{
		{"SELECT SUM(t.i + NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.i - NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.i * NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.i / NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.n + NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.n - NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.n * NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(t.n / NULL) FROM ss_t t", "NULL"},
		{"SELECT SUM(NULL + t.i) FROM ss_t t", "NULL"},
		{"SELECT AVG(t.i + NULL), MIN(t.i + NULL), MAX(t.i + NULL), COUNT(t.i + NULL) FROM ss_t t", "NULL,NULL,NULL,0"},
		{"SELECT AVG(t.n / NULL), MIN(t.n / NULL), MAX(t.n / NULL), COUNT(t.n / NULL) FROM ss_t t", "NULL,NULL,NULL,0"},
		{"SELECT SUM(t.i + NULL), COUNT(t.n * NULL) FROM ss_t t WHERE t.id > 1", "NULL,0"},
		{"SELECT COUNT(*) FROM (SELECT t.id, SUM(t.i + NULL) AS s FROM ss_t t GROUP BY t.id) q WHERE q.s IS NULL", "6"},
		{"SELECT sum(v + NULL) FROM ss_i", "NULL"},
		// Controls: the same aggregate inputs with no NULL operand, one a
		// float column whose sixth row is NULL so the bulk kernel reports a
		// NULL row for a real reason.
		{"SELECT SUM(t.i + 1), COUNT(t.i + 1) FROM ss_t t", "7,5"},
		{"SELECT SUM(t.f + 1), COUNT(t.f * 2), MIN(t.f - 1), MAX(t.f / 2) FROM ss_t t", "104.375,5,-3.5,50.0625"},
		{"SELECT SUM(t.f + 1), COUNT(t.f * 2) FROM ss_t t WHERE t.id > 1", "101.875,4"},
	}
	for name, cfg := range arms {
		t.Run(name, func(t *testing.T) {
			cfg.SpillDir = t.TempDir()
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, ddl := range []string{
				"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2))",
				"INSERT INTO ss_t VALUES (1, 3, 30, 1.5, 2.25), (2, -7, -70, -2.5, -3.5), (3, 5, 9000000000, 0.25, 10.00), " +
					"(4, 0, 0, 0.0, 0.00), (5, 1, 1, 100.125, 0.01), (6, NULL, NULL, NULL, NULL)",
				"CREATE TABLE ss_i (id BIGINT, v INT, g DOUBLE, m NUMERIC(10,2))",
				"INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			for _, c := range cells {
				if got := nullOperandRows(t, db, c.sql); got != c.want {
					t.Errorf("%s\n got  %s\n want %s (PostgreSQL 17.11)", c.sql, got, c.want)
				}
			}
		})
	}
}

// nullOperandRows renders a one-row answer as its comma-joined cells.
func nullOperandRows(t *testing.T, db *DB, sql string) string {
	t.Helper()
	s := moRun(db, sql)
	if len(s) > 4 && s[:4] == "ERR " {
		return s
	}
	// moRun prefixes the declared types; the values are what PostgreSQL pins.
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			return s[i+1:]
		}
	}
	return s
}
