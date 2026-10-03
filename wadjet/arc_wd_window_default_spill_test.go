// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A LAG / LEAD DEFAULT WIDENS THE RESULT AND IS READ AT THE ROW IT FILLS,
// ACROSS A WINDOW SPILL (#1435). The partitioned path evaluates sorted runs
// partition by partition (computePartitionColumnar), the empty-PARTITION-BY
// path streams (globalWindowStreamer, whose LAG ring and LEAD lookahead write
// the default for the rows past the edge): both read the default out of the
// column the planner materialized, of the result's type. At v0.25.3 the
// default 2.5 was written into the bigint value's vector (2), and a column
// default failed the write. Every want is PostgreSQL 17.11's over the same
// generate_series fixture; the spilled arm asserts the run files were written.
func TestArcWDWindowDefaultUnderSpill(t *testing.T) {
	ctx := context.Background()
	open := func(budget int64) *DB {
		cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if budget > 0 {
			cfg.MemoryBudget = budget
			cfg.SpillDir = t.TempDir()
		}
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		const cols = "CAST(x AS BIGINT) AS id, CAST(x % 5 AS BIGINT) AS g, " +
			"CASE WHEN x % 7 = 0 THEN NULL ELSE CAST(x * 10 AS BIGINT) END AS b, " +
			"CAST(x AS DOUBLE) + 0.5 AS d, DATE '2024-01-01' + CAST(x % 30 AS INTEGER) AS dt"
		qs := []string{fmt.Sprintf("CREATE TABLE wds AS SELECT %s FROM generate_series(1, 1000) AS s(x)", cols)}
		for lo := 1001; lo <= 20000; lo += 1000 {
			qs = append(qs, fmt.Sprintf("INSERT INTO wds SELECT %s FROM generate_series(%d, %d) AS s(x)", cols, lo, lo+999))
		}
		for _, q := range qs {
			if _, err := db.Execute(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		return db
	}
	plain := open(0)
	spilled := open(256 * 1024)
	defer exec.ForceSmallSpillRuns(4096)()

	// numeric is one scale per column here (catalog numeric-decimal r18):
	// trailing fractional zeros are stripped before the comparison.
	strip := regexp.MustCompile(`\.(\d*?)0+$`)
	norm := func(s string) string {
		s = strip.ReplaceAllString(s, ".$1")
		if len(s) > 0 && s[len(s)-1] == '.' {
			s = s[:len(s)-1]
		}
		return s
	}
	for _, c := range []struct {
		name, sql string
		want      []string // id|value rows PostgreSQL 17.11 answers
	}{
		{"global/lag_dec",
			`SELECT id, w FROM (SELECT id, LAG(b, 1, 2.5) OVER (ORDER BY id) AS w FROM wds) s WHERE id <= 2 ORDER BY id`,
			[]string{"1|2.5", "2|10"}},
		{"global/lead_column_default",
			`SELECT id, w FROM (SELECT id, LEAD(b, 3, d) OVER (ORDER BY id) AS w FROM wds) s WHERE id > 19996 ORDER BY id`,
			[]string{"19997|200000", "19998|19998.5", "19999|19999.5", "20000|20000.5"}},
		{"partitioned/lag_column_default",
			`SELECT id, w FROM (SELECT id, LAG(b, 2, d) OVER (PARTITION BY g ORDER BY id) AS w FROM wds) s WHERE id <= 11 ORDER BY id`,
			[]string{"1|1.5", "2|2.5", "3|3.5", "4|4.5", "5|5.5", "6|6.5", "7|7.5", "8|8.5", "9|9.5", "10|10.5", "11|10"}},
		{"partitioned/lag_dec_sum",
			`SELECT 0 AS id, sum(w) AS v FROM (SELECT LAG(b, 1, 2.5) OVER (PARTITION BY g ORDER BY id) AS w FROM wds) s`,
			[]string{"0|1713514392.5"}},
		{"global/lead_dec_sum",
			`SELECT 0 AS id, sum(w) AS v FROM (SELECT LEAD(b, 1, 2.5) OVER (ORDER BY id) AS w FROM wds) s`,
			[]string{"0|1714314282.5"}},
		{"global/date_timestamp",
			`SELECT id, CAST(w AS TEXT) AS w FROM (SELECT id, LAG(dt, 1, TIMESTAMP '2020-01-01 00:00:00') OVER (ORDER BY id) AS w FROM wds) s WHERE id <= 2 ORDER BY id`,
			[]string{"1|2020-01-01 00:00:00", "2|2024-01-02 00:00:00"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				db   *DB
			}{{"plain", plain}, {"spilled", spilled}} {
				before := exec.WindowRunsWritten.Load()
				res, err := arm.db.Query(ctx, c.sql)
				if err != nil {
					t.Fatalf("%s: %v\n  SQL: %s", arm.name, err, c.sql)
				}
				var got []string
				for i := range res.Rows {
					cells := res.Cells(i)
					got = append(got, fmt.Sprintf("%v|%s", cells[0], norm(fmt.Sprint(cells[1]))))
				}
				if fmt.Sprint(got) != fmt.Sprint(c.want) {
					t.Errorf("%s: %v, PostgreSQL 17.11 answers %v\n  SQL: %s", arm.name, got, c.want, c.sql)
				}
				if arm.name == "spilled" && exec.WindowRunsWritten.Load() == before {
					t.Errorf("spilled: WindowRunsWritten did not move — the window did not spill\n  SQL: %s", c.sql)
				}
			}
		})
	}
}
