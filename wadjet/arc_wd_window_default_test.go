// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The embedded arm of the LAG / LEAD default gate (#1435): the issue's three
// statements, the type a CREATE TABLE … AS of one declares, and what an
// INSERT … SELECT of one stores into INTEGER / NUMERIC(10,2) / DOUBLE. Every
// `want` is live PostgreSQL 17.11's answer over the same rows.
//
// At v0.25.3 the default was written into a vector of the VALUE's type, so
// `LAG(x, 1, 2.5)` over a bigint answered 2 on the rows the default fills and
// a CTAS of it declared bigint; PostgreSQL's result type is the common type
// of the value and the default — numeric. The five-arm table is
// coordinator.TestArcWDWindowDefaultEveryArm.
func TestArcWDWindowDefaultOnTheEmbeddedEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	const lag = "LAG(x, 1, 2.5) OVER (ORDER BY id)"
	for _, q := range []string{
		"CREATE TABLE wa_t (id BIGINT, g BIGINT, x BIGINT)",
		"INSERT INTO wa_t VALUES (1, 1, 10), (2, 1, 20), (3, 1, NULL), (4, 2, 40), (5, 2, 50), (6, 3, 60)",
		"CREATE TABLE wd_int (id BIGINT, w INTEGER)",
		"CREATE TABLE wd_num (id BIGINT, w NUMERIC(10,2))",
		"CREATE TABLE wd_dbl (id BIGINT, w DOUBLE)",
		"INSERT INTO wd_int SELECT id, " + lag + " FROM wa_t",
		"INSERT INTO wd_num SELECT id, " + lag + " FROM wa_t",
		"INSERT INTO wd_dbl SELECT id, " + lag + " FROM wa_t",
		"CREATE TABLE wd_ctas AS SELECT id, " + lag + " AS w FROM wa_t",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// numeric is one scale per column here and per value in PostgreSQL —
	// the catalogued value divergence numeric-decimal r18 — so the CTAS and
	// projection cells compare digits with trailing fractional zeros
	// stripped; the NUMERIC(10,2) column is compared as stored.
	strip := regexp.MustCompile(`\.(\d*?)0+\b`)
	norm := func(s string) string {
		return strings.ReplaceAll(strip.ReplaceAllString(s, ".$1"), ". ", " ")
	}
	for _, c := range []struct {
		sql, want string
		normalize bool
	}{
		{"SELECT id, " + lag + " FROM wa_t ORDER BY id", "1|2.5 2|10 3|20 4|NULL 5|40 6|50", true},
		{"SELECT id, LAG(x, 10, 2.5) OVER (PARTITION BY g ORDER BY id) FROM wa_t ORDER BY id", "1|2.5 2|2.5 3|2.5 4|2.5 5|2.5 6|2.5", true},
		{"SELECT id, LEAD(x, 10, 2.5) OVER (ORDER BY id) FROM wa_t ORDER BY id", "1|2.5 2|2.5 3|2.5 4|2.5 5|2.5 6|2.5", true},
		{"SELECT * FROM wd_int ORDER BY id", "1|3 2|10 3|20 4|NULL 5|40 6|50", false},
		{"SELECT * FROM wd_num ORDER BY id", "1|2.50 2|10.00 3|20.00 4|NULL 5|40.00 6|50.00", false},
		{"SELECT * FROM wd_dbl ORDER BY id", "1|2.5 2|10 3|20 4|NULL 5|40 6|50", false},
		{"SELECT * FROM wd_ctas ORDER BY id", "1|2.5 2|10 3|20 4|NULL 5|40 6|50", true},
		// A recursive CTE's column as the value (PostgreSQL: numeric).
		{"WITH RECURSIVE r(id, v) AS (SELECT id, x FROM wa_t WHERE id = 1 UNION ALL SELECT id + 1, v + 10 FROM r WHERE id < 3) " +
			"SELECT id, LAG(v, 1, 2.5) OVER (ORDER BY id) FROM r ORDER BY id", "1|2.5 2|10 3|20", true},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("#1435: %s refused: %v", c.sql, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			var cells []string
			for _, v := range res.Cells(i) {
				if v == nil {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, fmt.Sprint(v))
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
		got := strings.Join(rows, " ")
		if c.normalize {
			got = norm(got + " ")
			got = strings.TrimSpace(got)
		}
		if got != c.want {
			t.Errorf("#1435: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
	// The DECLARED type: numeric for the projection and for the table a CTAS
	// of it creates (PostgreSQL: `numeric`).
	for _, q := range []string{"SELECT id, " + lag + " AS w FROM wa_t", "SELECT * FROM wd_ctas"} {
		res, err := db.Query(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if len(res.OutputSchema) != 2 || res.OutputSchema[1].Type != parquet.TypeDecimal {
			t.Errorf("#1435: %s declares %v, want numeric (PostgreSQL 17.11)", q, res.OutputSchema)
		}
	}
}

// THE DEFAULT'S DECLARATION IS LINEAR IN DEPTH (arc WD's timing gate, the
// TestTCPFlagPlanningDepth shape): every level of sixteen derived tables runs
// a LAG whose default widens its input, so each level's declaration reads the
// level below's — once. Planning and running the 16-deep statement stays
// under 2 s.
func TestArcWDWindowDefaultPlanningDepth(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE wdp (id BIGINT, v BIGINT)",
		"INSERT INTO wdp VALUES (1, 10), (2, 20), (3, 30)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, depth := range []int{4, 8, 12, 16} {
		q := "SELECT id, v FROM wdp"
		for i := 0; i < depth; i++ {
			q = "SELECT id, LAG(v, 1, 2.5) OVER (ORDER BY id) AS v FROM (" + q + ") d"
		}
		start := time.Now()
		res, err := db.Query(ctx, q+" ORDER BY id")
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
		if len(res.Rows) != 3 {
			t.Fatalf("depth %d: %d rows", depth, len(res.Rows))
		}
		t.Logf("depth=%d elapsed=%s", depth, elapsed)
		if depth == 16 && elapsed > 2*time.Second {
			t.Errorf("depth %d took %s, want < 2s: the default's declaration is not linear in depth", depth, elapsed)
		}
	}
}
