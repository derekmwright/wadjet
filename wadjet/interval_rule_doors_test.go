// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The INTERVAL rule on the embedded engine's write and DDL doors (#1713d):
// a NEW column spelled INTERVAL is refused at any depth (the shared schema
// resolver every door uses), an existing DURATION column keeps its
// nanosecond meaning, and no write stores an interval — into a TEXT column it
// would be its text, the silent coercion the ruling forbids. PostgreSQL
// creates the column and stores the text; each refusal here is the ruling's.
// The controls are the writes that consume an interval inline or cast it to
// text explicitly, and the reads of a DURATION column.
func TestIntervalRuleOnTheWriteAndDDLDoors(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "ivw", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "dur", Type: parquet.TypeDuration, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO ivw (id, s, ts, dur) VALUES (1, '1 day', '2024-01-31 10:00:00', 86400000000000)"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ sql, state string }{
		// DDL: a new interval column, at any depth, through SQL.
		{"CREATE TABLE ivd1 (id BIGINT, d INTERVAL)", "0A000"},
		{"CREATE TABLE ivd2 (id BIGINT, d interval)", "0A000"},
		{"CREATE TABLE ivd3 (id BIGINT, d ARRAY(INTERVAL))", "0A000"},
		{"CREATE TABLE ivd4 (id BIGINT, r ROW(a BIGINT, b INTERVAL))", "0A000"},
		{"CREATE TABLE ivd5 (id BIGINT, m MAP(STRING, INTERVAL))", "0A000"},
		{"CREATE TABLE ivd6 (id BIGINT, d DURATION)", ""},
		{"CREATE TABLE ivd7 (id BIGINT, r ROW(interval BIGINT))", ""},
		// CTAS creates a column from a query that would publish one.
		{"CREATE TABLE ivc1 AS SELECT INTERVAL '1 day' AS d", "0A000"},
		{"CREATE TABLE ivc2 AS SELECT id, INTERVAL '1 day' * 2 AS d FROM ivw WITH NO DATA", "0A000"},
		{"CREATE TABLE ivc3 AS SELECT id, CAST(INTERVAL '1 day' * 2 AS TEXT) AS d FROM ivw", ""},
		// Writes.
		{"INSERT INTO ivw (id, s) VALUES (2, INTERVAL '1 day')", "0A000"},
		{"INSERT INTO ivw (id, s) VALUES (3, COALESCE(INTERVAL '1 day', NULL))", "0A000"},
		{"INSERT INTO ivw (id, s) SELECT 4, INTERVAL '1 day' * 2", "0A000"},
		{"INSERT INTO ivw (id, s) SELECT id + 10, CAST(s AS INTERVAL) FROM ivw", "0A000"},
		{"UPDATE ivw SET s = INTERVAL '1 day' * 3 WHERE id = 1", "0A000"},
		{"INSERT INTO ivw (id, s) VALUES (5, CAST(INTERVAL '1 day' * 2 AS TEXT))", ""},
		{"INSERT INTO ivw (id, ts) VALUES (6, TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month')", ""},
		{"UPDATE ivw SET ts = ts + INTERVAL '1 day' WHERE CAST(s AS INTERVAL) = INTERVAL '24 hours'", ""},
		{"DELETE FROM ivw WHERE CAST(s AS INTERVAL) > INTERVAL '30 days'", ""},
		{"UPDATE ivw SET s = s || INTERVAL '1 day' WHERE id = 1", "0A000"},
		{"MERGE INTO ivw t USING ivw src ON t.id = src.id WHEN MATCHED THEN UPDATE SET s = INTERVAL '1 day'", "0A000"},
		{"MERGE INTO ivw t USING ivw src ON t.id = src.id WHEN MATCHED AND CAST(t.s AS INTERVAL) > INTERVAL '30 days' THEN DELETE", ""},
	} {
		_, err := db.Query(ctx, tc.sql)
		got := sqlerr.StateOf(err)
		if tc.state == "" && err != nil {
			t.Errorf("%s\n  refused: %v", tc.sql, err)
			continue
		}
		if got != tc.state {
			t.Errorf("%s\n  SQLSTATE %q (%v), want %q", tc.sql, got, err, tc.state)
		}
		if err != nil && strings.Contains(err.Error(), "cannot store") {
			t.Errorf("%s reached the store guard: %v", tc.sql, err)
		}
	}
	// What the controls wrote, and the DURATION column read back unchanged.
	res, err := db.Query(ctx, "SELECT id, s, CAST(ts AS TEXT) AS t, dur FROM ivw ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for i := range res.Rows {
		c := res.Cells(i)
		parts := make([]string, len(c))
		for j, v := range c {
			if v == nil {
				parts[j] = "NULL"
			} else {
				parts[j] = fmt.Sprint(v)
			}
		}
		rows = append(rows, strings.Join(parts, ","))
	}
	want := "1,1 day,2024-02-01 10:00:00,86400000000000; 5,2 days,NULL,NULL; 6,NULL,2024-02-29 00:00:00,NULL"
	if got := strings.Join(rows, "; "); got != want {
		t.Errorf("rows\n  got  %s\n  want %s", got, want)
	}
}
