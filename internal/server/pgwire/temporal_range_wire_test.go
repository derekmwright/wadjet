// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestTemporalRangeRefusedOnTheWire is the wire door of arc VL round 4's range
// gate (wadjet.TestTemporalConstructionRefusesPastPostgreSQLRange holds every
// form × every statement door on the embedded API): a DATE or TIMESTAMP built
// past PostgreSQL's range is 22008 over the simple protocol and over the
// extended protocol with text AND binary result formats, and the table keeps
// the row it had. At base `SELECT d - i` answered `-5877585-08-24`, and the
// write doors stored that date — whose binary encoding was a zero-length value
// (round-3 review B1, wire1_tip.txt row 19).
func TestTemporalRangeRefusedOnTheWire(t *testing.T) {
	db, srv := setupRealDB(t)
	ctx := context.Background()
	if err := db.CreateTable(ctx, "rgw", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO rgw (id, i, d, ts) VALUES (1, 2147483647, '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	conn := connectPgconn(t, srv.Addr())
	stmts := []string{
		"SELECT d - i AS v FROM rgw",
		"SELECT d + i AS v FROM rgw",
		"SELECT DATE '2026-03-03' - 5000000 AS v",
		"SELECT ts + INTERVAL '300000000 years' AS v FROM rgw",
		"SELECT CAST(DATE '5874897-12-31' AS TIMESTAMP) AS v",
		"SELECT id FROM rgw WHERE d - i < d",
		"INSERT INTO rgw (id, d) VALUES (2, DATE '2026-03-03' + 2147483647)",
		"INSERT INTO rgw (id, ts) VALUES (3, TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '300000000 years')",
		"INSERT INTO rgw (id, d) SELECT 4, d - i FROM rgw",
		"UPDATE rgw SET d = d + i",
		"UPDATE rgw SET ts = ts + INTERVAL '300000000 years'",
		"MERGE INTO rgw USING rgw x ON rgw.id = x.id WHEN MATCHED THEN UPDATE SET d = x.d - x.i",
		"CREATE TABLE rgw_c AS SELECT d + i AS v FROM rgw",
		"DELETE FROM rgw WHERE d + i > d",
	}
	cells := 0
	for _, sql := range stmts {
		for _, mode := range []string{"simple", "text", "binary"} {
			cells++
			var err error
			switch mode {
			case "simple":
				_, err = conn.Exec(ctx, sql).ReadAll()
			case "text":
				err = conn.ExecParams(ctx, sql, nil, nil, nil, []int16{0}).Read().Err
			case "binary":
				err = conn.ExecParams(ctx, sql, nil, nil, nil, []int16{1}).Read().Err
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "22008" {
				t.Errorf("%s [%s]: got %v, PostgreSQL 17.11 22008", sql, mode, err)
			}
		}
	}
	res, err := db.Query(ctx, "SELECT id, CAST(d AS TEXT) AS d, CAST(ts AS TEXT) AS ts FROM rgw ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["d"], " ", res.Rows[0]["ts"]) != "2026-03-03 2026-03-03 10:20:30" {
		t.Errorf("rgw now holds %v, want the one row it started with", res.Rows)
	}
	t.Logf("%d statement × protocol cells, every one 22008", cells)
}
