// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestTemporalArithmeticPostgreSQLHasNoOperatorFor is the census row "a
// timestamp and a number" (arc VL round 3): PostgreSQL 17.11 has no `+` / `-`
// between a timestamp and a number, a date and a fractional number, a number
// and a date on the left of `-`, or two temporal values added, and says so
// with 42883 (sentences measured). Each answered an epoch count plus the
// operand here, and the single-process and DAG arms typed that number
// differently (`MAX(c_ts) + 0`). The shapes PostgreSQL does have keep
// answering.
func TestTemporalArithmeticPostgreSQLHasNoOperatorFor(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "ta", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO ta (id, d, ts) VALUES (1, '2026-03-03', '2026-03-03 10:00:00')"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ sql, msg string }{
		{"SELECT ts + 0 AS v FROM ta", "operator does not exist: timestamp without time zone + integer"},
		{"SELECT MAX(ts) + 0 AS v FROM ta", "operator does not exist: timestamp without time zone + integer"},
		{"SELECT now() + 1 AS v", "operator does not exist: timestamp without time zone + integer"},
		{"SELECT d + 1.5 AS v FROM ta", "operator does not exist: date + numeric"},
		{"SELECT CURRENT_DATE - 1.5 AS v", "operator does not exist: date - numeric"},
		{"SELECT 1 - d AS v FROM ta", "operator does not exist: integer - date"},
		{"SELECT d + ts AS v FROM ta", "operator does not exist: date + timestamp without time zone"},
	} {
		_, err := db.Query(ctx, tc.sql)
		if sqlerr.StateOf(err) != "42883" || err == nil || err.Error() != tc.msg {
			t.Errorf("%s: %v (SQLSTATE %q), want 42883 %q", tc.sql, err, sqlerr.StateOf(err), tc.msg)
		}
	}
	for _, sql := range []string{
		"SELECT d + 1 AS v FROM ta", "SELECT 1 + d AS v FROM ta", "SELECT d - d AS v FROM ta",
		"SELECT ts - ts AS v FROM ta", "SELECT ts + INTERVAL '1 hour' AS v FROM ta",
		"SELECT MAX(d) + 0 AS v FROM ta", "SELECT CURRENT_DATE - 1 AS v",
	} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Errorf("%s: %v, want an answer (PostgreSQL has the operator)", sql, err)
		}
	}
}
