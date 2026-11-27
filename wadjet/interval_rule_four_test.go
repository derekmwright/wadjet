// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestIntervalRuleFourRefusals: the architect's rule 4 on 1680c (ADR-0052
// §8). Where the engine raised "unknown function", "operator does not exist"
// or an invalid-value error for a statement PostgreSQL 17.11 ACCEPTS AND
// ANSWERS, the same raise point now names the capability refusal (0A000 with
// its stable identity); where PostgreSQL itself raises an error, the engine's
// code is PostgreSQL's, untouched. Nothing new reaches execution: each
// statement was refused before and is refused now, and a statement that
// answered still answers. PostgreSQL's answers (postgres:17-alpine, --locale=C,
// IntervalStyle postgres, TimeZone UTC) are in the comments and 1680c_r5.md.
func TestIntervalRuleFourRefusals(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "r4", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO r4 (id, f, ts) VALUES (1, 1.5, '2024-01-31 00:00:00')"); err != nil {
		t.Fatal(err)
	}
	cells := []struct {
		why, sql, code, reason string
	}{
		// age: PostgreSQL answers `2 mons`; the engine has no age.
		{"a bare age", "SELECT age(TIMESTAMP '2024-03-31', TIMESTAMP '2024-01-31') AS a", "0A000", "function-age"},
		{"age under the display cast", "SELECT CAST(age(TIMESTAMP '2024-03-31', ts) AS TEXT) AS a FROM r4", "0A000", "function-age"},
		{"age in a WHERE beside an interval", "SELECT id FROM r4 WHERE age(TIMESTAMP '2024-03-31', ts) > INTERVAL '1 day'", "0A000", "function-age"},
		{"age in an ORDER BY", "SELECT id FROM r4 ORDER BY age(TIMESTAMP '2024-03-31', ts)", "0A000", "function-age"},
		{"control: a name PostgreSQL lacks is its 42883", "SELECT agez(ts) AS a FROM r4", "42883", ""},
		// The INTERVAL literal reader: PostgreSQL answers `1 day 02:00:00`
		// and `2024-01-02 12:00:00`; it raises 22007 for a repeated unit.
		{"a literal of two units", "SELECT CAST(INTERVAL '1 day 2 hours' AS TEXT) AS a", "0A000", "interval-input-form"},
		{"a fractional literal applied", "SELECT ts + INTERVAL '1.5 days' AS a FROM r4", "0A000", "interval-input-form"},
		{"control: a repeated unit is PostgreSQL's 22007", "SELECT CAST(INTERVAL '1 day 1 day' AS TEXT) AS a", "22007", ""},
		{"control: an unknown unit word is PostgreSQL's 22007", "SELECT CAST(INTERVAL '1 dayz 2 hours' AS TEXT) AS a", "22007", ""},
		// interval × NUMERIC: PostgreSQL answers `1 day 06:00:00` (numeric is
		// cast to float8 implicitly); interval × text is its 42883.
		{"interval * numeric", "SELECT CAST(INTERVAL '1 day' * CAST(1.25 AS NUMERIC) AS TEXT) AS a", "0A000", "interval-numeric-factor"},
		{"numeric * interval", "SELECT CAST(CAST(f AS NUMERIC(5,2)) * INTERVAL '1 day' AS TEXT) AS a FROM r4", "0A000", "interval-numeric-factor"},
		{"control: interval * text is PostgreSQL's 42883", "SELECT CAST(INTERVAL '1 day' * CAST('2' AS TEXT) AS TEXT) AS a", "42883", ""},
		// An infinite timestamp difference: PostgreSQL answers `f` (the
		// difference is infinity) and raises 22008 for the same infinity on
		// both sides.
		{"an infinite difference PostgreSQL answers", "SELECT (TIMESTAMP 'infinity' - TIMESTAMP '2024-01-01') IS NULL AS a", "0A000", "timestamp-difference-to-number"},
		{"a column beside an infinity", "SELECT (ts - TIMESTAMP '-infinity') IS NULL AS a FROM r4", "0A000", "timestamp-difference-to-number"},
		{"control: the same infinity is PostgreSQL's 22008", "SELECT (TIMESTAMP 'infinity' - TIMESTAMP 'infinity') IS NULL AS a", "22008", ""},
	}
	for _, c := range cells {
		_, err := db.Query(ctx, c.sql)
		code, reason := sqlerr.StateOf(err), sqlerr.ReasonOf(err)
		if err == nil || code != c.code || reason != c.reason {
			t.Errorf("%s\n  %s\n  got %v (%s {%s}), want %s {%s}", c.why, c.sql, err, code, reason, c.code, c.reason)
		}
	}
	// The workaround the numeric-factor refusal names answers PostgreSQL's
	// value.
	res, err := db.Query(ctx, "SELECT CAST(INTERVAL '1 day' * CAST(1.25 AS DOUBLE PRECISION) AS TEXT) AS a")
	if err != nil || len(res.Rows) != 1 || res.Rows[0]["a"] != "1 day 06:00:00" {
		t.Errorf("the double precision factor: got %v, %v; PostgreSQL 1 day 06:00:00", res, err)
	}
}

// TestDMLStatementShapeRefusalsCarryAnIdentity: the write doors'
// statement-shape refusals that precede the interval rule (a subquery in
// INSERT ... VALUES or in an UPDATE's SET list, a MERGE ON that is not an
// equality between two columns) carry a stable identity, so the interval
// corpus credits them by identity (the rulings on 1680c: the earlier refusal
// is the outcome where it is truthful). Their sentences and SQLSTATE are
// unchanged, and nothing is written.
func TestDMLStatementShapeRefusalsCarryAnIdentity(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "w", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO w (id, s) VALUES (1, 'x')"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ sql, reason string }{
		{"INSERT INTO w (id, s) VALUES (2, (SELECT s FROM w))", "dml-values-subquery"},
		{"UPDATE w SET s = (SELECT s FROM w)", "dml-set-subquery"},
		{"MERGE INTO w USING w x ON w.id = x.id + 0 WHEN MATCHED THEN UPDATE SET s = 'y'", "merge-on-equality"},
	} {
		_, err := db.Execute(ctx, c.sql)
		if code, reason := sqlerr.StateOf(err), sqlerr.ReasonOf(err); code != "0A000" || reason != c.reason {
			t.Errorf("%s\n  got %v (%s {%s}), want 0A000 {%s}", c.sql, err, code, reason, c.reason)
		}
	}
	res, err := db.Query(ctx, "SELECT id, s FROM w ORDER BY id")
	if err != nil || len(res.Rows) != 1 || res.Rows[0]["s"] != "x" {
		t.Errorf("a refused write changed the table: %v, %v", res, err)
	}
}
