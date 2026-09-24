// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInsertSelectCurrentDateIntoDateColumn is #1254's own repro:
// `CURRENT_DATE` used to declare STRING (internal/engine/expr/expr.go
// registered it RetString), so assigning it — even positionally, through
// INSERT ... SELECT — into a DATE column was refused as a type mismatch,
// though `SELECT CURRENT_DATE` alone already answered a date-looking value.
// The wire door's OID is pinned separately
// (internal/server/pgwire/current_date_wire_test.go); this is the CLI/
// embedded door PostgreSQL's own repro exercises.
func TestInsertSelectCurrentDateIntoDateColumn(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ev3", schema, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Execute(ctx, "INSERT INTO ev3 SELECT NOW(), CURRENT_DATE, 2"); err != nil {
		t.Fatalf("INSERT ... SELECT CURRENT_DATE: %v", err)
	}

	res, err := db.Query(ctx, "SELECT d, n FROM ev3")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	if _, ok := res.Rows[0]["d"].(string); !ok {
		t.Errorf("d = %#v (%T), want a date-rendered string", res.Rows[0]["d"], res.Rows[0]["d"])
	}
	if n, ok := res.Rows[0]["n"].(int64); !ok || n != 2 {
		t.Errorf("n = %#v, want int64(2)", res.Rows[0]["n"])
	}
}

// TestCurrentDateEqualsCastNowAsDate pins the sibling behaviour #1254's
// review named: an expression this layer types STRUCTURALLY
// (validate_comparison_types.go) rather than from a scalar function's
// registered return type, so a comparison between CURRENT_DATE and a CAST
// stays answerable rather than becoming a cross-class refusal now that
// CURRENT_DATE's own declaration is correct.
func TestCurrentDateEqualsCastNowAsDate(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	res, err := db.Query(ctx, "SELECT (CURRENT_DATE = CAST(NOW() AS DATE)) AS eq")
	if err != nil {
		t.Fatalf("CURRENT_DATE = CAST(NOW() AS DATE): %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	if eq, ok := res.Rows[0]["eq"].(bool); !ok || !eq {
		t.Errorf("eq = %#v, want true", res.Rows[0]["eq"])
	}
}
