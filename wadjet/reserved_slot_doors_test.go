package wadjet_test

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Every DOOR of the reserved slot namespace refuses, and each has its own
// entry — the read-side refusal that shipped first had ZERO tests, and that is
// how it reached a state where CREATE TABLE, the Go API and INSERT all
// succeeded while every SELECT against the result failed.
//
// The doors are where the name is CREATED. Reading a stored column of that
// name is never refused (coordinator.TestStoredReservedColumnStaysReadable).
func TestReservedSlotNamespaceDoors(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	bad := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "__win_0", Type: parquet.TypeInt64, Nullable: true},
	}}
	good := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ok_col", Type: parquet.TypeInt64, Nullable: true},
	}}

	assertReserved := func(t *testing.T, door string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("the %s door ADMITTED a reserved slot name; a table created through it "+
				"puts a user column and the planner's own slot under one name", door)
		}
		if !strings.Contains(err.Error(), "__win_") {
			t.Errorf("the %s door refused without naming the family: %v", door, err)
		}
	}

	t.Run("the CreateTable API", func(t *testing.T) {
		assertReserved(t, "CreateTable", db.CreateTable(ctx, "api_bad", bad, nil))
		if err := db.CreateTable(ctx, "api_ok", good, nil); err != nil {
			t.Errorf("CreateTable refused an ordinary schema: %v", err)
		}
	})

	t.Run("CREATE TABLE in SQL", func(t *testing.T) {
		_, err := db.Query(ctx, `CREATE TABLE sql_bad (id BIGINT, __win_0 BIGINT)`)
		assertReserved(t, "CREATE TABLE", err)
		if _, err := db.Query(ctx, `CREATE TABLE sql_ok (id BIGINT, ok_col BIGINT)`); err != nil {
			t.Errorf("CREATE TABLE refused an ordinary schema: %v", err)
		}
	})

	t.Run("the Ingester", func(t *testing.T) {
		// NewIngester returns no error, so the refusal lands on the first call
		// that does something.
		ing := db.NewIngester("ing_bad", bad, nil, ingest.Config{MaxBufferRows: 8})
		assertReserved(t, "Ingester.Ingest",
			ing.Ingest(ctx, []map[string]any{{"id": int64(1), "__win_0": int64(2)}}))
		assertReserved(t, "Ingester.FlushAll", ing.FlushAll(ctx))

		okIng := db.NewIngester("ing_ok", good, nil, ingest.Config{MaxBufferRows: 8})
		if err := okIng.Ingest(ctx, []map[string]any{{"id": int64(1), "ok_col": int64(2)}}); err != nil {
			t.Errorf("the Ingester refused an ordinary schema: %v", err)
		}
	})

	t.Run("a name outside every family is admitted at every door", func(t *testing.T) {
		near := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			// One underscore, and a double-underscore name in no family: the
			// reservation is a named list, not a ban on leading underscores.
			{Name: "_win_0", Type: parquet.TypeInt64, Nullable: true},
			{Name: "__window", Type: parquet.TypeInt64, Nullable: true},
		}}
		if err := db.CreateTable(ctx, "near_ok", near, nil); err != nil {
			t.Errorf("CreateTable refused a name in no slot family: %v", err)
		}
	})
}
