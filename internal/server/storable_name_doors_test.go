package server

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A relation or column name that cannot be one component of an object key is
// refused at CREATE, on every door, with 42602.
//
// The lexer takes a DELIMITED identifier byte-exact, and a table's data lives
// at `tables/<name>/…`, so `CREATE TABLE "../../../tmp/x"` handed the object
// store a key that climbed out of its root — on `storage.type: file` an
// arbitrary file write (CodeQL go/path-injection #23/#24/#25). The store
// refuses such a key now; this is the layer where a person is told why, before
// a table exists whose every write would fail.
//
// A DELIBERATE DIVERGENCE from PostgreSQL, recorded in ADR-0012's list:
// PostgreSQL accepts all of these inside double quotes, because its relations
// are rows in pg_class and never filenames.
func TestNoDoorCreatesARelationWhoseNameIsNotStorable(t *testing.T) {
	refused := []string{
		`../../../tmp/x`,
		`../escape`,
		`a/b`,
		`a\b`,
		`..`,
		`.hidden`,
		`tables/../x`,
	}
	accepted := []string{
		`ok_table`,
		`Mixed_Case`,
		`with space`,
		`with"quote`,
		`x..y`, // ".." inside a name, not a traversal component
	}

	for _, name := range refused {
		t.Run("refused/"+name, func(t *testing.T) {
			for door, got := range createTableStates(t, name) {
				if got != "42602" {
					t.Errorf("%s door: CREATE TABLE %q answered %s, want 42602", door, name, got)
				}
			}
		})
	}
	for _, name := range accepted {
		t.Run("accepted/"+name, func(t *testing.T) {
			for door, got := range createTableStates(t, name) {
				if got != "<ok>" {
					t.Errorf("%s door: CREATE TABLE %q answered %s, want success", door, name, got)
				}
			}
		})
	}
}

// A COLUMN name is spelled into the namespace too: a partition key becomes a
// `<col>=<value>/` directory component.
func TestNoDoorCreatesAColumnWhoseNameIsNotStorable(t *testing.T) {
	ctx := context.Background()
	cat := catalog.NewWithStore(objstore.NewMemStore(), "test-bucket")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	err := cat.CreateTable(ctx, "t", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "../escape", Type: parquet.TypeInt64, Nullable: true},
	}}, nil)
	if err == nil {
		t.Fatal("the catalog accepted a column named \"../escape\"")
	}
	if s := doorStateOrNone(err); s != "42602" {
		t.Errorf("column name refusal carried %s, want 42602: %v", s, err)
	}
}

// createTableStates runs one CREATE TABLE with a DELIMITED name on all three
// doors and reports each door's SQLSTATE (or "<ok>").
func createTableStates(t *testing.T, name string) map[string]string {
	t.Helper()
	sql := `CREATE TABLE "` + strings.ReplaceAll(name, `"`, `""`) + `" (a INT64)`
	out := map[string]string{}

	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(ctx, sql); err != nil {
		out["embedded"] = doorStateOrNone(err)
	} else {
		out["embedded"] = "<ok>"
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	hcat := catalog.NewWithStore(store, "test")
	if err := hcat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := New(Config{Addr: ":0", Catalog: hcat}, logger)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()
	_, state, _, err := postDoorSQL(ts.URL, sql)
	if err != nil {
		t.Fatalf("HTTP door: %v", err)
	}
	if state == "" {
		out["HTTP"] = "<ok>"
	} else {
		out["HTTP"] = state
	}

	gdb, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer gdb.Close()
	g := NewGRPCServer(GRPCConfig{Catalog: gdb.Catalog(), DB: gdb}, slog.Default())
	// The gRPC door's typed CreateTable RPC, which does not go through SQL at
	// all — the name arrives as a field, so the lexer is not even involved and
	// the catalog is the only thing standing between it and the store.
	if _, err := g.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name:    name,
		Columns: []*wadjetv1.ColumnDef{{Name: "a", Type: "INT64", Nullable: true}},
	}); err != nil {
		if strings.Contains(err.Error(), "42602") {
			out["gRPC"] = "42602"
		} else {
			out["gRPC"] = doorStateOrNone(err)
		}
	} else {
		out["gRPC"] = "<ok>"
	}
	return out
}
