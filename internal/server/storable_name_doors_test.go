package server

import (
	"context"
	"fmt"
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

// A name longer than PostgreSQL's own identifier length is refused, on every
// door, with 42622.
//
// PostgreSQL TRUNCATES: measured live on postgres:17-alpine, an 80-byte name
// becomes 63 bytes with `NOTICE 42622 identifier "…" will be truncated to
// "…"`. Wadjet cannot, because the name is a component of the object key its
// data is stored under and two names truncated to one would be two tables at
// ONE location. Before this a 300-byte name was accepted at CREATE and then
// failed every write with ENAMETOOLONG — "a table whose data has no home",
// which is the failure ADR-0012's entry says the rule exists to prevent.
func TestNoDoorCreatesARelationWhoseNameIsTooLong(t *testing.T) {
	for _, tc := range []struct {
		n     int
		state string
	}{
		{1, "<ok>"},
		{62, "<ok>"},
		{catalog.MaxNameBytes, "<ok>"}, // exactly PostgreSQL's own length
		{catalog.MaxNameBytes + 1, "42622"},
		{300, "42622"},
	} {
		name := strings.Repeat("a", tc.n)
		t.Run(fmt.Sprintf("%d_bytes", tc.n), func(t *testing.T) {
			for door, got := range createTableStates(t, name) {
				if got != tc.state {
					t.Errorf("%s door: a %d-byte name answered %s, want %s",
						door, tc.n, got, tc.state)
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
		out["gRPC"] = grpcSQLState(err)
	} else {
		out["gRPC"] = "<ok>"
	}
	return out
}

// grpcSQLState reads the class out of a gRPC status message. That door renders
// it as text — `sqlErrorText` appends "(SQLSTATE 42602)" — rather than
// attaching it to the error, so `sqlerr.StateOf` cannot see it through the
// status wrapper.
func grpcSQLState(err error) string {
	msg := err.Error()
	const marker = "(SQLSTATE "
	if i := strings.Index(msg, marker); i >= 0 {
		rest := msg[i+len(marker):]
		if j := strings.IndexByte(rest, ')'); j == 5 {
			return rest[:5]
		}
	}
	return doorStateOrNone(err)
}
