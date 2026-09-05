package server

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A number no INT32 column can hold is 22003 on every door, and is never
// stored.
//
// PostgreSQL 17, measured on postgres:17-alpine:
//
//	INSERT INTO t(a int4) VALUES (3000000000)   ERROR 22003 integer out of range
//	INSERT INTO t(a int4) VALUES (-2147483649)  ERROR 22003 integer out of range
//	INSERT INTO t(a int4) VALUES ('NaN'::float8) ERROR 22003 integer out of range
//	INSERT INTO t(a int4) VALUES (2147483647)   INSERT 0 1
//
// The three SQL doors already refused these — wadjet/dml.go's
// assignIntegerValue is the guard — but the PROGRAMMATIC ingest boundary
// below did not: parquet's toInt32 wrapped 3000000000 into -1294967296 and
// wrote it, with no error anywhere (CodeQL #36/#37). This gate holds all four
// entries to the same answer, so a door added later cannot quietly take the
// wrapping path.
func TestNoDoorStoresAnInt32ItCannotHold(t *testing.T) {
	const create = "CREATE TABLE s1ovf (id INT64, a INT32, p PORT, pr PROTOCOL)"
	for _, tc := range []struct {
		name string
		sql  string
		// "" = the statement must succeed.
		state string
	}{
		{name: "int32 above the range", sql: "INSERT INTO s1ovf (id, a) VALUES (1, 3000000000)", state: "22003"},
		{name: "int32 below the range", sql: "INSERT INTO s1ovf (id, a) VALUES (1, -2147483649)", state: "22003"},
		{name: "port above the range", sql: "INSERT INTO s1ovf (id, p) VALUES (1, 3000000000)", state: "22003"},
		{name: "protocol above the range", sql: "INSERT INTO s1ovf (id, pr) VALUES (1, 3000000000)", state: "22003"},
		{name: "int32 at the maximum", sql: "INSERT INTO s1ovf (id, a) VALUES (1, 2147483647)"},
		{name: "int32 at the minimum", sql: "INSERT INTO s1ovf (id, a) VALUES (1, -2147483648)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.state
			if want == "" {
				want = "<ok>"
			}
			for door, got := range map[string]string{
				"embedded": embeddedInsertState(t, create, tc.sql),
				"HTTP":     httpInsertState(t, create, tc.sql),
			} {
				if got != want {
					t.Errorf("%s door: %s\n  answered %s, want %s", door, tc.sql, got, want)
				}
			}
			// The gRPC door renders a refusal as a status message and does
			// NOT carry the SQLSTATE for a Query error (sqlErrorText is
			// applied to CreateTable and not to Query) — a doors gap, not
			// this one, so the assertion here is the DISPOSITION: refused
			// where PostgreSQL refuses, accepted where it accepts.
			gotGRPC := grpcInsertState(t, create, tc.sql)
			refusedGRPC := gotGRPC != "<ok>"
			if refusedGRPC != (tc.state != "") {
				t.Errorf("gRPC door: %s\n  answered %s, want %s", tc.sql,
					map[bool]string{true: "a refusal", false: "success"}[refusedGRPC],
					map[bool]string{true: "a refusal", false: "success"}[tc.state != ""])
			}
		})
	}
}

func embeddedInsertState(t *testing.T, create, sql string) string {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(ctx, create); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if _, err := db.Execute(ctx, sql); err != nil {
		return doorStateOrNone(err)
	}
	return "<ok>"
}

func httpInsertState(t *testing.T, create, sql string) string {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := New(Config{Addr: ":0", Catalog: cat}, logger)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()
	if _, _, _, err := postDoorSQL(ts.URL, create); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	_, state, _, err := postDoorSQL(ts.URL, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if state == "" {
		return "<ok>"
	}
	return state
}

func grpcInsertState(t *testing.T, create, sql string) string {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g := NewGRPCServer(GRPCConfig{Catalog: db.Catalog(), DB: db}, slog.Default())
	if _, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: create}); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if _, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: sql}); err != nil {
		return doorStateOrNone(err)
	}
	return "<ok>"
}
