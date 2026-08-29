package pgwire

// What the WIRE carries for an aliased DELETE or UPDATE: the command tag.
//
// `DELETE FROM t AS a WHERE a.id = 1` reported `DELETE 3` and emptied the
// table (#686). A psql or JDBC client reads that tag as the statement's whole
// answer — there is no row set to check it against — so the tag WAS the wrong
// answer, delivered as a success. PostgreSQL 17 answers `DELETE 1`.

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func setupAliasDMLServer(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, srv := setupRealDB(t)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "pr686", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("pr686", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "n": int64(10)},
		{"id": int64(2), "n": int64(20)},
		{"id": int64(3), "n": int64(30)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestAliasedDMLCommandTagOnTheWire(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantTag string  // "" when the statement must be refused
		wantIDs []int64 // pr686 afterwards, ordered by id
	}{
		{name: "DELETE AS alias", sql: "DELETE FROM pr686 AS a WHERE a.id = 1",
			wantTag: "DELETE 1", wantIDs: []int64{2, 3}},
		{name: "DELETE bare alias", sql: "DELETE FROM pr686 a WHERE a.id = 1",
			wantTag: "DELETE 1", wantIDs: []int64{2, 3}},
		{name: "UPDATE AS alias", sql: "UPDATE pr686 AS a SET n = 99 WHERE a.id = 1",
			wantTag: "UPDATE 1", wantIDs: []int64{1, 2, 3}},
		{name: "DELETE aliased, table-qualified WHERE", sql: "DELETE FROM pr686 AS a WHERE pr686.id = 1",
			wantIDs: []int64{1, 2, 3}},
		{name: "DELETE with an empty WHERE", sql: "DELETE FROM pr686 AS a WHERE",
			wantIDs: []int64{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := setupAliasDMLServer(t)
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
			if err != nil {
				t.Fatalf("pgx connect: %v", err)
			}
			defer conn.Close(ctx)

			// The simple query protocol: pgwire routes DML through
			// handleQuery, which is the path a psql client takes.
			tag, execErr := conn.Exec(ctx, tc.sql, pgx.QueryExecModeSimpleProtocol)
			if tc.wantTag == "" {
				if execErr == nil {
					t.Fatalf("%s answered %q; it must be refused", tc.sql, tag.String())
				}
			} else {
				if execErr != nil {
					t.Fatalf("%s: %v", tc.sql, execErr)
				}
				if got := tag.String(); got != tc.wantTag {
					t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.wantTag)
				}
			}

			rows, err := conn.Query(ctx, "SELECT id FROM pr686 ORDER BY id", pgx.QueryExecModeSimpleProtocol)
			if err != nil {
				t.Fatalf("reading pr686 back: %v", err)
			}
			got, err := pgx.CollectRows(rows, pgx.RowTo[int64])
			if err != nil {
				t.Fatalf("collecting rows: %v", err)
			}
			if len(got) != len(tc.wantIDs) {
				t.Fatalf("%s left ids %v, want %v", tc.sql, got, tc.wantIDs)
			}
			for i := range got {
				if got[i] != tc.wantIDs[i] {
					t.Fatalf("%s left ids %v, want %v", tc.sql, got, tc.wantIDs)
				}
			}
		})
	}
}
