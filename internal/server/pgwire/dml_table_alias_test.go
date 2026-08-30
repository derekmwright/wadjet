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

	// A MERGE source: one row that matches pr686 and one that does not.
	if err := db.CreateTable(ctx, "src686", schema, nil); err != nil {
		t.Fatal(err)
	}
	sing := db.NewIngester("src686", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := sing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "n": int64(100)},
		{"id": int64(4), "n": int64(400)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sing.FlushAll(ctx); err != nil {
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

		// RETURNING is a legal clause this server has not implemented. On the
		// wire it used to answer three different ways depending on where the
		// clause landed in the statement text, and the INSERT spelling
		// answered `INSERT 0 1` with the clause dropped in silence — a client
		// waiting for the generated key got a success and no rows
		// (#686 R2-4).
		{name: "DELETE bare RETURNING", sql: "DELETE FROM pr686 RETURNING *",
			wantIDs: []int64{1, 2, 3}},
		{name: "DELETE WHERE RETURNING", sql: "DELETE FROM pr686 WHERE id = 1 RETURNING *",
			wantIDs: []int64{1, 2, 3}},
		{name: "UPDATE RETURNING", sql: "UPDATE pr686 SET n = 9 RETURNING id",
			wantIDs: []int64{1, 2, 3}},
		{name: "UPDATE WHERE RETURNING", sql: "UPDATE pr686 SET n = 9 WHERE id = 1 RETURNING id",
			wantIDs: []int64{1, 2, 3}},
		{name: "INSERT RETURNING", sql: "INSERT INTO pr686 (id, n) VALUES (9, 90) RETURNING id",
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

// A MERGE's command tag on the WIRE.
//
// pgwire matched only INSERT/UPDATE/DELETE prefixes, so every MERGE fell
// through to the QUERY path and reported `SELECT 1` — a tag naming the wrong
// statement and the wrong count. For a client the tag IS the statement's whole
// answer, so `SELECT 1` for a merge that changed nothing is a wrong answer
// delivered as a success. PostgreSQL 17.11 reports `MERGE <n>` (#686 R2-5).
func TestMergeCommandTagOnTheWire(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantTag string
		wantIDs []int64
	}{
		{name: "fires nothing", wantTag: "MERGE 0", wantIDs: []int64{1, 2, 3},
			sql: "MERGE INTO pr686 AS t USING src686 AS s ON t.id = s.id " +
				"WHEN MATCHED AND s.n > 1000 THEN DELETE"},
		{name: "deletes one", wantTag: "MERGE 1", wantIDs: []int64{2, 3},
			sql: "MERGE INTO pr686 AS t USING src686 AS s ON t.id = s.id WHEN MATCHED THEN DELETE"},
		{name: "updates one", wantTag: "MERGE 1", wantIDs: []int64{1, 2, 3},
			sql: "MERGE INTO pr686 AS t USING src686 AS s ON t.id = s.id " +
				"WHEN MATCHED THEN UPDATE SET n = s.n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := setupAliasDMLServer(t)
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
			if err != nil {
				t.Fatalf("pgx connect: %v", err)
			}
			defer conn.Close(ctx)

			tag, execErr := conn.Exec(ctx, tc.sql, pgx.QueryExecModeSimpleProtocol)
			if execErr != nil {
				t.Fatalf("%s: %v", tc.sql, execErr)
			}
			if got := tag.String(); got != tc.wantTag {
				t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.wantTag)
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
