package pgwire

// The census's PostgreSQL arm: the same shapes, the same digest, run against a
// live PostgreSQL 17 so the `pg` column of the corpus is re-verifiable rather
// than remembered.
//
// A rule read off documentation is a hypothesis; the server is the authority
// (docs/design/correctness-fix-protocol.md, method 1). Every `pg` string in
// dml_census_test.go was produced by this test, and this test re-checks them
// whenever an oracle is reachable — so an entry that was mis-transcribed, or
// one whose PostgreSQL answer changes across releases, is caught here instead
// of silently becoming the thing wadjet is measured against.
//
// It SKIPS when no server is reachable, so CI is unaffected:
//
//	WADJET_PG_ORACLE_DSN=postgres://wadjet:wadjet@127.0.0.1:55432/wadjet_oracle?sslmode=disable \
//	  go test -run TestDMLCensusMatchesPostgres ./internal/server/pgwire/
//
// The fixture is created inside a transaction that is ROLLED BACK, and every
// statement runs inside a savepoint that is rolled back after it, so the
// oracle database is left exactly as it was found.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// censusPGFixture is censusFixture in PostgreSQL's spelling. `COLLATE "C"` on
// the text column is not decoration: wadjet compares strings raw BYTES, and
// without it PostgreSQL's default collation makes `name > '5'` a different
// question (ADR-0012; the pg-oracle fixture does the same).
var censusPGFixture = []string{
	`CREATE TEMP TABLE arcb_pr (id bigint NOT NULL, n bigint, name text COLLATE "C")`,
	`CREATE TEMP TABLE arcb_src (id bigint, n bigint, name text COLLATE "C")`,
	`CREATE TEMP TABLE arcb_dup (id bigint, n bigint, name text COLLATE "C")`,
	`CREATE TEMP TABLE arcb_fl (id bigint, f double precision, n bigint)`,
	`CREATE TEMP TABLE arcb_ts (id bigint, t timestamp)`,
	`CREATE TEMP TABLE arcb_empty (id bigint, n bigint, name text COLLATE "C")`,
	`CREATE TEMP TABLE arcb_mix (id bigint, ts timestamp, flag boolean, ip inet, raw bytea, d numeric(9,2))`,
}

func TestDMLCensusMatchesPostgres(t *testing.T) {
	dsn := os.Getenv("WADJET_PG_ORACLE_DSN")
	if dsn == "" {
		t.Skip("WADJET_PG_ORACLE_DSN not set; the census's PostgreSQL arm needs a live PostgreSQL 17")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("no PostgreSQL oracle at %s: %v", dsn, err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Everything below happens inside this transaction and is rolled back.
	defer tx.Rollback(ctx) //nolint:errcheck // the rollback IS the cleanup

	for _, stmt := range append(append([]string{}, censusPGFixture...), censusSeed...) {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			t.Fatalf("PostgreSQL fixture %q: %v", stmt, err)
		}
	}

	record := os.Getenv("WADJET_CENSUS_RECORD") != ""
	var recorded []string
	for _, sh := range censusShapes() {
		got := censusPGAnswer(ctx, t, tx, sh, pgx.QueryExecModeSimpleProtocol)
		// An entry that declares a PROTOCOL SPLIT gets both halves measured.
		// Only those: running every shape twice would surface unrelated
		// simple-vs-extended differences this corpus does not claim anything
		// about, and the split is a property of multi-statement strings alone.
		gotOne := ""
		if sh.pgOne != "" {
			gotOne = censusPGAnswer(ctx, t, tx, sh, pgx.QueryExecModeExec)
		}
		if record {
			line := fmt.Sprintf("%s\n\tpg=%s", sh.name, got)
			if sh.pgOne != "" {
				line += fmt.Sprintf("\n\tpgOne=%s", gotOne)
			}
			recorded = append(recorded, line)
			continue
		}
		if got != sh.pg {
			t.Errorf("%s\n  PostgreSQL 17 answers %s\n  the census records    %s\n"+
				"  The corpus's PG column is wrong; fix the entry, not the engine.", sh.sql, got, sh.pg)
		}
		if sh.pgOne != "" && gotOne != sh.pgOne {
			t.Errorf("%s\n  PostgreSQL 17 answers %s on the EXTENDED protocol\n"+
				"  the census records    %s\n"+
				"  The corpus's pgOne column is wrong; fix the entry, not the engine.",
				sh.sql, gotOne, sh.pgOne)
		}
	}
	if record {
		// See the wadjet arm: a recording asserts nothing and must not be
		// readable as a pass (review P20).
		t.Logf("recorded PostgreSQL census:\n%s", strings.Join(recorded, "\n"))
		t.Fatal("WADJET_CENSUS_RECORD was set: this run RECORDED PostgreSQL's answers and asserted nothing.")
	}
}

// censusPGAnswer runs one shape inside a savepoint, in the requested protocol
// mode, and rolls it back.
//
// The MODE is a parameter because for a multi-statement string PostgreSQL's
// own answer depends on it: the simple query protocol runs the sequence, and
// every one-statement entry point answers 42601. Recording only one of those
// and comparing all three wadjet doors to it would have blamed wadjet for
// reproducing PostgreSQL faithfully.
func censusPGAnswer(ctx context.Context, t *testing.T, tx pgx.Tx, sh censusShape, mode pgx.QueryExecMode) string {
	t.Helper()
	if _, err := tx.Exec(ctx, "SAVEPOINT census"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT census"); err != nil {
			t.Fatalf("rolling back after %q: %v", sh.sql, err)
		}
	}()

	if sh.tbl == "" {
		cols, rows, err := censusPGRows(ctx, tx, sh.sql, mode)
		if err != nil {
			return "state=" + censusState(err)
		}
		return "rows=" + censusDigest(cols, rows)
	}

	head := ""
	if tag, err := censusPGExec(ctx, tx, sh.sql, mode); err != nil {
		// The statement aborted the savepoint's subtransaction; the digest
		// below cannot run until it is released, so report the class alone
		// beside the table state the fixture guarantees is untouched.
		state := censusState(err)
		if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT census"); rbErr != nil {
			t.Fatalf("rolling back after %q: %v", sh.sql, rbErr)
		}
		cols, rows, dErr := censusPGRows(ctx, tx, censusDigestSQL[sh.tbl], pgx.QueryExecModeSimpleProtocol)
		if dErr != nil {
			t.Fatalf("digest after %q: %v", sh.sql, dErr)
		}
		return "state=" + state + " table=" + censusDigest(cols, rows)
	} else {
		head = "tag=" + tag
	}
	cols, rows, err := censusPGRows(ctx, tx, censusDigestSQL[sh.tbl], pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatalf("digest after %q: %v", sh.sql, err)
	}
	return head + " table=" + censusDigest(cols, rows)
}

// censusPGExec runs one statement against PostgreSQL and returns its COMMAND
// TAG, in the requested protocol mode. It is the PostgreSQL twin of
// censusWireExec, and it is a separate function for the same reason: pgx v5
// short-circuits a zero-argument Exec onto the SIMPLE protocol whatever the
// mode says, so an Exec-based "extended" arm silently measures the simple door.
func censusPGExec(ctx context.Context, tx pgx.Tx, sql string, mode pgx.QueryExecMode) (string, error) {
	if mode == pgx.QueryExecModeSimpleProtocol {
		tag, err := tx.Exec(ctx, sql, mode)
		if err != nil {
			return "", err
		}
		return tag.String(), nil
	}
	rows, err := tx.Query(ctx, sql, mode)
	if err != nil {
		return "", err
	}
	for rows.Next() {
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	return rows.CommandTag().String(), nil
}

func censusPGRows(ctx context.Context, tx pgx.Tx, sql string, mode pgx.QueryExecMode) ([]string, []map[string]any, error) {
	rows, err := tx.Query(ctx, sql, mode)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var cols []string
	for _, fd := range rows.FieldDescriptions() {
		cols = append(cols, fd.Name)
	}
	var out []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if i < len(vals) {
				row[c] = vals[i]
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, out, nil
}
