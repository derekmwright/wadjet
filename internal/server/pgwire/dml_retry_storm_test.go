package pgwire

// P2: a DML statement that keeps losing the CAS reports 40001, and leaves the
// table exactly as it found it.
//
// `wadjet/dml.go`'s 40001 is the only producer of that class in the tree and
// NOTHING exercised it: the #691 race gate fires its compaction once, so every
// case there succeeds on attempt 2 and the exhaustion arm is untested code on
// a path the ADR describes at length. That gate's own `assertRaceFired`
// comment invokes method 10 — "a 'cannot happen' needs a fixture where it is
// attempted" — and did not apply it to this one.
//
// Firing the compaction on EVERY attempt is the fixture: the statement's
// markers can never name a file the manifest still holds, so it exhausts its
// retries and has to report the retryable class rather than a wrong table or
// a blanket 42000.

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestDMLRetryStormReports40001(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "DELETE", sql: "DELETE FROM arcb_pr WHERE id = 1"},
		{name: "UPDATE", sql: "UPDATE arcb_pr SET n = 99 WHERE id = 1"},
		{name: "MERGE", sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const intact = "[1:10:a 2:20:b 3:30:c]"

			t.Run("embedded", func(t *testing.T) {
				ctx := context.Background()
				db, hook := raceDB(t)
				armEveryAttempt(t, db, hook)
				_, err := db.Execute(ctx, tc.sql)
				if err == nil {
					t.Fatalf("%s succeeded with a compaction on every attempt; it cannot have committed anything", tc.sql)
				}
				if got := sqlerr.StateOf(err); got != "40001" {
					t.Errorf("%s: SQLSTATE %q, want 40001 serialization_failure (err: %v)", tc.sql, got, err)
				}
				after, qerr := db.Query(ctx, censusDigestSQL["pr"])
				if qerr != nil {
					t.Fatal(qerr)
				}
				if got := censusDigest(after.Columns, after.Rows); got != intact {
					t.Errorf("%s left %s; a refused statement writes NOTHING, want %s", tc.sql, got, intact)
				}
			})

			for _, door := range []struct {
				name string
				mode pgx.QueryExecMode
			}{
				{"simple", pgx.QueryExecModeSimpleProtocol},
				{"extended", pgx.QueryExecModeExec},
			} {
				t.Run(door.name, func(t *testing.T) {
					ctx := context.Background()
					db, hook := raceDB(t)
					srv := NewServer(db, Config{}, nil)
					if err := srv.Start("127.0.0.1:0"); err != nil {
						t.Fatal(err)
					}
					defer srv.Shutdown()
					conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
					if err != nil {
						t.Fatalf("pgx connect: %v", err)
					}
					defer conn.Close(ctx)

					armEveryAttempt(t, db, hook)
					_, execErr := censusWireExec(ctx, conn, tc.sql, door.mode)
					if execErr == nil {
						t.Fatalf("%s succeeded with a compaction on every attempt", tc.sql)
					}
					if got := censusState(execErr); got != "40001" {
						t.Errorf("%s: SQLSTATE %q on the %s door, want 40001 (err: %v)",
							tc.sql, got, door.name, execErr)
					}
					cols, rows, qerr := censusWireRows(ctx, conn, censusDigestSQL["pr"], door.mode)
					if qerr != nil {
						t.Fatal(qerr)
					}
					if got := censusDigest(cols, rows); got != intact {
						t.Errorf("%s left %s, want %s", tc.sql, got, intact)
					}
				})
			}
		})
	}
}

// armEveryAttempt points the hook at a full table rewrite that runs on EVERY
// manifest read rather than only the first, so the statement can never commit.
func armEveryAttempt(t *testing.T, db *wadjet.DB, hook *compactingKV) {
	t.Helper()
	ctx := context.Background()
	comp := compaction.New(db.Catalog(), slog.New(slog.DiscardHandler), compaction.DefaultConfig())
	hook.armAlways(func() {
		if _, err := comp.RewriteTable(ctx, "arcb_pr"); err != nil {
			t.Errorf("mid-statement compaction: %v", err)
		}
	})
}
