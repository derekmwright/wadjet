package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Every failure a DML door hands a client carries its SQLSTATE.
//
// #719: the four `Execute` doors (INSERT, UPDATE, DELETE, MERGE) each wrapped
// `catalog.GetTable`'s miss with `%w` and sent
// `table "x": table "x" not found` with NO class, while the FIFTH lookup in
// the same file — MERGE's SOURCE relation, which is reached through
// `db.Query` and therefore through the planner — answered 42P01 with
// PostgreSQL's own wording. One statement class, two dispositions, and a
// client that branches on 42P01 to re-resolve a name was sent nowhere.
//
// #814 is the same statement's other half and was found by the same census:
// `INSERT INTO zzp (nosuchcol) VALUES (1)` did not report the unknown column
// at all. The name went into a row map nothing would store, and the failure
// surfaced from the INGESTER as a NOT-NULL violation on `id` — a different
// column, a message that never mentions the mistyped name, and no class.
//
// Every expectation here is PostgreSQL 17's, measured live on the oracle
// (`--locale=C`, `\set VERBOSITY verbose`) rather than remembered:
//
//	INSERT INTO zzp (nosuchcol) VALUES (1)
//	  ERROR:  42703: column "nosuchcol" of relation "zzp" does not exist
//	DELETE FROM nosuchtable WHERE id = 1
//	  ERROR:  42P01: relation "nosuchtable" does not exist
//
// `Execute` is a single-process door — it is not reachable through the stage
// DAG — so this census has one arm by construction. The DAG's half of family C
// is #649, gated by coordinator.TestATaskErrorCarriesItsSQLStateOverTheDAG.
func TestEveryDMLDoorCarriesItsSQLState(t *testing.T) {
	ctx := context.Background()
	db := visibilityDB(t)
	mustDDL(t, db, "CREATE TABLE zzp (id INT64 NOT NULL, d92 DECIMAL(9,2))")
	mustExec(t, db, "INSERT INTO zzp VALUES (1, 1.25)")

	for _, tc := range []struct {
		name, sql, state string
		// msgHas is a substring the message must carry. PostgreSQL's own
		// wording where wadjet adopts it; otherwise the NAME the user got
		// wrong, which is the part a message is for.
		msgHas string
		// pgSays is PostgreSQL 17's verbatim first line, recorded beside the
		// shape so the expectation is auditable without the server.
		pgSays string
	}{
		// ---- 42P01, the four doors #719 names -----------------------------
		{name: "delete_missing_relation",
			sql: `DELETE FROM nosuchtable WHERE id = 1`, state: "42P01",
			msgHas: `relation "nosuchtable" does not exist`,
			pgSays: `42P01: relation "nosuchtable" does not exist`},
		{name: "update_missing_relation",
			sql: `UPDATE nosuchtable SET n = 1 WHERE id = 1`, state: "42P01",
			msgHas: `relation "nosuchtable" does not exist`,
			pgSays: `42P01: relation "nosuchtable" does not exist`},
		{name: "insert_missing_relation",
			sql: `INSERT INTO nosuchtable (id) VALUES (1)`, state: "42P01",
			msgHas: `relation "nosuchtable" does not exist`,
			pgSays: `42P01: relation "nosuchtable" does not exist`},
		{name: "merge_missing_target",
			sql: `MERGE INTO nosuchtable AS t USING zzp AS s ON t.id = s.id ` +
				`WHEN MATCHED THEN UPDATE SET id = s.id`, state: "42P01",
			msgHas: `relation "nosuchtable" does not exist`,
			pgSays: `42P01: relation "nosuchtable" does not exist`},
		// The CONTROL: the door that already worked, because it resolves the
		// relation through db.Query. It must keep working, and it must keep
		// saying the SAME thing the four above now say — the point of the fix
		// is that one statement class has one disposition.
		{name: "ctl_merge_missing_source",
			sql: `MERGE INTO zzp AS t USING nosuchtable AS s ON t.id = s.id ` +
				`WHEN MATCHED THEN UPDATE SET id = s.id`, state: "42P01",
			msgHas: `nosuchtable`,
			pgSays: `42P01: relation "nosuchtable" does not exist`},

		// ---- 42703, the column half ---------------------------------------
		{name: "insert_unknown_column",
			sql: `INSERT INTO zzp (nosuchcol) VALUES (1)`, state: "42703",
			msgHas: `column "nosuchcol" of relation "zzp" does not exist`,
			pgSays: `42703: column "nosuchcol" of relation "zzp" does not exist`},
		{name: "insert_unknown_column_beside_a_good_one",
			sql: `INSERT INTO zzp (id, nosuchcol) VALUES (2, 1)`, state: "42703",
			msgHas: `column "nosuchcol" of relation "zzp" does not exist`,
			pgSays: `42703: column "nosuchcol" of relation "zzp" does not exist`},
		// The two doors that already carried 42703, as controls: a fix at the
		// INSERT site must not move them.
		{name: "ctl_delete_unknown_column",
			sql: `DELETE FROM zzp WHERE nosuchcol = 1`, state: "42703",
			msgHas: `nosuchcol`,
			pgSays: `42703: column "nosuchcol" does not exist`},
		{name: "ctl_update_unknown_column",
			sql: `UPDATE zzp SET nosuchcol = 1 WHERE id = 1`, state: "42703",
			msgHas: `nosuchcol`,
			pgSays: `42703: column "nosuchcol" of relation "zzp" does not exist`},

		// ---- 22P02, the value class, as a control -------------------------
		// It already worked and localizes the fix: the class this commit adds
		// is the RELATION and COLUMN classes, not the value one.
		{name: "ctl_insert_bad_decimal_value",
			sql: `INSERT INTO zzp (id, d92) VALUES (3, 'zzz')`, state: "22P02",
			msgHas: `zzz`,
			pgSays: `22P02: invalid input syntax for type numeric: "zzz"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Execute(ctx, tc.sql)
			if err == nil {
				t.Fatalf("answered, but PostgreSQL 17 refuses this: %s\n  SQL: %s", tc.pgSays, tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE %q, want %q\n  wadjet: %v\n  PostgreSQL 17: %s\n  SQL: %s",
					got, tc.state, err, tc.pgSays, tc.sql)
			}
			if !strings.Contains(err.Error(), tc.msgHas) {
				t.Errorf("message %q does not carry %q\n  PostgreSQL 17: %s\n  SQL: %s",
					err.Error(), tc.msgHas, tc.pgSays, tc.sql)
			}
		})
	}

	// The INSERT that names a bad column must not have written anything —
	// the check has to precede the ingest, or a partially-applied statement
	// is the new defect. `insert_unknown_column_beside_a_good_one` above
	// carries id=2 in its VALUES list.
	rows := mustRows(t, db, "SELECT id FROM zzp ORDER BY id")
	if len(rows) != 1 {
		t.Errorf("zzp holds %d rows after the refused INSERTs, want 1 — a refused "+
			"statement wrote part of itself: %v", len(rows), rows)
	}
}
