package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// aliasDB686 is the fixture every case below runs against, rebuilt per case
// because each statement mutates it.
//
//	pr:  (1,10,'a') (2,20,'b') (3,30,'c')
//	src: (1,100,'x') (4,400,'y')   — one matching and one non-matching MERGE row
func aliasDB686(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE pr (id INT64, n INT64, name STRING)",
		"CREATE TABLE src (id INT64, n INT64, name STRING)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	for _, dml := range []string{
		"INSERT INTO pr VALUES (1, 10, 'a')",
		"INSERT INTO pr VALUES (2, 20, 'b')",
		"INSERT INTO pr VALUES (3, 30, 'c')",
		"INSERT INTO src VALUES (1, 100, 'x')",
		"INSERT INTO src VALUES (4, 400, 'y')",
	} {
		if _, err := db.Execute(ctx, dml); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// aliasRows686 renders pr as sorted "id:n:name" strings — the row SET, which
// is what a DML defect corrupts and what a rows-affected count alone cannot
// show.
func aliasRows686(t *testing.T, db *DB) []string {
	t.Helper()
	res, err := db.Query(context.Background(), "SELECT id, n, name FROM pr")
	if err != nil {
		t.Fatalf("reading pr back: %v", err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, fmt.Sprintf("%v:%v:%v", r["id"], r["n"], r["name"]))
	}
	sort.Strings(out)
	return out
}

// Every spelling of a DML table reference, crossed with every spelling of the
// WHERE that resolves against it, answered exactly as PostgreSQL 17.11
// answers it — the command tag, the SQLSTATE of a refusal, and the ROW SET
// left behind.
//
// The row set is the point. `DELETE FROM pr AS a WHERE a.id = 1` reported
// DELETE 3 and emptied the table (#686): the alias token ended the statement,
// WhereSQL came back empty, and an empty clause means "every row" to the
// executor. Every aliased DELETE in this table did that, including the ones
// whose WHERE names a column that does not exist — a statement PostgreSQL
// refuses deleted everything instead.
//
// The `want` column was read off a live postgres:17-alpine, one statement per
// freshly seeded table.
func TestDMLTableAliasMatchesPostgres(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		tag   string   // "" when the statement must be refused
		state string   // SQLSTATE of the refusal, "" when it must succeed
		rows  []string // pr afterwards
	}{
		// --- DELETE, the shapes that lost data ------------------------
		{name: "DELETE bare alias", sql: "DELETE FROM pr a WHERE a.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE AS alias", sql: "DELETE FROM pr AS a WHERE a.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE AS alias, unqualified WHERE", sql: "DELETE FROM pr AS a WHERE id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE AS alias, mixed WHERE", sql: "DELETE FROM pr AS a WHERE a.id = 1 AND n = 10",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE quoted table", sql: `DELETE FROM "pr" AS a WHERE a.id = 1`,
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE quoted alias", sql: `DELETE FROM pr AS "a" WHERE "a".id = 1`,
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE schema-qualified", sql: "DELETE FROM public.pr AS a WHERE a.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE alias spelled like a column", sql: "DELETE FROM pr AS id WHERE id.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE bare alias spelled like a column", sql: "DELETE FROM pr id WHERE id.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE alias spelled like the table", sql: "DELETE FROM pr AS pr WHERE pr.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},

		// --- DELETE, unaliased: unchanged --------------------------------
		{name: "DELETE unaliased", sql: "DELETE FROM pr WHERE id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE unaliased, table-qualified WHERE", sql: "DELETE FROM pr WHERE pr.id = 1",
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE unaliased, no WHERE", sql: "DELETE FROM pr",
			tag: "DELETE 3", rows: nil},
		{name: "DELETE aliased, no WHERE", sql: "DELETE FROM pr AS a",
			tag: "DELETE 3", rows: nil},

		// --- DELETE refusals ---------------------------------------------
		// An alias HIDES the table name: PostgreSQL answers 42P01 and hints
		// at the alias.
		{name: "DELETE aliased, table-qualified WHERE", sql: "DELETE FROM pr AS a WHERE pr.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE bare-aliased, table-qualified WHERE", sql: "DELETE FROM pr a WHERE pr.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE aliased, unknown qualifier", sql: "DELETE FROM pr AS a WHERE b.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE aliased, unknown column", sql: "DELETE FROM pr AS a WHERE a.nosuch = 1",
			state: "42703", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE with an empty WHERE", sql: "DELETE FROM pr AS a WHERE",
			state: "42601", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE with a tail this parser cannot read", sql: "DELETE FROM pr x y",
			state: "42601", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE unknown schema", sql: "DELETE FROM nosuchschema.pr WHERE id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},

		// --- UPDATE -------------------------------------------------------
		{name: "UPDATE bare alias", sql: "UPDATE pr a SET n = 99 WHERE a.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE AS alias", sql: "UPDATE pr AS a SET n = 99 WHERE a.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE AS alias, unqualified WHERE", sql: "UPDATE pr AS a SET n = 99 WHERE id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE AS alias, SET reads the alias", sql: "UPDATE pr AS a SET n = a.n + 1 WHERE a.id = 1",
			tag: "UPDATE 1", rows: []string{"1:11:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE quoted table", sql: `UPDATE "pr" AS a SET n = 99 WHERE a.id = 1`,
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE quoted alias", sql: `UPDATE pr AS "a" SET n = 99 WHERE "a".id = 1`,
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE schema-qualified", sql: "UPDATE public.pr AS a SET n = 99 WHERE a.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE alias spelled like a column", sql: "UPDATE pr AS id SET n = 99 WHERE id.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE bare alias spelled like a column", sql: "UPDATE pr id SET n = 99 WHERE id.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE alias spelled like the table", sql: "UPDATE pr AS pr SET n = 99 WHERE pr.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, two SET clauses", sql: "UPDATE pr AS a SET n = 1, name = 'z' WHERE a.id = 1",
			tag: "UPDATE 1", rows: []string{"1:1:z", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, no WHERE", sql: "UPDATE pr AS a SET n = 99",
			tag: "UPDATE 3", rows: []string{"1:99:a", "2:99:b", "3:99:c"}},
		{name: "UPDATE unaliased", sql: "UPDATE pr SET n = 99 WHERE id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE unaliased, table-qualified WHERE", sql: "UPDATE pr SET n = 99 WHERE pr.id = 1",
			tag: "UPDATE 1", rows: []string{"1:99:a", "2:20:b", "3:30:c"}},

		// --- UPDATE refusals ----------------------------------------------
		{name: "UPDATE aliased, table-qualified WHERE", sql: "UPDATE pr AS a SET n = 99 WHERE pr.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, table-qualified SET value", sql: "UPDATE pr AS a SET n = pr.n + 1 WHERE a.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, unknown qualifier", sql: "UPDATE pr AS a SET n = 99 WHERE b.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, unknown column in WHERE", sql: "UPDATE pr AS a SET n = 99 WHERE a.nosuch = 1",
			state: "42703", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, unknown SET target", sql: "UPDATE pr AS a SET nosuch = 9 WHERE a.id = 1",
			state: "42703", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE with an empty WHERE", sql: "UPDATE pr AS a SET n = 1 WHERE",
			state: "42601", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE unknown schema", sql: "UPDATE nosuchschema.pr SET n = 1 WHERE id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},

		// --- MERGE: the aliases it already read stay read ------------------
		{name: "MERGE both aliases",
			sql: "MERGE INTO pr AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
			tag: "MERGE 2", rows: []string{"1:100:a", "2:20:b", "3:30:c", "4:400:y"}},
		{name: "MERGE both aliases without AS",
			sql: "MERGE INTO pr t USING src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
			tag: "MERGE 2", rows: []string{"1:100:a", "2:20:b", "3:30:c", "4:400:y"}},
		{name: "MERGE unaliased",
			sql: "MERGE INTO pr USING src ON pr.id = src.id WHEN MATCHED THEN UPDATE SET n = src.n",
			tag: "MERGE 1", rows: []string{"1:100:a", "2:20:b", "3:30:c"}},
		{name: "MERGE target alias spelled like a column",
			sql: "MERGE INTO pr AS id USING src AS s ON id.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			tag: "MERGE 1", rows: []string{"1:100:a", "2:20:b", "3:30:c"}},
		{name: "MERGE source subquery",
			sql: "MERGE INTO pr AS t USING (SELECT * FROM src) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			tag: "MERGE 1", rows: []string{"1:100:a", "2:20:b", "3:30:c"}},
		{name: "MERGE matched delete",
			sql: "MERGE INTO pr AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
		// An alias hides the table name in a MERGE's ON condition too.
		{name: "MERGE aliased, target-qualified ON",
			sql:   "MERGE INTO pr AS t USING src AS s ON pr.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "MERGE aliased, source-qualified ON",
			sql:   "MERGE INTO pr AS t USING src AS s ON t.id = src.id WHEN MATCHED THEN UPDATE SET n = s.n",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "MERGE unknown qualifier in ON",
			sql:   "MERGE INTO pr AS t USING src AS s ON t.id = b.id WHEN MATCHED THEN UPDATE SET n = s.n",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			res, err := db.Execute(context.Background(), tc.sql)
			switch {
			case tc.state != "":
				if err == nil {
					t.Fatalf("%s answered %s %d; PostgreSQL refuses it with %s. pr is now %v",
						tc.sql, res.Command, res.RowsAffected, tc.state, aliasRows686(t, db))
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
			default:
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
					t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
				}
			}
			got := aliasRows686(t, db)
			if strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}

// THE EMPTY-PREDICATE BACKSTOP, at the executor.
//
// The parser now reads the alias, so the #686 spelling never reaches here —
// which is exactly why this test builds the broken input by hand instead of
// through SQL. It is the state the parser produced on the pre-fix tree: a
// statement that WRITES a WHERE beside a WhereSQL that parsed to nothing. The
// executor reads an empty clause as "every row", so accepting this input is
// how one dropped clause empties a table. Any future parser path that drops a
// clause fails the STATEMENT here rather than widening it.
func TestDMLRefusesAWhereThatParsedToNothing(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(db *DB) error
	}{
		{"DELETE", func(db *DB) error {
			_, err := db.executeDelete(ctx, &plansql.DeleteInfo{DMLTarget: plansql.DMLTarget{
				Table:   "pr",
				StmtSQL: "DELETE FROM pr AS a WHERE a.id = 1",
				// WhereSQL deliberately empty: the defect this backstops.
			}})
			return err
		}},
		{"UPDATE", func(db *DB) error {
			_, err := db.executeUpdate(ctx, &plansql.UpdateInfo{
				DMLTarget: plansql.DMLTarget{
					Table:   "pr",
					StmtSQL: "UPDATE pr AS a SET n = 99 WHERE a.id = 1",
				},
				SetClauses: []plansql.SetClause{{Column: "n", Value: "99"}},
			})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			before := aliasRows686(t, db)
			err := tc.run(db)
			if err == nil {
				t.Fatalf("a %s whose WHERE parsed to nothing ran; pr is now %v", tc.name, aliasRows686(t, db))
			}
			if got := sqlerr.StateOf(err); got != "XX000" {
				t.Errorf("SQLSTATE %q, want XX000 (err: %v)", got, err)
			}
			if after := aliasRows686(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused %s changed pr: %v -> %v", tc.name, before, after)
			}
		})
	}
}

// The backstop must not fire on a statement that is unconditional BECAUSE IT
// SAYS SO, which is legal SQL and PostgreSQL runs it.
func TestDMLWithNoWhereStaysUnconditional(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		tag  string
		rows []string
	}{
		{"DELETE FROM pr", "DELETE 3", nil},
		{"DELETE FROM pr AS a", "DELETE 3", nil},
		{"UPDATE pr SET n = 0", "UPDATE 3", []string{"1:0:a", "2:0:b", "3:0:c"}},
		{"UPDATE pr AS a SET n = 0", "UPDATE 3", []string{"1:0:a", "2:0:b", "3:0:c"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			db := aliasDB686(t)
			res, err := db.Execute(context.Background(), tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
				t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
			}
			if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}
