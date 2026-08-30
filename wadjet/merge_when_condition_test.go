package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// mergeOn is the header every case below shares: pr aliased t, src aliased s,
// joined on id. pr = (1,10,a) (2,20,b) (3,30,c); src = (1,100,x) (4,400,y),
// so exactly one source row matches and exactly one does not.
const mergeOn = "MERGE INTO pr AS t USING src AS s ON t.id = s.id"

// A WHEN clause's `AND <condition>` decides whether the clause FIRES.
//
// It was parsed and then never read: parseMerge stored it on the clause and
// executeMerge fired the first clause of the right kind whatever the condition
// said. `WHEN MATCHED AND s.n > 1000 THEN DELETE` deleted the row on a
// condition that is false and reported MERGE 1, where PostgreSQL 17.11
// reports MERGE 0 and touches nothing (#686 review F2). Every action and both
// kinds of clause had it, so an ordinary conditional MERGE — the reason the
// syntax exists — silently did the wrong thing.
//
// PostgreSQL's rule, which this now implements: the clauses of the matching
// kind are tried IN ORDER and the FIRST whose condition holds fires; a
// condition that is false, or NULL, does not hold; if none holds the row is
// left alone. Every expectation below was read off postgres:17-alpine.
func TestMergeWhenConditionDecidesWhetherTheClauseFires(t *testing.T) {
	insert := " THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)"
	for _, tc := range []struct {
		name string
		sql  string
		tag  string
		rows []string
	}{
		{name: "MATCHED AND false, DELETE",
			sql: mergeOn + " WHEN MATCHED AND s.n > 1000 THEN DELETE",
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "MATCHED AND true, DELETE",
			sql: mergeOn + " WHEN MATCHED AND s.n > 10 THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "MATCHED AND false, UPDATE",
			sql: mergeOn + " WHEN MATCHED AND s.n > 1000 THEN UPDATE SET n = s.n",
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "MATCHED AND true, UPDATE",
			sql: mergeOn + " WHEN MATCHED AND s.n > 10 THEN UPDATE SET n = s.n",
			tag: "MERGE 1", rows: []string{"1:100:a", "2:20:b", "3:30:c"}},
		// NULL is not TRUE, and PostgreSQL does not fire on it.
		{name: "MATCHED AND NULL",
			sql: mergeOn + " WHEN MATCHED AND NULL THEN DELETE",
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		// The condition sees the TARGET's columns as well as the source's.
		{name: "MATCHED AND a target column, true",
			sql: mergeOn + " WHEN MATCHED AND t.n < 15 THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "MATCHED AND a target column, false",
			sql: mergeOn + " WHEN MATCHED AND t.n > 15 THEN DELETE",
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "NOT MATCHED AND false, INSERT",
			sql: mergeOn + " WHEN NOT MATCHED AND s.n > 1000" + insert,
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "NOT MATCHED AND true, INSERT",
			sql: mergeOn + " WHEN NOT MATCHED AND s.n > 10" + insert,
			tag: "MERGE 1", rows: []string{"1:10:a", "2:20:b", "3:30:c", "4:400:y"}},

		// Clause ORDER. The first clause whose condition holds fires — which
		// is only observable once conditions are read at all.
		{name: "a false clause is skipped for the next one",
			sql: mergeOn + " WHEN MATCHED AND s.n > 1000 THEN DELETE WHEN MATCHED THEN UPDATE SET n = 0",
			tag: "MERGE 1", rows: []string{"1:0:a", "2:20:b", "3:30:c"}},
		{name: "a true clause wins over the one after it",
			sql: mergeOn + " WHEN MATCHED AND s.n > 10 THEN DELETE WHEN MATCHED THEN UPDATE SET n = 0",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "two conditions, the second one holds",
			sql: mergeOn + " WHEN MATCHED AND s.n > 1000 THEN DELETE WHEN MATCHED AND s.n > 10 THEN UPDATE SET n = 7",
			tag: "MERGE 1", rows: []string{"1:7:a", "2:20:b", "3:30:c"}},
		{name: "no clause holds, nothing happens",
			sql: mergeOn + " WHEN MATCHED AND s.n > 1000 THEN DELETE WHEN NOT MATCHED AND s.n > 1000" + insert,
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},

		// An unconditional clause still fires, which is the shape every
		// existing MERGE test uses.
		{name: "no condition at all still fires",
			sql: mergeOn + " WHEN MATCHED THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

// The refusals around a MERGE's conditions, ON clause and SET values.
//
// Two of these were messages naming a column nobody wrote: parseOnKeys split
// the ON clause's raw TEXT on the literal " AND " and then on the first "=",
// so `ON t.id <= s.id` split inside "<=" and reported 42703 for a column
// called `t.id <` (#336's mechanism, one clause over — #686 review F3b).
func TestMergeRefusalsMatchPostgres(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		state string
		// substr is a fragment the message must contain, so a refusal that
		// names the wrong token fails even though the SQLSTATE is right.
		substr string
	}{
		{name: "condition names an unknown column",
			sql:   mergeOn + " WHEN MATCHED AND s.nosuch > 1 THEN DELETE",
			state: "42703", substr: "s.nosuch"},
		{name: "condition with a trailing token",
			sql:   mergeOn + " WHEN MATCHED AND s.n > 10 garbage THEN DELETE",
			state: "42601", substr: "garbage"},
		{name: "SET value with a trailing token",
			sql:   mergeOn + " WHEN MATCHED THEN UPDATE SET n = s.n garbage",
			state: "42601", substr: "garbage"},
		{name: "ON with a trailing token",
			sql:   "MERGE INTO pr AS t USING src AS s ON t.id = s.id garbage WHEN MATCHED THEN DELETE",
			state: "42601", substr: "garbage"},
		// PostgreSQL ACCEPTS a non-equi ON (it fails only if a target row is
		// matched twice), so this is an unimplemented feature, not bad SQL.
		{name: "non-equi ON",
			sql:   "MERGE INTO pr AS t USING src AS s ON t.id <= s.id WHEN MATCHED THEN DELETE",
			state: "0A000", substr: "equality"},
		{name: "ON qualifier naming neither relation",
			sql:   "MERGE INTO pr AS t USING src AS s ON t.id = b.id WHEN MATCHED THEN DELETE",
			state: "42P01", substr: `"b"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			before := aliasRows686(t, db)
			res, err := db.Execute(context.Background(), tc.sql)
			if err == nil {
				t.Fatalf("%s answered %s %d; it must be refused", tc.sql, res.Command, res.RowsAffected)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
			}
			if !strings.Contains(err.Error(), tc.substr) {
				t.Errorf("%s: message %q does not name %q", tc.sql, err.Error(), tc.substr)
			}
			if after := aliasRows686(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused MERGE changed pr: %v -> %v", before, after)
			}
		})
	}
}

// A statement PostgreSQL accepts whose FEATURE this server has not
// implemented is 0A000, not a syntax error — and it must still refuse.
//
// `DELETE FROM pr RETURNING *` used to delete every row and hand the client
// back no rows at all: RETURNING was read as the table's ALIAS and the rest
// of the statement was dropped. ONLY had the mirror problem — it became the
// table NAME, so the statement failed with `table "ONLY" not found` carrying
// no SQLSTATE.
func TestDMLUnimplementedClausesAreReported(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		state string
		tag   string // non-empty when the statement must SUCCEED
		rows  []string
	}{
		{name: "DELETE RETURNING", sql: "DELETE FROM pr RETURNING *", state: "0A000",
			rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE aliased RETURNING", sql: "DELETE FROM pr AS a RETURNING *", state: "0A000",
			rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		// ONLY means "do not descend into inheritance children"; this server
		// has none, so every table is only itself and the keyword is accepted.
		{name: "DELETE FROM ONLY", sql: "DELETE FROM ONLY pr WHERE id = 1", tag: "DELETE 1",
			rows: []string{"2:20:b", "3:30:c"}},
		{name: "UPDATE ONLY", sql: "UPDATE ONLY pr SET n = 9 WHERE id = 1", tag: "UPDATE 1",
			rows: []string{"1:9:a", "2:20:b", "3:30:c"}},
		{name: "ONLY with an alias", sql: "DELETE FROM ONLY pr AS a WHERE a.id = 1", tag: "DELETE 1",
			rows: []string{"2:20:b", "3:30:c"}},
		// A quoted "returning" is a NAME, so it stays a usable alias.
		{name: "quoted RETURNING is an alias", sql: `DELETE FROM pr AS "returning" WHERE "returning".id = 1`,
			tag: "DELETE 1", rows: []string{"2:20:b", "3:30:c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			res, err := db.Execute(context.Background(), tc.sql)
			if tc.tag != "" {
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
					t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
				}
			} else {
				if err == nil {
					t.Fatalf("%s answered %s %d; it must be refused",
						tc.sql, res.Command, res.RowsAffected)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
			}
			if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}

// The MERGE target may be schema-qualified, the way a PostgreSQL client
// writes it. Reading only the first identifier made `MERGE INTO public.pr` a
// merge into a table named "public", and the failure then named the DOT
// ("MERGE: expected USING") rather than the qualifier.
func TestMergeTargetMayBeSchemaQualified(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		tag   string
		state string
		rows  []string
	}{
		{sql: "MERGE INTO public.pr AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
		{sql: "MERGE INTO nosuchschema.pr AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			db := aliasDB686(t)
			res, err := db.Execute(context.Background(), tc.sql)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s answered %s %d; want %s", tc.sql, res.Command, res.RowsAffected, tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
			} else {
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
					t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
				}
			}
			if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}

// A WHEN condition must be BOOLEAN, and the refusal comes before any row is
// touched.
//
// A non-boolean condition was read as FALSE — `compiled.Eval(...).(bool)`
// discarded the failed assertion — so the clause did not fire and the NEXT
// one did:
//
//	WHEN MATCHED AND s.n THEN DELETE WHEN MATCHED THEN UPDATE SET n = 5
//
// rewrote the row and reported MERGE 1, where PostgreSQL 17.11 raises 42804
// and writes nothing (#686 R2-1). The clause that fired was not the clause
// the statement named, which is worse than a wrong count.
//
// PostgreSQL's answers, all read off 17.11: a non-boolean TYPED expression is
// 42804, while an untyped STRING literal is cast to boolean instead — so
// `AND 'true'` fires, `AND 'false'` does not, and `AND 'x'` is 22P02.
func TestMergeWhenConditionMustBeBoolean(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		state string
		tag   string
		rows  []string
	}{
		{name: "an integer column, with a second clause behind it",
			sql:   mergeOn + " WHEN MATCHED AND s.n THEN DELETE WHEN MATCHED THEN UPDATE SET n = 5",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "an integer column", sql: mergeOn + " WHEN MATCHED AND s.n THEN DELETE",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "a text column", sql: mergeOn + " WHEN MATCHED AND s.name THEN DELETE",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "a target column", sql: mergeOn + " WHEN MATCHED AND t.n THEN DELETE",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "a numeric literal", sql: mergeOn + " WHEN MATCHED AND 1 THEN DELETE",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "arithmetic", sql: mergeOn + " WHEN MATCHED AND s.n + 1 THEN DELETE",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "a NOT MATCHED clause has the same rule",
			sql:   mergeOn + " WHEN NOT MATCHED AND s.n THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
			state: "42804", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "a string that does not spell a boolean",
			sql:   mergeOn + " WHEN MATCHED AND 'x' THEN DELETE",
			state: "22P02", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},

		// The literal CAST PostgreSQL performs on an untyped string.
		{name: "the string 'true' fires", sql: mergeOn + " WHEN MATCHED AND 'true' THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
		{name: "the string 'false' does not", sql: mergeOn + " WHEN MATCHED AND 'false' THEN DELETE",
			tag: "MERGE 0", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "a boolean literal still fires", sql: mergeOn + " WHEN MATCHED AND true THEN DELETE",
			tag: "MERGE 1", rows: []string{"2:20:b", "3:30:c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			res, err := db.Execute(context.Background(), tc.sql)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s answered %s %d; PostgreSQL refuses it with %s",
						tc.sql, res.Command, res.RowsAffected, tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
			} else {
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
					t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
				}
			}
			if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}

// A WHEN NOT MATCHED clause has no target row, so naming the target is 42P01.
//
// Both halves resolved against the MERGED namespace, so `t.n` resolved and
// then evaluated to NULL against a source-only row: the condition came out
// false and the clause silently did not fire, and an INSERT value read NULL
// (#686 R2-2). PostgreSQL raises "invalid reference to FROM-clause entry for
// table t" for both.
func TestMergeNotMatchedClauseCannotNameTheTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"in the condition",
			mergeOn + " WHEN NOT MATCHED AND t.n > 1 THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)"},
		{"in an INSERT value",
			mergeOn + " WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, t.n, s.name)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			before := aliasRows686(t, db)
			res, err := db.Execute(context.Background(), tc.sql)
			if err == nil {
				t.Fatalf("%s answered %s %d; PostgreSQL refuses it with 42P01",
					tc.sql, res.Command, res.RowsAffected)
			}
			if got := sqlerr.StateOf(err); got != "42P01" {
				t.Errorf("%s: SQLSTATE %q, want 42P01 (err: %v)", tc.sql, got, err)
			}
			if after := aliasRows686(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused MERGE changed pr: %v -> %v", before, after)
			}
		})
	}
}

// The SOURCE is still reachable from a NOT MATCHED clause, which is the whole
// point of one — so the guard above must not have closed the ordinary case.
func TestMergeNotMatchedClauseStillReadsTheSource(t *testing.T) {
	db := aliasDB686(t)
	sql := mergeOn + " WHEN NOT MATCHED AND s.n > 1 THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)"
	res, err := db.Execute(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != "MERGE 1" {
		t.Errorf("command tag %q, want MERGE 1", got)
	}
	want := []string{"1:10:a", "2:20:b", "3:30:c", "4:400:y"}
	if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("left pr as %v, want %v", got, want)
	}
}

// PostgreSQL 15+ WHEN NOT MATCHED BY SOURCE / BY TARGET. They walk DIFFERENT
// row sets, so reading past the BY and treating the clause as an ordinary NOT
// MATCHED would act on the wrong rows. An unimplemented FEATURE is 0A000 and
// a refusal — not the 42601 "expected THEN" it used to give, which named the
// wrong problem (#686 R2-3, wadjet#718).
func TestMergeNotMatchedBySourceIsReportedAsUnsupported(t *testing.T) {
	for _, sql := range []string{
		mergeOn + " WHEN NOT MATCHED BY SOURCE THEN DELETE",
		mergeOn + " WHEN NOT MATCHED BY TARGET THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
	} {
		t.Run(sql, func(t *testing.T) {
			db := aliasDB686(t)
			before := aliasRows686(t, db)
			if _, err := db.Execute(context.Background(), sql); err == nil {
				t.Fatalf("%s ran; it is not implemented and must be refused", sql)
			} else if got := sqlerr.StateOf(err); got != "0A000" {
				t.Errorf("SQLSTATE %q, want 0A000 (err: %v)", got, err)
			}
			if after := aliasRows686(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused MERGE changed pr: %v -> %v", before, after)
			}
		})
	}
}

// RETURNING is a legal clause this server has not implemented, on every DML
// statement that accepts one — so it is 0A000 everywhere and it REFUSES.
//
// It used to answer three different ways depending on where the clause landed
// in the statement text (#686 R2-4): bare `DELETE FROM pr RETURNING *` took it
// as the table's ALIAS and deleted every row; `DELETE ... WHERE id = 1
// RETURNING *` fed it to the WHERE's complete-parse and called legal SQL a
// 42601 syntax error; and `INSERT ... RETURNING id` dropped it in SILENCE and
// reported INSERT 1, so a client waiting for the inserted key got a success
// and no rows.
func TestReturningIsReportedAsUnsupportedOnEveryStatement(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"DELETE, bare", "DELETE FROM pr RETURNING *"},
		{"DELETE, aliased", "DELETE FROM pr AS a RETURNING *"},
		{"DELETE, after a WHERE", "DELETE FROM pr WHERE id = 1 RETURNING *"},
		{"UPDATE, no WHERE", "UPDATE pr SET n = 9 RETURNING id"},
		{"UPDATE, after a WHERE", "UPDATE pr SET n = 9 WHERE id = 1 RETURNING id"},
		{"INSERT", "INSERT INTO pr (id, n, name) VALUES (9, 90, 'z') RETURNING id"},
		{"MERGE", mergeOn + " WHEN MATCHED THEN DELETE RETURNING t.id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			before := aliasRows686(t, db)
			res, err := db.Execute(context.Background(), tc.sql)
			if err == nil {
				t.Fatalf("%s answered %s %d; RETURNING is not implemented and must be refused",
					tc.sql, res.Command, res.RowsAffected)
			}
			if got := sqlerr.StateOf(err); got != "0A000" {
				t.Errorf("%s: SQLSTATE %q, want 0A000 (err: %v)", tc.sql, got, err)
			}
			if after := aliasRows686(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused statement changed pr: %v -> %v", before, after)
			}
		})
	}
}

// A DOUBLE-QUOTED "returning" is a NAME, not the clause — PostgreSQL reserves
// the word, so that is the only way to spell a column or alias called it, and
// the statement-wide check must not swallow it.
func TestQuotedReturningIsStillAName(t *testing.T) {
	db := aliasDB686(t)
	sql := `DELETE FROM pr AS "returning" WHERE "returning".id = 1`
	res, err := db.Execute(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != "DELETE 1" {
		t.Errorf("command tag %q, want DELETE 1", got)
	}
	want := []string{"2:20:b", "3:30:c"}
	if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("left pr as %v, want %v", got, want)
	}
}

// mergeR3DB adds a fourth column and gives BOTH tables the same column names,
// so a BARE name in a clause is a name both relations spell — the shape the
// scope rule is observable on.
func mergeR3DB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE pr (id INT64, n INT64, name STRING, flag BOOL)",
		"CREATE TABLE src (id INT64, n INT64, name STRING, flag BOOL)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"INSERT INTO pr VALUES (1, 10, 'a', true)",
		"INSERT INTO pr VALUES (2, 20, 'b', true)",
		"INSERT INTO pr VALUES (3, 30, 'c', true)",
		"INSERT INTO src VALUES (1, 100, 'x', true)",
		"INSERT INTO src VALUES (4, 400, 'y', true)",
	} {
		if _, err := db.Execute(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func mergeR3Rows(t *testing.T, db *DB) []string {
	t.Helper()
	res, err := db.Query(context.Background(), "SELECT id, n FROM pr")
	if err != nil {
		t.Fatalf("reading pr back: %v", err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, fmt.Sprintf("%v:%v", r["id"], r["n"]))
	}
	sort.Strings(out)
	return out
}

// A WHEN NOT MATCHED clause resolves names against the SOURCE ALONE, because
// PostgreSQL removes the target from SCOPE there rather than merely forbidding
// it by name.
//
// The first implementation of the rule rejected a QUALIFIED target reference
// and then still resolved against the merged namespace — so a BARE name that
// both tables spell came back 42702 "ambiguous", where PostgreSQL resolves it
// to the source and runs the statement (#686 R3-1). The bare names are the
// half that matters in practice: an unaliased MERGE between two tables with
// the same column names is the ordinary upsert shape.
//
// Under a MATCHED clause the same bare name IS ambiguous, because there both
// relations are in scope — so the rule is per clause KIND, and both halves are
// asserted here. Every expectation read off postgres:17-alpine.
func TestMergeNotMatchedResolvesBareNamesAgainstTheSource(t *testing.T) {
	unaliased := "MERGE INTO pr USING src ON pr.id = src.id"
	aliased := "MERGE INTO pr AS t USING src AS s ON t.id = s.id"
	for _, tc := range []struct {
		name  string
		sql   string
		tag   string
		state string
		rows  []string
	}{
		{name: "bare name in the condition",
			sql: unaliased + " WHEN NOT MATCHED AND n > 1 THEN INSERT (id, n) VALUES (src.id, src.n)",
			tag: "MERGE 1", rows: []string{"1:10", "2:20", "3:30", "4:400"}},
		{name: "bare boolean column as the whole condition",
			sql: unaliased + " WHEN NOT MATCHED AND flag THEN INSERT (id, n) VALUES (src.id, src.n)",
			tag: "MERGE 1", rows: []string{"1:10", "2:20", "3:30", "4:400"}},
		{name: "bare names in the INSERT values",
			sql: unaliased + " WHEN NOT MATCHED THEN INSERT (id, n) VALUES (id, n)",
			tag: "MERGE 1", rows: []string{"1:10", "2:20", "3:30", "4:400"}},
		{name: "bare name in the condition, aliased relations",
			sql: aliased + " WHEN NOT MATCHED AND n > 1 THEN INSERT (id, n) VALUES (s.id, s.n)",
			tag: "MERGE 1", rows: []string{"1:10", "2:20", "3:30", "4:400"}},

		// The other half of the rule: under MATCHED both relations are in
		// scope, so the same bare name is ambiguous.
		{name: "the same bare name under MATCHED is ambiguous",
			sql:   unaliased + " WHEN MATCHED AND n > 1 THEN DELETE",
			state: "42702", rows: []string{"1:10", "2:20", "3:30"}},
		// A name the SOURCE does not have is 42703, not a silent NULL.
		{name: "a bare name the source does not have",
			sql:   aliased + " WHEN NOT MATCHED AND nosuch > 1 THEN INSERT (id, n) VALUES (s.id, s.n)",
			state: "42703", rows: []string{"1:10", "2:20", "3:30"}},
		// The qualified half still holds.
		{name: "a qualified target reference in the condition",
			sql:   aliased + " WHEN NOT MATCHED AND t.n > 1 THEN INSERT (id, n) VALUES (s.id, s.n)",
			state: "42P01", rows: []string{"1:10", "2:20", "3:30"}},
		{name: "a qualified target reference in the values",
			sql:   aliased + " WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, t.n)",
			state: "42P01", rows: []string{"1:10", "2:20", "3:30"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := mergeR3DB(t)
			res, err := db.Execute(context.Background(), tc.sql)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s answered %s %d; PostgreSQL refuses it with %s",
						tc.sql, res.Command, res.RowsAffected, tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
			} else {
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
					t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
				}
			}
			if got := mergeR3Rows(t, db); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}

// Only the NOT MATCHED forms of BY SOURCE / BY TARGET exist. `WHEN MATCHED BY
// SOURCE` is not an unimplemented feature, it is not SQL — PostgreSQL answers
// it with a syntax error at the BY, and reporting 0A000 would have told a user
// to wait for a feature that is never coming (#686 R3 small item).
func TestMergeMatchedByIsASyntaxError(t *testing.T) {
	unaliased := "MERGE INTO pr USING src ON pr.id = src.id"
	for _, tc := range []struct {
		sql   string
		state string
	}{
		{unaliased + " WHEN MATCHED BY SOURCE THEN DELETE", "42601"},
		{unaliased + " WHEN MATCHED BY TARGET THEN DELETE", "42601"},
		// The NOT MATCHED forms stay 0A000: those are real clause kinds this
		// server has not implemented (wadjet#718).
		{unaliased + " WHEN NOT MATCHED BY SOURCE THEN DELETE", "0A000"},
		{unaliased + " WHEN NOT MATCHED BY TARGET THEN INSERT (id, n) VALUES (src.id, src.n)", "0A000"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			db := mergeR3DB(t)
			before := mergeR3Rows(t, db)
			if _, err := db.Execute(context.Background(), tc.sql); err == nil {
				t.Fatalf("%s ran; it must be refused", tc.sql)
			} else if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
			}
			if after := mergeR3Rows(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused MERGE changed pr: %v -> %v", before, after)
			}
		})
	}
}
