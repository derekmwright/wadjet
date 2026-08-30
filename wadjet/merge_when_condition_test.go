package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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
