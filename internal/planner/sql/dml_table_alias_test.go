package sql

import (
	"strings"
	"testing"
)

// A DELETE or an UPDATE may name its table the way a SELECT does:
// `t`, `t a`, `t AS a`, `"t"`, `schema.t` — and the alias must not end the
// statement.
//
// It did. `DELETE FROM pr AS a WHERE a.id = 1` stopped at the alias token,
// left WhereSQL EMPTY, and the executor — for which an empty clause means
// "every row" — deleted the whole table and reported DELETE 3 where
// PostgreSQL 17 reports DELETE 1 (#686). The UPDATE spelling failed loudly
// instead ("expected SET after UPDATE pr"), which is why only DELETE lost
// data.
//
// Every expectation below was read off postgres:17-alpine.
func TestDMLTableReferenceSpellings(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		table     string
		qualifier string
		alias     string
		where     string
		set       []SetClause
	}{
		// --- DELETE ---------------------------------------------------
		{name: "DELETE bare", sql: "DELETE FROM pr WHERE id = 1",
			table: "pr", where: "id = 1"},
		{name: "DELETE bare alias", sql: "DELETE FROM pr a WHERE a.id = 1",
			table: "pr", alias: "a", where: "a.id = 1"},
		{name: "DELETE AS alias", sql: "DELETE FROM pr AS a WHERE a.id = 1",
			table: "pr", alias: "a", where: "a.id = 1"},
		{name: "DELETE AS alias, unqualified WHERE", sql: "DELETE FROM pr AS a WHERE id = 1",
			table: "pr", alias: "a", where: "id = 1"},
		{name: "DELETE quoted table", sql: `DELETE FROM "pr" AS a WHERE a.id = 1`,
			table: "pr", alias: "a", where: "a.id = 1"},
		{name: "DELETE quoted alias", sql: `DELETE FROM pr AS "a" WHERE "a".id = 1`,
			table: "pr", alias: "a", where: `"a".id = 1`},
		{name: "DELETE schema-qualified", sql: "DELETE FROM public.pr AS a WHERE a.id = 1",
			table: "pr", qualifier: "public", alias: "a", where: "a.id = 1"},
		{name: "DELETE catalog.schema-qualified", sql: "DELETE FROM wadjet.public.pr WHERE id = 1",
			table: "pr", qualifier: "wadjet.public", where: "id = 1"},
		{name: "DELETE alias spelled like a column", sql: "DELETE FROM pr AS id WHERE id.id = 1",
			table: "pr", alias: "id", where: "id.id = 1"},
		{name: "DELETE alias spelled like the table", sql: "DELETE FROM pr AS pr WHERE pr.id = 1",
			table: "pr", alias: "pr", where: "pr.id = 1"},
		{name: "DELETE aliased, no WHERE", sql: "DELETE FROM pr AS a",
			table: "pr", alias: "a"},
		{name: "DELETE aliased, trailing semicolon tail", sql: "DELETE FROM pr AS a WHERE a.id = 1;",
			table: "pr", alias: "a", where: "a.id = 1"},
		{name: "DELETE mixed qualified and bare WHERE", sql: "DELETE FROM pr AS a WHERE a.id = 1 AND n = 10",
			table: "pr", alias: "a", where: "a.id = 1 AND n = 10"},

		// --- UPDATE ---------------------------------------------------
		{name: "UPDATE bare", sql: "UPDATE pr SET n = 99 WHERE id = 1",
			table: "pr", where: "id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE bare alias", sql: "UPDATE pr a SET n = 99 WHERE a.id = 1",
			table: "pr", alias: "a", where: "a.id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE AS alias", sql: "UPDATE pr AS a SET n = 99 WHERE a.id = 1",
			table: "pr", alias: "a", where: "a.id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE AS alias, unqualified WHERE", sql: "UPDATE pr AS a SET n = 99 WHERE id = 1",
			table: "pr", alias: "a", where: "id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE quoted table", sql: `UPDATE "pr" AS a SET n = 99 WHERE a.id = 1`,
			table: "pr", alias: "a", where: "a.id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE quoted alias", sql: `UPDATE pr AS "a" SET n = 99 WHERE "a".id = 1`,
			table: "pr", alias: "a", where: `"a".id = 1`, set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE schema-qualified", sql: "UPDATE public.pr AS a SET n = 99 WHERE a.id = 1",
			table: "pr", qualifier: "public", alias: "a", where: "a.id = 1",
			set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE alias spelled like a column", sql: "UPDATE pr AS id SET n = 99 WHERE id.id = 1",
			table: "pr", alias: "id", where: "id.id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE alias spelled like the table", sql: "UPDATE pr AS pr SET n = 99 WHERE pr.id = 1",
			table: "pr", alias: "pr", where: "pr.id = 1", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE aliased, no WHERE", sql: "UPDATE pr AS a SET n = 99",
			table: "pr", alias: "a", set: []SetClause{{Column: "n", Value: "99"}}},
		{name: "UPDATE aliased, SET reads the alias", sql: "UPDATE pr AS a SET n = a.n + 1 WHERE a.id = 1",
			table: "pr", alias: "a", where: "a.id = 1", set: []SetClause{{Column: "n", Value: "a . n + 1"}}},
		{name: "UPDATE aliased, two SET clauses",
			sql:   "UPDATE pr AS a SET n = 1, name = 'z' WHERE a.id = 1",
			table: "pr", alias: "a", where: "a.id = 1",
			set: []SetClause{{Column: "n", Value: "1"}, {Column: "name", Value: "'z'"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.sql, err)
			}
			var target DMLTarget
			switch {
			case q.Delete != nil:
				target = q.Delete.DMLTarget
			case q.Update != nil:
				target = q.Update.DMLTarget
				if len(q.Update.SetClauses) != len(tc.set) {
					t.Fatalf("%d SET clauses, want %d: %+v", len(q.Update.SetClauses), len(tc.set), q.Update.SetClauses)
				}
				for i, want := range tc.set {
					if got := q.Update.SetClauses[i]; got.Column != want.Column || got.Value != want.Value {
						t.Errorf("SET clause %d = %+v, want %+v", i, got, want)
					}
				}
			default:
				t.Fatalf("%q parsed as neither a DELETE nor an UPDATE", tc.sql)
			}
			if !strings.EqualFold(target.Table, tc.table) {
				t.Errorf("table = %q, want %q", target.Table, tc.table)
			}
			if !strings.EqualFold(target.Qualifier, tc.qualifier) {
				t.Errorf("qualifier = %q, want %q", target.Qualifier, tc.qualifier)
			}
			if !strings.EqualFold(target.Alias, tc.alias) {
				t.Errorf("alias = %q, want %q", target.Alias, tc.alias)
			}
			if target.WhereSQL != tc.where {
				t.Errorf("WhereSQL = %q, want %q", target.WhereSQL, tc.where)
			}
			if target.StmtSQL == "" {
				t.Errorf("StmtSQL is empty; the empty-predicate backstop has nothing to read")
			}
		})
	}
}

// A statement whose tail this parser cannot read is REFUSED, never run.
//
// Each of these used to reach the executor with an empty WhereSQL and delete
// or update every row: the leftover tokens were dropped on the floor together
// with the WHERE that followed them. PostgreSQL answers every one with a
// syntax error, and so does this.
func TestDMLRefusesAStatementTailItCannotRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"DELETE with trailing words", "DELETE FROM pr garbage garbage2"},
		{"DELETE with a doubled AS", "DELETE FROM pr AS a AS b WHERE a.id = 1"},
		{"DELETE with an empty WHERE", "DELETE FROM pr AS a WHERE"},
		{"DELETE with an empty WHERE, unaliased", "DELETE FROM pr WHERE"},
		{"DELETE with RETURNING", "DELETE FROM pr AS a RETURNING *"},
		{"DELETE AS with no alias", "DELETE FROM pr AS WHERE id = 1"},
		{"DELETE AS a reserved word", "DELETE FROM pr AS where"},
		{"DELETE with a dangling qualifier", "DELETE FROM public. WHERE id = 1"},
		{"UPDATE with an empty WHERE", "UPDATE pr AS a SET n = 1 WHERE"},
		{"UPDATE AS with no alias", "UPDATE pr AS SET n = 1"},
		{"UPDATE with a doubled AS", "UPDATE pr AS a AS b SET n = 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql)
			if err == nil {
				var where string
				if q.Delete != nil {
					where = q.Delete.WhereSQL
				} else if q.Update != nil {
					where = q.Update.WhereSQL
				}
				t.Fatalf("Parse(%q) succeeded with WhereSQL %q; a tail this parser cannot read "+
					"must not become an unconditional statement", tc.sql, where)
			}
		})
	}
}

// HasTopLevelWhereToken is the backstop's only input, so it must answer from
// the LEXER: a WHERE inside a string literal, a quoted identifier or a
// subquery is not this statement's clause.
func TestHasTopLevelWhereToken(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"DELETE FROM pr WHERE id = 1", true},
		{"DELETE FROM pr AS a WHERE a.id = 1", true},
		{"delete from pr where id = 1", true},
		{"DELETE FROM pr", false},
		{"DELETE FROM pr AS a", false},
		{"UPDATE pr SET n = 1", false},
		{"UPDATE pr SET n = 1 WHERE id = 2", true},
		{"UPDATE pr SET s = 'WHERE'", false},
		{`UPDATE pr SET "where" = 1`, false},
		{"UPDATE pr SET n = (SELECT MAX(x) FROM u WHERE u.id = 1)", false},
		{"UPDATE pr SET n = (SELECT MAX(x) FROM u WHERE u.id = 1) WHERE id = 2", true},
		{"DELETE FROM pr WHERE id IN (SELECT id FROM u WHERE u.k = 1)", true},
		{"", false},
	} {
		if got := HasTopLevelWhereToken(tc.sql); got != tc.want {
			t.Errorf("HasTopLevelWhereToken(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// MERGE reads its aliases already; what it did not do was check that AS was
// followed by a NAME, so `MERGE INTO t AS USING s ...` took "USING" as the
// alias and then failed on the USING it had just eaten.
func TestMergeRefusesAnAliasThatIsNotAName(t *testing.T) {
	for _, sql := range []string{
		"MERGE INTO pr AS USING src AS s ON pr.id = s.id WHEN MATCHED THEN DELETE",
		"MERGE INTO pr AS t USING src AS ON t.id = src.id WHEN MATCHED THEN DELETE",
	} {
		if _, err := Parse(sql); err == nil {
			t.Errorf("Parse(%q) succeeded; AS must be followed by an alias", sql)
		}
	}
}

// The aliases MERGE does read stay read, on both sides and with or without AS.
func TestMergeReadsBothAliases(t *testing.T) {
	for _, tc := range []struct {
		sql            string
		target, source string
	}{
		{"MERGE INTO pr AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN DELETE", "t", "s"},
		{"MERGE INTO pr t USING src s ON t.id = s.id WHEN MATCHED THEN DELETE", "t", "s"},
		{"MERGE INTO pr USING src ON pr.id = src.id WHEN MATCHED THEN DELETE", "", ""},
		{"MERGE INTO pr AS t USING (SELECT * FROM src) AS s ON t.id = s.id WHEN MATCHED THEN DELETE", "t", "s"},
	} {
		q, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		if q.Merge.TargetAlias != tc.target || q.Merge.SourceAlias != tc.source {
			t.Errorf("%q: aliases (%q, %q), want (%q, %q)", tc.sql,
				q.Merge.TargetAlias, q.Merge.SourceAlias, tc.target, tc.source)
		}
	}
}

// ParseExpression and ParseExpressionComplete are two contracts, and the DML
// WHERE path needs the second one.
//
// ParseExpression stops where the expression grammar stops and reports
// success, which for a WHERE clause means the clause becomes its own PREFIX —
// and since the dropped tail is a conjunct that would have NARROWED it, a
// DELETE carrying that prefix removes rows the written predicate excludes
// (#686 review). Each entry below goes through both.
func TestParseExpressionCompleteRefusesTrailingText(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		complete bool // ParseExpressionComplete accepts it
	}{
		{sql: "id = 1", complete: true},
		{sql: "(id = 1)", complete: true},
		{sql: "id = 1 AND n = 10", complete: true},
		{sql: "id = 1 OR (n > 2 AND name LIKE 'a%')", complete: true},
		{sql: "UPPER(name) = 'A'", complete: true},
		{sql: "id BETWEEN 1 AND 2", complete: true},
		{sql: "CASE WHEN id = 1 THEN true ELSE false END", complete: true},
		{sql: "id IS NOT NULL", complete: true},
		{sql: "-5 < n", complete: true},

		{sql: "id = 1 garbage"},
		{sql: "(id = 1) garbage AND name = 'zzz'"},
		{sql: "name <> 'zzz' AND id ISNULL"},
		{sql: "id > 0 AND name @@ 'zzz'"},
		{sql: "id > 0 AND n # 3"},
		{sql: "id = 1 LIMIT 1"},
		{sql: "id = 1; DELETE FROM pr"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := ParseExpressionComplete(tc.sql)
			if tc.complete && err != nil {
				t.Fatalf("ParseExpressionComplete(%q): %v; it is a whole expression", tc.sql, err)
			}
			if !tc.complete && err == nil {
				t.Fatalf("ParseExpressionComplete(%q) accepted text it did not consume", tc.sql)
			}
			if !tc.complete {
				// The PREFIX parse is what made this silent: the old caller
				// got a usable node and no error at all.
				if _, perr := ParseExpression(tc.sql); perr != nil {
					t.Logf("note: ParseExpression(%q) now fails too (%v)", tc.sql, perr)
				}
			}
		})
	}
}
