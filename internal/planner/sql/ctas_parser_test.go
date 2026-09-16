// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestAQueryIsParsedTheSameInsideAWriteAsOutsideIt is the parser half of
// #1024. Every cell is a statement whose SELECT must reach the ONE
// parseSelectStatement: the same CTE extraction, positional-reference
// resolution, star refusal and window collection a bare SELECT gets. The
// assertion is therefore not "it parsed" but "the inner ParsedQuery is the
// one the identical bare SELECT produces".
func TestAQueryIsParsedTheSameInsideAWriteAsOutsideIt(t *testing.T) {
	cases := []struct {
		name  string
		write string
		query string // the same query, written bare
	}{
		{"Star", `CREATE TABLE t AS SELECT * FROM src`, `SELECT * FROM src`},
		{"Expressions", `CREATE TABLE t AS SELECT id, n+1 FROM src`, `SELECT id, n+1 FROM src`},
		{"CTE", `CREATE TABLE t AS WITH c AS (SELECT 1 AS k) SELECT k FROM c`,
			`WITH c AS (SELECT 1 AS k) SELECT k FROM c`},
		{"PositionalGroupBy", `CREATE TABLE t AS SELECT n, COUNT(*) FROM src GROUP BY 1`,
			`SELECT n, COUNT(*) FROM src GROUP BY 1`},
		{"Union", `CREATE TABLE t AS SELECT id FROM src UNION SELECT n FROM src`,
			`SELECT id FROM src UNION SELECT n FROM src`},
		{"OrderByLimit", `CREATE TABLE t AS SELECT id FROM src ORDER BY id DESC LIMIT 3`,
			`SELECT id FROM src ORDER BY id DESC LIMIT 3`},
		{"Window", `CREATE TABLE t AS SELECT id, COUNT(*) OVER () AS c FROM src`,
			`SELECT id, COUNT(*) OVER () AS c FROM src`},
		{"InsertSelect", `INSERT INTO t SELECT id, n FROM src`, `SELECT id, n FROM src`},
		{"InsertSelectCTE", `INSERT INTO t (a) WITH c AS (SELECT 1 AS k) SELECT k FROM c`,
			`WITH c AS (SELECT 1 AS k) SELECT k FROM c`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrote, err := Parse(tc.write)
			if err != nil {
				t.Fatalf("parsing %q: %v", tc.write, err)
			}
			bare, err := Parse(tc.query)
			if err != nil {
				t.Fatalf("parsing %q: %v", tc.query, err)
			}
			var inner *ParsedQuery
			switch {
			case wrote.CreateTable != nil:
				inner = wrote.CreateTable.AsSelect
			case wrote.Insert != nil:
				inner = wrote.Insert.Select
			}
			if inner == nil {
				t.Fatalf("%q carries no inner query", tc.write)
			}
			if inner.SQL != tc.query {
				t.Errorf("inner SQL = %q, want %q", inner.SQL, tc.query)
			}
			if got, want := len(inner.SelectInfo.Columns), len(bare.SelectInfo.Columns); got != want {
				t.Errorf("inner SELECT list has %d items, the bare query has %d", got, want)
			}
			if got, want := len(inner.CTEs), len(bare.CTEs); got != want {
				t.Errorf("inner query has %d CTEs, the bare query has %d", got, want)
			}
			if got, want := len(inner.Windows), len(bare.Windows); got != want {
				t.Errorf("inner query has %d window specs, the bare query has %d", got, want)
			}
			if got, want := inner.SelectInfo.GroupBy, bare.SelectInfo.GroupBy; len(got) != len(want) {
				t.Errorf("inner GROUP BY = %v, the bare query's is %v", got, want)
			} else {
				for i := range got {
					if got[i] != want[i] {
						t.Errorf("inner GROUP BY[%d] = %q, the bare query's is %q — the positional "+
							"reference resolved differently inside the write", i, got[i], want[i])
					}
				}
			}
		})
	}
}

// TestTheCTASGrammarRecordsWhatTheStatementWrote covers the three modifiers
// whose meaning is a DIFFERENT statement: IF NOT EXISTS, the positional rename
// list, and WITH [NO] DATA.
func TestTheCTASGrammarRecordsWhatTheStatementWrote(t *testing.T) {
	cases := []struct {
		sql         string
		name        string
		ifNotExists bool
		renames     []string
		withData    bool
		innerSQL    string
	}{
		{`CREATE TABLE t AS SELECT 1 AS a`, "t", false, nil, true, `SELECT 1 AS a`},
		{`CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS a`, "t", true, nil, true, `SELECT 1 AS a`},
		{`CREATE TABLE t (x, y) AS SELECT 1 AS a, 2 AS b`, "t", false, []string{"x", "y"}, true, `SELECT 1 AS a, 2 AS b`},
		{`CREATE TABLE t AS SELECT 1 AS a WITH NO DATA`, "t", false, nil, false, `SELECT 1 AS a`},
		{`CREATE TABLE t AS SELECT 1 AS a WITH DATA`, "t", false, nil, true, `SELECT 1 AS a`},
		{`CREATE TABLE IF NOT EXISTS t (x) AS SELECT 1 AS a WITH NO DATA`, "t", true, []string{"x"}, false, `SELECT 1 AS a`},
		// Method 10: the impossibility this grammar asserts is that a trailing
		// WITH inside the QUERY is not the clause. Attempt it from both sides.
		{`CREATE TABLE t AS SELECT note FROM src WHERE note = 'with no data'`, "t", false, nil, true,
			`SELECT note FROM src WHERE note = 'with no data'`},
		{`CREATE TABLE t AS SELECT s FROM (SELECT s FROM src) x WITH NO DATA`, "t", false, nil, false,
			`SELECT s FROM (SELECT s FROM src) x`},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			pq, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			ct := pq.CreateTable
			if ct == nil {
				t.Fatalf("not a CREATE TABLE: %v", pq.Type)
			}
			if ct.Name != tc.name {
				t.Errorf("name = %q, want %q", ct.Name, tc.name)
			}
			if ct.IfNotExists != tc.ifNotExists {
				t.Errorf("IfNotExists = %v, want %v", ct.IfNotExists, tc.ifNotExists)
			}
			if ct.WithData != tc.withData {
				t.Errorf("WithData = %v, want %v", ct.WithData, tc.withData)
			}
			if len(ct.AsColumnNames) != len(tc.renames) {
				t.Fatalf("AsColumnNames = %v, want %v", ct.AsColumnNames, tc.renames)
			}
			for i := range tc.renames {
				if ct.AsColumnNames[i] != tc.renames[i] {
					t.Errorf("AsColumnNames[%d] = %q, want %q", i, ct.AsColumnNames[i], tc.renames[i])
				}
			}
			if ct.AsSelect == nil {
				t.Fatal("no inner query")
			}
			if ct.AsSelect.SQL != tc.innerSQL {
				t.Errorf("inner SQL = %q, want %q", ct.AsSelect.SQL, tc.innerSQL)
			}
			if len(ct.Columns) != 0 {
				t.Errorf("a CTAS declares no columns, got %d", len(ct.Columns))
			}
		})
	}
}

// TestTheDeclaredCreateTableFormIsUnchanged is the boundary claim: the rename
// list and the definition list are told apart by one token, so the corpus
// carries a definition list whose first column name is followed by every kind
// of type spelling the grammar accepts.
func TestTheDeclaredCreateTableFormIsUnchanged(t *testing.T) {
	cases := []string{
		`CREATE TABLE t (a INT64)`,
		`CREATE TABLE t (a INT64, b STRING)`,
		`CREATE TABLE t (a DECIMAL(9,2), b VECTOR(4))`,
		`CREATE TABLE t (a ROW(x INT64, y STRING))`,
		`CREATE TABLE t (a MAP(STRING, INT64))`,
		`CREATE TABLE t (a ARRAY(DECIMAL(9,2)))`,
		`CREATE TABLE t (a INT64 NOT NULL, day STRING) PARTITION BY (day)`,
		`CREATE TABLE IF NOT EXISTS t (a INT64)`,
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			pq, err := Parse(sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if pq.CreateTable == nil {
				t.Fatalf("not a CREATE TABLE: %v", pq.Type)
			}
			if pq.CreateTable.AsSelect != nil {
				t.Fatalf("a declared CREATE TABLE was read as a CTAS")
			}
			if len(pq.CreateTable.Columns) == 0 {
				t.Fatalf("no columns parsed")
			}
		})
	}
}

// TestTheINSERTGrammarTellsVALUESFromAQuery keeps the VALUES form exactly
// where it was while admitting a query beside it.
func TestTheINSERTGrammarTellsVALUESFromAQuery(t *testing.T) {
	cases := []struct {
		sql      string
		columns  []string
		isSelect bool
		rows     int
	}{
		{`INSERT INTO t VALUES (1, 2)`, nil, false, 1},
		{`INSERT INTO t (a, b) VALUES (1, 2), (3, 4)`, []string{"a", "b"}, false, 2},
		{`INSERT INTO t SELECT a, b FROM s`, nil, true, 0},
		{`INSERT INTO t (a, b) SELECT a, b FROM s`, []string{"a", "b"}, true, 0},
		{`INSERT INTO t WITH c AS (SELECT 1 AS k) SELECT k FROM c`, nil, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			pq, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			in := pq.Insert
			if in == nil {
				t.Fatalf("not an INSERT: %v", pq.Type)
			}
			if (in.Select != nil) != tc.isSelect {
				t.Errorf("Select != nil = %v, want %v", in.Select != nil, tc.isSelect)
			}
			if len(in.Values) != tc.rows {
				t.Errorf("%d VALUES rows, want %d", len(in.Values), tc.rows)
			}
			if len(in.Columns) != len(tc.columns) {
				t.Fatalf("columns = %v, want %v", in.Columns, tc.columns)
			}
			for i := range tc.columns {
				if in.Columns[i] != tc.columns[i] {
					t.Errorf("columns[%d] = %q, want %q", i, in.Columns[i], tc.columns[i])
				}
			}
		})
	}
}

// TestAMalformedWriteQueryIsRefusedWithPostgresClass — PostgreSQL 17.11
// answers 42601 (syntax_error) to every one of these; measured.
func TestAMalformedWriteQueryIsRefusedWithPostgresClass(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{`CREATE TABLE t AS`, "42601"},
		{`CREATE TABLE t (a, b)`, "42601"},
		{`CREATE TABLE t (a, b) SELECT 1`, "42601"},
		// A column DEFINITION list and a query cannot both be written: the
		// declared branch used to drop the query on the floor and report
		// success over an empty table. PostgreSQL 17.11: `syntax error at or
		// near "AS"` (measured; round-2 P1).
		{`CREATE TABLE t (a INT64) AS SELECT 1`, "42601"},
		{`CREATE TABLE t (a INT64) PARTITION BY (a) AS SELECT 1`, "42601"},
		{`CREATE TABLE t (a INT64) GARBAGE`, "42601"},
		{`INSERT INTO t`, ""}, // the existing unterminated-statement refusal
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := Parse(tc.sql)
			if err == nil {
				t.Fatalf("%q parsed; PostgreSQL refuses it 42601", tc.sql)
			}
			if tc.want != "" && sqlerr.StateOf(err) != tc.want {
				t.Errorf("SQLSTATE %q, want %q (%v)", sqlerr.StateOf(err), tc.want, err)
			}
		})
	}
}

// TestSplitWithDataSuffix pins the token scan directly, including the shapes
// that are NOT the clause.
func TestSplitWithDataSuffix(t *testing.T) {
	cases := []struct {
		in       string
		rest     string
		withData bool
	}{
		{`SELECT 1`, `SELECT 1`, true},
		{`SELECT 1 WITH DATA`, `SELECT 1`, true},
		{`SELECT 1 WITH NO DATA`, `SELECT 1`, false},
		{`SELECT 1 with no data`, `SELECT 1`, false},
		{`WITH c AS (SELECT 1) SELECT * FROM c`, `WITH c AS (SELECT 1) SELECT * FROM c`, true},
		{`WITH c AS (SELECT 1) SELECT * FROM c WITH NO DATA`, `WITH c AS (SELECT 1) SELECT * FROM c`, false},
		{`SELECT 'with no data'`, `SELECT 'with no data'`, true},
		{`SELECT x FROM t WHERE s = 'with data'`, `SELECT x FROM t WHERE s = 'with data'`, true},
		{`SELECT (SELECT 1 WITH NO DATA)`, `SELECT (SELECT 1 WITH NO DATA)`, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			rest, withData := splitWithDataSuffix(tc.in)
			if rest != tc.rest || withData != tc.withData {
				t.Errorf("splitWithDataSuffix(%q) = (%q, %v), want (%q, %v)",
					tc.in, rest, withData, tc.rest, tc.withData)
			}
		})
	}
}

func TestRefuseUnnamedCTASOutput(t *testing.T) {
	if err := RefuseUnnamedCTASOutput([]string{"a", "b"}); err != nil {
		t.Errorf("named columns refused: %v", err)
	}
	err := RefuseUnnamedCTASOutput([]string{"a", "  "})
	if err == nil {
		t.Fatal("an empty output-column name was accepted")
	}
	if sqlerr.StateOf(err) != "42601" {
		t.Errorf("SQLSTATE %q, want 42601", sqlerr.StateOf(err))
	}
	if !strings.Contains(err.Error(), "column 2") {
		t.Errorf("the refusal does not name the position: %v", err)
	}
}
