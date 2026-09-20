// SPDX-License-Identifier: MIT

package format

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// dupCase is one result — columns and a single row of values BY POSITION —
// with the exact text each format must print for it.
type dupCase struct {
	name    string
	columns []string
	row     []any
	table   string
	csv     string
	json    string
}

// dupCases are the shapes a result carrying two output columns of one name
// arrives in. Every one of them holds DIFFERENT values in the colliding
// columns, which is what makes the cell discriminating: a renderer keyed by
// column NAME answers the LAST column's value in every position of that name,
// and a fixture whose colliding columns happened to agree would pass either
// way.
//
// The oracle is PostgreSQL 17.11 over the same shapes (psql, and `row_to_json`
// for the JSON rule):
//
//	SELECT * FROM la JOIN rb USING (id)   ->  id | tail | amt | note | tail
//	                                           2 |   20 | 200 |  300 |   99
//	SELECT row_to_json(t) FROM (SELECT 1 AS u, 2 AS u) t  ->  {"u":1,"u":2}
//
// so a duplicate key is what the authority emits and dropping one is not an
// option (#1218).
var dupCases = []dupCase{
	{
		name:    "a star over a USING join whose arms share a tail name",
		columns: []string{"id", "tail", "amt", "note", "tail"},
		row:     []any{int64(2), int64(20), int64(200), int64(300), int64(99)},
		table: "+----+------+-----+------+------+\n" +
			"| id | tail | amt | note | tail |\n" +
			"+----+------+-----+------+------+\n" +
			"| 2  | 20   | 200 | 300  | 99   |\n" +
			"+----+------+-----+------+------+\n" +
			"(1 rows)\n",
		csv: "id,tail,amt,note,tail\n2,20,200,300,99\n",
		json: "[\n  {\n" +
			"    \"id\": 2,\n" +
			"    \"tail\": 20,\n" +
			"    \"amt\": 200,\n" +
			"    \"note\": 300,\n" +
			"    \"tail\": 99\n" +
			"  }\n]\n",
	},
	{
		name:    "three output columns of one name",
		columns: []string{"t", "t", "t"},
		row:     []any{int64(20), int64(21), int64(22)},
		table: "+----+----+----+\n" +
			"| t  | t  | t  |\n" +
			"+----+----+----+\n" +
			"| 20 | 21 | 22 |\n" +
			"+----+----+----+\n" +
			"(1 rows)\n",
		csv:  "t,t,t\n20,21,22\n",
		json: "[\n  {\n    \"t\": 20,\n    \"t\": 21,\n    \"t\": 22\n  }\n]\n",
	},
	{
		name:    "one name over two different types",
		columns: []string{"m", "m"},
		row:     []any{int64(2), "x"},
		table: "+---+---+\n" +
			"| m | m |\n" +
			"+---+---+\n" +
			"| 2 | x |\n" +
			"+---+---+\n" +
			"(1 rows)\n",
		csv:  "m,m\n2,x\n",
		json: "[\n  {\n    \"m\": 2,\n    \"m\": \"x\"\n  }\n]\n",
	},
	{
		name:    "a NULL in one of them",
		columns: []string{"n", "n"},
		row:     []any{nil, int64(7)},
		table: "+------+---+\n" +
			"| n    | n |\n" +
			"+------+---+\n" +
			"| NULL | 7 |\n" +
			"+------+---+\n" +
			"(1 rows)\n",
		csv:  "n,n\nNULL,7\n",
		json: "[\n  {\n    \"n\": null,\n    \"n\": 7\n  }\n]\n",
	},
	{
		// The control: unique names, where the map form was exact. Its
		// rendering must be unchanged, duplicate-key handling or not.
		name:    "unique names are unchanged",
		columns: []string{"id", "name"},
		row:     []any{int64(1), "alice"},
		table: "+----+-------+\n" +
			"| id | name  |\n" +
			"+----+-------+\n" +
			"| 1  | alice |\n" +
			"+----+-------+\n" +
			"(1 rows)\n",
		csv:  "id,name\n1,alice\n",
		json: "[\n  {\n    \"id\": 1,\n    \"name\": \"alice\"\n  }\n]\n",
	},
}

// TestEveryFormatPrintsEveryColumnByPosition is the #1218 gate.
//
// Before the fix the renderers took rows keyed by column NAME, so the table
// and CSV forms printed the LAST colliding column's value under every heading
// of that name — `2,99,200,300,99` where the wire carries `2,20,200,300,99` —
// and JSON, the default, emitted one key and dropped the other column
// entirely.
func TestEveryFormatPrintsEveryColumnByPosition(t *testing.T) {
	for _, tc := range dupCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, f := range []struct {
				spelling string
				want     string
			}{
				{"table", tc.table},
				{"csv", tc.csv},
				{"json", tc.json},
			} {
				parsed, err := ParseFormat(f.spelling)
				if err != nil {
					t.Fatalf("ParseFormat(%q): %v", f.spelling, err)
				}
				var buf bytes.Buffer
				if err := Write(&buf, parsed, tc.columns, [][]any{tc.row}); err != nil {
					t.Fatalf("--format=%s: %v", f.spelling, err)
				}
				if got := buf.String(); got != f.want {
					t.Errorf("--format=%s over %v\n got:\n%s\nwant:\n%s",
						f.spelling, tc.columns, got, f.want)
				}
			}
		})
	}
}

// TestEveryAcceptedFormatSpellingIsCovered closes "every other --format":
// the spellings ParseFormat accepts are read out of its own source, so a
// format added later without a positional cell above fails here rather than
// shipping a renderer nobody held to the rule.
func TestEveryAcceptedFormatSpellingIsCovered(t *testing.T) {
	covered := map[string]bool{"table": true, "csv": true, "json": true}

	fset := token.NewFileSet()
	// The file, not a directory walk: a git worktree under .claude/worktrees/
	// is a second copy of this source (CLAUDE.md).
	src, err := os.ReadFile("format.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(fset, "format.go", src, 0)
	if err != nil {
		t.Fatalf("parsing format.go: %v", err)
	}

	var accepted []string
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ParseFormat" {
			return true
		}
		ast.Inspect(fn.Body, func(m ast.Node) bool {
			clause, ok := m.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range clause.List {
				lit, ok := expr.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}
				accepted = append(accepted, s)
			}
			return true
		})
		return false
	})

	sort.Strings(accepted)
	if len(accepted) == 0 {
		t.Fatal("no accepted --format spelling found in ParseFormat — the walk stopped " +
			"finding the switch it is meant to enumerate")
	}
	for _, spelling := range accepted {
		if !covered[spelling] {
			t.Errorf("--format=%s is accepted but no cell in dupCases holds it to the "+
				"positional rule: a result carrying two output columns of one name must "+
				"print both values in this format too (#1218)", spelling)
		}
	}
	if len(accepted) != len(covered) {
		t.Errorf("ParseFormat accepts %v but the gate covers %d spellings — the two lists "+
			"must agree", accepted, len(covered))
	}
}

// TestJSONKeepsBothKeysLikeRowToJSON states the JSON rule on its own, because
// it is the one a reader is most likely to doubt: duplicate keys in one object
// are legal JSON, and PostgreSQL — the authority on what a client of this
// engine expects — emits them (`row_to_json` over `SELECT 1 AS u, 2 AS u` is
// `{"u":1,"u":2}`). Dropping a key is the renderer deciding a column the query
// asked for does not exist.
func TestJSONKeepsBothKeysLikeRowToJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, JSON, []string{"u", "u"}, [][]any{{int64(1), int64(2)}}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if n := strings.Count(out, `"u"`); n != 2 {
		t.Fatalf("JSON emitted %d `u` keys, want 2 — PostgreSQL's row_to_json answers "+
			"{\"u\":1,\"u\":2}; output = %q", n, out)
	}
	if !strings.Contains(out, "1") || !strings.Contains(out, "2") {
		t.Fatalf("a column's value is missing from the JSON: %q", out)
	}
}

// TestARowShorterThanTheColumnListRendersNULL: a renderer is not the place a
// short row becomes a panic. Every format pads to the declared column list.
func TestARowShorterThanTheColumnListRendersNULL(t *testing.T) {
	for _, f := range []Format{Table, CSV, JSON} {
		var buf bytes.Buffer
		if err := Write(&buf, f, []string{"a", "b"}, [][]any{{int64(1)}}); err != nil {
			t.Fatalf("format %v: %v", f, err)
		}
		out := buf.String()
		if f == JSON {
			if !strings.Contains(out, "null") {
				t.Errorf("format %v: missing cell did not render as null: %q", f, out)
			}
			continue
		}
		if !strings.Contains(out, "NULL") {
			t.Errorf("format %v: missing cell did not render as NULL: %q", f, out)
		}
	}
}
