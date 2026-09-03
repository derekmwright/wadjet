package clickbench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The control that says the identifier fold did not break ClickBench (#731).
//
// `hits` is registered from a parquet file, so its catalog schema carries the
// dataset's own CamelCase column names — `WatchID`, `UserID`, `EventDate`,
// `SearchPhrase`, `ResolutionWidth` — and all 43 queries spell them CamelCase
// too. Before the fold, every reference matched the schema BYTE for byte and
// the queries worked by coincidence of spelling. After it, each reference is
// lower case and has to be RESOLVED against the CamelCase schema; if that
// resolution were missing, all 43 queries would answer columns of NULL
// instead of failing, which is the worst shape a regression can take.
//
// This control needs no data part, deliberately: `WADJET_HITS_PART` is not
// set in CI, so every other test in this package skips, and a gate that
// skips is not a gate. It reads the query file, takes the CamelCase names the
// queries themselves spell as the schema, parses each query (which folds the
// references), and asserts every reference resolves to exactly one column
// under the engine's own rule (`batch.ResolveSchemaIndex`).
func TestClickBenchIdentifiersResolveUnderTheFold(t *testing.T) {
	queries := readQueriesFile(t, "queries.sql")
	if len(queries) != 43 {
		t.Fatalf("read %d queries, want 43", len(queries))
	}
	schema := hitsSchemaFromQueries(t, queries)
	if len(schema) < 20 {
		t.Fatalf("only %d CamelCase column names recovered from the corpus; "+
			"this control is asserting nothing", len(schema))
	}
	camel := 0
	for _, c := range schema {
		if !batch.IsFoldedIdent(c.Name) {
			camel++
		}
	}
	if camel < 20 {
		t.Fatalf("only %d of %d recovered names are CamelCase; the control cannot "+
			"see the defect it exists for", camel, len(schema))
	}

	for i, q := range queries {
		refs := columnRefsOf(t, i+1, q)
		if len(refs) == 0 {
			continue // Q1 is SELECT COUNT(*) and names no column
		}
		for _, r := range refs {
			if !batch.IsFoldedIdent(r) {
				t.Errorf("Q%02d: reference %q reached the plan UNFOLDED; an unquoted "+
					"identifier folds at the lexer\n  SQL: %s", i+1, r, q)
				continue
			}
			if idx := batch.ResolveSchemaIndex(schema, r); idx < 0 {
				t.Errorf("Q%02d: reference %q resolves to NO column of the hits schema — "+
					"this is the shape that answers a column of NULLs\n  SQL: %s", i+1, r, q)
			}
		}
	}
}

// readQueriesFile returns the non-empty lines of a corpus file.
func readQueriesFile(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(".", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// hitsSchemaFromQueries recovers the CamelCase column names the corpus spells.
// The dataset is not on this machine; the queries are, and they carry the
// dataset's own spellings, which is the only thing this control needs.
func hitsSchemaFromQueries(t *testing.T, queries []string) []parquet.Column {
	t.Helper()
	seen := map[string]bool{}
	var cols []parquet.Column
	for _, q := range queries {
		for _, w := range camelWordsOf(q) {
			if seen[w] {
				continue
			}
			seen[w] = true
			cols = append(cols, parquet.Column{Name: w, Type: parquet.TypeString, Nullable: true})
		}
	}
	// The names camelWordsOf cannot recover from the text: `URL` is a real
	// hits column spelled in full upper case, indistinguishable there from a
	// keyword, and the rest are short lower-case aliases the corpus defines.
	for _, extra := range []string{
		"URL", "c", "u", "l", "k", "m", "p", "s", "d", "h", "q", "pageviews", "hits",
	} {
		if !seen[extra] {
			seen[extra] = true
			cols = append(cols, parquet.Column{Name: extra, Type: parquet.TypeString, Nullable: true})
		}
	}
	return cols
}

// camelWordsOf is every word in the SQL that carries an ASCII upper-case
// letter and a lower-case one — the shape of a ClickBench column name, and
// not the shape of the corpus's SQL keywords, which are written in full upper
// case.
func camelWordsOf(sql string) []string {
	var out []string
	word := strings.Builder{}
	flush := func() {
		w := word.String()
		word.Reset()
		if w == "" || !isCamel(w) {
			return
		}
		out = append(out, w)
	}
	inQuote := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if c == '\'' {
			inQuote = !inQuote
			flush()
			continue
		}
		if inQuote {
			continue
		}
		if c == '_' || c == '.' || (c >= '0' && c <= '9') ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			word.WriteByte(c)
			continue
		}
		flush()
	}
	flush()
	return out
}

func isCamel(w string) bool {
	upper, lower := false, false
	for i := 0; i < len(w); i++ {
		switch {
		case w[i] >= 'A' && w[i] <= 'Z':
			upper = true
		case w[i] >= 'a' && w[i] <= 'z':
			lower = true
		}
	}
	if !upper || !lower {
		return false
	}
	// A dotted word is a qualified reference; the control only carries bare
	// column names.
	return !strings.Contains(w, ".")
}

// columnRefsOf is every bare column reference the parser produced for one
// query, deduplicated. Aliases the query itself defines are excluded — they
// are not schema columns and resolve through the projection above.
func columnRefsOf(t *testing.T, n int, sql string) []string {
	t.Helper()
	parsed, err := plansql.Parse(strings.TrimSuffix(sql, ";"))
	if err != nil {
		t.Fatalf("Q%02d parse: %v\n  SQL: %s", n, err, sql)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("Q%02d ExtractSelect: %v\n  SQL: %s", n, err, sql)
	}
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		name = plansql.NormalizeIdentRef(strings.TrimSpace(name))
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, name)
	}
	addRefs := func(node plansql.Node) {
		if node == nil {
			return
		}
		refs, err := plansql.ColumnRefs(node)
		if err != nil {
			// A node ColumnRefs declines (a subquery, a window) carries no
			// bare reference this control can read; the rest of the query
			// still does.
			return
		}
		for _, r := range refs {
			add(r.Column)
		}
	}
	for _, c := range info.Columns {
		addRefs(c.ASTExpr)
		for _, a := range c.AggArgs {
			addRefs(a)
		}
	}
	addRefs(info.WhereExpr)
	for _, g := range info.GroupBy {
		if _, name, ok := plansql.SplitIdentRef(g); ok {
			add(name)
		}
	}
	return out
}
