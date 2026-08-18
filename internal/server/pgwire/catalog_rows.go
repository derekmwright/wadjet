package pgwire

// Single-row catalog relations: pg_database and pg_namespace.
//
// A client's database and schema pickers are populated by listing these two.
// pg_database queries reached the pg_catalog intercept, matched no branch of
// it, and fell through to the generic answer — the SELECT list's column names
// with no rows under them — so the picker rendered an empty database list and
// nothing downstream could be selected (issue #305 item 6).
//
// Both relations have exactly one row here: this server is one database
// ("wadjet") with one schema ("public"), the same constants the session
// functions current_database() and current_schema() report. What varies is
// which of the relation's columns a client asks for and what it labels them,
// and the answer has to be built from that — the columns declared in the
// RowDescription are the columns the DataRow carries, one decision, per the
// coherence invariant in describe_execute_test.go.

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// pgDatabaseAttrs is the one pg_database row, keyed by column name.
//
// Values carry their real Go types — bool, int, string — because the declared
// column OID is inferred from them (synthAnswer.colOID) and typed clients pick
// their accessor from that OID: a bool declared as text sent DataGrip's reader
// through getInt("f"). The wire text a bool renders to is still t/f
// (formatPgValue). The encoding is 6 (UTF8) and the collation names match a
// UTF-8 cluster, because that is what this server is. Columns of pg_database
// that carry no meaning here — the transaction-id horizons, the ACL — are
// absent, and a client asking for one gets NULL rather than a number this
// server made up.
func pgDatabaseAttrs() map[string]any {
	return map[string]any{
		"oid":            tableOID(expr.SessionCatalog),
		"datname":        expr.SessionCatalog,
		"datdba":         10,
		"encoding":       6,
		"datcollate":     "en_US.UTF-8",
		"datctype":       "en_US.UTF-8",
		"datlocprovider": "c",
		"datistemplate":  false,
		"datallowconn":   true,
		"datconnlimit":   -1,
		"dattablespace":  1663,
		"datacl":         nil,
		// pg_shdescription joined alongside: the database has no comment.
		"description": nil,
	}
}

// pgNamespaceAttrs is the one pg_namespace row.
func pgNamespaceAttrs() map[string]any {
	return map[string]any{
		"oid":      tableOID(expr.SessionSchema),
		"nspname":  expr.SessionSchema,
		"nspowner": 10,
		"nspacl":   nil,
	}
}

// matchPgDatabase answers a pg_database listing with the single database this
// server is.
func (c *pgConn) matchPgDatabase(sql string) *synthAnswer {
	return catalogRowAnswer(sql, pgDatabaseAttrs(), []string{
		"oid", "datname", "datdba", "encoding", "datcollate", "datctype",
		"datistemplate", "datallowconn",
	})
}

// catalogRowAnswer builds a one-row answer for a catalog relation whose whole
// contents this server knows, shaped by the statement's own SELECT list: the
// answer's columns are the labels the client wrote, and each value is the
// attribute that column names, or NULL for one this server does not model.
// `SELECT *` gets fallbackCols.
//
// It returns nil when the SELECT list names none of the relation's columns —
// an aggregate, a projection of something else — leaving the caller's
// empty-but-coherent fallback in place rather than answering a question that
// was not asked.
func catalogRowAnswer(sql string, attrs map[string]any, fallbackCols []string) *synthAnswer {
	items := selectItems(sql)
	if len(items) == 0 {
		return nil
	}

	// SELECT * — the client wants the relation's columns, so name them.
	if len(items) == 1 && items[0].expr == "*" {
		row := make(map[string]any, len(fallbackCols))
		for _, col := range fallbackCols {
			row[col] = attrs[col]
		}
		return singleRow(fallbackCols, row)
	}

	cols := make([]string, 0, len(items))
	row := make(map[string]any, len(items))
	known := false
	for _, it := range items {
		val, ok := attrs[it.expr]
		if ok {
			known = true
		}
		// Duplicate labels would collapse in the row map; the value under a
		// repeated label is the same either way, so only the first is kept.
		if _, dup := row[it.label]; !dup {
			cols = append(cols, it.label)
			row[it.label] = val
		}
	}
	if !known {
		return nil
	}
	return singleRow(cols, row)
}

// selectItem is one entry of a SELECT list: the attribute it reads, lowercased
// and stripped of any table qualifier, and the label the client will read the
// value back under.
type selectItem struct {
	expr  string
	label string
}

// selectItems splits a SELECT list into its entries. It is the same rough
// comma split extractSelectColumns does — the labels it produces match, so a
// statement this layer answers is labelled the way the rest of this layer
// labels one — but it keeps the expression alongside the label, which is what
// tells `datname AS TABLE_CAT` to read datname and report TABLE_CAT.
//
// Anything more structured than `[table.]column [AS label]` comes back with
// its whole text as the expression, which matches no attribute and so becomes
// NULL (or, if nothing in the list matches, declines the statement).
func selectItems(sql string) []selectItem {
	sql = strings.TrimSpace(sql)
	if len(sql) < 7 || !strings.EqualFold(sql[:7], "SELECT ") {
		return nil
	}
	entries := splitSelectList(sql[7:])
	if len(entries) > 0 {
		if t := strings.TrimSpace(entries[0]); len(t) > 9 && strings.EqualFold(t[:9], "DISTINCT ") {
			// DISTINCT belongs to the statement, not to the first column.
			entries[0] = t[9:]
		}
	}

	var items []selectItem
	for _, p := range entries {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		exprPart, label := p, ""
		if asIdx := lastTopLevelAS(p); asIdx >= 0 {
			exprPart = strings.TrimSpace(p[:asIdx])
			label = strings.Trim(strings.TrimSpace(p[asIdx+4:]), `"`)
		}
		// A cast reads the same attribute: N.oid::bigint is still oid.
		bare := exprPart
		if castIdx := strings.Index(bare, "::"); castIdx >= 0 {
			bare = strings.TrimSpace(bare[:castIdx])
		}
		// Strip a table qualifier: d.datname and pg_database.datname both
		// read datname.
		if dotIdx := strings.LastIndex(bare, "."); dotIdx >= 0 {
			bare = strings.TrimSpace(bare[dotIdx+1:])
		}
		bare = strings.Trim(bare, `"`)
		if label == "" {
			label = bare
		}
		items = append(items, selectItem{expr: strings.ToLower(bare), label: label})
	}
	return items
}

// splitSelectList scans the text after SELECT and returns the top-level
// comma-separated entries, stopping at the statement's own FROM. Unlike a
// substring search it survives what real clients send: newlines before
// FROM, commas inside function calls, and quoted identifiers — the scan
// tracks paren depth and both quote kinds, and FROM only terminates the
// list as a word at depth zero.
func splitSelectList(s string) []string {
	var entries []string
	depth := 0
	inSingle, inDouble := false, false
	start := 0
	wordAt := func(i int) bool {
		// s[i:i+4] is FROM as its own word.
		if i+4 > len(s) || !strings.EqualFold(s[i:i+4], "FROM") {
			return false
		}
		before := byte(' ')
		if i > 0 {
			before = s[i-1]
		}
		after := byte(' ')
		if i+4 < len(s) {
			after = s[i+4]
		}
		isWord := func(b byte) bool {
			return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
		}
		return !isWord(before) && !isWord(after)
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && ch == ',':
			entries = append(entries, s[start:i])
			start = i + 1
		case depth == 0 && (ch == 'f' || ch == 'F') && wordAt(i):
			entries = append(entries, s[start:i])
			return entries
		}
	}
	entries = append(entries, s[start:])
	return entries
}

// lastTopLevelAS finds the last " AS " outside quotes and parens, so that
// pg_get_userbyid(x) AS "owner" splits at the alias and CAST(x AS int8)
// does not.
func lastTopLevelAS(s string) int {
	depth := 0
	inSingle, inDouble := false, false
	last := -1
	for i := 0; i+4 <= len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && strings.EqualFold(s[i:i+4], " AS "):
			last = i
		}
	}
	return last
}
