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

// pgUserAttrs is the one pg_user row: the identity on this connection.
//
// usesuper is false because this server has no superuser: authorization is
// the RBAC/ABAC policy engine, not a cluster-wide bit, and a client that asks
// whether it may do anything it likes is owed the honest no. Clients use the
// answer to decide which admin-only nodes to offer, and PostgreSQL clients
// introspect perfectly well as ordinary users.
func pgUserAttrs(user string) map[string]any {
	return map[string]any{
		"usename":      user,
		"usesysid":     10,
		"usecreatedb":  false,
		"usesuper":     false,
		"userepl":      false,
		"usebypassrls": false,
		"passwd":       "********",
		"valuntil":     nil,
		"useconfig":    nil,
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
	return catalogRowsAnswer(sql, attrs, []map[string]any{attrs}, fallbackCols)
}

// catalogRowsAnswer is catalogRowAnswer over many rows: pg_class lists one row
// per table, pg_database one row per database. shape names the relation's
// columns (which of them the SELECT list may read); rows carry the values, and
// may be empty — a listing narrowed to nothing still has to answer with the
// columns the client asked for, not with a different shape.
func catalogRowsAnswer(sql string, shape map[string]any, rows []map[string]any, fallbackCols []string) *synthAnswer {
	items := selectItems(sql)
	if len(items) == 0 {
		return nil
	}

	// SELECT * — the client wants the relation's columns, so name them.
	if len(items) == 1 && items[0].expr == "*" {
		items = items[:0]
		for _, col := range fallbackCols {
			items = append(items, selectItem{expr: col, label: col})
		}
	}

	cols := make([]string, 0, len(items))
	picks := make([]selectItem, 0, len(items))
	seen := make(map[string]bool, len(items))
	known := false
	for _, it := range items {
		if _, ok := shape[it.expr]; ok {
			known = true
		}
		// Duplicate labels would collapse in the row map; the value under a
		// repeated label is the same either way, so only the first is kept.
		if seen[it.label] {
			continue
		}
		seen[it.label] = true
		cols = append(cols, it.label)
		picks = append(picks, it)
	}
	if !known {
		return nil
	}

	ans := &synthAnswer{cols: cols}
	for _, attrs := range rows {
		row := make(map[string]any, len(picks))
		for _, it := range picks {
			row[it.label] = attrs[it.expr]
		}
		ans.rows = append(ans.rows, row)
	}
	return ans
}

// pgClassAttrs is the pg_class row for one table. Everything this server can
// answer honestly is here; what it does not model (ACLs, index/trigger flags
// it has no concept of) is either its truthful constant or absent, so a client
// asking for it reads NULL rather than an invention.
//
// relkind is 'r' (ordinary table) for every entry: the catalog holds tables,
// and a client that reads relkind is deciding which node type to draw.
func pgClassAttrs(name string) map[string]any {
	return map[string]any{
		"oid":            tableOID(name),
		"relname":        name,
		"relnamespace":   tableOID(expr.SessionSchema),
		"relkind":        "r",
		"relowner":       10,
		"reltuples":      float64(-1), // PostgreSQL's "not yet vacuumed/analyzed"
		"relhasindex":    false,
		"relpersistence": "p",
		"relispartition": false,
		"relhasrules":    false,
		"relhastriggers": false,
		"relhassubclass": false,
		"reltablespace":  0,
		"reloptions":     nil,
		"relacl":         nil,
		"description":    nil,
	}
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
		} else if fields := strings.Fields(p); len(fields) == 2 {
			// SQL's implicit alias: `rolsuper is_super` labels the column
			// is_super with no AS. Without this the whole two-word text
			// became the column name, and a client reading its own label
			// found nothing under it. Only the exact two-token form is
			// treated this way — anything longer is an expression.
			exprPart, label = fields[0], strings.Trim(fields[1], `"`)
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
