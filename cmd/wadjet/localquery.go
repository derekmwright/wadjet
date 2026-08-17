package main

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// catalogFreeTableFuncs lists the table functions that read their data
// straight from a local path, a glob, or a URL. A statement sourced only
// from these needs neither the catalog nor the object store, so `wadjet
// query` can serve it from an in-memory store instead of dialing an S3
// endpoint that a first-run user has not set up (#303).
var catalogFreeTableFuncs = map[string]bool{
	"read_json":      true,
	"read_json_auto": true,
	"read_csv":       true,
	"read_csv_auto":  true,
	"read_parquet":   true,
}

// maxSourceWalkDepth bounds the recursion through derived tables and CTE
// bodies. Anything deeper is treated as unclassifiable.
const maxSourceWalkDepth = 16

// isCatalogFreeQuery reports whether every source in sqlText is one of the
// catalog-free table functions — directly, or through a CTE or derived
// table built only from them.
//
// The predicate is deliberately one-sided: a parse failure, a statement
// that is not a plain SELECT, or any source shape it cannot account for
// yields false, and the caller then takes the normal object-store path
// unchanged. It must never return true for a statement that touches a
// catalog table.
func isCatalogFreeQuery(sqlText string) bool {
	pq, err := plansql.Parse(sqlText)
	if err != nil || pq.Type != plansql.QuerySelect || pq.SelectInfo == nil {
		return false
	}
	visited := 0
	if !sourcesAreCatalogFree(pq.SelectInfo, map[string]bool{}, 0, &visited) {
		return false
	}
	// Every SELECT in the text must be one the walk accounted for. A
	// subquery buried in an expression — WHERE x IN (SELECT ... FROM
	// orders) — is held as raw SQL on the expression node and never
	// reaches the source walk, so counting is what keeps a catalog table
	// hiding there from being missed. A count inflated by the word
	// appearing inside a string literal only forces the fallback.
	return visited == countSelectWords(sqlText)
}

// sourcesAreCatalogFree walks one SELECT level. ctes holds the CTE names
// visible at this level (a bare reference to one of them is a reference to
// a query already checked, not to a catalog table); visited counts the
// SELECT bodies the walk has validated.
func sourcesAreCatalogFree(info *plansql.SelectInfo, ctes map[string]bool, depth int, visited *int) bool {
	if info == nil || depth > maxSourceWalkDepth {
		return false
	}

	visible := make(map[string]bool, len(ctes)+len(info.CTEs))
	for name := range ctes {
		visible[name] = true
	}
	// Names go in before the bodies are walked so a recursive CTE's
	// self-reference resolves.
	for _, cte := range info.CTEs {
		visible[strings.ToLower(cte.Name)] = true
	}
	for _, cte := range info.CTEs {
		body, err := plansql.Parse(cte.SQL)
		if err != nil || body.Type != plansql.QuerySelect || body.SelectInfo == nil {
			return false
		}
		if !sourcesAreCatalogFree(body.SelectInfo, visible, depth+1, visited) {
			return false
		}
	}

	// A set-operation node is a container: its arms hold the sources.
	if info.Union != nil {
		return sourcesAreCatalogFree(info.Union.Left, visible, depth+1, visited) &&
			sourcesAreCatalogFree(info.Union.Right, visible, depth+1, visited)
	}
	*visited++

	for _, t := range info.Tables {
		if !sourceIsCatalogFree(t.Name, t.IsFunction, visible, depth, visited) {
			return false
		}
	}
	for _, j := range info.Joins {
		if j.RightTableRef != nil {
			if !sourceIsCatalogFree(j.RightTableRef.Name, j.RightTableRef.IsFunction, visible, depth, visited) {
				return false
			}
			continue
		}
		if !sourceIsCatalogFree(j.RightTable, false, visible, depth, visited) {
			return false
		}
	}
	return true
}

// sourceIsCatalogFree classifies a single FROM/JOIN source.
func sourceIsCatalogFree(name string, isFunc bool, ctes map[string]bool, depth int, visited *int) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	// Derived table: the parser keeps the subquery as parenthesized raw SQL.
	if strings.HasPrefix(name, "(") {
		if !strings.HasSuffix(name, ")") {
			return false
		}
		inner, err := plansql.Parse(name[1 : len(name)-1])
		if err != nil || inner.Type != plansql.QuerySelect || inner.SelectInfo == nil {
			return false
		}
		return sourcesAreCatalogFree(inner.SelectInfo, ctes, depth+1, visited)
	}
	if isFunc {
		return catalogFreeTableFuncs[strings.ToLower(name)]
	}
	return ctes[strings.ToLower(name)]
}

// countSelectWords counts word-boundary occurrences of "select", case
// insensitive.
func countSelectWords(s string) int {
	lower := strings.ToLower(s)
	const word = "select"
	n, i := 0, 0
	for i+len(word) <= len(lower) {
		j := strings.Index(lower[i:], word)
		if j < 0 {
			break
		}
		start := i + j
		end := start + len(word)
		beforeOK := start == 0 || !isIdentByte(lower[start-1])
		afterOK := end == len(lower) || !isIdentByte(lower[end])
		if beforeOK && afterOK {
			n++
		}
		i = end
	}
	return n
}

func isIdentByte(b byte) bool {
	return b == '_' || b == '$' ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}
