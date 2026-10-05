// SPDX-License-Identifier: MIT

package pgwire

// Parameter type inference for placeholders the client did NOT declare.
//
// The protocol allows a Parse message to declare no parameter types (or OID 0
// for some of them) and leave the choice to the server — pgJDBC, psycopg and
// DataGrip all do this for values they have as text. Wadjet answered
// ParameterDescription with OID 0, which is legal, and then Bind rendered the
// undeclared parameter's text bytes as a QUOTED string literal, so
// `WHERE n_nationkey = $1` bound with 7 became `n_nationkey = '7'` — an
// int/string comparison the engine coerces to 0, silently matching the WRONG
// row (#365). The declared-OID path beside it was correct, which localizes
// the defect to inference, not to binding.
//
// The inference here is deliberately narrow and lexical: a placeholder that
// stands directly against a column in a comparison ($N <op> col or
// col <op> $N) takes that column's wire type, resolved from the schemas of
// the tables the statement references. Anything else keeps OID 0 and the old
// quoted-literal rendering. PostgreSQL's inference is the full type-checking
// pass; this covers the shape every driver actually sends — a filter on a
// column — without a planner round-trip.

import (
	"context"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// isIdentByte reports a byte that can continue an identifier (a qualifier's
// dot and a delimited identifier's quote included).
func isIdentByte(b byte) bool {
	return b == '_' || b == '.' || b == '"' ||
		(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func skipSpacesBack(s string, i int) int {
	for i > 0 && (s[i-1] == ' ' || s[i-1] == '\t' || s[i-1] == '\n' || s[i-1] == '\r') {
		i--
	}
	return i
}

// inferenceContext is the context the two schema-reading inference passes below
// run under: a short timeout, carrying THIS CONNECTION'S IDENTITY.
//
// The identity is the load-bearing half. Both passes used a bare
// context.Background(), so the table list they drew from was unfiltered and a
// statement that merely MENTIONED a relation — a string literal is enough,
// containsIdentWord reads the raw SQL — folded that relation's column types
// into the ParameterDescription. A denied relation's declared type reached the
// wire on the extended-protocol path every JDBC and pgx driver prepares
// through. Without the identity, `visibleCatalogTables` would instead refuse
// EVERY relation under auth (a nil identity is a refusal), which would silently
// un-type every parameter for every caller.
func (c *pgConn) inferenceContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if c.identity != nil {
		ctx = auth.ContextWithIdentity(ctx, c.identity)
	}
	// The TRUSTED environment, as well as the identity: a policy conditioned
	// on `env.source_ip` or `env.protocol` decides nothing without it, so a
	// deny that names one would not have matched here and the denied
	// relation's column types would have gone back on the wire anyway
	// (case P8).
	//
	// The SAME expression `queryContext` attaches for a statement — one
	// builder, one spelling, so the inference path and the statement path
	// cannot come to describe different environments for one connection.
	// `Time` is left for the decision to stamp (auth.DecisionEnvironment), and
	// the port is stripped where every door's is, in auth.
	ctx = auth.ContextWithEnvironment(ctx, auth.Environment{
		SourceIP: peerAddr(c.conn), Protocol: "pgwire",
	})
	return context.WithTimeout(ctx, 5*time.Second)
}

// isNestedTypeID reports whether t is one of the container types whose text
// rendering needs the column's DECLARED structure — ROW's field order,
// ARRAY's element type, MAP's key/value field names — because the Go value
// GetValue hands back carries none of that on its own: a ROW is a
// map[string]any with no remembered order, an ARRAY and a MAP are both a
// bare []any indistinguishable from each other without it (#471).
func isNestedTypeID(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeRow, parquet.TypeArray, parquet.TypeMap:
		return true
	}
	return false
}

// nestedColumnSchemas first uses the result metas' own declared nested shape,
// then falls back to catalog columns matched by name for the legacy path.
// Conflicting top-level types drop the name rather than confidently apply a
// wrong structure and lose fields. Skip all lookup if no output is nested.
// Only result-derived declarations may supply ordered positional fallback;
// catalog guesses return byName only. Coordinator output schema is authoritative.
// See docs/internals/pgwire-legacy-nested-schema-resolution.md for the design.
func (c *pgConn) nestedColumnSchemas(sql string, metas []wadjet.ColumnMeta) *nestedFieldSchema {
	needed := false
	for _, m := range metas {
		if isNestedTypeID(m.TypeID) {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}
	// The RESULT's own declaration first. A ROW the plan declared carries its
	// field list on the meta (wadjet.ColumnMeta.Fields), and that beats every
	// guess below it: it is per-result rather than a cross-table name match,
	// it is POSITIONAL, and it is the only source at all for a ROW no catalog
	// describes — which is what an aggregate that CONSTRUCTS one produces
	// (ohlcv's bar, #965). Before it, such a column reached
	// formatPgComposite with no declaration and rendered in SORTED-KEY order:
	// a well-formed DataRow carrying the right numbers in the wrong places.
	if ns := nestedSchemaFromMetas(metas); ns != nil {
		return ns
	}

	ctx, cancel := c.inferenceContext()
	defer cancel()

	tables, err := c.visibleCatalogTables(ctx)
	if err != nil {
		return nil
	}
	lowerSQL := strings.ToLower(sql)
	out := make(map[string]parquet.Column)
	conflicting := make(map[string]bool)
	for _, table := range tables {
		if !containsIdentWord(lowerSQL, strings.ToLower(table)) {
			continue
		}
		tm, err := c.db.Catalog().GetTable(ctx, c.db.Catalog().ResolveTableName(table))
		if err != nil || tm == nil {
			continue
		}
		for _, col := range tm.Schema.Columns {
			if prev, dup := out[col.Name]; dup && prev.Type != col.Type {
				conflicting[col.Name] = true
				continue
			}
			out[col.Name] = col
		}
	}
	for name := range conflicting {
		delete(out, name)
	}
	if len(out) == 0 {
		return nil
	}
	// Publish unique folded aliases for catalog names so unquoted output references
	// find their nested declaration (#731), without shadowing byte-exact entries.
	// Two catalog names folding to one alias are ambiguous: drop that alias,
	// matching batch/schema.go item 3; never let map iteration select field order
	// (ADR-0013). Top-level-type conflicts alone cannot detect this case.
	// These catalog guesses have no positional fallback; a miss renders through
	// the deterministic declaration-less path.
	// See docs/internals/pgwire-folded-nested-schema-aliases.md for the design.
	aliases := make(map[string]parquet.Column)
	ambiguous := make(map[string]bool)
	for name, col := range out {
		f := batch.FoldIdent(name)
		if f == name {
			continue
		}
		if _, taken := out[f]; taken {
			continue
		}
		if _, dup := aliases[f]; dup {
			ambiguous[f] = true
			continue
		}
		aliases[f] = col
	}
	for f := range ambiguous {
		delete(aliases, f)
	}
	for f, col := range aliases {
		out[f] = col
	}
	return &nestedFieldSchema{byName: out}
}

// containsIdentWord reports whether word appears in s bounded by
// non-identifier bytes — `nation` must not match `nation_region`.
func containsIdentWord(s, word string) bool {
	for from := 0; ; {
		i := strings.Index(s[from:], word)
		if i < 0 {
			return false
		}
		i += from
		before := i == 0 || !isIdentByte(s[i-1])
		afterIdx := i + len(word)
		after := afterIdx >= len(s) || !isIdentByte(s[afterIdx])
		if before && after {
			return true
		}
		from = i + 1
	}
}

// nestedSchemaFromMetas builds the declaration map out of the result's own
// column metadata. It answers only when EVERY nested-typed column has a
// declaration, so a result that mixes a declared ROW with an undeclared one
// still falls through to the catalog walk rather than half-answering.
//
// `ordered` is set, because these entries ARE the output column list in
// order — the positional fallback nestedColumnFor offers is exact here, which
// is what makes a renamed or duplicated ROW column resolve.
func nestedSchemaFromMetas(metas []wadjet.ColumnMeta) *nestedFieldSchema {
	any := false
	for _, m := range metas {
		switch m.TypeID {
		case parquet.TypeRow:
			if len(m.Fields) == 0 {
				return nil
			}
		case parquet.TypeArray:
			// An ARRAY's ELEMENT is the same kind of declaration a ROW's
			// field list is, and since #992 it rides the meta. Without it
			// here, a result whose array column is ALIASED — `SELECT a_i32
			// AS v` — fell through to the catalog lookup, which is keyed by
			// the catalog's own name and misses every alias; the renderer
			// then had no element type and the binary path no array to
			// write.
			if m.ElementType == nil {
				return nil
			}
		default:
			continue
		}
		any = true
	}
	if !any {
		return nil
	}
	byName := make(map[string]parquet.Column, len(metas))
	ordered := make([]parquet.Column, len(metas))
	for i, m := range metas {
		col := parquet.Column{Name: m.Name, Type: m.TypeID,
			Precision: m.Precision, Scale: m.Scale, Fields: m.Fields,
			ElementType: m.ElementType}
		ordered[i] = col
		byName[m.Name] = col
	}
	return &nestedFieldSchema{byName: byName, ordered: ordered}
}
