// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The declared-output walk answers the expression layer's container sites too
// (arc CW round 3, expr/operand_decl.go): a CAST that renders or converts a
// container, and every comparator that orders two, read their operand's
// declaration from nodeDeclaredType — the walk that types the same
// expression's projection output — asked against the input batch's executed
// columns. The expression package sits below this one, so the walk is
// registered with it here; every binary that compiles SQL links this package.
func init() { expr.SetShapeResolver(declaredShapeOf) }

// declaredShapeOf is node's declared type over schema as the column a vector
// of it is allocated from, or nil when the walk declines (or declares a type
// with no allocatable shape: a DECIMAL without its scale).
func declaredShapeOf(node plansql.Node, schema []parquet.Column, sub expr.SubqueryDeclFunc) *parquet.Column {
	decls := ColDecls{
		Types:  make(map[string]parquet.TypeID, len(schema)),
		Fields: map[string][]parquet.Column{},
		Elems:  map[string]parquet.Column{},
		Dec:    map[string]logical.DecimalMeta{},
	}
	for _, c := range schema {
		name := strings.ToLower(c.Name)
		decls.Types[name] = c.Type
		switch c.Type {
		case parquet.TypeDecimal:
			decls.Dec[name] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
		case parquet.TypeRow:
			if len(c.Fields) > 0 {
				decls.Fields[name] = c.Fields
			}
		case parquet.TypeArray, parquet.TypeMap:
			if c.ElementType != nil {
				decls.Elems[name] = c
			}
		}
	}
	if sub != nil {
		decls.subqueryDecl = func(sql string) (parquet.Column, bool) {
			t, p, s, ok := sub(sql)
			if !ok {
				return parquet.Column{}, false
			}
			return parquet.Column{Type: t, Precision: p, Scale: s, Nullable: true}, true
		}
	}
	d, c := nodeDeclaredType(node, decls)
	if c == expr.Undecided {
		return nil
	}
	col, ok := declColumn(d)
	if !ok {
		return nil
	}
	return &col
}
