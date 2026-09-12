package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// RefuseInvalidFixedRowField checks postfix notation against the resolved
// container declaration before evaluation. The binder supplies its scope; the
// compiler resolves the declarations available at compilation as a backstop.
func RefuseInvalidFixedRowField(n *plansql.FuncCallNode, resolvers ...func(plansql.Node) (DeclType, Confidence)) error {
	// OutputLabel is the field name for parser-rewritten postfix notation.
	// Explicit row_field calls keep their existing dynamic semantics, including MAPs.
	if n.OutputLabel == "" || !strings.EqualFold(n.Name, "row_field") || len(n.Args) != 2 {
		return nil
	}
	parent := n.Args[0]
	field, ok := n.Args[1].(*plansql.Lit)
	if !ok || field.Kind != plansql.LitString {
		return nil
	}
	d, confidence := FieldContainerType(parent, resolvers...)
	if confidence != Decided {
		return nil // a schema-free compiler cannot decide an input column's type
	}
	if d.ID != batch.TypeRow {
		// Integer storage uses int64 for some PostgreSQL int4 functions. Name
		// their declared SQL width, using the existing function-width table.
		if call, ok := parent.(*plansql.FuncCallNode); ok && d.ID == batch.TypeInt64 {
			if result, known := PGIntegerResultWidth(call.Name); known && result.Width == PGIntWidth4 {
				d.ID = batch.TypeInt32
			}
		}
		return sqlerr.New("42809", "column notation .%s applied to type %s, which is not a composite type", field.Value, rowFieldTypeName(d.ID))
	}
	if len(d.RowFields()) == 0 {
		return nil
	}
	if _, ok := d.Schema.Field(field.Value); !ok {
		return sqlerr.New("42703", "could not identify column %q in record data type", field.Value)
	}
	return nil
}

// FieldContainerType resolves through the registry declarations used by output
// inference. A caller's complete declaration wins over the schema-free fallback.
func FieldContainerType(n plansql.Node, resolvers ...func(plansql.Node) (DeclType, Confidence)) (DeclType, Confidence) {
	for _, resolve := range resolvers {
		if d, c := resolve(n); c == Decided {
			return d, c
		}
	}
	recur := func(n plansql.Node) (DeclType, Confidence) { return FieldContainerType(n, resolvers...) }
	switch v := n.(type) {
	case *plansql.ParenNode:
		return recur(v.Inner)
	case *plansql.Lit:
		switch v.Kind {
		case plansql.LitString:
			return Decl(batch.TypeString), Decided
		case plansql.LitNumber:
			if strings.ContainsAny(v.Value, ".eE") {
				return Decl(batch.TypeFloat64), Decided
			}
			return Decl(batch.TypeInt64), Decided
		}
	case *plansql.FuncCallNode:
		name := strings.ToLower(v.Name)
		if name == "count" {
			return Decl(batch.TypeInt64), Decided
		}
		if name == "row_field" && len(v.Args) == 2 {
			d, c := recur(v.Args[0])
			if f, ok := v.Args[1].(*plansql.Lit); ok && d.Schema != nil {
				if col, ok := d.Schema.Field(f.Value); ok {
					return DeclType{ID: col.Type, Schema: &col}, c
				}
			}
			return DeclType{}, Undecided
		}
		ret := DefaultRegistry.ReturnType(v.Name)
		d, c := ret.Resolve(len(v.Args), func(i int) (DeclType, Confidence) { return recur(v.Args[i]) })
		// Dynamic scalar declarations use the declared-output TEXT disposition.
		if c == Undecided && ret.kind == retDynamic {
			return Decl(batch.TypeString), Decided
		}
		return d, c
	}
	return DeclType{}, Undecided
}

func rowFieldTypeName(t batch.TypeID) string {
	switch t {
	case batch.TypeString:
		return "text"
	case batch.TypeInt64:
		return "bigint"
	case batch.TypeInt32:
		return "integer"
	case batch.TypeBool:
		return "boolean"
	case batch.TypeFloat64:
		return "double precision"
	case batch.TypeFloat32:
		return "real"
	case batch.TypeDecimal:
		return "numeric"
	case batch.TypeBytes:
		return "bytea"
	case batch.TypeTimestamp:
		return "timestamp without time zone"
	}
	return strings.ToLower(t.String())
}
