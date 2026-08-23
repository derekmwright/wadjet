package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// declaredOutputSchema derives, at PLAN time, the columns a query will
// produce — names from the SELECT list, types from the catalog annotation
// AnnotateScanColumns leaves on the scans beneath it.
//
// It exists for the one case the runtime cannot answer. Wadjet derives a
// result's schema from DATA FLOW: exec.CollectSink captures it from the first
// batch it CONSUMES and exec.Project resolves its output types from the first
// batch it SEES. A query that returns zero rows produces no batch, so it has
// no schema — and `SELECT a, b FROM t WHERE false` handed the client OID 25
// (text) for every column through pgwire's coordinator path, and no columns
// AT ALL through the coordinator's correlated-local route: not an empty table
// with headers, no table (#416).
//
// The result is ADVISORY. exec.CollectSink.SchemaHint is consulted only when
// the sink consumed nothing, so an approximation for a shape this walk cannot
// type exactly costs nothing on any non-empty result. Where a column's type
// cannot be resolved it is declared STRING, which is what both entry points
// already fall back to today — so an unresolved column is no worse than
// before while a resolved one is right.
//
// Naming follows the SELECT list exactly as the projection builder does
// (alias, else the unqualified column, else the cleaned expression text), so
// an empty result names its columns the way a non-empty one would.
func declaredOutputSchema(root *logical.Node) []parquet.Column {
	pn := findOutputProjectionNode(root)
	if pn == nil {
		return nil
	}
	projs := logical.VisibleProjections(pn.Projections)
	if len(projs) == 0 {
		return nil
	}
	var childTypes map[string]parquet.TypeID
	var strictInt map[string]bool
	if len(pn.Children) == 1 {
		childTypes = emittedColTypes(pn.Children[0])
		// The same integer-preserving-arithmetic hint the projection builder
		// passes: without it `id + 1` declares FLOAT64 here where the
		// operator emits INT64 (#297's rule), so an empty result would
		// disagree with a full one about the type of its own column.
		strictInt = strictIntArithCols(pn.Children[0])
	}
	out := make([]parquet.Column, 0, len(projs))
	for _, proj := range projs {
		name := declaredProjectionName(proj)
		if name == "" {
			// A column the walk cannot even name makes the whole answer
			// nil: a partially-named schema is worse than none, because
			// the client would receive a result whose column COUNT does
			// not match the SELECT list.
			return nil
		}
		out = append(out, parquet.Column{
			Name:     name,
			Type:     declaredProjectionType(proj, childTypes, strictInt),
			Nullable: true,
		})
	}
	return out
}

// declaredProjectionName mirrors the naming rule in the projection builder
// (buildProjectOp): the alias, else the unqualified column name, else the
// cleaned expression text.
func declaredProjectionName(proj logical.Projection) string {
	if proj.Alias != "" {
		return proj.Alias
	}
	if proj.Column != "" {
		return proj.Column
	}
	return cleanExpr(proj.Expr)
}

// declaredProjectionType answers what exec.Project will emit for one
// projection, which is NOT the type the planner puts on ProjectColumn: that
// declaration is a placeholder for a bare column reference and the operator
// overwrites it from the input batch. So a bare reference is typed from the
// input's columns here, the way the operator would, and only a COMPUTED
// expression takes inferProjectionTypeCols' answer.
func declaredProjectionType(proj logical.Projection, colTypes map[string]parquet.TypeID, strictInt map[string]bool) parquet.TypeID {
	if proj.IsAgg {
		// The aggregate below emitted a column under this alias; its type
		// is in the child's emitted map.
		if t, ok := lookupColType(colTypes, declaredProjectionName(proj)); ok {
			return t
		}
		return parquet.TypeString
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		return inferProjectionTypeCols(proj.ASTExpr, parquet.TypeString, strictInt, colTypes)
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	if t, ok := lookupColType(colTypes, ref); ok {
		return t
	}
	return parquet.TypeString
}

// lookupColType resolves a possibly-qualified name against a column-type map,
// falling back to the bare suffix the way every other name resolution in the
// planner does.
func lookupColType(colTypes map[string]parquet.TypeID, name string) (parquet.TypeID, bool) {
	if colTypes == nil || name == "" {
		return 0, false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if t, ok := colTypes[lc]; ok {
		return t, true
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		if t, ok := colTypes[lc[dot+1:]]; ok {
			return t, true
		}
	}
	return 0, false
}

// emittedColTypes describes the columns a node EMITS, by name.
//
// inputColTypes answers the same question for the nodes that pass their
// input through unchanged, and deliberately STOPS at anything that rebinds a
// name — Project, Aggregate, Window, the set operators — because descending
// past one of those would answer with the wrong value's type. This adds the
// two rebinding nodes whose output IS derivable: an Aggregate emits its group
// columns at their input types plus one column per aggregate at the type
// aggSpecOutputType declares, and a Project emits its own projections.
// Everything else still answers nil, and a nil map means every column falls
// back to STRING rather than to a guess.
func emittedColTypes(n *logical.Node) map[string]parquet.TypeID {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColTypes(n.Children[0])
		out := make(map[string]parquet.TypeID, len(n.GroupBy)+len(n.AggExprs))
		for _, g := range n.GroupBy {
			if t, ok := lookupColType(in, g); ok {
				out[strings.ToLower(g)] = t
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			if t, known := aggSpecOutputType(n, agg); known {
				out[name] = t
			}
		}
		return out
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColTypes(n.Children[0])
		strictInt := strictIntArithCols(n.Children[0])
		out := make(map[string]parquet.TypeID, len(n.Projections))
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			out[strings.ToLower(name)] = declaredProjectionType(proj, in, strictInt)
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColTypes(n.Children[0])
	case logical.NodeWindow:
		// Window appends its output columns to the input batch in place — it
		// drops nothing — so every input column passes through at its input
		// type, and only the window expressions themselves need typing. Using
		// windowSpecOutputType (walkStages/buildWindow's own resolver) keeps
		// this answer identical to what the operator actually emits: an INT64
		// passthrough column no longer declares STRING on a zero-row result,
		// and ROW_NUMBER/RANK/COUNT declare INT64, SUM/AVG declare FLOAT64,
		// and a value function (LAG/LEAD/FIRST_VALUE/...) or MIN/MAX declares
		// its argument column's type when that is known.
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColTypes(n.Children[0])
		out := make(map[string]parquet.TypeID, len(in)+len(n.WindowExprs))
		for k, t := range in {
			out[k] = t
		}
		for _, we := range n.WindowExprs {
			name := strings.ToLower(we.OutputCol)
			if name == "" {
				continue
			}
			out[name] = windowSpecOutputType(n, we)
		}
		return out
	}
	return inputColTypes(n)
}
