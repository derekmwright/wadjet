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
	projs, childTypes, childDecimal, strictInt, ok := declaredProjectionInputs(root)
	if !ok {
		return nil
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
		typ := declaredProjectionType(proj, childTypes, strictInt)
		col := parquet.Column{
			Name:     name,
			Type:     typ,
			Nullable: true,
		}
		if typ == parquet.TypeDecimal {
			// precision 0 (pgTypeMod's "unconstrained") when it cannot be
			// resolved — the honest fallback, not a fabricated (p,s) (#458).
			if m, ok := declaredProjectionDecimal(proj, childDecimal); ok {
				col.Precision, col.Scale = m.Precision, m.Scale
			}
		}
		out = append(out, col)
	}
	return out
}

// declaredWireUnconstrainedDecimal names the DECIMAL output columns whose
// PostgreSQL wire typmod must declare "unconstrained" (-1) even though this
// engine's own declared/exec schema keeps a real (p,s) for them.
//
// Verified live against postgres:17-alpine's \gdesc: an aggregate function
// call NEVER carries its argument's typmod through — MIN(n)/MAX(n)/
// MIN_BY(x,n)/MAX_BY(x,n)/SUM(n)/AVG(n) over a numeric(p,s) column all
// report an unconstrained numeric, and only a BARE column reference in the
// SELECT list keeps (p,s). declaredOutputSchema's own Precision/Scale answer
// stays real for these columns — internal/engine/exec/aggregate.go's DECIMAL
// vector allocation and internal/storage/parquet's file writer both key
// physical decisions off Precision/Scale (18-digit INT64 vs 38-digit
// FixedLenByteArray encoding), so zeroing it there would risk silently
// mis-encoding a materialized MIN/MAX-of-DECIMAL(38,s) result — this is
// wire-metadata ONLY, consulted solely by pgTypeMod (fold-in to #457/#458,
// FIX 2).
//
// proj.IsAgg is PostgreSQL's actual rule, not a MIN/MAX/SUM/AVG allowlist:
// any projection whose value came from an aggregate spec (declaredProjection
// Type already resolves its type from the aggregate's own emitted-type map)
// is, by that same fact, not a bare column reference — the one shape PG
// preserves typmod for.
func declaredWireUnconstrainedDecimal(root *logical.Node) map[string]bool {
	projs, childTypes, _, strictInt, ok := declaredProjectionInputs(root)
	if !ok {
		return nil
	}
	var out map[string]bool
	for _, proj := range projs {
		if !proj.IsAgg {
			continue
		}
		name := declaredProjectionName(proj)
		if name == "" {
			continue
		}
		if declaredProjectionType(proj, childTypes, strictInt) != parquet.TypeDecimal {
			continue
		}
		if out == nil {
			out = make(map[string]bool)
		}
		out[name] = true
	}
	return out
}

// declaredProjectionInputs is declaredOutputSchema's and
// declaredWireUnconstrainedDecimal's shared setup: the visible projection
// list plus the child's emitted-type maps each projection is resolved
// against. ok is false when there is nothing to declare (no output
// projection node, or an empty SELECT list) — callers return their own
// empty answer in that case rather than proceeding with nil maps.
func declaredProjectionInputs(root *logical.Node) (projs []logical.Projection, childTypes map[string]parquet.TypeID, childDecimal map[string]logical.DecimalMeta, strictInt map[string]bool, ok bool) {
	pn := findOutputProjectionNode(root)
	if pn == nil {
		return nil, nil, nil, nil, false
	}
	projs = logical.VisibleProjections(pn.Projections)
	if len(projs) == 0 {
		return nil, nil, nil, nil, false
	}
	if len(pn.Children) == 1 {
		childTypes = emittedColTypes(pn.Children[0])
		childDecimal = emittedColDecimal(pn.Children[0])
		// The same integer-preserving-arithmetic hint the projection builder
		// passes: without it `id + 1` declares FLOAT64 here where the
		// operator emits INT64 (#297's rule), so an empty result would
		// disagree with a full one about the type of its own column.
		strictInt = strictIntArithCols(pn.Children[0])
	}
	return projs, childTypes, childDecimal, strictInt, true
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

// declaredProjectionDecimal is declaredProjectionType's companion for the
// one piece a bare parquet.TypeID cannot carry: a DECIMAL projection's
// precision and scale (#458). It mirrors declaredProjectionType's own
// name-resolution exactly (aggregate output, bare/renamed column reference,
// or undecided for anything computed) so the two never disagree about WHICH
// column's metadata they are describing — only declaredProjectionType's
// caller decides whether the answer here is even consulted (only when the
// projection's declared type is itself DECIMAL).
func declaredProjectionDecimal(proj logical.Projection, decMeta map[string]logical.DecimalMeta) (logical.DecimalMeta, bool) {
	if proj.IsAgg {
		return lookupColDecimal(decMeta, declaredProjectionName(proj))
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		// A computed DECIMAL expression (CAST, arithmetic, a scalar
		// function): declaredProjectionType has no precision/scale
		// inference for these either, so this stays undecided rather than
		// guessing — the same honest "unconstrained" fallback as before.
		return logical.DecimalMeta{}, false
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	return lookupColDecimal(decMeta, ref)
}

// lookupColDecimal is lookupColType's companion for a DECIMAL-meta map.
func lookupColDecimal(decMeta map[string]logical.DecimalMeta, name string) (logical.DecimalMeta, bool) {
	if decMeta == nil || name == "" {
		return logical.DecimalMeta{}, false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if m, ok := decMeta[lc]; ok {
		return m, true
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		if m, ok := decMeta[lc[dot+1:]]; ok {
			return m, true
		}
	}
	return logical.DecimalMeta{}, false
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

// emittedColDecimal is emittedColTypes' companion for DECIMAL precision/
// scale (#458): the same walk over the same node kinds, but holding only
// entries whose column IS declared DECIMAL by emittedColTypes — a name
// present here and absent (or a different type) there would be a
// contradiction between the two answers describing the same column.
//
// A Window's own expressions never introduce a new DECIMAL (MIN/MAX/value
// functions over a DECIMAL argument declare DECIMAL via windowSpecOutputType,
// but this walk does not have a per-expression source-column resolver for
// window specs the way emittedColTypes does — window output precision/scale
// stays undecided, same as before #458), so only the input passthrough
// carries entries there.
func emittedColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColDecimal(n.Children[0])
		out := make(map[string]logical.DecimalMeta, len(n.GroupBy)+len(n.AggExprs))
		for _, g := range n.GroupBy {
			if m, ok := lookupColDecimal(in, g); ok {
				out[strings.ToLower(g)] = m
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			if m, known := aggSpecOutputDecimal(n, agg); known {
				out[name] = m
			}
		}
		return out
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColDecimal(n.Children[0])
		out := make(map[string]logical.DecimalMeta, len(n.Projections))
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			if m, ok := declaredProjectionDecimal(proj, in); ok {
				out[strings.ToLower(name)] = m
			}
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColDecimal(n.Children[0])
	case logical.NodeWindow:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColDecimal(n.Children[0])
	}
	return inputColDecimal(n)
}
