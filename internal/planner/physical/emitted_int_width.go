// This file holds the INTEGER WIDTH half of a node's emitted declaration, the
// companion to emittedColTypes (the carrier) and emittedColDecimal (the
// (p,s)). Governed by ADR-0024 item 2 and ADR-0012.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// emittedColIntWidth is emittedColTypes' companion for PostgreSQL's INTEGER
// WIDTH: the same walk over the same node kinds, holding an entry only for a
// column emittedColTypes declares with an INTEGER CARRIER (carriesIntWidth) —
// a name present here and non-integer there would be a contradiction between
// two answers about one column.
//
// It exists because the carrier is not the width. `SELECT BITWISE_AND(id, 3)
// AS v` publishes an INT64 column whose PostgreSQL type is integer, and a
// reader with only the carrier declared `SUM(v)` numeric where PostgreSQL
// declares bigint — while the DIRECT call one level down declared it right,
// because there the CALL was still visible in the AST. The width has to ride
// the declaration through every Project, derived table, CTE, set-operation
// arm, window slot and join the way a DECIMAL's (p,s) already does (#1018
// round 5, B1).
//
// An ABSENT entry means the declaration says nothing, and every reader falls
// back to the carrier — which for a base column IS the catalog's storage
// width, so a scan needs no entries at all.
func emittedColIntWidth(n *logical.Node) map[string]intWidth {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		in := colDecls{
			types:    emittedColTypes(child),
			fields:   inputColFields(child),
			dec:      emittedColDecimal(child),
			intWidth: emittedColIntWidth(child),
		}
		out := make(map[string]intWidth, len(n.GroupBy)+len(n.AggExprs))
		// A GROUP KEY is the value the input carried, so it keeps the input's
		// width — including a DERIVED key, which is emitted under its
		// expression TEXT and so has no name in the input at all. Losing it
		// there is how `SELECT SUM(k) FROM (SELECT id & 3 AS k … GROUP BY 1)`
		// would read int8 for an int4 key.
		for _, g := range n.GroupBy {
			name := strings.ToLower(strings.TrimSpace(g))
			if name == "" {
				continue
			}
			if w, ok := lookupColIntWidth(in.intWidth, g); ok {
				out[name] = w
				continue
			}
			if t, ok := lookupColType(in.types, g); ok {
				if w := catalogIntWidth(t); w != intWidthUnknown {
					out[name] = w
				}
				continue
			}
			if ast, ok := groupKeyAST(child, g); ok {
				if w := declaredIntWidth(ast, in); w != intWidthUnknown {
					out[name] = w
				}
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			t, known := aggSpecOutputType(n, agg)
			if !known || !carriesIntWidth(t) {
				continue
			}
			// MIN/MAX/MIN_BY/MAX_BY hand back a value the input HELD, so they
			// keep its width, exactly as aggSpecOutputDecimal keeps its (p,s).
			// SUM/AVG/COUNT and the rest ANSWER in their own declared type,
			// whose width is that type's own: `sum(int4)` is bigint, and a
			// bigint column's SUM is numeric.
			switch strings.ToLower(strings.TrimSpace(agg.Func)) {
			case "min", "max", "min_by", "max_by":
				if w, ok := aggArgIntWidth(agg, in); ok {
					out[name] = w
					continue
				}
			}
			out[name] = catalogIntWidth(t)
		}
		return out
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		strictInt := strictIntArithCols(child)
		decls := colDecls{
			types:    emittedColTypes(child),
			fields:   inputColFields(child),
			dec:      emittedColDecimal(child),
			intWidth: emittedColIntWidth(child),
		}
		out := make(map[string]intWidth, len(n.Projections))
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			if w := declaredProjectionIntWidth(proj, decls, strictInt); w != intWidthUnknown {
				out[strings.ToLower(name)] = w
			}
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColIntWidth(n.Children[0])
	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		// PostgreSQL resolves a set operation's column to the COMMON type of
		// its arms, and for two integers that is the WIDER one: a UNION ALL
		// of `id & 3` and `visits & 18` is bigint and its SUM is numeric,
		// while a UNION ALL of two int4 arms is integer and its SUM is
		// bigint (both measured on PostgreSQL 17.11).
		//
		// Every arm must answer, or nothing is recorded: an arm whose
		// declaration is silent could be the int8 one, and narrowing on
		// incomplete information is how a SUM that should be numeric would
		// come back as a bigint that can overflow.
		arms := setOpArmIntWidths(n)
		if len(arms) == 0 {
			return nil
		}
		cols, ok := setOpDeclaredOutputSchema(n)
		if !ok {
			return nil
		}
		names := setOpArmSchemas(n)
		if len(names) == 0 || len(cols) != len(names[0]) {
			return nil
		}
		out := make(map[string]intWidth, len(cols))
		for i, col := range cols {
			if !carriesIntWidth(col.Type) {
				continue
			}
			w := intWidthUnknown
			silent := false
			for _, arm := range arms {
				if i >= len(arm) || arm[i] == intWidthUnknown {
					silent = true
					break
				}
				w = widerIntWidth(w, arm[i])
			}
			if silent || w == intWidthUnknown {
				continue
			}
			out[strings.ToLower(names[0][i].Name)] = w
		}
		return out
	case logical.NodeWindow:
		// A Window appends its slots and drops nothing, so every input column
		// keeps its width and only the slots need one.
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		in := colDecls{
			types:    emittedColTypes(child),
			fields:   inputColFields(child),
			dec:      emittedColDecimal(child),
			intWidth: emittedColIntWidth(child),
		}
		out := make(map[string]intWidth, len(in.intWidth)+len(n.WindowExprs))
		for k, v := range in.intWidth {
			out[k] = v
		}
		for _, we := range n.WindowExprs {
			name := strings.ToLower(we.OutputCol)
			if name == "" {
				continue
			}
			d := windowSpecOutputType(n, we)
			if !carriesIntWidth(d.ID) {
				delete(out, name)
				continue
			}
			// The same split the grouped arm makes: a VALUE function and
			// MIN/MAX lift a value the input held and keep its width; an
			// accumulating or ranking slot answers in its own declared type.
			fn := strings.ToLower(strings.TrimSpace(we.Func))
			if windowValueFunc(fn) || fn == "min" || fn == "max" {
				if col := cleanExpr(we.InputColumn()); col != "" {
					if w, ok := in.colIntWidth(&plansql.ColRef{Column: col}); ok {
						out[name] = w
						continue
					}
					if t, ok := lookupColType(in.types, col); ok {
						out[name] = catalogIntWidth(t)
						continue
					}
				}
				// A COMPUTED argument has no column to read the width off,
				// and the carrier below is the INT64 every integer expression
				// materializes in — so `MIN(BITWISE_AND(int4,3)) OVER ()`
				// claimed int8 and its SUM went out numeric where PostgreSQL
				// says bigint. The same walk the grouped spelling's
				// aggArgIntWidth now takes (#1018 round 5 review, P1).
				if we.InputExpr != nil {
					if _, bare := we.InputExpr.(*plansql.ColRef); !bare {
						if w := declaredIntWidth(we.InputExpr, in); w != intWidthUnknown {
							out[name] = w
							continue
						}
					}
				}
			}
			out[name] = catalogIntWidth(d.ID)
		}
		return out
	case logical.NodeJoin:
		// The width half of emittedColTypes' join arm, by the same rule and
		// for the same reason: the two answers describe one column. A name
		// the two sides declare differently is DROPPED rather than resolved,
		// which leaves the reader on the carrier — the answer it had before.
		if len(n.Children) != 2 {
			return nil
		}
		left, right := emittedColIntWidth(n.Children[0]), emittedColIntWidth(n.Children[1])
		return withJoinArmQualifiers(n, left, right, mergeJoinSides(left, right))
	}
	return nil
}

// setOpArmIntWidths is setOpArmSchemasAndTypmods' width companion: one slice
// per arm, POSITIONAL, aligned with that arm's declared output schema.
func setOpArmIntWidths(n *logical.Node) [][]intWidth {
	var out [][]intWidth
	for _, c := range n.Children {
		if inner := setOpRoot(c); inner != nil {
			nested := setOpArmIntWidths(inner)
			if len(nested) == 0 {
				return nil
			}
			out = append(out, nested...)
			continue
		}
		schema := declaredOutputSchema(c, nil)
		if len(schema) == 0 {
			return nil
		}
		widths := emittedColIntWidth(c)
		arm := make([]intWidth, len(schema))
		for i, col := range schema {
			if w, ok := lookupColIntWidth(widths, col.Name); ok {
				arm[i] = w
				continue
			}
			arm[i] = catalogIntWidth(col.Type)
		}
		out = append(out, arm)
	}
	return out
}

// aggArgIntWidth is the width of an aggregate ARGUMENT, for the value-
// preserving aggregates (MIN/MAX/MIN_BY/MAX_BY).
//
// A COMPUTED argument takes the same walk every other declared width takes.
// Declining it was not silence: the caller then recorded `catalogIntWidth` of
// aggSpecOutputType, which is the INT64 CARRIER every integer expression is
// computed in, so `MIN(BITWISE_AND(int4_col, 3))` positively declared int8 and
// every reader above it made its SUM numeric — where PostgreSQL 17.11 answers
// `integer` for the MIN (measured: `min(id & 3)`, `max(id & 3)`,
// `min(regexp_count(name,'a'))`, grouped and `OVER ()`) and `bigint` for its
// SUM (#1018 round 5 review, P1). `declaredIntWidth` already knew that
// expression's width; this arm simply asks it, which is what the ColRef arm
// does one level down.
func aggArgIntWidth(agg logical.AggExpr, in colDecls) (intWidth, bool) {
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			if w := declaredIntWidth(agg.InputExpr, in); w != intWidthUnknown {
				return w, true
			}
			// Unknown is NOT int8. Leaving the caller to record the carrier is
			// what it did before this arm existed, and a width nobody can
			// prove must not be invented here.
			return intWidthUnknown, false
		}
	}
	col := strings.TrimSpace(agg.InputCol)
	if col == "" {
		return intWidthUnknown, false
	}
	if w, ok := in.colIntWidth(&plansql.ColRef{Column: col}); ok {
		return w, true
	}
	if t, ok := lookupColType(in.types, col); ok {
		return catalogIntWidth(t), true
	}
	return intWidthUnknown, false
}

// groupKeyAST recovers the expression a DERIVED group key was written as, so
// its width can be derived the way its type already is (derivedGroupKeyTypes).
func groupKeyAST(child *logical.Node, key string) (plansql.Node, bool) {
	if child == nil {
		return nil, false
	}
	ast, err := plansql.ParseExpression(key)
	if err != nil || ast == nil {
		return nil, false
	}
	if _, bare := ast.(*plansql.ColRef); bare {
		return nil, false
	}
	return ast, true
}

// declaredProjectionIntWidth is declaredProjectionDecl's width half: the same
// shapes resolved in the same order, so the width and the carrier can never
// describe two different columns (ADR-0022 rule 1).
func declaredProjectionIntWidth(proj logical.Projection, decls colDecls, strictInt map[string]bool) intWidth {
	declared := declaredProjectionDecl(proj, decls, strictInt)
	if !carriesIntWidth(declared.ID) {
		return intWidthUnknown
	}
	if proj.IsAgg {
		name := declaredProjectionName(proj)
		if w, ok := lookupColIntWidth(decls.intWidth, name); ok {
			return w
		}
		return catalogIntWidth(declared.ID)
	}
	if fc, ok := declaredFieldPath(proj, decls); ok {
		return catalogIntWidth(fc.Type)
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		// The producer may PUBLISH this expression as a column under its own
		// TEXT (a derived GROUP BY key, the DISTINCT lowering). Above such a
		// producer the expression is a NAME, and its width is the one the
		// producer declared — re-deriving it as structure would look for
		// columns the producer no longer emits (ADR-0026 §2c).
		if name := strings.TrimSpace(proj.Expr); name != "" {
			if w, ok := lookupColIntWidth(decls.intWidth, name); ok {
				return w
			}
			if t, ok := lookupColType(decls.types, name); ok {
				return catalogIntWidth(t)
			}
		}
		return declaredIntWidth(proj.ASTExpr, decls)
	}
	if cr, ok := bareColRefOf(proj.ASTExpr); ok {
		return declaredIntWidth(cr, decls)
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	if w, ok := lookupColIntWidth(decls.intWidth, ref); ok {
		return w
	}
	return catalogIntWidth(declared.ID)
}

// lookupColIntWidth is lookupColType's width companion, resolving a name that
// may still carry a qualifier the map is keyed without.
func lookupColIntWidth(widths map[string]intWidth, name string) (intWidth, bool) {
	if widths == nil || name == "" {
		return intWidthUnknown, false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if w, ok := widths[lc]; ok {
		return w, true
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		if w, ok := widths[lc[dot+1:]]; ok {
			return w, true
		}
	}
	return intWidthUnknown, false
}
