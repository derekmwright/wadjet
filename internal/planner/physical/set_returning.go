// SPDX-License-Identifier: MIT

package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Set-returning functions in a SELECT list (PostgreSQL §9.26, §38.5.8).
//
// `SELECT unnest(ix.indkey) attnum, generate_subscripts(ix.indkey, 1) ord
// FROM pg_index ix` is how SQLAlchemy reads a primary key's columns. The
// item is planned as its ARRAY argument by the ordinary Project, and an
// exec.ProjectSet directly above expands each row into one row per element
// — PostgreSQL 10's rule: as many rows as the longest set, shorter sets
// padded with NULL, a row whose sets are all empty dropped.
//
// Only a set-returning call that IS the item is planned; one nested in an
// expression, or in a list that also aggregates or windows (where
// PostgreSQL evaluates the set after the aggregate), is refused 0A000 rather
// than answered through a rule this planner does not implement.

// srfKind is what a set-returning SELECT item expands its array into.
type srfKind int

const (
	srfUnnest     srfKind = iota // the elements
	srfSubscripts                // the subscripts 1..n
	srfExpand                    // information_schema._pg_expandarray: ROW(x element, n subscript)
)

// srfName is a call's name with the schema qualifiers PostgreSQL resolves
// these functions under removed.
func srfName(name string) string {
	n := strings.ToLower(name)
	n = strings.TrimPrefix(n, "pg_catalog.")
	return strings.TrimPrefix(n, "information_schema.")
}

// setReturningCall reports whether n is a set-returning call this planner
// expands: unnest(array), generate_subscripts(array, dim),
// information_schema._pg_expandarray(array), or a field of the last —
// `(_pg_expandarray(a)).n` is generate_subscripts(a, 1) and `.x` is
// unnest(a), in lockstep, which is exactly what the composite's fields are.
// dim is the subscripts' dimension argument (nil otherwise).
func setReturningCall(n plansql.Node) (fn *plansql.FuncCallNode, arr, dim plansql.Node, kind srfKind, ok bool) {
	fn, ok = plansql.Unparen(n).(*plansql.FuncCallNode)
	if !ok {
		return nil, nil, nil, 0, false
	}
	one := func(k srfKind) (*plansql.FuncCallNode, plansql.Node, plansql.Node, srfKind, bool) {
		if len(fn.Args) == 0 {
			return fn, nil, nil, k, true
		}
		return fn, fn.Args[0], nil, k, true
	}
	switch srfName(fn.Name) {
	case "unnest":
		return one(srfUnnest)
	case "_pg_expandarray":
		return one(srfExpand)
	case "generate_subscripts":
		if len(fn.Args) == 2 {
			return fn, fn.Args[0], fn.Args[1], srfSubscripts, true
		}
		return one(srfSubscripts)
	case "row_field":
		if len(fn.Args) != 2 {
			return nil, nil, nil, 0, false
		}
		inner, isCall := plansql.Unparen(fn.Args[0]).(*plansql.FuncCallNode)
		field, isLit := fn.Args[1].(*plansql.Lit)
		if !isCall || !isLit || srfName(inner.Name) != "_pg_expandarray" || len(inner.Args) != 1 {
			return nil, nil, nil, 0, false
		}
		switch strings.ToLower(field.Value) {
		case "x":
			return inner, inner.Args[0], nil, srfUnnest, true
		case "n":
			return inner, inner.Args[0], &plansql.Lit{Value: "1", Kind: plansql.LitNumber}, srfSubscripts, true
		}
	}
	return nil, nil, nil, 0, false
}

// setReturningProjection rewrites a set-returning item to its array
// argument, keeping the name the item publishes, and answers the SetColumn
// the ProjectSet will expand it with (nil for any other item).
func (p *Planner) setReturningProjection(proj *logical.Projection, decls ColDecls, child *logical.Node, aggregatesOrWindows bool, index int) (*exec.SetColumn, error) {
	fn, arr, dimArg, kind, ok := setReturningCall(proj.ASTExpr)
	if !ok || proj.IsAgg {
		return nil, nil
	}
	if aggregatesOrWindows {
		return nil, sqlerr.New("0A000",
			"%s in a SELECT list that also aggregates or uses window functions is not supported", fn.Name)
	}
	if arr == nil || fn.Distinct || fn.Star || (kind == srfSubscripts && dimArg == nil) ||
		(kind != srfSubscripts && len(fn.Args) != 1 && srfName(fn.Name) != "row_field") {
		return nil, sqlerr.New("42883", "function %s with %d arguments does not exist", fn.Name, len(fn.Args))
	}
	col := &exec.SetColumn{Index: index, Subscripts: kind == srfSubscripts, Expand: kind == srfExpand}
	if el, ok := setElement(arr, decls); ok {
		col.Out = el
		col.OutKnown = true
	} else if cr, isRef := plansql.Unparen(arr).(*plansql.ColRef); isRef {
		if el, ok := scanElement(child, cr); ok {
			col.Out = el
			col.OutKnown = true
		}
	}
	if kind == srfSubscripts {
		lit, isLit := dimArg.(*plansql.Lit)
		if !isLit || lit.Kind != plansql.LitNumber {
			return nil, sqlerr.New("0A000",
				"generate_subscripts: the dimension must be an integer constant here, not %s", dimArg.String())
		}
		dim, err := strconv.ParseInt(lit.Value, 10, 64)
		if err != nil {
			return nil, sqlerr.New("22P02", "invalid input syntax for type integer: %q", lit.Value)
		}
		col.Dim = dim
	}
	if proj.Alias == "" {
		proj.Alias = srfName(fn.Name)
		if proj.PublishedName != "" {
			proj.Alias = proj.PublishedName
		}
	}
	proj.ASTExpr = arr
	proj.Expr = arr.String()
	proj.Column = ""
	return col, nil
}

// nestedSetArgument reports a MULTI-DIMENSIONAL array argument that is not a
// stored column: its element is itself an array. PostgreSQL's unnest yields
// the LEAVES of such a value and this engine, which holds it as an array of
// arrays, would yield the inner arrays — so it refuses, as it did before arc
// CW declared a constructor's element (round-4 review B3; round 5 returns
// multi-dimensional arrays to base behaviour, and the stored-column spelling,
// which answered the inner arrays at base too, is recorded in
// postgres-differences with the rest of the multi-dimensional semantics).
func nestedSetArgument(col *exec.SetColumn, arg plansql.Node) bool {
	if col.Out.Type != parquet.TypeArray {
		return false
	}
	_, isRef := plansql.Unparen(arg).(*plansql.ColRef)
	return !isRef
}

// finishSetColumn records what the expanded item publishes once its array
// argument's projection is planned: the element's declaration for unnest,
// int4 for generate_subscripts. An argument whose element is unknown, or a
// multi-dimensional one that is not a stored column (nestedSetArgument), is
// refused 0A000.
func finishSetColumn(col *exec.SetColumn, pc *exec.ProjectColumn, arg plansql.Node) error {
	if pc.Type != parquet.TypeArray && pc.Type != parquet.TypeString {
		return sqlerr.New("42883", "function %s(%s) does not exist", setName(col), pc.Type)
	}
	pc.Type = parquet.TypeArray
	pc.Computed = true
	// The Project materializes the array argument only with its element
	// declared, whichever function expands it.
	if pc.ElementType != nil {
		col.Out = *pc.ElementType
		col.OutKnown = true
	}
	if !col.OutKnown || nestedSetArgument(col, arg) {
		return sqlerr.New("0A000",
			"%s: the element type of its array argument is not known here, so the set cannot be declared", setName(col))
	}
	if pc.ElementType == nil {
		el := col.Out
		pc.ElementType = &el
	}
	switch {
	case col.Subscripts:
		col.Out = parquet.Column{Type: parquet.TypeInt32}
	case col.Expand:
		col.Out = expandedRow(col.Out)
	}
	col.Out.Name = pc.Name
	col.Out.Nullable = true
	return nil
}

func setName(col *exec.SetColumn) string {
	switch {
	case col.Subscripts:
		return "generate_subscripts"
	case col.Expand:
		return "_pg_expandarray"
	}
	return "unnest"
}

// expandedRow is information_schema._pg_expandarray's composite: x, the
// element, and n, its int4 subscript.
func expandedRow(elem parquet.Column) parquet.Column {
	x := elem
	x.Name = "x"
	x.Nullable = true
	return parquet.Column{Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
		x, {Name: "n", Type: parquet.TypeInt32, Nullable: true}}}
}

// setReturningDeclType is the declaration of a set-returning item for the
// output schema: unnest's element, generate_subscripts' int4.
func setReturningDeclType(n *plansql.FuncCallNode, decls ColDecls) (expr.DeclType, expr.Confidence, bool) {
	_, arr, _, kind, ok := setReturningCall(n)
	if !ok {
		return expr.DeclType{}, expr.Undecided, false
	}
	if kind == srfSubscripts {
		return expr.Decl(parquet.TypeInt32), expr.Decided, true
	}
	if arr == nil {
		return expr.DeclType{}, expr.Undecided, true
	}
	el, ok := setElement(arr, decls)
	if !ok {
		return expr.DeclType{}, expr.Undecided, true
	}
	if kind == srfExpand {
		row := expandedRow(el)
		return expr.DeclType{ID: parquet.TypeRow, Schema: &row}, expr.Decided, true
	}
	if el.Type == parquet.TypeDecimal {
		return expr.DeclDecimal(el.Precision, el.Scale), expr.Decided, true
	}
	return expr.DeclType{ID: el.Type, Schema: &el}, expr.Decided, true
}

// setElement is the declared ELEMENT of a set-returning call's array
// argument: a column's declared element, an ARRAY[…] literal's COMMON
// element type (expr.ArrayLitElementDecl — every element is materialized
// through it, so the first element's type read `unnest(ARRAY[1,2.5])` as
// 1, 2), or the element of an array-returning expression whose
// declaration carries one (current_schemas() is name[], whose element the
// registry declares as text).
func setElement(n plansql.Node, decls ColDecls) (parquet.Column, bool) {
	n = plansql.Unparen(n)
	el, ok := setElementDecl(n, decls)
	if ok && el.Type == parquet.TypeArray {
		// A multi-dimensional argument that is not a stored column declares
		// no set (nestedSetArgument): the element PostgreSQL yields is its
		// LEAF, not the inner array this engine holds.
		if _, isRef := n.(*plansql.ColRef); !isRef {
			return parquet.Column{}, false
		}
	}
	return el, ok
}

func setElementDecl(n plansql.Node, decls ColDecls) (parquet.Column, bool) {
	if cr, ok := n.(*plansql.ColRef); ok {
		if c, ok := decls.colDecl(cr); ok && c.Type == parquet.TypeArray && c.ElementType != nil {
			return *c.ElementType, true
		}
	}
	if al, ok := n.(*plansql.ArrayLitNode); ok {
		var decided []expr.DeclType
		for _, e := range al.Elements {
			if t, conf := nodeDeclaredType(e, decls); conf == expr.Decided {
				decided = append(decided, t)
			}
		}
		if t, ok := expr.ArrayLitElementDecl(decided); ok {
			return declTypeParts(t), true
		}
	}
	if t, _ := nodeDeclaredType(n, decls); t.ID == parquet.TypeArray && t.Schema != nil && t.Schema.ElementType != nil {
		return *t.Schema.ElementType, true
	}
	return parquet.Column{}, false
}

// scanElement finds a column reference's declared array ELEMENT on the scans
// beneath n: the scan the qualifier names, or the one scan that carries the
// bare name. Two scans declaring different elements for one bare name
// decide nothing.
func scanElement(n *logical.Node, ref *plansql.ColRef) (parquet.Column, bool) {
	var found parquet.Column
	hits := 0
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			if el, ok := n.ScanColElems[strings.ToLower(ref.Column)]; ok && n.ScanColTypes[strings.ToLower(ref.Column)] == parquet.TypeArray {
				if ref.Table == "" || strings.EqualFold(ref.Table, n.TableAlias) || strings.EqualFold(ref.Table, n.TableName) {
					if hits == 0 || found.Type != el.Type {
						hits++
					}
					found = el
				}
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(n)
	return found, hits == 1
}
