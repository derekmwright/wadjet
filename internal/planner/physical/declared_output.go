// SPDX-License-Identifier: MIT

// This file holds declared output for the physical planner, governed by ADR-0024 and ADR-0026.
package physical

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// inferProjectionTypeCols is inferProjectionType with the input's column types
// in hand, in the two shapes the planner can supply them.
//
// strictInt marks scan columns whose vectors are plain Int64/Int32:
// integer-preserving arithmetic (+, -, *, % over those columns and integer
// literals) declares Int64 instead of the blanket Float64, keeping derived
// GROUP BY keys on the typed-int aggregation paths (#297; ClickBench Q36's
// four ClientIP-derived keys). The rule mirrors expr.BinOpNumeric's runtime
// mode resolution and MUST stay a strict subset of it: a declared-int column
// over a float-mode expression reads NULL through the typed getter. The
// reverse direction (declared float, runtime int) is the pre-existing safe
// coercion.
//
// colTypes (inputColTypes) is the full column→catalog-type map, and it is what
// lets a bare column reference INSIDE the expression decide a type rather than
// leaving the polymorphic declarations to fall back to Float64 (#333).
func inferProjectionTypeCols(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, colTypes map[string]parquet.TypeID) parquet.TypeID {
	return inferProjectionTypeDecls(node, fallback, strictInt, ColDecls{Types: colTypes})
}

// inferProjectionTypeDecls is inferProjectionTypeCols with the ROW FIELDS of
// its input in hand as well as the column types, so a field path inside the
// expression can decide a type. Callers that hold the logical node the
// expression reads should use this one (InputColDecls); the map-only
// signature above stays for the callers whose types are synthesized rather
// than read off a scan (EmittedColTypes and friends), where there are no
// fields to carry.
func inferProjectionTypeDecls(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, decls ColDecls) parquet.TypeID {
	return inferProjectionDeclType(node, fallback, strictInt, decls).ID
}

// inferProjectionDeclType is inferProjectionTypeDecls with the parameterized
// part of the answer kept — a DECIMAL's (precision, scale), which a bare
// parquet.TypeID cannot carry and which the output vector must have or every
// value in it reads back at the wrong power of ten (ADR-0024 item 2).
// Callers that materialize a vector from the answer take this one; callers
// that only need the TypeID keep the wrapper above.
func inferProjectionDeclType(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, decls ColDecls) expr.DeclType {
	t, _ := inferProjectionDeclTypeConf(node, fallback, strictInt, decls)
	return t
}

// inferProjectionDeclTypeConf is InferProjectionDeclType with the CONFIDENCE
// it reached. It holds the BODY, and the wrapper above is one line, because
// two copies of the two arms below — integer-preserving arithmetic, and the
// declarations withheld for a bare reference — is how two callers come to
// disagree about one column.
//
// A projection may declare a GUESS: exec.Project builds the vector and the
// runtime corrects what it can. The GATHER may not — `evalDeclaredColumn`
// builds the vector from the declaration alone and `SetValue` renders whatever
// box arrives into it, so an UNDECIDED answer dressed as the STRING fallback
// turns a DATE into its epoch day (#831 review). That caller asks for the
// confidence and declines on Undecided.
func inferProjectionDeclTypeConf(node plansql.Node, fallback parquet.TypeID,
	strictInt map[string]bool, decls ColDecls) (expr.DeclType, expr.Confidence) {
	if strictInt != nil && expr.IntArithOn() {
		inner := node
		for {
			p, ok := inner.(*plansql.ParenNode)
			if !ok {
				break
			}
			inner = p.Inner
		}
		// A UNARY ± belongs here beside the binary operators, and leaving it
		// out is what made `ORDER BY` over a derived block's `-g` sort by
		// BYTES on the stage DAG. The binary spellings of the same value —
		// `g * -3`, `0 - g` — declare INT64 through this arm because
		// `strictInt` names the column; `-g` fell past it, and over an
		// AGGREGATE's output `decls` does not carry the group key either, so
		// NodeDeclaredType answered Undecided and the STRING fallback stood.
		// The sort key then materialized into a text vector and `-1` sorted
		// before `-5`. `expr.UnaryOp.Eval` negates an int64 as an int64
		// (#369), so the declaration this makes is the one the runtime keeps.
		//
		// Only an OPERATOR node: a bare `*plansql.ColRef` is deliberately
		// withheld from this walk (below), because exec.Project types a copy
		// from the column it copies.
		switch inner.(type) {
		case *plansql.BinaryOp, *plansql.UnaryOp:
			if intArithAllInt(inner, strictInt, decls) {
				return expr.Decl(parquet.TypeInt64), expr.Decided
			}
		}
	}
	_, bareRef := node.(*plansql.ColRef)
	if bareRef && !astIsFieldPath(node, decls) {
		// A bare reference copies its input column; exec.Project uses the input
		// schema, including renames/derived inputs. Withhold declarations to leave
		// the caller's fallback in charge (#333 concerns computed arguments).
		// Do NOT withhold for parenthesized references: their text matches no input
		// column, so runtime cannot correct the fallback. ROW field paths likewise
		// copy no column; retain their catalog declaration (#568).
		decls = ColDecls{}
	}
	// A guess is still the answer here: nothing is left to consult, and a
	// polymorphic function's fallback is what types SELECT NULLIF(int_col, 1)
	// numeric. Only expr.Undecided leaves the type to the caller.
	if t, c := nodeDeclaredType(node, decls); c != expr.Undecided {
		return t, c
	}
	return expr.Decl(fallback), expr.Undecided
}

// realArithBothReal reports whether both operands of an arithmetic node are
// REAL, which is the one pairing PostgreSQL answers in real.
//
// Measured on 17.11 over a float4 column: `real + real` and `real * real` and
// `real / real` are real, while `real + 1.0` is double precision (a numeric
// literal), `real + 1` is double precision (an integer) and `real + float8` is
// double precision. So the test is both sides, DECIDED, and FLOAT32 — nothing
// weaker, because widening the rule would narrow a value the server keeps at
// float8's width.
//
// The DECLARATION is the whole fix. This engine computes `r1 + r2` on the
// float64 carrier, and the exact sum, difference or product of two float32s is
// representable in a float64, so rounding it once into a float4 output vector
// is the correctly-rounded float4 answer — which is why `r + 1.0::real` at
// 2^24 answered 16777217 under a float8 declaration and answers PostgreSQL's
// 16777216 under this one (#1117). A result with no float4 is
// batch.FloatRangeError's 22003, the server's own answer.
//
// `%` is excluded: PostgreSQL has no float modulo at all, so there is no
// server type to follow and wadjet's own answer stays double.
func realArithBothReal(n *plansql.BinaryOp, decls ColDecls) bool {
	switch n.Op {
	case "+", "-", "*", "/":
	default:
		return false
	}
	isReal := func(side plansql.Node) bool {
		d, c := nodeDeclaredType(side, decls)
		return c == expr.Decided && d.ID == parquet.TypeFloat32
	}
	return isReal(n.Left) && isReal(n.Right)
}

// intWidthFullyKnown reports whether every integer operand of n names a width
// of its own — no leaf the walk cannot type.
//
// declaredIntWidth alone is not that test, and the difference is a query that
// stopped answering. Its combinator is widerIntWidth, whose rule is "an
// unknown operand contributes nothing rather than narrowing" — right for the
// AGGREGATE question it was written for, where an unknown argument leaves the
// accumulator alone — and read as a DECLARATION it says int4 for
// `(SELECT MAX(c_i64) ...) + 1`, whose left operand it cannot type at all.
// That declared an int4 output vector for a bigint total and the store guard
// refused the row: 22003 on a query PostgreSQL answers (#1070, found by
// pgwire.TestArcH1AScalarSubqueryDeclaresItsOwnTypeOnTheWire).
//
// So a declaration that narrows asks `declaredIntWidth(n) == intWidth4 &&
// intWidthFullyKnown(n)`, and the walk this makes is the same shape
// declaredIntWidth's: through the operators and the choice arms to the LEAVES,
// each of which must name a width of its own. bitwiseInt4Result is the one
// caller left — the arithmetic and CAST narrowings that were its other two
// were measured, reverted and recorded in ADR-0012.
func intWidthFullyKnown(node plansql.Node, decls ColDecls) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return intWidthFullyKnown(n.Inner, decls)
	case *plansql.UnaryOp:
		return intWidthFullyKnown(n.Inner, decls)
	case *plansql.BinaryOp:
		return intWidthFullyKnown(n.Left, decls) && intWidthFullyKnown(n.Right, decls)
	case *plansql.CaseNode:
		for _, w := range n.Whens {
			if !intWidthFullyKnown(w.Result, decls) {
				return false
			}
		}
		if n.Else != nil {
			return intWidthFullyKnown(n.Else, decls)
		}
		return true
	}
	// A LEAF — a column, a literal, a cast, a function call — answers for
	// itself: a function whose own PostgreSQL result width is int4 is int4
	// whatever its arguments are, and a name nothing declares is unknown.
	return declaredIntWidth(node, decls) != intWidthUnknown
}

// intArithColumnType is the set expr.operandIsInt's ColRef arm accepts. PORT
// and PROTOCOL are in it because their arithmetic runs on the int4 kernels
// (#1000): the declaration and the kernel have to name the same set, or the
// plan promises an integer the runtime does not produce — or, as here before
// the kernel moved, declines one it does.
func intArithColumnType(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt64, parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		return true
	}
	return false
}

// intArithAllInt mirrors expr.operandIsInt over the AST: int-typed scan
// columns, integer literals, and nested integer arithmetic. Anything
// unrecognized declines (Float64 declaration = today's behavior). `/` is
// integer division over integer operands (#369, ADR-0012), so it declares
// Int64 exactly as +,-,*,% do — mirroring expr.BinOpNumeric's runtime mode,
// of which this must stay a strict subset.
func intArithAllInt(node plansql.Node, strictInt map[string]bool, decls ColDecls) bool {
	switch n := node.(type) {
	case *plansql.BinaryOp:
		switch n.Op {
		case "+", "-", "*", "%", "/":
		default:
			return false
		}
		return intArithAllInt(n.Left, strictInt, decls) && intArithAllInt(n.Right, strictInt, decls)
	case *plansql.UnaryOp:
		// Unary ± preserves an integer operand's integer-ness, exactly as
		// NodeDeclaredType's own UnaryOp arm has it and as expr.UnaryOp.Eval
		// computes it.
		switch n.Op {
		case "+", "-":
		default:
			return false
		}
		return intArithAllInt(n.Inner, strictInt, decls)
	case *plansql.ParenNode:
		return intArithAllInt(n.Inner, strictInt, decls)
	case *plansql.ColRef:
		if strictInt != nil {
			if strictInt[strings.ToLower(n.Column)] {
				return true
			}
		} else if c, ok := decls.colDecl(n); ok && !decls.isFieldPath(n) {
			// A nil strictInt means the caller is the DECLARED-type walk
			// (NodeDeclaredType), which runs at every nested site — a CASE
			// branch, a COALESCE argument, an aggregate's input — and has no
			// scan-level column set to consult. The catalog types in decls
			// are the same authority colRefDeclaredType already trusts to
			// type a bare column reference at those very sites, so reading
			// them here claims nothing new; it only stops the claim from
			// evaporating the moment the reference sits under a `+`.
			return intArithColumnType(c.Type)
		}
		// A ROW FIELD PATH of a strictly-int type is one too. strictInt is
		// keyed by COLUMN name and a field is not a column, so `rw.n + 1`
		// declared FLOAT64 where `n + 1` over the same value declares INT64
		// — the two spellings of one question answering with different types
		// (#568, #297's rule).
		if f, ok := decls.field(n); ok && decls.isFieldPath(n) {
			return intArithColumnType(f.Type)
		}
		return false
	case *plansql.FuncCallNode:
		// A function DECLARED integer makes the arithmetic over it integer
		// arithmetic: `length(s) / 2` is 2 in PostgreSQL, not 2.5 (#636).
		// expr.isIntNative reads the same declaration off the same registry,
		// which is what keeps this a strict mirror of the runtime rather than
		// a second hand-maintained name list drifting away from it.
		if expr.FuncReturnsInteger(n.Name) {
			return true
		}
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return false
		}
		_, err := strconv.ParseInt(n.Value, 10, 64)
		return err == nil
	}
	// Everything else asks the DECLARED-TYPE walk, which is the same question
	// one clause up: a CAST to an integer type, a POLYMORPHIC function over
	// integer arguments (abs, coalesce, nullif, greatest, least), and a CASE
	// whose arms all resolve integer are integer expressions, so arithmetic
	// over them is integer arithmetic with its 22003 (#849, ADR-0024 item 2).
	//
	// The arms above are kept rather than folded into this one because they
	// answer WITHOUT decls — the scan-level strictInt set, an integer literal,
	// a FIXED registry declaration — and they are the hot path. This is the
	// tail: the node kinds NodeDeclaredType knows and the switch above does
	// not, which before #849 all fell through to `return false` and pinned the
	// whole expression to float64.
	//
	// Only a DECIDED answer counts. A GUESS is a polymorphic declaration's
	// fallback over arguments nothing resolved, and a wrong int claim here
	// declares an INT64 vector for a float the kernel will produce.
	// expr.binOpIntOperand is the runtime mirror and reads the same Ret.
	t, c := nodeDeclaredType(node, decls)
	if c != expr.Decided {
		return false
	}
	return t.ID == parquet.TypeInt64 || t.ID == parquet.TypeInt32
}

// strictIntArithCols resolves the strictly-int column set feeding a node,
// walking only shape-preserving single-child steps. It deliberately stops
// at Project nodes: a projection may rebind a name to a non-int value,
// and a wrong int claim corrupts (see inferProjectionTypeCols); declining
// just keeps the Float64 declaration.
func strictIntArithCols(n *logical.Node) map[string]bool {
	for n != nil {
		switch n.Type {
		case logical.NodeScan:
			return n.ScanStrictIntCols
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// inputColTypes reports catalog types visible at n's OUTPUT, keyed by
// lowercased name, or nil if unknown (#333). ScanColTypes comes from
// AnnotateScanColumns and is READ-ONLY; a sole scan's map is returned directly.
// Stop at name-rebinding nodes: Project, Aggregate, Window and set operators.
// Do not search every underlying scan as scanColumnType does. An unannotated
// scan makes the whole answer nil: a partial map cannot distinguish an unknown
// column from a name that is not a column.
func inputColTypes(n *logical.Node) map[string]parquet.TypeID {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		return n.ScanColTypes
	case logical.NodeDual:
		// The row a table-less LATERAL body sees IS the outer row, so the
		// columns its SELECT list may name are the outer subtree's (#1033).
		// Nil on every other Dual, which names nothing.
		return inputColTypes(n.LateralOuterScope)
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColTypes(n.Children[0])
	case logical.NodeWindow:
		// A window APPENDS its outputs to its input and renames nothing, so
		// its input's names survive and the SLOTS join them. Their type is
		// the stage's own answer, `WindowSpecOutputType` — the same one
		// emittedColDecimal reads, and the reason the two agree is that a
		// name typed in one and not in the other is a contradiction about one
		// column.
		//
		// A slot has no catalog column to read, and leaving it untyped is not
		// the safe side: an expression over a DECIMAL window output fell to
		// the float rule and the DAG's fragment refused to store the exact
		// value into a FLOAT64 vector, on a query the single-process path
		// answers (#729, ADR-0024 item 2).
		if len(n.Children) != 1 {
			return nil
		}
		return windowOutputColTypes(n, inputColTypes(n.Children[0]))
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		if items := lateralDualItemDecls(n); items != nil {
			// A table-less LATERAL body publishes its items and nothing else,
			// and its Project over the Dual is a stop this walk cannot see
			// through (#1033). Answer from the ONE derivation instead.
			merged := make(map[string]parquet.TypeID, len(items))
			for c, t := range inputColTypes(n.Children[0]) {
				merged[c] = t
			}
			for name, d := range items {
				merged[name] = d.ID
			}
			return merged
		}
		left, right := inputColTypes(n.Children[0]), inputColTypes(n.Children[1])
		if left == nil || right == nil {
			return nil
		}
		merged := make(map[string]parquet.TypeID, len(left)+len(right))
		for c, t := range left {
			merged[c] = t
		}
		for c, t := range right {
			if prev, dup := merged[c]; dup && prev != t {
				// Two sides carry the name at different types — a self-join
				// is not the only way to reach one. Drop it rather than pick
				// a side; the caller's fallback is the honest answer.
				delete(merged, c)
				continue
			}
			merged[c] = t
		}
		return merged
	}
	return nil
}

// inputColFields is inputColTypes' companion for the ROW FIELDS of its
// columns (#568): the same walk, sourced from ScanColFields, holding an entry
// only for a ROW column that declares fields. A name two scans disagree on is
// dropped rather than picking a side, exactly as the type walk does — a field
// path resolved against the wrong side's ROW is the same silent-wrong-column
// failure, one level down.
// aggregateProjectionFields is the ROW a projected AGGREGATE declares, when it
// declares one. Today that is the bar and nothing else: every other aggregate
// answers a scalar or hands back a value its INPUT column already declared,
// and the latter reaches this walk through the column below it.
//
// It goes through AggOhlcvOutputFields, which is the same function
// AggSpec.OutputFields and the operator's own schema come from — the ONE
// derivation ADR-0035 item 5 names. A second one here is how a bar comes to be
// declared one thing by the projection above it and another by the stage that
// produces it.
func aggregateProjectionFields(project *logical.Node, p logical.Projection) ([]parquet.Column, bool) {
	agg := logical.AggregateBelowProject(project)
	if agg == nil {
		return nil, false
	}
	want := strings.ToLower(cleanExpr(p.Alias))
	if want == "" {
		want = strings.ToLower(cleanExpr(p.Column))
	}
	for _, a := range agg.AggExprs {
		if strings.ToLower(cleanExpr(a.OutputCol)) != want {
			continue
		}
		return aggOhlcvOutputFields(agg, a)
	}
	return nil, false
}

// inputColFields is the ROW view of inputColShapes: each name's declared
// FIELDS (nil for a name whose shape is not a ROW, or that a computed item
// shadows).
func inputColFields(n *logical.Node) map[string][]parquet.Column {
	return shapeFields(inputColShapes(n))
}

// inputColElems is the ARRAY/MAP view of inputColShapes: each container
// name's whole declared column, element included (arc CW, #1133 #1303).
func inputColElems(n *logical.Node) map[string]parquet.Column {
	return shapeElems(inputColShapes(n))
}

func shapeFields(shapes map[string]parquet.Column) map[string][]parquet.Column {
	if shapes == nil {
		return nil
	}
	out := make(map[string][]parquet.Column, len(shapes))
	for k, c := range shapes {
		if c.Type == parquet.TypeRow {
			out[k] = c.Fields
			continue
		}
		out[k] = nil
	}
	return out
}

func shapeElems(shapes map[string]parquet.Column) map[string]parquet.Column {
	var out map[string]parquet.Column
	for k, c := range shapes {
		if (c.Type == parquet.TypeArray || c.Type == parquet.TypeMap) && c.ElementType != nil {
			if out == nil {
				out = make(map[string]parquet.Column)
			}
			out[k] = c
		}
	}
	return out
}

// declShape is the container SHAPE a declaration carries — a ROW's fields,
// an ARRAY's or MAP's element — and the zero Column (a name the shape walk
// holds but says nothing about) for everything else.
func declShape(d expr.DeclType) parquet.Column {
	switch d.ID {
	case parquet.TypeRow:
		if f := d.RowFields(); len(f) > 0 {
			return parquet.Column{Type: parquet.TypeRow, Fields: f}
		}
	case parquet.TypeArray, parquet.TypeMap:
		if d.Schema != nil && d.Schema.ElementType != nil {
			el := d.Schema.ElementType.Clone()
			return parquet.Column{Type: d.ID, ElementType: &el}
		}
	}
	return parquet.Column{}
}

// inputColShapes answers, per name a node's output carries, the CONTAINER
// shape that name is declared with — a ROW's field list, an ARRAY's or a
// MAP's element — which a bare TypeID cannot say. One walk serves both
// halves: the ROW fields a field path is typed from (#568) and the ARRAY/MAP
// element a column reference to a container is declared with, which is what
// a derived table, a CTE, a set operation and a zero-row result carry an
// array's element type through (arc CW; before it the element had no map at
// all and a container column reference declined to the STRING fallback —
// #1133, #1303). A name present with the zero Column is SHADOWED: something
// above rebinds it, and the shapes below no longer describe it.
func inputColShapes(n *logical.Node) map[string]parquet.Column {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		var out map[string]parquet.Column
		put := func(k string, c parquet.Column) {
			if out == nil {
				out = make(map[string]parquet.Column)
			}
			out[k] = c
		}
		for k, f := range n.ScanColFields {
			put(k, parquet.Column{Type: parquet.TypeRow, Fields: f})
		}
		for k, el := range n.ScanColElems {
			t := n.ScanColTypes[k]
			if t != parquet.TypeArray && t != parquet.TypeMap {
				continue
			}
			e := el
			put(k, parquet.Column{Type: t, ElementType: &e})
		}
		return out
	case logical.NodeDual:
		return inputColShapes(n.LateralOuterScope)
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColShapes(n.Children[0])

	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		cols, ok := setOpDeclaredOutputSchema(n)
		if !ok {
			return nil
		}
		var shapes map[string]parquet.Column
		for _, c := range cols {
			sh := parquet.Column{}
			switch {
			case c.Type == parquet.TypeRow && len(c.Fields) > 0:
				sh = parquet.Column{Type: parquet.TypeRow, Fields: c.Fields}
			case (c.Type == parquet.TypeArray || c.Type == parquet.TypeMap) && c.ElementType != nil:
				el := c.ElementType.Clone()
				sh = parquet.Column{Type: c.Type, ElementType: &el}
			default:
				continue
			}
			if shapes == nil {
				shapes = map[string]parquet.Column{}
			}
			shapes[strings.ToLower(c.Name)] = sh
		}
		return shapes
	case logical.NodeProject:
		// inputColTypes STOPS at a Project because a rename can bind a name
		// to a different value. The SHAPES walk does not have to: a
		// rename-only projection FORWARDS its columns, and which column each
		// output name forwards is written down right here. Mapping them is
		// what lets a field path through a derived table or a CTE keep its
		// type — `SELECT rw.n FROM (SELECT rw FROM t) s` answered string("9")
		// and `MIN(rw.n)` over the same subquery could not resolve its input
		// at all, because the walk answered nil the moment a Project was in
		// the way (#568) — and an array keep its element (#1303).
		//
		// A computed or aggregate item SHADOWS its own name with whatever it
		// declares itself: past that point the name is bound to a value the
		// shapes below do not describe.
		if len(n.Children) != 1 {
			return nil
		}
		below := inputColShapes(n.Children[0])
		var out map[string]parquet.Column
		for _, p := range n.Projections {
			if p.IsAgg {
				// An aggregate output is a NEW name, and its declaration
				// comes from the aggregate rather than from the columns
				// below. Forwarding `below`'s entry would be the defect this
				// arm's `return nil` was avoiding — `SUM(x) AS c_row` must
				// not inherit c_row's field list — but NIL-ing the whole map
				// also throws away every OTHER name in it, and it left a
				// ROW-valued aggregate with no plan-time declaration at all.
				//
				// So: publish what the aggregate itself declares (a bar's
				// ROW, #965), and SHADOW the name otherwise, which is the
				// precise statement of "the shapes below no longer describe
				// this name".
				name := strings.ToLower(cleanExpr(p.Alias))
				if name == "" {
					name = strings.ToLower(cleanExpr(p.Column))
				}
				if name == "" {
					return nil
				}
				if out == nil {
					out = make(map[string]parquet.Column)
				}
				if f, ok := aggregateProjectionFields(n, p); ok {
					out[name] = parquet.Column{Type: parquet.TypeRow, Fields: f}
				} else if sh, ok := aggOutputShapeBelow(below, p); ok {
					// The aggregate's OWN output shape, which the Aggregate
					// arm below publishes under its output column — a
					// container MIN/MAX's element. SHADOWING it lost the
					// element of every aggregate read through a projection
					// that renames it: a LATERAL body's `MAX(c2.ad) AS a`
					// declared a zero-row `t.a` as text (arc CW round 2, B1).
					out[name] = sh
				} else {
					out[name] = parquet.Column{}
				}
				continue
			}
			if p.Column == "" {
				// A computed item SHADOWS its own name, but leaves other names' shape
				// declarations intact (#965). Clearing the whole map loses the bar's (p,s)
				// when a computed group key sits beside OHLCV; WSHF child metadata retains
				// scale without precision. As with the aggregate arm, refuse the map if
				// this item's name cannot be spelled: a stale unshadowed entry is unsafe.
				name := strings.ToLower(cleanExpr(p.Alias))
				if name == "" {
					name = strings.ToLower(cleanExpr(p.Expr))
				}
				if name == "" {
					return nil
				}
				if out == nil {
					out = make(map[string]parquet.Column)
				}
				d, _ := nodeDeclaredType(p.ASTExpr, ColDecls{Types: inputColTypes(n.Children[0]),
					Fields: shapeFields(below), Elems: shapeElems(below), Dec: inputColDecimal(n.Children[0])})
				out[name] = declShape(d)
				continue
			}
			f, ok := below[strings.ToLower(cleanExpr(p.Column))]
			if !ok {
				continue
			}
			name := p.Alias
			if name == "" {
				name = p.Column
			}
			if out == nil {
				out = make(map[string]parquet.Column)
			}
			out[strings.ToLower(cleanExpr(name))] = f
		}
		return out
	case logical.NodeAggregate:
		// An aggregate publishes its OWN outputs and forwards nothing: every
		// name it emits is either a group KEY (whose shape comes from the
		// column below) or an aggregate OUTPUT. Only the second kind can
		// declare a ROW today — the bar — and it declares it through the one
		// derivation ADR-0035 item 5 names.
		var out map[string]parquet.Column
		var childShapes map[string]parquet.Column
		if len(n.Children) == 1 {
			for i, k := range groupKeyOutputs(n) {
				var ast plansql.Node
				if i < len(n.GroupByExprs) {
					ast = n.GroupByExprs[i]
				}
				if ast == nil {
					ast, _ = plansql.ParseExpression(n.GroupBy[i])
				}
				d := derivedGroupKeyDecl(n.GroupBy[i], ast, n.Children[0])
				sh := declShape(d)
				// A BARE column key forwards the column: derivedGroupKeyDecl
				// withholds a bare reference's declaration (exec.Project types
				// a copy from the column it copies), so a container key's
				// element came from nowhere and `SELECT DISTINCT av` /
				// `GROUP BY av` declared its zero-row array as text (arc CW
				// round 2, B1). The shape below IS the key's shape.
				if sh.Fields == nil && sh.ElementType == nil {
					if cr, ok := plansql.Unparen(ast).(*plansql.ColRef); ok {
						if childShapes == nil {
							childShapes = inputColShapes(n.Children[0])
						}
						// The qualified spelling first, then the bare one — a
						// join's walk drops a bare name its two sides declare
						// differently, so the bare entry is never the other
						// side's shape.
						if c, ok := childShapes[strings.ToLower(cr.Table+"."+cr.Column)]; ok && cr.Table != "" {
							sh = c
						} else if c, ok := childShapes[strings.ToLower(cleanExpr(cr.Column))]; ok {
							sh = c
						}
					}
				}
				if sh.Type != 0 || sh.Fields != nil || sh.ElementType != nil {
					if out == nil {
						out = map[string]parquet.Column{}
					}
					out[strings.ToLower(cleanExpr(k.Name))] = sh
					// And under the spelling the aggregate EMITS the key as
					// (`q.a` over a derived table), which is the key its
					// emitted TYPE is published under — colDecl reads the
					// type and the element from one key.
					out[strings.ToLower(strings.TrimSpace(k.Name))] = sh
				}
			}
		}
		var childDecls *ColDecls
		for i := range n.AggExprs {
			a := n.AggExprs[i]
			if f, ok := aggOhlcvOutputFields(n, a); ok {
				if out == nil {
					out = make(map[string]parquet.Column)
				}
				out[strings.ToLower(cleanExpr(a.OutputCol))] = parquet.Column{Type: parquet.TypeRow, Fields: f}
				continue
			}
			// MIN/MAX (and MIN_BY/MAX_BY) of a CONTAINER answer with an
			// input value (#426), so the output's shape is the input's —
			// the element a subscript over `MIN(ARRAY[x])` is declared from
			// (arc CW).
			switch strings.ToLower(a.Func) {
			case "min", "max", "min_by", "max_by":
			default:
				continue
			}
			if len(n.Children) != 1 {
				continue
			}
			if childDecls == nil {
				d := emittedColDecls(n.Children[0])
				childDecls = &d
			}
			arg := a.InputExpr
			if arg == nil && a.InputCol != "" {
				arg = &plansql.ColRef{Column: a.InputCol}
			}
			if arg == nil {
				continue
			}
			d, c := nodeDeclaredType(arg, *childDecls)
			if c != expr.Decided && a.InputExpr == nil {
				// The qualified spelling of a derived table's column (round 4).
				for _, ref := range aggInputRefs(a.InputCol)[1:] {
					if d, c = nodeDeclaredType(ref, *childDecls); c == expr.Decided {
						break
					}
				}
			}
			if c != expr.Decided {
				continue
			}
			if sh := declShape(d); sh.ElementType != nil || sh.Fields != nil {
				if out == nil {
					out = make(map[string]parquet.Column)
				}
				out[strings.ToLower(cleanExpr(a.OutputCol))] = sh
			}
		}
		return out
	case logical.NodeWindow:
		// A Window forwards its input and ADDS one column per window
		// function. A MIN/MAX (or a value function) over a container answers
		// the argument's value, so its output's shape is the argument's —
		// the declaration a zero-row `MIN(ARRAY[x]) OVER ()` is described
		// from (arc CW round 2, B1).
		if len(n.Children) != 1 {
			return nil
		}
		out := inputColShapes(n.Children[0])
		for _, we := range n.WindowExprs {
			name := strings.ToLower(strings.TrimSpace(we.OutputCol))
			if name == "" {
				continue
			}
			sh := declShape(windowSpecOutputType(n, we))
			if sh.Fields == nil && sh.ElementType == nil {
				delete(out, name)
				continue
			}
			if out == nil {
				out = map[string]parquet.Column{}
			}
			out[name] = sh
		}
		return out
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		if items := lateralDualItemDecls(n); items != nil {
			merged := make(map[string]parquet.Column, len(items))
			for c, f := range inputColShapes(n.Children[0]) {
				merged[c] = f
			}
			for name, d := range items {
				if sh := declShape(d); sh.Fields != nil || sh.ElementType != nil {
					merged[name] = sh
					continue
				}
				delete(merged, name)
			}
			return merged
		}
		left, right := inputColShapes(n.Children[0]), inputColShapes(n.Children[1])
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		merged := make(map[string]parquet.Column, len(left)+len(right))
		for c, f := range left {
			merged[c] = f
		}
		for c, f := range right {
			if prev, dup := merged[c]; dup && !sameShape(prev, f) {
				delete(merged, c)
				continue
			}
			merged[c] = f
		}
		return merged
	}
	return nil
}

// aggOutputShapeBelow is the container shape the Aggregate below publishes
// for the aggregate an IsAgg projection reads, under whichever spelling the
// projection names it by.
func aggOutputShapeBelow(below map[string]parquet.Column, p logical.Projection) (parquet.Column, bool) {
	for _, k := range []string{p.Column, p.Alias, p.Expr} {
		k = strings.ToLower(cleanExpr(k))
		if k == "" {
			continue
		}
		if sh, ok := below[k]; ok && (sh.Fields != nil || sh.ElementType != nil) {
			return sh, true
		}
	}
	return parquet.Column{}, false
}

// sameShape reports whether two container shapes describe one declaration,
// so a join that carries the name on both sides can keep it.
func sameShape(a, b parquet.Column) bool {
	if a.Type != b.Type || !sameRowFields(a.Fields, b.Fields) {
		return false
	}
	if (a.ElementType == nil) != (b.ElementType == nil) {
		return false
	}
	if a.ElementType == nil {
		return true
	}
	return a.ElementType.Type == b.ElementType.Type && sameShape(*a.ElementType, *b.ElementType) &&
		a.ElementType.Precision == b.ElementType.Precision && a.ElementType.Scale == b.ElementType.Scale
}

// sameRowFields reports whether two ROW declarations are the same shape, so a
// join that carries the name on both sides can keep it. parquet.Column holds
// a slice and is not comparable, which is why this is spelled out rather than
// written as ==.
func sameRowFields(a, b []parquet.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i].Name, b[i].Name) || a[i].Type != b[i].Type ||
			a[i].Precision != b[i].Precision || a[i].Scale != b[i].Scale ||
			a[i].Dimension != b[i].Dimension || !sameRowFields(a[i].Fields, b[i].Fields) {
			return false
		}
	}
	return true
}

// inputColDecls is the pair of walks above taken together: what a node's
// output columns declare, ready to hand to NodeDeclaredType. Callers that
// hold the logical node an expression reads should build the context here
// rather than passing inputColTypes alone, which cannot type a field path.
func inputColDecls(n *logical.Node) ColDecls {
	shapes := inputColShapes(n)
	return ColDecls{Types: inputColTypes(n), Fields: shapeFields(shapes), Elems: shapeElems(shapes), Dec: inputColDecimal(n)}
}

// windowOutputColTypes adds a Window node's own output SLOTS to the types its
// input carries. Shared by the two walks so the flat type and its (p,s) can
// never come from different rules.
func windowOutputColTypes(n *logical.Node, in map[string]parquet.TypeID) map[string]parquet.TypeID {
	var out map[string]parquet.TypeID
	for _, we := range n.WindowExprs {
		name := strings.ToLower(strings.TrimSpace(we.OutputCol))
		if name == "" {
			continue
		}
		if out == nil {
			out = make(map[string]parquet.TypeID, len(in)+len(n.WindowExprs))
			for k, v := range in {
				out[k] = v
			}
		}
		out[name] = windowSpecOutputType(n, we).ID
	}
	if out == nil {
		return in
	}
	return out
}

// windowOutputColDecimal is windowOutputColTypes' (p,s) companion: a slot whose
// declaration is a DECIMAL with a KNOWN scale carries it, and every other slot
// contributes nothing, exactly as emittedColDecimal has it.
func windowOutputColDecimal(n *logical.Node, in map[string]logical.DecimalMeta) map[string]logical.DecimalMeta {
	var out map[string]logical.DecimalMeta
	for _, we := range n.WindowExprs {
		name := strings.ToLower(strings.TrimSpace(we.OutputCol))
		if name == "" {
			continue
		}
		d := windowSpecOutputType(n, we)
		if d.ID != parquet.TypeDecimal || !d.DecKnown {
			continue
		}
		if out == nil {
			out = make(map[string]logical.DecimalMeta, len(in)+len(n.WindowExprs))
			for k, v := range in {
				out[k] = v
			}
		}
		out[name] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
	}
	if out == nil {
		return in
	}
	return out
}

// inputColDecimal is inputColTypes' companion for DECIMAL precision/scale
// (#458): the same walk, sourced from ScanColDecimal instead of
// ScanColTypes, and holding only entries a DECIMAL column has. A name two
// scans disagree on (different (p,s), same as a type disagreement above) is
// dropped rather than picking a side.
func inputColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		return n.ScanColDecimal
	case logical.NodeDual:
		return inputColDecimal(n.LateralOuterScope)
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColDecimal(n.Children[0])
	case logical.NodeWindow:
		// The (p,s) half of the window arm in inputColTypes, and it has to
		// move with it: a slot typed DECIMAL there and carrying no scale here
		// declares a DECIMAL with no scale, which reads every value back at
		// 10^0 (ADR-0024 item 2). See emittedColDecimal's NodeWindow arm,
		// which answers the same question for the emitted schema.
		if len(n.Children) != 1 {
			return nil
		}
		return windowOutputColDecimal(n, inputColDecimal(n.Children[0]))
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		if items := lateralDualItemDecls(n); items != nil {
			merged := make(map[string]logical.DecimalMeta, len(items))
			for c, m := range inputColDecimal(n.Children[0]) {
				merged[c] = m
			}
			for name, d := range items {
				if d.ID != parquet.TypeDecimal || !d.DecKnown {
					delete(merged, name)
					continue
				}
				merged[name] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
			}
			return merged
		}
		left, right := inputColDecimal(n.Children[0]), inputColDecimal(n.Children[1])
		if left == nil || right == nil {
			return nil
		}
		merged := make(map[string]logical.DecimalMeta, len(left)+len(right))
		for c, m := range left {
			merged[c] = m
		}
		for c, m := range right {
			if prev, dup := merged[c]; dup && prev != m {
				delete(merged, c)
				continue
			}
			merged[c] = m
		}
		return merged
	}
	return nil
}

// sourceColTypesThroughRenames is inputColTypes for an expression whose
// references were substituted through nested rename-only Projects (#387):
// after substitution the expression names only SOURCE columns, so the types
// visible BELOW the rename chain are the right ones — a plain rename rebinds
// names, not values. The walk descends only Projects that are pure
// column-forwarders (every item a plain column reference); a computed or
// aggregate item stops it with nil, because past that point a name may be
// rebound to a different value and inputColTypes' own warning applies.
// sourceColDeclsThroughRenames is sourceColTypesThroughRenames paired with
// the ROW fields visible at the same point, so an expression rewritten
// through a rename chain can still type a field path (#568).
func sourceColDeclsThroughRenames(n *logical.Node) ColDecls {
	for n != nil && n.Type == logical.NodeProject && len(n.Children) == 1 {
		for _, p := range n.Projections {
			if p.IsAgg || p.Column == "" {
				return ColDecls{}
			}
		}
		n = n.Children[0]
	}
	return inputColDecls(n)
}

func sourceColTypesThroughRenames(n *logical.Node) map[string]parquet.TypeID {
	for n != nil && n.Type == logical.NodeProject && len(n.Children) == 1 {
		for _, p := range n.Projections {
			if p.IsAgg || p.Column == "" {
				return nil
			}
		}
		n = n.Children[0]
	}
	return inputColTypes(n)
}

// strictIntArithColsThroughRenames mirrors sourceColTypesThroughRenames for
// the integer-preserving-arithmetic hint (#297): an expression rewritten by
// substituteNestedRenameRefs names only SOURCE columns, so the strict-int set
// visible BELOW the rename chain is the one to check the rewritten expression
// against (#445) — a plain rename forwards the exact int column, it does not
// rebind it to a different value.
func strictIntArithColsThroughRenames(n *logical.Node) map[string]bool {
	for n != nil && n.Type == logical.NodeProject && len(n.Children) == 1 {
		for _, p := range n.Projections {
			if p.IsAgg || p.Column == "" {
				return nil
			}
		}
		n = n.Children[0]
	}
	return strictIntArithCols(n)
}

// ColDecls is what a node's output columns declare, as far as the planner can
// know it: the flat catalog types (inputColTypes) plus, for the ROW columns
// among them, the FIELDS a field path can name (inputColFields).
//
// The second map exists because the first cannot answer the question. It is
// keyed by column name, and the `c` in `rw.c` is not a column of anything —
// so every lookup missed, the projection kept its STRING default, and a ROW
// field path was declared STRING whatever its real type (#568).
type ColDecls struct {
	Types  map[string]parquet.TypeID
	Fields map[string][]parquet.Column
	// Elems carries the whole declared column of the ARRAY and MAP entries in
	// types — the element a bare TypeID cannot say (arc CW). Without it a
	// column reference to a container declined, and a derived table, a CTE, a
	// set operation or a zero-row result over one declared the STRING
	// fallback (#1133, #1303). Filled from the same walk as Fields
	// (inputColShapes), so the two can never describe different columns.
	Elems map[string]parquet.Column
	// Dec carries the (precision, scale) of the DECIMAL entries in types.
	// A bare TypeID is not a type for a DECIMAL — a projection declared
	// DECIMAL without its scale builds an output vector that reads every
	// value back at the wrong power of ten — which is why colRefDeclaredType
	// used to decline the type outright (ADR-0024 item 2, #529/#555/#587).
	Dec map[string]logical.DecimalMeta
	// intWidth carries PostgreSQL's INTEGER WIDTH of the integer entries in
	// types — the fact that decides what SUM over the column declares, and
	// the one thing the carrier cannot say.
	//
	// It exists for the same reason dec does, one type family over. Every
	// integer computes and materializes in an int64 (ADR-0024's recorded
	// widening), so `BITWISE_AND(id, 3)` published by a derived table is an
	// INT64 column whose PostgreSQL type is integer — and the reader that
	// had only the carrier declared `SUM(v)` numeric where PostgreSQL
	// declares bigint, on every arm and both wire formats, while the DIRECT
	// call one level down declared it right (#1018 round 5, B1).
	//
	// An absent entry is "this declaration says nothing", not int4: the
	// reader then falls back to the carrier, which for a base column IS the
	// catalog's storage width.
	intWidth map[string]intWidth
	// subqueryDecl resolves a SCALAR SUBQUERY's single declared output
	// column, and nil means "this caller cannot ask" — which is what every
	// construction site that has no Planner leaves it at, and what
	// NodeDeclaredType's SubqueryNode arm declines on.
	//
	// It exists because a subquery is a WHOLE SECOND QUERY whose type lives
	// in the CATALOG, not in the enclosing query's columns: `SELECT id,
	// (SELECT MAX(c_i64) FROM typemx) AS mx` has nothing in `types` that
	// describes `mx`. Without it the projection fell to its STRING fallback
	// and a bigint came back as a Go string on every arm and every door,
	// where PostgreSQL declares int8 (#874) — and the const-arith fold saw an
	// Undecided operand and folded `+ 1` on the FLOAT rung (#714's third box).
	subqueryDecl func(sql string) (parquet.Column, bool)
	// subqueryIntWidth is subqueryDecl's WIDTH half, for the same reason
	// intWidth exists beside types: the subquery's declared column comes back
	// in the INT64 carrier every integer materializes in, which cannot say
	// whether SUM over it is bigint or numeric. Set together with
	// subqueryDecl (withSubqueryDecls) so the two describe one column.
	subqueryIntWidth func(sql string) (intWidth, bool)
	// PlaceholderTypes is the declared type of each `:scalar_N` deferred
	// literal, keyed by the placeholder's NAME.
	//
	// It is a map of its own rather than an entry in `types` because a
	// placeholder is not a column and must not be resolvable as one: a user
	// column called `scalar_1` would otherwise answer for it. The DAG's
	// SELECT-list lowering fills it from each producer's own plan, which is
	// the only thing that knows what the value will be (#874).
	PlaceholderTypes map[string]parquet.TypeID
}

// colType resolves a column reference to its declared type, mirroring the
// RUNTIME resolver (expr.ColRef.resolveSlow) step for step so a declaration
// never describes a different column than the one the operator will read:
// the full dotted spelling names a column of its own first, then the bare
// name, and only then is the qualifier read as a ROW column and the name as
// its field.
//
// The order is load-bearing in both directions. A delimited identifier that
// contains a dot ("id.orig_h", a flat Zeek JSON column) is ONE name the
// parser keeps whole in Column, so the bare lookup is what finds it; and a
// table alias that happens to match a ROW column must not turn a real column
// reference into a field path.
func (d ColDecls) colType(n *plansql.ColRef) (parquet.TypeID, bool) {
	c, ok := d.colDecl(n)
	if !ok {
		return 0, false
	}
	return c.Type, true
}

// colDecl is colType with the parameterized part of the declaration kept: a
// DECIMAL's (precision, scale). It resolves in exactly the order colType
// documents above, and reads the (p,s) out of the SAME key that answered the
// type, so the two halves can never describe different columns.
func (d ColDecls) colDecl(n *plansql.ColRef) (parquet.Column, bool) {
	at := func(key string) (parquet.Column, bool) {
		t, ok := d.Types[key]
		if !ok {
			return parquet.Column{}, false
		}
		col := parquet.Column{Name: key, Type: t, Fields: d.Fields[key]}
		e, ok := d.Elems[key]
		if !ok {
			// A QUALIFIED key (`c2.ad` over an aliased scan) whose type the
			// map carries under the qualifier while the shape walk keys the
			// column bare: the bare entry is this column's element, because
			// a join's shape walk drops a bare name its sides declare
			// differently (arc CW round 2, B1 — a decorrelated LATERAL's
			// `MAX(c2.ad)` declared no element).
			if dot := strings.LastIndexByte(key, '.'); dot >= 0 {
				e, ok = d.Elems[key[dot+1:]]
			}
		}
		if ok && e.Type == t && e.ElementType != nil {
			el := e.ElementType.Clone()
			col.ElementType = &el
		}
		if t == parquet.TypeDecimal {
			if m, ok := lookupColDecimal(d.Dec, key); ok {
				col.Precision, col.Scale = m.Precision, m.Scale
			}
		}
		return col, true
	}
	if n.Table != "" {
		if c, ok := at(strings.ToLower(n.Table + "." + n.Column)); ok {
			return c, true
		}
	}
	// A ROW FIELD PATH, asked BEFORE the qualifier is stripped, because the
	// runtime asks it there now (ADR-0022 rule 1: a declaration resolved in a
	// different order than the value describes a different column).
	//
	// `SELECT n.id, c_row.b FROM typemx_nested n JOIN decpair d ON n.id = d.id`
	// took the join arm's DECIMAL(18,4) `b` as the declaration for an INT64
	// field, so even after the value came back right the column described
	// itself — and declared itself on the WIRE — as somebody else's type
	// (#769).
	if c, ok := d.field(n); ok {
		return c, true
	}
	if c, ok := at(strings.ToLower(n.Column)); ok {
		return c, true
	}
	return parquet.Column{}, false
}

// colIntWidth resolves a column reference to the PostgreSQL INTEGER WIDTH its
// declaration carries, in exactly the order colDecl resolves the type — so the
// width and the type can never describe two different columns, which is the
// rule ADR-0022 item 1 states for every half of a declaration.
//
// ok=false means the declaration is silent, and the caller falls back to the
// carrier. A ROW FIELD is deliberately not answered here: a field's width is
// its own declared type's, which ColDecls.field already carries.
func (d ColDecls) colIntWidth(n *plansql.ColRef) (intWidth, bool) {
	if n == nil || len(d.intWidth) == 0 {
		return intWidthUnknown, false
	}
	if n.Table != "" {
		if w, ok := d.intWidth[strings.ToLower(n.Table+"."+n.Column)]; ok {
			return w, true
		}
	}
	if d.isFieldPath(n) {
		return intWidthUnknown, false
	}
	w, ok := d.intWidth[strings.ToLower(n.Column)]
	return w, ok
}

// field resolves n as a ROW field path and returns the field's full
// declaration — its type, and for a parameterized field the (p,s), dimension
// or nested shape that a bare TypeID cannot carry.
func (d ColDecls) field(n *plansql.ColRef) (parquet.Column, bool) {
	if n == nil || n.Table == "" || d.Fields == nil {
		return parquet.Column{}, false
	}
	fields, ok := d.Fields[strings.ToLower(n.Table)]
	if !ok {
		return parquet.Column{}, false
	}
	parent := parquet.Column{Type: parquet.TypeRow, Fields: fields}
	return parent.Field(n.Column)
}

// isFieldPath reports whether n names a ROW FIELD rather than a column of the
// input. It is the test every "is this a bare column reference?" predicate in
// the planner needs, because a field path is NOT one: nothing downstream
// resolves it by name — exec.columnIndexFallback has no ROW arm, and neither
// does the hash aggregate that calls it — so a field path has to be
// MATERIALIZED the way a computed expression is, not passed through as a name.
//
// It answers false whenever the reference resolves as a column first, in the
// same order colType uses, so a real qualified reference is never mistaken
// for one.
func (d ColDecls) isFieldPath(n *plansql.ColRef) bool {
	if n == nil || n.Table == "" {
		return false
	}
	// A column of the whole dotted spelling — a flat Zeek `id.orig_h` — is
	// that column and not a path into anything.
	if _, ok := d.Types[strings.ToLower(n.Table+"."+n.Column)]; ok {
		return false
	}
	// The BARE decline is gone, in the same order colDecl now uses: a join arm
	// publishing a column of the FIELD's name does not stop `c_row.b` being a
	// field path, and asking the bare name first is what made it stop (#769).
	_, ok := d.field(n)
	return ok
}

// astIsFieldPath is isFieldPath over an expression node, seeing through
// parentheses the way isComputedProjection does.
func astIsFieldPath(node plansql.Node, decls ColDecls) bool {
	_, ok := fieldPathRef(node, decls)
	return ok
}

// fieldPathRef returns the reference behind a ROW field path, and its
// undropped `parent.field` spelling. CleanExpr strips the qualifier from
// every column reference — right for a table alias, and the reason a field
// path arrives downstream as a bare field name no column carries — so this is
// the one place the whole path survives.
func fieldPathRef(node plansql.Node, decls ColDecls) (string, bool) {
	for {
		switch n := node.(type) {
		case *plansql.ColRef:
			if !decls.isFieldPath(n) {
				return "", false
			}
			return n.Table + "." + n.Column, true
		case *plansql.ParenNode:
			node = n.Inner
		default:
			return "", false
		}
	}
}

// colRefDeclaredType resolves a column reference against the declarations of
// its input (InputColDecls). Undecided — today's answer, and the caller's
// fallback with it — for a name no scan carries, a name two scans disagree
// on, and anything that is not a scan column at all: an aggregate output, a
// synthetic sort or group key. #331's machinery propagates a decision as
// fact, so a wrong confident answer here is worse than the guess it replaces.
//
// A ROW FIELD PATH resolves here too, on exactly the same terms as a column
// (ColDecls.colType). Before #568 it could not: the lookup was keyed by
// column name and `c` is not a column of `t(id, c_flat, rw)`, so `rw.c`
// answered Undecided and the caller's STRING fallback stood — which is how
// `SELECT rw.n` over an INT64 field returned string("9") and `ORDER BY rw.c`
// sorted a CIDR field by its stored text while `ORDER BY rw` over the same
// values sorted by inet.
func colRefDeclaredType(n *plansql.ColRef, decls ColDecls) (expr.DeclType, expr.Confidence) {
	c, ok := decls.colDecl(n)
	if !ok {
		return expr.DeclType{}, expr.Undecided
	}
	switch c.Type {
	case parquet.TypeDecimal:
		// A DECIMAL column reference DECIDES, and it decides its own (p,s)
		// — the widening ADR-0024 item 2 is built on. It used to decline
		// with the other parameterized types below, and that single decline
		// is why GREATEST/LEAST over a DECIMAL could not run at all (#529),
		// why COALESCE over one declared FLOAT64 (#555), and why a windowed
		// MIN over one described itself float8 on a zero-row result (#587):
		// every downstream rule fell to its non-DECIMAL default because
		// nothing below it ever said "decimal".
		//
		// Precision 0 is the "unconstrained" sentinel a computed DECIMAL
		// carries (#458) and is NOT a declaration: taken at face value it
		// would build an output vector at scale 0 and read every value back
		// a hundredfold out. That case still declines.
		if c.Precision <= 0 {
			return expr.DeclType{}, expr.Undecided
		}
		return expr.DeclDecimal(c.Precision, c.Scale), expr.Decided
	case parquet.TypeRow:
		if len(c.Fields) > 0 {
			return expr.DeclType{ID: c.Type, Schema: &c}, expr.Decided
		}
		return expr.DeclType{}, expr.Undecided
	case parquet.TypeArray, parquet.TypeMap:
		// A container DECIDES with its element, which ColDecls.Elems carries
		// (arc CW): the output vector is sized from it and the wire declares
		// the element's array OID. Without one — a container whose element no
		// walk could say — it declines like VECTOR below.
		if c.ElementType != nil {
			return expr.DeclType{ID: c.Type, Schema: &c}, expr.Decided
		}
		return expr.DeclType{}, expr.Undecided
	case parquet.TypeVector:
		// The other parameterized type: the catalog map carries the TypeID
		// and nothing else, and a projection declared VECTOR without its
		// dimension builds an output vector that reads back wrong.
		//
		// A field path of one of these types declines too, and for the same
		// reason — exec.Project repairs it from the input batch, where the
		// parent ROW vector's child carries the whole shape.
		return expr.DeclType{}, expr.Undecided
	}
	return expr.Decl(c.Type), expr.Decided
}

// declTypeParts carries the complete allocation declaration across every
// materialization boundary, including a fixed ROW's child fields and an
// ARRAY's or MAP's element (arc CW): every site that materializes a computed
// value — a projection, a sort, group or window key, an aggregate's input, a
// set-operation arm, a DAG stage's spec — allocates its vector from these
// parts, and a container vector allocated without its element has no child to
// hold one.
func declTypeParts(d expr.DeclType) parquet.Column {
	c := parquet.Column{Type: d.ID, Precision: d.Precision, Scale: d.Scale, Fields: d.RowFields()}
	if (d.ID == parquet.TypeArray || d.ID == parquet.TypeMap) && d.Schema != nil && d.Schema.ElementType != nil {
		el := d.Schema.ElementType.Clone()
		c.ElementType = &el
	}
	if d.ID == parquet.TypeVector && d.Schema != nil {
		c.Dimension = d.Schema.Dimension
	}
	return c
}

// emittedColDecls is InputColDecls over what a node EMITS rather than what it
// passes through: it descends INTO a Project, an Aggregate and a Window
// instead of stopping at them, which is the difference between seeing a
// DERIVED TABLE's columns and seeing nothing.
//
// `SELECT GREATEST(v, b) FROM (SELECT a AS v, b FROM t) x` is the shape:
// inputColTypes stops at the derived table's Project, so neither argument
// decided a type, GREATEST fell to its FLOAT64 fallback and the query failed
// with "cannot store string into FLOAT64 vector" — #529 unclosed for every
// query that names its DECIMAL through a subquery. It is the same walk
// declaredOutputSchema already resolves the OUTPUT projection against, so the
// SELECT list and the plan-declared schema now answer from one map.
func emittedColDecls(n *logical.Node) ColDecls {
	shapes := inputColShapes(n)
	return ColDecls{
		Types:    emittedColTypes(n),
		Fields:   shapeFields(shapes),
		Elems:    shapeElems(shapes),
		Dec:      emittedColDecimal(n),
		intWidth: emittedColIntWidth(n),
	}
}

// inferProjectionType infers the output parquet type from an AST expression
// node with nothing known about its input, returning the fallback when
// inference isn't possible.
func inferProjectionType(node plansql.Node, fallback parquet.TypeID) parquet.TypeID {
	return inferProjectionTypeCols(node, fallback, nil, nil)
}

// DeclaredTypeOfNode resolves an expression's declared type against table
// columns, using the query path's inference. DML must use the declaration,
// not the float64 box shared by float8 and numeric: assignment rounds float8
// half to EVEN and numeric half AWAY FROM ZERO (#699).
// nodeDeclaredType leaves missing column declarations undecided (#333), except
// under a UNARY SIGN, which declares float8 whatever its operand decided.
// Nested function callers must keep looking past a guessed type for a
// decided candidate (expr.Confidence, #331).
func DeclaredTypeOfNode(node plansql.Node, schema []parquet.Column) (expr.DeclType, expr.Confidence) {
	decls := ColDecls{
		Types:  make(map[string]parquet.TypeID, len(schema)),
		Fields: map[string][]parquet.Column{},
		Dec:    map[string]logical.DecimalMeta{},
	}
	for _, c := range schema {
		name := strings.ToLower(c.Name)
		decls.Types[name] = c.Type
		if c.Type == parquet.TypeDecimal {
			decls.Dec[name] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
		}
		if len(c.Fields) > 0 {
			decls.Fields[name] = c.Fields
		}
	}
	return nodeDeclaredType(node, decls)
}

func nodeDeclaredType(node plansql.Node, decls ColDecls) (expr.DeclType, expr.Confidence) {
	switch n := node.(type) {
	case *plansql.ColRef:
		return colRefDeclaredType(n, decls)
	case *plansql.BinaryOp:
		if n.Op == "||" {
			// `bytea || bytea` is BYTEA on the server, and an unknown-typed
			// literal beside one takes bytea too — so the result declares
			// OID 17 and its bytes go out as \x hex rather than raw under
			// text's 25 (#583).
			//
			// `text || bytea` is TEXT there, and that is not a detail: the
			// server resolves that pair through `text || anynonarray`, which
			// renders the bytea and concatenates as text —
			// `'AB'::text || '\x6869'::bytea` is `AB\x6869` with pg_typeof
			// text, measured on 17.11. Declaring bytea for it moved a RIGHT
			// declared class to a wrong one (round 2, B3). So bytea only when
			// no operand is DECLARED text; an unknown literal declares
			// nothing and is the case that takes bytea.
			if bytesOperand(n.Left, decls) || bytesOperand(n.Right, decls) {
				if !stringOperand(n.Left, decls) && !stringOperand(n.Right, decls) {
					return expr.Decl(parquet.TypeBytes), expr.Decided
				}
			}
			// String concatenation, not arithmetic. Declaring it Float64
			// handed the concat kernel an output vector with no BytesData,
			// so every row came back NULL (#328).
			return expr.Decl(parquet.TypeString), expr.Decided
		}
		if t, c := binOpTemporalType(n, decls); c != expr.Undecided {
			return t, c
		}
		if !binOpInvolvesTemporal(n, decls) {
			// ADR-0024 item 3: a DECIMAL operand makes this DECIMAL, at the
			// (p,s) batch.DecimalResultType names, and expr.BinOpNumeric's
			// decimal mode computes it exactly on the Int128 carrier (#555).
			// binOpDecimalType is that mode's AST mirror and must stay a
			// strict subset of it — see decimal_arith_type.go.
			if t, ok := binOpDecimalType(n, decls); ok {
				return t, expr.Decided
			}
			// Integer arithmetic over integer operands must declare INTEGER even when
			// nested in CASE, COALESCE or an aggregate (#369, #724; ADR-0024 item 2).
			// Use the projection's intArithAllInt predicate, a strict subset of runtime
			// integer mode, so a declaration never promises what the kernel cannot emit.
			// Require IntArithOn: WADJET_INT_ARITH=0 uses the float delegate.
			if expr.IntArithOn() && intArithAllInt(n, nil, decls) {
				// INT64, not the operands' own width. #1070 narrowed this to
				// int4 and the DAG did not follow for an expression over an
				// AGGREGATE or WINDOW slot: `MAX(c_i32) + 0` came back
				// integer on the single path and bigint on both stage arms,
				// which is the two-path divergence #813 item 1 is. Measured
				// and filed; the LITERAL half of #1070 shipped because it
				// agrees on every arm.
				return expr.Decl(parquet.TypeInt64), expr.Decided
			}
			if realArithBothReal(n, decls) {
				return expr.Decl(parquet.TypeFloat32), expr.Decided
			}
			return expr.Decl(parquet.TypeFloat64), expr.Decided
		}
	case *plansql.UnaryOp:
		// Unary ± preserves its operand's numeric type (expr.UnaryOp.Eval
		// negates int64 as int64 since #369). Declaring it — instead of the
		// String fallback — is what lets `ORDER BY -col` sort numerically:
		// the hidden key materializes into a typed vector rather than into
		// text, where "-0" vs "0" rendering used to decide the order.
		if n.Op == "-" || n.Op == "+" {
			t, c := nodeDeclaredType(n.Inner, decls)
			if c != expr.Undecided {
				// A negated numeric LITERAL keeps the exact fixed-point
				// contribution its spelling carries: negation moves no digit,
				// so `CASE … THEN d ELSE -0.5 END` folds to the same DECIMAL
				// `… ELSE 0.5 END` does (#695).
				withExact := func(d expr.DeclType) expr.DeclType {
					d.Exact, d.ExactSet = t.Exact, t.ExactSet
					return d
				}
				switch t.ID {
				case parquet.TypeInt64, parquet.TypeInt32,
					parquet.TypePort, parquet.TypeProtocol:
					// PORT and PROTOCOL negate as integers, the same set
					// intArithColumnType names: `-c_port` is an INTEGER on
					// both engines and the kernel produces one (#1000).
					//
					return withExact(expr.Decl(parquet.TypeInt64)), c
				case parquet.TypeFloat64:
					return withExact(expr.Decl(parquet.TypeFloat64)), c
				case parquet.TypeFloat32:
					// `-real` is real on the server, and negation moves no
					// digit, so the narrower declaration holds every value the
					// operand did (#1117).
					return withExact(expr.Decl(parquet.TypeFloat32)), c
				case parquet.TypeDecimal:
					// Negation moves no digit, so -d is a value the same
					// column holds and keeps its exact (p,s) — which is what
					// makes `SELECT -d` a numeric column rather than the
					// STRING the fallback used to declare, and what lets
					// `-d * 2` stay on the exact path (ADR-0024 item 2).
					// Only a DECIMAL whose (p,s) resolved: an unconstrained
					// one has no vector to allocate (#458).
					if t.DecKnown && decimalArithOperandDecided(n.Inner, decls) {
						return t, c
					}
				}
			}
			// AN UNDECIDED OPERAND IS STILL A NUMBER UNDER A UNARY SIGN.
			// The String fallback below is what `ORDER BY -a` over a relation
			// with no plan-time column types — a file or database reader —
			// used to take: the hidden sort key materialized as TEXT and the
			// rows came back in the order "-1" < "-2" < "-3" gives, which is
			// ASCENDING by `a` where PostgreSQL sorts descending. A wrong
			// ORDER, silently (round-1 review, N10).
			//
			// Float64 is the same declaration a BINARY arithmetic node over
			// the same undecided operand already takes, which is why
			// `ORDER BY 0 - a` and `ORDER BY a * -1` were right over the same
			// relation while `ORDER BY -a` was not. One rule for the sign,
			// whichever way it is written.
			return expr.Decl(parquet.TypeFloat64), expr.Decided
		}
	case *plansql.FuncCallNode:
		return funcReturnType(n, decls)
	case *plansql.ParenNode:
		return nodeDeclaredType(n.Inner, decls)
	case *plansql.CaseNode:
		return caseDeclaredType(n, decls)
	case *plansql.LiteralPlaceholder:
		// A DEFERRED LITERAL declares what its PRODUCER will produce (#874).
		// The DAG's SELECT-list lowering replaces a scalar subquery with this
		// node and then types the item; without the declaration the item fell
		// to the STRING fallback — and `(:scalar_1) + 1` folded on the FLOAT
		// rung — while the single-process path answered the subquery's own
		// type. Two paths disagreeing about a column's type is the one thing
		// the lowering is not allowed to do.
		if t, ok := decls.PlaceholderTypes[n.Name]; ok {
			return expr.Decl(t), expr.Decided
		}
		return expr.DeclType{}, expr.Undecided
	case *plansql.SubqueryNode:
		// A SCALAR SUBQUERY DECLARES THE TYPE OF ITS OWN OUTPUT COLUMN
		// (#874). The declaration comes from the subquery's own plan, which
		// is what subqueryOutputColumn already resolves for the BOXED
		// COMPARISON (#696) — the same answer, asked one layer earlier so
		// the projection allocates the right output vector instead of its
		// STRING fallback.
		//
		// Undecided when nothing can ask (a caller with no Planner) or when
		// the subquery does not resolve to exactly ONE column, which is the
		// honest answer: a wrong declaration here builds an output vector
		// that reads every value back wrong, and that is worse than the
		// fallback (ADR-0012 item 8).
		if decls.subqueryDecl == nil {
			return expr.DeclType{}, expr.Undecided
		}
		col, ok := decls.subqueryDecl(n.SQL)
		if !ok {
			return expr.DeclType{}, expr.Undecided
		}
		if n.Array {
			// ARRAY(subquery) is an array OF the subquery's column, and it
			// declares that element — bigint[] over a bigint column — so the
			// operator builds an array vector and the wire declares the
			// array type (arc PC round 2, B4).
			elem := col
			elem.Name = "element"
			elem.Nullable = true
			arr := parquet.Column{Name: col.Name, Type: parquet.TypeArray, Nullable: true, ElementType: &elem}
			return expr.DeclType{ID: parquet.TypeArray, Schema: &arr}, expr.Decided
		}
		if col.Type == parquet.TypeDecimal {
			// (p,s) or nothing: a DECIMAL declared without its scale builds a
			// vector that reads every value at the wrong power of ten.
			if col.Precision == 0 {
				return expr.DeclType{}, expr.Undecided
			}
			return expr.DeclDecimal(col.Precision, col.Scale), expr.Decided
		}
		return expr.DeclType{ID: col.Type, Schema: &col}, expr.Decided
	case *plansql.CmpExpr, *plansql.AndNode, *plansql.OrNode, *plansql.NotNode,
		*plansql.IsExpr, *plansql.LikeExpr, *plansql.BetweenExpr,
		*plansql.InExpr, *plansql.ExistsNode, *plansql.AnyAllExpr:
		// Predicates are boolean whatever their operands. Before #371 none
		// of these decided anything, so an aggregate over one — the
		// pre-aggregate projection has no runtime re-typing, unlike
		// exec.Project — fell back to Float64, the comparison kernel's
		// boolean writes were dropped, and BOOL_AND/BOOL_OR read 0 (false)
		// on every row.
		return expr.Decl(parquet.TypeBool), expr.Decided
	case *plansql.ArrayLitNode:
		return arrayLitDeclaredType(n, decls)
	case *plansql.CastNode:
		// A DECIMAL destination carries its own (p,s), and a BARE one takes
		// the operand's — neither of which a plain TypeID can express, which
		// is why `CAST(x AS DECIMAL(10,2))` used to declare STRING and
		// `CAST(x AS DECIMAL)` FLOAT64 (ADR-0024 item 3, #555).
		if t, ok := castDeclaredDecimal(n, decls); ok {
			return t, expr.Decided
		}
		// An ARRAY destination (`x::int[]`, CAST(x AS ARRAY(T))) declares the
		// array OF the element the same spelling declares as a scalar — so
		// `int[]` is bigint[] exactly as `CAST(x AS INT)` is bigint (ADR-0012
		// item 12) — and the evaluator converts each element to it.
		if el, ok := expr.ArrayCastElement(n.TypeName); ok {
			d, ok := arrayCastDecimalElement(n, el, decls)
			if !ok {
				d = expr.Decl(inferCastType(el))
			}
			// A multi-dimensional operand keeps its dimensions: the cast
			// converts its LEAVES (expr.castToArray), so the declaration is
			// the leaf type nested as deep as the operand (round 4, B2).
			for i := arrayCastExtraDims(n, decls); i > 0; i-- {
				inner, c := arrayOfDecl(d)
				if c != expr.Decided {
					return expr.DeclType{}, expr.Undecided
				}
				d = inner
			}
			return arrayOfDecl(d)
		}
		// A VECTOR destination declares a VECTOR of its dimension — the
		// evaluator converts (pgvector's array_to_vector), and the projection
		// sizes its output from the declared dimension (arc CW round 2).
		if _, _, ok := expr.VectorCastDim(n.TypeName); ok {
			col := parquet.Column{Type: parquet.TypeVector, Nullable: true, Dimension: castVectorDim(n)}
			return expr.DeclType{ID: parquet.TypeVector, Schema: &col}, expr.Decided
		}
		return expr.Decl(inferCastType(n.TypeName)), expr.Decided
	case *plansql.Lit:
		// Literal projections (e.g., SELECT 13, SELECT 'x') need a typed
		// output column so the runtime stores the value in the matching
		// typed slice instead of falling back to String. Without this,
		// `... IN (SELECT 13)` returns the literal as "13" and the IN
		// hash lookup against an int column fails to match.
		switch n.Kind {
		case plansql.LitNumber:
			// An integer literal declares INT32 or INT64 on its own, a
			// fractional one its spelling's DECIMAL (below; ADR-0024's
			// 2026-09-24 amendment) — and each CARRIES the exact fixed-point
			// (p,s) of its spelling beside it,
			// which is what a DECIMAL fold over it resolves against (#695).
			// `CASE … THEN d ELSE 0 END` is numeric in PostgreSQL and the
			// literal's `0` is DECIMAL(1,0) in that fold; a FLOAT COLUMN
			// beside the same DECIMAL carries no such (p,s) and keeps
			// PostgreSQL's float8.
			if v, err := strconv.ParseInt(n.Value, 10, 64); err == nil {
				// PostgreSQL's own literal rule, which declaredIntWidth
				// already reads for the aggregate question: an integer
				// literal is `integer` unless it does not fit, and then it is
				// `bigint`. Declaring int8 for every one of them put
				// `SELECT 1` on the wire under OID 20 where the server says
				// 23, and carried that width into every fold and aggregate
				// above it (#1070).
				if v >= math.MinInt32 && v <= math.MaxInt32 {
					return expr.DeclNumericLit(parquet.TypeInt32, n.Value), expr.Decided
				}
				return expr.DeclNumericLit(parquet.TypeInt64, n.Value), expr.Decided
			}
			// A fractional or exponent literal is PostgreSQL's `numeric`:
			// it declares the DECIMAL(p,s) of its spelling wherever it sits —
			// a bare projection, a CASE / COALESCE / GREATEST arm, a derived
			// table's or CTE's column, a VALUES list, a set-operation arm —
			// so `SELECT 2.50` is 2.50 (OID 1700) and so is the value it
			// assigns to a text column through any of them (arc VL round 5;
			// round-4 review B2: `CASE WHEN true THEN 2.50 END` stored `2.5`).
			// It closes ADR-0024's recorded literal deferral. A spelling past
			// what the DECIMAL carrier holds exactly (DeclNumericLit sets no
			// Exact) keeps FLOAT64: the box has already lost digits, and a
			// DECIMAL declaration would present the rounded double as exact.
			d := expr.DeclNumericLit(parquet.TypeFloat64, n.Value)
			if d.ExactSet {
				d.ID, d.Precision, d.Scale, d.DecKnown = parquet.TypeDecimal, d.Exact.Precision, d.Exact.Scale, true
			}
			return d, expr.Decided
		case plansql.LitBool:
			return expr.Decl(parquet.TypeBool), expr.Decided
		case plansql.LitString:
			// SQL's `unknown` (#724). PostgreSQL types a quoted literal from
			// the OTHER operands and coerces it to what they resolve to, so
			// it is DECIDED — `SELECT 'x'` is a text column, and so is a
			// composite whose every argument is quoted — while contributing
			// no rung of its own to a polymorphic fold. Calling it a plain
			// DECIDED string put a non-numeric decider in every call that
			// held one, expr.CommonDeclType could not fold, and the call fell
			// back to its FIRST argument: `GREATEST(bigint, real, double,
			// '1e39')` is double precision in PostgreSQL and was int64's
			// MINIMUM here, because the output vector does not narrow a value
			// past its range, it wraps it.
			return expr.DeclQuotedLit(n.Value), expr.Decided
		}
		// LitNull is SQL's `unknown`: it names no type AND produces no
		// value, which is a different fact from "this branch decided
		// nothing" and is why it is marked. COALESCE(d, NULL) is a numeric
		// expression on PostgreSQL and stays one here; a branch that decided
		// nothing but WILL produce a value makes a DECIMAL fold decline
		// (expr.CommonDeclType).
		return expr.DeclUntyped(), expr.Undecided
	}
	return expr.DeclType{}, expr.Undecided
}

// caseDeclaredType types a CASE from its result branches: the THEN
// expressions and the ELSE, which are the values the CASE can evaluate to
// (the WHEN conditions and a simple CASE's subject only steer). SQL requires
// the branches to share a type, so the first branch that decides one answers
// for the expression; a branch that only guesses (a polymorphic call whose
// arguments decided nothing, see expr.Confidence/#331) is kept as the
// fallback answer and reported Guessed, so a caller holding a candidate of
// its own can still prefer it. A missing ELSE is an implicit NULL and
// decides nothing, like LitNull.
//
// Before #372 CaseNode had no arm here at all, so MIN/MAX over a string
// CASE aggregated a Float64-declared projection that dropped every string
// write and answered the integer 0 — while the same CASE projected was
// correct, because exec.Project re-types from its input and the
// pre-aggregate projection does not.
func caseDeclaredType(n *plansql.CaseNode, decls ColDecls) (expr.DeclType, expr.Confidence) {
	var guess expr.DeclType
	guessed := false
	var decided []expr.DeclType
	sawUnknown := false
	consider := func(branch plansql.Node) {
		if branch == nil {
			// A missing ELSE is an implicit NULL: it produces no value, so
			// it neither decides nor blocks a DECIMAL fold.
			return
		}
		t, c := nodeDeclaredType(branch, decls)
		switch c {
		case expr.Decided:
			decided = append(decided, t)
		case expr.Guessed:
			if !guessed {
				guess, guessed = t, true
			}
		default:
			if !t.Untyped {
				sawUnknown = true
			}
		}
	}
	for _, w := range n.Whens {
		consider(w.Result)
	}
	consider(n.Else)
	// expr.CommonDeclType, not decided[0]: the first decider still wins for
	// every type but DECIMAL, where the branches have to agree on a (p,s)
	// that holds all of them or the narrower one truncates the wider
	// branch's digits into the output vector (ADR-0024 item 2).
	// COALESCE/GREATEST/LEAST reconcile through the same function, so a CASE
	// and the COALESCE it rewrites to cannot answer different types — and
	// they decline together on a branch that names no type but still
	// produces a value.
	if d, ok := expr.CommonDeclType(decided, sawUnknown); ok {
		return d, expr.Decided
	}
	if guessed {
		return guess, expr.Guessed
	}
	return expr.DeclType{}, expr.Undecided
}

// stringOperand/bytesOperand distinguish TEXT/bytea by DECLARATION, never
// by their byte-readable boxes (ADR-0012 item 8). A quoted literal is
// TypeString here but PostgreSQL unknown, so stringOperand excludes it:
// bytea || bytea and bytea || unknown resolve to bytea, text || bytea to text.
// funcReturnType uses the registry declaration the vector kernel writes.
// Undecided leaves the caller's fallback; Guessed is usable but a calling
// polymorphic function must prefer a decided candidate it still has (#331).
func stringOperand(n plansql.Node, decls ColDecls) bool {
	d, c := nodeDeclaredType(n, decls)
	return c != expr.Undecided && d.ID == parquet.TypeString && !d.Quoted
}

func bytesOperand(n plansql.Node, decls ColDecls) bool {
	d, c := nodeDeclaredType(n, decls)
	return c != expr.Undecided && d.ID == parquet.TypeBytes
}

// bytesPreservingReturn declares the result of a function PostgreSQL has over
// bytea and which answers in bytea. The set is the server's own catalog, not
// every function that happens to accept bytes:
//
//	substring(bytea, int [, int])   bytea
//	overlay(bytea placing bytea …)  bytea
//
// A text-only function over a bytea argument is 42883 on the server and still
// ANSWERS here — the other half of #583, deferred with its mechanism in the
// arc report: refusing it needs a plan-time argument-type check that every
// compile site reaches, and a per-row refusal would be the data-dependent
// shape #627 just closed.
func bytesPreservingReturn(n *plansql.FuncCallNode, decls ColDecls) (expr.DeclType, bool) {
	switch strings.ToLower(strings.TrimSpace(n.Name)) {
	case "substr", "substring", "overlay":
	default:
		return expr.DeclType{}, false
	}
	if len(n.Args) == 0 || !bytesOperand(n.Args[0], decls) {
		return expr.DeclType{}, false
	}
	return expr.Decl(parquet.TypeBytes), true
}

func funcReturnType(n *plansql.FuncCallNode, decls ColDecls) (expr.DeclType, expr.Confidence) {
	if t, c, ok := setReturningDeclType(n, decls); ok {
		return t, c
	}
	if strings.EqualFold(n.Name, "row_field") && len(n.Args) == 2 {
		parent, confidence := nodeDeclaredType(n.Args[0], decls)
		if field, ok := n.Args[1].(*plansql.Lit); ok && parent.Schema != nil {
			if c, found := parent.Schema.Field(field.Value); found {
				// A field is declared by its own column: a DECIMAL its
				// (p,s), a container its element or fields (arc CW); a
				// VECTOR, whose dimension a projection reads elsewhere,
				// declares nothing.
				if d, ok := expr.ColumnDecl(c); ok {
					return d, confidence
				}
				return expr.DeclType{}, expr.Undecided
			}
		}
	}

	// The scalar math functions that answer in their argument's OWN domain
	// take their type from that argument, which the registry's fixed
	// RetFloat64 declaration cannot express (ADR-0024 items 2 and 3, #668).
	if t, ok := scalarFnDeclaredDecimal(n, decls); ok {
		return t, expr.Decided
	}
	// ...and the same for the INTEGER and REAL domains, which is ABS and MOD
	// alone — every other member of that family IS double precision over an
	// integer in PostgreSQL (#768).
	if t, ok := scalarFnDeclaredNumericDomain(n, decls); ok {
		return t, expr.Decided
	}
	// The functions PostgreSQL DOES have over bytea and which answer IN bytea:
	// substring keeps its argument's type, so `substring(b from 1 for 1)` is
	// bytea and not the text those bytes spell (#583). The registry's fixed
	// RetString cannot express it, and RetSameAsArg would mirror EVERY
	// argument type — including a DECIMAL one, which substring does not
	// return.
	if t, ok := bytesPreservingReturn(n, decls); ok {
		return t, expr.Decided
	}
	if t, ok := dateShiftReturn(n, decls); ok {
		return t, expr.Decided
	}
	t, c := expr.DefaultRegistry.ReturnType(n.Name).Resolve(len(n.Args), func(i int) (expr.DeclType, expr.Confidence) {
		return nodeDeclaredType(n.Args[i], decls)
	})
	if c == expr.Undecided {
		return expr.DeclType{}, expr.Undecided
	}
	if t.ID == parquet.TypeDecimal && !t.DecKnown {
		// A DECIMAL the resolution could not put a (p,s) on is not a
		// declaration a projection can allocate a vector from (#458): the
		// vector would come out at scale 0. Decline, exactly as this
		// function did for every DECIMAL before ADR-0024.
		return expr.DeclType{}, expr.Undecided
	}
	switch t.ID {
	case parquet.TypeRow:
		if len(t.RowFields()) > 0 {
			return t, c
		}
		return expr.DeclType{}, expr.Undecided
	case parquet.TypeArray, parquet.TypeMap:
		// A container declares only WITH its element — the child vector is
		// sized from it and the wire's array OID is read off it. The
		// registry carries it for every container-returning function
		// (tcp_flags, map_keys, element_at over a nested array …, arc CW
		// #1017); one that cannot say declines rather than build an ARRAY
		// column that reads back empty.
		if t.Schema != nil && t.Schema.ElementType != nil {
			return t, c
		}
		return expr.DeclType{}, expr.Undecided
	case parquet.TypeInt64:
		if bitwiseInt4Result(n, decls) {
			return expr.Decl(parquet.TypeInt32), c
		}
	}
	return t, c
}

// dateShiftReturn is date_add / date_sub's declaration, which follows the
// FIRST argument's declared type the way the `date ± n` operator's does: a
// DATE shifted by a whole number of days is a DATE, and anything else — a
// TIMESTAMP, text, an INTERVAL shift — is a TIMESTAMP. expr.dateShift boxes
// by the same rule and expr.shiftProducedTemporal names it for consumers, so
// the declaration and the value agree (arc VL round 3: the registry declared
// both functions TEXT while they returned a date, so `UPDATE … SET d =
// date_add(d, 1)` was refused as a text source).
func dateShiftReturn(n *plansql.FuncCallNode, decls ColDecls) (expr.DeclType, bool) {
	switch strings.ToLower(n.Name) {
	case "date_add", "date_sub":
	default:
		return expr.DeclType{}, false
	}
	if len(n.Args) == 2 && !nodeIsInterval(n.Args[1], decls) &&
		nodeTemporalKind(n.Args[0], decls) == temporalDay && !isTextColRef(n.Args[0], decls) {
		return expr.Decl(parquet.TypeDate), true
	}
	return expr.Decl(parquet.TypeTimestamp), true
}

// bitwiseInt4Result reports whether this call is a member of the BITWISE
// family whose operands are all int4-domain, which PostgreSQL declares
// integer.
//
// `c_i32 & 6` is integer on 17.11 and `c_i64 & 6` is bigint — the family
// FOLLOWS its operands, exactly as `+ - *` do (ADR-0024 item 2), and the
// registry gave it one fixed bigint declaration instead. The value was always
// the same number; the wire put it under OID 20 where a client that binds on
// the declared type expects 23 (#1018).
//
// Narrow on purpose, in three ways. Only PGIntWidthOperands, the one class
// whose PostgreSQL result type is a function of its arguments. Only where the
// width walk PROVES int4: unknown leaves the bigint carrier alone, because
// guessing narrow is how an exact bigint becomes a wrapped integer. And only
// where the table says the result FITS that width — which excludes the three
// SHIFTS, measured: this engine shifts on the int64 carrier and PostgreSQL's
// int4 shift is modular, so declaring int4 for `w_i32 << 2` turned a query
// the server answers into a 22003 at the store guard.
func bitwiseInt4Result(n *plansql.FuncCallNode, decls ColDecls) bool {
	w, known := expr.PGIntegerResultWidth(n.Name)
	if !known || w.Width != expr.PGIntWidthOperands || !w.FitsOperands {
		return false
	}
	if widestArgIntWidth(n.Args, &w, decls) != intWidth4 {
		return false
	}
	// And every width-contributing argument must be KNOWN, for the reason
	// intWidthFullyKnown states: widerIntWidth lets an unknown operand be
	// narrowed by a known int4 sibling, which here would declare int4 for
	// `BITWISE_AND(<a bigint nothing typed>, 6)`.
	for i, a := range n.Args {
		if pgWidthArg(w, i) && !intWidthFullyKnown(a, decls) {
			return false
		}
	}
	return true
}

// inferCastType maps SQL type names to parquet types for CAST expressions.
//
// DATE and TIMESTAMP name the real column types because expr.Cast now produces
// their real representation — epoch days / epoch milliseconds — rather than
// passing its argument through (#340). The declared type is what turns that
// number back into a date at the output: the projection allocates a DATE
// vector, whose renderer is batch.FormatDate. Declaring String instead would
// print the day NUMBER, which is the mirror image of the bug being fixed.
//
// TIME stays a string: the engine has no time-of-day column type, so
// `TIME '10:00:00'` keeps its text, and so does expr.Cast.
func inferCastType(typeName string) parquet.TypeID {
	// FLOAT(n) carries its width in the NAME, so it matches no case label
	// below and used to reach `default: return TypeString` — a numeric value
	// under a STRING column, the #310/#443 shape (#652). PostgreSQL resolves
	// it by width: float(1..24) is real, float(25..53) is double precision.
	// parquet.FloatTypePrecision is the one reading of that rule; an
	// out-of-range n is refused by the evaluator with 22023, and declaring
	// double for it here costs nothing because no row is ever produced.
	if bits, err, ok := parquet.FloatTypePrecision(typeName); ok {
		if err == nil && bits <= 24 {
			return parquet.TypeFloat32
		}
		return parquet.TypeFloat64
	}
	if _, _, ok := expr.VectorCastDim(typeName); ok {
		return parquet.TypeVector
	}
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	// SIGNED is here because expr.IsIntegerCastDest lists it and Cast.Eval's
	// integer arm takes it: without it the evaluator produced an int64 and the
	// projection declared STRING, so `CAST(x AS SIGNED)` published the number
	// as text — the same disagreement between the two layers that #652 is
	// about, one spelling over.
	// INT32 joins the family rather than getting a carrier of its own: it is
	// a second spelling of int4, and the comment below is why every integer
	// spelling lands on INT64. Before #901 it matched no label here and no
	// label in Cast.Eval either, so `x::INT32` published its operand
	// unchanged under a STRING declaration — #310/#443's shape, and the one
	// #652 closed for names that answer to nothing at all.
	case "INTEGER", "INT", "INT4", "INT32", "BIGINT", "INT8", "INT64", "SMALLINT", "INT2", "SIGNED":
		// Every integer spelling lands on INT64, and #1070 did NOT move this
		// one. The cast evaluator enforces each spelling's own RANGE (22003
		// past it), so the value would fit an int4 column — but the DAG
		// declares a CAST OVER A WINDOW from the window's own output rather
		// than from the cast, so narrowing here made
		// `CAST(SUM(a) OVER () AS INTEGER)` int4 on the single path and int8
		// on the stage arms: one expression, two declarations, which is
		// exactly what #813 item 1 was and what this arc forbids. Measured,
		// and filed rather than half-fixed. The OID a client sees is int8
		// where PostgreSQL says int4/int2, in ADR-0012 item 12's list.
		return parquet.TypeInt64
	case "REAL", "FLOAT4", "FLOAT32":
		// float4, not float8: expr.Cast now ROUNDS to float32 for these two
		// spellings, so the projection has to allocate a column that can hold
		// what the evaluator produces. Declaring FLOAT64 would widen the
		// rounded value straight back and make `CAST(x AS REAL)` look like the
		// no-op it used to be — and it is a FLOAT32 column's own comparison
		// rules the result must then get (#631's width rule).
		//
		// Bare FLOAT stays below with DOUBLE: PostgreSQL's unqualified `float`
		// is double precision, not real (pg_typeof, verified live).
		return parquet.TypeFloat32
	case "FLOAT", "DOUBLE", "DOUBLE PRECISION", "FLOAT8", "FLOAT64", "NUMERIC", "DECIMAL":
		return parquet.TypeFloat64
	case "BOOLEAN", "BOOL":
		return parquet.TypeBool
	case "DATE":
		return parquet.TypeDate
	case "TIMESTAMP", "DATETIME", "TIMESTAMPTZ":
		return parquet.TypeTimestamp
	case "PORT", "PROTOCOL":
		// The declaration half of #901. Cast.Eval's integer arm answers an
		// int64 for these two now; declaring STRING for it published the
		// number as text under OID 25 where a PORT COLUMN declares int4
		// (OID 23, #834) — the same layer disagreement #652 is about, and
		// the reason the value never reached the store's int4 guard. Naming
		// the real type also puts the guard back on the path: the projection
		// allocates a PORT/PROTOCOL vector, and batch.IntegerRangeError is
		// the second net behind castIntInRange's.
		if strings.EqualFold(strings.TrimSpace(typeName), "PORT") {
			return parquet.TypePort
		}
		return parquet.TypeProtocol
	case "IPV4", "IP":
		return parquet.TypeIPv4
	case "IPV6":
		return parquet.TypeIPv6
	case "CIDR":
		return parquet.TypeCIDR
	case "MAC", "MACADDR":
		// The declaration half of #1092, the same shape as UUID's below. The
		// four address types declared STRING because Cast.Eval implemented
		// none of them; now that it parses, the projection has to allocate a
		// column that can HOLD what the evaluator produces, or a
		// `CREATE TABLE … AS SELECT CAST(col AS IPV4) FROM read_parquet(…)`
		// mints a STRING column over a foreign file's string column — which
		// is the whole reason the cast exists.
		return parquet.TypeMAC
	case "UUID":
		// The declaration half of #839. `CAST(x AS UUID)` declared STRING, so
		// the cast changed neither the value nor the type a client sees —
		// OID 25 for a column PostgreSQL declares 2950, and a driver that
		// branches on the OID (pgx's UUID scanner, pgJDBC's) never saw one.
		return parquet.TypeUUID
	default:
		// What is LEFT here is the destinations Cast.Eval does not implement
		// and passes its operand through — the containers, DURATION, BYTES.
		// A name that answers to NO type at all no
		// longer reaches this arm: expr.KnownCastDest refuses it at compile
		// with 42704, because declaring STRING for it made the two layers
		// agree with each other about a column PostgreSQL says cannot be
		// described (#652).
		return parquet.TypeString
	}
}

// castVectorDim is the dimension a VECTOR cast produces: its modifier, or for
// the unconstrained `VECTOR` an ARRAY constructor operand's own length. 0
// means the plan cannot say, which a projection refuses (projection_plan.go).
func castVectorDim(n *plansql.CastNode) int {
	dim, err, _ := expr.VectorCastDim(n.TypeName)
	if err != nil {
		return 0
	}
	if dim == 0 {
		if lit, ok := plansql.Unparen(n.Inner).(*plansql.ArrayLitNode); ok {
			dim = len(lit.Elements)
		}
	}
	return dim
}

// binOpTemporalType types the date-arithmetic shapes expr.BinOp evaluates,
// so the projection's output column holds what the evaluator produces — the
// disagreement #340 is about, in the other direction:
//
//	date - date             → BIGINT, a count of days
//	date ± integer          → DATE, the day n days away
//	integer + date          → DATE
//	date|timestamp ± interval, interval + date|timestamp → TIMESTAMP
//
// Every operand is judged by its DECLARED type, never by how it is spelled
// (arc VL round 3): `DATE '…' + CAST(1 AS INT)`, `(d + 1) + 1`, `d + i` over an
// INTEGER column and `CURRENT_DATE + 1 - 1` are all DATE, where the old rule
// accepted only a bare number literal on the integer side and declared every
// other spelling double precision while expr.BinOp produced a day count.
// expr.arithProducedTemporal is the same rule over the evaluator's boxes.
//
// Everything else declines and the caller's numeric rules stand. A TIMESTAMP
// minus a TIMESTAMP is SQL's INTERVAL and the engine has no interval column;
// expr.BinOp.dateArith leaves it on the numeric path and this agrees.
func binOpTemporalType(n *plansql.BinaryOp, decls ColDecls) (expr.DeclType, expr.Confidence) {
	if n.Op != "+" && n.Op != "-" {
		return expr.DeclType{}, expr.Undecided
	}
	lk := nodeTemporalKind(n.Left, decls)
	rk := nodeTemporalKind(n.Right, decls)
	switch {
	case (lk != temporalNone || nodeIsQuotedText(n.Left)) && nodeIsInterval(n.Right, decls):
		return expr.Decl(parquet.TypeTimestamp), expr.Decided
	case n.Op == "+" && (rk != temporalNone || nodeIsQuotedText(n.Right)) && nodeIsInterval(n.Left, decls):
		return expr.Decl(parquet.TypeTimestamp), expr.Decided
	case nodeIsQuotedText(n.Left) != nodeIsQuotedText(n.Right) &&
		(strictTemporalKind(n.Left, lk, decls) != temporalNone || strictTemporalKind(n.Right, rk, decls) != temporalNone):
		// A quoted operand beside a DATE / TIMESTAMP: the type PostgreSQL's
		// operator resolution gives it (expr.ResolveUnknownTemporal) —
		// `date - '…'` a day count, `ts - '…'` the documented milliseconds,
		// `ts + '…'` a TIMESTAMP. `date + '…'` is refused (42725) by the
		// typing rule and declares nothing.
		k := strictTemporalKind(n.Left, lk, decls)
		if k == temporalNone {
			k = strictTemporalKind(n.Right, rk, decls)
		}
		switch expr.ResolveUnknownTemporal(n.Op, k == temporalInstant) {
		case expr.UnknownAsDate:
			return expr.Decl(parquet.TypeInt64), expr.Decided
		case expr.UnknownAsTimestamp:
			return expr.Decl(parquet.TypeFloat64), expr.Decided
		case expr.UnknownAsInterval:
			return expr.Decl(parquet.TypeTimestamp), expr.Decided
		}
		return expr.DeclType{}, expr.Undecided
	case n.Op == "-" && lk == temporalDay && rk == temporalDay:
		return expr.Decl(parquet.TypeInt64), expr.Decided
	case n.Op == "-" && lk == temporalInstant && rk == temporalInstant:
		// No INTERVAL type: the difference of two instants is the documented
		// number of milliseconds (docs/postgres-differences.md), a double —
		// the value the kernel produces. Undecided, it was published as TEXT
		// (OID 25) on the wire: `now() - now()` read `0` as a string
		// (round-3 review N4).
		return expr.Decl(parquet.TypeFloat64), expr.Decided
	case lk == temporalDay && rk == temporalNone && nodeIsIntegerDeclared(n.Right, decls):
		return expr.Decl(parquet.TypeDate), expr.Decided
	case rk == temporalDay && lk == temporalNone && n.Op == "+" && nodeIsIntegerDeclared(n.Left, decls):
		return expr.Decl(parquet.TypeDate), expr.Decided
	}
	return expr.DeclType{}, expr.Undecided
}

// strictTemporalKind is an operand's temporal kind with a VARCHAR column
// counted as text, not as a day: the unknown-literal resolution is
// PostgreSQL's for a DATE or TIMESTAMP operand only.
func strictTemporalKind(node plansql.Node, k temporalKind, decls ColDecls) temporalKind {
	if isTextColRef(node, decls) {
		return temporalNone
	}
	return k
}

// temporalKind is what an operand of `date ± x` can be.
//
// A column the catalog declares VARCHAR counts as a day: that is how the
// TPC-H fixtures spell every date, and it is how `l_receiptdate - l_shipdate`
// reaches the operator at all. Nothing is lost by assuming it, because the
// only values the runtime can produce for a text column here are a day count
// (when the text parses as a date) and NULL — reading a text column as a
// number, which is what the caller's Float64 rule does, answers NULL either
// way.
type temporalKind int

const (
	temporalNone temporalKind = iota
	temporalDay
	temporalInstant
)

// nodeTemporalKind reports what kind of temporal value an operand carries,
// from its DECLARED type — a cast's destination, a column's catalog type, a
// function's registry declaration, a nested arithmetic node's own
// binOpTemporalType answer — so a DATE is a DATE however it was produced (arc
// VL round 3; round 2 added the function arm alone, and a nested `(d + 1) + 1`
// or `CURRENT_DATE + 1 - 1` still fell to double precision). A quoted literal
// is SQL's unknown and names nothing; a column the catalog declares VARCHAR is
// the one text operand read as a day (see temporalKind).
func nodeTemporalKind(node plansql.Node, decls ColDecls) temporalKind {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return nodeTemporalKind(n.Inner, decls)
	case *plansql.ColRef:
		t, ok := decls.colType(n)
		if !ok {
			return temporalNone
		}
		if t == parquet.TypeString {
			return temporalDay
		}
		return temporalKindOf(t)
	case *plansql.BinaryOp:
		// The arithmetic rule alone, not the whole declaration walk: a long
		// numeric chain `a + b + c …` stays linear here.
		t, c := binOpTemporalType(n, decls)
		if c != expr.Decided {
			return temporalNone
		}
		return temporalKindOf(t.ID)
	case *plansql.Lit, *plansql.IntervalLit:
		return temporalNone
	}
	t, c := nodeDeclaredType(node, decls)
	if c != expr.Decided {
		return temporalNone
	}
	return temporalKindOf(t.ID)
}

func temporalKindOf(t parquet.TypeID) temporalKind {
	switch t {
	case parquet.TypeDate:
		return temporalDay
	case parquet.TypeTimestamp:
		return temporalInstant
	}
	return temporalNone
}

// nodeIsIntegerDeclared reports whether an operand DECLARES an integer — the
// `n` of `date ± n`, by its type: an integer literal, an integer column (PORT
// and PROTOCOL are int4 arithmetic too, #1000), a cast to an integer type,
// integer arithmetic.
func nodeIsIntegerDeclared(node plansql.Node, decls ColDecls) bool {
	t, c := nodeDeclaredType(node, decls)
	return c == expr.Decided && intArithColumnType(t.ID)
}

// nodeIsQuotedText reports a quoted literal — SQL's unknown, which an INTERVAL
// shift resolves to PostgreSQL's preferred datetime type, timestamp
// (expr.textOperand is the runtime half).
func nodeIsQuotedText(node plansql.Node) bool {
	if p, ok := node.(*plansql.ParenNode); ok {
		return nodeIsQuotedText(p.Inner)
	}
	l, ok := node.(*plansql.Lit)
	return ok && l.Kind == plansql.LitString
}

// nodeIsInterval reports whether an operand is an INTERVAL: a literal, or a
// cast to one.
func nodeIsInterval(node plansql.Node, decls ColDecls) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return nodeIsInterval(n.Inner, decls)
	case *plansql.IntervalLit:
		return true
	case *plansql.CastNode:
		return strings.EqualFold(strings.TrimSpace(n.TypeName), "interval")
	}
	return false
}

// isTextColRef reports a column the catalog declares VARCHAR — a day to the
// `date ± n` operator (temporalKind), but not a DATE argument to date_add,
// whose text argument is a TIMESTAMP like any other text (dateShiftReturn).
func isTextColRef(node plansql.Node, decls ColDecls) bool {
	if p, ok := node.(*plansql.ParenNode); ok {
		return isTextColRef(p.Inner, decls)
	}
	cr, ok := node.(*plansql.ColRef)
	if !ok {
		return false
	}
	t, ok := decls.colType(cr)
	return ok && t == parquet.TypeString
}

// binOpInvolvesTemporal reports whether either operand of a BinaryOp is an
// INTERVAL or DECLARES a date or timestamp. Such an operator is temporal
// arithmetic, never numeric: binOpTemporalType types the shapes that have a
// type, and every other one (a timestamp plus a number, say) must not be
// declared a number either. Judged by declared type (arc VL round 3); it was a
// list of five function NAMES, which missed every other date-valued producer.
// A VARCHAR column is not temporal here — `s * 2` over one stays numeric.
func binOpInvolvesTemporal(b *plansql.BinaryOp, decls ColDecls) bool {
	return operandIsTemporal(b.Left, decls) || operandIsTemporal(b.Right, decls)
}

func operandIsTemporal(n plansql.Node, decls ColDecls) bool {
	if nodeIsInterval(n, decls) {
		return true
	}
	return nodeTemporalKind(n, decls) != temporalNone && !isTextColRef(n, decls)
}

// isPlainGroupKey reports whether a GROUP BY expression is a bare column
// reference the aggregate can resolve by name. A literal is NOT plain here:
// GROUP BY 1 (a positional ref resolved to a literal select item, or an
// actual constant key) has no input column — it needs the synthetic
// pre-projection like any computed expression, or the key silently
// resolves to a nonexistent column and every row lands in one NULL group.
//
// Nor is a ROW FIELD PATH, which is why decls is a parameter: `rw.n` parses
// to the same *plansql.ColRef a table-qualified reference does, and only the
// input's declarations tell them apart. exec.HashAggregate resolves its keys
// through columnIndexFallback, which has no ROW arm, so a field path handed
// through as a name failed with `GROUP BY key "rw.n" is not a column of its
// input`. The synthetic pre-projection materializes it instead, at the
// field's declared type (#568).
func isPlainGroupKey(node plansql.Node, decls ColDecls) bool {
	cr, ok := node.(*plansql.ColRef)
	return ok && !decls.isFieldPath(cr)
}

func isSimpleColRef(node plansql.Node) bool {
	// A bare column reference is the ONLY aggregate input that needs no
	// pre-projection: it already names a column of the aggregate's input.
	// A literal does NOT — `MIN(1)` must materialize a constant column and
	// aggregate over it, or the aggregate is handed a column literally named
	// "1" that no scan produces, and it errors (no GROUP BY) or drops every
	// group (with one). The DAG spec path treats only a *ColRef as bare
	// (plan.go's aggSpecs builder), and this gate must state the same rule
	// (#621).
	_, ok := node.(*plansql.ColRef)
	return ok
}

// arrayLitDeclaredType is an ARRAY[…] constructor's declaration: the array OF
// its elements' common type (expr.ArrayLitElementDecl — PostgreSQL's §10.5
// rule, the one UNION and CASE use), with that element carried so the
// projection builds an array vector and the wire declares the element's
// array OID. Before arc CW the constructor had no arm here, so it declared
// the STRING fallback: the value went out as Go's `[1 2 3]` under OID 25,
// and every reader above it — a subscript, ANY(), ORDER BY — read that text
// (#1250, #1303, #1021).
//
// An element nothing can type (a scalar subquery the caller cannot resolve)
// declines the whole constructor. A constructor of nothing but NULLs is
// text[], as PostgreSQL resolves `unknown`; one of NO elements has no type
// to declare (PostgreSQL refuses it outright, 42P18) and declines.
func arrayLitDeclaredType(n *plansql.ArrayLitNode, decls ColDecls) (expr.DeclType, expr.Confidence) {
	var decided []expr.DeclType
	for _, e := range n.Elements {
		t, c := nodeDeclaredType(e, decls)
		switch {
		case c == expr.Decided:
			decided = append(decided, t)
		case t.Untyped:
			// A NULL element names no type and adopts the others'.
		default:
			return expr.DeclType{}, expr.Undecided
		}
	}
	if len(decided) == 0 {
		if len(n.Elements) == 0 {
			return expr.DeclType{}, expr.Undecided
		}
		return arrayOfDecl(expr.Decl(parquet.TypeString))
	}
	el, ok := expr.ArrayLitElementDecl(decided)
	if !ok {
		return expr.DeclType{}, expr.Undecided
	}
	return arrayOfDecl(el)
}

// arrayOfDecl is the ARRAY declaration whose element is el — its (p,s), its
// own element or fields carried whole — or Undecided when el is not a
// declaration a child vector can be built from (a DECIMAL with no scale).
// arrayCastExtraDims is how many array levels a `T[]` cast's operand has
// beyond the one the destination spells: 0 for a one-dimensional operand.
func arrayCastExtraDims(n *plansql.CastNode, decls ColDecls) int {
	src, c := nodeDeclaredType(n.Inner, decls)
	if c != expr.Decided || src.ID != parquet.TypeArray || src.Schema == nil {
		return 0
	}
	extra := 0
	for el := src.Schema.ElementType; el != nil && el.Type == parquet.TypeArray; el = el.ElementType {
		extra++
	}
	return extra
}

func arrayOfDecl(el expr.DeclType) (expr.DeclType, expr.Confidence) {
	col, ok := declColumn(el)
	if !ok {
		return expr.DeclType{}, expr.Undecided
	}
	col.Name, col.Nullable = "element", true
	arr := parquet.Column{Type: parquet.TypeArray, Nullable: true, ElementType: &col}
	return expr.DeclType{ID: parquet.TypeArray, Schema: &arr}, expr.Decided
}

// declColumn is a declaration as the column it allocates: a container its
// whole shape, a DECIMAL its (p,s).
func declColumn(d expr.DeclType) (parquet.Column, bool) {
	switch d.ID {
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
		if d.Schema == nil {
			return parquet.Column{}, false
		}
		c := d.Schema.Clone()
		c.Type = d.ID
		if (c.Type == parquet.TypeRow && len(c.Fields) == 0) || (c.Type != parquet.TypeRow && c.ElementType == nil) {
			return parquet.Column{}, false
		}
		return c, true
	case parquet.TypeDecimal:
		if !d.DecKnown {
			return parquet.Column{}, false
		}
	case parquet.TypeVector:
		return parquet.Column{}, false
	}
	return declTypeParts(d), true
}
