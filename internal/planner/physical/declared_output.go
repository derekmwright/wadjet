// This file holds declared output for the physical planner, governed by ADR-0024 and ADR-0026.
package physical

import (
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
	return inferProjectionTypeDecls(node, fallback, strictInt, colDecls{types: colTypes})
}

// inferProjectionTypeDecls is inferProjectionTypeCols with the ROW FIELDS of
// its input in hand as well as the column types, so a field path inside the
// expression can decide a type. Callers that hold the logical node the
// expression reads should use this one (inputColDecls); the map-only
// signature above stays for the callers whose types are synthesized rather
// than read off a scan (emittedColTypes and friends), where there are no
// fields to carry.
func inferProjectionTypeDecls(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, decls colDecls) parquet.TypeID {
	return inferProjectionDeclType(node, fallback, strictInt, decls).ID
}

// inferProjectionDeclType is inferProjectionTypeDecls with the parameterized
// part of the answer kept — a DECIMAL's (precision, scale), which a bare
// parquet.TypeID cannot carry and which the output vector must have or every
// value in it reads back at the wrong power of ten (ADR-0024 item 2).
// Callers that materialize a vector from the answer take this one; callers
// that only need the TypeID keep the wrapper above.
func inferProjectionDeclType(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, decls colDecls) expr.DeclType {
	t, _ := inferProjectionDeclTypeConf(node, fallback, strictInt, decls)
	return t
}

// inferProjectionDeclTypeConf is inferProjectionDeclType with the CONFIDENCE
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
	strictInt map[string]bool, decls colDecls) (expr.DeclType, expr.Confidence) {
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
		// nodeDeclaredType answered Undecided and the STRING fallback stood.
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
		decls = colDecls{}
	}
	// A guess is still the answer here: nothing is left to consult, and a
	// polymorphic function's fallback is what types SELECT NULLIF(int_col, 1)
	// numeric. Only expr.Undecided leaves the type to the caller.
	if t, c := nodeDeclaredType(node, decls); c != expr.Undecided {
		return t, c
	}
	return expr.Decl(fallback), expr.Undecided
}

// intArithAllInt mirrors expr.operandIsInt over the AST: int-typed scan
// columns, integer literals, and nested integer arithmetic. Anything
// unrecognized declines (Float64 declaration = today's behavior). `/` is
// integer division over integer operands (#369, ADR-0012), so it declares
// Int64 exactly as +,-,*,% do — mirroring expr.BinOpNumeric's runtime mode,
// of which this must stay a strict subset.
func intArithAllInt(node plansql.Node, strictInt map[string]bool, decls colDecls) bool {
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
		// nodeDeclaredType's own UnaryOp arm has it and as expr.UnaryOp.Eval
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
			// (nodeDeclaredType), which runs at every nested site — a CASE
			// branch, a COALESCE argument, an aggregate's input — and has no
			// scan-level column set to consult. The catalog types in decls
			// are the same authority colRefDeclaredType already trusts to
			// type a bare column reference at those very sites, so reading
			// them here claims nothing new; it only stops the claim from
			// evaporating the moment the reference sits under a `+`.
			return c.Type == parquet.TypeInt64 || c.Type == parquet.TypeInt32
		}
		// A ROW FIELD PATH of a strictly-int type is one too. strictInt is
		// keyed by COLUMN name and a field is not a column, so `rw.n + 1`
		// declared FLOAT64 where `n + 1` over the same value declares INT64
		// — the two spellings of one question answering with different types
		// (#568, #297's rule).
		if f, ok := decls.field(n); ok && decls.isFieldPath(n) {
			return f.Type == parquet.TypeInt64 || f.Type == parquet.TypeInt32
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
	// tail: the node kinds nodeDeclaredType knows and the switch above does
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
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColTypes(n.Children[0])
	case logical.NodeWindow:
		// A window APPENDS its outputs to its input and renames nothing, so
		// its input's names survive and the SLOTS join them. Their type is
		// the stage's own answer, `windowSpecOutputType` — the same one
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
// It goes through aggOhlcvOutputFields, which is the same function
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

func inputColFields(n *logical.Node) map[string][]parquet.Column {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		return n.ScanColFields
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColFields(n.Children[0])
	case logical.NodeProject:
		// inputColTypes STOPS at a Project because a rename can bind a name
		// to a different value. The FIELDS walk does not have to: a
		// rename-only projection FORWARDS its columns, and which column each
		// output name forwards is written down right here. Mapping them is
		// what lets a field path through a derived table or a CTE keep its
		// type — `SELECT rw.n FROM (SELECT rw FROM t) s` answered string("9")
		// and `MIN(rw.n)` over the same subquery could not resolve its input
		// at all, because the walk answered nil the moment a Project was in
		// the way (#568).
		//
		// A computed or aggregate item stops the whole walk, exactly as
		// sourceColTypesThroughRenames stops for it: past that point a name
		// may be bound to a value the fields below do not describe.
		if len(n.Children) != 1 {
			return nil
		}
		below := inputColFields(n.Children[0])
		if below == nil {
			return nil
		}
		var out map[string][]parquet.Column
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
				// precise statement of "the fields below no longer describe
				// this name".
				name := strings.ToLower(cleanExpr(p.Alias))
				if name == "" {
					name = strings.ToLower(cleanExpr(p.Column))
				}
				if name == "" {
					return nil
				}
				if out == nil {
					out = make(map[string][]parquet.Column)
				}
				if f, ok := aggregateProjectionFields(n, p); ok {
					out[name] = f
				} else {
					out[name] = nil
				}
				continue
			}
			if p.Column == "" {
				// A computed item SHADOWS its own name, but leaves other names' field
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
					out = make(map[string][]parquet.Column)
				}
				out[name] = nil
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
				out = make(map[string][]parquet.Column)
			}
			out[strings.ToLower(cleanExpr(name))] = f
		}
		return out
	case logical.NodeAggregate:
		// An aggregate publishes its OWN outputs and forwards nothing: every
		// name it emits is either a group KEY (whose fields come from the
		// column below, and a container group key is a different question
		// this walk has never answered) or an aggregate OUTPUT. Only the
		// second kind can declare a ROW today — the bar — and it declares it
		// through the one derivation ADR-0035 item 5 names.
		var out map[string][]parquet.Column
		for i := range n.AggExprs {
			f, ok := aggOhlcvOutputFields(n, n.AggExprs[i])
			if !ok {
				continue
			}
			if out == nil {
				out = make(map[string][]parquet.Column)
			}
			out[strings.ToLower(cleanExpr(n.AggExprs[i].OutputCol))] = f
		}
		return out
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left, right := inputColFields(n.Children[0]), inputColFields(n.Children[1])
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		merged := make(map[string][]parquet.Column, len(left)+len(right))
		for c, f := range left {
			merged[c] = f
		}
		for c, f := range right {
			if prev, dup := merged[c]; dup && !sameRowFields(prev, f) {
				delete(merged, c)
				continue
			}
			merged[c] = f
		}
		return merged
	}
	return nil
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
// output columns declare, ready to hand to nodeDeclaredType. Callers that
// hold the logical node an expression reads should build the context here
// rather than passing inputColTypes alone, which cannot type a field path.
func inputColDecls(n *logical.Node) colDecls {
	return colDecls{types: inputColTypes(n), fields: inputColFields(n), dec: inputColDecimal(n)}
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
func sourceColDeclsThroughRenames(n *logical.Node) colDecls {
	for n != nil && n.Type == logical.NodeProject && len(n.Children) == 1 {
		for _, p := range n.Projections {
			if p.IsAgg || p.Column == "" {
				return colDecls{}
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

// colDecls is what a node's output columns declare, as far as the planner can
// know it: the flat catalog types (inputColTypes) plus, for the ROW columns
// among them, the FIELDS a field path can name (inputColFields).
//
// The second map exists because the first cannot answer the question. It is
// keyed by column name, and the `c` in `rw.c` is not a column of anything —
// so every lookup missed, the projection kept its STRING default, and a ROW
// field path was declared STRING whatever its real type (#568).
type colDecls struct {
	types  map[string]parquet.TypeID
	fields map[string][]parquet.Column
	// dec carries the (precision, scale) of the DECIMAL entries in types.
	// A bare TypeID is not a type for a DECIMAL — a projection declared
	// DECIMAL without its scale builds an output vector that reads every
	// value back at the wrong power of ten — which is why colRefDeclaredType
	// used to decline the type outright (ADR-0024 item 2, #529/#555/#587).
	dec map[string]logical.DecimalMeta
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
	// nodeDeclaredType's SubqueryNode arm declines on.
	//
	// It exists because a subquery is a WHOLE SECOND QUERY whose type lives
	// in the CATALOG, not in the enclosing query's columns: `SELECT id,
	// (SELECT MAX(c_i64) FROM typemx) AS mx` has nothing in `types` that
	// describes `mx`. Without it the projection fell to its STRING fallback
	// and a bigint came back as a Go string on every arm and every door,
	// where PostgreSQL declares int8 (#874) — and the const-arith fold saw an
	// Undecided operand and folded `+ 1` on the FLOAT rung (#714's third box).
	subqueryDecl func(sql string) (parquet.Column, bool)
	// placeholderTypes is the declared type of each `:scalar_N` deferred
	// literal, keyed by the placeholder's NAME.
	//
	// It is a map of its own rather than an entry in `types` because a
	// placeholder is not a column and must not be resolvable as one: a user
	// column called `scalar_1` would otherwise answer for it. The DAG's
	// SELECT-list lowering fills it from each producer's own plan, which is
	// the only thing that knows what the value will be (#874).
	placeholderTypes map[string]parquet.TypeID
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
func (d colDecls) colType(n *plansql.ColRef) (parquet.TypeID, bool) {
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
func (d colDecls) colDecl(n *plansql.ColRef) (parquet.Column, bool) {
	at := func(key string) (parquet.Column, bool) {
		t, ok := d.types[key]
		if !ok {
			return parquet.Column{}, false
		}
		col := parquet.Column{Name: key, Type: t}
		if t == parquet.TypeDecimal {
			if m, ok := lookupColDecimal(d.dec, key); ok {
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
// its own declared type's, which colDecls.field already carries.
func (d colDecls) colIntWidth(n *plansql.ColRef) (intWidth, bool) {
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
func (d colDecls) field(n *plansql.ColRef) (parquet.Column, bool) {
	if n == nil || n.Table == "" || d.fields == nil {
		return parquet.Column{}, false
	}
	fields, ok := d.fields[strings.ToLower(n.Table)]
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
func (d colDecls) isFieldPath(n *plansql.ColRef) bool {
	if n == nil || n.Table == "" {
		return false
	}
	// A column of the whole dotted spelling — a flat Zeek `id.orig_h` — is
	// that column and not a path into anything.
	if _, ok := d.types[strings.ToLower(n.Table+"."+n.Column)]; ok {
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
func astIsFieldPath(node plansql.Node, decls colDecls) bool {
	_, ok := fieldPathRef(node, decls)
	return ok
}

// fieldPathRef returns the reference behind a ROW field path, and its
// undropped `parent.field` spelling. cleanExpr strips the qualifier from
// every column reference — right for a table alias, and the reason a field
// path arrives downstream as a bare field name no column carries — so this is
// the one place the whole path survives.
func fieldPathRef(node plansql.Node, decls colDecls) (string, bool) {
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
// its input (inputColDecls). Undecided — today's answer, and the caller's
// fallback with it — for a name no scan carries, a name two scans disagree
// on, and anything that is not a scan column at all: an aggregate output, a
// synthetic sort or group key. #331's machinery propagates a decision as
// fact, so a wrong confident answer here is worse than the guess it replaces.
//
// A ROW FIELD PATH resolves here too, on exactly the same terms as a column
// (colDecls.colType). Before #568 it could not: the lookup was keyed by
// column name and `c` is not a column of `t(id, c_flat, rw)`, so `rw.c`
// answered Undecided and the caller's STRING fallback stood — which is how
// `SELECT rw.n` over an INT64 field returned string("9") and `ORDER BY rw.c`
// sorted a CIDR field by its stored text while `ORDER BY rw` over the same
// values sorted by inet.
func colRefDeclaredType(n *plansql.ColRef, decls colDecls) (expr.DeclType, expr.Confidence) {
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
	case parquet.TypeVector, parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
		// The other parameterized types: the catalog map carries the TypeID
		// and nothing else, and a projection declared VECTOR without its
		// dimension or ARRAY without its element type builds an output
		// vector that reads back wrong. funcReturnType declines the nested
		// types for the same reason.
		//
		// A field path of one of these types declines too, and for the same
		// reason — exec.Project repairs it from the input batch, where the
		// parent ROW vector's child carries the whole shape.
		return expr.DeclType{}, expr.Undecided
	}
	return expr.Decl(c.Type), expr.Decided
}

// declTypeParts splits a resolved declaration into the three fields the
// projection specs carry it in. One call site's worth of sugar, so a spec
// assignment stays one statement.
func declTypeParts(d expr.DeclType) (parquet.TypeID, int, int) {
	return d.ID, d.Precision, d.Scale
}

// emittedColDecls is inputColDecls over what a node EMITS rather than what it
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
func emittedColDecls(n *logical.Node) colDecls {
	return colDecls{
		types:    emittedColTypes(n),
		fields:   inputColFields(n),
		dec:      emittedColDecimal(n),
		intWidth: emittedColIntWidth(n),
	}
}

// inferProjectionType infers the output parquet type from an AST expression
// node with nothing known about its input, returning the fallback when
// inference isn't possible.
func inferProjectionType(node plansql.Node, fallback parquet.TypeID) parquet.TypeID {
	return inferProjectionTypeCols(node, fallback, nil, nil)
}

// ProjectionOutputType is inferProjectionType for callers outside this
// package. The worker's pre-aggregate projection compiles a derived GROUP BY
// key from its SQL TEXT and has no catalog to resolve the columns in it, so it
// needs the same rule the planner applies to a SELECT-list expression — the
// same reason distributed.AggSpec.InputType is carried on the spec.
//
// It used to declare every derived key String, which is right only when the
// expression returns one: CAST(l_shipdate AS DATE) evaluates to an epoch-day
// number, and a String vector stored it as the DIGITS of that number, so the
// stage DAG grouped by "8039" where the single-process path grouped by
// 1992-01-05 (#340).
//
// Only a DECIDED type is taken. A polymorphic declaration that answered with
// its own fallback (expr.Guessed) has decided nothing here, because the caller
// holds no column types for it to consult: COALESCE(n_name, n_comment) would
// answer Float64 from coalesce's numeric fallback, and a Float64 vector drops
// every string it is handed — 1 group where there are 25 (#331/#333). The
// caller's fallback stands in those cases, exactly as before.
func ProjectionOutputType(node plansql.Node, fallback parquet.TypeID) expr.DeclType {
	if t, c := nodeDeclaredType(node, colDecls{}); c == expr.Decided {
		return t
	}
	return expr.Decl(fallback)
}

// DeclaredTypeOfNode resolves an expression's declared type against table
// columns, using the query path's inference. DML must use the declaration,
// not the float64 box shared by float8 and numeric: assignment rounds float8
// half to EVEN and numeric half AWAY FROM ZERO (#699).
// nodeDeclaredType leaves missing column declarations undecided (#333).
// Nested function callers must keep looking past a guessed type for a
// decided candidate (expr.Confidence, #331).
func DeclaredTypeOfNode(node plansql.Node, schema []parquet.Column) (expr.DeclType, expr.Confidence) {
	decls := colDecls{
		types:  make(map[string]parquet.TypeID, len(schema)),
		fields: map[string][]parquet.Column{},
		dec:    map[string]logical.DecimalMeta{},
	}
	for _, c := range schema {
		name := strings.ToLower(c.Name)
		decls.types[name] = c.Type
		if c.Type == parquet.TypeDecimal {
			decls.dec[name] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
		}
		if len(c.Fields) > 0 {
			decls.fields[name] = c.Fields
		}
	}
	return nodeDeclaredType(node, decls)
}

func nodeDeclaredType(node plansql.Node, decls colDecls) (expr.DeclType, expr.Confidence) {
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
		if !binOpInvolvesInterval(n) {
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
				return expr.Decl(parquet.TypeInt64), expr.Decided
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
				case parquet.TypeInt64, parquet.TypeInt32:
					return withExact(expr.Decl(parquet.TypeInt64)), c
				case parquet.TypeFloat64, parquet.TypeFloat32:
					return withExact(expr.Decl(parquet.TypeFloat64)), c
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
		if t, ok := decls.placeholderTypes[n.Name]; ok {
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
		if col.Type == parquet.TypeDecimal {
			// (p,s) or nothing: a DECIMAL declared without its scale builds a
			// vector that reads every value at the wrong power of ten.
			if col.Precision == 0 {
				return expr.DeclType{}, expr.Undecided
			}
			return expr.DeclDecimal(col.Precision, col.Scale), expr.Decided
		}
		return expr.Decl(col.Type), expr.Decided
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
	case *plansql.CastNode:
		// A DECIMAL destination carries its own (p,s), and a BARE one takes
		// the operand's — neither of which a plain TypeID can express, which
		// is why `CAST(x AS DECIMAL(10,2))` used to declare STRING and
		// `CAST(x AS DECIMAL)` FLOAT64 (ADR-0024 item 3, #555).
		if t, ok := castDeclaredDecimal(n, decls); ok {
			return t, expr.Decided
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
			// The literal declares INT64 or FLOAT64 on its own — ADR-0024's
			// recorded deferral, and `SELECT 1.5` is still a double — and
			// CARRIES the exact fixed-point (p,s) of its spelling beside it,
			// which is what a DECIMAL fold over it resolves against (#695).
			// `CASE … THEN d ELSE 0 END` is numeric in PostgreSQL and the
			// literal's `0` is DECIMAL(1,0) in that fold; a FLOAT COLUMN
			// beside the same DECIMAL carries no such (p,s) and keeps
			// PostgreSQL's float8.
			if _, err := strconv.ParseInt(n.Value, 10, 64); err == nil {
				return expr.DeclNumericLit(parquet.TypeInt64, n.Value), expr.Decided
			}
			return expr.DeclNumericLit(parquet.TypeFloat64, n.Value), expr.Decided
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
func caseDeclaredType(n *plansql.CaseNode, decls colDecls) (expr.DeclType, expr.Confidence) {
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
func stringOperand(n plansql.Node, decls colDecls) bool {
	d, c := nodeDeclaredType(n, decls)
	return c != expr.Undecided && d.ID == parquet.TypeString && !d.Quoted
}

func bytesOperand(n plansql.Node, decls colDecls) bool {
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
func bytesPreservingReturn(n *plansql.FuncCallNode, decls colDecls) (expr.DeclType, bool) {
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

func funcReturnType(n *plansql.FuncCallNode, decls colDecls) (expr.DeclType, expr.Confidence) {
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
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
		// map_keys() really does return an ARRAY, and the declaration says
		// so, but a projection has no element type to size the child vector
		// with and an ARRAY column built without one reads back empty. Keep
		// the string fallback until a projection can carry a nested type.
		return expr.DeclType{}, expr.Undecided
	}
	return t, c
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
		// Every integer spelling lands on INT64: the engine has no int16
		// and reads an INT32 column as an int64 everywhere else. The cast
		// evaluator still enforces each spelling's own RANGE (22003 past
		// it), which is the half that changes a value; the OID it reaches
		// a client under is int8 where PostgreSQL says int4/int2, recorded
		// in ADR-0012 item 12's divergence list.
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
	case "UUID":
		// The declaration half of #839. `CAST(x AS UUID)` declared STRING, so
		// the cast changed neither the value nor the type a client sees —
		// OID 25 for a column PostgreSQL declares 2950, and a driver that
		// branches on the OID (pgx's UUID scanner, pgJDBC's) never saw one.
		return parquet.TypeUUID
	default:
		// What is LEFT here is the destinations Cast.Eval does not implement
		// and passes its operand through — the network types, the containers,
		// DURATION, BYTES, VECTOR. A name that answers to NO type at all no
		// longer reaches this arm: expr.KnownCastDest refuses it at compile
		// with 42704, because declaring STRING for it made the two layers
		// agree with each other about a column PostgreSQL says cannot be
		// described (#652).
		return parquet.TypeString
	}
}

// binOpTemporalType types the two date-arithmetic shapes expr.BinOp evaluates,
// so the projection's output column can hold what the evaluator produces —
// the disagreement #340 is about, in the other direction.
//
//	date - date → BIGINT, a count of days
//	date ± n    → DATE, the day n days away
//
// Everything else declines and the caller's numeric/interval rules stand. In
// particular a TIMESTAMP operand declines: SQL calls that difference an
// INTERVAL and the engine has no interval column, so expr.BinOp.dateArith
// leaves it on the numeric path and this must agree.
func binOpTemporalType(n *plansql.BinaryOp, decls colDecls) (expr.DeclType, expr.Confidence) {
	if n.Op != "+" && n.Op != "-" {
		return expr.DeclType{}, expr.Undecided
	}
	lk := nodeTemporalKind(n.Left, decls)
	rk := nodeTemporalKind(n.Right, decls)
	switch {
	case n.Op == "-" && lk == temporalDay && rk == temporalDay:
		return expr.Decl(parquet.TypeInt64), expr.Decided
	case lk == temporalDay && nodeIsPlainNumber(n.Right):
		return expr.Decl(parquet.TypeDate), expr.Decided
	case rk == temporalDay && n.Op == "+" && nodeIsPlainNumber(n.Left):
		return expr.Decl(parquet.TypeDate), expr.Decided
	}
	return expr.DeclType{}, expr.Undecided
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

// nodeTemporalKind reports what kind of temporal value an operand carries: a
// CAST names one outright, and a column reference has one in the catalog.
func nodeTemporalKind(node plansql.Node, decls colDecls) temporalKind {
	var t parquet.TypeID
	switch n := node.(type) {
	case *plansql.ParenNode:
		return nodeTemporalKind(n.Inner, decls)
	case *plansql.CastNode:
		t = inferCastType(n.TypeName)
	case *plansql.ColRef:
		var ok bool
		if t, ok = decls.colType(n); !ok {
			return temporalNone
		}
		if t == parquet.TypeString {
			return temporalDay
		}
	default:
		return temporalNone
	}
	switch t {
	case parquet.TypeDate:
		return temporalDay
	case parquet.TypeTimestamp:
		// An instant difference is an INTERVAL in SQL and this engine has no
		// interval column to hold one, so expr.BinOp.dateArith declines it
		// and the caller's numeric rules stand.
		return temporalInstant
	}
	return temporalNone
}

// nodeIsPlainNumber reports whether an operand is a whole number written into
// the query — the `n` of `date ± n`. A column or a computed expression is
// deliberately excluded: its runtime value decides whether expr.BinOp takes
// the date branch at all, and a projection column typed DATE on a guess would
// print an integer difference as a date.
func nodeIsPlainNumber(node plansql.Node) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return nodeIsPlainNumber(n.Inner)
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return false
		}
		_, err := strconv.ParseInt(n.Value, 10, 64)
		return err == nil
	}
	return false
}

// binOpInvolvesInterval reports whether either operand of a BinaryOp is an
// IntervalLit or a date/timestamp function (current_date, current_timestamp).
// Date ± interval produces a date string, not a numeric value.
func binOpInvolvesInterval(b *plansql.BinaryOp) bool {
	return nodeIsDateOrInterval(b.Left) || nodeIsDateOrInterval(b.Right)
}

func nodeIsDateOrInterval(n plansql.Node) bool {
	switch v := n.(type) {
	case *plansql.IntervalLit:
		return true
	case *plansql.FuncCallNode:
		lower := strings.ToLower(v.Name)
		return lower == "current_date" || lower == "current_timestamp" ||
			lower == "current_time" || lower == "now" ||
			lower == "date_add" || lower == "date_sub"
	case *plansql.BinaryOp:
		// Nested: (CURRENT_DATE - INTERVAL '1' DAY) + INTERVAL '2' HOUR
		return binOpInvolvesInterval(v)
	default:
		return false
	}
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
func isPlainGroupKey(node plansql.Node, decls colDecls) bool {
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

// inferRenameExprDecl types an expression the GATHER will evaluate, against
// the scope the producer below the output projection emits.
//
// It is the same call attachScanSelectProjections makes for a SELECT item its
// own fragment computes — one rule for a computed column's type, whichever
// operator ends up computing it. A scope it cannot read leaves the rename
// undeclared, and the gather keeps the runtime detections it had.
func inferRenameExprDecl(astExpr plansql.Node, scope *logical.Node) (expr.DeclType, bool) {
	if astExpr == nil || scope == nil || len(scope.Children) != 1 {
		return expr.DeclType{}, false
	}
	child := scope.Children[0]
	// emittedColDecls, not inputColDecls. The scope is the node BELOW the
	// output projection, and what the gather's expression reads is what that
	// node EMITS: a WINDOW's `__win_N` slots, and an AGGREGATE's `__agg_N`
	// outputs. `inputColTypes` has a Window arm (#729) and NO Aggregate arm,
	// so the window half of this family was typed and the aggregate half fell
	// through to the STRING fallback — `emittedColTypes`' aggregate arm
	// already declares each output from `aggSpecOutputType`, which is the same
	// rule the stage's own AggSpec carries. One rule, both slot families.
	//
	// And the declaration is made only when the inference DECIDED. A fallback
	// is not a declaration: typed STRING and declared anyway,
	// `CASE WHEN MAX(id) > 0 THEN MAX(c_date) ELSE NULL END` built a String
	// vector and `SetValue` rendered the epoch day `16195` where PostgreSQL
	// and the single-process path say `2014-05-05`. An undecided rename keeps
	// `evalExprColumn`'s float64 arm — what it had before the declaration
	// existed — so a shape this walk cannot type is never made worse by it.
	d, conf := inferProjectionDeclTypeConf(astExpr, parquet.TypeString,
		strictIntArithCols(child), emittedColDecls(child))
	if conf == expr.Undecided {
		return expr.DeclType{}, false
	}
	return d, true
}
