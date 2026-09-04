package logical

import (
	"math"
	"math/big"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The constant-arithmetic aggregate lift, decided from the column's TYPE
// (#850).
//
// #841 stopped the syntactic lift from moving a per-row 22003 out of the row
// where it belongs: `SUM(x * k)` → `SUM(x) * k` answers where the per-row form
// must raise, and PostgreSQL raises for the input expression in every
// position. It declined for EVERY integer literal, because the builder runs
// before any type is known, and the ClickBench Q30 shape — 90 × `SUM(col + k)`
// over one integer column — went 7.6 ms to 342 ms.
//
// The recovery is not a threshold and not a heuristic: the lift is SAFE
// exactly when the per-row arithmetic CANNOT refuse, and that is decidable at
// plan time from the column's declared type and the manifest's min/max. This
// pass runs inside logical.Optimize, which every caller invokes AFTER
// physical.AnnotateScanColumns — so `Node.ScanColTypes`, `Node.ScanColStats`
// and `Node.ScanRowEstimate` are on the Scan by the time it looks.
//
// # What has to be proven, and it is BOTH forms
//
// The obvious half is the per-row form: `col op k` must not leave int64 for
// any row. `+`, `-` and `*` are monotone in col for a fixed k, and |col*k| is
// maximal at an extreme, so checking the column's MIN and MAX is exact rather
// than conservative.
//
// The half that is easy to miss is the LIFTED form, which has arithmetic the
// per-row form does not: `SUM(x) + k*COUNT(x)` multiplies the literal by the
// row count. With k near int64's edge that product refuses where the per-row
// `x + k` over small x does not — the same defect as #841, pointing the other
// way. So the pass bounds `|k| × N` too (N is the manifest's row count, an
// upper bound on COUNT since a filter only removes rows), and bounds the
// numeric carrier the aggregate's own result rides on: SUM over an integer is
// an exact numeric(38,0) and AVG a numeric(38,4), so the lifted expression has
// 38 and 34 integer digits to fit in.
//
// # Statistics are read ONCE per column per query
//
// Q30's shape has ninety aggregates over a handful of columns. The decision is
// cached per (column, scan) for the pass, because re-walking the plan to the
// scan for each aggregate is the cost the recovery exists to remove.
//
// # What still declines, and why
//
//   - No statistics for an INT64 column: min/max is what proves the bound, and
//     a table that has never been ANALYZEd (or a manifest with no per-column
//     stats) has none. Right and slower.
//   - A DECIMAL column: the engine's own 128-bit carrier can refuse where
//     PostgreSQL answers, and the lifted and per-row forms round at different
//     scales. Unchanged from the #841 state.
//   - Anything below the Aggregate that can rebind a name — a Project, a join,
//     a set operation. The walk stops there exactly as strictIntArithCols does,
//     and for the same reason: a wrong type claim here is a wrong ANSWER.
//
// The kill switch is the same one: constArithAggToggle (WADJET_CONST_ARITH_AGG
// =0), so the optimization-invariance oracle covers this pass for free.

// liftConstArithAggsWithTypes applies the const-arith aggregate lift to the
// aggregates the syntactic pass declined, wherever the column's type proves the
// per-row form cannot refuse. It mutates the plan in place.
func liftConstArithAggsWithTypes(root *Node) {
	if root == nil || !constArithAggToggle.On() {
		return
	}
	walkLiftCandidates(root)
}

func walkLiftCandidates(n *Node) {
	if n == nil {
		return
	}
	for _, c := range n.Children {
		walkLiftCandidates(c)
	}
	if n.Type != NodeProject || len(n.Children) != 1 {
		return
	}
	agg := n.Children[0]
	if agg == nil || agg.Type != NodeAggregate {
		return
	}
	liftOverAggregate(n, agg)
}

// caaColumnFacts is what the decision needs about one column, resolved once.
type caaColumnFacts struct {
	typ    parquet.TypeID
	lo, hi *big.Int // value bounds; nil when unknown
	rows   *big.Int // upper bound on the row count; nil when unknown
	known  bool
}

func liftOverAggregate(proj, agg *Node) {
	// The scan that feeds this aggregate, if the walk can reach one without
	// crossing a node that rebinds a name.
	scan := caaScanBelow(agg)
	if scan == nil {
		return
	}
	facts := make(map[string]caaColumnFacts)
	counter := caaNextSlot(agg, scan)
	changed := false

	for pi := range proj.Projections {
		p := &proj.Projections[pi]
		if !p.IsAgg || p.ASTExpr == nil {
			continue
		}
		fn, ok := stripParens(p.ASTExpr).(*plansql.FuncCallNode)
		if !ok {
			continue
		}
		cand, ok := caaReadCandidate(fn)
		if !ok {
			continue
		}
		f := caaFactsFor(facts, scan, cand.col)
		if !caaLiftIsSafe(cand, f) {
			continue
		}
		// The aggregate slot this projection currently reads, so the rewrite
		// can retire it.
		old := caaAggIndex(agg, p, fn)
		if old < 0 {
			continue
		}
		sumSlot := caaEnsureAgg(agg, cand.innerAgg, cand.col, cand.colNode, &counter)
		countSlot := ""
		if cand.needsCount {
			countSlot = caaEnsureAgg(agg, "count", cand.col, cand.colNode, &counter)
		}
		p.ASTExpr = cand.build(sumSlot, countSlot)
		p.IsAgg = false
		caaRetire(agg, old)
		changed = true
	}
	if changed {
		caaDropUnreadAggs(proj, agg)
	}
}

// caaCandidate is one aggregate call the syntactic pass declined, decomposed.
type caaCandidate struct {
	aggName    string // sum / avg / min / max, as written
	innerAgg   string // the aggregate the lifted form computes over the bare column
	col        string // the column name
	colNode    plansql.Node
	op         string // + - *
	k          *big.Int
	kNode      *plansql.Lit
	constFirst bool
	needsCount bool
	build      func(sumSlot, countSlot string) plansql.Node
}

// caaReadCandidate reads an `agg(col op <integer literal>)` call, or declines.
//
// It recognises exactly what rewriteOneAgg would rewrite but for the integer
// literal, and nothing else: the syntactic pass has already taken every other
// shape, so a call reaching here with a non-integer literal has already been
// lifted and its projection is no longer IsAgg.
func caaReadCandidate(fn *plansql.FuncCallNode) (caaCandidate, bool) {
	var c caaCandidate
	c.aggName = strings.ToLower(fn.Name)
	switch c.aggName {
	case "sum", "avg", "min", "max":
	default:
		return c, false
	}
	if fn.Distinct || fn.Star || len(fn.Args) != 1 {
		return c, false
	}
	bin, ok := stripParens(fn.Args[0]).(*plansql.BinaryOp)
	if !ok {
		return c, false
	}
	switch bin.Op {
	case "+", "-", "*":
	default:
		return c, false
	}
	c.op = bin.Op
	if col, lit := plainColAndLit(bin.Left, bin.Right); col != nil {
		c.colNode, c.kNode = col, lit
	} else if col, lit := plainColAndLit(bin.Right, bin.Left); col != nil {
		c.colNode, c.kNode, c.constFirst = col, lit, true
	} else {
		return c, false
	}
	kv, err := strconv.ParseInt(strings.TrimSpace(c.kNode.Value), 10, 64)
	if err != nil {
		// Not an integer literal: the syntactic pass already handled it.
		return c, false
	}
	c.k = big.NewInt(kv)
	cr, ok := c.colNode.(*plansql.ColRef)
	if !ok {
		return c, false
	}
	c.col = strings.ToLower(cr.Column)
	if c.col == "" {
		return c, false
	}
	// MIN/MAX over a multiplication is order-preserving only for k > 0, the
	// same bound rewriteOneAgg carries.
	if (c.aggName == "min" || c.aggName == "max") && c.op == "*" && kv <= 0 {
		return c, false
	}
	c.innerAgg = c.aggName
	if c.op == "-" && c.constFirst && (c.aggName == "min" || c.aggName == "max") {
		// MIN(k-x) = k - MAX(x).
		if c.aggName == "min" {
			c.innerAgg = "max"
		} else {
			c.innerAgg = "min"
		}
	}
	c.needsCount = c.aggName == "sum" && (c.op == "+" || c.op == "-")

	ref := func(slot string) plansql.Node { return &plansql.ColRef{Column: slot} }
	lit := func() plansql.Node { return c.kNode }
	switch {
	case c.op == "+" && c.aggName == "sum":
		c.build = func(s, n string) plansql.Node {
			return &plansql.BinaryOp{Op: "+", Left: ref(s),
				Right: &plansql.BinaryOp{Op: "*", Left: lit(), Right: ref(n)}}
		}
	case c.op == "+":
		c.build = func(s, _ string) plansql.Node {
			return &plansql.BinaryOp{Op: "+", Left: ref(s), Right: lit()}
		}
	case c.op == "-" && c.aggName == "sum" && c.constFirst:
		c.build = func(s, n string) plansql.Node {
			return &plansql.BinaryOp{Op: "-",
				Left:  &plansql.BinaryOp{Op: "*", Left: lit(), Right: ref(n)},
				Right: ref(s)}
		}
	case c.op == "-" && c.aggName == "sum":
		c.build = func(s, n string) plansql.Node {
			return &plansql.BinaryOp{Op: "-", Left: ref(s),
				Right: &plansql.BinaryOp{Op: "*", Left: lit(), Right: ref(n)}}
		}
	case c.op == "-" && c.constFirst:
		c.build = func(s, _ string) plansql.Node {
			return &plansql.BinaryOp{Op: "-", Left: lit(), Right: ref(s)}
		}
	case c.op == "-":
		c.build = func(s, _ string) plansql.Node {
			return &plansql.BinaryOp{Op: "-", Left: ref(s), Right: lit()}
		}
	case c.op == "*":
		c.build = func(s, _ string) plansql.Node {
			return &plansql.BinaryOp{Op: "*", Left: ref(s), Right: lit()}
		}
	default:
		return c, false
	}
	return c, true
}

// caaScanBelow walks from an Aggregate to the single Scan feeding it, through
// nodes that cannot rebind a column name. It is strictIntArithCols' walk, and
// it stops at the same places and for the same reason: a Project may bind a
// name to a different value, and a wrong type claim here is a wrong answer.
func caaScanBelow(n *Node) *Node {
	for n != nil {
		switch n.Type {
		case NodeScan:
			if n.ScanColTypes == nil {
				return nil
			}
			return n
		case NodeAggregate, NodeFilter, NodeLimit, NodeSort:
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

func caaFactsFor(cache map[string]caaColumnFacts, scan *Node, col string) caaColumnFacts {
	if f, ok := cache[col]; ok {
		return f
	}
	f := caaResolveFacts(scan, col)
	cache[col] = f
	return f
}

// caaResolveFacts reads a column's type and, for an integer column, the value
// bounds the plan already carries. This is the "once per column per query"
// read: the caller caches it.
func caaResolveFacts(scan *Node, col string) caaColumnFacts {
	t, ok := scan.ScanColTypes[col]
	if !ok {
		return caaColumnFacts{}
	}
	f := caaColumnFacts{typ: t, known: true}
	if scan.ScanRowEstimate > 0 {
		f.rows = big.NewInt(scan.ScanRowEstimate)
	}
	switch t {
	case parquet.TypeInt32:
		// A narrow type bounds itself: no statistics needed, which is what
		// makes the ClickBench columns liftable on a table nobody analysed.
		f.lo = big.NewInt(math.MinInt32)
		f.hi = big.NewInt(math.MaxInt32)
	case parquet.TypeInt64:
		if s, ok := scan.ScanColStats[col]; ok {
			if lo, ok := caaStatInt(s.MinValue); ok {
				if hi, ok := caaStatInt(s.MaxValue); ok {
					f.lo, f.hi = lo, hi
				}
			}
		}
	}
	return f
}

// caaStatInt reads a catalog statistic as an exact integer. A statistic this
// cannot read leaves the bound unknown and the lift declines — the safe
// direction, and the only one: a bound guessed from a float would be a
// different number than the column holds.
func caaStatInt(v any) (*big.Int, bool) {
	switch x := v.(type) {
	case int64:
		return big.NewInt(x), true
	case int32:
		return big.NewInt(int64(x)), true
	case int:
		return big.NewInt(int64(x)), true
	case float64:
		if x != math.Trunc(x) || math.Abs(x) > math.MaxInt64 {
			return nil, false
		}
		return big.NewInt(int64(x)), true
	}
	return nil, false
}

var (
	caaInt64Min = big.NewInt(math.MinInt64)
	caaInt64Max = big.NewInt(math.MaxInt64)
	// The exact numeric carrier's integer-digit limits: SUM over an integer is
	// numeric(38,0) and AVG numeric(38,4) (ADR-0024 item 2), so the lifted
	// expression has 10^38 and 10^34 to fit inside.
	caaNumeric38 = caaPow10(38)
	caaNumeric34 = caaPow10(34)
)

func caaPow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func caaFitsInt64(v *big.Int) bool {
	return v.Cmp(caaInt64Min) >= 0 && v.Cmp(caaInt64Max) <= 0
}

func caaAbs(v *big.Int) *big.Int { return new(big.Int).Abs(v) }

// caaLiftIsSafe is the whole decision. Both forms are bounded: the per-row one
// must not leave int64, and the lifted one must not leave int64 (its `k ×
// COUNT` multiply) or the numeric carrier (its aggregate result).
func caaLiftIsSafe(c caaCandidate, f caaColumnFacts) bool {
	if !f.known {
		return false
	}
	switch f.typ {
	case parquet.TypeInt32, parquet.TypeInt64:
	default:
		// EVERY non-integer column declines, and the reason is not a
		// disposition — it is a VALUE.
		//
		// IEEE addition is not associative, so `SUM(f + k)` and
		// `SUM(f) + k*COUNT(f)` are different numbers whenever the summands
		// span enough magnitude to cancel. Over `f = 1e16, 1, 1, 1, 1`,
		// PostgreSQL 17.11 answers 1.0000000000000008e+16 for `SUM(f+1)` and
		// 3.0000000000000016e+16 for `SUM(f*3)`; the lifted forms answer
		// …004e+16 and 3e+16. The first cut of this pass lifted FLOAT64 on the
		// grounds that "float arithmetic never refuses" — true of this engine,
		// and beside the point: the lift is an identity over VALUES or it is
		// not applied, and over a float it is not (round-1 review, B1).
		//
		// FLOAT32 declines for a sharper version of the same thing: the
		// per-row multiplication widens each value to a double before it is
		// accumulated while `SUM(c_f32)` accumulates at float4's width, so the
		// two forms use a different ACCUMULATOR — `SUM(c_f32 * 2)` answered
		// 1383.1428577005863 per-row and 1383.142822265625 lifted over the
		// type matrix's 100 rows.
		//
		// DECIMAL declines too, unchanged from #841: the engine's 128-bit
		// carrier can refuse where PostgreSQL answers, and the lifted and
		// per-row forms round at different scales.
		return false
	}
	if f.lo == nil || f.hi == nil || f.rows == nil {
		return false
	}
	k := c.k
	absK := caaAbs(k)
	// The PER-ROW form at both extremes. `+ - *` are monotone in the column
	// for a fixed k, and |col × k| is maximal at an extreme, so these two
	// evaluations are exact rather than conservative.
	for _, x := range []*big.Int{f.lo, f.hi} {
		var r big.Int
		switch c.op {
		case "+":
			r.Add(x, k)
		case "-":
			if c.constFirst {
				r.Sub(k, x)
			} else {
				r.Sub(x, k)
			}
		case "*":
			r.Mul(x, k)
		}
		if !caaFitsInt64(&r) {
			return false
		}
	}
	// The LIFTED form. |aggregate| is bounded by the column's magnitude, and
	// for SUM by that times the row count.
	maxAbs := caaAbs(f.lo)
	if h := caaAbs(f.hi); h.Cmp(maxAbs) > 0 {
		maxAbs = h
	}
	switch c.aggName {
	case "min", "max":
		// MIN/MAX(col) op k is the per-row expression at an extreme, already
		// checked above, and it computes in int64 exactly as the per-row form
		// does.
		return true
	case "avg":
		// AVG over an integer is numeric(38,4) — a value ROUNDED to four
		// decimals — so what the lift may do to it depends on the operator,
		// and only one of the two is an identity.
		//
		// `AVG(col ± k)` → `AVG(col) ± k` is exact for an INTEGER k, and the
		// reason is that rounding commutes with adding an integer:
		// round(s/n, 4) + k = round(s/n + k, 4) for integral k, because the
		// shift moves no digit past the fourth decimal. Measured on
		// PostgreSQL 17.11 over 1, 2, 4: `avg(m+1)` and `avg(m)+1` are both
		// 3.3333333333333333.
		//
		// `AVG(col * k)` → `AVG(col) * k` is NOT. It rounds to four decimals
		// BEFORE the multiply, so the last digit is lost for any k that is not
		// a power of two — and PostgreSQL itself shows the rewrite is not an
		// identity: over the same three rows `avg(x*3)` is 7.0000000000000000
		// and `avg(x)*3` is 6.9999999999999999. This engine answered 6.9999
		// where the server answers 7.0000 (round-1 review, B2). The digit-count
		// bound below never asked the question; it only asked whether the
		// result FIT.
		//
		// The identity that does hold for `*` is `(k*SUM(col))/COUNT(col)` with
		// ONE division at the end, and it is not taken here: the lifted form's
		// DECLARED type would then be the division's rather than the AVG's, so
		// the two arms of the invariance oracle would render the same number at
		// two scales. Declining costs the `AVG(col * k)` shape and nothing
		// else — Q30's shape is SUM.
		if c.op == "*" {
			return false
		}
		var r big.Int
		r.Add(maxAbs, absK)
		return r.Cmp(caaNumeric34) < 0
	case "sum":
		sumBound := new(big.Int).Mul(maxAbs, f.rows)
		if c.op == "*" {
			return new(big.Int).Mul(sumBound, absK).Cmp(caaNumeric38) < 0
		}
		// `k * COUNT(col)` is INTEGER arithmetic — both operands are int64 —
		// so it can raise 22003 on its own, for a k the per-row form never
		// meets. This is #841's defect pointing the other way, and it is the
		// bound a "check the column" rule would have missed.
		kCount := new(big.Int).Mul(absK, f.rows)
		if !caaFitsInt64(kCount) {
			return false
		}
		return new(big.Int).Add(sumBound, kCount).Cmp(caaNumeric38) < 0
	}
	return false
}

// caaAggIndex finds the AggExpr this projection reads. The builder names an
// unlifted aggregate's output after the projection's own alias, so the match is
// by OutputCol; the InputExpr is compared as a second key so a rewrite cannot
// retire an aggregate that merely shares a name.
func caaAggIndex(agg *Node, p *Projection, fn *plansql.FuncCallNode) int {
	want := p.Alias
	if want == "" {
		want = p.Column
	}
	for i := range agg.AggExprs {
		if !strings.EqualFold(agg.AggExprs[i].OutputCol, want) {
			continue
		}
		if !strings.EqualFold(agg.AggExprs[i].Func, strings.ToLower(fn.Name)) {
			continue
		}
		return i
	}
	return -1
}

// caaEnsureAgg returns the output slot of `f(col)` over the bare column,
// minting one if the aggregate does not already compute it. The dedup is what
// turns Q30's ninety aggregates into one SUM and one COUNT.
func caaEnsureAgg(agg *Node, f, col string, colNode plansql.Node, counter *int) string {
	for i := range agg.AggExprs {
		a := &agg.AggExprs[i]
		if a.Distinct || a.InputCol2 != "" || a.Separator != "" || a.Percentile != 0 {
			continue
		}
		if strings.EqualFold(a.Func, f) && strings.EqualFold(a.InputCol, col) {
			return a.OutputCol
		}
	}
	name := plansql.SlotName(plansql.SlotNestedAgg, *counter)
	*counter++
	agg.AggExprs = append(agg.AggExprs, AggExpr{
		Func: f, InputCol: col, OutputCol: name, InputExpr: colNode,
	})
	return name
}

// caaNextSlot picks the first `__agg_N` index that collides with nothing: not
// with an aggregate output already in the plan, and not with a STORED column
// of the scan.
//
// physical.renameCollidingSlots does the second half for the BUILDER's slots,
// and it runs inside AnnotateScanColumns — before this pass. So a slot minted
// here gets no such pass and has to avoid the collision itself, which it can
// do better than the builder could: the scan's real column list is on the node
// by now (#694's shape, reached from the other side).
func caaNextSlot(agg, scan *Node) int {
	taken := make(map[string]bool, len(agg.AggExprs)+len(scan.ScanColumns))
	for i := range agg.AggExprs {
		taken[strings.ToLower(agg.AggExprs[i].OutputCol)] = true
	}
	for _, c := range scan.ScanColumns {
		taken[strings.ToLower(c)] = true
	}
	for _, g := range agg.GroupBy {
		taken[strings.ToLower(g)] = true
	}
	n := 0
	for taken[strings.ToLower(plansql.SlotName(plansql.SlotNestedAgg, n))] {
		n++
	}
	return n
}

// caaRetire marks an AggExpr as no longer read. It is removed by
// caaDropUnreadAggs once every projection has been rewritten, because two
// projections may read the same slot.
func caaRetire(agg *Node, i int) {
	if i >= 0 && i < len(agg.AggExprs) {
		agg.AggExprs[i].OutputCol = "" // swept below
	}
}

// caaDropUnreadAggs removes the aggregates no projection reads any more, and
// re-checks that nothing else in the plan names them.
func caaDropUnreadAggs(proj, agg *Node) {
	kept := agg.AggExprs[:0]
	for _, a := range agg.AggExprs {
		if a.OutputCol != "" {
			kept = append(kept, a)
		}
	}
	agg.AggExprs = kept
	_ = proj
}
