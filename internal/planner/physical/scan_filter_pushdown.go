package physical

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Scan-level filter pushdown (see scan/row_filter.go): eligible
// `col <op> literal` conjuncts of a Filter directly over a catalog scan
// are evaluated by the scan itself — dictionary-mask per chunk instead of
// gather+compare per row — and filter-only columns stop being
// materialized. The exec filter keeps only the residual conjuncts.
//
// Eligibility is deliberately strict (semantics parity with the
// expression layer): int-class columns take integral literals, string
// columns take string literals, DATE columns take parseable date strings
// (coerced to day numbers). Floats, timestamps-as-strings, and anything
// with coercion ambiguity stay in the exec filter. Local planning path
// only. Kill switch: WADJET_SCAN_FILTER=0.

// ScanFilterPushdowns counts filters (conjuncts) pushed into scans.
var ScanFilterPushdowns atomic.Int64

// ShapeOnlyColumnsPlanned counts columns handed to the scan for lengths-only
// decode. The optimization-invariance oracle asserts the corpus engages it.
var ShapeOnlyColumnsPlanned atomic.Int64

var scanFilterToggle = optswitch.Register("scan-filter", "WADJET_SCAN_FILTER",
	"scan-level filter pushdown: dictionary-mask evaluation, filter-only column elision, count-only batches")

// tryPushFilterIntoScan attempts the pushdown for one Filter node whose
// built child is css. It returns the predicates the exec filter must
// still evaluate (possibly the originals, untouched).
func (p *Planner) tryPushFilterIntoScan(ctx context.Context, node *logical.Node, css *catalogScanSource) []logical.Predicate {
	orig := node.Predicates
	if !scanFilterToggle.On() || css.cache != nil ||
		p.StreamingSources != nil || p.MaterializedInputs != nil || p.ScanFileFilter != nil {
		return orig
	}
	scanNode := node.Children[0]
	if scanNode.Type != logical.NodeScan || scanNode.IsTableFunc || scanNode.SampleMethod != "" {
		return orig
	}
	meta, err := p.catalog.GetTable(ctx, scanNode.TableName)
	if err != nil {
		return orig
	}
	// Nested schemas take the row-based fallback scan, which never
	// evaluates rowPreds — pushing there would silently DROP the filter.
	if (&parquet.Schema{Columns: meta.Schema.Columns}).HasNestedColumns() {
		return orig
	}
	colType := make(map[string]parquet.TypeID, len(meta.Schema.Columns))
	canon := make(map[string]string, len(meta.Schema.Columns))
	for _, c := range meta.Schema.Columns {
		colType[strings.ToLower(c.Name)] = c.Type
		canon[strings.ToLower(c.Name)] = c.Name
	}

	var pushed []scan.RowPred
	var pushedCols []string
	var residual []logical.Predicate
	for _, pred := range orig {
		if pred.ASTExpr == nil {
			residual = append(residual, pred)
			continue
		}
		structured, rest := logical.SplitConjunctsForPushdown(pred.ASTExpr)
		for _, c := range rest {
			// LIKE conjuncts push as pattern predicates when the pattern
			// column is filter-only, so the pushdown ELIDES its
			// materialization (TPC-H Q13's `o_comment NOT LIKE` class).
			// A selected pattern column pays a double page read — the
			// scan filter walks its pages AND the decode materializes
			// them; metal measured that as a straight regression when
			// materialization was full (ClickBench Q23 +31%, Q24 +28%;
			// 2026-08-17 validation). Under sel-aware decode (#299) the
			// second pass copies only selected rows — and the selection
			// the pushdown produces prunes EVERY other byte-array column
			// in the read schema — so the gate lifts with it.
			if rp, name, ok := makeLikeRowPred(canon, colType, c); ok &&
				(likeColFilterOnly(scanNode, name) || scan.SelDecodeOn()) {
				pushed = append(pushed, rp)
				pushedCols = append(pushedCols, name)
				continue
			}
			residual = append(residual, logical.Predicate{Raw: c.String(), ASTExpr: c})
		}
		for _, sc := range structured {
			name, ok := canon[strings.ToLower(sc.Column)]
			if !ok {
				residual = append(residual, logical.Predicate{Raw: sc.Raw, ASTExpr: sc.ASTExpr})
				continue
			}
			rp, ok := makeRowPred(name, colType[strings.ToLower(sc.Column)], sc)
			if !ok {
				residual = append(residual, logical.Predicate{Raw: sc.Raw, ASTExpr: sc.ASTExpr})
				continue
			}
			pushed = append(pushed, rp)
			pushedCols = append(pushedCols, name)
		}
	}
	if len(pushed) == 0 {
		return orig
	}
	css.rowPreds = pushed
	ScanFilterPushdowns.Add(int64(len(pushed)))

	// Drop filter-only columns from materialization when the residual no
	// longer references them: the scan filter reads their pages itself.
	if len(scanNode.FilterOnlyColumns) > 0 && len(css.requiredCols) > 0 {
		residualRefs := make(map[string]bool, 4)
		for _, r := range residual {
			if r.ASTExpr != nil {
				collectASTCols(r.ASTExpr, residualRefs)
			}
		}
		filterOnly := make(map[string]bool, len(scanNode.FilterOnlyColumns))
		for _, c := range scanNode.FilterOnlyColumns {
			filterOnly[strings.ToLower(c)] = true
		}
		droppable := make(map[string]bool, len(pushedCols))
		for _, c := range pushedCols {
			lc := strings.ToLower(c)
			if filterOnly[lc] && !residualRefs[lc] {
				droppable[lc] = true
			}
		}
		if len(droppable) > 0 {
			kept := css.requiredCols[:0:0]
			for _, c := range css.requiredCols {
				if !droppable[strings.ToLower(c)] {
					kept = append(kept, c)
				}
			}
			if len(kept) == 0 {
				// Nothing left to materialize — the row-count sentinel
				// keeps batch lengths flowing (narrowest-column decode).
				kept = []string{logical.RowCountOnlyColumn}
			}
			css.requiredCols = kept
		}
	}
	return residual
}

// likeColFilterOnly reports whether the scan marks col as referenced by
// the filter and nothing above it — the eligibility condition for LIKE
// pushdown to pay (the drop pass then removes it from materialization).
func likeColFilterOnly(scanNode *logical.Node, col string) bool {
	for _, c := range scanNode.FilterOnlyColumns {
		if strings.EqualFold(c, col) {
			return true
		}
	}
	return false
}

// makeLikeRowPred recognizes a `col [NOT] LIKE 'literal'` conjunct over a
// string column and returns its pattern RowPred. Anything else — computed
// left sides, non-literal patterns, non-string columns — declines.
func makeLikeRowPred(canon map[string]string, colType map[string]parquet.TypeID, c plansql.Node) (scan.RowPred, string, bool) {
	n := c
	for {
		if pn, ok := n.(*plansql.ParenNode); ok {
			n = pn.Inner
			continue
		}
		break
	}
	le, ok := n.(*plansql.LikeExpr)
	if !ok {
		return scan.RowPred{}, "", false
	}
	cr, ok := le.Left.(*plansql.ColRef)
	if !ok {
		return scan.RowPred{}, "", false
	}
	lit, ok := le.Pattern.(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return scan.RowPred{}, "", false
	}
	name, ok := canon[strings.ToLower(cr.Column)]
	if !ok {
		return scan.RowPred{}, "", false
	}
	switch colType[strings.ToLower(cr.Column)] {
	case parquet.TypeString, parquet.TypeBytes:
	default:
		return scan.RowPred{}, "", false
	}
	op := scan.OpLike
	if le.Not {
		op = scan.OpNotLike
	}
	return scan.RowPred{Col: name, Op: op, Value: lit.Value}, name, true
}

// makeRowPred normalizes one structured conjunct into a scan.RowPred,
// declining anything whose comparison semantics would not be exact.
func makeRowPred(colName string, typ parquet.TypeID, sc logical.Predicate) (scan.RowPred, bool) {
	switch sc.Op {
	case "=", "!=", "<", "<=", ">", ">=":
	default:
		return scan.RowPred{}, false
	}
	switch typ {
	case parquet.TypeInt64, parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		if v, ok := sc.Value.(int64); ok {
			return scan.RowPred{Col: colName, Op: sc.Op, Value: v}, true
		}
	case parquet.TypeDate:
		if str, ok := sc.Value.(string); ok {
			if ts, err := time.Parse("2006-01-02", str); err == nil {
				return scan.RowPred{Col: colName, Op: sc.Op, Value: ts.Unix() / 86400}, true
			}
		}
		if v, ok := sc.Value.(int64); ok {
			return scan.RowPred{Col: colName, Op: sc.Op, Value: v}, true
		}
	case parquet.TypeString:
		if v, ok := sc.Value.(string); ok {
			return scan.RowPred{Col: colName, Op: sc.Op, Value: v}, true
		}
	case parquet.TypeBytes:
		// The literal is read by byteain, PostgreSQL's own bytea input
		// function, so `b = '\x6869'` pushes down the two bytes it names and
		// not the six characters of its spelling (#582). The two runtime arms
		// (kernel.toBytesString, exec.bytesFilterVal) read the same function,
		// so the three sites cannot disagree about what the literal is.
		if v, ok := sc.Value.(string); ok {
			return scan.RowPred{Col: colName, Op: sc.Op, Value: kernel.ByteaConstText(v)}, true
		}
	}
	return scan.RowPred{}, false
}

// collectASTCols gathers lowercase column names referenced by an AST.
func collectASTCols(n plansql.Node, out map[string]bool) {
	switch t := n.(type) {
	case *plansql.ColRef:
		out[strings.ToLower(t.Column)] = true
	case *plansql.CmpExpr:
		collectASTCols(t.Left, out)
		collectASTCols(t.Right, out)
	case *plansql.BinaryOp:
		collectASTCols(t.Left, out)
		collectASTCols(t.Right, out)
	case *plansql.AndNode:
		collectASTCols(t.Left, out)
		collectASTCols(t.Right, out)
	case *plansql.OrNode:
		collectASTCols(t.Left, out)
		collectASTCols(t.Right, out)
	case *plansql.NotNode:
		collectASTCols(t.Inner, out)
	case *plansql.ParenNode:
		collectASTCols(t.Inner, out)
	case *plansql.UnaryOp:
		collectASTCols(t.Inner, out)
	case *plansql.LikeExpr:
		collectASTCols(t.Left, out)
		collectASTCols(t.Pattern, out)
	case *plansql.InExpr:
		collectASTCols(t.Left, out)
		for _, v := range t.Values {
			collectASTCols(v, out)
		}
	case *plansql.BetweenExpr:
		collectASTCols(t.Left, out)
		collectASTCols(t.Low, out)
		collectASTCols(t.High, out)
	case *plansql.IsExpr:
		collectASTCols(t.Left, out)
	case *plansql.FuncCallNode:
		for _, a := range t.Args {
			collectASTCols(a, out)
		}
	case *plansql.CaseNode:
		if t.Subject != nil {
			collectASTCols(t.Subject, out)
		}
		for _, w := range t.Whens {
			collectASTCols(w.Cond, out)
			collectASTCols(w.Result, out)
		}
		if t.Else != nil {
			collectASTCols(t.Else, out)
		}
	case *plansql.CastNode:
		collectASTCols(t.Inner, out)
	// A residual conjunct holding a correlated subquery reads the outer
	// columns that subquery correlates on, per row, out of the batch. Missing
	// them here lets the drop pass below strip a column another conjunct
	// happened to push into the scan filter — the same disappearing
	// correlated reference as #347, one layer down.
	case *plansql.SubqueryNode:
		for _, c := range plansql.OuterColumnCandidates(t.SQL) {
			out[c] = true
		}
	case *plansql.ExistsNode:
		for _, c := range plansql.OuterColumnCandidates(t.SQL) {
			out[c] = true
		}
	case *plansql.AnyAllExpr:
		collectASTCols(t.Left, out)
		for _, v := range t.Values {
			collectASTCols(v, out)
		}
	case *plansql.TupleNode:
		for _, e := range t.Elements {
			collectASTCols(e, out)
		}
	}
}
