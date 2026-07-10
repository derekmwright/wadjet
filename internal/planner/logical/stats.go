package logical

import (
	"math"
	"strings"
)

// RelStatsOf returns CBO-derived statistics for the given subtree.
// Exported so the physical planner can consult cardinality-driven
// estimates when making structural decisions (broadcast vs shuffle,
// dynamic-filter eligibility, etc.) without re-implementing the
// recursion.
func RelStatsOf(n *Node) RelStats {
	return estimateSubtreeStats(n)
}

// RelStats holds estimated statistics for a plan subtree.
type RelStats struct {
	Rows   float64
	ColNDV map[string]float64 // lowercase unqualified column name → estimated NDV
	// ColHist carries opaque histogram pointers (*catalog.Histogram) for
	// columns that have one in the catalog. Used by estimatePredSelectivity
	// to compute range/equality selectivity from real data distributions.
	// Stored as any to keep the logical package free of catalog imports.
	ColHist map[string]any
}

// estimateSubtreeStats estimates cardinality and per-column NDV for a plan subtree.
func estimateSubtreeStats(n *Node) RelStats {
	if n == nil {
		return RelStats{Rows: 1, ColNDV: map[string]float64{}}
	}

	switch n.Type {
	case NodeScan:
		return estimateScanStats(n)

	case NodeFilter:
		child := estimateSubtreeStats(n.Children[0])
		sel := estimatePredSelectivityWithHist(n.Predicates, child.ColHist)
		rows := math.Max(1, child.Rows*sel)
		return RelStats{Rows: rows, ColNDV: scaleNDVs(child.ColNDV, rows), ColHist: child.ColHist}

	case NodeAggregate:
		child := estimateSubtreeStats(n.Children[0])
		if len(n.GroupBy) == 0 {
			return RelStats{Rows: 1, ColNDV: map[string]float64{}}
		}
		groups := 1.0
		for _, g := range n.GroupBy {
			col := strings.ToLower(stripQualifier(g))
			if ndv, ok := child.ColNDV[col]; ok {
				groups *= ndv
			} else {
				groups *= math.Sqrt(child.Rows)
			}
		}
		rows := math.Min(child.Rows, groups)
		return RelStats{Rows: math.Max(1, rows), ColNDV: scaleNDVs(child.ColNDV, rows)}

	case NodeJoin:
		if len(n.Children) < 2 {
			return RelStats{Rows: 1000, ColNDV: map[string]float64{}}
		}
		left := estimateSubtreeStats(n.Children[0])
		right := estimateSubtreeStats(n.Children[1])
		return estimateJoinStats(left, right, n.JoinCond, n.JoinType)

	case NodeLimit:
		child := estimateSubtreeStats(n.Children[0])
		if n.LimitVal > 0 && float64(n.LimitVal) < child.Rows {
			rows := float64(n.LimitVal)
			return RelStats{Rows: rows, ColNDV: scaleNDVs(child.ColNDV, rows)}
		}
		return child

	case NodeDistinct:
		child := estimateSubtreeStats(n.Children[0])
		rows := child.Rows
		for _, ndv := range child.ColNDV {
			if ndv < rows {
				rows = ndv
			}
		}
		return RelStats{Rows: math.Max(1, rows), ColNDV: child.ColNDV}

	case NodeProject:
		if len(n.Children) > 0 {
			return estimateSubtreeStats(n.Children[0])
		}
		return RelStats{Rows: 1, ColNDV: map[string]float64{}}

	default:
		if len(n.Children) > 0 {
			return estimateSubtreeStats(n.Children[0])
		}
		return RelStats{Rows: 1000, ColNDV: map[string]float64{}}
	}
}

// estimateScanStats estimates statistics for a base table scan.
func estimateScanStats(n *Node) RelStats {
	rows := float64(n.ScanRowEstimate)
	if rows <= 0 {
		rows = 10000 // unknown table fallback
	}

	// Apply scan-level predicate selectivity
	scanSel := 1.0
	for range n.ScanPredicates {
		scanSel *= 0.33
	}
	for range n.PartitionFilter {
		scanSel *= 0.25
	}
	rows *= math.Max(scanSel, 0.0001)

	ndv := make(map[string]float64, len(n.ScanColumns))
	hist := make(map[string]any, len(n.ScanColumns))
	for _, col := range n.ScanColumns {
		lc := strings.ToLower(col)
		if cs, ok := n.ScanColStats[lc]; ok && cs.MinValue != nil && cs.MaxValue != nil {
			ndv[lc] = estimateNDVFromStats(cs, rows)
		} else if cs, ok := n.ScanColStats[lc]; ok && cs.NDV > 0 {
			ndv[lc] = math.Min(float64(cs.NDV), rows)
		} else {
			ndv[lc] = estimateColumnNDV(col, rows)
		}
		if cs, ok := n.ScanColStats[lc]; ok && cs.Histogram != nil {
			hist[lc] = cs.Histogram
		}
	}

	return RelStats{Rows: math.Max(1, rows), ColNDV: ndv, ColHist: hist}
}

// estimateNDVFromStats estimates distinct values using catalog statistics.
//
// Preference order:
//  1. Catalog HLL NDV when populated (CBO Phase 2). Ground-truth-derived,
//     ~1% standard error. Computed at ingest time or by ANALYZE TABLE.
//  2. min/max range for numeric types (max-min+1, capped at row count).
//     Overstates for sparse keys but cheap to compute.
//  3. String / non-numeric: moderate-cardinality heuristic.
func estimateNDVFromStats(cs ScanColumnStats, tableRows float64) float64 {
	if cs.NDV > 0 {
		return math.Min(float64(cs.NDV), tableRows)
	}
	minF, minOk := toFloat(cs.MinValue)
	maxF, maxOk := toFloat(cs.MaxValue)
	if minOk && maxOk {
		rangeNDV := maxF - minF + 1
		if rangeNDV < 1 {
			rangeNDV = 1
		}
		return math.Min(rangeNDV, tableRows)
	}
	// String or non-numeric: assume moderate cardinality
	return math.Min(tableRows, math.Max(10, math.Sqrt(tableRows)*10))
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}

// estimateColumnNDV estimates distinct values for a column based on naming heuristics.
func estimateColumnNDV(colName string, tableRows float64) float64 {
	name := strings.ToLower(colName)

	// Key/ID columns — primary or foreign keys, typically unique
	if strings.HasSuffix(name, "key") || strings.HasSuffix(name, "_id") || name == "id" {
		return tableRows
	}

	// Date columns — bounded cardinality
	if strings.Contains(name, "date") {
		return math.Min(tableRows, 2557) // ~7 years of days
	}

	// Status / type / flag / priority — low cardinality
	for _, token := range []string{"status", "type", "flag", "priority", "mktsegment", "orderpriority", "shipmode", "returnflag", "linestatus", "orderstatus"} {
		if strings.Contains(name, token) || name == token {
			return math.Min(tableRows, 10)
		}
	}

	// Name columns — moderate-high cardinality
	if strings.Contains(name, "name") {
		return math.Min(tableRows, tableRows*0.8)
	}

	// Comment / address / phone — high cardinality
	if strings.Contains(name, "comment") || strings.Contains(name, "address") || strings.Contains(name, "phone") {
		return tableRows * 0.95
	}

	// Numeric amounts, balances, prices — moderate
	if strings.Contains(name, "price") || strings.Contains(name, "cost") ||
		strings.Contains(name, "balance") || strings.Contains(name, "quantity") ||
		strings.Contains(name, "discount") || strings.Contains(name, "tax") {
		return math.Min(tableRows, math.Max(100, tableRows*0.1))
	}

	// Default: moderate cardinality
	return math.Min(tableRows, math.Max(10, math.Sqrt(tableRows)*10))
}

// estimatePredSelectivityWithHist computes combined selectivity, using
// per-column histograms (when present) for range and equality
// predicates. Falls back to estimatePredSelectivity's hardcoded
// fractions when no histogram exists for the predicate's column.
//
// The histograms come from RelStats.ColHist, which the Scan case in
// estimateSubtreeStats populates from the catalog's TableColumnStats.
// They're opaque (any) at this layer to avoid a logical→catalog
// import; callers cast via the histogramSelector indirection.
func estimatePredSelectivityWithHist(preds []Predicate, colHist map[string]any) float64 {
	sel := 1.0
	for _, p := range preds {
		s := histPredSelectivity(p, colHist)
		if s < 0 {
			s = singlePredSelectivity(p)
		}
		sel *= s
	}
	return math.Max(sel, 0.0001)
}

// HistogramStats is the interface a per-column histogram must implement
// to participate in selectivity estimation. catalog.Histogram satisfies
// it. Kept as an interface so the logical package stays free of a
// catalog dependency.
type HistogramStats interface {
	SelectivityLE(v any) float64
	SelectivityLT(v any) float64
	SelectivityRange(lo, hi any) float64
	SelectivityEQ(v any) float64
}

// histPredSelectivity returns the histogram-driven selectivity for a
// single predicate, or -1 if no usable histogram or unsupported op.
func histPredSelectivity(p Predicate, colHist map[string]any) float64 {
	if colHist == nil || p.Column == "" || p.Value == nil {
		return -1
	}
	h, ok := colHist[strings.ToLower(p.Column)].(HistogramStats)
	if !ok || h == nil {
		return -1
	}
	switch strings.ToLower(p.Op) {
	case "=", "eq":
		return clamp01(h.SelectivityEQ(p.Value))
	case "<", "lt":
		return clamp01(h.SelectivityLT(p.Value))
	case "<=", "le":
		return clamp01(h.SelectivityLE(p.Value))
	case ">", "gt":
		return clamp01(1 - h.SelectivityLE(p.Value))
	case ">=", "ge":
		return clamp01(1 - h.SelectivityLT(p.Value))
	case "in":
		// Sum of per-value EQ selectivities, capped at 1.0. Works when
		// Value is a slice; otherwise fall back.
		switch vs := p.Value.(type) {
		case []any:
			var s float64
			for _, v := range vs {
				s += h.SelectivityEQ(v)
			}
			return clamp01(s)
		}
		return -1
	case "between":
		// Value is typically a 2-tuple of (lo, hi). Fall back if not.
		switch vs := p.Value.(type) {
		case []any:
			if len(vs) == 2 {
				return clamp01(h.SelectivityRange(vs[0], vs[1]))
			}
		}
		return -1
	}
	return -1
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// singlePredSelectivity is the per-predicate fallback when no
// histogram is available — same hardcoded fractions as the original
// estimatePredSelectivity.
func singlePredSelectivity(p Predicate) float64 {
	switch strings.ToLower(p.Op) {
	case "=", "eq":
		return 0.1
	case "<", "<=", ">", ">=", "lt", "le", "gt", "ge":
		return 0.33
	case "!=", "<>", "ne":
		return 0.9
	case "is_null":
		return 0.05
	case "is_not_null":
		return 0.95
	case "in":
		return 0.25
	case "between":
		return 0.25
	case "like":
		return 0.1
	default:
		return 0.33
	}
}

// estimateJoinStats estimates the output statistics of a join.
func estimateJoinStats(left, right RelStats, joinCond, joinType string) RelStats {
	leftNDV, rightNDV := resolveJoinKeyNDVs(joinCond, left, right)
	outputRows := estimateJoinCard(left.Rows, right.Rows, leftNDV, rightNDV, joinType)

	// Merge column NDVs from both sides, capped at output rows
	merged := make(map[string]float64, len(left.ColNDV)+len(right.ColNDV))
	for col, ndv := range left.ColNDV {
		merged[col] = math.Min(ndv, outputRows)
	}
	for col, ndv := range right.ColNDV {
		merged[col] = math.Min(ndv, outputRows)
	}

	return RelStats{Rows: math.Max(1, outputRows), ColNDV: merged}
}

// estimateJoinCard estimates the output cardinality of a join.
//
// For inner equi-joins, the cardinality lies between max(leftRows,
// rightRows) (the FK→PK natural ceiling) and leftRows*rightRows/maxNDV
// (the genuine many-to-many product). We pick the larger of the two:
// the Selinger formula bites only when both sides have NDV materially
// smaller than their row counts (many-to-many on a shared key); in the
// far more common FK→PK shape it reduces to max(L,R).
//
// Why not always Selinger? When NDV comes from a heuristic (the
// _key-suffix shortcut returns tableRows, overstating NDV), the
// Selinger formula collapses output below max(L,R), incorrectly
// "shrinking" FK→PK joins. The floor at max(L,R) keeps the formula
// well-behaved under both heuristic and HLL-derived NDVs.
func estimateJoinCard(leftRows, rightRows, leftNDV, rightNDV float64, joinType string) float64 {
	jt := strings.ToLower(joinType)

	switch {
	case jt == "" || jt == "join" || jt == "inner" || jt == "inner join":
		ceiling := math.Max(leftRows, rightRows)
		// Sanity-bound NDVs to row counts (HLL can overshoot near
		// precision limit; heuristics may exceed).
		ndvL := math.Min(leftNDV, leftRows)
		ndvR := math.Min(rightNDV, rightRows)
		maxNDV := math.Max(ndvL, ndvR)
		if maxNDV < 1 {
			return ceiling
		}
		selinger := leftRows * rightRows / maxNDV
		if selinger > ceiling {
			// Genuine many-to-many: both NDVs are small relative to
			// their row counts, so each match multiplies output.
			return selinger
		}
		return ceiling

	case strings.HasPrefix(jt, "left"):
		return leftRows // left join preserves all left rows

	case strings.HasPrefix(jt, "right"):
		return rightRows

	case strings.HasPrefix(jt, "full"):
		return leftRows + rightRows // conservative upper bound

	case jt == "cross":
		return leftRows * rightRows

	case jt == "semi":
		// At most leftRows, reduced by match probability
		matchProb := math.Min(1.0, rightRows/math.Max(rightNDV, 1))
		return leftRows * matchProb

	case jt == "anti":
		matchProb := math.Min(1.0, rightRows/math.Max(rightNDV, 1))
		return math.Max(1, leftRows*(1-matchProb))

	default:
		return math.Max(leftRows, rightRows)
	}
}

// resolveJoinKeyNDVs extracts NDV estimates for join key columns from a join condition.
func resolveJoinKeyNDVs(joinCond string, left, right RelStats) (float64, float64) {
	if joinCond == "" {
		// Cross join
		return left.Rows, right.Rows
	}

	parts := strings.Split(strings.ToLower(joinCond), " and ")
	var leftNDVs, rightNDVs []float64

	for _, part := range parts {
		part = strings.TrimSpace(part)
		ref1, ref2, found := strings.Cut(part, " = ")
		if !found {
			continue // skip non-equality conditions
		}
		ref1 = strings.TrimSpace(ref1)
		ref2 = strings.TrimSpace(ref2)

		col1 := strings.ToLower(stripQualifier(ref1))
		col2 := strings.ToLower(stripQualifier(ref2))

		// Try to match columns to left/right stats
		ndv1 := resolveNDV(col1, col2, left, right)
		ndv2 := resolveNDV(col2, col1, right, left)

		leftNDVs = append(leftNDVs, ndv1)
		rightNDVs = append(rightNDVs, ndv2)
	}

	if len(leftNDVs) == 0 {
		// No equality predicates — assume low selectivity
		return left.Rows, right.Rows
	}

	// For multi-key joins, use the maximum NDV (most selective key pair)
	maxLeft, maxRight := leftNDVs[0], rightNDVs[0]
	for i := 1; i < len(leftNDVs); i++ {
		if leftNDVs[i] > maxLeft {
			maxLeft = leftNDVs[i]
		}
		if rightNDVs[i] > maxRight {
			maxRight = rightNDVs[i]
		}
	}

	return maxLeft, maxRight
}

// resolveNDV looks up NDV for a column name, trying primary stats first then fallback.
func resolveNDV(col, otherCol string, primary, fallback RelStats) float64 {
	if ndv, ok := primary.ColNDV[col]; ok {
		return ndv
	}
	if ndv, ok := fallback.ColNDV[col]; ok {
		return ndv
	}
	// Both columns might have the same name — try otherCol in primary
	if ndv, ok := primary.ColNDV[otherCol]; ok {
		return ndv
	}
	// Fallback: assume moderate cardinality
	return math.Max(primary.Rows, fallback.Rows) * 0.1
}

// stripQualifier removes "table." prefix from a column reference.
func stripQualifier(ref string) string {
	if idx := strings.LastIndex(ref, "."); idx >= 0 {
		return ref[idx+1:]
	}
	return ref
}

// scaleNDVs returns a copy of NDVs with each value capped at the given row count.
func scaleNDVs(ndvs map[string]float64, rows float64) map[string]float64 {
	out := make(map[string]float64, len(ndvs))
	for col, ndv := range ndvs {
		out[col] = math.Min(ndv, math.Max(1, rows))
	}
	return out
}

// hashJoinCost estimates the CPU cost of a hash join.
// build = right side (materialized into hash table), probe = left side (streams through).
// Build cost is higher than probe cost because building requires hash table
// construction, memory allocation, and potential resizing. This ensures the
// CBO prefers building small tables (dimensions) and probing large ones (facts).
func hashJoinCost(probeRows, buildRows float64) float64 {
	const (
		buildCostPerRow = 2.0 // cost to hash, insert, allocate
		probeCostPerRow = 1.0 // cost to hash and look up
	)
	return buildRows*buildCostPerRow + probeRows*probeCostPerRow
}

// distributedExchangeCost prices the exchange a distributed hash join adds
// on top of its CPU cost. A broadcast-scale build replicates cheaply and the
// probe side stays in place (cost ≈ 0 at plan-order granularity); a bigger
// build forces hash-repartition of BOTH sides — serialize, materialize,
// re-read. The SF10 A/B pairs of 2026-07-09 showed why the DP cannot ignore
// this: a bushy composite that replaced broadcast-fused probes with a
// repartition pair regressed Q08 +123% while pure-CPU cost called it
// cheaper. Applied only in the BushyJoinReorder regime so flag-off plan
// selection is untouched.
//
// broadcastableRows approximates the runtime bytes threshold
// (isBroadcastCandidate, ~100 MB) at typical analytic row widths;
// exchangeCostPerRow prices serialize+write+read+deserialize relative to
// hashJoinCost's 1.0/row probe unit.
func distributedExchangeCost(probeRows, buildRows float64) float64 {
	const (
		broadcastableRows  = 1_000_000
		exchangeCostPerRow = 2.0
	)
	if buildRows <= broadcastableRows {
		return 0
	}
	return (probeRows + buildRows) * exchangeCostPerRow
}

// mergeNDVs combines column NDV maps from two relations, capping at outputRows.
func mergeNDVs(left, right map[string]float64, outputRows float64) map[string]float64 {
	merged := make(map[string]float64, len(left)+len(right))
	for col, ndv := range left {
		merged[col] = math.Min(ndv, outputRows)
	}
	for col, ndv := range right {
		merged[col] = math.Min(ndv, outputRows)
	}
	return merged
}
