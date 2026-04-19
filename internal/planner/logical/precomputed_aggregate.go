package logical

import "fmt"

// PreComputedAggregate is a signature describing a derived aggregate whose
// result is already materialized elsewhere. When a logical-plan Aggregate
// node matches a signature, SubstitutePreComputedAggregates replaces it with
// a Scan node bearing SyntheticAlias; the caller (worker) is expected to
// wire SyntheticAlias → CacheFiles into the physical planner's
// StreamingSources map so the scan reads the cached rows directly.
//
// Phase 1 matches only aggregates that are GROUP BY GroupByCols over a
// single Scan of InputTable (no filters on the scan, no nested joins). More
// general shapes are rejected and left in-plan.
type PreComputedAggregate struct {
	InputTable     string
	GroupByCols    []string
	AggOutputCols  []string // OutputCol names from each AggSpec, in order
	SyntheticAlias string   // unique scan alias to substitute
}

// SubstitutePreComputedAggregates walks the plan tree and replaces each
// Aggregate node whose shape matches one of sigs with a synthetic Scan.
// Returns the set of substitutions actually performed (alias → signature),
// so the caller can verify every pre-computed input was consumed. A
// signature that never matched is logged by the caller; this pass does not
// error on unmatched signatures (the worker may have re-planned a shape
// that no longer contains the aggregate, in which case falling back to the
// in-pipeline execution is correct).
//
// Matching is conservative: a signature must match on GroupByCols (order
// and content) AND on the full set of AggExpr output names. Any other
// cases fall through untouched.
func SubstitutePreComputedAggregates(root *Node, sigs []PreComputedAggregate) (map[string]bool, error) {
	if root == nil || len(sigs) == 0 {
		return nil, nil
	}
	used := make(map[string]bool)
	if err := substituteInPlace(root, sigs, used); err != nil {
		return nil, err
	}
	return used, nil
}

func substituteInPlace(node *Node, sigs []PreComputedAggregate, used map[string]bool) error {
	if node == nil {
		return nil
	}
	// Rewrite children first so nested aggregates are considered before the
	// parent. A pre-computed aggregate replaces the WHOLE subtree — we don't
	// want a parent's match to block a child from also matching; walk leaves
	// first.
	for i, child := range node.Children {
		if child == nil {
			continue
		}
		sig, matched := matchAggregate(child, sigs)
		if matched {
			node.Children[i] = synthScan(sig)
			used[sig.SyntheticAlias] = true
			continue
		}
		if err := substituteInPlace(child, sigs, used); err != nil {
			return err
		}
	}
	return nil
}

// matchAggregate returns the first signature whose shape matches node, if
// any. Match rules (Phase 1):
//   - node.Type == NodeAggregate
//   - node.GroupBy == sig.GroupByCols (same order, same content)
//   - node has exactly one child, a NodeScan whose TableName == sig.InputTable
//   - the scan has no filter predicates (keeps Phase 1 semantics tight)
//   - the set of AggExpr OutputNames exactly equals sig.AggOutputCols
func matchAggregate(node *Node, sigs []PreComputedAggregate) (PreComputedAggregate, bool) {
	if node.Type != NodeAggregate {
		return PreComputedAggregate{}, false
	}
	if len(node.Children) != 1 {
		return PreComputedAggregate{}, false
	}
	child := node.Children[0]
	if child == nil || child.Type != NodeScan {
		return PreComputedAggregate{}, false
	}
	if len(child.ScanPredicates) > 0 || len(child.Predicates) > 0 || len(child.PartitionFilter) > 0 {
		return PreComputedAggregate{}, false
	}
	aggOutputs := make([]string, len(node.AggExprs))
	for i, a := range node.AggExprs {
		aggOutputs[i] = a.OutputCol
	}
	for _, sig := range sigs {
		if !stringsEqual(node.GroupBy, sig.GroupByCols) {
			continue
		}
		if child.TableName != sig.InputTable {
			continue
		}
		if !stringsEqualSet(aggOutputs, sig.AggOutputCols) {
			continue
		}
		return sig, true
	}
	return PreComputedAggregate{}, false
}

// synthScan produces the replacement node: a bare Scan that the physical
// planner will route through StreamingSources[SyntheticAlias] instead of
// hitting the catalog. The table name IS the synthetic alias — the physical
// planner's scan-alias computation uses node.TableName directly when no
// duplicates of the same base table are in play.
func synthScan(sig PreComputedAggregate) *Node {
	return &Node{
		Type:      NodeScan,
		TableName: sig.SyntheticAlias,
	}
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringsEqualSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	for _, c := range seen {
		if c != 0 {
			return false
		}
	}
	return true
}

// assertSignatureNonEmpty is a guard callers can use in tests to catch
// malformed signatures before they silently fail to match.
func assertSignatureNonEmpty(sig PreComputedAggregate) error {
	if sig.InputTable == "" {
		return fmt.Errorf("precomputed aggregate: empty InputTable")
	}
	if len(sig.GroupByCols) == 0 {
		return fmt.Errorf("precomputed aggregate: empty GroupByCols")
	}
	if sig.SyntheticAlias == "" {
		return fmt.Errorf("precomputed aggregate: empty SyntheticAlias")
	}
	return nil
}

// _ = assertSignatureNonEmpty  // exported for test use
var _ = assertSignatureNonEmpty
