// This file holds scan filters for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"strings"
)

// frontLoadBlooms reorders the operator chain to move bloom filter operators
// whose key columns exist in the source scan schema to the front. In multi-way
// join pipelines, this allows selective bloom filters (e.g., from semi-joins
// with HAVING filters) to eliminate rows before expensive join probes.
func frontLoadBlooms(source exec.Source, ops []exec.UnaryOperator) []exec.UnaryOperator {
	if len(ops) < 3 {
		return ops // need at least bloom+probe+bloom to benefit
	}

	// Get source scan columns
	var scanCols map[string]bool
	switch s := source.(type) {
	case *catalogScanSource:
		scanCols = make(map[string]bool, len(s.requiredCols))
		for _, c := range s.requiredCols {
			scanCols[c] = true
		}
	case *scannerExecSource:
		scanCols = make(map[string]bool, len(s.requiredCols))
		for _, c := range s.requiredCols {
			scanCols[c] = true
		}
	}
	if len(scanCols) == 0 {
		return ops
	}

	// Find bloom filters that can be applied to the source scan (their key
	// columns all exist in the scan schema) and that are NOT already at the
	// front of the pipeline (i.e., there's a non-bloom op before them).
	firstNonBloom := -1
	for i, op := range ops {
		if _, ok := op.(*exec.BloomFilterOp); !ok {
			firstNonBloom = i
			break
		}
	}
	if firstNonBloom < 0 {
		return ops // all ops are blooms (unlikely)
	}

	var front, rest []exec.UnaryOperator
	for i, op := range ops {
		bf, isBF := op.(*exec.BloomFilterOp)
		if isBF && i > firstNonBloom {
			// This bloom is after a non-bloom op — check if it can be front-loaded
			allPresent := true
			for _, key := range bf.KeyColumns() {
				if !scanCols[key] {
					allPresent = false
					break
				}
			}
			if allPresent {
				front = append(front, op)
				continue
			}
		}
		rest = append(rest, op)
	}

	if len(front) == 0 {
		return ops
	}
	return append(front, rest...)
}

// attachBloomToScanSource walks the probe-side source chain to find a
// catalogScanSource and attaches a bloom filter for row-group-level pruning.
func attachBloomToScanSource(source exec.Source, bsf *exec.BloomScanFilter) {
	switch s := source.(type) {
	case *catalogScanSource:
		s.SetBloomFilter(bsf)
	case *pipelineSource:
		attachBloomToScanSource(s.source, bsf)
	}
}

// attachDynamicFilterToScanSource walks the probe-side source chain to find a
// catalogScanSource and attaches a dynamic min/max range filter for row-group pruning.
func attachDynamicFilterToScanSource(source exec.Source, ranges []exec.DynamicRange) {
	switch s := source.(type) {
	case *catalogScanSource:
		s.SetDynamicFilter(ranges)
	case *pipelineSource:
		attachDynamicFilterToScanSource(s.source, ranges)
	}
}

// findScanRowEstimate returns the total row estimate from scan nodes in a subtree.
// Used to pre-allocate hash join arena and index.
// groupKeyNDVEstimate resolves a GROUP-KEY cardinality estimate from the
// scan's merged-HLL column stats: the per-column NDVs' product (single
// column: the NDV itself), capped by the scan row estimate — the true
// group count can exceed neither. Returns 0 (no hint) when any key column
// lacks stats (synthetic __gb_expr keys, expression keys, missing HLL) or
// the input shape hides the scan (joins, subqueries): sizing then falls
// back to organic growth, which is never wrong, just slower.
func groupKeyNDVEstimate(child *logical.Node, groupByCols []string) int64 {
	if len(groupByCols) == 0 {
		return 0
	}
	n := child
	for n != nil && n.Type != logical.NodeScan {
		switch n.Type {
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort:
			if len(n.Children) != 1 {
				return 0
			}
			n = n.Children[0]
		default:
			return 0
		}
	}
	if n == nil || n.ScanColStats == nil {
		return 0
	}
	est := int64(1)
	for _, c := range groupByCols {
		cs, ok := n.ScanColStats[c]
		if !ok {
			// Stats keys carry catalog casing; group cols may be
			// SQL-normalized.
			for name, v := range n.ScanColStats {
				if strings.EqualFold(name, c) {
					cs, ok = v, true
					break
				}
			}
		}
		if !ok || cs.NDV <= 0 {
			return 0
		}
		// Overflow-safe product; anything past the row estimate is capped
		// below anyway.
		if est > (1<<62)/cs.NDV {
			est = 1 << 62
			break
		}
		est *= cs.NDV
	}
	if n.ScanRowEstimate > 0 && est > n.ScanRowEstimate {
		est = n.ScanRowEstimate
	}
	return est
}

func findScanRowEstimate(node *logical.Node) int64 {
	if node == nil {
		return 0
	}
	if node.Type == logical.NodeScan {
		return node.ScanRowEstimate
	}
	// For aggregates, row count is much smaller than scan (assume 10% or 2M max).
	// At SF100, high-cardinality GROUP BY (e.g. Q17: ~20M l_partkey values)
	// can produce millions of groups; 100K was too low and forced repeated
	// hash table doublings during execution.
	if node.Type == logical.NodeAggregate {
		est := findScanRowEstimate(node.Children[0])
		reduced := est / 10
		if reduced > 2_000_000 {
			reduced = 2_000_000
		}
		if reduced < 1 {
			reduced = 1
		}
		return reduced
	}
	var total int64
	for _, child := range node.Children {
		total += findScanRowEstimate(child)
	}
	return total
}

// extractColumnRefs extracts raw column name references from a string that
// may be a simple column ("l_shipdate"), a qualified column ("n1.n_name"),
// or an expression ("substr(l_shipdate, 1, 4)", "l_extendedprice * (1 - l_discount)").
// Returns the string itself if it's a simple/qualified column name.
func extractColumnRefs(s string) []string {
	// Simple or qualified column name: contains only alphanumerics, underscores, dots
	isSimple := true
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			isSimple = false
			break
		}
	}
	if isSimple {
		return []string{s}
	}

	// Expression: extract identifier tokens that look like column references.
	// Tokenize by splitting on non-identifier characters, then filter out
	// SQL keywords and numeric literals.
	var refs []string
	seen := make(map[string]bool)
	start := -1
	for i, c := range s {
		isIdent := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '.' || (c >= '0' && c <= '9')
		if isIdent {
			if start == -1 {
				start = i
			}
		} else {
			if start >= 0 {
				tok := s[start:i]
				start = -1
				if isColumnRef(tok) && !seen[tok] {
					refs = append(refs, tok)
					seen[tok] = true
				}
			}
		}
	}
	if start >= 0 {
		tok := s[start:]
		if isColumnRef(tok) && !seen[tok] {
			refs = append(refs, tok)
			seen[tok] = true
		}
	}
	return refs
}

// isColumnRef returns true if a token looks like a column reference:
// not a number, not a SQL keyword, contains at least one underscore or letter.
func isColumnRef(tok string) bool {
	if len(tok) == 0 {
		return false
	}
	// Pure number
	allDigit := true
	for _, c := range tok {
		if c < '0' || c > '9' {
			allDigit = false
			break
		}
	}
	if allDigit {
		return false
	}
	// SQL keywords to skip
	lower := strings.ToLower(tok)
	switch lower {
	case "case", "when", "then", "else", "end", "and", "or", "not", "in",
		"is", "null", "true", "false", "like", "between", "as", "asc", "desc":
		return false
	}
	return true
}
