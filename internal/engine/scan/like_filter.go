// SPDX-License-Identifier: MIT

package scan

import (
	"fmt"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// LIKE pushdown (extends the Level-3 scan filter to pattern predicates).
//
// ClickBench Q22/Q23/Q24 profile as pure column decode: `URL LIKE
// '%google%'` materialized every URL before the exec filter ran a single
// match. Evaluating the pattern here rides the same structure as the
// comparison preds — ONCE per dictionary entry on dict pages, per value
// on plain pages — so non-matching strings never materialize.
//
// Semantics parity: the matcher is kernel.CompileLike, the ONE text LIKE
// implementation the exec filter, its kernel and the expression evaluator call
// too (#1209): byte-wise and case-sensitive, '%' any run, '_' any single byte,
// and the BACKSLASH as the escape. The planner only pushes literal patterns
// over string columns, and never a pattern that can raise 22025 (one ending in
// a lone backslash, which only the matcher reaching it may report). NULL
// never matches LIKE or NOT LIKE (SQL three-valued logic — the page walkers
// already treat null rows as non-matching for every pred).

// RowPred.Op values for pattern predicates.
const (
	OpLike    = "like"
	OpNotLike = "not_like"
)

// compileLike builds a matcher for a LIKE pattern; kernel.CompileLike
// specializes the wildcard shapes that dominate real predicates (equality,
// prefix, suffix, contains) on the UNESCAPED literal. A pattern that can raise
// 22025 is refused rather than matched without its error: the planner does not
// push one (physical.makeLikeRowPred), so reaching here with one is a bug.
func compileLike(pattern string, negate bool) (func([]byte) bool, error) {
	m, ok := kernel.CompileLike(pattern).Matcher()
	if !ok {
		return nil, fmt.Errorf("scan filter: LIKE pattern %q can raise 22025 and is not pushable", pattern)
	}
	if negate {
		return func(s []byte) bool { return !m(bytesView(s)) }, nil
	}
	return func(s []byte) bool { return m(bytesView(s)) }, nil
}

// bytesView reads a page value as a string without copying it; the matcher
// does not retain it.
func bytesView(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }

// isLikeOp reports whether a RowPred is a pattern predicate.
func isLikeOp(op string) bool { return op == OpLike || op == OpNotLike }
