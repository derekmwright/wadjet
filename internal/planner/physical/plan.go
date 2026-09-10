// Package physical converts logical plans to physical execution plans.
package physical

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ScalarDeferToggle gates deferring ALL uncorrelated scalar subqueries to
// distributed producer stages (not only CTE-referencing ones). Off
// (WADJET_SCALAR_DEFER=0) reverts them to eager plan-time execution on the
// coordinator's single-process pipeline.
//
// REGISTERED rather than read with a bare os.Getenv since #659: the switch
// decides whether a SELECT-list item becomes a producer stage or is left to
// the coordinator-local route, so it changes which ENGINE answers a query --
// and the rule is that a switch which can change the row set extends the
// invariance oracle. Reading it off the registry also lets a gate flip it
// without an env round trip.
var ScalarDeferToggle = optswitch.Register("scalar-defer", "WADJET_SCALAR_DEFER",
	"defer every uncorrelated scalar subquery to a distributed producer stage")

// ProbeSplitMinBytes is the minimum size of the largest scan required to
// activate probe-split. Below this, the orchestration overhead exceeds the
// parallelism benefit. Exported so tests can lower it to exercise the
// distributed path on tiny datasets — otherwise every test silently runs
// the single-worker path and distributed-only bugs (like the SF100 build
// cache Q02 regression) never get caught.
var ProbeSplitMinBytes int64 = 64 * 1024 * 1024

// ReverseBloomThreshold and ReverseBloomInnerThreshold gate the reverse-bloom
// optimization (see buildJoin). Declared as vars so regression tests can lower
// them to fire on tiny SF0.x datasets — TestTPCHReverseBloomForcedSF001 does
// exactly that — and so they can be raised at runtime to turn the optimization
// off without rebuilding.
//
// These lines used to say the vars existed "to disable the optimization while
// we hunt the SF100 Q05 0-rows bug whose triggering code path is somewhere in
// this optimization", and that the semi/anti threshold stayed at 10M because
// there was "no evidence of bugs there yet". Both halves are settled now, and
// not in the direction the second one guessed.
//
// A 0-rows MECHANISM in this optimization is identified and fixed (#543):
// reverseBloomBridge installed the bloom whether or not the key column had
// been found in the probe output, so a probeKey that did not resolve produced
// an EMPTY bloom that rejected every build row — a join answering over an
// empty build side, which is 0 rows for an inner or semi join. Forcing both
// thresholds to 100 over the SF0.01 corpus fires it on exactly one query,
// Q21, whose probeKey arrives alias-qualified as "l1.l_orderkey" against
// batches carrying "l_orderkey": on the parent commit Q21 returns 0 rows
// where the answer is 1 (and 0 where it is 100 at SF1). Init now refuses to
// install a bloom whose column never resolved or that received no keys.
//
// Whether that mechanism is what produced the Q05 incident at SF100 was never
// reduced to a repro and is not claimed here: Q05's own reverse blooms resolve
// their columns at SF0.01, and the corpus-wide forced run shows Q21 as the
// only unresolved one. What IS claimed is that this optimization could return
// 0 rows for a reason that had nothing to do with the query, that the reason
// is now gone, and that a gate runs the whole corpus with both thresholds
// forced down so the next one cannot hide behind a production threshold.
//
// The semi/anti threshold's "no evidence of bugs there yet" was wrong twice
// over: #543's key-encoding divergence was semi/anti-only in practice, since
// that is where string keys appear, and the empty-bloom mechanism above fires
// on a semi/anti query. The threshold stays at 10M for COST reasons.
//
// Init reads WADJET_REVERSE_BLOOM_INNER_THRESHOLD if set, so the bench can
// disable the inner-join path on SF100 without rebuilding the binary.
var (
	ReverseBloomThreshold      int64 = 10_000_000
	ReverseBloomInnerThreshold int64 = 50_000_000
)

// reverseBloomToggle is the kill switch for the whole reverse-bloom path
// (#287's convention: WADJET_REVERSE_BLOOM=0 disables). The optimization
// removes build-side rows before they reach the hash table, so a defect in it
// is a defect in the ANSWER — #543 dropped every row of a string-keyed
// semi/anti build — and the invariance oracle can only compare against a run
// without it if there is a switch to turn it off.
var reverseBloomToggle = optswitch.Register("reverse-bloom", "WADJET_REVERSE_BLOOM",
	"reverse-bloom pushdown: build a bloom from the probe side's join keys and filter the build-side scan with it")

func init() {
	if v := os.Getenv("WADJET_REVERSE_BLOOM_INNER_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			ReverseBloomInnerThreshold = n
		}
	}
}

// maxFusedBuildBytes is the per-fused-build EstimatedBytes ceiling above
// which fuseJoinStages refuses to absorb a broadcast join. Above this size,
// the cluster-wide S3 amplification of replicating the cache to every
// probe-split shard task outweighs the savings from skipping the
// intermediate exchange-replicate materialization. Tune via SF100+ deploys
// once we have measured numbers; 1 GB is conservative.
//
// Var rather than const so tests can lower it to exercise the skip path on
// small fixtures.
var maxFusedBuildBytes int64 = 1 * 1024 * 1024 * 1024

// ReverseBloomsInstalled counts reverse-bloom filters actually pushed onto a
// build-side scan. A gate that means to exercise this path asserts on it:
// without it, a test can only prove the query answered, not that the
// optimization it was written for ever engaged.
var ReverseBloomsInstalled atomic.Int64

// BuildSemiAntiFilter compiles a non-equality join filter string (e.g., "l_suppkey != l_suppkey")
// into a function that evaluates the condition on probe and build batch rows.
// Convention: left of operator = probe column, right = build column.
//
// The returned closure lazily resolves column indices on first call and caches
// them, avoiding per-row ColumnByName lookups. Comparisons use typed dispatch
// (int32, int64, float64, string) instead of fmt.Sprint conversion.
//
// HashJoin's probe runs in parallel — multiple workers call this filter
// concurrently against probe and build batches whose schemas are stable
// across the lifetime of the query (same logical plan → same projected
// columns). Use sync.Once to resolve indices safely on first call; later
// calls become a single relaxed atomic load on the once.done flag.
// SemiAntiNE gates the distinct-pair semi/anti build fast path
// (exec/join_semianti_ne.go). Kill switch WADJET_SEMIANTI_NE=0.
var SemiAntiNE atomic.Bool

func init() {
	SemiAntiNE.Store(os.Getenv("WADJET_SEMIANTI_NE") != "0")
}

// resolveNullsLast determines whether nulls should sort last for a given order
// expression. An explicit NULLS FIRST / NULLS LAST always wins; otherwise the
// engine default applies: NULLS LAST for ASC, NULLS FIRST for DESC.
//
// That is PostgreSQL's rule, chosen deliberately. SQL leaves the default
// implementation-defined and DuckDB picks NULLS LAST in both directions, but
// wadjet speaks the PostgreSQL wire protocol, so a psql/DataGrip/Superset user
// writing ORDER BY x DESC expects PostgreSQL's placement. The DuckDB gate is
// held to the same rule by setting default_null_order in the oracle rather
// than by exempting entries, so the comparison keeps its full strength.
//
// See distributed.SortKeySpec.PlaceNullsLast, which has to agree with this
// function key for key or the two execution paths sort differently.
func resolveNullsLast(ob logical.OrderExpr) bool {
	if ob.NullsFirst != nil {
		return !*ob.NullsFirst // NullsFirst=true => NullsLast=false, and vice versa
	}
	return !ob.Desc
}

// isComputedProjection reports whether a SELECT item's value is COMPUTED
// rather than read straight from an input column. Only a bare column
// reference — optionally parenthesised — reads an input column; everything
// else (function call, arithmetic, CASE, CAST, concatenation) produces a new
// value whose type comes from the expression, not from whatever input column
// happens to share the output's alias (#327).
//
// A nil AST expression is the pre-AST projection form, which is always a
// plain column.
func isComputedProjection(e plansql.Node) bool {
	for {
		switch n := e.(type) {
		case nil:
			return false
		case *plansql.ColRef:
			return false
		case *plansql.ParenNode:
			e = n.Inner
		default:
			return true
		}
	}
}

// cleanExpr drops the table qualifier from a COLUMN REFERENCE, and leaves
// everything else exactly as written.
//
// The distinction is the whole of the function. Its callers hand it text that
// is usually `t.col` and sometimes an arbitrary expression, and the second
// kind has no qualifier to strip: the first dot in `concat(t0.c0, t0.c1)`
// separates a table from a column only if you already know the text is a
// column reference. A naive SplitN on '.' does not, so it returned
// `c0, t0.c1)` — a fragment of the expression, parentheses and commas
// included, which then became the OUTPUT COLUMN NAME a client binds by
// (#513).
//
// plansql.SplitIdentRef is the test, because it is the lexer: it accepts
// `col`, `t.col` and the delimited spellings (`"id.orig_h"` is ONE name, a
// flat Zeek JSON column with no qualifier — #304) and rejects anything that
// does not end after the identifier, which is every function call, operator
// expression and literal.
func cleanExpr(s string) string {
	s = strings.TrimSpace(s)
	if _, name, ok := plansql.SplitIdentRef(s); ok {
		return name
	}
	return s
}
