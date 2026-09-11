package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decomposeOhlcv rewrites OHLCV(...) AS out to OHLCV_STATE(...) AS __ohlcv_state#out (ADR-0035).
// A finished bar cannot be re-aggregated: partials ship states, intermediate
// workers use OHLCV_STATE_MERGE, and worker.applyOhlcvFold finishes the final ROW.
// Exact/float domain and scales travel inside the encoded state; merges need only its string.
// Declared fields travel on AggSpec.OutputFields: the planner derives them for the final fold.
// There is one state and one finish, no variance-style <kind>.
// The synthetic's # is illegal in identifiers, including delimited ones, preventing user collisions.
// Return the original slice unchanged if no bar is present.
// See docs/internals/distributed-ohlcv-state-decomposition.md for the design.
func decomposeOhlcv(specs []distributed.AggSpec) []distributed.AggSpec {
	if !anyOhlcvFunc(func(i int) string { return specs[i].Func }, len(specs)) {
		return specs
	}
	out := make([]distributed.AggSpec, 0, len(specs))
	for _, a := range specs {
		if !isOhlcvFunc(a.Func) {
			out = append(out, a)
			continue
		}
		s := a
		s.Func = exec.OhlcvStateFunc
		s.OutputCol = ohlcvStateColumn(a.OutputCol)
		s.OutputType = distributed.WindowTypePtr(int(parquet.TypeString))
		out = append(out, s)
	}
	return out
}

// OhlcvStateRoutes reports how many aggregate stages this coordinator
// dispatched with a bar decomposed into its mergeable state. Zero means every
// bar took the one-level RawInputAggregate shape — correct, and NOT what
// ADR-0035 claims.
func (c *Coordinator) OhlcvStateRoutes() int64 { return c.ohlcvStateRoutes.Load() }

// decomposeOhlcvFor is decomposeOhlcv with the route counted. Every dispatch
// site goes through it; the bare function stays pure so it can be unit-tested
// and so the physical variant can share its predicate.
func (c *Coordinator) decomposeOhlcvFor(specs []distributed.AggSpec) []distributed.AggSpec {
	out := decomposeOhlcv(specs)
	if anyOhlcvFunc(func(i int) string { return specs[i].Func }, len(specs)) {
		c.ohlcvStateRoutes.Add(1)
	}
	return out
}

func ohlcvStateColumn(outputCol string) string { return exec.OhlcvStateColumn(outputCol) }

func isOhlcvFunc(fn string) bool {
	return strings.ToLower(strings.TrimSpace(fn)) == exec.OhlcvFunc
}

func anyOhlcvFunc(at func(int) string, n int) bool {
	for i := 0; i < n; i++ {
		if isOhlcvFunc(at(i)) {
			return true
		}
	}
	return false
}
