package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decomposeOhlcv is ADR-0035's rewrite for the bar, and it is decomposeVar's
// shape exactly:
//
//	OHLCV(ts, price, volume) AS out  →  OHLCV_STATE(...) AS __ohlcv_state#out
//
// A FINISHED bar cannot be re-aggregated — the partial-then-merge shape would
// run OHLCV over per-task ROWs, which is not a bar at all — so what ships is
// the STATE each partial accumulated. Intermediate merge stages fold states
// into states (the worker rewrites OHLCV_STATE into OHLCV_STATE_MERGE in merge
// mode, as it does VAR_STATE), and the final stage's fold turns the last one
// into the ROW the query asked for (worker.applyOhlcvFold).
//
// The state's DOMAIN — exact or float, and the scales — travels inside the
// encoded state, so a merge stage needs nothing but the string. The declared
// FIELDS travel on the spec (AggSpec.OutputFields), because only the final
// fold needs them and only the planner can derive them.
//
// Unlike the variance family there is no <kind>: one state, one way to finish
// it. The synthetic is `__ohlcv_state#<out>` and `#` is illegal in an
// identifier, delimited or not, so it cannot collide with a user's column.
//
// Returns the original slice unchanged when no bar is present.
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
