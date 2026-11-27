// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// scanFilterTarget is the ELIGIBILITY GUARD for every scan-level filter
// derived from a hash join's build keys — the bloom scan filter and the
// dynamic min/max range (#1677). Such a filter prunes a row group by
// comparing the build's key values with the STORED column's statistics, so it
// is right only when the probe key the join compares is that stored value. It
// returns the catalog scan the filter may be installed on, or nil.
//
// It is fail-closed: it walks from the join's probe down to the scan and
// every hop must be proved, or nothing is installed. The hops it can prove:
//
//   - a row filter (the Filter family, a bloom pre-check): it narrows the
//     selection and never changes a value or adds a row — a row-filter
//     policy is one;
//   - a lower hash join's probe (exec.HashJoinProbe.ForwardsProbeColumn): an
//     INNER, LEFT, SEMI or ANTI join forwarding the probe's column, with no
//     build column the key could resolve to instead;
//   - a projection that passes the column through by name, unchanged and
//     alone (projectForwards);
//   - a pipelineSource, whose own operators are walked by the same rules.
//
// Anything else — a computed, cast, renamed or masked projection (a policy
// mask is a projection), a set operation, an aggregate, a window, a LIMIT, a
// lateral, a source that is not a base-table scan — is unproved. The scan
// itself must read col as a stored column, and must not be feeding the
// multi-consumer scan cache: a filter on the scan that fills the cache would
// prune the rows every other consumer replays.
//
// Losing a pruning opportunity is acceptable; a wrong prune is a silently
// wrong answer. Before this guard the attachment walked through any
// pipelineSource to the scan and ignored the operators in between, so
// `a.ssn IN (SELECT ssn …)` under a mask ranged the stored ssn by the masked
// build keys and pruned every row group, and `(SELECT y AS x FROM t) q WHERE
// q.x IN (…)` pruned t by its stored x.
func scanFilterTarget(source exec.Source, ops []exec.UnaryOperator, col string) *catalogScanSource {
	if scanFilterGuardOffForTest {
		return unguardedScanTarget(source)
	}
	if col == "" {
		return nil
	}
	for i := len(ops) - 1; i >= 0; i-- {
		if !opForwardsColumn(ops[i], col) {
			return nil
		}
	}
	switch s := source.(type) {
	case *catalogScanSource:
		if s.cache != nil {
			return nil
		}
		for _, c := range s.requiredCols {
			if c == col {
				return s
			}
		}
		return nil
	case *pipelineSource:
		return scanFilterTarget(s.source, s.ops, col)
	}
	return nil
}

// scanFilterGuardOffForTest restores the walk the guard replaced (through any
// pipelineSource to the scan, ignoring every operator): the negative control
// for the guard's tests. TEST ONLY.
var scanFilterGuardOffForTest bool

func unguardedScanTarget(source exec.Source) *catalogScanSource {
	switch s := source.(type) {
	case *catalogScanSource:
		return s
	case *pipelineSource:
		return unguardedScanTarget(s.source)
	}
	return nil
}

// opForwardsColumn reports whether op is proved to hand its input's column
// col to its output unchanged, adding no rows (scanFilterTarget).
func opForwardsColumn(op exec.UnaryOperator, col string) bool {
	switch o := op.(type) {
	case *exec.Filter, *exec.ChainFilter, *exec.ColColFilter, *exec.InFilter,
		*exec.KernelFilter, *exec.LikeFilter, *exec.MatchNothingFilter,
		*exec.NullCheckFilter, *exec.OrFilter, *exec.BloomFilterOp:
		return true
	case *exec.HashJoinProbe:
		return o.ForwardsProbeColumn(col)
	case *exec.Project:
		return projectForwards(o, col)
	}
	return false
}

// projectForwards reports whether p passes its input column col through
// under the same name, as a plain copy, and is the only output a reference
// to col can resolve to. A rename, a computation, a cast, a mask or a
// positional source is not a proof.
func projectForwards(p *exec.Project, col string) bool {
	bare := bareColumn(col)
	found := false
	for _, pc := range p.Projections {
		if !strings.EqualFold(bareColumn(pc.Name), bare) {
			continue
		}
		if found || pc.Name != col || pc.DirectCopy != col || pc.Computed || pc.SourceIdxSet {
			return false
		}
		found = true
	}
	return found
}

// bareColumn is name without its qualifier: the part after the last dot.
func bareColumn(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}
