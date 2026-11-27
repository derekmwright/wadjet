// SPDX-License-Identifier: MIT

package exec

import "strings"

// ForwardsProbeColumn reports whether this probe is proven to hand a column
// named col from its PROBE input to its output unchanged, as the only output
// column a reference to col can resolve to. The physical planner asks it
// while proving that a join key above this probe is still the stored value of
// a column of the scan below it, before it installs a scan-level filter
// derived from that join's build keys (physical.scanFilterTarget, #1677).
//
// It answers false — no proof — for every join kind but INNER, LEFT, SEMI and
// ANTI (a RIGHT or FULL join supplies rows whose probe columns are NULL, a
// right semi/anti emits build rows), and for an INNER or LEFT join whose build
// side carries a column of the same bare name (the reference above could read
// the build's column), or whose build schema is not known yet.
func (p *HashJoinProbe) ForwardsProbeColumn(col string) bool {
	h := p.join
	switch h.JoinType {
	case SemiJoin, AntiJoin:
		// The output is the probe batch under a narrower selection.
		return true
	case InnerJoin, LeftJoin:
	default:
		return false
	}
	h.mu.Lock()
	schema := h.buildSchema
	h.mu.Unlock()
	if schema == nil {
		return false
	}
	want := bareIdent(col)
	for _, c := range schema {
		if strings.EqualFold(bareIdent(c.Name), want) {
			return false
		}
	}
	return true
}

// bareIdent is name without its qualifier: the part after the last dot.
func bareIdent(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}
