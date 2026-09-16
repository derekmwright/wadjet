// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

// refuseJoin parks the first refusal; PlanDistributed returns it. First one
// wins so a nested join's specific message is not overwritten by an outer
// one's.
func (p *StagePlanner) refuseJoin(err error) {
	if p.joinCondErr == nil {
		p.joinCondErr = err
	}
}
