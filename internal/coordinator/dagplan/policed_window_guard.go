// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"
)

// ErrPolicedWindowUnderJoinDistributed routes a plan to the coordinator
// single-process pipeline when a window reads a scan with a policy projection
// and has a consumer other than the final gather. This retains the published
// values when the distributed window-to-join path would read stored values.
// CheckPolicedWindowUnderJoin runs after each planning pass. The distributed
// implementation remains tracked by #1297; see ADR-0021 §1s.
var ErrPolicedWindowUnderJoinDistributed = errors.New(
	"a window over a policed relation feeding a join is not evaluable on the distributed path")

// CheckPolicedWindowUnderJoin refuses a stage plan in which a WINDOW stage
// reads (through any chain of exchange, project, filter or window stages) a
// scan that carries a security projection, and is consumed by anything other
// than the final gather.
func CheckPolicedWindowUnderJoin(stages []Stage) error {
	byID := make(map[string]*Stage, len(stages))
	consumers := make(map[string][]string, len(stages))
	for i := range stages {
		byID[stages[i].ID] = &stages[i]
		for _, d := range stages[i].Dependencies {
			consumers[d] = append(consumers[d], stages[i].ID)
		}
	}
	var readsPoliced func(id string, seen map[string]bool) bool
	readsPoliced = func(id string, seen map[string]bool) bool {
		if seen[id] {
			return false
		}
		seen[id] = true
		s, ok := byID[id]
		if !ok {
			return false
		}
		if s.Type == StageScan {
			return len(s.SecurityProjectExprs) > 0
		}
		for _, d := range s.Dependencies {
			if readsPoliced(d, seen) {
				return true
			}
		}
		return false
	}
	for i := range stages {
		w := &stages[i]
		if w.Type != StageWindow || !readsPoliced(w.ID, map[string]bool{}) {
			continue
		}
		for _, cid := range consumers[w.ID] {
			c := byID[cid]
			if c != nil && c.Type == StageExchangeGather {
				continue
			}
			return fmt.Errorf("%w (stage %s, consumed by %s)", ErrPolicedWindowUnderJoinDistributed, w.ID, cid)
		}
	}
	return nil
}
