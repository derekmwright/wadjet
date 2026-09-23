// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"
)

// ErrPolicedWindowUnderJoinDistributed hands a plan whose WINDOW stage reads a
// policed scan and feeds a JOIN back to the coordinator, which runs it on the
// single-process pipeline (Coordinator.runPolicedWindowLocal).
//
// MEASURED, on the nine policy doors (arc LT): over `e7bal`, whose `bal` is
// masked to 0, every shape of the form
//
//	… JOIN LATERAL (SELECT c.id … FROM e7bal c WHERE c.bal = b.bal
//	                ORDER BY c.id LIMIT 2) s ON true            -- the planner's window
//	… JOIN LATERAL (SELECT c.id, ROW_NUMBER() OVER (PARTITION BY c.bal …)
//	                FROM e7bal c WHERE c.bal = b.bal) s ON true  -- the user's window
//	… JOIN (SELECT c.bal AS k, c.id AS m FROM e7bal c
//	        QUALIFY ROW_NUMBER() OVER (PARTITION BY c.bal …) <= 2) s ON s.k = b.bal
//
// answers the MASK's pairing on the five single-process doors and the ADMIN
// pairing — each row matched to itself, `a=k|m=k` — on the four doors that
// dispatch a stage DAG, which is arithmetic on the column the policy hides.
// The same body with no window (`… WHERE c.bal = b.bal AND c.id <= 2`) and a
// window over the policed scan with no join above it (`ROW_NUMBER() OVER
// (PARTITION BY bal) FROM e7bal`) answer the mask on all nine. The stage plan
// carries the security projection on BOTH scans (the coordinator's own
// EXPLAIN VERBOSE prints it), so the loss is at dispatch or execution of the
// window → exchange-repartition → hash_join chain and is not localised by
// this arc; it is filed `distributed`, priority high, with the plans and the
// door answers beside it.
//
// Until it is closed, no such plan executes on the DAG: a window stage whose
// input is a policed scan and whose output anything but the final gather
// consumes is refused here and the coordinator runs the plan single-process,
// which is the mask's answer. A refusal a rewriting pass could bypass is what
// CheckSecurityFilterOrder was added for; this one is its neighbour, asked
// after every pass, for the same reason.
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
