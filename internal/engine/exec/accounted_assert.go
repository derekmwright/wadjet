// SPDX-License-Identifier: MIT

package exec

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// Compile-time assertions that the pipeline-breakers implement the Phase-2
// memory.AccountedOperator interface. Extended as each operator is migrated.
var (
	_ memory.AccountedOperator = (*HashAggregate)(nil)
	_ memory.AccountedOperator = (*Sort)(nil)
	_ memory.AccountedOperator = (*Window)(nil)
	_ memory.AccountedOperator = (*HashJoin)(nil)
)

// beginRelief marks an operator OpSpilling for a relief call that its own
// lifecycle check (made under the operator's lock) has already admitted, and
// reports false — writing nothing — when the operator has been closed.
//
// The registry calls a selected operator's SpillSome outside its own lock, so
// the call can land after the operator unregistered and stored OpClosed
// (#1706, line 1706k). Storing OpSpilling unconditionally, as every SpillSome
// did, turned that OpClosed back into OpSpilling and then, on the way out,
// into OpActive. A compare-and-swap from the observed state never overwrites
// OpClosed.
func beginRelief(state *atomic.Int32) bool {
	for {
		cur := state.Load()
		if cur == int32(memory.OpClosed) {
			return false
		}
		if state.CompareAndSwap(cur, int32(memory.OpSpilling)) {
			return true
		}
	}
}

// endRelief undoes beginRelief unless the operator closed meanwhile.
func endRelief(state *atomic.Int32) {
	state.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
}
