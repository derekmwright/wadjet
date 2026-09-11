package physical

import (
	"errors"
)

// ErrGroupKeyDistributed refuses keys whose value no fragment emits or whose
// resolution contains aggregate/window calls that a scalar projection cannot
// evaluate. Publication and resolution are distinct (ADR-0026 §2); merge mode
// reads published columns and carries no resolution list (#794).
// Coordinator.ExecuteSQL hands off to local execution, like
// ErrDistinctDistributed/ErrGroupingSetsDistributed. runRefusedLocal has an
// 8×localFastPathBytes budget: routed joins can fail under a small setting,
// even if the DAG could finish; routing is not a guarantee of a slower answer.
// See docs/internals/distributed-group-key-refusals.md for the design.
var ErrGroupKeyDistributed = errors.New(
	"this GROUP BY key needs a published name the stage cannot carry")
