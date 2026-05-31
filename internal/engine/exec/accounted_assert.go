package exec

import "github.com/citc-tech/wadjet/internal/engine/memory"

// Compile-time assertions that the pipeline-breakers implement the Phase-2
// memory.AccountedOperator interface. Extended as each operator is migrated.
var (
	_ memory.AccountedOperator = (*HashAggregate)(nil)
	_ memory.AccountedOperator = (*Sort)(nil)
)
