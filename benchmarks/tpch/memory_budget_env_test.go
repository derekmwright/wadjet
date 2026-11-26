// SPDX-License-Identifier: MIT

package tpch

import (
	"os"
	"strconv"
)

// tpchMemoryBudget reads WADJET_TPCH_MEMORY_BUDGET (bytes): the pool budget
// the streaming-loaded suites (TestTPCHBaselineSF1, TestTPCHQueriesLarge) run
// under. 0, the detected default, when unset.
func tpchMemoryBudget() int64 {
	v, err := strconv.ParseInt(os.Getenv("WADJET_TPCH_MEMORY_BUDGET"), 10, 64)
	if err != nil {
		return 0
	}
	return v
}
