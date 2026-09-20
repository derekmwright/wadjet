// SPDX-License-Identifier: MIT

package wadjet

import "github.com/derekmwright/wadjet/internal/planner/physical"

// readerSchemaReadsForTest is the planner's count of reader-input opens, read
// from this package so a gate here can assert that a cell which must NOT read
// and a control which MUST read are told apart by the same counter the door
// gates use.
func readerSchemaReadsForTest() int64 { return physical.ReaderSchemaReads.Load() }
