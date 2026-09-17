// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// NewComputedColumnsOp returns an operator that passes every input column
// through and appends the computed ones.
//
// The type is aggPreProject, named for its first caller. It is exported
// through a constructor rather than moved because it has a second caller now
// with the same need and none of the aggregate's context: the window
// fragment, which must compute an expression PARTITION BY key before
// exec.Window can resolve it by name (#585) and which — like the pre-
// aggregate projection — cannot narrow the batch, since the window's output
// is every input column plus its own.
func NewComputedColumnsOp(cols []exec.ProjectColumn) exec.UnaryOperator {
	return localPlanFacts.NewComputedColumnsOpWithMeta(cols, nil)
}
