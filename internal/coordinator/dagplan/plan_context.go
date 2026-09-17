// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import "github.com/derekmwright/wadjet/internal/planner/physical"

// localPlanFacts serves walks whose inputs fully describe the local plan.
// Per-statement operations use StagePlanner.PlanContext and its shared planner.
var localPlanFacts physical.PlanContext
