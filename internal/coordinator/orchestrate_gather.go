// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// orchestrateGather dispatches a StageExchangeGather. It wraps the
// coordinator-side merge helper so stage-DAG dispatch (added in Phase 2
// Task 14) can call a named backend per Exchange type. No new runtime
// logic — just the stage-type guard + delegation.
//
// Phase 2 Task 13: shim added; not yet called from any dispatch site.
// Phase 2 Task 14 wires it in. MergeInfo survives until Task 19 deletes
// the legacy side-channel.
func (c *Coordinator) orchestrateGather(
	stage dagplan.Stage,
	in BatchStream,
	columns []string,
	mi *logical.MergeInfo,
) ([]*batch.RecordBatch, int64, error) {
	if stage.Type != dagplan.StageExchangeGather {
		in.Close()
		return nil, 0, fmt.Errorf(
			"orchestrate gather: wrong stage type %q (expected %q)",
			stage.Type, dagplan.StageExchangeGather,
		)
	}
	return c.mergeProbePartials(in, columns, mi)
}
