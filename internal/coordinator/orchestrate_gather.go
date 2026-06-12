package coordinator

import (
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
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
	stage physical.Stage,
	in BatchStream,
	columns []string,
	mi *logical.MergeInfo,
) ([]*batch.RecordBatch, int64, error) {
	if stage.Type != physical.StageExchangeGather {
		in.Close()
		return nil, 0, fmt.Errorf(
			"orchestrate gather: wrong stage type %q (expected %q)",
			stage.Type, physical.StageExchangeGather,
		)
	}
	return c.mergeProbePartials(in, columns, mi)
}
