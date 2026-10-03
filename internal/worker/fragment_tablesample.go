// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// sampledFragmentSource applies a scan fragment's TABLESAMPLE (#1411) to its
// source: the engine's own sampler (exec.NewSampledSource), the one the
// single-process scan runs, over the rows the source's batches SELECT — the
// delete markers SetDeleteMarkers applied narrow Sel, and the sample is drawn
// from what is left. Its first Next checks the range, as the coordinator did
// before dispatch. src unchanged when the spec carries no sample.
func sampledFragmentSource(src exec.Source, spec *distributed.TableSampleSpec) exec.Source {
	if spec == nil {
		return src
	}
	return &sampledScanSource{
		Source: exec.NewSampledSource(src, exec.TableSample{Method: spec.Method, Percent: spec.Percent, Null: spec.Null}),
		inner:  src,
	}
}

// sampledScanSource is the sampler with the source it wraps reachable, so an
// optional-interface lookup (batchRecyclerOf) still finds the scan beneath.
// A batch the sampler drops is never delivered, and an undelivered batch stays
// collectable (scan.BackingPool's contract).
type sampledScanSource struct {
	exec.Source
	inner exec.Source
}

func (s *sampledScanSource) unwrapSource() exec.Source { return s.inner }
