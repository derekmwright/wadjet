package worker

import (
	"bytes"
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/exec"
)

// materializeDynamicFilters translates a probe-side OpSpec.DynamicFilters
// into the in-process exec.DynamicRange + exec.BloomScanFilter forms the
// scan source consumes. Staged blooms (BloomKey set) are fetched from the
// worker's object store on-demand; inline blooms are used as-is.
//
// Failures are partial: a single failed filter is dropped from the result
// (probe-scan runs without it) rather than failing the task. Correctness
// is preserved either way — the bloom is an optimization, not a semantic.
func (e *Executor) materializeDynamicFilters(
	specs []distributed.DynamicFilterSpec,
) ([]exec.DynamicRange, []*exec.BloomScanFilter, error) {
	if len(specs) == 0 {
		return nil, nil, nil
	}
	var ranges []exec.DynamicRange
	var blooms []*exec.BloomScanFilter
	for _, s := range specs {
		// Deferred (attach-on-arrival) specs are not materializable up
		// front — the artifact may not exist yet. The fragment wrap site
		// handles them via the poll-and-install path; every other consume
		// path skips them (scan runs unfiltered, always correct).
		if s.Deferred {
			continue
		}
		if s.HasRange {
			ranges = append(ranges, exec.DynamicRange{
				Column:   s.TargetColumn,
				MinValue: int64ToAny(s.KeyType, s.Min),
				MaxValue: int64ToAny(s.KeyType, s.Max),
			})
		}
		bloom, mask, ok := e.loadBloomFromSpec(s)
		if !ok {
			continue
		}
		blooms = append(blooms, &exec.BloomScanFilter{
			Bloom:     bloom,
			BloomMask: mask,
			Column:    s.TargetColumn,
			UseIntKey: true,
		})
	}
	return ranges, blooms, nil
}

// loadBloomFromSpec returns the bloom for a spec, fetching from S3 if it
// was staged out-of-band. Returns ok=false when the bloom is unavailable
// or empty — caller skips and proceeds without that filter.
func (e *Executor) loadBloomFromSpec(s distributed.DynamicFilterSpec) ([]uint64, uint64, bool) {
	if len(s.Bloom) > 0 {
		return s.Bloom, s.BloomMask, true
	}
	if s.BloomBucket == "" || s.BloomKey == "" {
		return nil, 0, false
	}
	ctx := context.Background()
	rc, _, err := e.store.Get(ctx, s.BloomBucket, s.BloomKey)
	if err != nil {
		e.logger.Warn("dynamic_filter: staged bloom fetch failed; proceeding without filter",
			"filter_id", s.FilterID, "bucket", s.BloomBucket, "key", s.BloomKey, "error", err)
		return nil, 0, false
	}
	defer rc.Close()
	art, err := distributed.DecodeDynamicFilterArtifact(rc)
	if err != nil {
		e.logger.Warn("dynamic_filter: staged bloom decode failed; proceeding without filter",
			"filter_id", s.FilterID, "error", err)
		return nil, 0, false
	}
	if len(art.Bloom) == 0 {
		return nil, 0, false
	}
	return art.Bloom, art.BloomMask, true
}

func int64ToAny(keyType string, v int64) any {
	switch keyType {
	case "int32", "date":
		return int32(v)
	default:
		return v
	}
}

// finalizeDynamicFilterEmits drains each emit op's partial Snapshot,
// serializes via the WDF1 codec, uploads as a sideband artifact at
// queries/<queryID>/dynfilter/<stageID>/<taskID>-<filterID>.wdf in the
// task's ResultBucket, and appends a DynamicFilterPartialRef to result.
//
// One PUT per FilterID. The partial is small (sized to the configured
// BloomBits, default tens of KB to a few MB at the eligibility ceiling),
// and rides alongside the task's main result upload — no extra round-trip
// against the task critical path beyond the PUT itself.
//
// On error: the partial upload is best-effort. If serialization or upload
// fails we log and continue; the coordinator's union step treats missing
// partials as no-op (a missing partial degrades the filter's selectivity,
// it doesn't corrupt results). Probe-side execution is always safe with
// or without the dynamic filter.
func (e *Executor) finalizeDynamicFilterEmits(
	ctx context.Context,
	task distributed.Task,
	emitOps []*exec.DynamicFilterEmitOp,
	specs []distributed.DynamicFilterEmit,
	result *distributed.ResultNotification,
) error {
	if len(emitOps) == 0 {
		return nil
	}
	if task.ResultBucket == "" {
		// No upload destination; nothing we can do. Coordinator will see no
		// partials for this task and degrade the filter gracefully.
		e.logger.Warn("dynamic_filter_emit: empty ResultBucket; skipping partial upload",
			"task_id", task.ID, "filters", len(emitOps))
		return nil
	}
	// Index spec by FilterID to attach the KeyColumn label to the ref. ops
	// and specs are 1:1 by construction (buildDynamicFilterEmitOps preserves
	// order), so we could rely on positional matching; the FilterID lookup
	// keeps the code robust if that invariant ever changes.
	specByID := make(map[string]distributed.DynamicFilterEmit, len(specs))
	for _, s := range specs {
		specByID[s.FilterID] = s
	}

	for _, op := range emitOps {
		snap := op.Snapshot()
		spec := specByID[snap.FilterID]

		// A partial that saw rows but never resolved its key column is
		// POISON: it is missing keys that exist in the stream, and a bloom
		// missing live keys falsely rejects rows at the consume side. Skip
		// the upload; the coordinator's completeness check (one partial per
		// task) then drops the whole FilterID and consumers run unfiltered.
		if snap.Unresolved {
			e.logger.Warn("dynamic_filter_emit: key column unresolved; partial withheld",
				"task_id", task.ID, "filter_id", snap.FilterID)
			continue
		}

		artifact := &distributed.DynamicFilterArtifact{
			KeyType:   snap.KeyType,
			HasRange:  snap.HasRange,
			Min:       snap.Min,
			Max:       snap.Max,
			RowCount:  snap.RowCount,
			Bloom:     snap.Bloom,
			BloomMask: snap.BloomMask,
		}

		var buf bytes.Buffer
		if err := distributed.EncodeDynamicFilterArtifact(&buf, artifact); err != nil {
			e.logger.Warn("dynamic_filter_emit: encode failed; partial dropped",
				"task_id", task.ID, "filter_id", snap.FilterID, "error", err)
			continue
		}
		// Count-in-key: when the coordinator stamped the stage's partial
		// count, the key carries it (".of<N>") so an attach-on-arrival
		// consumer listing the prefix learns the completeness target from
		// any single partial. The prefix comes verbatim from the spec (the
		// coordinator stamps the same value into the consumer side — single
		// source of truth for the layout). Task retries reuse the task ID →
		// same key, idempotent overwrite, so the count never double-counts.
		key := fmt.Sprintf("queries/%s/dynfilter/%s/%s-%s.wdf",
			task.QueryID, task.StageID, task.ID, snap.FilterID)
		if spec.PartialPrefix != "" && spec.StagePartials > 0 {
			key = fmt.Sprintf("%s%s-%s.of%d.wdf",
				spec.PartialPrefix, task.ID, snap.FilterID, spec.StagePartials)
		}
		reader := bytes.NewReader(buf.Bytes())
		if _, err := e.store.Put(ctx, task.ResultBucket, key,
			reader, int64(buf.Len()), "application/octet-stream"); err != nil {
			e.logger.Warn("dynamic_filter_emit: upload failed; partial dropped",
				"task_id", task.ID, "filter_id", snap.FilterID,
				"bucket", task.ResultBucket, "key", key, "error", err)
			continue
		}
		result.DynamicFilterPartials = append(result.DynamicFilterPartials,
			distributed.DynamicFilterPartialRef{
				FilterID: snap.FilterID,
				Bucket:   task.ResultBucket,
				Key:      key,
			})
	}
	return nil
}
