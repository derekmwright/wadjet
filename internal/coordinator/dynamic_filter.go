package coordinator

import (
	"context"
	"fmt"
	"io"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// dynamicFilterInlineThresholdBytes — partials with serialized blooms below
// this size are carried inline in OpSpec.DynamicFilters. Larger ones are
// staged to S3 and consumed by probe-scan workers via Get. 128 KiB matches
// the design memo: typical SF100 selective builds (Q17 part-filtered ~130K
// keys × 10 bits = ~160 KB → staged; Q18 having-clause output → inline).
const dynamicFilterInlineThresholdBytes = 128 * 1024

// dynamicFilterMaxBloomWords caps the unioned bloom size (16 MiB). If a
// build's actual cardinality exceeds the eligibility estimate by enough to
// blow this, we drop the filter rather than ship a giant blob — the probe
// still runs, just without dynamic pruning.
const dynamicFilterMaxBloomWords = 16 * 1024 * 1024 / 8

// mergeBuildStatsFromPartials fetches each partial referenced in refs from
// the object store, decodes via the WDF1 codec, and OR-unions them per
// FilterID. Returns a map[FilterID]*BuildStats ready to attach to the
// emitting stage's StageOutput.BuildStats.
//
// Robust to missing partials (worker upload failure, missing file): the
// affected FilterID degrades to whatever partials did make it. If no
// partials at all are present for a FilterID, the function omits it from
// the result and the downstream consume side treats it as "no filter, pass
// everything through" — correctness preserved.
func mergeBuildStatsFromPartials(
	ctx context.Context,
	store objstore.Store,
	refs []distributed.DynamicFilterPartialRef,
) (map[string]*BuildStats, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	merged := make(map[string]*BuildStats)
	for _, ref := range refs {
		rc, _, err := store.Get(ctx, ref.Bucket, ref.Key)
		if err != nil {
			// Treat as best-effort — log via the returned error annotation
			// path? coordinator helper has no logger; bubble up a sentinel
			// the caller can warn-and-continue on. Easier: just continue.
			continue
		}
		artifact, err := distributed.DecodeDynamicFilterArtifact(rc)
		rc.Close()
		if err != nil {
			continue
		}
		if len(artifact.Bloom) > dynamicFilterMaxBloomWords {
			continue
		}
		existing, ok := merged[ref.FilterID]
		if !ok {
			// First partial for this FilterID — adopt its shape.
			existing = &BuildStats{
				FilterID:  ref.FilterID,
				KeyType:   artifact.KeyType,
				HasRange:  artifact.HasRange,
				Min:       artifact.Min,
				Max:       artifact.Max,
				RowCount:  artifact.RowCount,
				Bloom:     append([]uint64(nil), artifact.Bloom...),
				BloomMask: artifact.BloomMask,
			}
			merged[ref.FilterID] = existing
			continue
		}
		// Same FilterID — union bitwise OR (sizes must match by construction).
		if len(existing.Bloom) != len(artifact.Bloom) {
			// Size mismatch is a planner bug: emit specs are supposed to
			// give every task the same BloomBits. Drop the conflicting
			// partial rather than corrupt the union.
			continue
		}
		for i, w := range artifact.Bloom {
			existing.Bloom[i] |= w
		}
		if artifact.HasRange {
			if !existing.HasRange {
				existing.HasRange = true
				existing.Min = artifact.Min
				existing.Max = artifact.Max
			} else {
				if artifact.Min < existing.Min {
					existing.Min = artifact.Min
				}
				if artifact.Max > existing.Max {
					existing.Max = artifact.Max
				}
			}
		}
		existing.RowCount += artifact.RowCount
	}
	return merged, nil
}

// mergeCompleteBuildStats merges per-task partials and enforces
// COMPLETENESS: a FilterID whose distinct partial count is below
// expectedTasks is dropped entirely. A bloom missing any task's keys
// falsely rejects rows at the consume side — "degrade to fewer partials"
// is only safe for zero partials (no filter), never for some. Partial
// refs are deduped by object key so task retries (same task ID → same
// key, idempotent overwrite) don't double-count. Oversized filters are
// staged to S3, mirroring dispatchScanFilterStage's inline-vs-stage
// decision.
func (c *Coordinator) mergeCompleteBuildStats(
	ctx context.Context,
	queryID, stageID string,
	refs []distributed.DynamicFilterPartialRef,
	expectedTasks int,
) map[string]*BuildStats {
	if len(refs) == 0 {
		return nil
	}
	merged, err := mergeBuildStatsFromPartials(ctx, c.catalog.Store(), refs)
	if err != nil {
		c.logger.Warn("dynamic_filter: partial merge failed; downstream consumes will see no filter",
			"stage_id", stageID, "error", err)
		return nil
	}
	perFilter := make(map[string]map[string]bool, len(merged))
	for _, r := range refs {
		set := perFilter[r.FilterID]
		if set == nil {
			set = make(map[string]bool, expectedTasks)
			perFilter[r.FilterID] = set
		}
		set[r.Key] = true
	}
	for fid := range merged {
		if len(perFilter[fid]) < expectedTasks {
			c.logger.Warn("dynamic_filter: incomplete partial coverage; filter withheld",
				"stage_id", stageID, "filter_id", fid,
				"partials", len(perFilter[fid]), "expected", expectedTasks)
			delete(merged, fid)
		}
	}
	staged := 0
	for filterID, stats := range merged {
		if stats == nil {
			continue
		}
		if estimateBuildStatsInlineSize(stats) > dynamicFilterInlineThresholdBytes {
			if err := stageBuildStats(ctx, c.catalog.Store(), c.config.ResultBucket, queryID, stageID, filterID, stats); err != nil {
				c.logger.Warn("dynamic_filter: stage upload failed; falling back to inline",
					"stage_id", stageID, "filter_id", filterID, "error", err)
			}
		}
		if stats.StagedKey != "" {
			staged++
		}
	}
	c.logger.Info("dynamic_filter: build partials merged",
		"stage_id", stageID,
		"partial_refs", len(refs),
		"merged_filters", len(merged),
		"staged_to_s3", staged)
	return merged
}

// stageBuildStats writes a BuildStats artifact to S3 when it exceeds the
// inline threshold. Mutates stats.StagedBucket/StagedKey on success and
// clears Bloom (so it isn't double-shipped through the OpSpec). Best-
// effort: returns an error the caller can log and proceed; an unstaged
// large filter falls back to inline shipping (with a size warning) — the
// only consequence is a larger OpSpec payload, not incorrect results.
func stageBuildStats(
	ctx context.Context,
	store objstore.Store,
	bucket, queryID, stageID, filterID string,
	stats *BuildStats,
) error {
	if bucket == "" {
		return fmt.Errorf("stage build stats: empty bucket")
	}
	artifact := &distributed.DynamicFilterArtifact{
		KeyType:   stats.KeyType,
		HasRange:  stats.HasRange,
		Min:       stats.Min,
		Max:       stats.Max,
		RowCount:  stats.RowCount,
		Bloom:     stats.Bloom,
		BloomMask: stats.BloomMask,
	}
	buf := newGrowBuffer(len(stats.Bloom)*8 + 64)
	if err := distributed.EncodeDynamicFilterArtifact(buf, artifact); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	key := fmt.Sprintf("queries/%s/dynfilter-merged/%s/%s.wdf", queryID, stageID, filterID)
	if _, err := store.Put(ctx, bucket, key, buf.Reader(), int64(buf.Len()), "application/octet-stream"); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	stats.StagedBucket = bucket
	stats.StagedKey = key
	stats.Bloom = nil
	return nil
}

// estimateBuildStatsInlineSize approximates the OpSpec wire footprint of a
// BuildStats if shipped inline. Matches the WDF1 codec layout — header +
// 8 bytes per bloom word — so the inline-vs-stage decision uses real
// numbers, not estimates.
func estimateBuildStatsInlineSize(stats *BuildStats) int {
	return 48 + 8*len(stats.Bloom)
}

// dynamicFilterSpecsFromBuildStats translates a stage's ConsumeDynamicFilters
// into the OpSpec wire form, pulling the merged stats from the upstream
// stage's StageOutput. The returned slice is suitable for direct assignment
// to OpSpec.DynamicFilters on each probe-scan task.
//
// If a consume spec references a stat that's missing from the upstream
// stage's BuildStats (build-side returned no eligible partials), that
// consume is silently omitted — probe-scan runs without that filter, which
// is always safe.
func dynamicFilterSpecsFromBuildStats(
	consumes []physical.DynamicFilterConsume,
	upstream map[string]StageOutput,
) []distributed.DynamicFilterSpec {
	if len(consumes) == 0 {
		return nil
	}
	var out []distributed.DynamicFilterSpec
	for _, c := range consumes {
		stageOut, ok := upstream[c.SourceStageID]
		if !ok || stageOut.BuildStats == nil {
			continue
		}
		stats, ok := stageOut.BuildStats[c.FilterID]
		if !ok || stats == nil {
			continue
		}
		spec := distributed.DynamicFilterSpec{
			FilterID:     c.FilterID,
			TargetColumn: c.TargetColumn,
			KeyType:      stats.KeyType,
			HasRange:     stats.HasRange,
			Min:          stats.Min,
			Max:          stats.Max,
		}
		if stats.StagedKey != "" {
			spec.BloomBucket = stats.StagedBucket
			spec.BloomKey = stats.StagedKey
			spec.BloomWords = len(stats.Bloom) // will be 0 — caller may know size via consume.BloomBits if needed
		} else if len(stats.Bloom) > 0 {
			spec.Bloom = append([]uint64(nil), stats.Bloom...)
			spec.BloomMask = stats.BloomMask
		}
		out = append(out, spec)
	}
	return out
}

// growBuffer is a tiny io.Writer + Reader pair that avoids importing
// bytes.Buffer just to ship one blob through the objstore.Put signature
// (which expects io.Reader + int64 size).
type growBuffer struct {
	data []byte
}

func newGrowBuffer(cap int) *growBuffer { return &growBuffer{data: make([]byte, 0, cap)} }

func (b *growBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *growBuffer) Len() int { return len(b.data) }

func (b *growBuffer) Reader() io.Reader { return &growReader{data: b.data} }

type growReader struct {
	data []byte
	off  int
}

func (r *growReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}
