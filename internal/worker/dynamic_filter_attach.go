package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Attach-on-arrival consume support
// (docs/design/attach-on-arrival-dynamic-filters.md): a Deferred
// DynamicFilterSpec names a merged-artifact key that may not exist yet when
// the task starts. The scan begins unfiltered; a per-worker singleflight
// poller watches the key and delivers the bloom to every waiting fragment,
// which installs it mid-scan (drop-only semantics — results identical).

// dfLateGroupAttach gates full-layer delivery of resolved deferred filters
// (§Full-layer delivery): iterator-level forwarding on fragment scans and
// the whole deferred promotion on shuffle tasks. Kill switch
// WADJET_DF_LATE_GROUP_ATTACH=0 restores the pre-2026-08-09 behavior —
// row-level-only fragment installs, deferred specs inert on shuffle tasks.
var dfLateGroupAttach = os.Getenv("WADJET_DF_LATE_GROUP_ATTACH") != "0"

// deferredBloomPollInterval bounds attach latency after the coordinator
// stages the merged artifact. Each poll is one small GET against the result
// bucket, singleflighted per key across all tasks on the worker.
const deferredBloomPollInterval = 250 * time.Millisecond

// deferredBloomPollDeadline caps a poll loop's lifetime. A filter that was
// withheld (incomplete partials) never appears; the deadline stops the loop
// well after any real scan would have ended. Consumers that already
// finished simply never install it.
const deferredBloomPollDeadline = 10 * time.Minute

// Guarded re-emits wait at finalize until the guard's poll TERMINATES —
// resolve or genuine withhold (the poller's own deadline caps the wait).
// The first SF100 pair (2026-08-07, trt 372e2da) proved any shorter cap
// wrong twice over: a 10s cap expired while the dim chain was 300ms-10s+
// away, flushing UNGUARDED and destroying the downstream fact filter's
// entire selectivity (5 hop-B tasks, buffered=240000 dropped=0, cold
// Q05/Q21 +50%). Waiting is strictly ≤ the old start barrier — the
// barrier also waited for the same dim chain, before scanning a single
// row — and the unguarded flush now happens only when WAIT mode would
// also have run filterless (withheld upstream).

// pendingBloom is the shared result slot for one staged-artifact key. done
// closes exactly once; after that, every field is immutable.
type pendingBloom struct {
	done  chan struct{}
	bloom []uint64
	mask  uint64
	ok    bool
	// Build-key range from the resolved artifact(s), when every contributing
	// artifact carried one. Late attach forwards it to the iterator layer so
	// a deferred filter gets the same row-group range pruning a dispatch-time
	// spec's HasRange/Min/Max would have provided.
	hasRange bool
	min, max int64
	// source records how the bloom resolved — "merged" (coordinator-staged
	// union) or "partials" (consumer-side incremental union) — with the
	// partial count for the latter. A/B telemetry only.
	source   string
	partials int
}

// resolved returns the bloom if the poll completed successfully.
func (p *pendingBloom) resolved() ([]uint64, uint64, bool) {
	select {
	case <-p.done:
		return p.bloom, p.mask, p.ok
	default:
		return nil, 0, false
	}
}

// pollDeferredBloom returns the pending slot for a staged key, starting the
// singleflight poll goroutine on first request. The loop runs on a
// background context (task contexts are per-attempt and shorter-lived than
// the interest in the filter) with a hard deadline; the map entry is
// removed when the loop exits, so a later request after success re-fetches
// once and resolves immediately.
func (e *Executor) pollDeferredBloom(spec distributed.DynamicFilterSpec) *pendingBloom {
	key := spec.BloomBucket + "/" + spec.BloomKey
	e.dfAttachMu.Lock()
	if e.dfAttachPolls == nil {
		e.dfAttachPolls = make(map[string]*pendingBloom)
	}
	if p, ok := e.dfAttachPolls[key]; ok {
		e.dfAttachMu.Unlock()
		return p
	}
	p := &pendingBloom{done: make(chan struct{})}
	e.dfAttachPolls[key] = p
	e.dfAttachMu.Unlock()

	go func() {
		e.logger.Info("dynamic_filter: late attach poll started",
			"filter_id", spec.FilterID, "bucket", spec.BloomBucket,
			"key", spec.BloomKey, "partial_prefix", spec.PartialPrefix)
		defer func() {
			e.dfAttachMu.Lock()
			delete(e.dfAttachPolls, key)
			e.dfAttachMu.Unlock()
			close(p.done)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), deferredBloomPollDeadline)
		defer cancel()
		ticker := time.NewTicker(deferredBloomPollInterval)
		defer ticker.Stop()
		acc := &partialUnion{filterID: spec.FilterID, seen: make(map[string]bool)}
		for {
			// Merged key first: it is authoritative (the coordinator's
			// completeness-gated union) and the fallback when the emitter ran
			// on a binary that doesn't stamp ".of<N>" counts. A zero-byte
			// TOMBSTONE at the key means the coordinator withheld the filter
			// — terminate now (ok=false) instead of waiting out the
			// deadline; consumers stay unfiltered and guarded re-emits flush
			// unguarded, both the correct withhold degradations.
			art, ok, tombstone := e.tryLoadStagedBloom(ctx, spec.BloomBucket, spec.BloomKey)
			if tombstone {
				e.logger.Info("dynamic_filter: filter withheld (tombstone)",
					"filter_id", spec.FilterID, "key", spec.BloomKey)
				return
			}
			if ok {
				p.bloom, p.mask, p.ok = art.Bloom, art.BloomMask, true
				p.hasRange, p.min, p.max = art.HasRange, art.Min, art.Max
				p.source = "merged"
				return
			}
			// Incremental publication: list the emitter's partial prefix and
			// OR partials as they land. Activation requires FULL ".of<N>"
			// coverage — a union missing any task's keys falsely rejects
			// rows, so partial coverage never filters (mirrors the
			// coordinator's completeness gate).
			if spec.PartialPrefix != "" {
				if e.tryUnionPartials(ctx, spec, acc) {
					if acc.bloom == nil {
						// Complete but every partial was empty: the emitter
						// matched nothing anywhere. Terminate without a
						// bloom (consumers/guards proceed unfiltered — the
						// conservative pre-existing degradation).
						e.logger.Info("dynamic_filter: incremental partial union complete (all empty)",
							"filter_id", spec.FilterID, "partials", acc.merged)
						return
					}
					p.bloom, p.mask, p.ok = acc.bloom, acc.mask, true
					p.hasRange, p.min, p.max = acc.rangeUnion()
					p.source = "partials"
					p.partials = acc.merged
					e.logger.Info("dynamic_filter: incremental partial union complete",
						"filter_id", spec.FilterID, "partials", acc.merged)
					return
				}
			}
			select {
			case <-ctx.Done():
				e.logger.Info("dynamic_filter: late attach poll expired",
					"filter_id", spec.FilterID, "key", spec.BloomKey)
				return
			case <-ticker.C:
			}
		}
	}()
	return p
}

// partialUnion accumulates one filter's per-task partial artifacts across
// poll ticks. seen holds keys already consumed OR permanently rejected
// (decode failure, size mismatch, oversize) — rejected keys never count
// toward completeness, so a poisoned partial degrades to "filter never
// activates", exactly matching the coordinator's withhold semantics.
type partialUnion struct {
	filterID string
	seen     map[string]bool
	bloom    []uint64
	mask     uint64
	merged   int
	expected int // parsed from the ".of<N>" key suffix; 0 = not yet known
	// Range union across partials. A non-empty partial WITHOUT a range
	// breaks the union (an incomplete range falsely prunes); empty partials
	// contribute no keys and leave it intact.
	hasRange    bool
	rangeBroken bool
	min, max    int64
}

// rangeUnion returns the folded build-key range, valid only when every
// non-empty partial carried one.
func (u *partialUnion) rangeUnion() (hasRange bool, min, max int64) {
	if u.rangeBroken || !u.hasRange {
		return false, 0, 0
	}
	return true, u.min, u.max
}

// dynamicFilterMaxBloomWords mirrors the coordinator-side cap (16 MiB): a
// partial whose bloom exceeds it is rejected rather than unioned, and the
// filter degrades to never-activating — the coordinator withholds it too.
const dynamicFilterMaxBloomWords = 16 * 1024 * 1024 / 8

// tryUnionPartials runs one discovery sweep: list the prefix, fetch+union
// any new partials for this filter, and report whether coverage is complete.
// Transient failures (list error, fetch error) leave the key unseen so the
// next tick retries; structural rejections mark it seen-but-uncounted.
func (e *Executor) tryUnionPartials(ctx context.Context, spec distributed.DynamicFilterSpec, acc *partialUnion) bool {
	objs, err := e.store.List(ctx, spec.BloomBucket, objstore.ListOptions{Prefix: spec.PartialPrefix})
	if err != nil {
		return false
	}
	for _, obj := range objs {
		if acc.seen[obj.Key] {
			continue
		}
		n, ok := parsePartialCount(obj.Key, acc.filterID)
		if !ok {
			continue // other filter's partial, or un-stamped legacy key
		}
		if n > acc.expected {
			acc.expected = n
		}
		rc, _, err := e.store.Get(ctx, spec.BloomBucket, obj.Key)
		if err != nil {
			continue // transient: retry next tick
		}
		art, err := distributed.DecodeDynamicFilterArtifact(rc)
		rc.Close()
		if err != nil {
			continue // treat as transient (mirrors tryLoadStagedBloom): retry next tick
		}
		if len(art.Bloom) == 0 {
			// EMPTY partial: a zero-row emitter task's legitimate "no keys
			// from me" contribution — counts toward completeness, adopts no
			// shape (mirrors the coordinator merge; see
			// mergeBuildStatsFromPartials).
			acc.seen[obj.Key] = true
			acc.merged++
			continue
		}
		if len(art.Bloom) > dynamicFilterMaxBloomWords {
			acc.seen[obj.Key] = true // structural: never counts (coordinator drops it too)
			continue
		}
		if art.HasRange {
			if !acc.hasRange {
				acc.hasRange, acc.min, acc.max = true, art.Min, art.Max
			} else {
				if art.Min < acc.min {
					acc.min = art.Min
				}
				if art.Max > acc.max {
					acc.max = art.Max
				}
			}
		} else {
			acc.rangeBroken = true
		}
		if acc.bloom == nil {
			acc.bloom = append([]uint64(nil), art.Bloom...)
			acc.mask = art.BloomMask
		} else if len(art.Bloom) != len(acc.bloom) {
			// Size mismatch is a planner bug (emit specs give every task the
			// same BloomBits). Reject rather than corrupt the union; the
			// coordinator's merge drops it too, so neither side activates.
			acc.seen[obj.Key] = true
			continue
		} else {
			for i, w := range art.Bloom {
				acc.bloom[i] |= w
			}
		}
		acc.seen[obj.Key] = true
		acc.merged++
	}
	return acc.expected > 0 && acc.merged >= acc.expected
}

// parsePartialCount extracts N from a partial key of the form
// ".../<taskID>-<filterID>.of<N>.wdf". Returns ok=false for keys that
// belong to a different filter or carry no count stamp.
func parsePartialCount(key, filterID string) (int, bool) {
	name := key
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	if !strings.HasSuffix(name, ".wdf") {
		return 0, false
	}
	name = strings.TrimSuffix(name, ".wdf")
	i := strings.LastIndex(name, ".of")
	if i < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(name[i+len(".of"):])
	if err != nil || n <= 0 {
		return 0, false
	}
	if !strings.HasSuffix(name[:i], "-"+filterID) {
		return 0, false
	}
	return n, true
}

// registerDeferredBlooms builds the fragment's deferred-consume registry:
// one poll slot per Deferred spec, keyed by FilterID. Shared by the source
// bloom wrapper (mid-scan install) and guarded re-emits (retro-filter at
// settle) so both observe the same resolution.
func (e *Executor) registerDeferredBlooms(specs []distributed.DynamicFilterSpec) map[string]*deferredBloomFilter {
	var out map[string]*deferredBloomFilter
	for _, s := range specs {
		if !s.Deferred || s.BloomBucket == "" || s.BloomKey == "" {
			continue
		}
		if out == nil {
			out = make(map[string]*deferredBloomFilter, len(specs))
		}
		out[s.FilterID] = &deferredBloomFilter{
			pb:       e.pollDeferredBloom(s),
			filterID: s.FilterID,
			column:   s.TargetColumn,
			keyType:  s.KeyType,
		}
	}
	return out
}

// pendingInSpecOrder flattens a deferred registry back into the spec list's
// order — installs must be deterministic across runs for A/B comparability.
func pendingInSpecOrder(specs []distributed.DynamicFilterSpec, deferred map[string]*deferredBloomFilter) []*deferredBloomFilter {
	var out []*deferredBloomFilter
	for _, s := range specs {
		if d, ok := deferred[s.FilterID]; ok {
			out = append(out, d)
		}
	}
	return out
}

// resolvedScanForms builds the two scan-pushdown forms of a resolved
// deferred filter: the iterator-level BloomScanFilter (+DynamicRange when
// the artifact carried one) for row-group pruning, and the row-level
// BloomFilterOp. A dynamic filter is one pushdown object — arrival timing
// must not change which layers it reaches, so late attach delivers exactly
// what a dispatch-time spec would have.
func (d *deferredBloomFilter) resolvedScanForms(ctx context.Context) (*exec.BloomFilterOp, []exec.DynamicRange, *exec.BloomScanFilter) {
	bsf := &exec.BloomScanFilter{
		Bloom:     d.pb.bloom,
		BloomMask: d.pb.mask,
		Column:    d.column,
		UseIntKey: true,
	}
	var ranges []exec.DynamicRange
	if d.pb.hasRange {
		ranges = []exec.DynamicRange{{
			Column:   d.column,
			MinValue: int64ToAny(d.keyType, d.pb.min),
			MaxValue: int64ToAny(d.keyType, d.pb.max),
		}}
	}
	op := exec.NewBloomFilterOp(d.pb.bloom, d.pb.mask, []string{d.column}, true)
	if err := op.Init(ctx); err != nil {
		op = nil
	}
	return op, ranges, bsf
}

// dynamicFilterAppender is implemented by scan sources that accept
// additional iterator-level dynamic filters mid-scan (group pruning for
// row groups not yet assigned, plus every later file the source opens).
type dynamicFilterAppender interface {
	AddDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter)
}

// promoteDeferredFilters polls each pending deferred filter once, without
// blocking. Resolved filters are handed to install as their scan-pushdown
// forms (a nil op means the row-level Init failed; the iterator-level
// forms are always valid). Polls that terminated without a bloom are
// dropped — the scan stays unfiltered for that filter, the documented
// withhold degradation. Returns the still-pending remainder. Must run on
// the scan's producer goroutine.
func promoteDeferredFilters(ctx context.Context, pending []*deferredBloomFilter, logger *slog.Logger, taskID, stageID string, groupLevel bool, install func(op *exec.BloomFilterOp, ranges []exec.DynamicRange, bsf *exec.BloomScanFilter)) []*deferredBloomFilter {
	remaining := pending[:0]
	for _, d := range pending {
		if _, _, ok := d.pb.resolved(); !ok {
			select {
			case <-d.pb.done:
				// Poll finished without a bloom (withheld/expired): drop the
				// pending entry, scan stays unfiltered for this filter.
				if logger != nil {
					logger.Info("dynamic_filter: late attach never arrived",
						"task_id", taskID, "stage_id", stageID,
						"filter_id", d.filterID, "batches_seen", d.batches)
				}
			default:
				d.batches++
				remaining = append(remaining, d)
			}
			continue
		}
		op, ranges, bsf := d.resolvedScanForms(ctx)
		install(op, ranges, bsf)
		if logger != nil {
			source := d.pb.source
			if source == "" {
				source = "merged"
			}
			logger.Info("dynamic_filter: late attach installed",
				"task_id", taskID, "stage_id", stageID,
				"filter_id", d.filterID, "batches_before_attach", d.batches,
				"source", source, "partials", d.pb.partials,
				"group_level", groupLevel, "has_range", d.pb.hasRange)
		}
	}
	return remaining
}

// pendingBloomProbe adapts a pendingBloom poll slot to the exec.GuardProbe
// interface consumed by guarded re-emit ops.
type pendingBloomProbe struct{ pb *pendingBloom }

func (p pendingBloomProbe) TryResolve() ([]uint64, uint64, bool) { return p.pb.resolved() }

func (p pendingBloomProbe) Done() bool {
	select {
	case <-p.pb.done:
		return true
	default:
		return false
	}
}

func (p pendingBloomProbe) Wait(ctx context.Context) ([]uint64, uint64, bool) {
	select {
	case <-p.pb.done:
		return p.pb.bloom, p.pb.mask, p.pb.ok
	case <-ctx.Done():
		return nil, 0, false
	}
}

// taskBlobPriority reports whether a marshaled Task carries the Priority /
// PriorityDeep flags, without decoding the full task. Task blobs are JSON
// (distributed.Marshal); a partial decode into a two-field struct is
// microseconds against multi-second tasks. Used by the gRPC dispatch
// handler to route latency-critical tasks onto the priority queues (leaf
// vs deep — disjoint slot pools, see the lane-split comment in worker.go).
func taskBlobPriority(blob []byte) (priority, deep bool) {
	var p struct {
		Priority     bool `json:"priority"`
		PriorityDeep bool `json:"priority_deep"`
	}
	if err := json.Unmarshal(blob, &p); err != nil {
		return false, false
	}
	return p.Priority, p.PriorityDeep
}

// tryLoadStagedBloom is the quiet fetch used by the poll loop — a missing
// key is the EXPECTED state until the coordinator stages the merge, so it
// must not log per attempt (4 attempts/second). tombstone=true reports a
// zero-byte object: the coordinator's explicit "withheld, stop waiting"
// marker.
func (e *Executor) tryLoadStagedBloom(ctx context.Context, bucket, key string) (art *distributed.DynamicFilterArtifact, ok, tombstone bool) {
	rc, _, err := e.store.Get(ctx, bucket, key)
	if err != nil {
		return nil, false, false
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, false, false
	}
	// Zero BYTES read (not store-reported size, which not every impl
	// populates) is the tombstone test — a real artifact always has a
	// header, so an empty body can only be the coordinator's marker.
	if len(data) == 0 {
		return nil, false, true
	}
	art, err = distributed.DecodeDynamicFilterArtifact(bytes.NewReader(data))
	if err != nil || len(art.Bloom) == 0 {
		return nil, false, false
	}
	return art, true, false
}
