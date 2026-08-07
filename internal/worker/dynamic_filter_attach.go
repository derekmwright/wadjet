package worker

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// Attach-on-arrival consume support
// (docs/design/attach-on-arrival-dynamic-filters.md): a Deferred
// DynamicFilterSpec names a merged-artifact key that may not exist yet when
// the task starts. The scan begins unfiltered; a per-worker singleflight
// poller watches the key and delivers the bloom to every waiting fragment,
// which installs it mid-scan (drop-only semantics — results identical).

// deferredBloomPollInterval bounds attach latency after the coordinator
// stages the merged artifact. Each poll is one small GET against the result
// bucket, singleflighted per key across all tasks on the worker.
const deferredBloomPollInterval = 250 * time.Millisecond

// deferredBloomPollDeadline caps a poll loop's lifetime. A filter that was
// withheld (incomplete partials) never appears; the deadline stops the loop
// well after any real scan would have ended. Consumers that already
// finished simply never install it.
const deferredBloomPollDeadline = 10 * time.Minute

// guardFinalizeWait bounds how long a guarded re-emit waits at finalize for
// its upstream bloom before flushing the buffer unguarded. Only paid when
// the mid-scan finished before the upstream dim chain — rare (dims are
// tiny), and the wider-bloom alternative costs the downstream fact filter
// its entire selectivity, so a generous bound is the right trade.
const guardFinalizeWait = 10 * time.Second

// pendingBloom is the shared result slot for one staged-artifact key. done
// closes exactly once; after that, bloom/mask/ok are immutable.
type pendingBloom struct {
	done  chan struct{}
	bloom []uint64
	mask  uint64
	ok    bool
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
			// on a binary that doesn't stamp ".of<N>" counts.
			if bloom, mask, ok := e.tryLoadStagedBloom(ctx, spec.BloomBucket, spec.BloomKey); ok {
				p.bloom, p.mask, p.ok = bloom, mask, true
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
					p.bloom, p.mask, p.ok = acc.bloom, acc.mask, true
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
		if err != nil || len(art.Bloom) == 0 {
			continue // treat as transient (mirrors tryLoadStagedBloom): retry next tick
		}
		if len(art.Bloom) > dynamicFilterMaxBloomWords {
			acc.seen[obj.Key] = true // structural: never counts (coordinator drops it too)
			continue
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
		}
	}
	return out
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

// taskBlobPriority reports whether a marshaled Task carries the Priority
// flag, without decoding the full task. Task blobs are JSON
// (distributed.Marshal); a partial decode into a one-field struct is
// microseconds against multi-second tasks. Used by the gRPC dispatch
// handler to route latency-critical tasks onto the priority queue.
func taskBlobPriority(blob []byte) bool {
	var p struct {
		Priority bool `json:"priority"`
	}
	if err := json.Unmarshal(blob, &p); err != nil {
		return false
	}
	return p.Priority
}

// tryLoadStagedBloom is the quiet fetch used by the poll loop — a missing
// key is the EXPECTED state until the coordinator stages the merge, so it
// must not log per attempt (4 attempts/second).
func (e *Executor) tryLoadStagedBloom(ctx context.Context, bucket, key string) ([]uint64, uint64, bool) {
	rc, _, err := e.store.Get(ctx, bucket, key)
	if err != nil {
		return nil, 0, false
	}
	defer rc.Close()
	art, err := distributed.DecodeDynamicFilterArtifact(rc)
	if err != nil || len(art.Bloom) == 0 {
		return nil, 0, false
	}
	return art.Bloom, art.BloomMask, true
}
