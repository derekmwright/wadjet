package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
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

// pendingBloom is the shared result slot for one staged-artifact key. done
// closes exactly once; after that, bloom/mask/ok are immutable.
type pendingBloom struct {
	done  chan struct{}
	bloom []uint64
	mask  uint64
	ok    bool
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
		for {
			if bloom, mask, ok := e.tryLoadStagedBloom(ctx, spec.BloomBucket, spec.BloomKey); ok {
				p.bloom, p.mask, p.ok = bloom, mask, true
				return
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
