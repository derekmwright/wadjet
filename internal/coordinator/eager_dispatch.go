package coordinator

import (
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// Eager consumer dispatch, Phase C1 (docs/design/eager-consumer-dispatch.md).
//
// This file owns the coordinator side of the manifest feed: as each task
// of an eager-edge producer stage reaches a successful terminal state,
// the coordinator republishes a compact ProducerTaskManifest on
// EagerManifestSubject(root, stage). Consumers' manifest sources
// subscribe before dispatch and receive already-published manifests via
// the task spec (subscribe-then-replay, memo §3.2), so publication here
// is fire-and-forget Core NATS: a lost message is repaired by the replay
// list on consumer retry, and the S3/peer fetch tiers are unaffected.

// EagerManifestsPublished counts manifests published across all queries —
// the mechanism marker proving the eager path engaged in a benchmark log
// (the SortMergeJoinsPlanned/DynamicFiltersPlanned observability
// precedent).
var EagerManifestsPublished atomic.Int64

// EagerEdgesPlanned counts consumer stages that cleared dispatch on an
// eager feed instead of the done barrier — the memo §8 activation marker
// for the SF100 pair (grep "eager dispatch: consumer cleared early" /
// this counter's log line in benchmark.log).
var EagerEdgesPlanned atomic.Int64

// eagerManifestPublisher returns a taskRetrier onSuccess hook that
// publishes one manifest per successful producer task, or nil when eager
// dispatch is disabled or the root query ID is unavailable (legacy
// pipeline tasks without scratch anchors never have eager consumers).
//
// feed, when non-nil, receives every manifest into its replay list BEFORE
// the NATS publish, making the feed the single source of truth for
// late-built consumers: a consumer task built after the append sees the
// manifest in its Replay; one built before has already subscribed (or will
// catch the republisher's next re-send).
func (c *Coordinator) eagerManifestPublisher(rootQueryID, stageID string, feed *eagerFeed) func(taskID string, attempt int, files []string, workerID string, final bool, partBytes []int64) {
	if !c.config.EagerDispatch || !c.config.StreamingExchange || rootQueryID == "" || c.nc == nil {
		return nil
	}
	subject := distributed.EagerManifestSubject(rootQueryID, stageID)
	return func(taskID string, attempt int, files []string, workerID string, final bool, partBytes []int64) {
		m := distributed.ProducerTaskManifest{
			StageID:  stageID,
			TaskID:   taskID,
			Attempt:  attempt,
			Files:    files,
			WorkerID: workerID,
			PeerAddr: c.workers.PeerAddr(workerID),
			Final:    final,
		}
		if feed != nil {
			// Replay before accounting: a consumer released by the
			// decision threshold must see every counted task's manifest
			// in its Replay snapshot.
			feed.appendReplay(m)
			feed.noteCompletion(partBytes)
		}
		// Clearance-driven activation (§13 precondition 2): no NATS
		// traffic until a consumer actually cleared on this feed. A feed
		// that never activates costs nothing beyond the in-memory replay
		// list; a consumer clearing later gets history from its task-spec
		// Replay snapshot plus the activation backlog flush, and the
		// republisher heals the remaining loss windows.
		if feed == nil || !feed.isActive() {
			return
		}
		if !c.publishEagerManifest(subject, m) {
			return
		}
		c.logger.Info("eager manifest published",
			"stage_id", stageID, "task_id", taskID, "attempt", attempt,
			"files", len(files), "worker", workerID, "final", final)
	}
}

// publishEagerManifest marshals and publishes one manifest, counting it
// on success. Publish failures are fire-and-forget by design (the replay
// list and republisher cover consumer retries).
func (c *Coordinator) publishEagerManifest(subject string, m distributed.ProducerTaskManifest) bool {
	data, err := distributed.Marshal(m)
	if err != nil {
		c.logger.Error("eager manifest marshal failed",
			"stage_id", m.StageID, "task_id", m.TaskID, "error", err)
		return false
	}
	if err := c.nc.Publish(subject, data); err != nil {
		c.logger.Warn("eager manifest publish failed (replay list covers consumer retries)",
			"stage_id", m.StageID, "task_id", m.TaskID, "error", err)
		return false
	}
	EagerManifestsPublished.Add(1)
	return true
}

// flushEagerManifestBacklog publishes every manifest accumulated before
// the feed's activation. Called once, at first consumer clearance: the
// cleared consumer's task spec already carries these in its Replay, but
// the flush closes the snapshot→subscribe window immediately instead of
// waiting out a republisher tick, and it is what makes activation-gated
// publication observable at all for producers that finished before any
// consumer cleared.
func (c *Coordinator) flushEagerManifestBacklog(f *eagerFeed) {
	if c.nc == nil {
		return
	}
	subject := distributed.EagerManifestSubject(f.rootQueryID, f.manifestStageID)
	backlog := f.replaySnapshot()
	published := 0
	for _, m := range backlog {
		if c.publishEagerManifest(subject, m) {
			published++
		}
	}
	if published > 0 {
		c.logger.Info("eager manifest backlog flushed",
			"stage_id", f.manifestStageID, "manifests", published)
	}
}
