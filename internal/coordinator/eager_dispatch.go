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
func (c *Coordinator) eagerManifestPublisher(rootQueryID, stageID string, feed *eagerFeed) func(taskID string, attempt int, files []string, workerID string, final bool) {
	if !c.config.EagerDispatch || !c.config.StreamingExchange || rootQueryID == "" || c.nc == nil {
		return nil
	}
	subject := distributed.EagerManifestSubject(rootQueryID, stageID)
	return func(taskID string, attempt int, files []string, workerID string, final bool) {
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
			feed.appendReplay(m)
		}
		data, err := distributed.Marshal(m)
		if err != nil {
			c.logger.Error("eager manifest marshal failed",
				"stage_id", stageID, "task_id", taskID, "error", err)
			return
		}
		if err := c.nc.Publish(subject, data); err != nil {
			c.logger.Warn("eager manifest publish failed (replay list covers consumer retries)",
				"stage_id", stageID, "task_id", taskID, "error", err)
			return
		}
		EagerManifestsPublished.Add(1)
		c.logger.Info("eager manifest published",
			"stage_id", stageID, "task_id", taskID, "attempt", attempt,
			"files", len(files), "worker", workerID, "final", final)
	}
}
