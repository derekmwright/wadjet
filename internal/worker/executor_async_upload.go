package worker

import (
	"context"
	"os"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// asyncUploadEligible reports whether this task's stage outputs may be
// reported complete before their S3 upload lands (streaming exchange
// Phase B). Requires the LocalStageCache (the retained local file is what
// peers fetch and what the background upload reads) and a derivable root
// query ID (upload cancellation is root-scoped).
func (e *Executor) asyncUploadEligible(task *distributed.Task) (root string, ok bool) {
	if !task.AsyncUpload || e.uploads == nil || e.localCache == nil || e.spillDir == "" {
		return "", false
	}
	root = distributed.TaskRootQueryID(task)
	return root, root != ""
}

// stageOutputRefusedForTerminalQuery reports whether this task's stage
// output must not be uploaded because its query is already terminal on
// this worker. A one-shot per-query cleanup cannot win a race against a
// straggler upload; ADR-0009's tombstone is the structurally honest side
// of that race, and it already governs the ASYNC path (uploadManager's
// queryState returns nil for a tombstoned root). Extending it to the
// synchronous uploads closes the last producer of a re-created
// queries/<id>/* prefix (#625 M3, ADR-0028).
//
// A refusal is not a task failure: the query is terminal, so nothing will
// ever read this output. It is counted so an operator can see it.
func (e *Executor) stageOutputRefusedForTerminalQuery(task *distributed.Task) bool {
	if e.uploads == nil {
		return false
	}
	root := distributed.TaskRootQueryID(task)
	if root == "" || !e.uploads.IsTerminal(root) {
		return false
	}
	StageUploadsRefused.Add(1)
	e.logger.Debug("stage output not uploaded: query already terminal",
		"task_id", task.ID, "root_query_id", root)
	return true
}

// finishStageOutputAsync completes one finalized local stage-output file
// the Phase-B way: adopt it into the LocalStageCache (peers and the
// background upload read the adopted copy), record it on the result, and
// write the small-payload KV tier from local bytes. Returns the background
// upload job. ok=false when adoption fails (cross-device rename etc.) —
// the caller must upload synchronously instead, because the local file
// dies with the task spill dir and nothing else could ever produce the
// durable copy.
func (e *Executor) finishStageOutputAsync(ctx context.Context, task *distributed.Task, key, localPath string, size int64, compress bool, result *distributed.ResultNotification) (uploadJob, bool) {
	adopted := e.localCache.Adopt(task.QueryID, key, localPath)
	if adopted == "" {
		return uploadJob{}, false
	}
	result.ResultFiles = append(result.ResultFiles, key)
	result.UploadPendingKeys = append(result.UploadPendingKeys, key)
	result.SizeBytes += size

	// KV fast-read tier for small payloads, written from the local copy —
	// no S3 dependency, so the tier stays as hot as on the sync path.
	if e.resultKV != nil && size <= natsKVResultThreshold {
		if payload, readErr := os.ReadFile(adopted); readErr == nil {
			if _, putErr := e.resultKV.Put(ctx, natsKVKey(key), payload); putErr != nil {
				e.logger.Debug("KV cache write failed (upload pending, peers cover reads)",
					"task_id", task.ID, "key", key, "err", putErr)
			}
		}
	}

	return uploadJob{
		bucket:   task.ResultBucket,
		key:      key,
		srcPath:  adopted,
		compress: compress,
		tmpDir:   e.spillDir,
		size:     size,
	}, true
}
