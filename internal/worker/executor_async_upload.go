package worker

import (
	"context"
	"os"

	"github.com/citc-tech/wadjet/internal/distributed"
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
