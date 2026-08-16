package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// buildPreComputedAggregateMeta converts a candidate + cache paths into the
// metadata shape that travels on physical.Stage and later becomes the
// distributed.PreComputedAggregate wire message. Extracted so the
// coordinator-routing and test paths share one construction site.
func buildPreComputedAggregateMeta(
	cand physical.AggregateShuffleCandidate,
	stages []physical.Stage,
	cacheFiles []string,
) (physical.PreComputedAggregateMeta, error) {
	byID := make(map[string]physical.Stage, len(stages))
	for _, s := range stages {
		byID[s.ID] = s
	}
	agg, ok := byID[cand.AggregateStageID]
	if !ok {
		return physical.PreComputedAggregateMeta{}, fmt.Errorf("aggregate stage %q not found", cand.AggregateStageID)
	}
	scan, ok := byID[cand.InputScanID]
	if !ok {
		return physical.PreComputedAggregateMeta{}, fmt.Errorf("input scan %q not found", cand.InputScanID)
	}
	return physical.PreComputedAggregateMeta{
		InputTable:  scan.TableName,
		GroupByCols: append([]string(nil), agg.GroupByCols...),
		AggSpecs:    append([]physical.AggSpec(nil), agg.AggSpecs...),
		CacheFiles:  append([]string(nil), cacheFiles...),
	}, nil
}

// aggregateShuffleThreshold gates aggregate-shuffle detection independently
// of shuffleBuildThreshold. The base-table shuffle threshold (4 GB) was
// calibrated for SF100 where lineitem is ~37 GB; at SF10 compressed lineitem
// is ~3.7 GB so that threshold never fired. But the broadcast-duplication
// cost of a derived aggregate is real at SF10 too — each worker would
// materialize the full decompressed aggregate output (~6 GB in memory for
// Q17's 5M groups × row width) if we don't pre-compute it centrally.
//
// 1 GB makes SF10 lineitem trip it; SF100 easily crosses it. Tests lower to
// 1 byte to force the path. Declared as var so tests can override.
var aggregateShuffleThreshold int64 = 1 * 1024 * 1024 * 1024 // 1 GB

// aggregateShuffleTimeout caps the pre-compute task's wall time. The
// aggregate SQL scans its input table in full; at SF100 scale this can
// easily take several minutes, so the timeout mirrors buildCacheTimeout.
const aggregateShuffleTimeout = 8 * time.Minute

// preComputeDerivedAggregate dispatches a single pipeline task that runs the
// inner aggregate SQL and returns the S3 paths where the result was written.
// This is Phase 1 of the shuffle-distributed-aggregate work: compute once,
// broadcast by passing the resulting paths to every probe-split task as a
// pre-computed input. The probe tasks' worker-planner hook (landing in a
// follow-up commit) substitutes the aggregate subtree with a streaming source
// of those paths.
//
// Phase 1 runs the aggregate on a SINGLE worker (no partitioning). This is
// safe when the aggregate output is small enough to fit on one worker (Q17's
// ~5M groups × 16 B ≈ 80 MB at SF100). Phase 1b will extend to
// partial-then-merge distributed aggregation when the output is too large to
// broadcast.
//
// Returns an empty slice (not error) if the aggregate produced zero rows —
// probe tasks that receive an empty pre-compute input fall back to the
// existing in-pipeline execution.
func (c *Coordinator) preComputeDerivedAggregate(
	parentCtx context.Context,
	parentQueryID string,
	cand physical.AggregateShuffleCandidate,
	stages []physical.Stage,
) ([]string, error) {
	sqlText, err := physical.BuildAggregateShuffleSQL(cand, stages)
	if err != nil {
		return nil, fmt.Errorf("build aggregate pre-compute SQL: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, aggregateShuffleTimeout)
	defer cancel()

	cacheQueryID := fmt.Sprintf("ac-%s-%s", parentQueryID, cand.AggregateStageID)
	resultPrefix := fmt.Sprintf("queries/%s/aggregate-cache/%s/", parentQueryID, cand.AggregateStageID)

	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      cacheQueryID,
		StageID:      "aggregate-cache-compute",
		Type:         distributed.TaskTypePipeline,
		SQLText:      sqlText,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		ResultPrefix: resultPrefix,
		CreatedAt:    time.Now(),
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}

	trackerStages := map[string]*StageInfo{
		"aggregate-cache-compute": {
			StageID:    "aggregate-cache-compute",
			Type:       distributed.TaskTypePipeline,
			TotalTasks: 1,
		},
	}
	c.tracker.Register(cacheQueryID, sqlText, trackerStages, []string{"aggregate-cache-compute"})
	c.tracker.Start(cacheQueryID)
	defer c.tracker.Delete(cacheQueryID)

	done := make(chan struct{}, 1)
	subject := distributed.QueryResultSubject(cacheQueryID)
	var resultPath string
	var resultInline []byte
	var resultRows int64
	var resultErr string
	var resultMu sync.Mutex

	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if unmarshalErr := distributed.Unmarshal(msg.Data, &r); unmarshalErr != nil {
			return
		}
		c.workers.MarkWorkerSeen(r.WorkerID)
		resultMu.Lock()
		if !r.Success {
			resultErr = r.Error
		} else {
			resultPath = r.ResultPath
			resultInline = r.InlineData
			resultRows = r.NumRows
		}
		resultMu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing for aggregate pre-compute result: %w", err)
	}
	defer sub.Unsubscribe()

	c.logger.Info("aggregate pre-compute: dispatching",
		"parent_query", parentQueryID,
		"agg_stage", cand.AggregateStageID,
		"input_scan", cand.InputScanAlias,
		"input_bytes", cand.InputScanBytes,
		"sql", sqlText)

	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing aggregate pre-compute task: %w", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	resultMu.Lock()
	defer resultMu.Unlock()

	if resultErr != "" {
		return nil, fmt.Errorf("aggregate pre-compute failed: %s", resultErr)
	}
	if resultPath != "" {
		c.logger.Info("aggregate pre-compute: complete",
			"agg_stage", cand.AggregateStageID, "rows", resultRows, "path", resultPath)
		return []string{resultPath}, nil
	}
	if resultRows == 0 {
		c.logger.Info("aggregate pre-compute: produced zero rows (empty input) — falling back",
			"agg_stage", cand.AggregateStageID)
		return nil, nil
	}
	if len(resultInline) > 0 {
		// Worker inlined the result below inlineResultThreshold. Promote to S3
		// so downstream probe tasks have a file to stream from. Mirrors the
		// same rescue path in preScanOneTable.
		path := resultPrefix + task.ID + ".wshf"
		if _, putErr := c.catalog.Store().Put(ctx, c.config.ResultBucket, path,
			bytes.NewReader(resultInline), int64(len(resultInline)),
			"application/octet-stream"); putErr != nil {
			return nil, fmt.Errorf("promoting inline aggregate pre-compute result to S3: %w", putErr)
		}
		c.logger.Info("aggregate pre-compute: complete (promoted inline)",
			"agg_stage", cand.AggregateStageID, "rows", resultRows, "path", path)
		return []string{path}, nil
	}
	return nil, fmt.Errorf("aggregate pre-compute returned %d rows but no result path or inline data", resultRows)
}
