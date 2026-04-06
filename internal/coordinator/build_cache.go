package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// buildCacheThreshold is the minimum build-side table size (in bytes) that
// triggers the broadcast cache path. Tables below this threshold are small
// enough that each worker reading them independently is fine; above this,
// the N× duplication causes OOM (e.g., orders at SF100 ≈ 15GB, partsupp ≈ 8GB).
const buildCacheThreshold = 2 * 1024 * 1024 * 1024 // 2 GB

// preScanBuildTables pre-scans large build-side tables before dispatching
// probe-split pipeline tasks. For each build scan stage whose EstimatedBytes
// exceeds buildCacheThreshold, this dispatches a single-worker scan pipeline
// task that reads the table and writes the result to S3. The returned map
// (alias → result file paths) is embedded in each probe-split pipeline task
// as PreScannedInputs, so workers load the shared cache instead of re-scanning
// the source N times.
//
// This eliminates the N× build-side memory duplication that OOMs Q09 at SF100
// (e.g., 3 workers × 15GB orders hash tables = ~45GB peak vs 15GB with caching).
func (c *Coordinator) preScanBuildTables(ctx context.Context, parentQueryID string, sql string, stages []physical.Stage, probeAlias string) (map[string][]string, error) {
	largeBuildScans := physical.LargeBuildScans(stages, probeAlias, buildCacheThreshold)
	if len(largeBuildScans) == 0 {
		return nil, nil
	}

	c.logger.Info("build-side broadcast cache: pre-scanning large build tables",
		"parent_query", parentQueryID,
		"tables", len(largeBuildScans),
	)

	// Pre-scan each large build table concurrently using scan-only pipeline tasks.
	// Each task selects all rows from the table and writes the result as a .wshf file.
	type scanResult struct {
		alias string
		files []string
		err   error
	}
	results := make([]scanResult, len(largeBuildScans))
	var wg sync.WaitGroup

	for i, stage := range largeBuildScans {
		wg.Add(1)
		go func(idx int, s physical.Stage) {
			defer wg.Done()
			files, err := c.preScanOneTable(ctx, parentQueryID, s)
			results[idx] = scanResult{alias: s.ScanAlias, files: files, err: err}
		}(i, stage)
	}
	wg.Wait()

	cacheMap := make(map[string][]string, len(largeBuildScans))
	for _, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("pre-scanning build table %s: %w", r.alias, r.err)
		}
		if len(r.files) > 0 {
			cacheMap[r.alias] = r.files
			c.logger.Info("build cache ready",
				"alias", r.alias, "files", len(r.files))
		}
	}

	return cacheMap, nil
}

// preScanOneTable dispatches a single pipeline task to scan the build table
// and write its rows to S3. Returns the result file paths.
func (c *Coordinator) preScanOneTable(ctx context.Context, parentQueryID string, stage physical.Stage) ([]string, error) {
	cacheQueryID := fmt.Sprintf("bc-%s-%s", parentQueryID, stage.ScanAlias)
	resultPrefix := fmt.Sprintf("queries/%s/build-cache/%s/", parentQueryID, stage.ScanAlias)

	// Construct minimal SQL to scan just this table with the columns needed.
	// We select all columns so the workers get full rows for hash table construction.
	scanSQL := fmt.Sprintf("SELECT * FROM %s", stage.TableName)

	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      cacheQueryID,
		StageID:      "build-cache-scan",
		Type:         distributed.TaskTypePipeline,
		SQLText:      scanSQL,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		ResultPrefix: resultPrefix,
		CreatedAt:    time.Now(),
	}

	// Cluster routing
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}

	// Register a minimal tracker for this ephemeral query.
	trackerStages := map[string]*StageInfo{
		"build-cache-scan": {
			StageID:    "build-cache-scan",
			Type:       distributed.TaskTypePipeline,
			TotalTasks: 1,
		},
	}
	c.tracker.Register(cacheQueryID, scanSQL, trackerStages, []string{"build-cache-scan"})
	c.tracker.Start(cacheQueryID)
	defer c.tracker.Delete(cacheQueryID)

	// Subscribe for results before publishing to avoid the race.
	done := make(chan struct{}, 1)
	subject := distributed.QueryResultSubject(cacheQueryID)
	var resultPath string
	var resultErr string
	var resultMu sync.Mutex

	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if unmarshalErr := distributed.Unmarshal(msg.Data, &r); unmarshalErr != nil {
			return
		}
		resultMu.Lock()
		if !r.Success {
			resultErr = r.Error
		} else {
			resultPath = r.ResultPath
		}
		resultMu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing for build cache result: %w", err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing build cache scan task: %w", err)
	}

	// Wait for completion (with context timeout).
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	resultMu.Lock()
	defer resultMu.Unlock()

	if resultErr != "" {
		return nil, fmt.Errorf("build cache scan failed: %s", resultErr)
	}
	if resultPath == "" {
		// Empty table — no rows to cache. Workers will get an empty scan result.
		return nil, nil
	}
	return []string{resultPath}, nil
}
