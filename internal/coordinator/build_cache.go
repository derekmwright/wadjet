package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// buildCacheThreshold is the minimum build-side table size (in bytes) that
// triggers the broadcast cache path. Tables below this threshold are small
// enough that each worker reading them independently is fine; above this,
// the N× duplication causes OOM (e.g., orders at SF100 ≈ 15GB, partsupp ≈ 8GB).
const buildCacheThreshold = 2 * 1024 * 1024 * 1024 // 2 GB

// buildCacheGroupSize is the number of Parquet files per pre-scan task. It
// also bounds the size of each individual cache (.wshf) file, which matters
// because cachedFileStreamSource currently downloads each cache file fully
// into memory via getFileData → io.ReadAll before streaming chunks out of
// the resulting byte slice. With group size 8 a single SF100 partsupp cache
// file is ~10 GB, which OOM-kills any worker that needs to load it during
// the probe phase. Group size 2 caps each cache file at ~2.5 GB, comfortably
// within the per-worker memory budget on c7gd.4xlarge (32 GB) instances.
//
// The right long-term fix is to make cachedFileStreamSource read incrementally
// from S3 (or download to local NVMe and mmap), at which point this constant
// can go back up to optimise S3 round-trip count. Until then, prefer many
// small cache files over a few huge ones.
//
// Declared as var (not const) so regression tests can lower it further to
// force multiple groups on tiny SF0.01 tables and exercise the file-splitting
// correctness path.
var buildCacheGroupSize = 2

// buildCacheTimeout is the maximum time to wait for a single build cache
// pre-scan to complete. If exceeded, the coordinator falls back to per-worker
// scanning (which may OOM at very large scale but is functionally correct).
const buildCacheTimeout = 8 * time.Minute

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
	threshold := int64(buildCacheThreshold)
	if c.BuildCacheThreshold > 0 {
		threshold = c.BuildCacheThreshold
	}
	largeBuildScans := physical.LargeBuildScans(stages, probeAlias, threshold)
	if len(largeBuildScans) == 0 {
		return nil, nil
	}

	c.logger.Info("build-side broadcast cache: pre-scanning large build tables",
		"parent_query", parentQueryID,
		"tables", len(largeBuildScans),
	)

	// Pre-scan each large build table concurrently. Each table is further split
	// into groups of buildCacheGroupSize files to avoid a single worker accumulating
	// all rows of a large table (e.g., partsupp ~11.8GB) in RAM before writing to S3.
	type scanResult struct {
		alias string
		files []string
		err   error
	}

	// Count total goroutines: one per (table, fileGroup) pair.
	type tableGroup struct {
		stage    physical.Stage
		groupIdx int
		files    []string
	}
	var groups []tableGroup
	for _, stage := range largeBuildScans {
		fileGroups := chunkFiles(stage.ScanFiles, buildCacheGroupSize)
		for gi, fg := range fileGroups {
			groups = append(groups, tableGroup{stage: stage, groupIdx: gi, files: fg})
		}
	}

	// Run all groups in parallel under a shared cancellable context. As soon
	// as one group fails, errgroup cancels groupCtx so the other in-flight
	// pre-scans abort instead of running to completion against a query that's
	// already known to fail. Without this, a Q09 SF100 build cache failure on
	// one orders group would still pay for the other 8 groups' minutes of S3
	// scan time before surfacing the error.
	results := make([]scanResult, len(groups))
	g, groupCtx := errgroup.WithContext(ctx)
	for i, tg := range groups {
		idx, tg := i, tg
		g.Go(func() error {
			files, err := c.preScanOneTable(groupCtx, parentQueryID, tg.stage, tg.groupIdx, tg.files)
			results[idx] = scanResult{alias: tg.stage.ScanAlias, files: files, err: err}
			if err != nil {
				return fmt.Errorf("pre-scanning build table %s: %w", tg.stage.ScanAlias, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Merge results: collect all output file paths per alias.
	cacheMap := make(map[string][]string, len(largeBuildScans))
	for _, r := range results {
		cacheMap[r.alias] = append(cacheMap[r.alias], r.files...)
	}
	for alias, files := range cacheMap {
		if len(files) == 0 {
			delete(cacheMap, alias)
			continue
		}
		c.logger.Info("build cache ready", "alias", alias, "files", len(files))
	}

	return cacheMap, nil
}

// prunedScanColumns returns the subset of stage.Columns that actually exist on
// the table's schema. The optimizer's column-pruning pass over-approximates by
// putting every referenced column from the surrounding join chain on every
// scan node, so a partsupp:1 stage might list region/part/nation columns
// alongside its own. The downstream scanner silently filters those out, but
// the SQL parser used by the build-cache pre-scan task doesn't — so we have to
// intersect against the real schema here before constructing SELECT lists.
//
// Returns nil on any catalog lookup error or when stage.Columns is empty,
// which causes the caller to fall back to SELECT *.
func (c *Coordinator) prunedScanColumns(ctx context.Context, stage physical.Stage) []string {
	if len(stage.Columns) == 0 {
		return nil
	}
	tbl, err := c.catalog.GetTable(ctx, stage.TableName)
	if err != nil || tbl == nil {
		return nil
	}
	valid := make(map[string]bool, len(tbl.Schema.Columns))
	for _, col := range tbl.Schema.Columns {
		valid[col.Name] = true
	}
	// Walk stage.Columns in order so the SELECT list is deterministic
	// regardless of map iteration order, and dedupe along the way.
	seen := make(map[string]bool, len(stage.Columns))
	out := make([]string, 0, len(stage.Columns))
	for _, c := range stage.Columns {
		if seen[c] {
			continue
		}
		seen[c] = true
		if valid[c] {
			out = append(out, c)
		}
	}
	return out
}

// chunkFiles splits a slice of file paths into chunks of at most size elements.
func chunkFiles(files []string, size int) [][]string {
	if len(files) == 0 {
		return [][]string{nil} // one group with no filter = full table scan
	}
	var chunks [][]string
	for len(files) > 0 {
		n := size
		if n > len(files) {
			n = len(files)
		}
		chunks = append(chunks, files[:n])
		files = files[n:]
	}
	return chunks
}

// preScanOneTable dispatches a single pipeline task to scan a subset of files
// from the build table and write the rows to S3. groupIdx disambiguates multiple
// tasks for the same table when the file list is split into chunks.
// If fileSubset is non-nil, a ScanFileFilter is set on the task so only those
// files are read; a nil fileSubset means scan all files (full table).
func (c *Coordinator) preScanOneTable(parentCtx context.Context, parentQueryID string, stage physical.Stage, groupIdx int, fileSubset []string) ([]string, error) {
	ctx, cancel := context.WithTimeout(parentCtx, buildCacheTimeout)
	defer cancel()
	cacheQueryID := fmt.Sprintf("bc-%s-%s-%d", parentQueryID, stage.ScanAlias, groupIdx)
	resultPrefix := fmt.Sprintf("queries/%s/build-cache/%s/%d/", parentQueryID, stage.ScanAlias, groupIdx)

	// Construct SQL to scan just this table with the columns the downstream
	// query actually uses. The optimizer's column-pruning pass populates
	// stage.Columns with all columns referenced anywhere in the join/subquery
	// chain — including columns from OTHER tables (e.g. partsupp:1's stage
	// in Q02 lists r_name, p_mfgr, s_phone, etc. that don't exist in
	// partsupp). The downstream scanner silently filters those out against
	// the parquet schema, but the SQL parser doesn't, so we have to
	// intersect with the actual table schema before building the SELECT
	// list. Falling back to SELECT * is correct but bloats the cache by
	// 3-5x for wide tables like orders.
	scanSQL := fmt.Sprintf("SELECT * FROM %s", stage.TableName)
	if cols := c.prunedScanColumns(parentCtx, stage); len(cols) > 0 {
		scanSQL = fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), stage.TableName)
	}

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

	// Restrict the scan to only the assigned file subset so each group task
	// reads a bounded slice of the table rather than the full dataset.
	//
	// CRITICAL: key by stage.TableName, NOT stage.ScanAlias. The pre-scan SQL
	// is "SELECT * FROM <TableName>" which, when re-planned on the worker, has
	// exactly one scan with alias == TableName (no ":N" suffix). If the original
	// query had duplicate scans of this table (e.g. Q02's correlated subquery:
	// ScanAlias="partsupp:1"), keying the filter by ScanAlias would miss the
	// worker's lookup and every group task would silently scan the full table —
	// defeating the entire purpose of file-group splitting and OOM'ing at SF100.
	if fileSubset != nil {
		task.ScanFileFilter = map[string][]string{stage.TableName: fileSubset}
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
	if resultPath != "" {
		return []string{resultPath}, nil
	}
	if resultRows == 0 {
		// Genuinely empty scan — nothing to cache. Workers will get no rows
		// from this group; merged cache map correctly reflects that.
		return nil, nil
	}
	if len(resultInline) > 0 {
		// Worker inlined the result because it fit under inlineResultThreshold
		// (default 512KB). Without writing it to S3 ourselves, downstream probe-
		// split workers would have no file to read from and would silently miss
		// the rows in this group — producing wrong Q02 row counts at SF0.01 and
		// worse corruption at larger scales. Promote inline data to S3 so the
		// cache path is always backed by a real file.
		path := resultPrefix + task.ID + ".wshf"
		if _, putErr := c.catalog.Store().Put(ctx, c.config.ResultBucket, path,
			bytes.NewReader(resultInline), int64(len(resultInline)),
			"application/octet-stream"); putErr != nil {
			return nil, fmt.Errorf("promoting inline build cache result to S3: %w", putErr)
		}
		return []string{path}, nil
	}
	// NumRows > 0 but neither ResultPath nor InlineData — the worker lost
	// the data somewhere. Treating this as empty would silently drop rows
	// from the build cache and produce wrong query results downstream.
	return nil, fmt.Errorf("build cache scan returned %d rows but no result path or inline data", resultRows)
}

// orchestrateReplicate dispatches a StageExchangeReplicate. It adapts
// the pre-scan implementation (preScanBuildTables) to the stage-DAG
// dispatch interface. The pre-scan function is retained as the actual
// implementation until Phase 2 Task 19 deletes the legacy path.
//
// Phase 2 Task 12: shim added, but orchestrateReplicate is not yet
// called from any dispatch site. Task 14 introduces the stage-DAG
// dispatcher that routes to this function.
func (c *Coordinator) orchestrateReplicate(
	ctx context.Context,
	stage physical.Stage,
	parentQueryID string,
	sql string,
	allStages []physical.Stage,
	probeAlias string,
) (map[string][]string, error) {
	if stage.Type != physical.StageExchangeReplicate {
		return nil, fmt.Errorf(
			"orchestrate replicate: wrong stage type %q (expected %q)",
			stage.Type, physical.StageExchangeReplicate,
		)
	}
	return c.preScanBuildTables(ctx, parentQueryID, sql, allStages, probeAlias)
}
