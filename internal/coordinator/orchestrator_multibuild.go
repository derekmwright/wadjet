package coordinator

// orchestrator_multibuild.go — dormant multi-build shuffle orchestrator.
// Ported from feat/broadcast-hazard-mitigation @ 67ba055.
// NOT wired into dispatch; called only from the smoke test until Phase 3.
//
// Phase 3 will:
//   - Replace shuffleGroupStub with the real hazard-detection types
//     (physical.BroadcastHazard → shuffleGroup conversion).
//   - Wire orchestrateMultiBuildShuffle into the coordinator dispatch path
//     via the distribution-property Exchange insertion pass.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// shuffleGroup bundles the build sides that share a single probe alias and
// probe keys, so they can be dispatched as one multi-build shuffle stage:
// one probe-side shuffle + N build-side shuffles, all writing into the same
// partition layout.
//
// TODO(phase-3): replace with the real hazard-derived type once the
// distribution-property Exchange pass produces these groups from
// physical.BroadcastHazard detection.
type shuffleGroup struct {
	ProbeAlias   string
	ProbeKeys    []string
	Builds       []shuffleBuildSide
	JoinStageIDs []string
}

// shuffleBuildSide describes one build-side participant of a shuffleGroup.
//
// TODO(phase-3): align with physical.ShuffleCandidate fields.
type shuffleBuildSide struct {
	JoinStageID string
	Alias       string
	Keys        []string
	Bytes       int64
}

// MultiShuffleLayout describes the shard file layout produced by an N+1
// shuffle (M builds + 1 probe). Distinct from ShuffleLayout (single-build)
// in shuffle_orchestrator.go.
//
// BuildAliases is the ordered list of build-side scan aliases.
// BuildShardFiles[alias][p] returns the S3 keys for that build's partition p.
type MultiShuffleLayout struct {
	ProbeAlias      string
	NumPartitions   int
	BuildAliases    []string
	BuildShardFiles map[string][][]string
	ProbeShardFiles [][]string
}

// orchestrateMultiBuildShuffle runs N+1 shuffle stages (M builds + 1 probe)
// in parallel and returns the resulting shard layout. M=1 is the single-build
// case; M=2 is the Q21 pattern. Failure of any side cancels all.
//
// NOT called from dispatch in Phase 2. Phase 3 will wire this in via the
// Exchange insertion pass once the distribution-property model is complete.
func (c *Coordinator) orchestrateMultiBuildShuffle(
	ctx context.Context,
	queryID string,
	group shuffleGroup,
	stages []physical.Stage,
	workerCount int,
) (*MultiShuffleLayout, error) {
	numParts := workerCount * shufflePartitionMultiplier

	// Locate the probe scan and each build scan stage.
	probeStage, err := findScanStageByAlias(stages, group.ProbeAlias)
	if err != nil {
		return nil, fmt.Errorf("probe scan %q: %w", group.ProbeAlias, err)
	}
	buildStages := make(map[string]physical.Stage, len(group.Builds))
	buildAliases := make([]string, 0, len(group.Builds))
	for _, b := range group.Builds {
		s, err := findScanStageByAlias(stages, b.Alias)
		if err != nil {
			return nil, fmt.Errorf("build scan %q: %w", b.Alias, err)
		}
		buildStages[b.Alias] = s
		buildAliases = append(buildAliases, b.Alias)
	}

	c.logger.Info("multi-build shuffle: starting",
		"query_id", queryID,
		"probe_alias", group.ProbeAlias,
		"build_aliases", buildAliases,
		"join_stage_ids", group.JoinStageIDs,
		"num_partitions", numParts,
		"worker_count", workerCount,
		"build_count", len(group.Builds),
	)

	g, gctx := errgroup.WithContext(ctx)
	buildShards := make(map[string][][]string, len(group.Builds))
	var buildShardsMu sync.Mutex
	var probeShards [][]string

	for _, b := range group.Builds {
		b := b
		bs := buildStages[b.Alias]
		g.Go(func() error {
			shards, err := c.runShuffleSide(gctx, queryID, "build-"+b.Alias, bs, b.Keys, numParts, workerCount)
			if err != nil {
				return fmt.Errorf("build-side shuffle for %s: %w", b.Alias, err)
			}
			buildShardsMu.Lock()
			buildShards[b.Alias] = shards
			buildShardsMu.Unlock()
			return nil
		})
	}
	g.Go(func() error {
		shards, err := c.runShuffleSide(gctx, queryID, "probe", probeStage, group.ProbeKeys, numParts, workerCount)
		if err != nil {
			return fmt.Errorf("probe-side shuffle for %s: %w", group.ProbeAlias, err)
		}
		probeShards = shards
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("orchestrateMultiBuildShuffle: %w", err)
	}

	c.logger.Info("multi-build shuffle: complete",
		"query_id", queryID,
		"num_partitions", numParts,
		"build_count", len(buildShards),
	)

	return &MultiShuffleLayout{
		ProbeAlias:      group.ProbeAlias,
		NumPartitions:   numParts,
		BuildAliases:    buildAliases,
		BuildShardFiles: buildShards,
		ProbeShardFiles: probeShards,
	}, nil
}

// findScanStageByAlias locates a scan stage by its ScanAlias. Returns an
// error when no scan with that alias exists.
func findScanStageByAlias(stages []physical.Stage, alias string) (physical.Stage, error) {
	for _, s := range stages {
		if s.Type == "scan" && s.ScanAlias == alias {
			return s, nil
		}
	}
	return physical.Stage{}, fmt.Errorf("no scan stage with alias %q", alias)
}

// buildMultiShufflePipelineTasks creates one pipeline task per worker using a
// MultiShuffleLayout. Each worker receives the probe and all build shard files
// for its assigned partitions via PreScannedInputs.
//
// Distinct from buildShufflePipelineTasks (single-build) in shuffle_orchestrator.go.
// TODO(phase-3): call from the Exchange-insertion dispatch path.
func buildMultiShufflePipelineTasks(
	queryID, sql, resultBucket string,
	layout *MultiShuffleLayout,
	workerCount int,
) []distributed.Task {
	resultPrefix := fmt.Sprintf("queries/%s/pipeline-0/", queryID)
	tasks := make([]distributed.Task, 0, workerCount)
	for w := 0; w < workerCount; w++ {
		parts := assignPartitionsToWorker(w, workerCount, layout.NumPartitions)
		if len(parts) == 0 {
			continue
		}
		var probeFiles []string
		for _, p := range parts {
			probeFiles = append(probeFiles, layout.ProbeShardFiles[p]...)
		}
		preScanned := map[string][]string{
			layout.ProbeAlias: probeFiles,
		}
		for _, alias := range layout.BuildAliases {
			var buildFiles []string
			for _, p := range parts {
				buildFiles = append(buildFiles, layout.BuildShardFiles[alias][p]...)
			}
			preScanned[alias] = buildFiles
		}
		tasks = append(tasks, distributed.Task{
			ID:               uuid.New().String()[:8],
			QueryID:          queryID,
			StageID:          "pipeline-0",
			Type:             distributed.TaskTypePipeline,
			SQLText:          sql,
			DataBucket:       resultBucket,
			ResultBucket:     resultBucket,
			ResultPrefix:     resultPrefix,
			PartialAggregate: true,
			PreScannedInputs: preScanned,
			CreatedAt:        time.Now(),
		})
	}
	return tasks
}
