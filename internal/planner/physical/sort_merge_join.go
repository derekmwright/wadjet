package physical

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// SortMergeJoinsPlanned counts joins routed through the sort-merge path,
// process-wide. Observability: the TPC-H forced-on gate test uses it to
// prove the route fired (and, on the default config, that it stayed
// dormant); ops dashboards can sample it the same way.
var SortMergeJoinsPlanned atomic.Int64

// shouldSortMergeJoin decides whether an inner equi-join takes the
// sort-merge path (docs/design/sort-merge-join.md §3.4): both sides'
// estimated post-selectivity bytes must reach SortMergeJoinBytes. An
// unknown estimate (no scan root — e.g. a join-on-join input — or a missing
// manifest) keeps the hash path: SMJ is an opt-in for provably-big sides,
// never a default for uncertainty.
func (p *Planner) shouldSortMergeJoin(node *logical.Node) bool {
	if p.SortMergeJoinBytes <= 0 || len(node.Children) < 2 {
		return false
	}
	buildBytes, ok := p.estimateSubtreeBytes(node.Children[1])
	if !ok || buildBytes < p.SortMergeJoinBytes {
		return false
	}
	probeBytes, ok := p.estimateSubtreeBytes(node.Children[0])
	if !ok || probeBytes < p.SortMergeJoinBytes {
		return false
	}
	return true
}

// buildSortMergeJoin constructs the local sort-merge join: the build (right)
// side drains into the operator in a goroutine — overlapping with probe-side
// preparation AND probe-side consumption, since the two side buffers are
// independent — and the probe (left) side runs as a child pipeline inside
// the returned source adapter. Mirrors buildJoin's hash wiring for alias,
// spill, and output-filter semantics.
func (p *Planner) buildSortMergeJoin(ctx context.Context, node *logical.Node, leftKeys, rightKeys []string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	SortMergeJoinsPlanned.Add(1)
	j := exec.NewSortMergeJoin(leftKeys, rightKeys)

	// Set build-side table alias for column disambiguation in self-joins
	if alias := findScanAlias(node.Children[1]); alias != "" {
		j.BuildTableAlias = alias
	}
	// Multi-table build subtrees carry per-column origin aliases so each
	// duplicate qualifies under its OWNING scan (nil for single-scan builds).
	j.BuildColOrigins = subtreeNamingOf(node.Children[1]).buildColOrigins()
	if sm := p.getSpillManager(); sm != nil {
		j.Spill = sm
	}
	// Push output filter into the join to avoid materializing intermediate
	// columns not needed by upstream operators (HashJoinProbe.OutputFilter
	// parity — the shared schema helper applies it identically).
	if len(node.NeededColumns) > 0 {
		filter := make(map[string]bool, len(node.NeededColumns))
		for _, col := range node.NeededColumns {
			filter[col] = true
		}
		j.OutputFilter = filter
	}

	rightSource, rightOps, _, err := p.buildPipeline(ctx, node.Children[1])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building sort-merge join right side: %w", err)
	}
	buildSource := &pipelineSource{source: rightSource, ops: rightOps}

	buildDone := make(chan struct{})
	var buildErr error
	go func() {
		defer close(buildDone)
		// The build runs on its own goroutine so it can overlap with the
		// probe side's preparation, which also means no recover above it:
		// a panic anywhere in the build pipeline ended the process rather
		// than the query (#511). buildErr is read after the barrier, so
		// delivering it there is all the adapter needs.
		//
		// Registered after close(buildDone) so it runs FIRST on the way
		// out: buildErr is set before the barrier opens.
		defer exec.CatchQueryPanic(ctx, "sort-merge join build", func(err error) {
			buildErr = fmt.Errorf("sort-merge join build side: %w", err)
		})
		if err := j.Build(ctx, buildSource); err != nil {
			buildErr = fmt.Errorf("sort-merge join build side: %w", err)
		}
	}()

	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		<-buildDone // prevent goroutine leak
		return nil, nil, nil, fmt.Errorf("building sort-merge join left side: %w", err)
	}

	return &smjSourceAdapter{
		childSource: leftSource,
		childOps:    leftOps,
		join:        j,
		barrier:     buildDone,
		buildErr:    &buildErr,
	}, nil, &exec.CollectSink{}, nil
}

// smjProbeSink feeds the probe pipeline into the join while deferring
// Finalize: the pipeline's own Finalize call would race the build goroutine
// (SortMergeJoin.Finalize requires the build phase complete and starts the
// merge). The adapter finalizes after the build barrier instead.
type smjProbeSink struct {
	*exec.SortMergeJoin
}

func (s smjProbeSink) Finalize(context.Context) error { return nil }

// smjSourceAdapter wraps the probe child pipeline + sort-merge join into a
// Source (the sortSourceAdapter pattern): the first Next runs the probe
// pipeline into the join — concurrently with the build goroutine — waits for
// the build barrier, finalizes the merge, and streams joined batches.
type smjSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	join        *exec.SortMergeJoin
	barrier     <-chan struct{}
	buildErr    *error
	initialized bool
}

func (s *smjSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (s *smjSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.initialized {
		s.initialized = true
		pipe := &exec.Pipeline{
			Source:  s.childSource,
			Ops:     s.childOps,
			Sink:    smjProbeSink{s.join},
			Workers: innerPipelineWorkers(s.childSource),
		}
		if err := pipe.Run(ctx); err != nil {
			<-s.barrier // the build goroutine owns join state; let it finish
			return nil, err
		}
		<-s.barrier
		if *s.buildErr != nil {
			return nil, *s.buildErr
		}
		if err := s.join.Finalize(ctx); err != nil {
			return nil, err
		}
	}
	return s.join.Next(ctx)
}

func (s *smjSourceAdapter) Close() error {
	s.join.Close()
	return s.childSource.Close()
}

func (s *smjSourceAdapter) RowsScanned() int64 {
	if sp, ok := s.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}
