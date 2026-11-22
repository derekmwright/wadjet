// SPDX-License-Identifier: MIT

// This file holds join sources for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// deferredJoinBridge creates a pipeline break for deferred hash join builds.
// Init runs the child pipeline (scan → early probes) with parallel workers,
// overlapping with the deferred build goroutine. After the child pipeline
// completes, it waits for the build barrier, then replays collected batches
// as a Source for the deferred probe operators.
type deferredJoinBridge struct {
	ChildSource exec.Source
	ChildOps    []exec.UnaryOperator
	Barrier     <-chan struct{}
	BuildErr    *error
	Workers     int
	spill       *memory.SpillManager

	collector *exec.SpillableBatchCollector
}

func (d *deferredJoinBridge) Init(ctx context.Context) error {
	// Run child pipeline (scan → early probes) to collect filtered batches.
	// This overlaps with the deferred build goroutine(s) running in background.
	// The collector charges the tracker and spills past pressure — the raw
	// BatchSink it replaces pinned the entire collected probe side in
	// untracked heap while the deferred build held its hash table (double
	// residency, invisible to SpillManager victim selection).
	d.collector = &exec.SpillableBatchCollector{Spill: d.spill}
	pipe := &exec.Pipeline{
		Source:  d.ChildSource,
		Ops:     d.ChildOps,
		Sink:    d.collector,
		Workers: d.Workers,
	}
	if err := pipe.Run(ctx); err != nil {
		d.collector.Release()
		// Wait for build goroutine to prevent leak
		select {
		case <-d.Barrier:
		default:
		}
		return fmt.Errorf("deferred join child pipeline: %w", err)
	}

	// Wait for deferred build to complete
	select {
	case <-d.Barrier:
	case <-ctx.Done():
		d.collector.Release()
		return ctx.Err()
	}
	if *d.BuildErr != nil {
		d.collector.Release()
		return *d.BuildErr
	}
	return nil
}

func (d *deferredJoinBridge) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return d.collector.NextReplay(ctx)
}

func (d *deferredJoinBridge) Close() error {
	if d.collector != nil {
		d.collector.Release()
	}
	return nil
}

// reverseBloomBridge runs the probe-side child pipeline first, builds a bloom
// filter from the collected join key values, injects it into the build-side
// scan, then signals the build goroutine to start. This drastically reduces
// build-side I/O for semi/anti joins where the probe result is much smaller
// than the build table (e.g. Q21: 60M lineitem rows reduced to ~2M when only
// ~500K orders match).
type reverseBloomBridge struct {
	ChildSource   exec.Source
	ChildOps      []exec.UnaryOperator
	rbBuildSource *exec.Source // pointer to the goroutine's build source (swappable)
	buildSource   exec.Source  // original build source (unwrapped)
	buildStart    chan struct{}
	Barrier       <-chan struct{}
	BuildErr      *error
	probeKey      string // probe-side column to extract bloom from
	buildKey      string // build-side column to filter
	Workers       int
	spill         *memory.SpillManager

	collector *exec.SpillableBatchCollector
}

func (rb *reverseBloomBridge) Init(ctx context.Context) error {
	// Phase 1: Run child pipeline to collect probe-side batches. The
	// spill-backed collector charges the tracker and degrades to disk past
	// pressure — the raw BatchSink it replaces pinned the full probe side
	// (e.g. a 1/N lineitem split at SF100) in untracked heap while the
	// downstream join held its build. It also stays clear of CollectSink's
	// Finalize→ToRows boxing, which this bridge never needed.
	rb.collector = &exec.SpillableBatchCollector{Spill: rb.spill}
	pipe := &exec.Pipeline{
		Source:  rb.ChildSource,
		Ops:     rb.ChildOps,
		Sink:    rb.collector,
		Workers: rb.Workers,
	}
	if err := pipe.Run(ctx); err != nil {
		rb.collector.Release()
		close(rb.buildStart)
		<-rb.Barrier
		return fmt.Errorf("reverse bloom child pipeline: %w", err)
	}

	// Phase 2: Build bloom from the collected key column — a streaming
	// pass over the collector (reads spilled runs back from disk), so the
	// bloom build adds no resident copy.
	//
	// The builder owns the key encoding for BOTH sides of this filter: it
	// freezes it from the inserted column's own type and hands it to the op
	// it produces. That used to be two independent derivations — the insert
	// side hashed raw bytes while the probe side hashed the join's canonical
	// [null-flag][value] key, and the probe's int-vs-bytes dispatch came from
	// a THIRD reading, of the collector's parquet schema. For any bytes-backed
	// key they never agreed, so the filter rejected every build row and the
	// join answered on an empty build side (#543).
	bb := exec.NewBloomBuilder(rb.collector.Rows())
	if bb != nil {
		// A key column that changes type between batches has no single right
		// encoding, so the builder refuses it. That is a reason to answer with
		// NO filter, never a reason to fail the query: the join is correct
		// without one. Iterate's own errors (a spilled run that will not read
		// back) are real and still propagate.
		var encErr error
		if err := rb.collector.Iterate(func(b *batch.RecordBatch) error {
			if encErr != nil {
				return nil
			}
			encErr = bb.Add(b, rb.probeKey)
			return nil
		}); err != nil {
			rb.collector.Release()
			close(rb.buildStart)
			<-rb.Barrier
			return fmt.Errorf("reverse bloom build: %w", err)
		}
		if encErr != nil {
			slog.Warn("reverse bloom key column is not one type across the probe output — filter not installed",
				"probe_key", rb.probeKey, "build_key", rb.buildKey, "err", encErr)
			bb = nil
		}
	}

	// Phase 3: Inject bloom filter into build-side pipeline.
	//
	// Two conditions before it goes in. The column has to have RESOLVED and
	// carried keys: a bloom built from a column no batch had rejects
	// everything, which for an anti-join invents unmatched probe rows. And
	// the filter has to match keys taken from its own insert side — a bloom
	// never has false negatives, so a miss there means the two sides encode
	// differently and every rejection is a lost row. Neither failure is worth
	// failing the query over: the join is correct without the filter, just
	// slower. Both are worth saying out loud.
	if bb != nil && bb.Resolved() && bb.Inserted() > 0 {
		bloomOp := bb.FilterOp(rb.buildKey)
		if err := bloomOp.SelfCheck(); err != nil {
			exec.BloomSelfCheckFailures.Add(1)
			slog.Error("reverse bloom rejects its own probe keys — filter NOT installed",
				"probe_key", rb.probeKey, "build_key", rb.buildKey, "err", err)
		} else {
			*rb.rbBuildSource = &pipelineSource{
				source: rb.buildSource,
				ops:    []exec.UnaryOperator{bloomOp},
			}
			ReverseBloomsInstalled.Add(1)
		}
	} else if bb != nil && !bb.Resolved() {
		slog.Warn("reverse bloom key column not found in the probe output — filter not installed",
			"probe_key", rb.probeKey, "build_key", rb.buildKey)
	}

	// Phase 4: Signal build goroutine to start with the bloom-filtered scan.
	close(rb.buildStart)

	// Wait for build to complete.
	select {
	case <-rb.Barrier:
	case <-ctx.Done():
		rb.collector.Release()
		return ctx.Err()
	}
	if *rb.BuildErr != nil {
		rb.collector.Release()
		return *rb.BuildErr
	}
	return nil
}

func (rb *reverseBloomBridge) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return rb.collector.NextReplay(ctx)
}

func (rb *reverseBloomBridge) Close() error {
	if rb.collector != nil {
		rb.collector.Release()
	}
	return nil
}

// joinFlushSource wraps a join probe pipeline and, after the probe side is
// exhausted, drains the probe's flush phase: the spilled partitions first,
// then the resident unmatched build rows (see Next).
type joinFlushSource struct {
	inner    exec.Source
	innerOps []exec.UnaryOperator
	probe    *exec.HashJoinProbe
	pipeline *pipelineSource
	flushed  bool
	drained  bool
}

func (s *joinFlushSource) Init(ctx context.Context) error {
	s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	return s.pipeline.Init(ctx)
}

func (s *joinFlushSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.flushed {
		b, err := s.pipeline.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
		// Probe exhausted — flush unmatched build-side rows. The probe names
		// the probe half of those rows itself: this source only sees the
		// join's OUTPUT batches, and passing one of those as the probe schema
		// mapped every preserved column onto the NULL side (a RIGHT JOIN's
		// unmatched rows came back with the preserved side blank). A join that
		// emitted no output batch at all — nothing matched — used to skip the
		// flush entirely and lose all of them.
		s.flushed = true
	}
	// NextFlush, not FlushUnmatchedRows: a RIGHT/FULL join whose build
	// EVICTED partitions owes the spilled partitions' joined output and their
	// build-side unmatched rows too, and this source is the only driver for
	// this shape — the probe sits in innerOps here, never in the outer
	// Pipeline's Ops, so exec.Pipeline.flushSpilledOps never sees it. Calling
	// the resident-only flush left every spilled partition unprocessed and
	// then dereferenced the nil'd build slots its own arena still pointed at
	// (#550). NextFlush walks the spilled partitions first and ends with
	// exactly the FlushUnmatchedRows this used to call.
	for !s.drained {
		b, err := s.probe.NextFlush(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			s.drained = true
			break
		}
		if b.ActiveLen() == 0 {
			continue
		}
		return b, nil
	}
	return nil, nil
}

// Close releases the probe pipeline whether or not Init ever ran.
//
// pipeline is assigned in Init, and a source can be constructed and then
// closed without one — a plan whose execution is abandoned between
// buildPipeline and the first Init (an early return, a cancellation, a set
// operation that decides not to pull a branch). Dereferencing the nil
// pipeline there crashed the whole server process (#510), and skipping the
// close instead would leak the source and operators that construction
// already built. Close what exists.
func (s *joinFlushSource) Close() error {
	if s.pipeline == nil {
		s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	}
	return s.pipeline.Close()
}

// rightSemiFlushSource wraps a join probe pipeline for RightSemiJoin/RightAntiJoin.
// During probing, no rows are output (probe marks matched build entries).
// After probing completes, emits matched (RightSemi) or unmatched (RightAnti) build rows.
type rightSemiFlushSource struct {
	inner    exec.Source
	innerOps []exec.UnaryOperator
	probe    *exec.HashJoinProbe
	joinType exec.JoinType
	pipeline *pipelineSource
	flushed  bool
	drained  bool
	resident bool
	result   *batch.RecordBatch
}

func (s *rightSemiFlushSource) Init(ctx context.Context) error {
	s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	return s.pipeline.Init(ctx)
}

func (s *rightSemiFlushSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.flushed {
		// Drain the probe pipeline. A RightSemi/RightAnti probe returns nil
		// for each input batch — it only marks matched entries — so this loop
		// used to pull until exhaustion and DISCARD what came back, on the
		// reasoning that nothing could come back.
		//
		// That reasoning stopped being true when `pipelineSource` learned to
		// drain its operators' spilled partitions (#1010): the probe is in
		// `innerOps`, it is a `FlushableOperator`, and the batches its
		// evicted partitions replay now arrive HERE. Discarding them lost
		// them for good — the `NextFlush` loop below then found the probe
		// already drained — so `WHERE EXISTS` over a build that evicted a
		// partition answered 149 rows for PostgreSQL's 150, silently, on
		// every path this source runs on (#1010 round 2).
		//
		// A batch this source is handed is the join's OUTPUT, whichever
		// mechanism produced it. It is returned, exactly as `joinFlushSource`
		// returns what its own pipeline hands back — the difference between
		// the two wrappers was the whole defect. `s.flushed` stays false, so
		// the next call resumes the drain where it left off.
		for {
			b, err := s.pipeline.Next(ctx)
			if err != nil {
				return nil, err
			}
			if b == nil {
				break
			}
			if b.ActiveLen() == 0 {
				continue
			}
			return b, nil
		}
		s.flushed = true
	}
	// A spilling build evicts partitions, and the arena entries pointing at
	// the evicted rows are skipped by the flushes below — the spilled
	// partitions are replayed from disk here instead, each emitting its own
	// matched/unmatched build rows (#550). Without this drain those rows are
	// dropped and the partition's probe rows are never probed at all.
	for !s.drained {
		b, err := s.probe.NextFlush(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			s.drained = true
			break
		}
		if b.ActiveLen() == 0 {
			continue
		}
		return b, nil
	}
	if s.result == nil && !s.resident {
		s.resident = true
		if s.joinType == exec.RightSemiJoin {
			s.result = s.probe.FlushMatched()
		} else {
			s.result = s.probe.FlushAntiMatched()
		}
	}
	if s.result != nil {
		b := s.result
		s.result = nil
		return b, nil
	}
	return nil, nil
}

// Close releases the probe pipeline whether or not Init ever ran — the same
// contract, and the same #510 crash, as joinFlushSource.Close above.
func (s *rightSemiFlushSource) Close() error {
	if s.pipeline == nil {
		s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	}
	return s.pipeline.Close()
}

// mapJoinType converts a join type string (e.g. "join", "left join",
// "right join", "full outer join", "cross join") to a canonical short
// form used by the distributed planner.
func mapJoinType(vt string) string {
	lower := strings.ToLower(strings.TrimSpace(vt))
	switch {
	case lower == "cross" || strings.Contains(lower, "cross"):
		return "cross"
	case strings.Contains(lower, "full"):
		return "full"
	case strings.Contains(lower, "right"):
		return "right"
	case strings.Contains(lower, "semi"):
		return "semi"
	case strings.Contains(lower, "anti"):
		return "anti"
	case strings.Contains(lower, "left"):
		return "left"
	default:
		return "inner"
	}
}

// mapExecJoinType converts a canonical join type string to exec.JoinType.
func mapExecJoinType(jt string) exec.JoinType {
	switch jt {
	case "left":
		return exec.LeftJoin
	case "right":
		return exec.RightJoin
	case "full":
		return exec.FullOuterJoin
	case "cross":
		return exec.CrossJoin
	case "semi":
		return exec.SemiJoin
	case "anti":
		return exec.AntiJoin
	default:
		return exec.InnerJoin
	}
}

// parseSemiAntiNE recognizes a join filter that is EXACTLY one
// column-to-column not-equal condition ("l1.l_suppkey <> l2.l_suppkey").
// That is the decorrelated-EXISTS self-inequality class the distinct-pair
// build serves; anything else (conjunctions, other operators, literals)
// returns ok=false and stays on the generic closure path.
func parseSemiAntiNE(filter string) (probeCol, buildCol string, ok bool) {
	if !semiAntiNE.Load() || filter == "" {
		return "", "", false
	}
	parts := strings.Split(strings.ToLower(filter), " and ")
	if len(parts) != 1 {
		return "", "", false
	}
	part := strings.TrimSpace(parts[0])
	var idx int
	var opLen int
	if i := strings.Index(part, " <> "); i >= 0 {
		idx, opLen = i, 4
	} else if i := strings.Index(part, " != "); i >= 0 {
		idx, opLen = i, 4
	} else {
		return "", "", false
	}
	left := strings.TrimSpace(part[:idx])
	right := strings.TrimSpace(part[idx+opLen:])
	if !isBareColumnRef(left) || !isBareColumnRef(right) {
		return "", "", false
	}
	// Qualified: exec resolves both through columnIndexFallback, and the
	// qualifier is what distinguishes a joined build's colliding names.
	return left, right, true
}

// isBareColumnRef accepts identifier-shaped refs (optionally qualified);
// rejects literals, expressions, and anything with quoting or operators.
func isBareColumnRef(s string) bool {
	if s == "" {
		return false
	}
	hasLetter := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			hasLetter = true
		case r >= '0' && r <= '9', r == '.':
		default:
			return false
		}
	}
	return hasLetter
}

// filterColumnIndex delegates to exec.ColumnIndexFallback: exact spelling,
// then bare lookup for a qualified reference, then UNIQUE qualified lookup for
// a bare reference; ambiguity returns -1, never a guess (#527). Preserve
// identifier folding (#731) and ROW-field-path guards (ADR-0022).
// The camel-case invariance battery does not distinguish this site: its
// EXISTS/IN residuals use folded tier, so reverting delegation owns no cells.
func filterColumnIndex(b *batch.RecordBatch, name string) int {
	return exec.ColumnIndexFallback(b, name)
}

// filterOperandNull reports whether row of v is SQL NULL, reading the bitmap
// as GetValue does: a view's own bit first, then its base's through Indices.
// Read-only — build batches are shared by concurrent probe workers, so this
// must not touch Bitmap.HasNulls' cache.
func filterOperandNull(v *batch.Vector, row int) bool {
	if v.Nulls.IsNullFast(row) {
		return true
	}
	return v.Base != nil && v.Base.Nulls.IsNullFast(int(v.Indices[row]))
}
