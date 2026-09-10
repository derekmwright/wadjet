// This file holds join sources for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

import (
	"context"
	"fmt"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"log/slog"
	"strings"
	"sync"
)

// deferredJoinBridge creates a pipeline break for deferred hash join builds.
// Init runs the child pipeline (scan → early probes) with parallel workers,
// overlapping with the deferred build goroutine. After the child pipeline
// completes, it waits for the build barrier, then replays collected batches
// as a Source for the deferred probe operators.
type deferredJoinBridge struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	barrier     <-chan struct{}
	buildErr    *error
	workers     int
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
		Source:  d.childSource,
		Ops:     d.childOps,
		Sink:    d.collector,
		Workers: d.workers,
	}
	if err := pipe.Run(ctx); err != nil {
		d.collector.Release()
		// Wait for build goroutine to prevent leak
		select {
		case <-d.barrier:
		default:
		}
		return fmt.Errorf("deferred join child pipeline: %w", err)
	}

	// Wait for deferred build to complete
	select {
	case <-d.barrier:
	case <-ctx.Done():
		d.collector.Release()
		return ctx.Err()
	}
	if *d.buildErr != nil {
		d.collector.Release()
		return *d.buildErr
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
	childSource   exec.Source
	childOps      []exec.UnaryOperator
	rbBuildSource *exec.Source // pointer to the goroutine's build source (swappable)
	buildSource   exec.Source  // original build source (unwrapped)
	buildStart    chan struct{}
	barrier       <-chan struct{}
	buildErr      *error
	probeKey      string // probe-side column to extract bloom from
	buildKey      string // build-side column to filter
	workers       int
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
		Source:  rb.childSource,
		Ops:     rb.childOps,
		Sink:    rb.collector,
		Workers: rb.workers,
	}
	if err := pipe.Run(ctx); err != nil {
		rb.collector.Release()
		close(rb.buildStart)
		<-rb.barrier
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
			<-rb.barrier
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
	case <-rb.barrier:
	case <-ctx.Done():
		rb.collector.Release()
		return ctx.Err()
	}
	if *rb.buildErr != nil {
		rb.collector.Release()
		return *rb.buildErr
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

// preservesBuildSide reports whether a canonical join kind emits build-side
// rows that found no probe partner — the rows a RIGHT or FULL join exists to
// preserve, produced after probing by HashJoinProbe.FlushUnmatchedRows.
//
// Every distributed layout that REPLICATES the build side across tasks is
// unsound for these: each task holds the whole build and sees only its slice
// of the probe, so each would emit the same unmatched rows. Broadcast
// (walkStages) and skew-split (coordinator.planSkewSplitTasks) both gate on
// this; the hash-shuffle layout is sound because a partition's build and
// probe rows land on the same task.
func preservesBuildSide(jt string) bool {
	return jt == "right" || jt == "full"
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

// ParseSemiAntiNE recognizes a join filter that is EXACTLY one
// column-to-column not-equal condition ("l1.l_suppkey <> l2.l_suppkey").
// That is the decorrelated-EXISTS self-inequality class the distinct-pair
// build serves; anything else (conjunctions, other operators, literals)
// returns ok=false and stays on the generic closure path.
func ParseSemiAntiNE(filter string) (probeCol, buildCol string, ok bool) {
	if !SemiAntiNE.Load() || filter == "" {
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

func BuildSemiAntiFilter(filter string) func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	type filterCond struct {
		probeCol string
		op       string
		buildCol string
	}
	var conds []filterCond
	parts := strings.Split(strings.ToLower(filter), " and ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Try operators from longest to shortest to avoid partial matches
		for _, op := range []string{"!=", ">=", "<=", "<>", ">", "<"} {
			idx := strings.Index(part, " "+op+" ")
			if idx >= 0 {
				// Kept QUALIFIED. A join emits a build column under its
				// relation's qualifier whenever the bare name collides on
				// the probe side, so stripping here resolved the filter to
				// whichever relation reorderJoins put on the probe — the
				// #527 defect one layer down from the logical plan.
				// filterColumnIndex falls back to the bare name for the
				// single-relation builds that emit it that way.
				left := strings.TrimSpace(part[:idx])
				right := strings.TrimSpace(part[idx+len(op)+2:])
				conds = append(conds, filterCond{probeCol: left, op: op, buildCol: right})
				break
			}
		}
	}

	if len(conds) == 0 {
		return nil
	}

	// Pre-encode operator as int for switch-free dispatch in hot path.
	const (
		opNE = iota
		opGT
		opLT
		opGE
		opLE
		opEQ
	)
	ops := make([]int, len(conds))
	for i, c := range conds {
		switch c.op {
		case "!=", "<>":
			ops[i] = opNE
		case ">":
			ops[i] = opGT
		case "<":
			ops[i] = opLT
		case ">=":
			ops[i] = opGE
		case "<=":
			ops[i] = opLE
		default:
			ops[i] = opEQ
		}
	}

	// Resolved once on first probe; sync.Once provides happens-before so
	// concurrent workers see the same probeIdxs / buildIdxs after the first
	// call returns. The schemas don't change for the lifetime of the join,
	// so caching the indices forever is safe.
	probeIdxs := make([]int, len(conds))
	buildIdxs := make([]int, len(conds))
	var resolveOnce sync.Once

	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		resolveOnce.Do(func() {
			for i, c := range conds {
				probeIdxs[i] = filterColumnIndex(probe, c.probeCol)
				buildIdxs[i] = filterColumnIndex(build, c.buildCol)
			}
		})

		for i := range conds {
			pi, bi := probeIdxs[i], buildIdxs[i]
			if pi < 0 || bi < 0 {
				return false
			}
			pv := probe.Columns[pi]
			bv := build.Columns[bi]
			if !evalFilterTyped(pv, bv, probeRow, buildRow, ops[i]) {
				return false
			}
		}
		return true
	}
}

// filterColumnIndex resolves a semi/anti join filter's column name against a
// batch: the exact spelling first, then the bare name for a qualified
// reference over a single-relation build, then a UNIQUE qualified column for
// a bare reference. Ambiguity resolves to -1 rather than to a guess — a
// filter that silently reads the wrong relation's column is what #527 was.
//
// It DELEGATES rather than restating that rule, because the restatement
// drifted. This function's own doc has claimed to mirror
// exec.columnIndexFallback since it was written, and stopped doing so twice:
// at #731 the mirror lost the identifier fold, so a folded reference (which
// is what the lexer produces for every unquoted identifier) misses a
// CamelCase build column byte-exactly and rejects EVERY candidate — a SEMI
// join answering 0 rows and an ANTI join answering all of them, in silence;
// and it never grew the ROW-field-path guard ADR-0022 added there, so
// `attrs.score` fell through to a bare `score` published by some other
// relation. One resolver, one rule.
//
// The camel-case invariance battery does not yet distinguish this site — the
// extra conjuncts on its EXISTS/IN shapes name `tier`, which the fixture
// spells folded, so the byte-exact probe happened to answer. Measured with
// every other fix in place and this one reverted, it owns 0 of the battery's
// cells; the delegation is the drift closed, not a cell recovered.
func filterColumnIndex(b *batch.RecordBatch, name string) int {
	return exec.ColumnIndexFallback(b, name)
}

// evalFilterTyped compares two vector values at given rows using typed dispatch.
// Avoids interface boxing and fmt.Sprint allocation on every comparison.
func evalFilterTyped(pv, bv *batch.Vector, pRow, bRow, op int) bool {
	const (
		opNE = iota
		opGT
		opLT
		opGE
		opLE
		opEQ
	)
	switch pv.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		a, b := pv.Int32Data[pRow], bv.Int32Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		a, b := pv.Int64Data[pRow], bv.Int64Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeFloat64:
		a, b := pv.Float64Data[pRow], bv.Float64Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeFloat32:
		a, b := pv.Float32Data[pRow], bv.Float32Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeString:
		a, b := pv.BytesData.StringValue(pRow), bv.BytesData.StringValue(bRow)
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	default:
		// Fallback for other types: use GetValue + fmt.Sprint
		as := fmt.Sprint(pv.GetValue(pRow))
		bs := fmt.Sprint(bv.GetValue(bRow))
		switch op {
		case opNE:
			return as != bs
		case opGT:
			return as > bs
		case opLT:
			return as < bs
		case opGE:
			return as >= bs
		case opLE:
			return as <= bs
		default:
			return as == bs
		}
	}
}
