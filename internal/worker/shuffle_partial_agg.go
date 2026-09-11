package worker

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// cappedPartialAgg precombines shuffle rows using planner-vetted self-mergeable
// SUM/MIN/MAX specs with OutputCol==InputCol.
// Flush groups downstream and start a fresh epoch when state exceeds capBytes;
// never spill. Poor clustering degrades toward one aggregate row per input.
// Every epoch must emit the same WSHF schema, fixed from the first batch.
// Consumers resolve names/types per batch because SUM can widen raw types;
// DECIMAL SUM preserves its column scale (#455).
// See docs/internals/worker-capped-shuffle-partial-aggregation.md for the design.
type cappedPartialAgg struct {
	groupBy  []string
	aggs     []exec.AggColumn
	capBytes int64

	agg     *exec.HashAggregate
	inRows  int64
	outRows int64
	flushes int64

	// Group-index observability, summed across epochs: how many
	// flat->bucketed conversions this operator paid, whether its layout was
	// pinned flat at construction, and the group ceiling that pinned it.
	// Logged per task by the shuffle executor.
	conversions  int
	bornFlat     bool
	groupCeiling int64

	// resolved/disabled implement first-batch schema intersection: the
	// planner's PartialAggKeys derive from the exchange's DECLARED payload,
	// which is an over-approximated union by planner convention ("the
	// reader ignores columns that don't exist"). Grouping by a declared-
	// but-absent column would make HashAggregate materialize it as an
	// all-null placeholder in every flush — the placeholder then collides
	// with the REAL column of the same name on a downstream join's build
	// side, forcing alias qualification and breaking bare-name resolution
	// for every consumer after it (SF100 Q18 composed-stack 0-rows,
	// results/20260803-223926). Keys are intersected against the first
	// batch's actual schema; specs whose input is absent are dropped; if
	// nothing combinable remains (or a partition key is itself absent),
	// the operator disables itself and the stream passes through raw.
	resolved      bool
	disabled      bool
	droppedKeys   int
	partitionKeys []string
}

// defaultPartialAggCapBytes bounds the per-task hash state. 128 MB keeps
// the operator invisible next to worker GOMEMLIMITs (tens of GB) while
// holding ~2-4M groups per epoch — far more than a well-clustered input
// needs between flushes.
const defaultPartialAggCapBytes = 128 << 20

func newCappedPartialAgg(keys []string, specs []distributed.AggSpec, capBytes int64) *cappedPartialAgg {
	return newCappedPartialAggPartitioned(keys, specs, nil, capBytes)
}

// newCappedPartialAggPartitioned additionally pins the exchange's partition
// keys: if any of them is absent from the runtime schema the operator
// disables itself entirely (a phantom partition key means the declared
// payload has diverged from the stream beyond what key-intersection can
// repair).
func newCappedPartialAggPartitioned(keys []string, specs []distributed.AggSpec, partitionKeys []string, capBytes int64) *cappedPartialAgg {
	if capBytes <= 0 {
		capBytes = defaultPartialAggCapBytes
	}
	aggs := make([]exec.AggColumn, len(specs))
	unknown := false
	for i, s := range specs {
		fn, known := parseAggFuncString(s.Func)
		if !known {
			// This operator is an optimization — pre-combining rows in the
			// shuffle sender — so an unrecognized function means "don't",
			// not "sum it instead" (#353). markExchangePartialAgg only
			// marks bare SUM/MIN/MAX today, so this is belt-and-braces.
			unknown = true
		}
		if s.Distinct {
			// A DISTINCT aggregate has no partial form this operator can
			// combine: two senders that each saw the same value would each
			// contribute it, and the merge above adds them (#703 — the same
			// double-count #291 recorded for COUNT(DISTINCT), which reaches
			// here as the "count_distinct" func name and is refused by the
			// parse above). Disabling the pre-combine is free: the plan
			// already routes every DISTINCT aggregate through the one-level
			// RawInputAggregate shape.
			unknown = true
		}
		aggs[i] = exec.AggColumn{
			Func:         fn,
			InputCol:     s.InputCol,
			InputCol2:    s.InputCol2,
			InputCol3:    s.InputCol3,
			OutputFields: aggSpecOutputFields(s),
			Separator:    s.Separator,
			Percentile:   s.Percentile,
			OutputCol:    s.OutputCol,
			OutputType:   aggSpecOutputType(s),
		}
	}
	return &cappedPartialAgg{
		groupBy: keys, aggs: aggs, capBytes: capBytes, partitionKeys: partitionKeys,
		resolved: unknown, disabled: unknown,
	}
}

// resolveAgainst runs once on the first batch and intersects configured
// keys/specs with the actual schema. Resolve via batch.ResolveSchemaIndex,
// rewriting plan-folded names to stored spellings (#731).
// A partial byte-exact match must not drop CamelCase grouping keys and combine
// different groups. Keep flushed payload names identical to the raw names.
// The camel-case invariance corpus does not isolate this site: its scan already
// supplies schema spellings, so passing that gate alone does not prove it.
// See docs/internals/worker-partial-agg-schema-intersection.md for the design.
func (p *cappedPartialAgg) resolveAgainst(b *batch.RecordBatch) {
	p.resolved = true
	resolve := func(name string) (string, bool) {
		if i := batch.ResolveSchemaIndex(b.Schema, name); i >= 0 {
			return b.Schema[i].Name, true
		}
		return name, false
	}
	for _, k := range p.partitionKeys {
		if _, ok := resolve(k); !ok {
			p.disabled = true
			return
		}
	}
	keptKeys := p.groupBy[:0]
	for _, k := range p.groupBy {
		if name, ok := resolve(k); ok {
			keptKeys = append(keptKeys, name)
		} else {
			p.droppedKeys++
		}
	}
	p.groupBy = keptKeys
	keptAggs := p.aggs[:0]
	for _, a := range p.aggs {
		name, ok := resolve(a.InputCol)
		if !ok {
			continue
		}
		// Name-preserving specs (OutputCol == InputCol, which is what the
		// planner's eligibility pass emits) move to the schema's spelling
		// together, so the flushed column keeps the name the raw payload
		// had. A spec that renames is left exactly as declared.
		if a.OutputCol == a.InputCol {
			a.OutputCol = name
		}
		a.InputCol = name
		keptAggs = append(keptAggs, a)
	}
	p.aggs = keptAggs
	// Nothing combinable, or grouping degenerated to zero keys (which
	// would collapse a flush to one row and change row multiplicity for
	// non-aggregate consumers): pass through raw.
	if len(p.aggs) == 0 || len(p.groupBy) == 0 {
		p.disabled = true
	}
}

func (p *cappedPartialAgg) ensureAgg(ctx context.Context, b *batch.RecordBatch) error {
	if !p.resolved {
		p.resolveAgainst(b)
	}
	if p.disabled || p.agg != nil {
		return nil
	}
	p.agg = exec.NewHashAggregate(p.groupBy, p.aggs)
	// Declare the epoch bound BEFORE Init: this operator finalizes and
	// rebuilds the aggregate every capBytes, so its group index cannot
	// outlive one epoch and must not bet on a runtime flat->bucketed
	// conversion it can never amortize (exec/two_level_hash.go,
	// twoLevelBoundedMinGroups; SF100 Q18 measured 3-4x on the stage).
	p.agg.SetEpochByteCap(p.capBytes)
	if err := p.agg.Init(ctx); err != nil {
		p.agg = nil
		return fmt.Errorf("partial agg init: %w", err)
	}
	return nil
}

// consume feeds one input batch. It returns flushed partial batches when
// the epoch cap was exceeded, nil otherwise. The caller forwards any
// returned batches to the sink before consuming further input. When the
// operator disabled itself at resolve time the input batch is returned
// verbatim (raw passthrough).
func (p *cappedPartialAgg) consume(ctx context.Context, b *batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if err := p.ensureAgg(ctx, b); err != nil {
		return nil, err
	}
	p.inRows += int64(b.ActiveLen())
	if p.disabled {
		p.outRows += int64(b.ActiveLen())
		return []*batch.RecordBatch{b}, nil
	}
	if err := p.agg.Consume(ctx, b); err != nil {
		return nil, fmt.Errorf("partial agg consume: %w", err)
	}
	if p.agg.StateBytes() < p.capBytes {
		return nil, nil
	}
	return p.flush(ctx)
}

// drain flushes the final epoch at end of stream.
func (p *cappedPartialAgg) drain(ctx context.Context) ([]*batch.RecordBatch, error) {
	if p.agg == nil {
		return nil, nil
	}
	return p.flush(ctx)
}

// flush finalizes the current epoch's HashAggregate, collects its output
// batches, and resets for the next epoch.
func (p *cappedPartialAgg) flush(ctx context.Context) ([]*batch.RecordBatch, error) {
	if err := p.agg.Finalize(ctx); err != nil {
		return nil, fmt.Errorf("partial agg finalize: %w", err)
	}
	var out []*batch.RecordBatch
	for {
		b, err := p.agg.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("partial agg drain: %w", err)
		}
		if b == nil {
			break
		}
		p.outRows += int64(b.ActiveLen())
		out = append(out, b)
	}
	p.conversions += p.agg.IndexConversions()
	p.bornFlat = p.agg.IndexBornFlat()
	p.groupCeiling = p.agg.GroupCeiling()
	if err := p.agg.Close(); err != nil {
		return nil, fmt.Errorf("partial agg close: %w", err)
	}
	p.agg = nil
	p.flushes++
	return out, nil
}
