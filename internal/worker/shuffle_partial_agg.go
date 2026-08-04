package worker

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
)

// cappedPartialAgg pre-combines shuffle-task rows on the exchange's
// partial-agg keys before they reach the partitioning sink (exchange
// partial aggregation — the Trino-style reduce-before-ship mechanism).
// Specs are name-preserving (OutputCol == InputCol) and restricted to
// self-mergeable functions (SUM/MIN/MAX) by the planner's eligibility
// pass, so consumers are untouched: a grouped final_aggregate merges
// partials exactly as it would raw rows, and a join consumer probes the
// same (key, value) column names.
//
// Memory is bounded by capBytes: when the hash state exceeds the cap the
// current groups are flushed downstream and a fresh epoch begins. Poorly
// clustered inputs therefore degrade to shipping ~one row per input row
// in aggregate form — never spilling, never OOMing. The output schema is
// identical across epochs (same HashAggregate config), which the
// partitioned sink requires: it locks the WSHF schema from the first
// batch it consumes.
//
// NOTE on types: SUM over a Decimal column emits Float64 (the same
// erosion the downstream aggregate applies today via
// kernel.Accumulator.FinalSum), so the shipped column type may differ
// from the raw payload's. Consumers resolve WSHF columns by name and
// type per batch, and every chunk this operator emits shares one schema.
type cappedPartialAgg struct {
	groupBy  []string
	aggs     []exec.AggColumn
	capBytes int64

	agg     *exec.HashAggregate
	inRows  int64
	outRows int64
	flushes int64

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
	for i, s := range specs {
		aggs[i] = exec.AggColumn{
			Func:       parseAggFuncString(s.Func),
			InputCol:   s.InputCol,
			OutputCol:  s.OutputCol,
			OutputType: aggOutputTypeString(s.Func),
		}
	}
	return &cappedPartialAgg{groupBy: keys, aggs: aggs, capBytes: capBytes, partitionKeys: partitionKeys}
}

// resolveAgainst intersects the configured keys/specs with the actual
// batch schema (see the type comment). Called once, on the first batch.
func (p *cappedPartialAgg) resolveAgainst(b *batch.RecordBatch) {
	p.resolved = true
	present := make(map[string]bool, len(b.Schema))
	for _, c := range b.Schema {
		present[c.Name] = true
	}
	for _, k := range p.partitionKeys {
		if !present[k] {
			p.disabled = true
			return
		}
	}
	keptKeys := p.groupBy[:0]
	for _, k := range p.groupBy {
		if present[k] {
			keptKeys = append(keptKeys, k)
		} else {
			p.droppedKeys++
		}
	}
	p.groupBy = keptKeys
	keptAggs := p.aggs[:0]
	for _, a := range p.aggs {
		if present[a.InputCol] {
			keptAggs = append(keptAggs, a)
		}
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
	if err := p.agg.Close(); err != nil {
		return nil, fmt.Errorf("partial agg close: %w", err)
	}
	p.agg = nil
	p.flushes++
	return out, nil
}
