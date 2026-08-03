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
}

// defaultPartialAggCapBytes bounds the per-task hash state. 128 MB keeps
// the operator invisible next to worker GOMEMLIMITs (tens of GB) while
// holding ~2-4M groups per epoch — far more than a well-clustered input
// needs between flushes.
const defaultPartialAggCapBytes = 128 << 20

func newCappedPartialAgg(keys []string, specs []distributed.AggSpec, capBytes int64) *cappedPartialAgg {
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
	return &cappedPartialAgg{groupBy: keys, aggs: aggs, capBytes: capBytes}
}

func (p *cappedPartialAgg) ensureAgg(ctx context.Context) error {
	if p.agg != nil {
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
// returned batches to the sink before consuming further input.
func (p *cappedPartialAgg) consume(ctx context.Context, b *batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if err := p.ensureAgg(ctx); err != nil {
		return nil, err
	}
	p.inRows += int64(b.ActiveLen())
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
