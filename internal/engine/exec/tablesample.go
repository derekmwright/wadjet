// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TableSample is a TABLESAMPLE clause as a scan applies it (#1411). The
// argument has already been read as PostgreSQL's real, once, when the
// statement was planned (physical.TablesampleArgument): Percent is that float4
// value widened exactly to float64, and Null says the argument was NULL. The
// RANGE is not checked there — PostgreSQL checks it when the scan begins — so
// a TableSample may carry any value, and the sampled source checks it.
//
// It is the one description of a sample both paths apply: the single-process
// scan (physical.Planner.buildScan) and the worker's scan fragment, which
// receives it in the fragment's scan spec, so a sampled table is sampled by
// the same code wherever it is read.
type TableSample struct {
	Method  string  // "BERNOULLI" or "SYSTEM"
	Percent float64 // the argument as real, unchecked
	Null    bool    // the argument is NULL
}

// SampleBlockRows is the unit SYSTEM samples: PostgreSQL's SYSTEM keeps or
// drops whole PAGES; this engine has no pages, so it keeps or drops whole
// blocks of this many physical rows as the scan reads them — a batch of at
// most batch.DefaultBatchSize rows is one block, a longer one several.
const SampleBlockRows = batch.DefaultBatchSize

// CheckSamplePercent is the range PostgreSQL checks when a sample scan begins
// (tsm_bernoulli / tsm_system's BeginSampleScan, nodeSamplescan's NULL check):
// NULL, NaN, below 0 or above 100 is 2202H.
func CheckSamplePercent(pct float64, isNull bool) error {
	if isNull {
		return sqlerr.New("2202H", "TABLESAMPLE parameter cannot be null")
	}
	if math.IsNaN(pct) || pct < 0 || pct > 100 {
		return sqlerr.New("2202H", "sample percentage must be between 0 and 100")
	}
	return nil
}

// NewSampledSource is a sampled scan: src's rows, each kept with probability
// Percent / 100 (BERNOULLI) or in blocks of SampleBlockRows (SYSTEM).
//
// Its first Next is where the scan begins, and where the percentage's range
// is checked — over a table with no rows too. The sample is drawn from the
// rows src's batch SELECTS: a scan narrows its batches with a selection
// vector (a DELETE's markers keep Len physical and drop the deleted rows from
// Sel), and a sampler that redrew from 0..Len-1 returned the deleted rows.
// A batch with no row kept is not passed on.
//
// Next is safe to call from several goroutines (a parallel pipeline pulls one
// source from each of its workers): the check runs once, and the draws come
// from math/rand/v2's top-level generator, which is safe for concurrent use.
func NewSampledSource(src Source, ts TableSample) Source {
	return &sampledSource{Source: src, ts: ts}
}

type sampledSource struct {
	Source
	ts       TableSample
	once     sync.Once
	rangeErr error
}

func (s *sampledSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	s.once.Do(func() { s.rangeErr = CheckSamplePercent(s.ts.Percent, s.ts.Null) })
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	for {
		b, err := s.Source.Next(ctx)
		if err != nil || b == nil {
			return b, err
		}
		if SampleBatch(b, s.ts.Method, s.ts.Percent) {
			return b, nil
		}
	}
}

// SampleBatch narrows b's selection to the rows the sample keeps and reports
// whether any row is left. pct is in [0, 100] (CheckSamplePercent refused
// anything else before the first batch). Len stays the batch's PHYSICAL row
// count and Sel lists the kept rows, as every other narrowing does.
func SampleBatch(b *batch.RecordBatch, method string, pct float64) bool {
	if b == nil || b.ActiveLen() == 0 {
		return false
	}
	threshold := pct / 100
	if threshold >= 1 {
		return true
	}
	if method == "SYSTEM" {
		return sampleBlocks(b, threshold)
	}
	sel := make([]uint32, 0, b.ActiveLen())
	if b.Sel != nil {
		for _, i := range b.Sel {
			if rand.Float64() < threshold {
				sel = append(sel, i)
			}
		}
	} else {
		for i := range b.Len {
			if rand.Float64() < threshold {
				sel = append(sel, uint32(i))
			}
		}
	}
	b.Sel = sel
	return len(sel) > 0
}

// sampleBlocks keeps or drops each SampleBlockRows block of b's physical rows
// as a whole, keeping only the rows b already selects inside a kept block.
func sampleBlocks(b *batch.RecordBatch, threshold float64) bool {
	blocks := (b.Len + SampleBlockRows - 1) / SampleBlockRows
	if blocks <= 1 {
		return rand.Float64() < threshold
	}
	keep := make([]bool, blocks)
	kept := false
	for i := range keep {
		keep[i] = rand.Float64() < threshold
		kept = kept || keep[i]
	}
	if !kept {
		return false
	}
	var sel []uint32
	if b.Sel != nil {
		sel = make([]uint32, 0, len(b.Sel))
		for _, i := range b.Sel {
			if keep[int(i)/SampleBlockRows] {
				sel = append(sel, i)
			}
		}
	} else {
		sel = make([]uint32, 0, b.Len)
		for i := range b.Len {
			if keep[i/SampleBlockRows] {
				sel = append(sel, uint32(i))
			}
		}
	}
	b.Sel = sel
	return len(sel) > 0
}
