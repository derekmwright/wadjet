// This file holds operator sources for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"os"
	"runtime"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// scanParallelism returns the worker count for scan/decode/pipeline
// parallelism, honoring WADJET_SCAN_WORKERS when set (>0). Default is
// runtime.NumCPU(), the historical behavior — but a 2026-08-17 profiling
// pass measured the fast-query tier DOUBLING its wall time from decode
// over-subscription past the memory-bandwidth knee (24 workers 87.6ms vs
// 12 workers 43.8ms on a 12-core box, every profile symbol inflating
// uniformly with zero contention symbols). The env knob exists to A/B a
// lower default on the benchmark metal before changing it for everyone.
func scanParallelism() int {
	if v := os.Getenv("WADJET_SCAN_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU()
}

// innerPipelineWorkers returns the number of parallel workers for an inner
// pipeline (aggregate/sort child). Returns 0 (serial) unless the source is
// a concurrent-safe scan source.
func innerPipelineWorkers(src exec.Source) int {
	switch src.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		return scanParallelism()
	}
	return 0
}

// aggSourceAdapter wraps a child pipeline + hash aggregate into a Source.
type aggSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	agg         *exec.HashAggregate
	initialized bool
	// pipe is the inner pipeline this adapter runs. It is HELD, not
	// discarded: it owns the child ops and the morsel-parallel clone
	// sinks, and only its Close reaches them. Discarding it left a
	// cancelled GROUP BY's agg-spill-*.bin files on disk for the process
	// lifetime (#625 M2).
	pipe *exec.Pipeline
}

// ServesHeldState marks the adapter's output phase as a held-state drain —
// exempt from heap-backpressure pauses (exec.HeldStateSource).
func (a *aggSourceAdapter) ServesHeldState() bool { return true }

func (a *aggSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (a *aggSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !a.initialized {
		a.initialized = true
		// Run child pipeline into aggregate
		a.pipe = &exec.Pipeline{
			Source:  a.childSource,
			Ops:     a.childOps,
			Sink:    a.agg,
			Workers: innerPipelineWorkers(a.childSource),
		}
		if err := a.pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return a.agg.Next(ctx)
}

func (a *aggSourceAdapter) RowsScanned() int64 {
	if sp, ok := a.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

func (a *aggSourceAdapter) Close() error {
	if a.pipe != nil {
		// Reaches the child ops and the clone sinks as well as the
		// aggregate and the source.
		return a.pipe.Close()
	}
	a.agg.Close()
	return a.childSource.Close()
}

// sortSourceAdapter wraps a child pipeline + sort into a Source.
// When sort.Limit >= 0, it truncates results to the top N rows after
// sorting (Top-K optimization: avoids materializing the full sorted
// result). The bound lives only on sort.Limit — no separate limitN field to
// keep in sync — so a real LIMIT 0 (sort.Limit == 0) truncates correctly
// instead of colliding with sort.Limit's own "no limit" sentinel (#481).
type sortSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	sort        *exec.Sort
	initialized bool
	pipe        *exec.Pipeline // held so Close reaches the child ops and clones (#625 M2)
}

// ServesHeldState marks the adapter's output phase as a held-state drain —
// exempt from heap-backpressure pauses (exec.HeldStateSource).
func (s *sortSourceAdapter) ServesHeldState() bool { return true }

func (s *sortSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (s *sortSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.initialized {
		s.initialized = true
		s.pipe = &exec.Pipeline{
			Source:  s.childSource,
			Ops:     s.childOps,
			Sink:    s.sort,
			Workers: innerPipelineWorkers(s.childSource),
		}
		if err := s.pipe.Run(ctx); err != nil {
			return nil, err
		}
		// Top-K truncation: discard everything beyond sort.Limit rows. >= 0,
		// not > 0 — a real LIMIT 0 must truncate to zero rows too (#481).
		if s.sort.Limit >= 0 {
			s.sort.Truncate(s.sort.Limit)
		}
	}
	return s.sort.Next(ctx)
}

func (s *sortSourceAdapter) Close() error {
	if s.pipe != nil {
		return s.pipe.Close()
	}
	s.sort.Close()
	return s.childSource.Close()
}

func (s *sortSourceAdapter) RowsScanned() int64 {
	if sp, ok := s.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

// windowSourceAdapter wraps a child pipeline + window into a Source.
// ServesHeldState — see aggSourceAdapter (exec.HeldStateSource).
func (w *windowSourceAdapter) ServesHeldState() bool { return true }

type windowSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	win         *exec.Window
	initialized bool
	pipe        *exec.Pipeline // held so Close reaches the child ops and clones (#625 M2)
}

func (w *windowSourceAdapter) Init(_ context.Context) error { return nil }

func (w *windowSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !w.initialized {
		w.initialized = true
		w.pipe = &exec.Pipeline{
			Source: w.childSource,
			Ops:    w.childOps,
			Sink:   w.win,
		}
		if err := w.pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return w.win.Next(ctx)
}

func (w *windowSourceAdapter) RowsScanned() int64 {
	if sp, ok := w.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

func (w *windowSourceAdapter) Close() error {
	if w.pipe != nil {
		return w.pipe.Close()
	}
	w.win.Close()
	return w.childSource.Close()
}
