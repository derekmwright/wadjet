package exec

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// ProfileStats holds execution statistics for a single pipeline operator.
type ProfileStats struct {
	Name       string        // operator name (e.g., "Filter", "HashAggregate")
	WallTime   time.Duration // total wall time spent in this operator
	RowsIn     int64         // total input rows
	RowsOut    int64         // total output rows
	Calls      int64         // number of calls (Next/Execute/Consume)
}

// ProfileCollector aggregates stats from all operators in a pipeline.
type ProfileCollector struct {
	stats []*ProfileStats
}

// NewProfileCollector creates a new collector.
func NewProfileCollector() *ProfileCollector {
	return &ProfileCollector{}
}

// Add registers a stats entry and returns it. The caller (wrapper) writes to it.
func (c *ProfileCollector) Add(name string) *ProfileStats {
	s := &ProfileStats{Name: name}
	c.stats = append(c.stats, s)
	return s
}

// Stats returns all collected stats in pipeline order.
func (c *ProfileCollector) Stats() []*ProfileStats {
	return c.stats
}

// ProfiledSource wraps a Source to collect timing and row count stats.
type ProfiledSource struct {
	inner Source
	stats *ProfileStats
}

// WrapSource decorates a Source with profiling. Only call when EXPLAIN ANALYZE is active.
func WrapSource(src Source, collector *ProfileCollector, name string) *ProfiledSource {
	return &ProfiledSource{
		inner: src,
		stats: collector.Add(name),
	}
}

func (p *ProfiledSource) Init(ctx context.Context) error {
	return p.inner.Init(ctx)
}

func (p *ProfiledSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	start := time.Now()
	b, err := p.inner.Next(ctx)
	atomic.AddInt64((*int64)(&p.stats.WallTime), int64(time.Since(start)))
	atomic.AddInt64(&p.stats.Calls, 1)
	if b != nil {
		atomic.AddInt64(&p.stats.RowsOut, int64(b.ActiveLen()))
	}
	return b, err
}

func (p *ProfiledSource) Close() error {
	return p.inner.Close()
}

// Inner returns the wrapped source (for type assertions like ScanStatsProvider).
func (p *ProfiledSource) Inner() Source {
	return p.inner
}

// ProfiledOperator wraps a UnaryOperator to collect timing and row count stats.
type ProfiledOperator struct {
	inner UnaryOperator
	stats *ProfileStats
}

// WrapOperator decorates a UnaryOperator with profiling.
func WrapOperator(op UnaryOperator, collector *ProfileCollector, name string) *ProfiledOperator {
	return &ProfiledOperator{
		inner: op,
		stats: collector.Add(name),
	}
}

func (p *ProfiledOperator) Init(ctx context.Context) error {
	return p.inner.Init(ctx)
}

func (p *ProfiledOperator) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	rowsIn := int64(0)
	if in != nil {
		rowsIn = int64(in.ActiveLen())
	}
	start := time.Now()
	out, err := p.inner.Execute(ctx, in)
	atomic.AddInt64((*int64)(&p.stats.WallTime), int64(time.Since(start)))
	atomic.AddInt64(&p.stats.Calls, 1)
	atomic.AddInt64(&p.stats.RowsIn, rowsIn)
	if out != nil {
		atomic.AddInt64(&p.stats.RowsOut, int64(out.ActiveLen()))
	}
	return out, err
}

func (p *ProfiledOperator) Close() error {
	return p.inner.Close()
}

// Inner returns the wrapped operator (for type assertions like DoneSignaler).
func (p *ProfiledOperator) Inner() UnaryOperator {
	return p.inner
}

// Done delegates to the inner operator if it implements DoneSignaler.
func (p *ProfiledOperator) Done() bool {
	if ds, ok := p.inner.(DoneSignaler); ok {
		return ds.Done()
	}
	return false
}

// Clone delegates to the inner operator if it implements Cloneable.
// Returns a new ProfiledOperator wrapping the cloned inner, with fresh stats.
func (p *ProfiledOperator) Clone() UnaryOperator {
	if c, ok := p.inner.(Cloneable); ok {
		return &ProfiledOperator{
			inner: c.Clone(),
			stats: &ProfileStats{Name: p.stats.Name},
		}
	}
	// Inner is not cloneable — return a new wrapper with its own stats
	// to avoid shared-stats corruption.
	return &ProfiledOperator{
		inner: p.inner,
		stats: &ProfileStats{Name: p.stats.Name},
	}
}

// ProfiledSink wraps a Sink to collect timing and row count stats.
type ProfiledSink struct {
	inner Sink
	stats *ProfileStats
}

// WrapSink decorates a Sink with profiling.
func WrapSink(sink Sink, collector *ProfileCollector, name string) *ProfiledSink {
	return &ProfiledSink{
		inner: sink,
		stats: collector.Add(name),
	}
}

func (p *ProfiledSink) Init(ctx context.Context) error {
	return p.inner.Init(ctx)
}

func (p *ProfiledSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	rowsIn := int64(0)
	if b != nil {
		rowsIn = int64(b.ActiveLen())
	}
	start := time.Now()
	err := p.inner.Consume(ctx, b)
	atomic.AddInt64((*int64)(&p.stats.WallTime), int64(time.Since(start)))
	atomic.AddInt64(&p.stats.Calls, 1)
	atomic.AddInt64(&p.stats.RowsIn, rowsIn)
	return err
}

func (p *ProfiledSink) Finalize(ctx context.Context) error {
	start := time.Now()
	err := p.inner.Finalize(ctx)
	atomic.AddInt64((*int64)(&p.stats.WallTime), int64(time.Since(start)))
	return err
}

func (p *ProfiledSink) Close() error {
	return p.inner.Close()
}

// Inner returns the wrapped sink.
func (p *ProfiledSink) Inner() Sink {
	return p.inner
}

// WrapPipeline wraps all operators in a pipeline with profiling decorators.
// Returns the collector for reading stats after execution.
func WrapPipeline(pipeline *Pipeline) *ProfileCollector {
	collector := NewProfileCollector()

	// Wrap source
	srcName := operatorName(pipeline.Source)
	pipeline.Source = WrapSource(pipeline.Source, collector, srcName)

	// Wrap operators
	for i, op := range pipeline.Ops {
		opName := operatorName(op)
		pipeline.Ops[i] = WrapOperator(op, collector, opName)
	}

	// Wrap sink
	sinkName := operatorName(pipeline.Sink)
	pipeline.Sink = WrapSink(pipeline.Sink, collector, sinkName)

	return collector
}

// operatorName returns a human-readable name for an operator based on its type.
func operatorName(op any) string {
	t := fmt.Sprintf("%T", op)
	// Strip package prefix: "*exec.KernelFilter" → "KernelFilter"
	if idx := strings.LastIndex(t, "."); idx >= 0 {
		t = t[idx+1:]
	}
	return t
}

// FormatAnalyzeStats formats profiling stats as annotation lines.
// Each entry becomes: "  OperatorName (actual rows=N, time=Xms, calls=N)"
func FormatAnalyzeStats(stats []*ProfileStats) []string {
	lines := make([]string, 0, len(stats))
	for _, s := range stats {
		line := fmt.Sprintf("  → %s (rows_in=%d, rows_out=%d, time=%s, calls=%d)",
			s.Name, s.RowsIn, s.RowsOut, formatDuration(s.WallTime), s.Calls)
		lines = append(lines, line)
	}
	return lines
}

// formatDuration formats a duration for human display.
func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fµs", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}
