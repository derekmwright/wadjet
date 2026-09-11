package exec

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ColumnPrune is a lightweight UnaryOperator that drops unneeded columns
// from batches flowing through a pipeline. Unlike Project, it operates at
// the column/vector level (zero-copy) — O(keepCols) per batch, not O(rows).
type ColumnPrune struct {
	keepCols map[string]bool
	resolved bool
	indices  []int
	schema   []parquet.Column
}

// NewColumnPrune creates a column prune operator that keeps only the named columns.
func NewColumnPrune(keep []string) *ColumnPrune {
	m := make(map[string]bool, len(keep))
	for _, c := range keep {
		m[c] = true
	}
	return &ColumnPrune{keepCols: m}
}

func (c *ColumnPrune) Init(_ context.Context) error { return nil }

func (c *ColumnPrune) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}

	// Resolve indices lazily on the first batch: keep all exact-name matches, then
	// add columnIndexFallback matches for folded and alias-qualified plan references (#731).
	// The fallback is additive: duplicate exact names all remain; ambiguous references
	// never guess an arm. Catalog CamelCase and qualified references to bare outputs
	// must follow the same resolution rule as other consumers.
	// A total miss passes the full-width batch through, as other pruners do;
	// pruning is optional, but a zero-column stream can break a downstream shuffle.
	// See docs/internals/column-prune-reference-resolution.md for the design.
	if !c.resolved {
		c.resolved = true
		keep := make([]bool, len(in.Schema))
		for i, col := range in.Schema {
			if c.keepCols[col.Name] {
				keep[i] = true
			}
		}
		for name := range c.keepCols {
			if i := columnIndexFallback(in, name); i >= 0 {
				keep[i] = true
			}
		}
		for i, col := range in.Schema {
			if keep[i] {
				c.indices = append(c.indices, i)
				c.schema = append(c.schema, col)
			}
		}
		// Keeping everything, or nothing: either way, no pruning.
		if len(c.indices) == len(in.Schema) || len(c.indices) == 0 {
			c.indices = nil
			c.schema = nil
		}
	}

	// No pruning needed — pass through.
	if c.indices == nil {
		return in, nil
	}

	out := &batch.RecordBatch{
		Schema:  c.schema,
		Columns: make([]*batch.Vector, len(c.indices)),
		Len:     in.Len,
		Sel:     in.Sel,
	}
	for i, idx := range c.indices {
		out.Columns[i] = in.Columns[idx]
	}
	return out, nil
}

func (c *ColumnPrune) Close() error { return nil }

func (c *ColumnPrune) Clone() UnaryOperator {
	keep := make([]string, 0, len(c.keepCols))
	for col := range c.keepCols {
		keep = append(keep, col)
	}
	return NewColumnPrune(keep)
}
