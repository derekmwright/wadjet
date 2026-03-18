package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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

	// Lazy resolve column indices on first batch.
	if !c.resolved {
		c.resolved = true
		for i, col := range in.Schema {
			if c.keepCols[col.Name] {
				c.indices = append(c.indices, i)
				c.schema = append(c.schema, col)
			}
		}
		// If we'd keep everything, short-circuit.
		if len(c.indices) == len(in.Schema) {
			c.indices = nil
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
