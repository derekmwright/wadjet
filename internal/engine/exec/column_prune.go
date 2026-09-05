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

	// Lazy resolve column indices on first batch.
	//
	// Two passes, and the second is the one that survives a name the batch
	// spells differently. The keep list is a set of column REFERENCES off
	// the plan: it arrives folded from the lexer (#731) while the batch
	// carries the catalog's own spelling (`WatchID` for a parquet-registered
	// table), and it may be alias-QUALIFIED (`a.regionid`) over a stream
	// that publishes the column bare — which is ordinary for a CTE read
	// twice under two aliases. columnIndexFallback is the resolver that owns
	// both of those, and it is what every other consumer of a planner-emitted
	// column name in this package already uses; a byte-exact map probe alone
	// is not that rule restated, it is a different one. The pass is ADDITIVE
	// — it can only mark a column kept that the exact pass did not — so a
	// schema carrying two columns of one name still keeps both, and an
	// ambiguous reference declines to resolve rather than guessing an arm.
	//
	// A TOTAL miss passes the batch through at full width instead of
	// emitting a ZERO-COLUMN one. That is the fallback every other pruner on
	// this path already takes (worker.wshfShufflePruneKeep,
	// cachedFileStreamSource.projectColumns, physical.buildReadSchema): a
	// prune is a memory optimization, so declining to prune is always
	// available, while emitting no columns at all is not a narrower answer
	// but a broken stream — the shuffle sink above it refuses with
	// `key "a.regionid" not in schema` and the query fails.
	//
	// Measured on the camel-case invariance battery with
	// WADJET_SCAN_COL_SANITIZE=0 and every other site fixed: reverting this
	// pass alone costs 6 cells, and 3 of those survive even when the scan's
	// read set carries the schema's spelling — a CTE read twice under two
	// aliases reaches here with a keep list that is entirely qualified, which
	// no spelling fix upstream can repair.
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
