package exec

import (
	"context"
	"sort"
	"sync"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/memory"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// WindowFunc identifies a window function type.
type WindowFunc int

const (
	WinRowNumber WindowFunc = iota
	WinRank
	WinDenseRank
	WinSum
	WinCount
	WinAvg
	WinMin
	WinMax
)

// WindowFrameSpec describes a window frame specification for execution.
type WindowFrameSpec struct {
	Mode  string // "rows" or "range"
	Start WindowBound
	End   WindowBound
}

// WindowBound describes one end of a window frame.
type WindowBound struct {
	Type   string // "unbounded_preceding", "preceding", "current_row", "following", "unbounded_following"
	Offset int
}

// WindowColumn defines a window function computation.
type WindowColumn struct {
	Func        WindowFunc
	InputCol    string // for aggregate window functions (empty for ranking funcs)
	OutputCol   string
	OutputType  parquet.TypeID
	PartitionBy []string
	OrderBy     []SortKey
	Frame       *WindowFrameSpec // optional frame specification
}

// Window is a SinkSource that collects all rows, partitions and sorts them,
// computes window function values, and emits the original rows with computed
// window columns appended.
// When a SpillManager is set, Window will spill input batches to disk under
// memory pressure and read them back during Finalize.
type Window struct {
	Columns []WindowColumn
	Spill   *memory.SpillManager // optional: enables spill-to-disk

	mu         sync.Mutex
	batches    []*batch.RecordBatch
	totalRows  int
	schema     []parquet.Column
	spillFiles []string

	result  []*batch.RecordBatch
	pos     int
	emitted bool
}

// NewWindow creates a new window operator.
func NewWindow(cols []WindowColumn) *Window {
	return &Window{Columns: cols}
}

func (w *Window) Init(_ context.Context) error {
	w.batches = nil
	w.totalRows = 0
	w.result = nil
	w.pos = 0
	w.emitted = false
	return nil
}

func (w *Window) Consume(_ context.Context, b *batch.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.schema == nil {
		w.schema = b.Schema
	}
	w.batches = append(w.batches, b)
	w.totalRows += b.ActiveLen()

	// Spill to disk if memory pressure is high
	if w.Spill != nil && w.Spill.ShouldSpill() && len(w.batches) > 0 {
		var rows []map[string]any
		for _, sb := range w.batches {
			rows = append(rows, sb.ToRows()...)
		}
		path, err := w.Spill.SpillRows(rows)
		if err != nil {
			return err
		}
		w.spillFiles = append(w.spillFiles, path)
		w.batches = w.batches[:0]
		w.totalRows = 0
	}
	return nil
}

func (w *Window) Finalize(_ context.Context) error {
	// Collect all rows: in-memory + spilled
	rows := make([]map[string]any, 0, w.totalRows)
	for _, b := range w.batches {
		rows = append(rows, b.ToRows()...)
	}
	w.batches = nil

	for _, f := range w.spillFiles {
		spilled, err := memory.ReadSpilledRows(f)
		if err != nil {
			return err
		}
		rows = append(rows, spilled...)
	}
	w.spillFiles = nil

	if len(rows) == 0 {
		return nil
	}

	// Build output schema: original columns + window columns
	outSchema := make([]parquet.Column, len(w.schema))
	copy(outSchema, w.schema)
	for _, wc := range w.Columns {
		outSchema = append(outSchema, parquet.Column{
			Name:     wc.OutputCol,
			Type:     wc.OutputType,
			Nullable: true,
		})
	}

	// Compute each window function
	for _, wc := range w.Columns {
		computeWindowFunc(rows, wc)
	}

	// Materialize into output batches
	for pos := 0; pos < len(rows); {
		end := pos + batch.DefaultBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		w.result = append(w.result, batch.FromRows(outSchema, rows[pos:end]))
		pos = end
	}
	return nil
}

func (w *Window) Close() error { return nil }

// Next returns windowed results in batches.
func (w *Window) Next(_ context.Context) (*batch.RecordBatch, error) {
	if w.pos >= len(w.result) {
		return nil, nil
	}
	b := w.result[w.pos]
	w.pos++
	return b, nil
}

// computeWindowFunc computes a single window function over all rows,
// writing the result directly into each row map.
func computeWindowFunc(rows []map[string]any, wc WindowColumn) {
	// Build sort keys: partition columns first, then order columns
	var sortKeys []SortKey
	for _, pc := range wc.PartitionBy {
		sortKeys = append(sortKeys, SortKey{Column: pc, Order: Ascending})
	}
	sortKeys = append(sortKeys, wc.OrderBy...)

	// Sort rows by partition + order keys (stable to preserve input order)
	if len(sortKeys) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			for _, key := range sortKeys {
				vi := rows[i][key.Column]
				vj := rows[j][key.Column]
				viNil := vi == nil
				vjNil := vj == nil
				if viNil && vjNil {
					continue
				}
				if viNil || vjNil {
					if key.NullsLast {
						return !viNil
					}
					return viNil
				}
				cmp := compareAny(vi, vj)
				if cmp == 0 {
					continue
				}
				if key.Order == Descending {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}

	// Walk rows and compute window values per partition
	n := len(rows)
	i := 0
	for i < n {
		// Find partition boundary
		partEnd := i + 1
		for partEnd < n && samePartition(rows[i], rows[partEnd], wc.PartitionBy) {
			partEnd++
		}

		partition := rows[i:partEnd]
		computePartition(partition, wc)
		i = partEnd
	}
}

// samePartition returns true if two rows have the same partition key values.
func samePartition(a, b map[string]any, partCols []string) bool {
	for _, col := range partCols {
		if compareAny(a[col], b[col]) != 0 {
			return false
		}
	}
	return true
}

// computePartition computes the window function for a single partition.
func computePartition(rows []map[string]any, wc WindowColumn) {
	switch wc.Func {
	case WinRowNumber:
		for i, row := range rows {
			row[wc.OutputCol] = int64(i + 1)
		}

	case WinRank:
		rank := int64(1)
		for i, row := range rows {
			if i > 0 && !sameOrderValues(rows[i-1], row, wc.OrderBy) {
				rank = int64(i + 1)
			}
			row[wc.OutputCol] = rank
		}

	case WinDenseRank:
		rank := int64(1)
		for i, row := range rows {
			if i > 0 && !sameOrderValues(rows[i-1], row, wc.OrderBy) {
				rank++
			}
			row[wc.OutputCol] = rank
		}

	case WinSum:
		if len(wc.OrderBy) > 0 {
			// Running sum (cumulative)
			var sum float64
			for _, row := range rows {
				v := toFloat64(row[wc.InputCol])
				sum += v
				row[wc.OutputCol] = sum
			}
		} else {
			// Partition-level sum
			var sum float64
			for _, row := range rows {
				sum += toFloat64(row[wc.InputCol])
			}
			for _, row := range rows {
				row[wc.OutputCol] = sum
			}
		}

	case WinCount:
		if len(wc.OrderBy) > 0 {
			// Running count
			var count int64
			for _, row := range rows {
				count++
				row[wc.OutputCol] = count
			}
		} else {
			// Partition-level count
			count := int64(len(rows))
			for _, row := range rows {
				row[wc.OutputCol] = count
			}
		}

	case WinAvg:
		if len(wc.OrderBy) > 0 {
			// Running average
			var sum float64
			for i, row := range rows {
				sum += toFloat64(row[wc.InputCol])
				row[wc.OutputCol] = sum / float64(i+1)
			}
		} else {
			// Partition-level average
			var sum float64
			for _, row := range rows {
				sum += toFloat64(row[wc.InputCol])
			}
			avg := sum / float64(len(rows))
			for _, row := range rows {
				row[wc.OutputCol] = avg
			}
		}

	case WinMin:
		if len(wc.OrderBy) > 0 {
			// Running min
			var minVal any
			for _, row := range rows {
				v := row[wc.InputCol]
				if minVal == nil || compareAny(v, minVal) < 0 {
					minVal = v
				}
				row[wc.OutputCol] = minVal
			}
		} else {
			// Partition-level min
			var minVal any
			for _, row := range rows {
				v := row[wc.InputCol]
				if minVal == nil || compareAny(v, minVal) < 0 {
					minVal = v
				}
			}
			for _, row := range rows {
				row[wc.OutputCol] = minVal
			}
		}

	case WinMax:
		if len(wc.OrderBy) > 0 {
			// Running max
			var maxVal any
			for _, row := range rows {
				v := row[wc.InputCol]
				if maxVal == nil || compareAny(v, maxVal) > 0 {
					maxVal = v
				}
				row[wc.OutputCol] = maxVal
			}
		} else {
			// Partition-level max
			var maxVal any
			for _, row := range rows {
				v := row[wc.InputCol]
				if maxVal == nil || compareAny(v, maxVal) > 0 {
					maxVal = v
				}
			}
			for _, row := range rows {
				row[wc.OutputCol] = maxVal
			}
		}
	}
}

// sameOrderValues returns true if two rows have identical values for all order columns.
func sameOrderValues(a, b map[string]any, orderBy []SortKey) bool {
	for _, ob := range orderBy {
		if compareAny(a[ob.Column], b[ob.Column]) != 0 {
			return false
		}
	}
	return true
}
