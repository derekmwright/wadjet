package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// Distinct is a UnaryOperator that removes duplicate rows.
// It maintains a hash set of serialized row keys and drops rows already seen.
type Distinct struct {
	seen   map[string]struct{}
	keyBuf []byte
}

// NewDistinct creates a new Distinct operator.
func NewDistinct() *Distinct {
	return &Distinct{}
}

func (d *Distinct) Init(_ context.Context) error {
	d.seen = make(map[string]struct{})
	d.keyBuf = make([]byte, 0, 256)
	return nil
}

func (d *Distinct) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	sel := make([]uint16, 0, in.Len)

	checkRow := func(row int) {
		key := d.rowKey(in, row)
		if _, exists := d.seen[key]; !exists {
			d.seen[key] = struct{}{}
			sel = append(sel, uint16(row))
		}
	}
	if in.Sel != nil {
		for _, idx := range in.Sel {
			checkRow(int(idx))
		}
	} else {
		for i := 0; i < in.Len; i++ {
			checkRow(i)
		}
	}

	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (d *Distinct) Close() error {
	d.seen = nil
	return nil
}

// rowKey serializes all column values for a row into a unique string key.
func (d *Distinct) rowKey(b *batch.RecordBatch, row int) string {
	d.keyBuf = d.keyBuf[:0]
	for i, col := range b.Columns {
		if i > 0 {
			d.keyBuf = append(d.keyBuf, 0)
		}
		if col.Nulls.IsNullFast(row) {
			d.keyBuf = append(d.keyBuf, "<null>"...)
			continue
		}
		d.keyBuf = appendColumnValue(d.keyBuf, col, row, col.Type)
	}
	return string(d.keyBuf)
}
