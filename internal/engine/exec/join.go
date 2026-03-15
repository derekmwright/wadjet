package exec

import (
	"context"
	"fmt"
	"sync"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// JoinType identifies the kind of join.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
)

// HashJoin implements a hash join with build and probe phases.
// Build side is fully consumed into a hash table, then probe side streams through.
type HashJoin struct {
	JoinType  JoinType
	LeftKeys  []string // join key columns from left (probe) side
	RightKeys []string // join key columns from right (build) side

	mu        sync.Mutex
	hashTable map[string][]map[string]any // hash key -> build side rows
	buildDone bool
	buildSchema []parquet.Column
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(joinType JoinType, leftKeys, rightKeys []string) *HashJoin {
	return &HashJoin{
		JoinType:  joinType,
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
		hashTable: make(map[string][]map[string]any),
	}
}

// Build consumes all rows from the build (right) side into the hash table.
func (h *HashJoin) Build(ctx context.Context, source Source) error {
	if err := source.Init(ctx); err != nil {
		return fmt.Errorf("build source init: %w", err)
	}
	defer source.Close()

	for {
		b, err := source.Next(ctx)
		if err != nil {
			return fmt.Errorf("build source next: %w", err)
		}
		if b == nil {
			break
		}

		h.mu.Lock()
		if h.buildSchema == nil {
			h.buildSchema = b.Schema
		}

		rows := b.ToRows()
		for _, row := range rows {
			key := h.buildKey(row)
			h.hashTable[key] = append(h.hashTable[key], row)
		}
		h.mu.Unlock()
	}

	h.buildDone = true
	return nil
}

// BuildFromRows loads the build side directly from rows.
func (h *HashJoin) BuildFromRows(schema []parquet.Column, rows []map[string]any) {
	h.buildSchema = schema
	for _, row := range rows {
		key := h.buildKey(row)
		h.hashTable[key] = append(h.hashTable[key], row)
	}
	h.buildDone = true
}

// Probe is a UnaryOperator that probes the hash table for each input batch.
func (h *HashJoin) Probe() *HashJoinProbe {
	return &HashJoinProbe{join: h}
}

func (h *HashJoin) buildKey(row map[string]any) string {
	key := ""
	for i, col := range h.RightKeys {
		if i > 0 {
			key += "\x00"
		}
		key += fmt.Sprint(row[col])
	}
	return key
}

func (h *HashJoin) probeKey(b *batch.RecordBatch, row int) string {
	key := ""
	for i, col := range h.LeftKeys {
		if i > 0 {
			key += "\x00"
		}
		v := b.ColumnByName(col)
		if v != nil {
			key += fmt.Sprint(v.GetValue(row))
		}
	}
	return key
}

// HashJoinProbe is a UnaryOperator that probes the build-side hash table.
type HashJoinProbe struct {
	join *HashJoin
}

func (p *HashJoinProbe) Init(_ context.Context) error {
	if !p.join.buildDone {
		return fmt.Errorf("hash join build phase not complete")
	}
	return nil
}

func (p *HashJoinProbe) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// Build output schema: left columns + right columns (excluding join keys from right)
	outSchema := p.outputSchema(in.Schema)

	var resultRows []map[string]any

	iter := batchIterator(in)
	for _, row := range iter {
		key := p.join.probeKey(in, row)
		matches := p.join.hashTable[key]

		if len(matches) == 0 {
			if p.join.JoinType == LeftJoin {
				// Emit left row with nulls for right side
				outRow := make(map[string]any, len(outSchema))
				for _, col := range in.Schema {
					outRow[col.Name] = in.ColumnByName(col.Name).GetValue(row)
				}
				for _, col := range p.join.buildSchema {
					if !p.isRightJoinKey(col.Name) || !p.leftHasColumn(col.Name, in.Schema) {
						if _, exists := outRow[col.Name]; !exists {
							outRow[col.Name] = nil
						}
					}
				}
				resultRows = append(resultRows, outRow)
			}
			continue
		}

		for _, matchRow := range matches {
			outRow := make(map[string]any, len(outSchema))
			// Left side values
			for _, col := range in.Schema {
				outRow[col.Name] = in.ColumnByName(col.Name).GetValue(row)
			}
			// Right side values (skip columns that would collide with left)
			for k, v := range matchRow {
				if _, exists := outRow[k]; !exists {
					outRow[k] = v
				}
			}
			resultRows = append(resultRows, outRow)
		}
	}

	if len(resultRows) == 0 {
		return nil, nil
	}

	return batch.FromRows(outSchema, resultRows), nil
}

func (p *HashJoinProbe) Close() error { return nil }

func (p *HashJoinProbe) outputSchema(leftSchema []parquet.Column) []parquet.Column {
	var out []parquet.Column
	out = append(out, leftSchema...)

	seen := make(map[string]bool, len(leftSchema))
	for _, col := range leftSchema {
		seen[col.Name] = true
	}

	for _, col := range p.join.buildSchema {
		if !seen[col.Name] {
			col.Nullable = true // right side can be null in left join
			out = append(out, col)
		}
	}
	return out
}

func (p *HashJoinProbe) isRightJoinKey(name string) bool {
	for _, k := range p.join.RightKeys {
		if k == name {
			return true
		}
	}
	return false
}

func (p *HashJoinProbe) leftHasColumn(name string, leftSchema []parquet.Column) bool {
	for _, col := range leftSchema {
		if col.Name == name {
			return true
		}
	}
	return false
}
