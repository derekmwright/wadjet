// SPDX-License-Identifier: MIT

package sysrows

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// Source scans one system relation. Its rows are materialized when the scan
// starts, from the storage catalog through the identity's view installed on
// the statement's context (Access), and handed out in batches of the
// engine's size; every operator above it is the ordinary engine.
type Source struct {
	rel  *syscatalog.Relation
	cat  *catalog.Catalog
	rows []map[string]any
	pos  int
	done bool
}

// NewSource is the scan of rel over cat.
func NewSource(rel *syscatalog.Relation, cat *catalog.Catalog) *Source {
	return &Source{rel: rel, cat: cat}
}

// Init materializes the relation.
func (s *Source) Init(ctx context.Context) error {
	rows, err := rowsFor(ctx, s.cat, s.rel)
	if err != nil {
		return err
	}
	s.rows = rows
	return nil
}

// Next returns the next batch, then nil. A relation with no rows still
// returns ONE empty batch carrying its columns: a relation with no rows is
// still a relation, and the result it feeds has a shape.
func (s *Source) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if s.done {
		return nil, nil
	}
	n := len(s.rows) - s.pos
	if n > batch.DefaultBatchSize {
		n = batch.DefaultBatchSize
	}
	b, err := batch.FromRowsChecked(s.rel.Columns, s.rows[s.pos:s.pos+n])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", s.rel.FuncName(), err)
	}
	s.pos += n
	if s.pos >= len(s.rows) {
		s.done = true
	}
	return b, nil
}

// Close releases the materialized rows.
func (s *Source) Close() error {
	s.rows = nil
	return nil
}
