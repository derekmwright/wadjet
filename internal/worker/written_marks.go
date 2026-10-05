// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"sort"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// writtenMarks collects, for one task, the columns its output sinks wrote as
// columns created from an unconstrained numeric (ADR-0024 §10). A `.wshf`
// exchange header carries no such mark, so the task reports it beside its
// files (distributed.ResultNotification.UnconstrainedColumns) and every
// reader of the stage's output stamps it back (applyDeclaredScanSchema).
//
// A name is reported only when every batch that carried the column marked
// it: one unmarked batch makes it unmarked, which is the column's printing
// at 1c1083d0 rather than a mark on a value that is not the column's.
type writtenMarks struct {
	mu       sync.Mutex
	marked   map[string]bool
	unmarked map[string]bool
	last     *parquet.Column
}

type writtenMarksKey struct{}

// withWrittenMarks gives a task's context its collector.
func withWrittenMarks(ctx context.Context) (context.Context, *writtenMarks) {
	m := &writtenMarks{}
	return context.WithValue(ctx, writtenMarksKey{}, m), m
}

// noteWrittenSchema records one output batch's schema; an output sink calls it
// for every batch it writes. A schema already seen (decoded and pooled
// batches share one) costs one comparison.
func noteWrittenSchema(ctx context.Context, schema []parquet.Column) {
	if ctx == nil || len(schema) == 0 {
		return
	}
	m, _ := ctx.Value(writtenMarksKey{}).(*writtenMarks)
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last == &schema[0] {
		return
	}
	m.last = &schema[0]
	names := exec.UnconstrainedNames(schema)
	inBatch := make(map[string]bool, len(names))
	for _, n := range names {
		inBatch[n] = true
	}
	for _, c := range schema {
		if c.Type != parquet.TypeDecimal {
			continue
		}
		k := exec.UnconstrainedKey(c.Name)
		if inBatch[k] {
			if m.marked == nil {
				m.marked = make(map[string]bool)
			}
			m.marked[k] = true
			continue
		}
		if m.unmarked == nil {
			m.unmarked = make(map[string]bool)
		}
		m.unmarked[k] = true
	}
}

// names is the task's answer, sorted.
func (m *writtenMarks) names() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.marked {
		if !m.unmarked[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
