// SPDX-License-Identifier: MIT

package exec

import (
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// UnconstrainedNames is the set of DECIMAL columns of schema created from an
// unconstrained numeric (parquet.Column.Unconstrained, ADR-0024 §10), by
// name, sorted. A name two columns share is left out unless both are marked:
// a reader stamping by name could not tell them apart, and an unmarked
// column printed as a marked one would lose its trailing zeros.
func UnconstrainedNames(schema []parquet.Column) []string {
	var marked, unmarked map[string]bool
	for _, c := range schema {
		if c.Type != parquet.TypeDecimal {
			continue
		}
		k := strings.ToLower(c.Name)
		if c.Unconstrained {
			if marked == nil {
				marked = make(map[string]bool)
			}
			marked[k] = true
			continue
		}
		if unmarked == nil {
			unmarked = make(map[string]bool)
		}
		unmarked[k] = true
	}
	var out []string
	for k := range marked {
		if !unmarked[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// UnconstrainedKey is the form UnconstrainedNames and UnconstrainedStamp
// compare a column name in.
func UnconstrainedKey(name string) string { return strings.ToLower(name) }

// UnconstrainedStamp marks the DECIMAL columns a stage reads from an
// exchange that the producing stage wrote as columns created from an
// unconstrained numeric (ADR-0024 §10). A `.wshf` header names a column's
// type, precision and scale and nothing else, so a batch decoded from one
// has lost the mark the producer's batch carried; every exchange read
// applies the producer's answer here, the way the gather applies the plan's
// (WithPlannedUnconstrained). The zero value stamps nothing.
//
// It costs one comparison per batch: decoded batches of one file share a
// schema slice, and the stamped copy is reused for as long as they do.
type UnconstrainedStamp struct {
	names map[string]bool
	in    []parquet.Column
	out   []parquet.Column
}

// NewUnconstrainedStamp is the stamp for the named columns (case-insensitive).
func NewUnconstrainedStamp(names ...[]string) *UnconstrainedStamp {
	s := &UnconstrainedStamp{}
	for _, list := range names {
		for _, n := range list {
			if s.names == nil {
				s.names = make(map[string]bool)
			}
			s.names[strings.ToLower(n)] = true
		}
	}
	return s
}

// Apply marks b's named DECIMAL columns, replacing b.Schema with a stamped
// copy (the decoded schema is shared and never written).
func (s *UnconstrainedStamp) Apply(b *batch.RecordBatch) {
	if s == nil || len(s.names) == 0 || b == nil || len(b.Schema) == 0 {
		return
	}
	if len(s.in) == len(b.Schema) && &s.in[0] == &b.Schema[0] {
		b.Schema = s.out
		return
	}
	in := b.Schema
	out := in
	for i, c := range in {
		if c.Type != parquet.TypeDecimal || c.Unconstrained || !s.names[strings.ToLower(c.Name)] {
			continue
		}
		if &out[0] == &in[0] {
			out = make([]parquet.Column, len(in))
			copy(out, in)
		}
		out[i].Unconstrained = true
	}
	s.in, s.out = in, out
	b.Schema = out
}
