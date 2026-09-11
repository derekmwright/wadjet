package exec

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SetOpEmit reads one grouped row with left/right multiplicities A/B and drops counts (#346).
// INTERSECT emits 1 iff A>0 and B>0; INTERSECT ALL emits min(A,B).
// EXCEPT emits 1 iff A>0 and B==0; EXCEPT ALL emits max(0,A-B).
// Distinct forms use selection and zero-copy column slicing; ALL materializes
// multiplicity, with output size equal to the true answer size for the input batch.
// Values are opaque: upstream GROUP BY already makes NULLs equal for membership.
// Counts are non-NULL SUMs of 0/1 tags over at least one row per group;
// read a NULL count as zero defensively.
// See docs/internals/set-operation-multiplicity-emission.md for the design.
type SetOpEmit struct {
	op    string // "intersect" | "except"
	all   bool
	lname string
	rname string

	resolved  bool
	lIdx      int
	rIdx      int
	keepIdx   []int
	outSchema []parquet.Column
}

// NewSetOpEmit validates the spec and constructs the operator.
func NewSetOpEmit(op string, all bool, leftCol, rightCol string) (*SetOpEmit, error) {
	if op != "intersect" && op != "except" {
		return nil, fmt.Errorf("set_op_emit: unknown operation %q", op)
	}
	if leftCol == "" || rightCol == "" || leftCol == rightCol {
		return nil, fmt.Errorf("set_op_emit: count columns must be two distinct names, got (%q, %q)", leftCol, rightCol)
	}
	return &SetOpEmit{op: op, all: all, lname: leftCol, rname: rightCol}, nil
}

func (s *SetOpEmit) Init(_ context.Context) error { return nil }

func (s *SetOpEmit) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || in.ActiveLen() == 0 {
		return nil, nil
	}
	if !s.resolved {
		if err := s.resolve(in); err != nil {
			return nil, err
		}
	}
	lVec, rVec := in.Columns[s.lIdx], in.Columns[s.rIdx]

	if !s.all {
		// Distinct forms: k ∈ {0,1} — select, never copy. The selection is
		// freshly allocated per batch: downstream breakers and sinks may
		// retain the batch, so a reused scratch slice would corrupt an
		// earlier output.
		sel := make([]uint32, 0, in.ActiveLen())
		for i := 0; i < in.ActiveLen(); i++ {
			row := i
			if in.Sel != nil {
				row = int(in.Sel[i])
			}
			if s.copies(setOpCount(lVec, row), setOpCount(rVec, row)) > 0 {
				sel = append(sel, uint32(row))
			}
		}
		if len(sel) == 0 {
			return nil, nil
		}
		out := &batch.RecordBatch{
			Schema:  s.outSchema,
			Columns: make([]*batch.Vector, len(s.keepIdx)),
			Len:     in.Len,
			Sel:     sel,
		}
		for i, idx := range s.keepIdx {
			out.Columns[i] = in.Columns[idx]
		}
		return out, nil
	}

	// ALL forms: k may exceed 1, so rows materialize k times each.
	cols := make([]*batch.Vector, len(s.keepIdx))
	for i, idx := range s.keepIdx {
		cols[i] = batch.NewVectorLike(in.Columns[idx])
	}
	total := 0
	for i := 0; i < in.ActiveLen(); i++ {
		row := i
		if in.Sel != nil {
			row = int(in.Sel[i])
		}
		k := s.copies(setOpCount(lVec, row), setOpCount(rVec, row))
		for c := int64(0); c < k; c++ {
			for j, idx := range s.keepIdx {
				cols[j].AppendFrom(in.Columns[idx], row)
			}
		}
		total += int(k)
	}
	if total == 0 {
		return nil, nil
	}
	return &batch.RecordBatch{
		Schema:  s.outSchema,
		Columns: cols,
		Len:     total,
	}, nil
}

// copies is the operation's count rule.
func (s *SetOpEmit) copies(a, b int64) int64 {
	switch s.op {
	case "intersect":
		if !s.all {
			if a > 0 && b > 0 {
				return 1
			}
			return 0
		}
		if a < b {
			return a
		}
		return b
	default: // "except"
		if !s.all {
			if a > 0 && b == 0 {
				return 1
			}
			return 0
		}
		if a > b {
			return a - b
		}
		return 0
	}
}

// setOpCount reads a count cell as int64. The counting aggregate's SUM output is
// Int64 here, but the read goes through the numeric accessor so a widened
// (Float64) partial path cannot silently zero it; NULL reads as 0.
func setOpCount(v *batch.Vector, row int) int64 {
	f, ok := v.GetNumericFloat64(row)
	if !ok {
		return 0
	}
	return int64(f)
}

func (s *SetOpEmit) resolve(in *batch.RecordBatch) error {
	s.lIdx, s.rIdx = -1, -1
	for i, col := range in.Schema {
		switch col.Name {
		case s.lname:
			s.lIdx = i
		case s.rname:
			s.rIdx = i
		default:
			s.keepIdx = append(s.keepIdx, i)
			s.outSchema = append(s.outSchema, col)
		}
	}
	if s.lIdx < 0 || s.rIdx < 0 {
		return fmt.Errorf("set_op_emit: count columns (%q, %q) not in input schema %v",
			s.lname, s.rname, schemaNames(in.Schema))
	}
	if len(s.keepIdx) == 0 {
		return fmt.Errorf("set_op_emit: input carries only the count columns — no result columns to emit")
	}
	s.resolved = true
	return nil
}

func schemaNames(schema []parquet.Column) []string {
	names := make([]string, len(schema))
	for i, c := range schema {
		names[i] = c.Name
	}
	return names
}

func (s *SetOpEmit) Close() error { return nil }

// Clone gives parallel drivers a scratch-independent copy.
func (s *SetOpEmit) Clone() UnaryOperator {
	return &SetOpEmit{op: s.op, all: s.all, lname: s.lname, rname: s.rname}
}
