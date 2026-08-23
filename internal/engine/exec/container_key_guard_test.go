package exec

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestMalformedContainerKeyColumnFailsLoudly pins the two defensive guards in
// #408's group-key serializer.
//
// Both used to `return buf` — a constant key for every row of the column,
// which is the defect #408 fixed wearing a different hat: one key means one
// group, so GROUP BY, DISTINCT, the set operations, COUNT(DISTINCT) and the
// hash joins all answer from a single key. The query comes back with a
// plausible wrong answer and nothing anywhere says so. A column whose storage
// contradicts its own declared shape is a bug in whoever built it, and the
// only safe report is a loud one.
func TestMalformedContainerKeyColumnFailsLoudly(t *testing.T) {
	cases := []struct {
		name string
		vec  func() *batch.Vector
		want string
	}{
		{
			// Dimension says 4 floats per row; the storage holds 2.
			name: "vector shorter than its dimension",
			vec: func() *batch.Vector {
				v := batch.NewVector(batch.TypeVector, 2)
				v.VectorDim = 4
				v.Float32Data = []float32{1, 2}
				v.Len = 2
				return v
			},
			want: "declares dimension 4",
		},
		{
			// An ARRAY column with no offsets: there is no way to know where
			// row 0's elements start or end.
			name: "array with no offsets",
			vec: func() *batch.Vector {
				v := batch.NewVector(batch.TypeArray, 2)
				v.Offsets = nil
				v.Len = 2
				return v
			},
			want: "needs offsets[1]",
		},
		{
			// Offsets say row 0 spans two elements; there is no child vector
			// holding them.
			name: "array elements with no child vector",
			vec: func() *batch.Vector {
				v := batch.NewVector(batch.TypeArray, 2)
				v.Offsets = []int32{0, 2, 2}
				v.Child = nil
				v.Len = 2
				return v
			},
			want: "no child vector",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v := tc.vec()
			err := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = RecoverFatalEval(r)
					}
				}()
				appendColumnValue(nil, v, 0, v.Type)
				return nil
			}()
			if err == nil {
				t.Fatal("a malformed container key column produced a key instead of an error — " +
					"every row of it becomes one group")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the defect (%q)", err, tc.want)
			}
			var coded *sqlerr.Error
			if !errors.As(err, &coded) {
				t.Fatalf("error %q carries no SQLSTATE; a client cannot tell it from a crash", err)
			}
		})
	}
}

// malformedArraySource hands a Pipeline one batch whose ARRAY column has no
// offsets.
type malformedArraySource struct {
	schema []parquet.Column
	served bool
}

func (s *malformedArraySource) Init(_ context.Context) error { return nil }

func (s *malformedArraySource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.served {
		return nil, nil
	}
	s.served = true
	b := batch.NewRecordBatch(s.schema, 2)
	b.Columns[0].Offsets = nil
	return b, nil
}

func (s *malformedArraySource) Close() error { return nil }

// TestMalformedContainerKeyReachesTheClientAsAnError proves the panic is the
// FatalEvalPanic class the pipeline converts, not a process kill: a grouped
// query over the malformed column returns an error rather than taking the
// worker down or answering one group.
func TestMalformedContainerKeyReachesTheClientAsAnError(t *testing.T) {
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeArray,
		ElementType: &parquet.Column{Name: "e", Type: parquet.TypeInt64}}}
	agg := NewHashAggregate([]string{"a"}, []AggColumn{
		{Func: AggCount, InputCol: "a", OutputCol: "c", OutputType: parquet.TypeInt64},
	})
	pipe := &Pipeline{Source: &malformedArraySource{schema: schema}, Sink: agg}
	err := pipe.Run(context.Background())
	if err == nil {
		t.Fatal("GROUP BY over a malformed ARRAY column succeeded; it answered one group")
	}
	if !strings.Contains(err.Error(), "malformed ARRAY group key column") {
		t.Fatalf("error %q does not name the malformed column", err)
	}
}
