package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// F5 regression: an ORDER BY key whose column type resolves to no
// comparator must fail loudly, not silently merge every ORDER-BY peer group
// that differs only on that key into one — the same failure mode a dropped
// PARTITION BY key has. Before the fix, globalWindowStreamer.resolveKernels
// set a nil compare entry and samePeer silently skipped it instead of
// erroring, exactly the bug class sort_merge_join.go's
// resolveCompareKernels already refuses for join keys.
func TestGlobalWindowStreamerResolveKernels_UnsupportedTypeErrors(t *testing.T) {
	v := batch.NewVector(batch.TypeID(200), 3)
	b := &batch.RecordBatch{
		Columns: []*batch.Vector{v},
		Schema:  []parquet.Column{{Name: "o", Type: parquet.TypeID(200)}},
		Len:     3,
	}
	s := &globalWindowStreamer{orderIdxs: []int{0}}
	if err := s.resolveKernels(b); err == nil {
		t.Fatal("expected an error for an unsupported ORDER BY type, got nil")
	}
}
