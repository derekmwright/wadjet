package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func renamesFor(pairs ...[2]string) []physical.OutputRename {
	out := make([]physical.OutputRename, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, physical.OutputRename{From: p[0], To: p[1]})
	}
	return out
}

// A duplicate output name is addressed ORDINALLY — the k-th rename spelled X
// takes the k-th column spelled X — and a group the rule cannot satisfy must
// resolve to NOTHING rather than to a first match. Handing every member of a
// group the same index is the defect renameSourceIndices exists to end, and
// resolveRenameSource is deterministic in From, so falling back for a GROUP
// reintroduces it exactly.
func TestRenameSourceIndicesDistinctOrRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []string
		renames []physical.OutputRename
		want    []int
	}{
		{"unique names resolve as before",
			[]string{"a", "b"}, renamesFor([2]string{"a", "x"}, [2]string{"b", "y"}), []int{0, 1}},
		{"two columns for two renames",
			[]string{"u", "u"}, renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}), []int{0, 1}},
		{"duplicates separated by another column",
			[]string{"u", "other", "u"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}), []int{0, 2}},
		{"three columns for three renames",
			[]string{"u", "u", "u"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}, [2]string{"u", "u"}),
			[]int{0, 1, 2}},
		// ONE column for N outputs is the shared-SOURCE case, not a failure:
		// the producer did not materialize the select list, so N items
		// reading one source share one column. `SELECT DISTINCT k AS u, k AS
		// u` really is that column twice, and refusing it drops a column.
		{"one column for two renames is that column twice",
			[]string{"u", "other"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}), []int{0, 0}},
		{"one column for three renames",
			[]string{"other", "u"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}, [2]string{"u", "u"}),
			[]int{1, 1, 1}},
		// SEVERAL but fewer than the outputs asking: no counting rule maps
		// them, so the group resolves to nothing rather than to a first match.
		{"two columns for three renames is refused",
			[]string{"u", "u", "other"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}, [2]string{"u", "u"}),
			[]int{-1, -1, -1}},
		{"no column at all is refused",
			[]string{"other"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}), []int{-1, -1}},
		// A refused group must not take its neighbours down with it.
		{"an unsatisfiable group does not disturb a resolvable one",
			[]string{"u", "u", "keep"},
			renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}, [2]string{"u", "u"},
				[2]string{"keep", "keep"}),
			[]int{-1, -1, -1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := renameSourceIndices(tc.columns, tc.renames)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// A group that cannot be satisfied has to turn the whole projection OFF. The
// first cut resolved it to a first match instead, so project stayed true and
// apply copied one source into every output — the original bug, silently.
func TestUnsatisfiableDuplicateGroupDegradesToRenameOnly(t *testing.T) {
	renames := renamesFor([2]string{"u", "u"}, [2]string{"u", "u"}, [2]string{"u", "u"})
	br := newBatchRenamer(renames, []string{"u", "u", "other"})
	if br.project {
		t.Errorf("project stayed on for a name that addresses 2 columns and 3 outputs — " +
			"apply would then hand some of them the same column")
	}
}

// The shared-SOURCE case keeps projecting: one column read by N outputs is a
// complete answer, and refusing it would drop columns from the result — which
// is what `SELECT DISTINCT k AS u, k AS u` returned when the first cut of the
// refusal was too broad.
func TestSharedSourceDuplicateGroupStillProjects(t *testing.T) {
	renames := renamesFor([2]string{"u", "u"}, [2]string{"u", "u"})
	br := newBatchRenamer(renames, []string{"u", "other"})
	if !br.project {
		t.Errorf("project turned off for two outputs reading ONE source column — " +
			"the result would lose a column")
	}
}

// apply must resolve against the batch it is HANDED. Caching the indices the
// renamer was built with is wrong in a way a length check cannot catch: a
// batch of the same width whose columns are in a different order reads them
// transposed.
func TestBatchRenamerResolvesAgainstEachBatch(t *testing.T) {
	renames := renamesFor([2]string{"a", "x"}, [2]string{"b", "y"})
	br := newBatchRenamer(renames, []string{"a", "b"})
	if !br.project {
		t.Fatalf("expected a projecting renamer")
	}

	// Same width, reversed order — what a differently-planned upstream stage
	// or a replayed spill batch can hand over.
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "b", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeInt64},
	}, 1)
	b.Columns[0].SetValue(0, int64(200)) // b
	b.Columns[1].SetValue(0, int64(100)) // a

	out := br.apply(b)
	if len(out.Columns) != 2 {
		t.Fatalf("output has %d columns, want 2", len(out.Columns))
	}
	if got := out.Columns[0].GetValue(0); got != int64(100) {
		t.Errorf("output column x = %v, want 100 (the value of a) — the renamer read the "+
			"batch in the order it was BUILT with, not the order it was given", got)
	}
	if got := out.Columns[1].GetValue(0); got != int64(200) {
		t.Errorf("output column y = %v, want 200 (the value of b)", got)
	}
	if out.Schema[0].Name != "x" || out.Schema[1].Name != "y" {
		t.Errorf("output schema %v, want [x y]", out.Schema)
	}
}

// The ordinal rule under a batch that interleaves a hidden sort key: names
// scope the matching, so the k-th `u` column in THIS batch is what the k-th
// `u` rename reads, and __sortkey_N — a spelling no user alias can collide
// with — neither joins the group nor shifts it.
func TestDuplicateRenamesFollowTheBatchOrder(t *testing.T) {
	renames := renamesFor([2]string{"u", "u"}, [2]string{"u", "u"})
	br := newBatchRenamer(renames, []string{"u", "u"})
	if !br.project {
		t.Fatalf("expected a projecting renamer")
	}
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "u", Type: parquet.TypeInt64},
		{Name: "__sortkey_0", Type: parquet.TypeInt64},
		{Name: "u", Type: parquet.TypeInt64},
	}, 1)
	b.Columns[0].SetValue(0, int64(1))
	b.Columns[1].SetValue(0, int64(99))
	b.Columns[2].SetValue(0, int64(2))

	out := br.apply(b)
	if len(out.Columns) != 2 {
		t.Fatalf("output has %d columns, want 2", len(out.Columns))
	}
	if got := out.Columns[0].GetValue(0); got != int64(1) {
		t.Errorf("first output = %v, want 1", got)
	}
	if got := out.Columns[1].GetValue(0); got != int64(2) {
		t.Errorf("second output = %v, want 2 — the second `u` column, not the first", got)
	}
}
