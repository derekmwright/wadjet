package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #426, operator level. The end-to-end statement lives in
// wadjet.TestMinMaxOverContainers; this one pins the two seams a query
// through the embedded API does not always reach:
//
//   - the parallel-clone merge (CloneSink + mergeSinkState), which is also
//     the algebra the DISTRIBUTED partial→final split relies on: MIN of MINs;
//   - a group that sees only NULLs, which must finalize NULL rather than the
//     empty container the retained-value slot would otherwise leave behind.

func acmSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}
}

// acmBatch builds one batch from (group, array) pairs; a nil array is a NULL
// container. Batches are built directly rather than through FromRows so the
// nested column's shape comes from the schema.
func acmBatch(tb testing.TB, rows []struct {
	g int32
	a any
}) *batch.RecordBatch {
	tb.Helper()
	b := batch.NewRecordBatch(acmSchema(), len(rows))
	for i, r := range rows {
		b.Columns[0].SetValue(i, r.g)
		b.Columns[1].SetValue(i, r.a)
	}
	return b
}

type acmRow = struct {
	g int32
	a any
}

func TestContainerMinMaxAcrossCloneMerge(t *testing.T) {
	ctx := context.Background()
	// Two clones, each seeing a disjoint slice of the same groups: the
	// winner of group 1 is in the second clone and the winner of group 2 in
	// the first, so a merge that kept the primary's value unconditionally —
	// or dropped the clone's — is wrong either way.
	left := acmBatch(t, []acmRow{
		{1, []any{"m"}},
		{2, []any{"a"}},
		{3, nil},
	})
	right := acmBatch(t, []acmRow{
		{1, []any{"a", "z"}},
		{2, []any{"z"}},
		{3, nil},
	})

	primary := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggMin, InputCol: "c_arr", OutputCol: "lo", OutputType: parquet.TypeArray},
		{Func: AggMax, InputCol: "c_arr", OutputCol: "hi", OutputType: parquet.TypeArray},
	})
	c1 := primary.CloneSink().(*HashAggregate)
	c2 := primary.CloneSink().(*HashAggregate)
	for _, pair := range []struct {
		agg *HashAggregate
		in  *batch.RecordBatch
	}{{c1, left}, {c2, right}} {
		if err := pair.agg.Init(ctx); err != nil {
			t.Fatalf("clone init: %v", err)
		}
		if err := pair.agg.Consume(ctx, pair.in); err != nil {
			t.Fatalf("clone consume: %v", err)
		}
		if err := pair.agg.Finalize(ctx); err != nil {
			t.Fatalf("clone finalize: %v", err)
		}
	}
	if err := primary.Init(ctx); err != nil {
		t.Fatalf("primary init: %v", err)
	}
	primary.mergeSinkState(c1)
	primary.mergeSinkState(c2)
	if err := primary.Finalize(ctx); err != nil {
		t.Fatalf("primary finalize: %v", err)
	}

	got := map[int32][2]any{}
	for {
		out, err := primary.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if out == nil {
			break
		}
		for i := 0; i < out.Len; i++ {
			g := out.Columns[0].GetValue(i).(int32)
			got[g] = [2]any{out.Columns[1].GetValue(i), out.Columns[2].GetValue(i)}
		}
	}
	want := map[int32][2]any{
		1: {[]any{"a", "z"}, []any{"m"}},
		2: {[]any{"a"}, []any{"z"}},
		// Every input NULL: MIN/MAX ignore them and the group answers NULL.
		3: {nil, nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged MIN/MAX:\n got %#v\nwant %#v", got, want)
	}
}

// TestContainerMinMaxRetainsAcrossBatchReuse is the retention property. The
// state keeps a value out of an input batch for the whole aggregation, and
// the input's arenas are rewritten by whoever reuses the backing next — the
// same hazard GetValue's BYTES arm exists for. Consuming a SECOND batch that
// overwrites the first's storage in place must not change the answer.
func TestContainerMinMaxRetainsAcrossBatchReuse(t *testing.T) {
	ctx := context.Background()
	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggMin, InputCol: "c_arr", OutputCol: "lo", OutputType: parquet.TypeArray},
	})
	if err := agg.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	b := acmBatch(t, []acmRow{{1, []any{"aaa"}}, {1, []any{"zzz"}}})
	if err := agg.Consume(ctx, b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// Scribble the very storage the state would be pointing at if it had
	// aliased instead of copied: the ARRAY's element strings live in the
	// CHILD's byte arena, and the offsets that index it in the parent.
	child := b.Columns[1].Child
	for i := range child.BytesData.Data {
		child.BytesData.Data[i] = 'X'
	}
	for i := range b.Columns[1].Offsets {
		b.Columns[1].Offsets[i] = 0
	}
	b2 := acmBatch(t, []acmRow{{1, []any{"mmm"}}})
	if err := agg.Consume(ctx, b2); err != nil {
		t.Fatalf("consume 2: %v", err)
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	out, err := agg.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if out == nil || out.Len != 1 {
		t.Fatalf("got %v rows", out)
	}
	if got, want := out.Columns[0].GetValue(0), []any{"aaa"}; !reflect.DeepEqual(got, want) {
		t.Errorf("MIN after the input batch was overwritten: got %#v, want %#v", got, want)
	}
}
