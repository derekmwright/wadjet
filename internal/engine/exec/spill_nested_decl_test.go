package exec

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// f2NestedDeclSchema is the declaration under test: every carrier a
// `parquet.Column` can hold beyond name/type/nullable — DECIMAL's (p,s), a
// VECTOR's dimension, an ARRAY's ElementType, a ROW's Fields, a MAP's entry
// ROW (ElementType holding Fields), and a container nested in a container.
func f2NestedDeclSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "vec", Type: parquet.TypeVector, Dimension: 4, Nullable: true},
		{Name: "arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64, Nullable: true},
			{Name: "b", Type: parquet.TypeString, Nullable: true},
			{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		}},
		{Name: "mp", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
		// A container INSIDE a container: the recursion is the claim, and a
		// codec that wrote one level would pass without this column.
		{Name: "deep", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "inner", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
		}},
	}
}

func f2NestedDeclBatch(t testing.TB, start, n int) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch(f2NestedDeclSchema(), n)
	for i := 0; i < n; i++ {
		id := int64(start + i)
		b.Columns[0].Int64Data[i] = id
		b.Columns[1].SetValue(i, batch.Int128{Lo: uint64(1000 + id)})
		b.Columns[2].SetValue(i, []float32{float32(id), 1, 2, 3})
		b.Columns[3].SetValue(i, []any{fmt.Sprintf("e-%d", id)})
		b.Columns[4].SetValue(i, map[string]any{
			"a": id, "b": fmt.Sprintf("s-%d", id), "d": batch.Int128{Lo: uint64(id)},
		})
		// A MAP is ARRAY(ROW(key,value)) in the storage layer, so its rows are
		// written as a list of entry ROWs (a bare map is refused, #397).
		b.Columns[5].SetValue(i, []any{map[string]any{
			"key": fmt.Sprintf("k%d", id), "value": id,
		}})
		b.Columns[6].SetValue(i, map[string]any{"inner": []any{id, id + 1}})
	}
	b.Len = n
	return b
}

// A spill file's per-column header is a DECLARATION, and a declaration that
// drops the nested half is not one (#865).
//
// The file carried name, type, nullable and — for DECIMAL only — (p,s). The
// vector's own children rode in the data section, so the VALUES came back;
// what did not was the schema the reader hands to its caller. A container
// column came back declared `ROW` with no fields, `ARRAY` with no element,
// `VECTOR` with no dimension, and every consumer that allocates from the
// declaration (`batch.NewColumnVector`, and every operator above the join
// that does) then minted an empty vector for it.
func TestASpillFileCarriesTheWholeColumnDeclaration(t *testing.T) {
	original := f2NestedDeclBatch(t, 0, 3)
	// A NULL row in every container, so the null path is exercised beside the
	// value path rather than instead of it.
	for _, c := range original.Columns[1:] {
		c.WriteNullAt(2)
	}

	path, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{original})
	if err != nil {
		t.Fatalf("writeSpillBatches: %v", err)
	}
	got, err := readSpillBatches(path)
	if err != nil {
		t.Fatalf("readSpillBatches: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d batches, want 1", len(got))
	}
	want := f2NestedDeclSchema()
	if !reflect.DeepEqual(got[0].Schema, want) {
		for i := range want {
			if i < len(got[0].Schema) && !reflect.DeepEqual(got[0].Schema[i], want[i]) {
				t.Errorf("column %d (%s) came back declared\n  %+v\nwant\n  %+v",
					i, want[i].Name, got[0].Schema[i], want[i])
			}
		}
		t.Fatalf("the spill file's schema is not the schema that was written")
	}

	// The declaration is not decoration: a consumer that allocates from it
	// has to get a vector that can HOLD the column's values. This is the
	// shape #865 reached through — the join's output batch.
	for i, col := range got[0].Schema {
		dst := batch.NewColumnVector(col, got[0].Len)
		for row := 0; row < got[0].Len; row++ {
			dst.CopyValueFrom(row, got[0].Columns[i], row)
		}
		for row := 0; row < got[0].Len; row++ {
			gotV, wantV := dst.GetValue(row), original.Columns[i].GetValue(row)
			if fmt.Sprint(gotV) != fmt.Sprint(wantV) {
				t.Errorf("%s row %d through a vector allocated from the file's own "+
					"declaration: got %v, want %v", col.Name, row, gotV, wantV)
			}
		}
	}
}

// The end of the same claim, at the operator: a join whose build SPILLED emits
// output batches declaring what the unspilled one declares.
//
// #865's symptom was a matched row's ROW column reading back NULL under a
// memory budget, on both the preserved and the padded side of an OUTER join.
// The values were never lost at the operator — `Vector.CopyValueFrom` mints a
// container's children lazily — so a gate that reads the operator's rows
// passes while every consumer ABOVE it that allocates from the schema drops
// the column. This asserts the DECLARATION, per join type.
func TestASpilledJoinDeclaresItsContainerColumns(t *testing.T) {
	for _, jt := range []struct {
		name string
		typ  JoinType
	}{{"inner", InnerJoin}, {"left", LeftJoin}, {"right", RightJoin}, {"full", FullOuterJoin}} {
		t.Run(jt.name, func(t *testing.T) {
			var buildBatches []*batch.RecordBatch
			for off := 0; off < 5000; off += 1000 {
				buildBatches = append(buildBatches, f2NestedDeclBatch(t, off, 1000))
			}
			probeSchema := []parquet.Column{{Name: "pid", Type: parquet.TypeInt64}}
			pb := batch.NewRecordBatch(probeSchema, 200)
			for i := 0; i < 200; i++ {
				pb.Columns[0].Int64Data[i] = int64(i * 2)
			}
			pb.Len = 200

			tracker := memory.NewTracker("test", 250_000)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()

			hj := NewHashJoin(jt.typ, []string{"pid"}, []string{"id"})
			hj.Spill = sm
			hj.MemTracker = tracker
			if err := hj.Build(context.Background(), NewBatchSource(buildBatches)); err != nil {
				t.Fatalf("build: %v", err)
			}
			// ADR-0027 decision 5: a spill gate proves it spilled.
			if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
				t.Fatalf("the build did not spill, so this cell compares two in-memory answers")
			}

			sink := &declSink{t: t, want: f2NestedDeclSchema()}
			pipe := &Pipeline{
				Source: NewBatchSource([]*batch.RecordBatch{pb}),
				Ops:    []UnaryOperator{hj.Probe()},
				Sink:   sink,
			}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatalf("pipeline: %v", err)
			}
			if sink.batches == 0 {
				t.Fatalf("no output batches to check")
			}
		})
	}
}

// declSink asserts, per batch, that every build-side container column is
// declared the way the build declared it.
type declSink struct {
	t        *testing.T
	want     []parquet.Column
	batches  int
	reported map[string]bool
}

func (s *declSink) Init(context.Context) error { return nil }

func (s *declSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.batches++
	if s.reported == nil {
		s.reported = map[string]bool{}
	}
	byName := map[string]parquet.Column{}
	for _, c := range s.want {
		byName[c.Name] = c
	}
	for _, got := range b.Schema {
		want, ok := byName[got.Name]
		if !ok || got.Type != want.Type || s.reported[got.Name] {
			continue
		}
		// NULLABILITY is not part of the claim: an OUTER join widens the
		// padded side's columns to nullable, which is correct. Everything
		// else in the declaration has to survive the spill boundary.
		got.Nullable, want.Nullable = true, true
		if !reflect.DeepEqual(got, want) {
			s.reported[got.Name] = true
			s.t.Errorf("output column %q is declared\n  %+v\nthe build declares it\n  %+v",
				got.Name, got, want)
		}
	}
	return nil
}

func (s *declSink) Finalize(context.Context) error { return nil }
func (s *declSink) Close() error                   { return nil }
