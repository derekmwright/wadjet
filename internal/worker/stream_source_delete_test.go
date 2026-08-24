package worker

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #491's hard part is not "skip a row", it is "skip the RIGHT row".
//
// A merge-on-read delete marker names a FILE-ABSOLUTE index, and the DAG's
// scan source hands batches out one row group at a time, may skip row groups
// (dynamic filters), and may be reading a SHARD that starts at a group other
// than group 0. Applying a marker at the batch's local index would delete the
// wrong rows on every one of those, and the corpus-level tests cannot see it:
// their fixtures are one row group per file.
//
// These read a file whose row groups are small enough to make the offset
// arithmetic load-bearing, and assert the surviving VALUES rather than a row
// count — a count is satisfied by deleting any k rows.

var dmSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "v", Type: parquet.TypeInt64},
}}

// dmArraySchema forces the row-based fallback: RowGroupIter refuses
// Array/Map, so the source decodes the whole file into fallbackBatches and
// the offset has to come from a running count instead of the iterator.
var dmArraySchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "v", Type: parquet.TypeInt64},
	{Name: "tags", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
}}

// writeDeleteTestFile writes rows whose v is the row's own file-absolute
// index, so a surviving value IS the row identity the marker names.
func writeDeleteTestFile(t *testing.T, store objstore.Store, bucket, path string, rows, rowGroup int, schema parquet.Schema) {
	t.Helper()
	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.RowGroupSize = rowGroup
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]map[string]any, rows)
	for i := range data {
		data[i] = map[string]any{"v": int64(i)}
		if len(schema.Columns) > 1 {
			data[i]["tags"] = []any{fmt.Sprintf("t%d", i)}
		}
	}
	if err := w.WriteRows(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
		t.Fatal(err)
	}
}

// dmDrain returns every surviving v, sorted.
func dmDrain(t *testing.T, src *cachedFileStreamSource) []int64 {
	t.Helper()
	out, _ := dmDrainPath(t, src)
	return out
}

// dmDrainPath also reports which decode path the source took, so an arm
// written to exercise the row-reader fallback cannot silently drift onto the
// columnar iterator (or the reverse) and keep passing.
func dmDrainPath(t *testing.T, src *cachedFileStreamSource) ([]int64, string) {
	t.Helper()
	ctx := context.Background()
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var out []int64
	path := "none"
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		switch {
		case src.fallbackBatches != nil:
			path = "row-reader"
		case src.parquetIter != nil:
			if _, ok := src.parquetIter.(*decodeAheadStatsIter); ok {
				path = "decode-ahead"
			} else {
				path = "row-group-iter"
			}
		}
		if b == nil {
			break
		}
		col := b.Columns[0]
		if b.Sel == nil {
			for i := 0; i < b.Len; i++ {
				out = append(out, col.Int64Data[i])
			}
			continue
		}
		if len(b.Sel) == 0 {
			t.Fatal("a batch with an empty selection reached the consumer; it should have been dropped")
		}
		for _, i := range b.Sel {
			out = append(out, col.Int64Data[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, path
}

func dmWant(t *testing.T, got []int64, want []int64, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %v (%d rows), want %v (%d rows)", what, got, len(got), want, len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: %v, want %v", what, got, want)
		}
	}
}

// The offsets have to be file-absolute across row groups. Deleting one row
// out of each row group is the discriminator: applied at the batch-local
// index instead, every deleted index would land in group 0.
func TestStreamSourceDeleteMarkersAreFileAbsoluteAcrossRowGroups(t *testing.T) {
	ex, store := newConsumer(t, "b")
	const rows, rg = 40, 8 // 5 row groups
	writeDeleteTestFile(t, store, "b", "tables/t/chunk_0.parquet", rows, rg, dmSchema)
	files := []string{"tables/t/chunk_0.parquet"}

	// One row from each row group, including the last.
	deleted := []int64{0, 9, 17, 26, 39}
	var want []int64
	for i := int64(0); i < rows; i++ {
		if i != 0 && i != 9 && i != 17 && i != 26 && i != 39 {
			want = append(want, i)
		}
	}

	// Both parquet iterators: the serial RowGroupIter and the parallel
	// DecodeAheadIter, which delivers in source order but decodes out of it
	// and therefore carries its own per-group offset.
	for _, decodeAhead := range []bool{false, true} {
		ex.SetScanDecodeAhead(decodeAhead, 0)
		src := newCachedFileStreamSource(ex, "", "b", files)
		src.SetDeleteMarkers(map[string]*scan.DeleteSet{files[0]: scan.NewDeleteSet(deleted)})
		got, path := dmDrainPath(t, src)
		wantPath := "row-group-iter"
		if decodeAhead {
			wantPath = "decode-ahead"
		}
		if path != wantPath {
			t.Fatalf("decode_ahead=%v took the %s path, want %s", decodeAhead, path, wantPath)
		}
		dmWant(t, got, want, fmt.Sprintf("one delete per row group (%s)", path))
	}
	ex.SetScanDecodeAhead(false, 0)
}

// A SHARDED read covers row groups [k, m) of the file — its first batch does
// NOT start at file row 0. This is the case the single-process scanner solves
// with rgUnit.rgRowOffset and the one a naive per-source running count gets
// wrong.
func TestStreamSourceDeleteMarkersSurviveRowGroupSharding(t *testing.T) {
	ex, store := newConsumer(t, "b")
	const rows, rg, shards = 40, 8, 5
	writeDeleteTestFile(t, store, "b", "tables/t/chunk_0.parquet", rows, rg, dmSchema)
	files := []string{"tables/t/chunk_0.parquet"}
	deleted := []int64{0, 9, 17, 26, 39}
	del := map[string]*scan.DeleteSet{files[0]: scan.NewDeleteSet(deleted)}

	var all []int64
	for shard := 0; shard < shards; shard++ {
		src := newCachedFileStreamSource(ex, "", "b", files)
		src.SetShard(shard, shards)
		src.SetDeleteMarkers(del)
		all = append(all, dmDrain(t, src)...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	var want []int64
	for i := int64(0); i < rows; i++ {
		if !scan.NewDeleteSet(deleted).Contains(i) {
			want = append(want, i)
		}
	}
	dmWant(t, all, want, "the union of every shard")
}

// Markers are keyed by FILE, so a multi-file source must apply each file's
// own set — and none to the files that have none. Getting this wrong by
// applying one file's set to all of them is invisible to a row count when
// the files are the same length.
func TestStreamSourceDeleteMarkersArePerFile(t *testing.T) {
	ex, store := newConsumer(t, "b")
	var files []string
	for f := 0; f < 3; f++ {
		path := fmt.Sprintf("tables/t/chunk_%d.parquet", f)
		writeDeleteTestFile(t, store, "b", path, 10, 4, dmSchema)
		files = append(files, path)
	}
	// Only the middle file has markers, and they name rows 1 and 2.
	src := newCachedFileStreamSource(ex, "", "b", files)
	src.SetDeleteMarkers(map[string]*scan.DeleteSet{
		files[1]: scan.NewDeleteSet([]int64{1, 2}),
	})

	got := dmDrain(t, src)
	// Each file holds 0..9, so the union is three copies of each value
	// except 1 and 2, which survive twice.
	counts := map[int64]int{}
	for _, v := range got {
		counts[v]++
	}
	for v := int64(0); v < 10; v++ {
		want := 3
		if v == 1 || v == 2 {
			want = 2
		}
		if counts[v] != want {
			t.Fatalf("value %d survives %d times, want %d (markers applied to the wrong file)", v, counts[v], want)
		}
	}
}

// The Array/Map schema takes the row-reader fallback, which decodes the file
// in one shot rather than through the row-group iterator. It gets its offset
// from a running count, and that count must advance over a batch whose rows
// were all deleted.
func TestStreamSourceDeleteMarkersOnTheRowReaderFallback(t *testing.T) {
	ex, store := newConsumer(t, "b")
	writeDeleteTestFile(t, store, "b", "tables/t/chunk_0.parquet", 12, 4, dmArraySchema)
	files := []string{"tables/t/chunk_0.parquet"}

	src := newCachedFileStreamSource(ex, "", "b", files)
	src.SetDeleteMarkers(map[string]*scan.DeleteSet{files[0]: scan.NewDeleteSet([]int64{0, 5, 11})})
	got, path := dmDrainPath(t, src)
	if path != "row-reader" {
		t.Fatalf("the Array schema took the %s path; this arm no longer covers the fallback", path)
	}
	dmWant(t, got, []int64{1, 2, 3, 4, 6, 7, 8, 9, 10}, "row-reader fallback")
}

// The projection-intersection path (#448 F5) narrows the read to the columns
// present in the file while the table also carries a container column. That
// changes the decode path, so it gets its own arm — the markers must survive
// the narrowing.
func TestStreamSourceDeleteMarkersUnderProjectionIntersection(t *testing.T) {
	ex, store := newConsumer(t, "b")
	writeDeleteTestFile(t, store, "b", "tables/t/chunk_0.parquet", 16, 4, dmArraySchema)
	files := []string{"tables/t/chunk_0.parquet"}

	// "v" exists, "__derived" does not — the intersection keeps "v" and
	// routes the read through the columnar iterator instead of the
	// container fallback.
	src := newCachedFileStreamSourceWithProjection(ex, "", "b", files, []string{"v", "__derived"})
	src.SetDeleteMarkers(map[string]*scan.DeleteSet{files[0]: scan.NewDeleteSet([]int64{3, 4, 15})})
	got, path := dmDrainPath(t, src)
	if path != "row-group-iter" {
		t.Fatalf("the intersection took the %s path; #448 F5 narrows to the columnar reader", path)
	}
	dmWant(t, got, []int64{0, 1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, "projection intersection")
}

// Every row of a row group deleted: the batch must be DROPPED, not shipped
// with an empty selection (dmDrain fails on one), and the scan must continue
// into the next group rather than stopping.
func TestStreamSourceDropsFullyDeletedRowGroups(t *testing.T) {
	ex, store := newConsumer(t, "b")
	const rows, rg = 16, 4
	writeDeleteTestFile(t, store, "b", "tables/t/chunk_0.parquet", rows, rg, dmSchema)
	files := []string{"tables/t/chunk_0.parquet"}

	// Wipe the middle two row groups entirely.
	var deleted []int64
	for i := int64(4); i < 12; i++ {
		deleted = append(deleted, i)
	}
	src := newCachedFileStreamSource(ex, "", "b", files)
	src.SetDeleteMarkers(map[string]*scan.DeleteSet{files[0]: scan.NewDeleteSet(deleted)})
	dmWant(t, dmDrain(t, src), []int64{0, 1, 2, 3, 12, 13, 14, 15}, "two row groups fully deleted")

	// And the whole file.
	all := make([]int64, rows)
	for i := range all {
		all[i] = int64(i)
	}
	src = newCachedFileStreamSource(ex, "", "b", files)
	src.SetDeleteMarkers(map[string]*scan.DeleteSet{files[0]: scan.NewDeleteSet(all)})
	if got := dmDrain(t, src); len(got) != 0 {
		t.Fatalf("a fully-deleted file yielded %v", got)
	}
}

// A malformed marker payload must fail the task, not decode to an empty set:
// answering with the deleted rows still in it is exactly what #491 is about.
func TestTaskDeleteSetsRejectsAMalformedPayload(t *testing.T) {
	_, err := taskDeleteSets(distributed.Task{
		ID: "t1",
		DeleteMarkers: []distributed.DeleteSpec{
			{File: "tables/t/chunk_0.parquet", Runs: []byte{0xff}},
		},
	})
	if err == nil {
		t.Fatal("a truncated delete-marker payload must fail the task")
	}
}

// The common case must cost nothing: no markers on the wire, no map.
func TestTaskDeleteSetsIsNilWithoutMarkers(t *testing.T) {
	got, err := taskDeleteSets(distributed.Task{ID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil for a task with no markers, got %v", got)
	}
}
