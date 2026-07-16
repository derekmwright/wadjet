package worker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// multiTypeSchema exercises every WSHF-supported column class: fixed
// widths, packed bools, variable bytes, and decimal (scale/precision ride
// the schema).
var multiTypeSchema = []parquet.Column{
	{Name: "i64", Type: parquet.TypeInt64},
	{Name: "i32", Type: parquet.TypeInt32},
	{Name: "f64", Type: parquet.TypeFloat64},
	{Name: "f32", Type: parquet.TypeFloat32},
	{Name: "b", Type: parquet.TypeBool},
	{Name: "s", Type: parquet.TypeString},
	{Name: "dec", Type: parquet.TypeDecimal, Scale: 2, Precision: 15},
}

// buildMultiTypeWSHF writes nChunks chunks of rowsPerChunk pseudo-random
// rows through the production shuffleWriter and patches the header count,
// mirroring what Finalize does.
func buildMultiTypeWSHF(tb testing.TB, seed int64, nChunks, rowsPerChunk int) []byte {
	tb.Helper()
	rng := rand.New(rand.NewSource(seed))
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, multiTypeSchema)
	if err := sw.writeHeader(); err != nil {
		tb.Fatalf("writeHeader: %v", err)
	}
	for c := 0; c < nChunks; c++ {
		b := batch.NewRecordBatch(multiTypeSchema, rowsPerChunk)
		for i := 0; i < rowsPerChunk; i++ {
			if rng.Intn(8) == 0 {
				// Row null in every column (bitmap bit stays 0).
				continue
			}
			b.Columns[0].Nulls.SetValid(i)
			b.Columns[0].Int64Data[i] = rng.Int63()
			b.Columns[1].Nulls.SetValid(i)
			b.Columns[1].Int32Data[i] = int32(rng.Int31())
			b.Columns[2].Nulls.SetValid(i)
			b.Columns[2].Float64Data[i] = rng.Float64()
			b.Columns[3].Nulls.SetValid(i)
			b.Columns[3].Float32Data[i] = rng.Float32()
			b.Columns[4].Nulls.SetValid(i)
			b.Columns[4].BoolData[i] = rng.Intn(2) == 1
			b.Columns[5].Nulls.SetValid(i)
			s := fmt.Sprintf("row-%d-%x", i, rng.Int63())
			if rng.Intn(10) == 0 {
				s = "" // zero-length value on a valid row
			}
			b.Columns[5].BytesData.Set(i, []byte(s))
			b.Columns[6].Nulls.SetValid(i)
			b.Columns[6].DecimalData.Data[i] = batch.Int128{Lo: rng.Uint64(), Hi: rng.Int63() - rng.Int63()}
		}
		if err := sw.writeChunk(b.Columns, nil, rowsPerChunk); err != nil {
			tb.Fatalf("writeChunk %d: %v", c, err)
		}
	}
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[4:], sw.numChunks)
	return data
}

type nopReadCloser struct{ io.Reader }

func (nopReadCloser) Close() error { return nil }

// openStreaming positions rc after the outer magic the way the tiered
// open path does, and constructs the streaming reader.
func openStreaming(tb testing.TB, wire []byte) *streamingShuffleReader {
	tb.Helper()
	wshc := bytes.HasPrefix(wire, compressedMagic[:])
	r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(wire[4:])}, wshc)
	if err != nil {
		tb.Fatalf("newStreamingShuffleReader: %v", err)
	}
	return r
}

func drain(tb testing.TB, next func() (*batch.RecordBatch, error)) []*batch.RecordBatch {
	tb.Helper()
	var out []*batch.RecordBatch
	for {
		b, err := next()
		if err != nil {
			tb.Fatalf("Next: %v", err)
		}
		if b == nil {
			return out
		}
		out = append(out, b)
	}
}

// requireBatchesEqual compares two decoded batch sequences column-by-column.
func requireBatchesEqual(tb testing.TB, want, got []*batch.RecordBatch) {
	tb.Helper()
	if len(want) != len(got) {
		tb.Fatalf("batch count mismatch: %d vs %d", len(want), len(got))
	}
	for bi := range want {
		w, g := want[bi], got[bi]
		if w.Len != g.Len {
			tb.Fatalf("batch %d row count: %d vs %d", bi, w.Len, g.Len)
		}
		for ci, col := range multiTypeSchema {
			wc, gc := w.Columns[ci], g.Columns[ci]
			for i := 0; i < w.Len; i++ {
				if wc.Nulls.IsNull(i) != gc.Nulls.IsNull(i) {
					tb.Fatalf("batch %d col %s row %d null mismatch", bi, col.Name, i)
				}
				if wc.Nulls.IsNull(i) {
					continue
				}
				var same bool
				switch col.Type {
				case parquet.TypeInt64:
					same = wc.Int64Data[i] == gc.Int64Data[i]
				case parquet.TypeInt32:
					same = wc.Int32Data[i] == gc.Int32Data[i]
				case parquet.TypeFloat64:
					same = wc.Float64Data[i] == gc.Float64Data[i]
				case parquet.TypeFloat32:
					same = wc.Float32Data[i] == gc.Float32Data[i]
				case parquet.TypeBool:
					same = wc.BoolData[i] == gc.BoolData[i]
				case parquet.TypeString:
					same = bytes.Equal(wc.BytesData.Value(i), gc.BytesData.Value(i))
				case parquet.TypeDecimal:
					same = wc.DecimalData.Data[i] == gc.DecimalData.Data[i]
				}
				if !same {
					tb.Fatalf("batch %d col %s row %d value mismatch", bi, col.Name, i)
				}
			}
		}
	}
}

func TestStreamingShuffleReader_MatchesMmapReader(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 42, 5, 333)

	mm, err := newShuffleChunkReader(wire)
	if err != nil {
		t.Fatalf("newShuffleChunkReader: %v", err)
	}
	want := drain(t, mm.Next)

	sr := openStreaming(t, wire)
	defer sr.Close()
	got := drain(t, sr.Next)

	requireBatchesEqual(t, want, got)
	if sr.Delivered() != len(got) {
		t.Fatalf("Delivered() = %d, want %d", sr.Delivered(), len(got))
	}
	// Schema fidelity, including decimal scale/precision (issue #144 class).
	if sr.schema[6].Scale != 2 || sr.schema[6].Precision != 15 {
		t.Fatalf("decimal schema lost: scale=%d precision=%d", sr.schema[6].Scale, sr.schema[6].Precision)
	}
}

func TestStreamingShuffleReader_WSHC(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 7, 3, 500)
	compressed := CompressShuffleData(wire)
	if !bytes.HasPrefix(compressed, compressedMagic[:]) {
		t.Skip("payload did not compress; WSHC path not exercised")
	}

	mm, err := newShuffleChunkReader(wire)
	if err != nil {
		t.Fatalf("newShuffleChunkReader: %v", err)
	}
	want := drain(t, mm.Next)

	sr := openStreaming(t, compressed)
	defer sr.Close()
	got := drain(t, sr.Next)
	requireBatchesEqual(t, want, got)
}

func TestStreamingShuffleReader_TruncationIsAnError(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 9, 4, 200)
	// Cut points: mid-header, mid-first-chunk, mid-last-chunk.
	for _, cut := range []int{6, 40, len(wire) / 2, len(wire) - 3} {
		t.Run(fmt.Sprintf("cut=%d", cut), func(t *testing.T) {
			truncated := wire[:cut]
			r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(truncated[4:])}, false)
			if err != nil {
				return // header-stage truncation error is a pass
			}
			defer r.Close()
			for {
				b, err := r.Next()
				if err != nil {
					return // surfaced, not silently EOF'd
				}
				if b == nil {
					t.Fatal("truncated stream drained cleanly — rows dropped on the floor")
				}
			}
		})
	}
}

func TestStreamingShuffleReader_CountMismatchIsAnError(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 11, 2, 100)
	inflated := append([]byte(nil), wire...)
	binary.LittleEndian.PutUint32(inflated[4:], 3) // promise one more chunk than exists

	r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(inflated[4:])}, false)
	if err != nil {
		t.Fatalf("header parse: %v", err)
	}
	defer r.Close()
	seen := 0
	for {
		b, err := r.Next()
		if err != nil {
			if seen != 2 {
				t.Fatalf("errored after %d batches, want 2 clean batches first", seen)
			}
			return
		}
		if b == nil {
			t.Fatal("inflated chunk count drained cleanly")
		}
		seen++
	}
}

func TestStreamingShuffleReader_FixedWidthLengthMismatch(t *testing.T) {
	// Single int64 column, one chunk: header is 4(magic)+4(count)+2(cols)+
	// 2(nameLen)+1(name "v")+1(type) = 14 bytes; then numRows u32 @14,
	// bitmapWords u32 @18, bitmap words*8, dataLen u32 next.
	wire := makeWshfBytes(t, []int64{1, 2, 3})
	words := int(binary.LittleEndian.Uint32(wire[18:]))
	dataLenOff := 22 + words*8
	corrupt := append([]byte(nil), wire...)
	binary.LittleEndian.PutUint32(corrupt[dataLenOff:], binary.LittleEndian.Uint32(wire[dataLenOff:])+8)

	r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(corrupt[4:])}, false)
	if err != nil {
		t.Fatalf("header parse: %v", err)
	}
	defer r.Close()
	if _, err := r.Next(); err == nil {
		t.Fatal("corrupt fixed-width length decoded cleanly")
	}
}

func TestStreamingShuffleReader_EmptyFile(t *testing.T) {
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, multiTypeSchema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	r := openStreaming(t, buf.Bytes())
	defer r.Close()
	b, err := r.Next()
	if err != nil || b != nil {
		t.Fatalf("empty file: got (%v, %v), want (nil, nil)", b, err)
	}
	if r.Delivered() != 0 {
		t.Fatalf("Delivered() = %d on empty file", r.Delivered())
	}
}

func TestStreamingShuffleReader_CloseIdempotentAndBatchesSurvive(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 3, 1, 50)
	sr := openStreaming(t, wire)
	got := drain(t, sr.Next)
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// Batches are copies; touching them after Close must be safe.
	mm, _ := newShuffleChunkReader(wire)
	requireBatchesEqual(t, drain(t, mm.Next), got)
}

func FuzzStreamingShuffleReader(f *testing.F) {
	f.Add(buildMultiTypeWSHF(f, 1, 2, 64)[4:])
	f.Add(makeWshfBytesForFuzz()[4:])
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(data)}, false)
		if err != nil {
			return
		}
		defer r.Close()
		for i := 0; i < 1<<12; i++ {
			b, err := r.Next()
			if err != nil || b == nil {
				return
			}
		}
	})
}

// makeWshfBytesForFuzz avoids the *testing.T requirement of makeWshfBytes.
func makeWshfBytesForFuzz() []byte {
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64}}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	_ = sw.writeHeader()
	b := batch.NewRecordBatch(schema, 4)
	for i := 0; i < 4; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
		b.Columns[0].Nulls.SetValid(i)
	}
	_ = sw.writeChunk(b.Columns, nil, 4)
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[4:], sw.numChunks)
	return data
}
