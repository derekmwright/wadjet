package worker

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestShuffleFormatRoundTrip(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "price", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "count", Type: parquet.TypeInt32, Nullable: true},
	}

	// Create a batch with mixed data including nulls
	b := batch.NewRecordBatch(schema, 5)
	b.Columns[0].Int64Data[0] = 100
	b.Columns[0].Int64Data[1] = 200
	b.Columns[0].Int64Data[2] = 300
	b.Columns[0].Int64Data[3] = 400
	b.Columns[0].Int64Data[4] = 500

	b.Columns[1].BytesData.Set(0, []byte("alice"))
	b.Columns[1].BytesData.Set(1, []byte("bob"))
	b.Columns[1].BytesData.Set(2, []byte(""))
	b.Columns[1].Nulls.SetNull(2) // null string
	b.Columns[1].BytesData.Set(3, []byte("dave"))
	b.Columns[1].BytesData.Set(4, []byte("eve"))

	b.Columns[2].Float64Data[0] = 1.5
	b.Columns[2].Float64Data[1] = 2.5
	b.Columns[2].Float64Data[2] = 3.5
	b.Columns[2].Float64Data[3] = 4.5
	b.Columns[2].Nulls.SetNull(3)
	b.Columns[2].Float64Data[4] = 5.5

	b.Columns[3].Int32Data[0] = 10
	b.Columns[3].Int32Data[1] = 20
	b.Columns[3].Int32Data[2] = 30
	b.Columns[3].Int32Data[3] = 40
	b.Columns[3].Int32Data[4] = 50

	// Write with selection vector (rows 0, 2, 4)
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}

	sel := []uint32{0, 2, 4}
	if err := sw.writeChunk(b.Columns, sel, len(sel)); err != nil {
		t.Fatal(err)
	}

	// Write another chunk with all rows
	if err := sw.writeChunk(b.Columns, nil, 5); err != nil {
		t.Fatal(err)
	}

	// Patch chunk count
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)

	// Read back
	batches, err := shuffleReadBatches(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	// Check first batch (selected rows 0, 2, 4)
	b1 := batches[0]
	if b1.Len != 3 {
		t.Fatalf("batch 1: expected 3 rows, got %d", b1.Len)
	}
	if b1.Columns[0].Int64Data[0] != 100 {
		t.Errorf("batch 1 row 0 id: got %d, want 100", b1.Columns[0].Int64Data[0])
	}
	if b1.Columns[0].Int64Data[1] != 300 {
		t.Errorf("batch 1 row 1 id: got %d, want 300", b1.Columns[0].Int64Data[1])
	}
	if b1.Columns[0].Int64Data[2] != 500 {
		t.Errorf("batch 1 row 2 id: got %d, want 500", b1.Columns[0].Int64Data[2])
	}

	// Check string column round-trip
	if string(b1.Columns[1].BytesData.Value(0)) != "alice" {
		t.Errorf("batch 1 row 0 name: got %q, want alice", b1.Columns[1].BytesData.Value(0))
	}
	// Row 2 was null
	if !b1.Columns[1].Nulls.IsNull(1) {
		t.Error("batch 1 row 1 name should be null")
	}
	if string(b1.Columns[1].BytesData.Value(2)) != "eve" {
		t.Errorf("batch 1 row 2 name: got %q, want eve", b1.Columns[1].BytesData.Value(2))
	}

	// Check float64 column
	if b1.Columns[2].Float64Data[0] != 1.5 {
		t.Errorf("batch 1 row 0 price: got %f, want 1.5", b1.Columns[2].Float64Data[0])
	}

	// Check second batch (all 5 rows)
	b2 := batches[1]
	if b2.Len != 5 {
		t.Fatalf("batch 2: expected 5 rows, got %d", b2.Len)
	}
	if b2.Columns[0].Int64Data[3] != 400 {
		t.Errorf("batch 2 row 3 id: got %d, want 400", b2.Columns[0].Int64Data[3])
	}
	// Row 3 price should be null
	if !b2.Columns[2].Nulls.IsNull(3) {
		t.Error("batch 2 row 3 price should be null")
	}
}

// TestShuffleChunkReaderTruncatedFile is a regression test for the SF100 build
// cache silent-row-loss bug. The reader used to return (nil, nil) — a clean
// EOF — when the header promised more chunks than the file actually contained,
// causing downstream queries to silently complete with missing rows. The fix
// surfaces a real error so the worker fails the task instead.
func TestShuffleChunkReaderTruncatedFile(t *testing.T) {
	// Build a tiny one-chunk shuffle file, then lie in the header about how
	// many chunks it contains.
	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64, Nullable: true}}

	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].Int64Data[0] = 1
	b.Columns[0].Int64Data[1] = 2

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := sw.writeChunk(b.Columns, nil, 2); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	// Patch the chunk count header to claim 5 chunks even though only 1 was
	// actually written. This is exactly the shape a streaming write would take
	// if a multi-chunk write was interrupted partway through.
	data[4] = 5

	// shuffleReadBatches must error rather than silently returning the partial
	// data.
	if _, err := shuffleReadBatches(data); err == nil {
		t.Error("shuffleReadBatches: want error on truncated file, got nil")
	}

	// shuffleChunkReader.Next must error after consuming the one valid chunk
	// rather than silently returning EOF.
	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.Next()
	if err != nil || first == nil {
		t.Fatalf("first chunk: err=%v batch=%v", err, first)
	}
	if _, err := r.Next(); err == nil {
		t.Error("shuffleChunkReader.Next: want truncation error after first chunk, got nil")
	}
}

func TestShuffleFormatDetection(t *testing.T) {
	// Binary shuffle format
	shuffleData := []byte{'W', 'S', 'H', 'F', 0, 0, 0, 0}
	if !isShuffleFormat(shuffleData) {
		t.Error("should detect shuffle format")
	}

	// Parquet format (starts with PAR1)
	parquetData := []byte{'P', 'A', 'R', '1', 0, 0, 0, 0}
	if isShuffleFormat(parquetData) {
		t.Error("should not detect parquet as shuffle format")
	}

	// Too short
	if isShuffleFormat([]byte{1, 2}) {
		t.Error("should not detect short data as shuffle format")
	}
}

func TestShuffleCompressedRoundTrip(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: false},
	}

	// Create enough data that S2 compression actually kicks in (needs >10% savings).
	const rowCount = 2048
	rb := batch.NewRecordBatch(schema, rowCount)
	for i := 0; i < rowCount; i++ {
		rb.Columns[0].Int64Data[i] = int64(i)
		rb.Columns[1].BytesData.Set(i, []byte("repeated-value-for-compression"))
	}

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := sw.writeChunk(rb.Columns, nil, rowCount); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	raw[4] = byte(sw.numChunks)

	// Compress to WSHC format
	compressed := CompressShuffleData(raw)
	if len(compressed) < 4 {
		t.Fatal("compressed data too short")
	}
	if string(compressed[:4]) != "WSHC" {
		t.Fatalf("expected WSHC magic, got %q (data may be too small to compress)", compressed[:4])
	}

	// Verify WSHC is detected as shuffle format
	if !isShuffleFormat(compressed) {
		t.Error("compressed shuffle data should be detected as shuffle format")
	}

	// Decompress and read — must succeed
	decompressed, err := DecompressShuffleData(compressed)
	if err != nil {
		t.Fatalf("DecompressShuffleData: %v", err)
	}
	batches, err := shuffleReadBatches(decompressed)
	if err != nil {
		t.Fatalf("shuffleReadBatches after decompress: %v", err)
	}
	if len(batches) != 1 || batches[0].Len != rowCount {
		t.Fatalf("expected 1 batch with %d rows, got %d batches", rowCount, len(batches))
	}
	if batches[0].Columns[0].Int64Data[0] != 0 {
		t.Errorf("got id=%d, want 0", batches[0].Columns[0].Int64Data[0])
	}
	if batches[0].Columns[0].Int64Data[rowCount-1] != int64(rowCount-1) {
		t.Errorf("got last id=%d, want %d", batches[0].Columns[0].Int64Data[rowCount-1], rowCount-1)
	}

	// Reading compressed data WITHOUT decompressing must fail
	// (this was the bug in executeMergeSorted)
	_, err = shuffleReadBatches(compressed)
	if err == nil {
		t.Error("reading WSHC data without decompressing should fail")
	}
}

func BenchmarkShuffleWriteVsParquet(b *testing.B) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64, Nullable: false},
		{Name: "value", Type: parquet.TypeFloat64, Nullable: false},
		{Name: "label", Type: parquet.TypeString, Nullable: false},
	}

	// Create a batch with 2048 rows
	rb := batch.NewRecordBatch(schema, 2048)
	for i := 0; i < 2048; i++ {
		rb.Columns[0].Int64Data[i] = int64(i)
		rb.Columns[1].Float64Data[i] = float64(i) * 1.5
		rb.Columns[2].BytesData.Set(i, []byte("test-value-string"))
	}

	b.Run("binary_shuffle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			sw := newShuffleWriter(&buf, schema)
			sw.writeHeader()
			sw.writeChunk(rb.Columns, nil, 2048)
		}
	})

}

// Regression test for the WSHF decimal schema header (issue #144 suite
// finding): the header carried only (name, type), so the receiving side
// rebuilt decimal columns at scale 0 and every value rendered as its raw
// scaled integer — distributed GROUP BY decimal keys came back as "25"
// instead of "0.25". Scale+precision now ride the header for decimal
// columns only.
func TestShuffleFormatDecimalScaleRoundTrip(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeDecimal, Nullable: true, Precision: 12, Scale: 2},
	}
	b := batch.NewRecordBatch(schema, 3)
	for i, v := range []int64{325, -150, 1234} { // 3.25, -1.50, 12.34 at scale 2
		b.Columns[0].Int64Data[i] = int64(i)
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].DecimalData.Data[i] = batch.Int128From(v)
		b.Columns[1].Nulls.SetValid(i)
	}
	b.Len = 3

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	sel := []uint32{0, 1, 2}
	if err := sw.writeChunk(b.Columns, sel, 3); err != nil {
		t.Fatal(err)
	}
	payload := buf.Bytes()
	payload[4] = 1 // patch chunk count like the gather sink does

	out, err := shuffleReadBatches(payload)
	if err != nil {
		t.Fatalf("shuffleReadBatches: %v", err)
	}
	if len(out) != 1 || out[0].Len != 3 {
		t.Fatalf("got %d batches", len(out))
	}
	if got := out[0].Schema[1].Scale; got != 2 {
		t.Fatalf("decoded schema scale = %d, want 2 (header lost it)", got)
	}
	want := []string{"3.25", "-1.5", "12.34"}
	for i, w := range want {
		if got := fmt.Sprintf("%v", out[0].Columns[1].GetValue(i)); got != w {
			t.Fatalf("row %d: %v, want %s", i, got, w)
		}
	}

	// The streaming chunk reader parses the same header.
	cr, err := newShuffleChunkReader(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := cr.schema[1].Scale; got != 2 {
		t.Fatalf("chunk reader schema scale = %d, want 2", got)
	}
}

// buildTestWSHF builds a small complete WSHF payload with enough
// redundancy for both codecs to clear the ≥10% savings heuristic.
func buildRoundTripWSHF(tb testing.TB) []byte {
	tb.Helper()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: false},
	}
	const rowCount = 2048
	rb := batch.NewRecordBatch(schema, rowCount)
	for i := 0; i < rowCount; i++ {
		rb.Columns[0].Int64Data[i] = int64(i)
		rb.Columns[1].BytesData.Set(i, []byte("repeated-value-for-compression"))
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := sw.writeChunk(rb.Columns, nil, rowCount); err != nil {
		tb.Fatal(err)
	}
	raw := buf.Bytes()
	raw[4] = byte(sw.numChunks)
	return raw
}

// withExchangeZstd flips the WSHZ upload envelope for the test body and
// restores the prior state afterward.
func withExchangeZstd(tb testing.TB, on bool) {
	tb.Helper()
	prev := exchangeZstd
	exchangeZstd = on
	tb.Cleanup(func() { exchangeZstd = prev })
}

// WSHZ (zstd) envelope round trip, mirroring TestShuffleCompressedRoundTrip
// (docs/design/exchange-zstd-wire.md).
func TestShuffleZstdRoundTrip(t *testing.T) {
	withExchangeZstd(t, true)
	raw := buildRoundTripWSHF(t)

	compressed := CompressShuffleData(raw)
	if len(compressed) < 4 {
		t.Fatal("compressed data too short")
	}
	if string(compressed[:4]) != "WSHZ" {
		t.Fatalf("expected WSHZ magic under WADJET_EXCHANGE_ZSTD, got %q", compressed[:4])
	}
	if !isShuffleFormat(compressed) {
		t.Error("WSHZ data should be detected as shuffle format")
	}
	if c, ok := codecForMagic([4]byte{'W', 'S', 'H', 'Z'}); !ok || c != codecZstd {
		t.Fatalf("codecForMagic(WSHZ) = (%v,%v), want (codecZstd,true)", c, ok)
	}

	decompressed, err := DecompressShuffleData(compressed)
	if err != nil {
		t.Fatalf("DecompressShuffleData: %v", err)
	}
	if !bytes.Equal(decompressed, raw) {
		t.Fatalf("WSHZ round trip not byte-identical: %d bytes vs %d raw", len(decompressed), len(raw))
	}
	batches, err := shuffleReadBatches(decompressed)
	if err != nil {
		t.Fatalf("shuffleReadBatches after WSHZ decompress: %v", err)
	}
	if len(batches) != 1 || batches[0].Len != 2048 {
		t.Fatalf("expected 1 batch with 2048 rows, got %d batches", len(batches))
	}
	if _, err := shuffleReadBatches(compressed); err == nil {
		t.Error("reading WSHZ data without decompressing should fail")
	}

	// Flag off must keep producing WSHC — and both envelopes must remain
	// decodable side by side (mixed objects within one query during a
	// flag flip).
	withExchangeZstd(t, false)
	s2c := CompressShuffleData(raw)
	if string(s2c[:4]) != "WSHC" {
		t.Fatalf("expected WSHC with flag off, got %q", s2c[:4])
	}
	for _, payload := range [][]byte{compressed, s2c} {
		out, err := DecompressShuffleData(payload)
		if err != nil {
			t.Fatalf("mixed-envelope decompress (%q): %v", payload[:4], err)
		}
		if !bytes.Equal(out, raw) {
			t.Fatalf("mixed-envelope round trip (%q) not byte-identical", payload[:4])
		}
	}
}

// File-streaming WSHZ path: compressShuffleStream writes WSHZ, and both
// streamDecompressShuffle and the streaming reader decode it.
func TestShuffleZstdFileStreamRoundTrip(t *testing.T) {
	withExchangeZstd(t, true)
	raw := buildRoundTripWSHF(t)

	srcPath := filepath.Join(t.TempDir(), "src.wshf")
	if err := os.WriteFile(srcPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	dstPath := filepath.Join(t.TempDir(), "dst.wshz")
	size, useCompressed, err := CompressShuffleFile(srcPath, dstPath)
	if err != nil {
		t.Fatalf("CompressShuffleFile: %v", err)
	}
	if !useCompressed {
		t.Fatal("expected compression to clear the 10% heuristic")
	}
	wire, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(wire)) != size {
		t.Fatalf("reported size %d != file size %d", size, len(wire))
	}
	if string(wire[:4]) != "WSHZ" {
		t.Fatalf("expected WSHZ file magic, got %q", wire[:4])
	}

	// Transcode path (openShuffleFile/streamShuffleToSpill): body after
	// magic → plain WSHF, byte-identical.
	var out bytes.Buffer
	if err := streamDecompressShuffle(bytes.NewReader(wire[4:]), &out, codecZstd); err != nil {
		t.Fatalf("streamDecompressShuffle(zstd): %v", err)
	}
	if !bytes.Equal(out.Bytes(), raw) {
		t.Fatal("streamed WSHZ transcode not byte-identical")
	}

	// Streaming-decode path (openShuffleStreaming): reader positioned
	// after the outer magic, decodes chunks straight off the zstd stream.
	r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(wire[4:])}, codecZstd)
	if err != nil {
		t.Fatalf("newStreamingShuffleReader(WSHZ): %v", err)
	}
	defer r.Close()
	rows := 0
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("streaming WSHZ Next: %v", err)
		}
		if b == nil {
			break
		}
		rows += b.Len
	}
	if rows != 2048 {
		t.Fatalf("streaming WSHZ decoded %d rows, want 2048", rows)
	}
}
