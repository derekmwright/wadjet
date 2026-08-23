package parquet

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The numbers a page header declares size allocations before anything looks
// at the page body: one int32 per value for the definition levels, one per
// value for the dictionary indices, one offset per byte-array value. The
// header bound is MaxPageValues; the exact bound, for a flat leaf, is the row
// group's own row count.

// TestMaxPageValuesMatchesWhatWritersProduce pins the constant against the
// measurement rather than against itself. parquet-cpp caps a data page at
// 20,000 values and parquet-mr at 20,000 rows whatever the byte target:
// pyarrow asked for 2 GiB pages over 40 million BOOLEAN rows still wrote
// pages of exactly 20,000, and the largest page in this repo's corpus is the
// same 20,000.
func TestMaxPageValuesMatchesWhatWritersProduce(t *testing.T) {
	const largestObserved = 20_000
	if MaxPageValues < largestObserved*100 {
		t.Errorf("MaxPageValues = %d leaves less than 100x headroom over the %d a real writer emits",
			MaxPageValues, largestObserved)
	}
	if MaxPageValues > 1<<25 {
		t.Errorf("MaxPageValues = %d admits a %d-byte level allocation from a page header alone",
			MaxPageValues, MaxPageValues*4)
	}
	// A page cannot hold more values than a row group holds rows, so the two
	// ceilings have to relate: a legal row group of MaxRowsPerRowGroup rows
	// whose leaf cannot be page-split (BOOLEAN, FIXED_LEN, nested) would
	// otherwise be a file this package writes and refuses. writeDataPage
	// refuses to produce such a page; this pins the reason.
	if MaxPageValues > MaxRowsPerRowGroup {
		t.Errorf("MaxPageValues %d is above MaxRowsPerRowGroup %d", MaxPageValues, int64(MaxRowsPerRowGroup))
	}
}

func TestCheckRLECountRefusesBeforeAllocating(t *testing.T) {
	if _, err := DecodeRLEInt32(nil, 1, -1); err == nil {
		t.Error("a negative RLE count was accepted")
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := DecodeRLEInt32([]byte{0x02, 0x00}, 1, MaxPageValues+1); err == nil {
		t.Error("an RLE count past MaxPageValues was accepted")
	}
	if _, _, err := DecodeRLEInt32WithLength([]byte{2, 0, 0, 0, 0x02, 0x00}, 1, MaxPageValues+1); err == nil {
		t.Error("an RLE count past MaxPageValues was accepted through the length-prefixed door")
	}
	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Errorf("refusing the counts allocated %d bytes", grew)
	}
	// The ceiling itself still decodes.
	if _, err := DecodeRLEInt32([]byte{0x02, 0x01}, 1, 1); err != nil {
		t.Errorf("an honest RLE page: %v", err)
	}
}

// TestDeltaHeaderDoesNotWalkPastTheBody: findDeltaDataEnd used to keep
// walking blocks after the data ran out — its varint reader returns 0 without
// advancing once it reaches the end — allocating a bit-width array per block
// against a value count the header claimed. A seven-byte body cost
// milliseconds, and that is what stalled FuzzDecodePageValues.
func TestDeltaHeaderDoesNotWalkPastTheBody(t *testing.T) {
	// blockSize 2^20, 2^20 miniblocks, ten million values, first value 0 —
	// every one of those inside the reasonableness gate, out of nine bytes.
	body := append(deltaTestHeader(1<<20, 1<<20, 10_000_000, 0), 0x00)
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"delta_length_byte_array", func() error { _, err := DecodeDeltaLengthByteArray(body, 8); return err }},
		{"delta_byte_array", func() error { _, err := DecodeDeltaByteArray(body, 8); return err }},
		{"delta_binary_packed", func() error { _, err := DecodeDeltaBinaryPackedInt64(body, 8); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			err := tc.run()
			elapsed := time.Since(start)
			runtime.ReadMemStats(&after)
			if err == nil {
				t.Error("a nine-byte page decoded ten million values")
			}
			if elapsed > 50*time.Millisecond {
				t.Errorf("refusing it took %v", elapsed)
			}
			if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<20 {
				t.Errorf("refusing it allocated %d bytes out of a %d-byte body", grew, len(body))
			}
		})
	}
}

// deltaTestHeader builds a DELTA_BINARY_PACKED header: block size, miniblock
// count, total values, first value (zigzag).
func deltaTestHeader(blockSize, miniblocks, totalValues, firstValue uint64) []byte {
	var out []byte
	put := func(v uint64) {
		var buf [binary.MaxVarintLen64]byte
		out = append(out, buf[:binary.PutUvarint(buf[:], v)]...)
	}
	put(blockSize)
	put(miniblocks)
	put(totalValues)
	put(firstValue << 1)
	return out
}

// TestPageValueCountIsHeldToTheRowGroup: for a flat leaf the row group's row
// count is exact — the chunk's pages sum to it — so a page header claiming
// more is refused before its levels are allocated.
func TestPageValueCountIsHeldToTheRowGroup(t *testing.T) {
	good := boundsFixture(t) // 64 rows, one INT64 column
	if err := readChunkPages(t, good); err != nil {
		t.Fatalf("the fixture does not read: %v", err)
	}
	// Halve the row group's declared rows: the chunk's one page then claims
	// more values than the row group has.
	raw := shrinkRowGroupRows(t, good, 32)
	err := readChunkPages(t, raw)
	if err == nil {
		t.Fatal("a page claiming more values than the row group has rows was accepted")
	}
	if !strings.Contains(err.Error(), "rows left") {
		t.Errorf("error %q does not name the row budget", err)
	}
}

// shrinkRowGroupRows rewrites the file's row counts, keeping the footer's
// total consistent with its row groups so the footer validator lets it
// through to the page reader.
func shrinkRowGroupRows(t *testing.T, raw []byte, rows int64) []byte {
	t.Helper()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	start := len(raw) - 8 - footerLen
	md, err := DecodeFileMetaData(raw[start : start+footerLen])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	md.NumRows = 0
	for i := range md.RowGroups {
		md.RowGroups[i].NumRows = rows
		md.NumRows += rows
	}
	footer := EncodeFileMetaData(md)
	out := make([]byte, 0, start+len(footer)+8)
	out = append(out, raw[:start]...)
	out = append(out, footer...)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(footer)))
	out = append(out, l[:]...)
	return append(out, "PAR1"...)
}

// TestWriterRefusesAPageItCannotReadBack: pageRowRanges splits by BYTES and
// declines to split BOOLEAN, INT96, FIXED_LEN and nested leaves at all, so a
// large enough RowGroupSize puts more values in one page than MaxPageValues
// allows — a file this package writes and then refuses. The writer says so
// instead, and names the knob.
func TestWriterRefusesAPageItCannotReadBack(t *testing.T) {
	// Reaching MaxPageValues honestly would mean writing 16.7M rows, so the
	// page range is exercised through pageRowRanges directly.
	lb := &leafBuffer{col: Column{Name: "b", Type: TypeBool}, physical: PhysicalBoolean}
	nw := &NativeWriter{config: DefaultWriterConfig()}
	_, _, err := nw.writeDataPage(lb, pageRange{0, MaxPageValues + 1, 0, MaxPageValues + 1}, true)
	if err == nil {
		t.Fatal("the writer produced a page past MaxPageValues")
	}
	for _, want := range []string{`column "b"`, "RowGroupSize"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// And an ordinary BOOLEAN file still round-trips, so the guard is on the
// ceiling and not on the shape.
func TestBooleanColumnStillWritesOnePageAndReads(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "b", Type: TypeBool}}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 5000)
	for i := range rows {
		rows[i] = map[string]any{"b": i%3 == 0}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	back, err := r.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(back), len(rows))
	}
	for i := range rows {
		if back[i]["b"] != rows[i]["b"] {
			t.Fatalf("row %d = %v, want %v", i, back[i]["b"], rows[i]["b"])
		}
	}
}
