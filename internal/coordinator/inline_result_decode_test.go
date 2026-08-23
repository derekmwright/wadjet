package coordinator

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// decodeInlineResult used to answer (nil, nil, 0) and log at Debug for every
// failure — a corrupt payload, a truncated shuffle blob, an unreadable
// parquet result. The caller cannot tell that apart from a worker that
// legitimately produced no rows, so one bad partial silently removed that
// worker's whole share of the answer and the query came back short with
// nothing said. readOneResultFile, reading the same bytes off S3 instead of
// inline, has always returned the error.
func TestDecodeInlineResultReportsFailures(t *testing.T) {
	var c Coordinator
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"s2 frame that is not one", append([]byte("WSHC"), 0xFF, 0xFF, 0xFF, 0xFF), "decompressing"},
		{"WSHF header with no body", []byte("WSHF"), "shuffle"},
		// The truncated BODY cases (#422). Each of these used to index
		// straight off the end of the slice inside the decode goroutine of
		// readInlineResults, which nothing above recovers: a short payload
		// from one worker took the whole coordinator down.
		{"header promises a chunk that is not there",
			append([]byte("WSHF"), 1, 0, 0, 0, 1, 0), "truncated"},
		{"schema name runs past the end",
			append([]byte("WSHF"), 1, 0, 0, 0, 1, 0, 0xFF, 0x00), "schema"},
		{"chunk row count with no column data",
			truncatedInlineChunk(), "chunk"},
		{"not parquet at all", []byte("this is not a parquet file, nor a shuffle blob"), "parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches, cols, rows, err := c.decodeInlineResult(tc.data)
			if err == nil {
				t.Fatalf("accepted: %d batches, %v, %d rows", len(batches), cols, rows)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if batches != nil || rows != 0 {
				t.Errorf("results came back alongside the error: %d batches, %d rows", len(batches), rows)
			}
		})
	}
}

// TestDecodeInlineResultDecodesWSHZ pins a fix that had no test: before
// #422 unified the read side into internal/wshf, the coordinator's inline
// decode only knew the WSHF and WSHC (s2) magics — a WSHZ-enveloped payload
// fell into the "not parquet at all" branch and failed with a parquet-open
// error, even though the bytes were a perfectly good shuffle result. WSHZ is
// what a worker's stage/shuffle upload carries under
// WADJET_EXCHANGE_ZSTD=1 (docs/design/exchange-zstd-wire.md); this test sets
// that flag only to document the real trigger — decodeInlineResult itself
// never reads it, it dispatches purely on the four-byte magic via
// wshf.Decompress.
func TestDecodeInlineResultDecodesWSHZ(t *testing.T) {
	t.Setenv("WADJET_EXCHANGE_ZSTD", "1")

	var c Coordinator
	data := wshzInlineFixture(t)

	batches, cols, rows, err := c.decodeInlineResult(data)
	if err != nil {
		t.Fatalf("decodeInlineResult(WSHZ): %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	if len(cols) != 1 || cols[0] != "x" {
		t.Fatalf("cols = %v, want [x]", cols)
	}
	if len(batches) != 1 || batches[0].Len != 1 {
		t.Fatalf("batches = %v, want one batch of one row", batches)
	}
	got := batches[0].Columns[0].Float64Data[0]
	if got != 42.5 {
		t.Fatalf("decoded value = %v, want 42.5", got)
	}
}

// wshzInlineFixture builds a valid WSHF payload for one float64=42.5 row
// (the same hand-built layout TestReadScalarFromStageOutput_WSHFDecode
// uses) and wraps it in a WSHZ envelope: magic "WSHZ" + a zstd stream of
// the raw WSHF bytes, exactly what wshf.Decompress expects to unwrap.
func wshzInlineFixture(tb testing.TB) []byte {
	tb.Helper()
	var raw []byte
	raw = append(raw, 'W', 'S', 'H', 'F')
	raw = binary.LittleEndian.AppendUint32(raw, 1) // 1 chunk
	raw = binary.LittleEndian.AppendUint16(raw, 1) // 1 col
	raw = binary.LittleEndian.AppendUint16(raw, 1) // name length = 1
	raw = append(raw, 'x')
	raw = append(raw, byte(parquet.TypeFloat64))
	raw = binary.LittleEndian.AppendUint32(raw, 1) // numRows = 1
	raw = binary.LittleEndian.AppendUint32(raw, 1) // bitmapWords = 1
	raw = binary.LittleEndian.AppendUint64(raw, 1) // bit 0 = valid
	raw = binary.LittleEndian.AppendUint32(raw, 8) // dataLen = 8 bytes
	raw = binary.LittleEndian.AppendUint64(raw, math.Float64bits(42.5))

	var body bytes.Buffer
	zw, err := zstd.NewWriter(&body)
	if err != nil {
		tb.Fatalf("zstd.NewWriter: %v", err)
	}
	if _, err := zw.Write(raw); err != nil {
		tb.Fatalf("zstd write: %v", err)
	}
	if err := zw.Close(); err != nil {
		tb.Fatalf("zstd close: %v", err)
	}

	data := append([]byte{}, wshf.MagicWSHZ[:]...)
	return append(data, body.Bytes()...)
}

// truncatedInlineChunk is a syntactically valid WSHF header for one INT64
// column promising one chunk of eight rows, then nothing. The bytes the
// chunk claims never arrive.
func truncatedInlineChunk() []byte {
	b := []byte("WSHF")
	b = append(b, 1, 0, 0, 0) // numChunks = 1
	b = append(b, 1, 0)       // numCols = 1
	b = append(b, 1, 0)       // name length = 1
	b = append(b, 'v')
	b = append(b, byte(parquet.TypeInt64))
	b = append(b, 8, 0, 0, 0) // chunk row count = 8, and the payload ends
	return b
}
