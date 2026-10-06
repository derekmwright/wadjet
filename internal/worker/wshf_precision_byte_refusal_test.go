// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/wshf"
)

// decimalPrecisionByteOffset walks a WSHF header to the precision byte of
// DECIMAL column col.
func decimalPrecisionByteOffset(t *testing.T, data []byte, col int) int {
	t.Helper()
	off := 10 // magic + chunk count + column count
	for i := 0; ; i++ {
		nameLen := int(data[off]) | int(data[off+1])<<8
		off += 2 + nameLen
		typ := data[off]
		off++
		if typ == byte(markFixtureSchema(false, false, false)[1].Type) {
			if i == col {
				return off + 1 // scale, then precision
			}
			off += 2
		}
	}
}

// openEveryWay opens data through every WSHF reader and returns the first
// error each one reports, reading the header and every chunk.
func openEveryWay(data []byte) map[string]error {
	out := map[string]error{}
	_, err := wshf.DecodeBatches(data)
	out["DecodeBatches"] = err
	if cr, err := wshf.NewChunkReader(data); err != nil {
		out["ChunkReader"] = err
	} else {
		for {
			b, err := cr.Next()
			if err != nil || b == nil {
				out["ChunkReader"] = err
				break
			}
		}
	}
	r, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(data[4:])}, wshf.CodecNone)
	if err != nil {
		out["streaming"] = err
	} else {
		for {
			b, err := r.Next()
			if err != nil || b == nil {
				out["streaming"] = err
				break
			}
		}
		r.Close()
	}
	return out
}

// TestWSHFDecoderRefusesAnInvalidPrecisionByte is #1662: a DECIMAL column's
// header precision byte holds a precision of at most 38
// (parquet.MaxDecimalDigits) plus the flag bits a writer sets. A byte past
// that — a precision of 39..63 with or without a flag bit — is a corrupt or
// foreign header; every reader refuses it rather than reading a precision of
// 39, 63 or 103 and decoding the chunk under it.
func TestWSHFDecoderRefusesAnInvalidPrecisionByte(t *testing.T) {
	good := encodeMarkFixture(t, markFixtureSchema(false, false, false))
	for how, err := range openEveryWay(good) {
		if err != nil {
			t.Fatalf("%s: the valid fixture does not decode: %v", how, err)
		}
	}
	off := decimalPrecisionByteOffset(t, good, 1)
	if good[off] != 10 {
		t.Fatalf("fixture: precision byte at %d is %#x, want 0x0a", off, good[off])
	}
	for _, bad := range []byte{39, 63, 0x80 | 39, 0x80 | 63, 0x40 | 39, 0xC0 | 63} {
		data := bytes.Clone(good)
		data[off] = bad
		for how, err := range openEveryWay(data) {
			if err == nil {
				t.Errorf("precision byte %#x: %s decoded the file", bad, how)
			}
		}
	}
}
