// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/wshf"
)

// TestWSHFEveryReaderRefusesABadDisplayScaleSection: the extent validator
// checks the display-scale section's mode and every code exactly as the
// decoder does (ADR-0010: "every reader … refuses an unknown mode or a code
// past the column's scale"). At 60f9f2d5 ValidateChunkBytes returned nil for
// both (PS1 closure review B3).
func TestWSHFEveryReaderRefusesABadDisplayScaleSection(t *testing.T) {
	valid := writeDisplayScaleBytesFixture(t, displayScaleBytesFixture(t, "varying"))
	for _, tc := range []struct {
		kind string
		at   int // offset from the end
		b    byte
	}{
		{"badmode", 4, 7},
		{"badcode", 3, 9},
		{"badcode_last", 1, 5},
		{"badcode_uniform", 0, 0},
	} {
		d := bytes.Clone(valid)
		if tc.kind == "badcode_uniform" {
			d = writeDisplayScaleBytesFixture(t, displayScaleBytesFixture(t, "uniform2"))
			d[len(d)-1] = 9
		} else {
			d[len(d)-tc.at] = tc.b
		}
		cur := wshf.NewCursor(d)
		schema, _, err := wshf.ParseHeader(&cur)
		if err != nil {
			t.Fatalf("%s: header: %v", tc.kind, err)
		}
		if err := wshf.ValidateChunkBytes(schema, 3, d[cur.Pos()+4:]); err == nil {
			t.Errorf("%s: ValidateChunkBytes accepted the chunk", tc.kind)
		}
		if _, err := wshf.DecodeBatches(d); err == nil {
			t.Errorf("%s: DecodeBatches accepted the chunk", tc.kind)
		}
		cr, err := wshf.NewChunkReader(d)
		if err == nil {
			_, err = cr.Next()
		}
		if err == nil {
			t.Errorf("%s: ChunkReader accepted the chunk", tc.kind)
		}
		sr, err := newStreamingShuffleReader(nopReadCloser{bytes.NewReader(d[4:])}, wshf.CodecNone)
		if err == nil {
			_, err = sr.Next()
			sr.Close()
		}
		if err == nil {
			t.Errorf("%s: the streaming reader accepted the chunk", tc.kind)
		}
	}
	// The valid chunk passes every reader.
	cur := wshf.NewCursor(valid)
	schema, _, err := wshf.ParseHeader(&cur)
	if err != nil {
		t.Fatal(err)
	}
	if err := wshf.ValidateChunkBytes(schema, 3, valid[cur.Pos()+4:]); err != nil {
		t.Errorf("valid: ValidateChunkBytes: %v", err)
	}
	if _, err := wshf.DecodeBatches(valid); err != nil {
		t.Errorf("valid: DecodeBatches: %v", err)
	}
}
