package parquet

import (
	"bytes"
	"fmt"
	"math"
	"testing"
)

// #971, round-1 B1: the DDL door and the writer are held to ONE bound.
//
// At 0a3da4ff `ResolveColumn("v", "VECTOR(4611686018427387905)")` returned a
// column the writer then finalized with a wrapped four-byte type_length, so a
// CREATE TABLE anybody can type produced a file pyarrow opens as
// fixed_size_binary[4] and wadjet reads back as VECTOR(1). The parser now asks
// the writer's own function, so the two cannot disagree.
func TestTheDDLDoorAndTheWriterBoundVectorTheSameWay(t *testing.T) {
	dims := []int{
		1, 3, MaxVectorDimension, // legal
		MaxVectorDimension + 1, math.MaxInt32, // refused: past the format's width
		1 << 61, 1 << 62, (1 << 62) + 1, math.MaxInt, // refused: the round-1 B1 overflow class
		0, -1, math.MinInt, // refused: not a dimension
	}
	for _, dim := range dims {
		spelling := fmt.Sprintf("VECTOR(%d)", dim)
		_, ddlErr := ResolveColumn("v", spelling)

		var buf bytes.Buffer
		_, writerErr := NewWriter(&buf, Schema{Columns: []Column{{
			Name: "v", Type: TypeVector, Dimension: dim, Nullable: true,
		}}}, DefaultWriterConfig())

		legal := dim >= 1 && dim <= MaxVectorDimension
		if (ddlErr == nil) != (writerErr == nil) {
			t.Errorf("%s: DDL says %v but the writer says %v — the two bounds disagree, which is how "+
				"#971 reached a file through CREATE TABLE (round-1 B1)", spelling, ddlErr, writerErr)
			continue
		}
		if legal && writerErr != nil {
			t.Errorf("%s: a legal dimension was refused: %v", spelling, writerErr)
		}
		if !legal && writerErr == nil {
			t.Errorf("%s: an unwritable dimension was accepted", spelling)
		}
		if writerErr != nil && buf.Len() != 0 {
			t.Errorf("%s: %d bytes were written before the refusal", spelling, buf.Len())
		}
	}

	// A dimension too large for Go's own int does not quietly become a small
	// one either: the scanner fails and the door refuses.
	if _, err := ResolveColumn("v", "VECTOR(99999999999999999999999)"); err == nil {
		t.Error("VECTOR(99999999999999999999999) was accepted")
	}
}

// #971, round-1 B1: every VECTOR dimension the writer ACCEPTS produces a file
// whose declared width is the one that was asked for — checked here through
// the whole writer, not through the helper, and read back through the row
// reader and pyarrow.
//
// MaxVectorDimension is declared-only (one value of it is two gigabytes); the
// small dimensions carry a real value.
func TestAnAcceptedVectorDimensionSurvivesTheFile(t *testing.T) {
	for _, dim := range []int{1, 3, MaxVectorDimension} {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, Schema{Columns: []Column{{
			Name: "v", Type: TypeVector, Dimension: dim, Nullable: true,
		}}}, DefaultWriterConfig())
		if err != nil {
			t.Fatalf("VECTOR(%d) was refused: %v", dim, err)
		}
		rows := 0
		if dim <= 3 {
			vals := make([]float32, dim)
			for i := range vals {
				vals[i] = float32(i + 1)
			}
			if err := w.WriteRows([]map[string]any{{"v": vals}}); err != nil {
				t.Fatalf("VECTOR(%d) write: %v", dim, err)
			}
			rows = 1
		}
		if err := w.Close(); err != nil {
			t.Fatalf("VECTOR(%d) close: %v", dim, err)
		}

		r, err := NewReaderFromBytes(buf.Bytes())
		if err != nil {
			t.Fatalf("VECTOR(%d) reopen: %v", dim, err)
		}
		got := r.Schema().Columns[0]
		if got.Type != TypeVector || got.Dimension != dim {
			t.Errorf("VECTOR(%d) reads back as %v(%d) — the declaration did not survive the footer "+
				"(round-1 B1/N3)", dim, got.Type, got.Dimension)
		}
		mustPyArrowRead(t, buf.Bytes(), rows, fmt.Sprintf("VECTOR(%d)", dim))
	}
}

// #971, round-1 N3: the footer's own encoder narrows Dimension a second time,
// outside buildLeafSchemaElement — the cast the round-0 sweep missed, and what
// turned Dimension 4611686018427387905 into the VECTOR(1) the reader handed
// back. A dimension the format cannot carry is encoded as an obviously absent
// 0, never as a wrapped number that looks like a real declaration.
func TestTheFooterNeverDeclaresAWrappedVectorDimension(t *testing.T) {
	roundTrip := func(dim int) int {
		t.Helper()
		enc := newThriftEncoder()
		enc.encodeLogicalType(&LogicalType{Type: LogicalVector, Dimension: dim})
		lt, err := newThriftDecoder(enc.Bytes()).decodeLogicalType()
		if err != nil {
			t.Fatalf("decoding the encoded LogicalType for VECTOR(%d): %v", dim, err)
		}
		return lt.Dimension
	}
	for _, dim := range []int{1, 3, MaxVectorDimension} {
		if got := roundTrip(dim); got != dim {
			t.Errorf("a legal VECTOR(%d) came back as VECTOR(%d)", dim, got)
		}
	}
	for _, dim := range []int{MaxVectorDimension + 1, 1 << 61, 1 << 62, (1 << 62) + 1, math.MaxInt, -1} {
		if got := roundTrip(dim); got != 0 {
			t.Errorf("VECTOR(%d) was encoded as VECTOR(%d) — a wrapped dimension the reader would "+
				"hand back as a real declaration (round-1 B1/N3)", dim, got)
		}
	}
}
