package wshf

import (
	"errors"
	"io"
	"testing"
)

// TestCursorRefusesEveryShortRead pins the property the whole package rests
// on: no accessor ever indexes past the end, whatever the length field
// says. Each case is a read that the old hand-written walks performed by
// slicing directly (#422).
func TestCursorRefusesEveryShortRead(t *testing.T) {
	data := []byte{1, 2, 3}
	for _, tc := range []struct {
		name string
		read func(c *Cursor) error
	}{
		{"take past the end", func(c *Cursor) error { _, err := c.Take(4, "x"); return err }},
		{"take a negative length", func(c *Cursor) error { _, err := c.Take(-1, "x"); return err }},
		{"peek past the end", func(c *Cursor) error { _, err := c.Peek(9, "x"); return err }},
		{"skip past the end", func(c *Cursor) error { return c.Skip(4, "x") }},
		{"u32 with three bytes left", func(c *Cursor) error { _, err := c.U32("x"); return err }},
		{"u16 at the last byte", func(c *Cursor) error {
			if err := c.Skip(2, "x"); err != nil {
				return err
			}
			_, err := c.U16("x")
			return err
		}},
		{"u8 at the end", func(c *Cursor) error {
			if err := c.Skip(3, "x"); err != nil {
				return err
			}
			_, err := c.U8("x")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCursor(data)
			if err := tc.read(&c); err == nil {
				t.Fatal("accepted a read past the end")
			}
		})
	}
}

// TestCursorShortReadIsUnexpectedEOF keeps the sentinel intact so callers
// can distinguish "the bytes ran out" from "the bytes disagree".
func TestCursorShortReadIsUnexpectedEOF(t *testing.T) {
	c := NewCursor([]byte{1})
	if _, err := c.U32("v"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
	if c.Pos() != 0 {
		t.Fatalf("a failed read advanced the cursor to %d", c.Pos())
	}
}

// TestCursorLen32BoundsTheClaim: a length field is rejected against its
// ceiling before anything allocates by it, even when the payload happens
// to be long enough to satisfy the read.
func TestCursorLen32BoundsTheClaim(t *testing.T) {
	c := NewCursor([]byte{0xFF, 0xFF, 0xFF, 0x7F})
	if n, err := c.Len32("payload", 1<<20); err == nil {
		t.Fatalf("accepted length %d over a 1 MiB ceiling", n)
	}
	c = NewCursor([]byte{4, 0, 0, 0, 9, 9, 9, 9})
	n, err := c.Len32("payload", 1<<20)
	if err != nil || n != 4 {
		t.Fatalf("Len32 = (%d, %v), want (4, nil)", n, err)
	}
	if b, err := c.Take(n, "payload"); err != nil || len(b) != 4 {
		t.Fatalf("Take after Len32 = (%v, %v)", b, err)
	}
	if c.Remaining() != 0 {
		t.Fatalf("remaining = %d, want 0", c.Remaining())
	}
}

// TestParseHeaderRejectsGarbage covers the header fields ahead of any
// chunk: a payload shorter than the fixed header, a wrong magic, and a
// schema whose name length points past the end.
func TestParseHeaderRejectsGarbage(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"shorter than the header", []byte("WSHF12")},
		{"wrong magic", append([]byte("PAR1"), make([]byte, 32)...)},
		{"schema promised, none present", append([]byte("WSHF"), 0, 0, 0, 0, 1, 0)},
		{"name length past the end", append([]byte("WSHF"), 0, 0, 0, 0, 1, 0, 0xFF, 0x00)},
		{"column count over the ceiling", append([]byte("WSHF"), 0, 0, 0, 0, 0xFF, 0xFF)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewChunkReader(tc.data); err == nil {
				t.Fatal("accepted")
			}
			if _, err := DecodeBatches(tc.data); err == nil {
				t.Fatal("DecodeBatches accepted")
			}
		})
	}
}
