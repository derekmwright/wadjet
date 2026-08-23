package wshf

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Cursor is a bounds-checked walk over a WSHF byte slice. Every read
// returns an error instead of indexing past the end, which is the whole
// point: the decoder's counts and lengths come OUT of the bytes it is
// walking, so a truncated or corrupt payload otherwise turns a length
// field into a slice bound (#422 — a short inline result panicked the
// coordinator's decode goroutine, which nothing above recovers).
//
// The zero value is not usable; construct with NewCursor. Cursors are
// values, not pointers to the data they walk — Take returns a subslice
// that ALIASES the underlying bytes, so callers must copy anything that
// outlives the payload (the column readers all copy).
type Cursor struct {
	data []byte
	pos  int
}

// NewCursor returns a cursor positioned at the start of data.
func NewCursor(data []byte) Cursor { return Cursor{data: data} }

// NewCursorAt returns a cursor positioned at pos, for the callers that
// resume a walk they interrupted (the chunk reader's per-chunk position).
func NewCursorAt(data []byte, pos int) Cursor {
	if pos > len(data) {
		pos = len(data)
	}
	if pos < 0 {
		pos = 0
	}
	return Cursor{data: data, pos: pos}
}

// Pos is the cursor's byte offset. Strictly monotonic.
func (c *Cursor) Pos() int { return c.pos }

// Size is the length of the payload being walked.
func (c *Cursor) Size() int { return len(c.data) }

// Remaining is how many bytes are left ahead of the cursor.
func (c *Cursor) Remaining() int { return len(c.data) - c.pos }

// short builds the one error shape every bounds failure reports, naming
// what was being read, how much it needed, and where it ran out.
func (c *Cursor) short(what string, n int) error {
	return fmt.Errorf("%s: need %d bytes at offset %d of %d: %w",
		what, n, c.pos, len(c.data), io.ErrUnexpectedEOF)
}

// Take advances by n and returns those bytes, or an error if fewer than n
// remain. A negative n is a corrupt length field, not a rewind.
func (c *Cursor) Take(n int, what string) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("%s: negative length %d at offset %d", what, n, c.pos)
	}
	if c.Remaining() < n {
		return nil, c.short(what, n)
	}
	b := c.data[c.pos : c.pos+n]
	c.pos += n
	return b, nil
}

// Skip advances by n without returning the bytes.
func (c *Cursor) Skip(n int, what string) error {
	_, err := c.Take(n, what)
	return err
}

// Peek returns the next n bytes without advancing.
func (c *Cursor) Peek(n int, what string) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("%s: negative length %d at offset %d", what, n, c.pos)
	}
	if c.Remaining() < n {
		return nil, c.short(what, n)
	}
	return c.data[c.pos : c.pos+n], nil
}

// U8 reads one byte.
func (c *Cursor) U8(what string) (uint8, error) {
	b, err := c.Take(1, what)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// U16 reads a little-endian uint16.
func (c *Cursor) U16(what string) (uint16, error) {
	b, err := c.Take(2, what)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

// U32 reads a little-endian uint32.
func (c *Cursor) U32(what string) (uint32, error) {
	b, err := c.Take(4, what)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

// Len32 reads a little-endian uint32 length field and returns it as an int
// bounded by max — a length is a claim about bytes that have not been
// checked yet, so it is range-checked before anything allocates or slices
// by it. On a 32-bit platform a uint32 near 2^32 would also wrap negative
// as an int; the max check rejects it either way.
func (c *Cursor) Len32(what string, max int) (int, error) {
	v, err := c.U32(what)
	if err != nil {
		return 0, err
	}
	n := int(v)
	if n < 0 || n > max {
		return 0, fmt.Errorf("%s: implausible length %d at offset %d (max %d)", what, v, c.pos-4, max)
	}
	return n, nil
}
