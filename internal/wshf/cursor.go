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

// takeErr builds the error for Take/Peek's failure branch: a negative n is
// a corrupt length field, never a rewind; anything else means fewer than n
// bytes remain.
//
// noinline is load-bearing, not cosmetic, and so is having exactly ONE
// call site for it in each of Take/Peek (this used to be two: a
// fmt.Errorf built directly in each function's own body for the negative
// case, plus a call to a separate short() for the other). Every read in
// this package funnels through Take, so its own compiled body is the one
// piece of code that runs once per field of every chunk of every WSHF
// payload decoded — keeping the rare, string-heavy error formatting out
// of it, in one shared function instead of duplicated across Take AND
// Peek, is a real reduction in what that hot body carries.
//
// It does not, despite that, make Take or Peek inlinable at their own
// call sites: the compiler charges a fixed extraCallCost (57,
// cmd/compile/internal/inline.inlineExtraCallCost) against a function's
// own inlining budget (80) for every call it makes to something the
// compiler won't inline, REGARDLESS of which runtime branch is actually
// taken — the inliner's cost model is a static syntactic estimate, not a
// hot/cold-path-weighted one. Take's own logic (the bounds check, the
// slice, the pointer advance) already costs ~34 on its own
// (`go build -gcflags="-m -m" ./internal/wshf/` with the noinline call
// swapped for a plain `io.EOF` return shows this), and 34+57 still clears
// the 80 budget; Peek fares only slightly better. Before this change each
// carried a SECOND such call (or a fully inlined fmt.Errorf, worse
// still), so the prior costs were 192 (Take) and 183 (Peek) against the
// current 94 and 85 — smaller, but not under the line. Confirmed no
// regression either way: BenchmarkWSHFDecodeHot (internal/worker) shows
// no statistically significant change (benchstat, n=10 interleaved,
// p=0.579) — expected, since nothing outside this file's own compiled
// functions changes shape.
//
//go:noinline
func (c *Cursor) takeErr(n int, what string) error {
	if n < 0 {
		return fmt.Errorf("%s: negative length %d at offset %d", what, n, c.pos)
	}
	return fmt.Errorf("%s: need %d bytes at offset %d of %d: %w",
		what, n, c.pos, len(c.data), io.ErrUnexpectedEOF)
}

// Take advances by n and returns those bytes, or an error if fewer than n
// remain. A negative n is a corrupt length field, not a rewind: the
// uint(n) conversion wraps a negative n to a huge value, so the same
// single comparison rejects it and an insufficient remainder alike
// (c.pos never exceeds len(c.data), so len(c.data)-c.pos is never
// negative and the conversion on that side is always exact).
func (c *Cursor) Take(n int, what string) ([]byte, error) {
	if uint(n) > uint(len(c.data)-c.pos) {
		return nil, c.takeErr(n, what)
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

// Peek returns the next n bytes without advancing. See Take for the
// bounds-check shape.
func (c *Cursor) Peek(n int, what string) ([]byte, error) {
	if uint(n) > uint(len(c.data)-c.pos) {
		return nil, c.takeErr(n, what)
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

// implausibleLengthErr reports a length claim rejected against its
// ceiling. noinline for the reason takeErr is: see above — Len32 also
// calls U32 (inlinable on its own, cost 80, folding Take's non-inlined
// call into it), so it was the most expensive of the three even before
// this change (211) and remains so after (182): the improvement is
// isolating this one rarely-taken format string out of Len32's own body,
// not crossing the inliner's threshold.
//
//go:noinline
func implausibleLengthErr(what string, v uint32, pos, max int) error {
	return fmt.Errorf("%s: implausible length %d at offset %d (max %d)", what, v, pos, max)
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
		return 0, implausibleLengthErr(what, v, c.pos-4, max)
	}
	return n, nil
}
