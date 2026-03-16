// Package batch provides the core columnar data structures for the execution engine.
package batch

import "math/bits"

// Bitmap is a compact null bitmap using 1 bit per row.
type Bitmap struct {
	data []uint64
	len  int
}

// NewBitmap creates a new bitmap with the given capacity, all bits set to 1 (non-null).
func NewBitmap(n int) Bitmap {
	words := (n + 63) / 64
	data := make([]uint64, words)
	// Set all bits to 1 (non-null)
	for i := range data {
		data[i] = ^uint64(0)
	}
	// Clear excess bits in the last word
	if rem := n % 64; rem > 0 {
		data[len(data)-1] = (uint64(1) << rem) - 1
	}
	return Bitmap{data: data, len: n}
}

// NewBitmapAllNull creates a bitmap with all bits cleared (all null).
func NewBitmapAllNull(n int) Bitmap {
	words := (n + 63) / 64
	return Bitmap{data: make([]uint64, words), len: n}
}

// IsNull returns true if the bit at position i is 0 (null).
// Includes bounds checking for safety at API boundaries.
func (b *Bitmap) IsNull(i int) bool {
	if i < 0 || i >= b.len {
		return true
	}
	return b.data[i/64]&(1<<uint(i%64)) == 0
}

// IsNullFast returns true if the bit at position i is 0 (null).
// No bounds checking — caller must ensure 0 <= i < b.len.
// Use in hot loops where the index is known to be valid.
func (b *Bitmap) IsNullFast(i int) bool {
	return b.data[i/64]&(1<<uint(i%64)) == 0
}

// SetNull sets the bit at position i to 0 (null).
func (b *Bitmap) SetNull(i int) {
	if i < 0 || i >= b.len {
		return
	}
	word := i / 64
	bit := uint(i % 64)
	b.data[word] &^= 1 << bit
}

// SetValid sets the bit at position i to 1 (non-null).
func (b *Bitmap) SetValid(i int) {
	if i < 0 || i >= b.len {
		return
	}
	word := i / 64
	bit := uint(i % 64)
	b.data[word] |= 1 << bit
}

// NullCount returns the number of null (0) bits.
func (b *Bitmap) NullCount() int {
	count := 0
	for _, w := range b.data {
		count += bits.OnesCount64(w)
	}
	return b.len - count
}

// Len returns the number of bits.
func (b *Bitmap) Len() int {
	return b.len
}

// Grow returns a bitmap that can hold at least newLen bits, preserving existing data.
// If the current bitmap is already large enough, it is returned as-is.
func (b Bitmap) Grow(newLen int) Bitmap {
	if newLen <= b.len {
		return b
	}
	newWords := (newLen + 63) / 64
	if newWords <= len(b.data) {
		// Enough capacity, just extend the valid bits in the new range
		// Set new bits to valid (1) by default
		for i := b.len; i < newLen; i++ {
			b.data[i/64] |= 1 << uint(i%64)
		}
		b.len = newLen
		return b
	}
	data := make([]uint64, newWords)
	copy(data, b.data)
	// Set new words to all-valid
	for i := len(b.data); i < newWords; i++ {
		data[i] = ^uint64(0)
	}
	// Clear excess bits in last word
	if rem := newLen % 64; rem > 0 {
		data[newWords-1] &= (uint64(1) << rem) - 1
	}
	return Bitmap{data: data, len: newLen}
}
