// Package batch provides the core columnar data structures for the execution engine.
package batch

import "math/bits"

// Bitmap is a compact null bitmap using 1 bit per row.
type Bitmap struct {
	data     []uint64
	len      int
	hasNulls int8 // -1 = unknown, 0 = no nulls, 1 = has nulls
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
	return Bitmap{data: data, len: n, hasNulls: 0}
}

// NewBitmapAllNull creates a bitmap with all bits cleared (all null).
func NewBitmapAllNull(n int) Bitmap {
	words := (n + 63) / 64
	hn := int8(1)
	if n == 0 {
		hn = 0
	}
	return Bitmap{data: make([]uint64, words), len: n, hasNulls: hn}
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
	b.hasNulls = 1
}

// SetNullRange sets bits [start, start+count) to 0 (null) using word-level
// operations. For runs spanning full 64-bit words, entire words are zeroed
// in a single assignment instead of 64 individual bit clears.
func (b *Bitmap) SetNullRange(start, count int) {
	if count <= 0 || start < 0 || start >= b.len {
		return
	}
	end := start + count
	if end > b.len {
		end = b.len
	}
	b.hasNulls = 1

	startWord := start / 64
	endWord := (end - 1) / 64
	startBit := uint(start % 64)
	endBit := uint((end - 1) % 64)

	if startWord == endWord {
		// Range fits in a single word: clear bits [startBit..endBit]
		mask := uint64(0)
		for i := startBit; i <= endBit; i++ {
			mask |= 1 << i
		}
		b.data[startWord] &^= mask
		return
	}

	// Clear tail bits of first word
	if startBit > 0 {
		mask := ^((uint64(1) << startBit) - 1) // bits [startBit..63]
		b.data[startWord] &^= mask
		startWord++
	}

	// Zero full words in the middle
	for w := startWord; w < endWord; w++ {
		b.data[w] = 0
	}

	// Clear head bits of last word
	mask := (uint64(1) << (endBit + 1)) - 1 // bits [0..endBit]
	b.data[endWord] &^= mask
}

// SetValid sets the bit at position i to 1 (non-null).
func (b *Bitmap) SetValid(i int) {
	if i < 0 || i >= b.len {
		return
	}
	word := i / 64
	bit := uint(i % 64)
	b.data[word] |= 1 << bit
	if b.hasNulls == 1 {
		b.hasNulls = -1 // may or may not still have nulls
	}
}

// HasNulls returns true if any bit is 0 (null). Short-circuits on the first
// non-full word, making it O(1) in the common all-valid case.
// Result is cached — repeated calls are free.
func (b *Bitmap) HasNulls() bool {
	if b.hasNulls >= 0 {
		return b.hasNulls == 1
	}
	if len(b.data) == 0 {
		b.hasNulls = 0
		return false
	}
	// Check all words except the last
	for i := 0; i < len(b.data)-1; i++ {
		if b.data[i] != ^uint64(0) {
			b.hasNulls = 1
			return true
		}
	}
	// Last word: only valid bits should be set
	last := b.data[len(b.data)-1]
	rem := b.len % 64
	if rem == 0 {
		if last != ^uint64(0) {
			b.hasNulls = 1
			return true
		}
		b.hasNulls = 0
		return false
	}
	mask := (uint64(1) << uint(rem)) - 1
	if last != mask {
		b.hasNulls = 1
		return true
	}
	b.hasNulls = 0
	return false
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

// Words returns the raw uint64 bitmap data for word-level operations.
func (b *Bitmap) Words() []uint64 {
	return b.data
}

// ResetNonNull resets the bitmap to all non-null (all bits 1) for n elements,
// reusing the existing backing slice when capacity allows. This avoids
// allocation on the batch pool hot path.
func (b *Bitmap) ResetNonNull(n int) {
	words := (n + 63) / 64
	if cap(b.data) >= words {
		b.data = b.data[:words]
	} else {
		b.data = make([]uint64, words)
	}
	for i := range b.data {
		b.data[i] = ^uint64(0)
	}
	if rem := n % 64; rem > 0 {
		b.data[len(b.data)-1] = (uint64(1) << rem) - 1
	}
	b.len = n
	b.hasNulls = 0
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
	return Bitmap{data: data, len: newLen, hasNulls: -1}
}
