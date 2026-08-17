package scan

import "math/bits"

// rowBitmap is the scan filter's row-match mask for one row group: one BIT
// per row instead of one byte. At the 1M-row row groups ClickBench parts
// use that is 128 KB instead of 1 MB, and every whole-mask pass
// EvalRowGroupPreds makes turns word-wide — set-all becomes 16K stores,
// the match count becomes a popcount loop, and the selection vector is
// built by walking set bits instead of testing every byte.
//
// Layout: bit (i & 63) of words[i>>6] is row i, LSB first. Bits past n are
// held at zero by reset so count and appendSel never see them.
type rowBitmap struct {
	words []uint64
	n     int
	// allSet reports that no bit has been cleared since reset. Only ever
	// a fast-path hint: false is always safe.
	allSet bool
}

// reset sizes the bitmap for n rows and sets every row's bit.
func (b *rowBitmap) reset(n int) {
	nw := (n + 63) / 64
	if cap(b.words) < nw {
		b.words = make([]uint64, nw)
	}
	b.words = b.words[:nw]
	for i := range b.words {
		b.words[i] = ^uint64(0)
	}
	if r := n & 63; r != 0 && nw > 0 {
		b.words[nw-1] = ^uint64(0) >> uint(64-r)
	}
	b.n = n
	b.allSet = true
}

func (b *rowBitmap) get(i int) bool { return b.words[i>>6]&(1<<uint(i&63)) != 0 }

func (b *rowBitmap) clear(i int) {
	b.words[i>>6] &^= 1 << uint(i&63)
	b.allSet = false
}

// clearRange clears rows [lo, hi). Out-of-range ends are clamped to the
// bitmap — callers validate spans against the page window first and this
// only keeps a corrupt file from turning into an index panic.
func (b *rowBitmap) clearRange(lo, hi int) {
	if lo < 0 {
		lo = 0
	}
	if hi > b.n {
		hi = b.n
	}
	if lo >= hi {
		return
	}
	b.allSet = false
	lw, hw := lo>>6, (hi-1)>>6
	lomask := ^uint64(0) << uint(lo&63)          // bits >= lo within word lw
	himask := ^uint64(0) >> uint(63-((hi-1)&63)) // bits <= hi-1 within word hw
	if lw == hw {
		b.words[lw] &^= lomask & himask
		return
	}
	b.words[lw] &^= lomask
	for w := lw + 1; w < hw; w++ {
		b.words[w] = 0
	}
	b.words[hw] &^= himask
}

// count returns the number of set rows.
func (b *rowBitmap) count() int {
	if b.allSet {
		return b.n
	}
	c := 0
	for _, w := range b.words {
		c += bits.OnesCount64(w)
	}
	return c
}

// appendSel appends the set row indices, ascending, to dst.
func (b *rowBitmap) appendSel(dst []uint32) []uint32 {
	for wi, w := range b.words {
		base := uint32(wi << 6)
		for w != 0 {
			dst = append(dst, base+uint32(bits.TrailingZeros64(w)))
			w &= w - 1
		}
	}
	return dst
}
