// SPDX-License-Identifier: MIT

package batch

// A DECIMAL value's DISPLAY scale (ADR-0024's 2026-10-06 amendment, arc PS
// stage 1). PostgreSQL's numeric keeps a display scale on every value: an
// unconstrained value prints its own fraction digits (`2.50`, `1.5`, `7`)
// whatever the scale of the column it sits in. The carrier here stays one
// Int128 per value at the vector's one scale S (DecimalColumn.Scale); the
// display scale is a per-value CODE beside it:
//
//	0..38          the value prints that many fraction digits (≤ S; the
//	               carrier's digits past it are zero, invariant I1)
//	DScaleCarrier  the value has no display scale of its own: it prints at
//	               S, or trimmed when its column carries ADR-0024 §10's
//	               unconstrained mark — the text every value printed before
//	DScaleUnknown  a value whose display scale was lost (a stored bare
//	               NUMERIC read back, or a value moved out of such a column
//	               into one without the mark): it prints trimmed
//
// The codes ride the vector, never the plan, and nothing that compares,
// hashes, sorts or keys a value reads them (invariant I5): equal numbers have
// equal carriers at one scale, so `2.50 = 2.5` and they are one group.

import "strings"

const (
	// DScaleCarrier is the code of a value with no display scale of its
	// own; it is the zero value of DecimalColumn.DAll, so every vector built
	// without one is uniform at it and prints exactly as before.
	DScaleCarrier uint8 = 0xFE
	// DScaleUnknown is the code of a value whose display scale was lost; it
	// prints without the carrier's trailing zeros.
	DScaleUnknown uint8 = 0xFF
)

// dAllCode decodes DecimalColumn.DAll: 0 is DScaleCarrier (the zero value),
// -1 is DScaleUnknown, d+1 is the explicit display scale d.
func dAllCode(d int16) uint8 {
	switch {
	case d == 0:
		return DScaleCarrier
	case d < 0:
		return DScaleUnknown
	}
	return uint8(d - 1)
}

func dAllOf(code uint8) int16 {
	switch code {
	case DScaleCarrier:
		return 0
	case DScaleUnknown:
		return -1
	}
	return int16(code) + 1
}

// UniformDScale is the code every value without a per-row entry has.
func (c *DecimalColumn) UniformDScale() uint8 { return dAllCode(c.DAll) }

// DScaleCode is row i's display-scale code.
func (c *DecimalColumn) DScaleCode(i int) uint8 {
	if c.DScale != nil && i < len(c.DScale) {
		return c.DScale[i]
	}
	return dAllCode(c.DAll)
}

// HasDisplayScale reports whether any row may carry a code other than
// DScaleCarrier — whether a consumer that moves this column must move the
// codes too. A column uniform at DScaleCarrier is the column every reader
// read before the display scale existed.
func (c *DecimalColumn) HasDisplayScale() bool {
	return c.DScale != nil || c.DAll != 0
}

// SetDScaleCode records row i's code. A column whose rows all share one code
// keeps no array (a uniform column costs nothing): the first row that
// differs allocates one, every other row reading the uniform code, and a
// write to the column's first or last row — where a fill in either order
// ends — collapses the array back into DAll when every row shares one code
// again, so a column filled at one display scale other than the carrier's
// (`2.50`, `7.00`, `1.25` into a scale-4 carrier) holds DAll and no array.
func (c *DecimalColumn) SetDScaleCode(i int, code uint8) {
	c.setDScaleCode(i, code)
	if c.DScale != nil && (i == 0 || i == len(c.Data)-1) {
		c.collapseDScale()
	}
}

// setDScaleCode is SetDScaleCode without the collapse, for a loop that
// writes many rows and collapses once at its end.
func (c *DecimalColumn) setDScaleCode(i int, code uint8) {
	if c.DScale == nil {
		if code == dAllCode(c.DAll) {
			return
		}
		if i == 0 && len(c.Data) <= 1 {
			// The column's only row: it is uniform at its code.
			c.DAll = dAllOf(code)
			return
		}
		n := len(c.Data)
		if i >= n {
			n = i + 1
		}
		u := dAllCode(c.DAll)
		c.DScale = make([]uint8, n)
		if u != 0 {
			for k := range c.DScale {
				c.DScale[k] = u
			}
		}
	} else if i >= len(c.DScale) {
		c.growDScale(i + 1)
	}
	c.DScale[i] = code
}

// collapseDScale drops the per-row array when every row of the column
// shares one code, which then becomes DAll. It stops at the first row that
// differs from row 0, so a varying column costs a short scan.
func (c *DecimalColumn) collapseDScale() {
	n := len(c.Data)
	if c.DScale == nil || n == 0 {
		return
	}
	first := c.DScaleCode(0)
	for k := 1; k < n; k++ {
		if c.DScaleCode(k) != first {
			return
		}
	}
	c.DScale = nil
	c.DAll = dAllOf(first)
}

// UniformDScaleOver reports the one code rows [0, n) share, for a writer
// that serializes n rows: ok is false when they differ. It never allocates
// and never changes the column.
func (c *DecimalColumn) UniformDScaleOver(n int) (code uint8, ok bool) {
	if c.DScale == nil || n <= 0 {
		return dAllCode(c.DAll), true
	}
	first := c.DScaleCode(0)
	for k := 1; k < n; k++ {
		if c.DScaleCode(k) != first {
			return 0, false
		}
	}
	return first, true
}

// growDScale extends the per-row array to n entries, the new ones at the
// uniform code.
func (c *DecimalColumn) growDScale(n int) {
	u := dAllCode(c.DAll)
	for len(c.DScale) < n {
		c.DScale = append(c.DScale, u)
	}
}

// SetUniformDScale makes every row display at code.
func (c *DecimalColumn) SetUniformDScale(code uint8) {
	c.DScale = nil
	c.DAll = dAllOf(code)
}

// ResetDScale is SetUniformDScale(DScaleCarrier): the state of a vector
// before anything wrote a display scale into it. Every reuse of a vector's
// storage for new values starts here, so a reused vector never shows a
// previous batch's codes.
func (c *DecimalColumn) ResetDScale() {
	c.DScale = nil
	c.DAll = 0
}

// truncateDScale keeps the codes of the first n rows after Data shrank to n.
func (c *DecimalColumn) truncateDScale(n int) {
	if len(c.DScale) > n {
		c.DScale = c.DScale[:n]
	}
}

// CopyRow writes src's row si — carrier and display scale — into row di.
// The two columns hold their carriers at the same scale (the caller's
// contract, as for every direct carrier copy).
func (c *DecimalColumn) CopyRow(di int, src *DecimalColumn, si int) {
	c.Data[di] = src.Data[si]
	if src.DScale == nil && src.DAll == c.DAll && c.DScale == nil {
		return
	}
	c.SetDScaleCode(di, src.DScaleCode(si))
}

// CopyRange copies n rows src[si:] into c[di:], carriers and codes.
func (c *DecimalColumn) CopyRange(di int, src *DecimalColumn, si, n int) {
	copy(c.Data[di:di+n], src.Data[si:si+n])
	c.copyCodes(di, src, si, n)
}

// CopyCodes copies the display-scale codes of n rows src[si:] into c[di:],
// for a caller that moved the carriers itself.
func (c *DecimalColumn) CopyCodes(di int, src *DecimalColumn, si, n int) {
	c.copyCodes(di, src, si, n)
}

func (c *DecimalColumn) copyCodes(di int, src *DecimalColumn, si, n int) {
	if n <= 0 || (src.DScale == nil && src.DAll == c.DAll && c.DScale == nil) {
		return
	}
	if di == 0 && c.DScale == nil && src.DScale == nil && n >= len(c.Data) {
		// Every row of c is overwritten by a uniform source: c is uniform.
		c.DAll = src.DAll
		return
	}
	for k := 0; k < n; k++ {
		c.setDScaleCode(di+k, src.DScaleCode(si+k))
	}
	c.collapseDScale()
}

// Gather writes src's rows sel[0..] into c's rows 0.., carriers and codes.
func (c *DecimalColumn) Gather(src *DecimalColumn, sel []uint32) {
	for i, idx := range sel {
		c.Data[i] = src.Data[idx]
	}
	c.GatherCodes(src, sel)
}

// GatherCodes is Gather's display-scale half, for a caller that gathered the
// carriers itself (a NULL-aware or rescaling loop).
func (c *DecimalColumn) GatherCodes(src *DecimalColumn, sel []uint32) {
	if src.DScale == nil && src.DAll == c.DAll && c.DScale == nil {
		return
	}
	if src.DScale == nil && c.DScale == nil && len(sel) >= len(c.Data) {
		c.DAll = src.DAll
		return
	}
	for i, idx := range sel {
		c.setDScaleCode(i, src.DScaleCode(int(idx)))
	}
	c.collapseDScale()
}

// AppendRow appends src's row si (carrier and code); a NULL row appends a
// zero carrier at the uniform code.
func (c *DecimalColumn) AppendRow(src *DecimalColumn, si int, isNull bool) {
	var x Int128
	code := dAllCode(c.DAll)
	if !isNull {
		x = src.Data[si]
		code = src.DScaleCode(si)
	}
	c.Data = append(c.Data, x)
	if c.DScale != nil || code != dAllCode(c.DAll) {
		// An append never turns a varying column uniform, so no collapse:
		// a uniform append fill keeps no array through the one-row rule.
		c.setDScaleCode(len(c.Data)-1, code)
	}
}

// Text is the ONE printer of a DECIMAL value (ADR-0024 §10 as amended): the
// carrier at the vector's scale, cut to the value's display scale. marked is
// the column's ADR-0024 §10 unconstrained mark, which decides only how a
// value with no display scale of its own (DScaleCarrier) prints.
func (c *DecimalColumn) Text(i int, marked bool) string {
	s := c.Data[i].FormatDecimal(c.Scale)
	code := c.DScaleCode(i)
	switch {
	case code == DScaleCarrier:
		if marked {
			return TrimDecimalText(s)
		}
		return s
	case code == DScaleUnknown:
		return TrimDecimalText(s)
	}
	return cutDecimalText(s, c.Scale, int(code))
}

// cutDecimalText cuts s — FormatDecimal's text at scale — to d fraction
// digits. It never drops a nonzero digit: a carrier whose digits past d are
// not zero breaks invariant I1, and printing it at its full scale is the
// loud answer, where the cut would print a different number.
func cutDecimalText(s string, scale, d int) string {
	if d >= scale || scale <= 0 {
		return s
	}
	drop := scale - d
	for k := len(s) - drop; k < len(s); k++ {
		if s[k] != '0' {
			return s
		}
	}
	out := s[:len(s)-drop]
	if d == 0 {
		out = out[:len(out)-1] // the point
		if out == "-0" {
			return "0"
		}
	}
	return out
}

// DecimalTextDScale is the display scale a numeric TEXT names, the way
// PostgreSQL's numeric input reads it: its fraction digits less a positive
// exponent (`2.50` → 2, `7` → 0, `1.5e-3` → 4, `1.25e1` → 1, `1e5` → 0).
// ok is false for text that is not a plain number.
func DecimalTextDScale(s string) (int, bool) {
	s = strings.TrimSpace(s)
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	digits, frac := 0, 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			frac++
		}
	}
	if digits+frac == 0 {
		return 0, false
	}
	exp := 0
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		neg := false
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			neg = s[i] == '-'
			i++
		}
		n := 0
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			if n < 10000 {
				n = n*10 + int(s[i]-'0')
			}
			i++
		}
		if i == start {
			return 0, false
		}
		if neg {
			n = -n
		}
		exp = n
	}
	if i != len(s) {
		return 0, false
	}
	d := frac - exp
	if d < 0 {
		d = 0
	}
	return d, true
}

// textDScaleCode is the code a value written from text gets in a column at
// scale: its display scale when that is below the carrier's, and
// DScaleCarrier when it names every digit the carrier holds (or more, which
// the write rounded away) — so text at the column's own scale, which every
// DECIMAL box of a value at that scale is, leaves the column uniform.
func textDScaleCode(s string, scale int) uint8 {
	d, ok := DecimalTextDScale(s)
	if !ok || d >= scale {
		return DScaleCarrier
	}
	return uint8(d)
}

// GatherDScaleCodes is GatherCodes over any index type, for a caller whose
// carrier loop gathers by an []int or []int32 of source rows: dst row di gets
// src row rows[di]'s code. Two columns without display scales cost one
// check.
func GatherDScaleCodes[T ~int | ~int32 | ~uint32 | ~int64](dst, src *DecimalColumn, rows []T) {
	GatherDScaleCodesAt(dst, src, 0, rows)
}

// GatherDScaleCodesAt is GatherDScaleCodes into dst rows start, start+1, ….
func GatherDScaleCodesAt[T ~int | ~int32 | ~uint32 | ~int64](dst, src *DecimalColumn, start int, rows []T) {
	if !src.HasDisplayScale() && !dst.HasDisplayScale() {
		return
	}
	for di, si := range rows {
		dst.setDScaleCode(start+di, src.DScaleCode(int(si)))
	}
	dst.collapseDScale()
}
