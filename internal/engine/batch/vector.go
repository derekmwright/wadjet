package batch

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// formatIPv4 formats a uint32 IPv4 address as a string without allocating net.IP.
func formatIPv4(v uint32) string {
	var buf [15]byte // max "255.255.255.255"
	n := putIPv4(buf[:], v)
	return string(buf[:n])
}

// putIPv4 writes v's dotted quad into dst (which must hold 15 bytes) and
// returns how many bytes it wrote. Split out of formatIPv4 so FormatIPv6's
// v4-mapped tail can render into its OWN stack buffer instead of building a
// string it immediately copies. dst is written and never retained, so the
// caller's array stays on the stack.
func putIPv4(dst []byte, v uint32) int {
	n := 0
	for i := 3; i >= 0; i-- {
		if i < 3 {
			dst[n] = '.'
			n++
		}
		octet := v >> (uint(i) * 8) & 0xFF
		if octet >= 100 {
			dst[n] = '0' + byte(octet/100)
			n++
			octet %= 100
			dst[n] = '0' + byte(octet/10)
			n++
			dst[n] = '0' + byte(octet%10)
			n++
		} else if octet >= 10 {
			dst[n] = '0' + byte(octet/10)
			n++
			dst[n] = '0' + byte(octet%10)
			n++
		} else {
			dst[n] = '0' + byte(octet)
			n++
		}
	}
	return n
}

// FormatIPv6 renders a 16-byte IPv6 address the way PostgreSQL's inet output
// function does, which is NOT what net.IP.String() does for two families:
//
//	::ffff:10.0.0.1   Go collapses a v4-MAPPED address to its bare dotted quad
//	                  (`10.0.0.1`), a value the engine itself says the column
//	                  does not equal — `a = '10.0.0.1'` is false and
//	                  `a = '::ffff:10.0.0.1'` is true, both correctly (#580).
//	::1.2.3.4         Go renders a v4-COMPATIBLE address in hex (`::102:304`)
//	                  where the server prints the embedded quad.
//
// PostgreSQL's rule, measured on 17.11 rather than remembered: take the FIRST
// longest run of zero 16-bit words of length >= 2 and write it `::`; render
// the trailing four bytes as a dotted quad when that run starts at word 0 and
// is either six words long (`::a.b.c.d`) or five words long with word 5 equal
// to 0xffff (`::ffff:a.b.c.d`). Everything else is lower-case hex groups. The
// zero-run choice is Go's too, so only the dotted-quad rule differs.
//
// Measured cells (`SELECT '<lit>'::inet` on 17.11): `::ffff:10.0.0.1`,
// `::ffff:0.0.0.0`, `::ffff:255.255.255.255`, `::1.2.3.4`, `::2`, `::1`, `::`,
// `0:1::`, `1::`, `2001:db8::1:0:0:1`, `::ffff:0:102:304`, `64:ff9b::102:304`.
//
// The comparison and ordering value is untouched: this is the printed form
// only, and `net.ParseIP` reads the text back to the same sixteen bytes, which
// is what kernel.IPv6RowKey and exec.boxedIPv6Compare rely on.
func FormatIPv6(raw []byte) string {
	if len(raw) != 16 {
		return ""
	}
	var words [8]uint16
	for i := range words {
		words[i] = uint16(raw[i*2])<<8 | uint16(raw[i*2+1])
	}
	bestBase, bestLen, curBase, curLen := -1, 0, -1, 0
	for i := 0; i < 8; i++ {
		if words[i] == 0 {
			if curBase == -1 {
				curBase, curLen = i, 1
			} else {
				curLen++
			}
			continue
		}
		if curBase != -1 {
			if curLen > bestLen {
				bestBase, bestLen = curBase, curLen
			}
			curBase = -1
		}
	}
	if curBase != -1 && curLen > bestLen {
		bestBase, bestLen = curBase, curLen
	}
	if bestLen < 2 {
		bestBase = -1
	}
	// The dotted-quad tail fires at word 6 only, so a run of SEVEN zero words
	// (`::2`) never reaches it — which is why the server prints `::2` and not
	// `::0.0.0.2`.
	quadTail := bestBase == 0 && (bestLen == 6 || (bestLen == 5 && words[5] == 0xffff))
	// One stack buffer and one allocation, the way formatMAC does it: the
	// strings.Builder plus a strconv.FormatUint per non-zero hextet this
	// replaces cost up to nine heap strings per address, on a path every
	// IPv6 value goes through (round-2 review B2-4). 45 = the longest form,
	// "0000:0000:0000:0000:0000:ffff:255.255.255.255".
	const hexd = "0123456789abcdef"
	var buf [45]byte
	n := 0
	for i := 0; i < 8; i++ {
		if bestBase != -1 && i >= bestBase && i < bestBase+bestLen {
			if i == bestBase {
				buf[n] = ':'
				n++
			}
			continue
		}
		if i != 0 {
			buf[n] = ':'
			n++
		}
		if i == 6 && quadTail {
			n += putIPv4(buf[n:], uint32(raw[12])<<24|uint32(raw[13])<<16|
				uint32(raw[14])<<8|uint32(raw[15]))
			return string(buf[:n])
		}
		w := words[i]
		if w >= 0x1000 {
			buf[n] = hexd[w>>12]
			n++
		}
		if w >= 0x100 {
			buf[n] = hexd[(w>>8)&0xf]
			n++
		}
		if w >= 0x10 {
			buf[n] = hexd[(w>>4)&0xf]
			n++
		}
		buf[n] = hexd[w&0xf]
		n++
	}
	if bestBase != -1 && bestBase+bestLen == 8 {
		buf[n] = ':'
		n++
	}
	return string(buf[:n])
}

// formatMAC formats a uint64 (lower 48 bits) as a MAC address without allocating net.HardwareAddr.
func formatMAC(v uint64) string {
	const hex = "0123456789abcdef"
	var buf [17]byte // "aa:bb:cc:dd:ee:ff"
	for i := 0; i < 6; i++ {
		if i > 0 {
			buf[i*3-1] = ':'
		}
		b := byte(v >> uint((5-i)*8))
		buf[i*3] = hex[b>>4]
		buf[i*3+1] = hex[b&0xf]
	}
	return string(buf[:17])
}

// TypeID is an alias for the parquet TypeID used throughout the engine.
type TypeID = parquet.TypeID

const (
	TypeBool      = parquet.TypeBool
	TypeInt32     = parquet.TypeInt32
	TypeInt64     = parquet.TypeInt64
	TypeFloat32   = parquet.TypeFloat32
	TypeFloat64   = parquet.TypeFloat64
	TypeString    = parquet.TypeString
	TypeBytes     = parquet.TypeBytes
	TypeTimestamp = parquet.TypeTimestamp
	TypeIPv4      = parquet.TypeIPv4
	TypeIPv6      = parquet.TypeIPv6
	TypeCIDR      = parquet.TypeCIDR
	TypeMAC       = parquet.TypeMAC
	TypePort      = parquet.TypePort
	TypeProtocol  = parquet.TypeProtocol
	TypeDuration  = parquet.TypeDuration
	TypeUUID      = parquet.TypeUUID
	TypeDate      = parquet.TypeDate
	TypeDecimal   = parquet.TypeDecimal
	TypeArray     = parquet.TypeArray
	TypeRow       = parquet.TypeRow
	TypeMap       = parquet.TypeMap
	TypeVector    = parquet.TypeVector
)

// BytesColumn stores variable-length byte data (strings, binary) with zero
// per-row allocations using an offset/data layout.
type BytesColumn struct {
	Offsets []uint32 // len = num_rows + 1
	Data    []byte   // contiguous buffer

	// ShapeOnly marks a column decoded for its SHAPE only: Offsets carry
	// the real per-row byte lengths but Data was never written (the
	// lengths-only scan decode, internal/engine/scan/lengths_decode.go).
	// LENGTH()/octet_length(), IS [NOT] NULL and the empty-string
	// comparisons answer off Offsets and the null mask alone; any attempt
	// to read a VALUE is a planner-analysis bug and panics immediately
	// rather than returning a wrong answer. Copy paths propagate the flag
	// instead of moving bytes that do not exist.
	ShapeOnly bool
}

// NewBytesColumn creates a new BytesColumn with the given capacity.
// Pre-allocates offsets for positional access (all offsets start at 0 = empty strings).
// Data arena is lazily allocated: starts empty and grows on first use. Hot paths
// (scan BulkSet, gather PreAllocBytes) know the exact size they need, so eager
// pre-allocation just wastes memclr on unused capacity. For pooled batches, the
// grown capacity is retained across Reset cycles.
func NewBytesColumn(capacity int) BytesColumn {
	offsets := make([]uint32, capacity+1)
	return BytesColumn{
		Offsets: offsets,
	}
}

// PreAllocBytes ensures the Data arena has at least n bytes of capacity.
// Use this when the expected total byte size is known (e.g., from Parquet metadata)
// to avoid reallocations during sequential Set calls.
func (bc *BytesColumn) PreAllocBytes(n int) {
	if cap(bc.Data) < n {
		newData := make([]byte, len(bc.Data), n)
		copy(newData, bc.Data)
		bc.Data = newData
	}
}

// Set writes a value at positional index i. The BytesColumn must have been
// created with NewBytesColumn(capacity >= i+1). Values must be set in order
// (i = 0, 1, 2, ...) because later offsets depend on prior data length.
func (bc *BytesColumn) Set(i int, val []byte) {
	bc.refuseValueIntoShapeOnly(len(val))
	bc.Data = append(bc.Data, val...)
	bc.Offsets[i+1] = uint32(len(bc.Data))
}

// refuseValueIntoShapeOnly is copyShapeRange's guard in the other direction,
// and the two together are what keep one column from carrying both encodings.
//
// copyShapeRange refuses a SHAPE landing on a destination that holds VALUES.
// This refuses n bytes of VALUE landing on a destination already marked
// shape-only. Without it the mix was silent and produced a wrong LENGTH rather
// than an error: the offsets advance by the appended bytes while the earlier
// rows' offsets describe lengths of bytes that were never written, so the pair
// goes descending and LengthAt's defence against a malformed pair answers 0.
// Before #791 the mix was loud by accident — GetValue panicked on the shape
// rows — and teaching the row boundary to box a length took that away, so the
// guard is now explicit (ADR-0023 item 6: neither encoder may write bytes its
// own reader refuses).
//
// ZERO bytes is not a value write. WriteNullAt advances a shape-only column's
// offsets through Set(i, nil), and a zero-length row is the same thing
// copyShapeRange writes for a zero-length shape row.
func (bc *BytesColumn) refuseValueIntoShapeOnly(n int) {
	if n > 0 && bc.ShapeOnly {
		panic("batch: value bytes written into a shape-only BytesColumn — the scan decoded " +
			"lengths only, so this column cannot also carry values (planner analysis bug)")
	}
}

// SetString writes a string value at positional index i. Same contract as
// Set (sequential i), but takes a string: `append(dst, s...)` copies
// straight out of the string, where Set's callers had to materialize a
// []byte(s) conversion first — one heap allocation per row on the
// string-producing projection paths.
func (bc *BytesColumn) SetString(i int, val string) {
	bc.refuseValueIntoShapeOnly(len(val))
	bc.Data = append(bc.Data, val...)
	bc.Offsets[i+1] = uint32(len(bc.Data))
}

// BulkSet copies a contiguous block of byte array data into the column,
// computing offsets from the source offset array. This replaces n individual
// Set calls with a single bulk append + offset arithmetic, reducing memmove
// overhead for Parquet page loading.
func (bc *BytesColumn) BulkSet(dstOffset int, srcData []byte, srcOffsets []uint32, n int) {
	baseOff := uint32(len(bc.Data))
	srcBase := srcOffsets[0]
	bc.refuseValueIntoShapeOnly(int(srcOffsets[n] - srcBase))
	bc.Data = append(bc.Data, srcData[srcBase:srcOffsets[n]]...)
	for i := 0; i < n; i++ {
		bc.Offsets[dstOffset+i+1] = baseOff + (srcOffsets[i+1] - srcBase)
	}
}

// BulkCopy copies a contiguous range [srcOff, srcOff+count) from src into
// dst at [dstOff, dstOff+count). Uses a single Data append + offset arithmetic
// instead of per-element Set calls, reducing memmove overhead for batch merging.
func (dst *BytesColumn) BulkCopy(dstOff int, src *BytesColumn, srcOff, count int) {
	if src.ShapeOnly {
		dst.copyShapeRange(dstOff, src, srcOff, count)
		return
	}
	baseOff := uint32(len(dst.Data))
	srcStart := src.Offsets[srcOff]
	srcEnd := src.Offsets[srcOff+count]
	dst.refuseValueIntoShapeOnly(int(srcEnd - srcStart))
	dst.Data = append(dst.Data, src.Data[srcStart:srcEnd]...)
	for i := 0; i < count; i++ {
		dst.Offsets[dstOff+i+1] = baseOff + (src.Offsets[srcOff+i+1] - srcStart)
	}
}

// SetFrom copies a single value from src at position si into dst at position di.
// Combines Value + Set into one call, avoiding the intermediate slice creation
// and reducing function call overhead in gather loops. Values must be set in
// order (di = 0, 1, 2, ...) because later offsets depend on prior data length.
func (dst *BytesColumn) SetFrom(di int, src *BytesColumn, si int) {
	if src.ShapeOnly {
		dst.copyShapeRange(di, src, si, 1)
		return
	}
	srcStart := src.Offsets[si]
	srcEnd := src.Offsets[si+1]
	dst.refuseValueIntoShapeOnly(int(srcEnd - srcStart))
	dst.Data = append(dst.Data, src.Data[srcStart:srcEnd]...)
	dst.Offsets[di+1] = uint32(len(dst.Data))
}

// copyShapeRange propagates a shape-only source: the destination inherits
// the per-row lengths and the ShapeOnly mark, and no bytes move. Mixing a
// shape-only source into a destination that already holds real values would
// silently corrupt it, so that combination panics with the same diagnosis
// Value gives.
func (dst *BytesColumn) copyShapeRange(dstOff int, src *BytesColumn, srcOff, count int) {
	if len(dst.Data) > 0 {
		panic("batch: shape-only column copied into a destination holding values — some consumer of this column is not a shape consumer (planner analysis bug)")
	}
	dst.ShapeOnly = true
	cur := dst.Offsets[dstOff]
	for i := 0; i < count; i++ {
		cur += uint32(src.LengthAt(srcOff + i))
		dst.Offsets[dstOff+i+1] = cur
	}
}

// Value returns the byte slice at position i.
//
// Defensive against a gather-output hazard: when HashJoin's
// gatherBuildVector skips unmatched rows without calling
// BytesData.SetFrom, the destination Offsets may end up with
// Offsets[i+1] == 0 while Offsets[i] > 0, producing a malformed
// descending pair. Treat this as empty rather than panicking; the
// null bitmap alongside the column records the "no value" state
// authoritatively, and downstream filter / projection kernels already
// consult it.
func (bc *BytesColumn) Value(i int) []byte {
	if bc.ShapeOnly {
		panic("batch: value read from a shape-only BytesColumn — the scan decoded lengths only, so some consumer of this column is not a shape consumer (planner analysis bug)")
	}
	start := bc.Offsets[i]
	end := bc.Offsets[i+1]
	if end < start {
		return nil
	}
	return bc.Data[start:end]
}

// IsShapeOnly reports whether this vector's bytes were never decoded — the
// lengths-only scan decode, or a copy that propagated the mark. A VIEW is
// shape-only when the vector it looks through is.
func (v *Vector) IsShapeOnly() bool {
	if v == nil {
		return false
	}
	if v.Base != nil {
		return v.Base.IsShapeOnly()
	}
	return v.BytesData.ShapeOnly
}

// setShapeOnlyLen writes one row of a shape-only column: the length advances
// the offsets and no byte is written. Sequential, like Set and copyShapeRange,
// because offset i+1 is defined against offset i.
//
// The destination must be a bytes-backed column that holds no values. A
// shape-only length landing on a column with real bytes in it, or on a
// fixed-width one, is the same confusion copyShapeRange refuses and it is
// refused the same way — the #361 guard, which the pipeline seams turn into a
// query error rather than a wrong answer.
func (v *Vector) setShapeOnlyLen(i, n int) {
	switch v.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
	default:
		v.mismatch(ShapeOnlyLen(n))
		return
	}
	bc := &v.BytesData
	if len(bc.Data) > 0 {
		panic("batch: a shape-only length written into a column holding values — " +
			"some consumer of this column is not a shape consumer (planner analysis bug)")
	}
	bc.ShapeOnly = true
	bc.Offsets[i+1] = bc.Offsets[i] + uint32(n)
}

// ShapeOnlyLen is what the boxing boundary hands back for a row of a
// SHAPE-ONLY column: the value is NOT AVAILABLE, and this is its byte length.
//
// It exists because a shape-only column has to survive a ROW-SHAPED detour.
// The scan decodes lengths and no bytes when the planner proves every use of
// the column reads its shape (COUNT, LENGTH, IS NULL, empty-string), and the
// vector paths carry that faithfully — copyShapeRange propagates the mark
// rather than moving bytes that do not exist. The row paths could not: a
// grouped aggregate under memory pressure buffers its input through
// RecordBatch.ToRows, whose per-row box comes from Vector.GetValue, and the
// only thing GetValue could produce for such a row was the panic that says a
// value was read (#791).
//
// So the box is neither the value nor a rendering of it. It is a REFUSAL that
// carries the length: LengthAt's answer, and nothing else. Written back
// through SetValue it reconstructs a shape-only column with the same
// per-row lengths, so what comes out of the detour is what went in — and a
// consumer that then wants the bytes raises the same guard it always did,
// at the same place, saying the same thing.
//
// A type of its own rather than an int: an int would be indistinguishable
// from a value at every `switch v := x.(type)` in the tree, which is exactly
// how a lossy encoding gets written by accident (#632, ADR-0023 item 6 — an
// encoder must never write bytes its own reader refuses, and a renderer is
// not a value).
type ShapeOnlyLen int

// LengthAt returns the byte length of row i without reading the value. It
// is the only value-shaped accessor valid on a shape-only column, and it
// mirrors Value's defensive handling of the descending-offset hazard.
func (bc *BytesColumn) LengthAt(i int) int {
	start := bc.Offsets[i]
	end := bc.Offsets[i+1]
	if end < start {
		return 0
	}
	return int(end - start)
}

// StringValue returns the string at position i.
func (bc *BytesColumn) StringValue(i int) string {
	return string(bc.Value(i))
}

// UnsafeStringValue returns a zero-copy string view of the value at position i.
// The returned string shares the BytesColumn's backing buffer and is only valid
// while the BytesColumn is not modified or recycled. Use for transient comparisons
// in filter/sort kernels where the string is consumed immediately.
func (bc *BytesColumn) UnsafeStringValue(i int) string {
	b := bc.Value(i)
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// Len returns the number of values.
func (bc *BytesColumn) Len() int {
	return len(bc.Offsets) - 1
}

// Reset clears the bytes column for reuse.
func (bc *BytesColumn) Reset() {
	for i := range bc.Offsets {
		bc.Offsets[i] = 0
	}
	bc.Data = bc.Data[:0]
	bc.ShapeOnly = false
}

// ResetForWrite resizes the column to hold exactly n values and clears it for
// a fresh sequential write, retaining the data arena's capacity. Callers that
// know the total byte size still call PreAllocBytes afterwards; once the arena
// has reached its high-water mark that call becomes a no-op instead of a
// fresh multi-hundred-KB span. See (*Vector).ResetForWrite.
func (bc *BytesColumn) ResetForWrite(n int) {
	if cap(bc.Offsets) >= n+1 {
		bc.Offsets = bc.Offsets[:n+1]
	} else {
		bc.Offsets = make([]uint32, n+1)
	}
	clear(bc.Offsets)
	bc.Data = bc.Data[:0]
	bc.ShapeOnly = false
}

// MemBytes returns the heap bytes consumed by the offset and data slices.
//
// Offsets are sized by len (the logical rows+1), but the data arena is sized by
// cap: a pooled BytesColumn retains its grown arena capacity across Reset cycles
// (see NewBytesColumn), so cap(Data) is the true resident footprint. This is the
// honest byte count that replaces the b.Len*48 estimate in EstimateBatchBytes.
func (bc *BytesColumn) MemBytes() int64 {
	return int64(len(bc.Offsets))*4 + int64(cap(bc.Data)) // 4 = sizeof(uint32) offset
}

// Vector holds a single column of data. Uses typed slices instead of interface{}.
type Vector struct {
	Type        TypeID
	Len         int
	Nulls       Bitmap
	BoolData    []bool
	Int32Data   []int32
	Int64Data   []int64
	Float32Data []float32
	Float64Data []float64
	BytesData   BytesColumn
	DecimalData DecimalColumn // for TypeDecimal

	// VECTOR type: fixed-dimension float32 embeddings
	// Row i's vector: Float32Data[i*VectorDim : (i+1)*VectorDim]
	VectorDim int // VECTOR: dimensionality (number of float32 elements per row)

	// Nested type fields (ARRAY, ROW, MAP)
	Offsets    []int32   // ARRAY/MAP: offsets[i]..offsets[i+1] delimit child elements for row i
	Child      *Vector   // ARRAY: flat vector of all element values
	Children   []*Vector // ROW: one vector per field (same length as parent)
	FieldNames []string  // ROW: names of child fields

	// View (dictionary) form: when Base != nil this vector owns no typed
	// storage — logical row i is Base row Indices[i]. Nulls is the view's OWN
	// override bitmap (a null bit marks the row null regardless of Base; a
	// valid bit defers to Base's nullness through Indices). Base is always an
	// owned vector: NewViewVector composes indices when handed a view base, so
	// views never chain. Views are read-only and understood only by the
	// view-aware accessors (GetValue, CopyValueFrom-as-source, Flatten,
	// MemBytes); typed hot-path accessors (GetInt64, Int64Data[i], ...) fail
	// loud on a view because the typed slices are nil. See view.go.
	Base    *Vector
	Indices []uint32

	// claimed records that a consumer retains this vector's storage past the
	// call that produced it — see (*RecordBatch).Detach, the engine-wide
	// signal for exactly that, and (*Vector).Claim. A producer that reuses
	// vector backing across output batches must surrender a claimed vector
	// instead of writing over it. The claim rides the VECTOR rather than the
	// batch shell because the batch a retaining consumer holds is not always
	// the batch the producer emitted: ColumnPrune, the set-op emitter and
	// partitioned aggregation's per-partition views all mint a derived
	// RecordBatch over the SAME *Vector pointers.
	claimed bool
}

// Claim marks this vector's storage as retained by a consumer, recursively
// through the view base it reads from and its nested children. Once claimed a
// vector is never reused by its producer: the claim is sticky because nothing
// tracks when the retaining consumer is finished (a Sort holds its input until
// Finalize), and a wrong answer here is silent data corruption.
func (v *Vector) Claim() {
	if v == nil || v.claimed {
		return
	}
	v.claimed = true
	if v.Base != nil {
		v.Base.Claim()
	}
	if v.Child != nil {
		v.Child.Claim()
	}
	for _, ch := range v.Children {
		ch.Claim()
	}
}

// Claimed reports whether a consumer has claimed this vector's storage.
func (v *Vector) Claimed() bool { return v != nil && v.claimed }

// ResetForWrite resizes an OWNED vector to exactly n rows and clears its
// per-row state — null bits back to non-null, fixed-width slots to zero, the
// bytes arena emptied — while RETAINING every backing allocation's capacity.
// A producer that reuses one vector across output batches therefore allocates
// only when n passes its high-water mark, where a fresh NewColumnVector
// allocates (and the runtime zeroes) a new span every single batch.
//
// Slots are cleared, not merely resized: the gather loops skip writing null
// and unmatched rows, so a stale value under a null bit would be a
// reuse-visible difference from the freshly-zeroed path for any reader that
// looks at a null slot's value. The clear costs the same memclr `make` was
// already paying; what is saved is the allocation, i.e. the Go heap lock.
//
// Nested ARRAY/MAP/ROW element storage is append-built and is NOT reset here;
// callers must not reuse vectors of those types (the join emit path guards
// them out and mints fresh).
func (v *Vector) ResetForWrite(n int) {
	if v.Base != nil {
		panic("batch: ResetForWrite on a view vector — views own no storage")
	}
	v.Indices = nil
	v.Len = n
	v.Nulls.ResetNonNull(n)
	switch v.Type {
	case TypeBool:
		v.BoolData = resizeCleared(v.BoolData, n)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		v.Int32Data = resizeCleared(v.Int32Data, n)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		v.Int64Data = resizeCleared(v.Int64Data, n)
	case TypeFloat32:
		v.Float32Data = resizeCleared(v.Float32Data, n)
	case TypeFloat64:
		v.Float64Data = resizeCleared(v.Float64Data, n)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData.ResetForWrite(n)
	case TypeDecimal:
		v.DecimalData.Data = resizeCleared(v.DecimalData.Data, n)
	case TypeVector:
		v.Float32Data = resizeCleared(v.Float32Data, n*v.VectorDim)
	default:
		panic("batch: ResetForWrite on a nested column — element storage is append-built")
	}
}

// resizeCleared re-slices s to exactly n zeroed elements, reusing the backing
// array whenever it is large enough. The zeroing matches what make() would
// have produced, so a reused vector is indistinguishable from a fresh one.
func resizeCleared[T any](s []T, n int) []T {
	if cap(s) >= n {
		s = s[:n]
		clear(s)
		return s
	}
	return make([]T, n)
}

// NewVector creates a new vector of the given type and length.
func NewVector(typ TypeID, length int) *Vector {
	return NewVectorWithScale(typ, length, 0)
}

// NewVectorWithScale creates a new vector with scale metadata (used for DECIMAL).
func NewVectorWithScale(typ TypeID, length int, scale int) *Vector {
	v := &Vector{
		Type:  typ,
		Len:   length,
		Nulls: NewBitmap(length),
	}
	switch typ {
	case TypeBool:
		v.BoolData = make([]bool, length)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		v.Int32Data = make([]int32, length)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		v.Int64Data = make([]int64, length)
	case TypeFloat32:
		v.Float32Data = make([]float32, length)
	case TypeFloat64:
		v.Float64Data = make([]float64, length)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData = NewBytesColumn(length)
	case TypeDecimal:
		v.DecimalData = NewDecimalColumn(length, scale)
	case TypeVector:
		// VectorDim is set by the caller via NewVectorVector or newVectorFromColumn
	case TypeArray, TypeMap:
		v.Offsets = make([]int32, length+1)
		// Child vector is set by the caller after construction
	case TypeRow:
		// Children and FieldNames are set by the caller after construction
	}
	return v
}

// EnsureLen grows the vector's backing storage so positions [0, n) are
// addressable for in-place writes (Set / index-assign), preserving existing
// values, defaulting new fixed-width slots to zero and new null bits to
// non-null. Backing arrays grow geometrically (via append) for amortized O(1)
// appends, so an append-style builder can grow a column across many source
// batches instead of pre-sizing to a worst-case capacity. Sets Len to n.
//
// Scalar, bytes, decimal and fixed-dim VECTOR columns are fully supported.
// Nested ARRAY/MAP element storage and ROW children are NOT grown here (their
// element storage is appended by SetValue); callers that build nested columns
// should use pre-sized batches. The hash-join accumulator path that relies on
// EnsureLen guards nested schemas to a pre-sized path for this reason.
func (v *Vector) EnsureLen(n int) {
	if n <= v.Len {
		return
	}
	if v.Base != nil {
		// Views are read-only; growing one for in-place writes is always a
		// caller bug. Fail loud rather than writing into nil typed slices.
		panic("batch: EnsureLen on a view vector — Flatten() first")
	}
	v.Nulls.EnsureLen(n)
	switch v.Type {
	case TypeBool:
		if cap(v.BoolData) >= n {
			v.BoolData = v.BoolData[:n]
		} else {
			g := make([]bool, n, growCap(cap(v.BoolData), n))
			copy(g, v.BoolData)
			v.BoolData = g
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		if cap(v.Int32Data) >= n {
			v.Int32Data = v.Int32Data[:n]
		} else {
			g := make([]int32, n, growCap(cap(v.Int32Data), n))
			copy(g, v.Int32Data)
			v.Int32Data = g
		}
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		if cap(v.Int64Data) >= n {
			v.Int64Data = v.Int64Data[:n]
		} else {
			g := make([]int64, n, growCap(cap(v.Int64Data), n))
			copy(g, v.Int64Data)
			v.Int64Data = g
		}
	case TypeFloat32:
		if cap(v.Float32Data) >= n {
			v.Float32Data = v.Float32Data[:n]
		} else {
			g := make([]float32, n, growCap(cap(v.Float32Data), n))
			copy(g, v.Float32Data)
			v.Float32Data = g
		}
	case TypeFloat64:
		if cap(v.Float64Data) >= n {
			v.Float64Data = v.Float64Data[:n]
		} else {
			g := make([]float64, n, growCap(cap(v.Float64Data), n))
			copy(g, v.Float64Data)
			v.Float64Data = g
		}
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		// Offsets need length n+1 (one trailing offset). Data grows separately
		// via Set/SetFrom appends.
		if cap(v.BytesData.Offsets) >= n+1 {
			v.BytesData.Offsets = v.BytesData.Offsets[:n+1]
		} else {
			g := make([]uint32, n+1, growCap(cap(v.BytesData.Offsets), n+1))
			copy(g, v.BytesData.Offsets)
			v.BytesData.Offsets = g
		}
	case TypeDecimal:
		if cap(v.DecimalData.Data) >= n {
			v.DecimalData.Data = v.DecimalData.Data[:n]
		} else {
			g := make([]Int128, n, growCap(cap(v.DecimalData.Data), n))
			copy(g, v.DecimalData.Data)
			v.DecimalData.Data = g
		}
	case TypeVector:
		if v.VectorDim > 0 {
			need := n * v.VectorDim
			if cap(v.Float32Data) >= need {
				v.Float32Data = v.Float32Data[:need]
			} else {
				g := make([]float32, need, growCap(cap(v.Float32Data), need))
				copy(g, v.Float32Data)
				v.Float32Data = g
			}
		}
	case TypeArray, TypeMap:
		if cap(v.Offsets) >= n+1 {
			v.Offsets = v.Offsets[:n+1]
		} else {
			g := make([]int32, n+1, growCap(cap(v.Offsets), n+1))
			copy(g, v.Offsets)
			v.Offsets = g
		}
	case TypeRow:
		for _, c := range v.Children {
			c.EnsureLen(n)
		}
	}
	v.Len = n
}

// growCap returns a new capacity >= need, at least doubling the old capacity so
// repeated EnsureLen growth amortizes to O(1) per appended row.
func growCap(old, need int) int {
	c := old
	if c == 0 {
		c = need
	}
	for c < need {
		c *= 2
	}
	return c
}

// NewVectorVector creates a new VECTOR column with fixed dimensionality.
// Storage: Float32Data of length * dim, where row i occupies [i*dim, (i+1)*dim).
func NewVectorVector(length, dim int) *Vector {
	v := NewVector(TypeVector, length)
	v.VectorDim = dim
	v.Float32Data = make([]float32, length*dim)
	return v
}

// VectorAt returns a slice of float32 values for row i of a VECTOR column.
func (v *Vector) VectorAt(i int) []float32 {
	if v.VectorDim <= 0 || i*v.VectorDim >= len(v.Float32Data) {
		return nil
	}
	return v.Float32Data[i*v.VectorDim : (i+1)*v.VectorDim]
}

// SetVector sets the float32 values for row i of a VECTOR column.
func (v *Vector) SetVector(i int, vals []float32) {
	if v.VectorDim <= 0 {
		return
	}
	copy(v.Float32Data[i*v.VectorDim:(i+1)*v.VectorDim], vals)
	v.Nulls.SetValid(i)
}

// NewArrayVector creates a new ARRAY vector with the given length and element type.
// The child vector starts with capacity 0; callers append elements and update offsets.
func NewArrayVector(length int, elemType TypeID) *Vector {
	v := NewVector(TypeArray, length)
	v.Child = NewVector(elemType, 0)
	return v
}

// NewRowVector creates a new ROW/STRUCT vector with named child fields.
func NewRowVector(length int, fieldNames []string, fieldTypes []TypeID) *Vector {
	v := NewVector(TypeRow, length)
	v.FieldNames = fieldNames
	v.Children = make([]*Vector, len(fieldNames))
	for i, ft := range fieldTypes {
		v.Children[i] = NewVector(ft, length)
	}
	return v
}

// NewMapVector creates a new MAP vector. Internally stored as ARRAY(ROW("key","value")).
func NewMapVector(length int, keyType, valueType TypeID) *Vector {
	v := NewVector(TypeMap, length)
	child := NewRowVector(0, []string{"key", "value"}, []TypeID{keyType, valueType})
	child.Type = TypeRow
	v.Child = child
	return v
}

// GetValue returns the value at position i as an interface{}.
// Note: returns boxed values for numeric types (unavoidable with any return type).
// Prefer typed accessors (GetInt64, GetFloat64, etc.) in hot paths.
func (v *Vector) GetValue(i int) any {
	if v.Nulls.IsNullFast(i) {
		return nil
	}
	if v.Base != nil {
		// View: own-null already checked above; defer to base through the
		// index (base applies its own null bitmap).
		return v.Base.GetValue(int(v.Indices[i]))
	}
	if v.BytesData.ShapeOnly {
		// The bytes were never decoded. Hand back the length — the one thing
		// this column HAS — rather than the panic that used to be the only
		// answer here, which stopped a correct query the moment it took a
		// row-shaped detour (#791). The mark travels with the box, so
		// SetValue rebuilds a shape-only column and a later value read raises
		// the guard at its own site.
		return ShapeOnlyLen(v.BytesData.LengthAt(i))
	}
	// Hot types first as if-chain for better branch prediction
	switch v.Type {
	case TypeInt64, TypeTimestamp:
		// TIMESTAMP stays a bare int64 of epoch milliseconds here, unlike
		// DATE below. This box is NOT a display boundary: it is also the
		// GROUP BY key (aggregate.go's keyValues -> serializeKey), the
		// aggregate/window spill row encoding, the window comparator, and
		// the row map an UPDATE mutates and re-ingests. Every one of those
		// type-switches on int64 and silently degrades on anything else —
		// a formatted value would collapse distinct timestamps into one
		// group, order a window arbitrarily, and rewrite updated rows as
		// epoch 0. Rendering happens at the renderers instead, which still
		// hold the column's declared type: see FormatTimestamp.
		return v.Int64Data[i]
	case TypeFloat64:
		return v.Float64Data[i]
	case TypeString:
		return v.BytesData.StringValue(i)
	case TypeInt32:
		return v.Int32Data[i]
	case TypeBool:
		return v.BoolData[i]
	case TypeFloat32:
		return v.Float32Data[i]
	case TypeBytes:
		// COPY, never the arena slice BytesData.Value returns. GetValue is a
		// BOXING boundary: every other arm here hands back a value that owns
		// its storage (String materializes a string, IPv6/CIDR/UUID format,
		// VECTOR make+copy), and the consumers are the ones that KEEP the box
		// past the batch — minMaxByState.bestVal for MIN_BY/MAX_BY,
		// groupStateExtras.keyValues for a BYTES GROUP BY key, the global
		// window's first/last/nth, the correlated-subquery value map, ToRows.
		// None of them Claim or Detach, so an arena alias is rewritten under
		// them by any producer that reuses its output backing: Project's
		// BatchPool plus Pipeline.runSerial's Release, the join-emit vector
		// reuse (69aecbb / ADR-0016) and the scan row-group backing pool
		// (docs/design/scan-output-backing-reuse.md). MIN_BY over a BYTES
		// column returned the LAST row group's bytes instead of the min's.
		// One allocation on a path that is already boxing into an interface.
		raw := v.BytesData.Value(i)
		if raw == nil {
			return []byte(nil)
		}
		out := make([]byte, len(raw))
		copy(out, raw)
		return out
	case TypeIPv4:
		return formatIPv4(uint32(v.Int64Data[i]))
	case TypeIPv6:
		return FormatIPv6(v.BytesData.Value(i))
	case TypeCIDR:
		return v.BytesData.StringValue(i)
	case TypeMAC:
		return formatMAC(uint64(v.Int64Data[i]))
	case TypePort, TypeProtocol:
		return v.Int32Data[i]
	case TypeDuration:
		return v.Int64Data[i]
	case TypeUUID:
		raw := v.BytesData.Value(i)
		if len(raw) == 16 {
			return formatUUID(raw)
		}
		return ""
	case TypeDate:
		return FormatDate(v.Int32Data[i])
	case TypeDecimal:
		return v.DecimalData.Data[i].FormatDecimal(v.DecimalData.Scale)
	case TypeVector:
		if v.VectorDim <= 0 {
			return nil
		}
		dst := make([]float32, v.VectorDim)
		copy(dst, v.Float32Data[i*v.VectorDim:(i+1)*v.VectorDim])
		return dst
	case TypeArray, TypeMap:
		if v.Child == nil {
			return nil
		}
		start := int(v.Offsets[i])
		end := int(v.Offsets[i+1])
		elems := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			elems = append(elems, v.Child.GetValue(j))
		}
		return elems
	case TypeRow:
		if v.Children == nil {
			return nil
		}
		row := make(map[string]any, len(v.Children))
		for j, child := range v.Children {
			name := fmt.Sprintf("f%d", j)
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			row[name] = child.GetValue(i)
		}
		return row
	default:
		return nil
	}
}

// SetValue sets the value at position i from an interface{}.
// For string/bytes types, values must be set in sequential order (i = 0, 1, 2, ...).
//
// A non-nil value whose type has no conversion into this vector's storage
// PANICS with *TypeMismatchError (#361) instead of silently keeping the
// zero value — see that type's doc for the contract and the seams that
// convert the panic into a query error. nil is a NULL; STRING/BYTES coerce
// everything through its string form; a parseable-type string that fails to
// parse (IPv4, MAC, UUID) keeps its historical value-level behavior.
func (v *Vector) SetValue(i int, val any) {
	if val == nil {
		// WriteNullAt advances variable-length bookkeeping for EVERY shape —
		// bytes offsets, ARRAY/MAP offsets, ROW children. The old inline
		// version handled only the flat bytes types, so a nil on a nested
		// column skipped its slot and every later row read back shifted
		// (the recurring offsets-on-NULL corruption class).
		v.WriteNullAt(i)
		return
	}
	v.Nulls.SetValid(i)
	if n, ok := val.(ShapeOnlyLen); ok {
		v.setShapeOnlyLen(i, int(n))
		return
	}
	switch v.Type {
	case TypeBool:
		// A value-shape mismatch here used to write NULL, and before that an
		// unchecked assertion killed the process; it now raises the #361
		// guard, which the pipeline seams convert into a query error — the
		// #310 rule ("a wrong type may cost a wrong answer, never the
		// server") upgraded to "an error, never the server".
		switch tv := val.(type) {
		case bool:
			v.BoolData[i] = tv
		case int64:
			v.BoolData[i] = tv != 0
		case int:
			v.BoolData[i] = tv != 0
		case int32:
			v.BoolData[i] = tv != 0
		case float64:
			v.BoolData[i] = tv != 0
		default:
			v.mismatch(val)
		}
	case TypeInt32:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			// A widened box narrowing back into an int4 column. It wrapped
			// silently until #841's review: `ABS(<int32 column>)` at the int4
			// minimum answered -2147483648 because the kernel's correct int64
			// 2147483648 had no int32 here. See IntegerRangeError.
			v.Int32Data[i] = v.int32OrRaise(int64(tv))
		case int64:
			v.Int32Data[i] = v.int32OrRaise(tv)
		case float64:
			v.Int32Data[i] = v.int32FromFloatOrRaise(tv)
		default:
			v.mismatch(val)
		}
	case TypeInt64:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		default:
			v.mismatch(val)
		}
	case TypeTimestamp:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		case string:
			// A TIMESTAMP written as its own WALL-CLOCK TEXT. This arm is what
			// lets a timestamp-valued FUNCTION declare timestamp (#868): the
			// scalar registry has one Go result per entry and every
			// instant-valued function produces expr.formatInstant's text —
			// PostgreSQL's own spelling for the instant — so the projection
			// materializes that text back into the epoch milliseconds this
			// type is. The round trip is exact: FormatTimestamp writes at
			// millisecond resolution and this reads it back at the same one.
			//
			// TypeInt64 keeps its own arm above and does NOT take a string:
			// an integer column has no text form to read, and the two shared
			// this switch only because they share the storage.
			ms, ok := timestampTextMillis(tv)
			if !ok {
				v.mismatch(val)
				return
			}
			v.Int64Data[i] = ms
		case time.Time:
			v.Int64Data[i] = tv.UTC().UnixMilli()
		default:
			v.mismatch(val)
		}
	case TypeFloat32:
		switch tv := val.(type) {
		case float32:
			v.Float32Data[i] = tv
		case float64:
			v.Float32Data[i] = float32(tv)
		case int64:
			v.Float32Data[i] = float32(tv)
		case int:
			v.Float32Data[i] = float32(tv)
		case int32:
			v.Float32Data[i] = float32(tv)
		default:
			v.mismatch(val)
		}
	case TypeFloat64:
		switch tv := val.(type) {
		case float64:
			v.Float64Data[i] = tv
		case float32:
			v.Float64Data[i] = float64(tv)
		case int64:
			v.Float64Data[i] = float64(tv)
		case int:
			v.Float64Data[i] = float64(tv)
		case int32:
			// The lossless widening #361's shape wanted: MIN(int32_col)
			// OVER a float64-declared window dropped every value here.
			v.Float64Data[i] = float64(tv)
		default:
			v.mismatch(val)
		}
	case TypeString:
		switch tv := val.(type) {
		case string:
			v.BytesData.Set(i, []byte(tv))
		case []byte:
			v.BytesData.Set(i, tv)
		default:
			// Coerce non-string values to string representation
			v.BytesData.Set(i, []byte(fmt.Sprint(val)))
		}
	case TypeBytes:
		// Coerces like TypeString above: any value has a string form, so a
		// bytes destination can always hold it. Deliberately NOT guarded.
		switch tv := val.(type) {
		case []byte:
			v.BytesData.Set(i, tv)
		case string:
			v.BytesData.Set(i, []byte(tv))
		default:
			v.BytesData.Set(i, []byte(fmt.Sprint(val)))
		}
	case TypeIPv4:
		switch tv := val.(type) {
		case string:
			ip := net.ParseIP(tv)
			if ip == nil {
				return
			}
			ip4 := ip.To4()
			if ip4 == nil {
				return
			}
			v.Int64Data[i] = int64(binary.BigEndian.Uint32(ip4))
		case int64:
			v.Int64Data[i] = tv
		case int32:
			v.Int64Data[i] = int64(tv)
		default:
			v.mismatch(val)
		}
	case TypeIPv6:
		switch tv := val.(type) {
		case string:
			ip := net.ParseIP(tv)
			if ip == nil {
				v.BytesData.Set(i, nil)
				return
			}
			v.BytesData.Set(i, []byte(ip.To16()))
		case []byte:
			// Raw 16-byte form, the vector's own storage representation.
			v.BytesData.Set(i, tv)
		default:
			v.mismatch(val)
		}
	case TypeCIDR:
		s, ok := val.(string)
		if !ok {
			v.mismatch(val)
		}
		v.BytesData.Set(i, []byte(s))
	case TypeMAC:
		switch tv := val.(type) {
		case string:
			hw, err := net.ParseMAC(tv)
			if err != nil || len(hw) != 6 {
				return
			}
			var n uint64
			for _, b := range hw {
				n = (n << 8) | uint64(b)
			}
			v.Int64Data[i] = int64(n)
		case int64:
			v.Int64Data[i] = tv
		default:
			v.mismatch(val)
		}
	case TypePort, TypeProtocol:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case float64:
			v.Int32Data[i] = int32(tv)
		default:
			v.mismatch(val)
		}
	case TypeDuration:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		default:
			v.mismatch(val)
		}
	case TypeUUID:
		switch tv := val.(type) {
		case string:
			raw := parseUUID(tv)
			v.BytesData.Set(i, raw)
		case []byte:
			v.BytesData.Set(i, tv)
		default:
			v.mismatch(val)
		}
	case TypeDate:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case string:
			days, ok := parseDateString(tv)
			if !ok {
				// An unparseable or nonexistent calendar date is the type's
				// value-parse failure, exactly as an unparseable IPv4/MAC/UUID
				// is above: store NULL, never the epoch. Reading '2026-02-30'
				// back as 1970-01-01 on the row->batch path (JSON reader,
				// columnar scan) was silent data corruption (#560).
				v.Nulls.SetNull(i)
				return
			}
			v.Int32Data[i] = days
		default:
			v.mismatch(val)
		}
	case TypeDecimal:
		switch tv := val.(type) {
		case Int128:
			v.DecimalData.Data[i] = tv
		case parquet.Decimal128:
			// The parquet row reader's box for a DECIMAL wider than 64
			// bits. It cannot hand back a batch.Int128 — batch imports
			// parquet, not the other way round — so the two-word form
			// crosses the boundary and is re-boxed here. Without this arm
			// the value would fall through to mismatch(), which is the
			// point: the alternative shape was an int64 holding the LOW 64
			// BITS of the unscaled value, a different number returned
			// silently (#419).
			v.DecimalData.Data[i] = Int128{Hi: tv.Hi, Lo: tv.Lo}
		case int64:
			// An INTEGER box is the ALREADY-SCALED unscaled carrier, written
			// verbatim: ADR-0018 §4 says a DECIMAL value IS an unscaled
			// integer plus the column's declared scale, and this arm is the
			// INGEST spelling of that — a writer that has already scaled its
			// value hands it over as an int64 rather than paying for text.
			//
			// It is therefore NOT the arm for a value at scale 0. A set
			// operation's integer arm is such a value, and reading it here
			// divided every integer by 10^scale (1 came back as 0.01,
			// #547/#541); the reconciliation happens at the CALLER, which
			// rewrites the box to decimal text before the arms meet
			// (physical.coerceSetOpArmRows). SetValueChecked — the sibling
			// every value-producing row→batch caller takes — REFUSES an
			// integer box into a DECIMAL column for exactly this reason, so
			// this reinterpretation stays reachable only from ingest.
			v.DecimalData.Data[i] = Int128From(tv)
		case int:
			v.DecimalData.Data[i] = Int128From(int64(tv))
		case int32:
			v.DecimalData.Data[i] = Int128From(int64(tv))
		case float64:
			v.DecimalData.Data[i] = Int128FromFloat64(tv, v.DecimalData.Scale)
		case float32:
			v.DecimalData.Data[i] = Int128FromFloat64(float64(tv), v.DecimalData.Scale)
		case string:
			// SATURATING, because this is the unchecked writer: text with no
			// Int128 at this scale lands on Int128Max and text that is not a
			// number lands on zero. Value-producing callers take
			// SetValueChecked, which reports both (#553, ADR-0024 item 4).
			v.DecimalData.Data[i] = ParseDecimalString(tv, v.DecimalData.Scale)
		default:
			v.mismatch(val)
		}
	case TypeVector:
		if v.VectorDim <= 0 {
			return
		}
		switch tv := val.(type) {
		case []float32:
			v.SetVector(i, tv)
		case []any:
			off := i * v.VectorDim
			for j := 0; j < v.VectorDim && j < len(tv); j++ {
				switch fv := tv[j].(type) {
				case float32:
					v.Float32Data[off+j] = fv
				case float64:
					v.Float32Data[off+j] = float32(fv)
				case int64:
					v.Float32Data[off+j] = float32(fv)
				}
			}
		default:
			v.mismatch(val)
		}
	case TypeArray, TypeMap:
		if v.Child == nil {
			return
		}
		var elems []any
		switch tv := val.(type) {
		case []any:
			elems = tv
		case []map[string]any:
			// parquet-go returns []map[string]any for repeated groups
			elems = make([]any, len(tv))
			for j, m := range tv {
				// Unwrap single-key maps (LIST element wrapper: {"element": value})
				if len(m) == 1 {
					for _, v := range m {
						elems[j] = v
						break
					}
				} else {
					elems[j] = m
				}
			}
		default:
			// The guard matters doubly here: the old silent `return` did
			// not even advance Offsets, so every LATER row of the column
			// read back shifted — the wrong value became someone else's.
			//
			// A bare map[string]any is refused too, MAP vector or not. It
			// is the row-level shape of a MAP AND the box of a ROW, and
			// this function cannot tell them apart: batch.FromRows converts
			// the first at the boundary where the context IS known, and
			// what reaches here is a ROW written into a mis-derived MAP
			// vector — a live stage-DAG defect (#397) whose only current
			// report is this guard.
			v.mismatch(val)
		}
		start := v.Child.Len
		for _, elem := range elems {
			appendToVector(v.Child, elem)
		}
		v.Offsets[i] = int32(start)
		v.Offsets[i+1] = int32(v.Child.Len)
	case TypeRow:
		if v.Children == nil {
			return
		}
		row, ok := val.(map[string]any)
		if !ok {
			// Same double stake as ARRAY/MAP above: the old silent return
			// skipped every child's slot for this row.
			v.mismatch(val)
		}
		for j, child := range v.Children {
			name := ""
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			child.SetValue(i, row[name])
		}
	}
}

// SetValueChecked is SetValue for a caller producing a stored VALUE rather
// than ingesting an already-encoded one.
//
// SetValue's DECIMAL arms answer a conversion they cannot make exactly with
// the nearest thing they can store — the saturated end of the Int128 range
// for text too wide at this scale, zero for text that is not a number, the
// raw carrier for an integer box, and a float64 round trip for a float box.
// Each is right for the caller it was built for (a comparison bound, #462;
// ingest's already-scaled carrier, ADR-0018 §4) and each is a silently wrong
// ROW anywhere else: a 10^30 union arm came back as
// 17014118346046923173168730371.5884105727 (#553) and an integer arm came
// back divided by 10^scale (#547/#541).
//
// So this sibling exists rather than a signature change on SetValue: the
// unchecked writer keeps its callers and its cost, and every value-producing
// row→batch path (FromRowsChecked, and through it the single-process
// set-operation adapter) takes this one. Every DECIMAL box is exact-or-error
// here — text through the checked parser, a float through its shortest
// round-trip spelling, an integer refused outright — and the errors carry
// PostgreSQL's SQLSTATEs: 22003 for a value with no carrier, 22P02 for text
// that names no number (ADR-0024 item 4).
//
// Every other type, and every other box, delegates to SetValue unchanged.
func (v *Vector) SetValueChecked(i int, val any) error {
	if v == nil || v.Type != TypeDecimal || val == nil {
		v.SetValue(i, val)
		return nil
	}
	switch tv := val.(type) {
	case string:
		d, err := ParseDecimalStringChecked(tv, v.DecimalData.Scale)
		if err != nil {
			return err
		}
		v.Nulls.SetValid(i)
		v.DecimalData.Data[i] = d
		return nil
	case int64, int, int32:
		// The ingest reinterpretation (SetValue's int arms) is a CARRIER
		// hand-off, not a conversion, so a caller carrying VALUES must not
		// reach it: at scale 2 the integer 1 would be stored as 0.01. The
		// set-operation adapter rewrites such a box to decimal text before
		// this point (physical.coerceSetOpArmRows), so arriving here with one
		// means the reconciliation did not run — reported rather than stored.
		return sqlerr.New("22003",
			"integer value %v reached a DECIMAL(scale %d) column as a raw unscaled carrier: "+
				"an integer is a value at scale 0 and must be multiplied by 10^%d first "+
				"(ADR-0018 §4, ADR-0024 item 4)",
			tv, v.DecimalData.Scale, v.DecimalData.Scale)
	case float64:
		return v.setCheckedDecimalFloat(i, tv, 64)
	case float32:
		return v.setCheckedDecimalFloat(i, float64(tv), 32)
	}
	v.SetValue(i, val)
	return nil
}

// setCheckedDecimalFloat stores a float box into a DECIMAL column EXACTLY or
// not at all.
//
// SetValue's float arms go through Int128FromFloat64, which multiplies by
// 10^scale in float64 and then converts through int64 — so it loses digits
// past float64's ~16 significant ones and is undefined past 2^63 entirely.
// That is the unchecked writer's contract and it stays. Here the promise is
// exact-or-error (ADR-0024 item 4), so the float is spelled as its SHORTEST
// round-trip decimal text — the unique decimal that reads back as this same
// float, which is also what PostgreSQL prints for it — and resolved through
// the checked text path, which reports 22003 when that decimal has no Int128
// at the column's scale. bitSize picks the float32 or float64 spelling, so a
// real holding 0.1 stores as 0.1 rather than as its 55-digit exact expansion.
//
// A float with no decimal spelling at all (NaN and the infinities) has no
// DECIMAL value either: strconv writes "NaN"/"+Inf"/"-Inf", all three of which
// PostgreSQL's numeric input reads as VALUES, and the checked parser reports
// 22003 for them — a value with no carrier, not an input-syntax error.
// ADR-0024 item 6 makes NaN a comparison literal and never a stored value, so
// refusing is the answer there too (#534).
func (v *Vector) setCheckedDecimalFloat(i int, f float64, bitSize int) error {
	d, err := ParseDecimalStringChecked(strconv.FormatFloat(f, 'f', -1, bitSize), v.DecimalData.Scale)
	if err != nil {
		return err
	}
	v.Nulls.SetValid(i)
	v.DecimalData.Data[i] = d
	return nil
}

// mapEntryRows converts a MAP's row-level Go map into the entry rows the
// vector stores: one ROW per key, carrying the child ROW's own two field
// names so a schema that named them something other than key/value still
// lands in the right children.
//
// Entries come out in KEY ORDER. Go map iteration is randomized, and the
// order is observable: it is the order GetValue hands back, the order a
// comparator sees, and the order a re-write puts on disk. Sorting is what
// makes the same file read back the same way twice — and makes the
// single-process and distributed arms agree on a MAP value at all.
//
// The parquet writer sorts on the same rule (parquet.sortedMapKeys), because
// this vector and that writer are the two ways the same map reaches disk and
// they have to put it there the same way.
func mapEntryRows(child *Vector, m map[string]any) []any {
	keyName, valName := "key", "value"
	if len(child.FieldNames) == 2 {
		keyName, valName = child.FieldNames[0], child.FieldNames[1]
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{keyName: mapKeyValue(child, k), valName: m[k]})
	}
	return out
}

// mapKeyValue coerces a MAP's row-level key to something the key child can
// hold. Row-level keys are always strings — the parquet reader stringifies
// whatever the file carried and a Go map's key is the writer's only source —
// so a numerically-typed key column would otherwise take a string and raise
// the type guard from inside the scan, trading one process-killer for
// another. Every other family (STRING/BYTES/DATE/DECIMAL/the network types/
// UUID) parses its own string form already.
func mapKeyValue(child *Vector, k string) any {
	if len(child.Children) == 0 {
		return k
	}
	switch child.Children[0].Type {
	case TypeInt32, TypeInt64, TypePort, TypeProtocol, TypeDuration, TypeTimestamp:
		n, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil
		}
		return n
	case TypeFloat32, TypeFloat64:
		f, err := strconv.ParseFloat(k, 64)
		if err != nil {
			return nil
		}
		return f
	case TypeBool:
		b, err := strconv.ParseBool(k)
		if err != nil {
			return nil
		}
		return b
	}
	return k
}

// appendToVector appends a single value to a vector, growing its backing storage.
// Used by ARRAY/MAP SetValue to build up the child vector.
func appendToVector(v *Vector, val any) {
	idx := v.Len
	v.Len++
	v.Nulls = v.Nulls.Grow(v.Len)

	// Always grow typed storage (needed even for null to keep indices valid)
	switch v.Type {
	case TypeBool:
		v.BoolData = append(v.BoolData, false)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		v.Int32Data = append(v.Int32Data, 0)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		v.Int64Data = append(v.Int64Data, 0)
	case TypeFloat32:
		v.Float32Data = append(v.Float32Data, 0)
	case TypeFloat64:
		v.Float64Data = append(v.Float64Data, 0)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData.Offsets = append(v.BytesData.Offsets, v.BytesData.Offsets[len(v.BytesData.Offsets)-1])
	case TypeDecimal:
		v.DecimalData.Data = append(v.DecimalData.Data, Int128{})
	case TypeArray, TypeMap:
		v.Offsets = append(v.Offsets, v.Offsets[len(v.Offsets)-1])
	case TypeRow:
		for _, child := range v.Children {
			appendToVector(child, nil)
		}
	}

	if val == nil {
		v.Nulls.SetNull(idx)
		return
	}
	v.SetValue(idx, val)
}

// --- Typed accessors (zero-allocation hot path) ---

// GetInt64 returns the int64 value at position i. Returns (0, false) if null.
func (v *Vector) GetInt64(i int) (int64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Int64Data[i], true
}

// GetFloat64 returns the float64 value at position i. Returns (0, false) if null.
func (v *Vector) GetFloat64(i int) (float64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Float64Data[i], true
}

// GetInt32 returns the int32 value at position i. Returns (0, false) if null.
func (v *Vector) GetInt32(i int) (int32, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Int32Data[i], true
}

// GetFloat32 returns the float32 value at position i. Returns (0, false) if null.
func (v *Vector) GetFloat32(i int) (float32, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Float32Data[i], true
}

// GetBool returns the bool value at position i. Returns (false, false) if null.
func (v *Vector) GetBool(i int) (bool, bool) {
	if v.Nulls.IsNullFast(i) {
		return false, false
	}
	return v.BoolData[i], true
}

// GetString returns the string value at position i. Returns ("", false) if null.
func (v *Vector) GetString(i int) (string, bool) {
	if v.Nulls.IsNullFast(i) {
		return "", false
	}
	return v.BytesData.StringValue(i), true
}

// GetNumericFloat64 returns any numeric column value as float64 without boxing.
// Handles Int32, Int64, Float32, Float64, Timestamp types.
func (v *Vector) GetNumericFloat64(i int) (float64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	switch v.Type {
	case TypeInt64, TypeTimestamp:
		return float64(v.Int64Data[i]), true
	case TypeFloat64:
		return v.Float64Data[i], true
	case TypeInt32:
		return float64(v.Int32Data[i]), true
	case TypeFloat32:
		return float64(v.Float32Data[i]), true
	case TypeDecimal:
		return v.DecimalData.Data[i].ToFloat64(v.DecimalData.Scale), true
	default:
		return 0, false
	}
}

// memBytesMaxDepth bounds the recursion into nested type children so a
// pathological (or cyclic) schema cannot blow the stack while accounting.
const memBytesMaxDepth = 3

// MemBytes returns the heap bytes resident in this vector's backing storage:
// the null bitmap plus the typed data slice, recursing into nested children for
// ARRAY/MAP/ROW. It is the byte-true accounting primitive for the memory
// tracker, replacing the per-type b.Len*48 estimate in EstimateBatchBytes. It
// deliberately omits any operator-specific overhead (e.g. the HashJoin hash
// index charge) — that stays at the call site.
func (v *Vector) MemBytes() int64 {
	return v.memBytes(0)
}

func (v *Vector) memBytes(depth int) int64 {
	if v == nil || depth > memBytesMaxDepth {
		return 0
	}
	if v.Base != nil {
		// View: indices + own bitmap. Base storage is accounted by its
		// owner (build tracker / pinned probe input), never double-charged
		// here — the same rule that keeps shared broadcast-cache vectors
		// single-owner.
		return int64(len(v.Nulls.Words()))*8 + int64(len(v.Indices))*4
	}
	// Null bitmap: stored as []uint64 words, one bit per row.
	size := int64(len(v.Nulls.Words())) * 8 // 8 = sizeof(uint64) word

	switch v.Type {
	case TypeBool:
		size += int64(len(v.BoolData)) // 1 byte per bool
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		size += int64(len(v.Int32Data)) * 4
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		size += int64(len(v.Int64Data)) * 8 // IPv4/MAC are stored as int64
	case TypeFloat32:
		size += int64(len(v.Float32Data)) * 4
	case TypeFloat64:
		size += int64(len(v.Float64Data)) * 8
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		size += v.BytesData.MemBytes()
	case TypeDecimal:
		size += int64(len(v.DecimalData.Data)) * 16 // Int128 = {Hi int64, Lo uint64}
	case TypeVector:
		size += int64(len(v.Float32Data)) * 4 // Len*VectorDim float32s
	case TypeArray, TypeMap:
		size += int64(len(v.Offsets)) * 4 // int32 offsets
		size += v.Child.memBytes(depth + 1)
	case TypeRow:
		for _, c := range v.Children {
			size += c.memBytes(depth + 1)
		}
		for _, n := range v.FieldNames {
			size += int64(len(n)) + 16 // string header (ptr+len) + backing bytes
		}
	}
	return size
}

// String returns a debug representation of the vector.
func (v *Vector) String() string {
	return fmt.Sprintf("Vector{type=%v, len=%d, nulls=%d}", v.Type, v.Len, v.Nulls.NullCount())
}

// FormatValue formats any value for display, producing SQL-like text
// for nested types (arrays, rows, maps).
func FormatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch tv := v.(type) {
	case []float32:
		return formatFloat32Slice(tv)
	case []any:
		return formatArrayValue(tv)
	case map[string]any:
		return formatRowValue(tv)
	case string:
		return tv
	default:
		return fmt.Sprint(v)
	}
}

func formatFloat32Slice(v []float32) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%g", f))
	}
	buf.WriteByte(']')
	return buf.String()
}

func formatArrayValue(arr []any) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, elem := range arr {
		if i > 0 {
			buf.WriteString(", ")
		}
		if elem == nil {
			buf.WriteString("NULL")
		} else if s, ok := elem.(string); ok {
			buf.WriteByte('\'')
			buf.WriteString(s)
			buf.WriteByte('\'')
		} else {
			buf.WriteString(FormatValue(elem))
		}
	}
	buf.WriteByte(']')
	return buf.String()
}

func formatRowValue(row map[string]any) string {
	var buf strings.Builder
	buf.WriteByte('{')
	first := true
	for k, v := range row {
		if !first {
			buf.WriteString(", ")
		}
		first = false
		buf.WriteString(k)
		buf.WriteString(": ")
		if v == nil {
			buf.WriteString("NULL")
		} else if s, ok := v.(string); ok {
			buf.WriteByte('\'')
			buf.WriteString(s)
			buf.WriteByte('\'')
		} else {
			buf.WriteString(FormatValue(v))
		}
	}
	buf.WriteByte('}')
	return buf.String()
}

// formatUUID formats 16 raw bytes as a UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func formatUUID(b []byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
}

// parseUUID parses a UUID string into 16 raw bytes.
// Accepts "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" or "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".
func parseUUID(s string) []byte {
	// Remove dashes
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return nil
	}
	raw := make([]byte, 16)
	_, err := hex.Decode(raw, clean)
	if err != nil {
		return nil
	}
	return raw
}

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// FormatDate formats days-since-epoch as "2006-01-02".
func FormatDate(days int32) string {
	// Delegates so the scan's DATE→STRING coercion and the parquet row
	// reader's cannot drift apart (parquet.CoercibleTo).
	return parquet.FormatDateDays(days)
}

// timestampTextMillis reads a TIMESTAMP's wall-clock text into the epoch
// milliseconds the engine's TIMESTAMP is. It is FormatTimestamp's inverse and
// accepts every spelling parquet.ParseTimestampWallClock does, which is the
// accept-set a stored value already goes through — one reading of a timestamp
// text, not two.
func timestampTextMillis(s string) (int64, bool) {
	t, ok := parquet.ParseTimestampWallClock(s)
	if !ok {
		return 0, false
	}
	return t.UTC().UnixMilli(), true
}

// FormatTimestamp renders epoch MILLISECONDS — the engine's one timestamp
// unit — the way PostgreSQL renders a `timestamp` (OID 1114): UTC,
// "2006-01-02 15:04:05", with a fractional part only when the millisecond
// component is non-zero.
//
// This is the display half of a deliberate split. The COMPUTE half boxes a
// TIMESTAMP column as a bare int64 (ColRef.Eval, pinned by
// TestTemporalColumnBoxingUnchanged) because comparison, arithmetic,
// GROUP BY key serialization, spill codecs and the UPDATE read-modify-write
// path all read it as a number; Vector.GetValue keeps that same int64 for
// exactly those consumers. The two halves agree because they are the same
// value in the same unit — one rendered, one raw — and the conversion
// between them lives here, applied by renderers that still hold the
// column's declared type. Formatting inside GetValue instead would push the
// rendered form into every compute path that shares that boxing.
func FormatTimestamp(ms int64) string {
	t := time.UnixMilli(ms).UTC()
	if ms%1000 == 0 {
		return t.Format("2006-01-02 15:04:05")
	}
	// PostgreSQL TRIMS a fractional second's trailing zeros: `.5` and `.12`,
	// never `.500` and `.120`. Measured live on 17.11 —
	// `timestamp '1996-03-13 14:25:36.5'::text` is `1996-03-13 14:25:36.5`.
	// This engine printed three digits always, on the wire as well as through
	// every expression that renders an instant, because pgwire's send path
	// calls this same function (#544).
	//
	// Go's `.9` verb does exactly that trimming and, unlike a hand-rolled
	// strip, cannot leave a bare point behind.
	return t.Format("2006-01-02 15:04:05.999")
}

// parseDateString parses a DATE string to days since epoch via the shared
// parquet.ParseDateDays — the one accept-set and classification the filter
// path, the writers and the ingest boundary share, which also carries #451's
// civil-days arithmetic (t.Unix(), not the saturating t.Sub) and the int32
// range check. ok is false for an unparseable or nonexistent calendar date;
// the caller stores NULL for it rather than the epoch (#560).
func parseDateString(s string) (int32, bool) {
	d, err := parquet.ParseDateDays(s)
	if err != nil {
		return 0, false
	}
	return d, true
}

// IntStorageType reports whether a column of type t stores its values in an
// integer slice (Int32Data or Int64Data), so that a boxed value of that
// column has an exact int64 STORAGE form.
//
// It is the type set writeIntKeyToColumn and appendTypedIntKey already
// enumerate in exec, stated once here beside the two boxings it reconciles.
func IntStorageType(t TypeID) bool {
	switch t {
	case TypeBool,
		TypeInt32, TypePort, TypeProtocol, TypeDate,
		TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		return true
	}
	return false
}

// KeyStorageInt is the inverse of GetValue's boxing for the types
// IntStorageType names: it answers the int64 a column of type t STORES for
// the boxed value v, whichever of that type's legal boxes v happens to be.
//
// It exists because one value of such a type has more than one box in this
// engine and they are not interchangeable as bytes. GetValue FORMATS the
// three whose storage is not their text — DATE to "2006-01-02", IPv4 to its
// dotted quad, MAC to its colon form — while the aggregate's int-keyed SoA
// path, its migration to the generic map (migrateToGenericMap) and the
// packed-key unpacking all hand back the raw integer. A group key that
// serializes whichever box it is given therefore has TWO identities for one
// value, which is #788: a k-way merge compares bytes, so "14610" and
// "\n2010-01-01" never combined and every DATE group came out twice with the
// right total and the wrong grouping.
//
// The answer is a function of (t, value) and of nothing else, so it lives
// beside GetValue and SetValue — the two boxings it has to agree with — and
// its parses are literally theirs (parseDateString, net.ParseIP, net.ParseMAC).
//
// ok is false for a box that no column of type t can produce (a string that
// is not a date/address, a float where an integer is stored). The caller
// decides what an impossible box means; this never guesses an integer for it,
// because a wrong integer is a wrong GROUP.
func KeyStorageInt(v any, t TypeID) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	case bool:
		if t != TypeBool {
			return 0, false
		}
		if tv {
			return 1, true
		}
		return 0, true
	case string:
		return parseKeyStorageText(tv, t)
	case []byte:
		return parseKeyStorageText(string(tv), t)
	}
	return 0, false
}

// parseKeyStorageText re-reads a DISPLAY form back into storage for the three
// int-stored types GetValue formats. Every other int-stored type boxes as an
// integer, so text for one of them is not a value that column can hold.
func parseKeyStorageText(s string, t TypeID) (int64, bool) {
	switch t {
	case TypeDate:
		d, ok := parseDateString(s)
		if !ok {
			return 0, false
		}
		return int64(d), true
	case TypeIPv4:
		ip := net.ParseIP(s)
		if ip == nil {
			return 0, false
		}
		ip4 := ip.To4()
		if ip4 == nil {
			return 0, false
		}
		return int64(binary.BigEndian.Uint32(ip4)), true
	case TypeMAC:
		hw, err := net.ParseMAC(s)
		if err != nil || len(hw) != 6 {
			return 0, false
		}
		var n uint64
		for _, b := range hw {
			n = (n << 8) | uint64(b)
		}
		return int64(n), true
	}
	return 0, false
}

// --- Nested-aware typed copy primitives ---
//
// These three primitives are the kernel surface that lets Sort/Window
// columnar runs and the run merger carry ARRAY/MAP/ROW columns without
// boxing. They share one contract with BytesColumn: destinations are
// written SEQUENTIALLY (row 0, 1, 2, ...), and null rows still advance
// variable-length bookkeeping (array offsets, row children) — skipping a
// null would shift every later row's elements.

// NewVectorLike returns an empty (zero-row) vector with src's type and
// nested structure: child element types, ROW field names, VECTOR dim and
// DECIMAL scale. Element storage is appended by AppendFrom.
func NewVectorLike(src *Vector) *Vector {
	if src == nil {
		return nil
	}
	v := &Vector{Type: src.Type}
	switch src.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData = NewBytesColumn(0)
	case TypeDecimal:
		v.DecimalData = NewDecimalColumn(0, src.DecimalData.Scale)
	case TypeVector:
		v.VectorDim = src.VectorDim
	case TypeArray, TypeMap:
		v.Offsets = []int32{0}
		v.Child = NewVectorLike(src.Child)
	case TypeRow:
		v.Children = make([]*Vector, len(src.Children))
		for j, ch := range src.Children {
			v.Children[j] = NewVectorLike(ch)
		}
		v.FieldNames = append([]string(nil), src.FieldNames...)
	}
	return v
}

// AppendFrom appends src[si] to dst, growing dst by one row. Typed copy —
// no boxing, no string round-trips — recursive for nested types. dst's
// nested structure must match src's (build it with NewVectorLike).
func (v *Vector) AppendFrom(src *Vector, si int) {
	if src.Base != nil {
		// View source: mirror CopyValueFrom's redirect.
		if src.Nulls.IsNullFast(si) {
			appendToVector(v, nil)
			return
		}
		src, si = src.Base, int(src.Indices[si])
	}
	idx := v.Len
	v.Len++
	v.Nulls = v.Nulls.Grow(v.Len)
	isNull := src.Nulls.IsNull(si)
	if isNull {
		v.Nulls.SetNull(idx)
	}
	switch v.Type {
	case TypeBool:
		var x bool
		if !isNull {
			x = src.BoolData[si]
		}
		v.BoolData = append(v.BoolData, x)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		var x int32
		if !isNull {
			x = src.Int32Data[si]
		}
		v.Int32Data = append(v.Int32Data, x)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		var x int64
		if !isNull {
			x = src.Int64Data[si]
		}
		v.Int64Data = append(v.Int64Data, x)
	case TypeFloat32:
		var x float32
		if !isNull {
			x = src.Float32Data[si]
		}
		v.Float32Data = append(v.Float32Data, x)
	case TypeFloat64:
		var x float64
		if !isNull {
			x = src.Float64Data[si]
		}
		v.Float64Data = append(v.Float64Data, x)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		if isNull {
			v.BytesData.Offsets = append(v.BytesData.Offsets, uint32(len(v.BytesData.Data)))
		} else {
			start, end := src.BytesData.Offsets[si], src.BytesData.Offsets[si+1]
			v.BytesData.Data = append(v.BytesData.Data, src.BytesData.Data[start:end]...)
			v.BytesData.Offsets = append(v.BytesData.Offsets, uint32(len(v.BytesData.Data)))
		}
	case TypeDecimal:
		var x Int128
		if !isNull {
			x = src.DecimalData.Data[si]
		}
		v.DecimalData.Data = append(v.DecimalData.Data, x)
	case TypeVector:
		dim := v.VectorDim
		if dim > 0 {
			off := len(v.Float32Data)
			v.Float32Data = append(v.Float32Data, make([]float32, dim)...)
			if !isNull {
				copy(v.Float32Data[off:off+dim], src.Float32Data[si*dim:(si+1)*dim])
			}
		}
	case TypeArray, TypeMap:
		if v.Child == nil && src.Child != nil {
			v.Child = NewVectorLike(src.Child)
		}
		last := v.Offsets[len(v.Offsets)-1]
		if isNull || src.Child == nil {
			v.Offsets = append(v.Offsets, last)
		} else {
			start, end := src.Offsets[si], src.Offsets[si+1]
			for j := start; j < end; j++ {
				v.Child.AppendFrom(src.Child, int(j))
			}
			v.Offsets = append(v.Offsets, last+(end-start))
		}
	case TypeRow:
		if v.Children == nil && src.Children != nil {
			v.Children = make([]*Vector, len(src.Children))
			for j, ch := range src.Children {
				v.Children[j] = NewVectorLike(ch)
			}
			v.FieldNames = append([]string(nil), src.FieldNames...)
		}
		for j, child := range v.Children {
			if isNull || j >= len(src.Children) {
				appendToVector(child, nil)
			} else {
				child.AppendFrom(src.Children[j], si)
			}
		}
	}
}

// CopyValueFrom writes src[si] into position di of dst using typed access —
// no boxing, no string round-trips — for every column type including nested
// ARRAY/MAP/ROW. Fixed-width slots are indexed; variable-length storage
// (bytes data, array child elements, lazily-created row children) is
// appended, so writes must be SEQUENTIAL per column (di = 0, 1, 2, ...) —
// the same contract BytesColumn.Set has always had. Null source rows still
// advance offsets and children; skipping them would shift every later row.
//
// Destination shape is flexible per level: parent slots may be
// pre-allocated (NewRecordBatch with full nested schema) or append-built
// (NewVectorLike); ROW children handle both — indexed writes when
// pre-allocated, appends when built lazily.
func (v *Vector) CopyValueFrom(di int, src *Vector, si int) {
	if src.Base != nil {
		// View source: an own-null row becomes a null write (its index value
		// is meaningless); otherwise copy from the base row it references.
		if src.Nulls.IsNullFast(si) {
			v.WriteNullAt(di)
			return
		}
		src, si = src.Base, int(src.Indices[si])
	}
	isNull := src.Nulls.IsNull(si)
	if isNull {
		v.Nulls.SetNull(di)
	} else {
		v.Nulls.SetValid(di)
	}
	switch v.Type {
	case TypeBool:
		if !isNull {
			v.BoolData[di] = src.BoolData[si]
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		if !isNull {
			v.Int32Data[di] = src.Int32Data[si]
		}
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		if !isNull {
			v.Int64Data[di] = src.Int64Data[si]
		}
	case TypeFloat32:
		if !isNull {
			v.Float32Data[di] = src.Float32Data[si]
		}
	case TypeFloat64:
		if !isNull {
			v.Float64Data[di] = src.Float64Data[si]
		}
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		if isNull {
			v.BytesData.Set(di, nil)
		} else {
			v.BytesData.SetFrom(di, &src.BytesData, si)
		}
	case TypeDecimal:
		if !isNull {
			v.DecimalData.Data[di] = src.DecimalData.Data[si]
		}
	case TypeVector:
		dim := src.VectorDim
		if v.VectorDim == 0 {
			v.VectorDim = dim
		}
		if dim > 0 {
			need := (di + 1) * dim
			if len(v.Float32Data) < need {
				v.Float32Data = append(v.Float32Data, make([]float32, need-len(v.Float32Data))...)
			}
			if !isNull {
				copy(v.Float32Data[di*dim:(di+1)*dim], src.Float32Data[si*dim:(si+1)*dim])
			}
		}
	case TypeArray, TypeMap:
		if v.Child == nil && src.Child != nil {
			v.Child = NewVectorLike(src.Child)
		}
		var base int32
		if v.Child != nil {
			base = int32(v.Child.Len)
		}
		v.Offsets[di] = base
		if isNull || src.Child == nil {
			v.Offsets[di+1] = base
			return
		}
		start, end := src.Offsets[si], src.Offsets[si+1]
		for j := start; j < end; j++ {
			v.Child.AppendFrom(src.Child, int(j))
		}
		v.Offsets[di+1] = base + (end - start)
	case TypeRow:
		if v.Children == nil && src.Children != nil {
			v.Children = make([]*Vector, len(src.Children))
			for j, ch := range src.Children {
				v.Children[j] = NewVectorLike(ch)
			}
			v.FieldNames = append([]string(nil), src.FieldNames...)
		}
		for j, child := range v.Children {
			srcOK := !isNull && j < len(src.Children)
			if child.Len > di {
				// Pre-allocated parallel child (NewRecordBatch with nested
				// schema): indexed write.
				if srcOK {
					child.CopyValueFrom(di, src.Children[j], si)
				} else {
					child.WriteNullAt(di)
				}
			} else {
				// Append-built child (NewVectorLike): grow by one.
				if srcOK {
					child.AppendFrom(src.Children[j], si)
				} else {
					appendToVector(child, nil)
				}
			}
		}
	}
}

// WriteNullAt writes a null into position di of a pre-allocated vector,
// advancing variable-length bookkeeping (bytes offsets, array offsets, row
// children) so later sequential writes stay aligned. This is THE null-write
// primitive for indexed sequential writers — any writer that sets the null
// bit without advancing these slots corrupts every later row in the column.
func (v *Vector) WriteNullAt(di int) {
	v.Nulls.SetNull(di)
	switch v.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData.Set(di, nil)
	case TypeArray, TypeMap:
		var base int32
		if v.Child != nil {
			base = int32(v.Child.Len)
		}
		v.Offsets[di] = base
		v.Offsets[di+1] = base
	case TypeRow:
		for _, child := range v.Children {
			if child.Len > di {
				child.WriteNullAt(di)
			} else {
				appendToVector(child, nil)
			}
		}
	}
}

// SetComputedChecked is SetValueChecked for a caller whose value came out of an
// EXPRESSION rather than off a wire or a file.
//
// The two differ over exactly one box: an INTEGER. SetValueChecked refuses one
// into a DECIMAL column because its callers are row→batch adapters, where an
// integer box is the ALREADY-SCALED carrier of ADR-0018 §4 and storing it as a
// value would divide it by 10^scale (#547/#541). An expression has no such
// spelling: `expr.ColRef` over a DECIMAL column boxes the value's rendered
// TEXT, exact arithmetic boxes text, and the only way an integer reaches a
// DECIMAL output vector is as a genuine value at scale 0 — the integer branch
// of a choice construct PostgreSQL types numeric (#695).
//
// So this sibling exists rather than a widening of SetValueChecked: the row
// adapter keeps its refusal, and the expression sites (exec.Project,
// physical.aggPreProject and expr.EvalDecimalInto) take this one. It is also
// what makes the box rule DRIFT-PROOF. expr.decimalChoiceArm classifies arms
// by node kind to compute the result TYPE, and a kind it has not learned yet
// makes the fold decline — which used to mean the integer box met the DECIMAL
// vector the PLAN had already allocated and the query died with a 22003 for a
// value PostgreSQL answers (`CASE WHEN … THEN d ELSE CAST(i AS BIGINT) END`).
// The store no longer depends on that classification being complete.
//
// The scaling is checked: an integer too large to carry at this scale is
// 22003, never a wrapped number.
//
// Two limits it shares with SetValueChecked, both recorded rather than fixed
// because no SQL surface reaches either today:
//
//   - A box of a type a DECIMAL column cannot take at all — a bool, a []byte —
//     falls through to SetValue, whose mismatch() PANICS. The query boundary
//     recovers it, so a client sees an internal error rather than PostgreSQL's
//     42804 datatype_mismatch. Nothing in the SQL layer produces such a box for
//     a DECIMAL output: the type fold declines for every non-numeric arm, so
//     the vector would not be a DECIMAL one.
//   - Neither writer enforces the DECLARED PRECISION, only the scale, because
//     batch.DecimalColumn carries Scale and no precision. So a value inside the
//     Int128 but past the type's own 10^p band is stored:
//     `GREATEST(numeric(38,30), 100000000::bigint)` writes 39 digits under a
//     type capped at 38. ADR-0024 item 4 makes the declared precision the bound
//     that matters, and the set-operation coercion is the only door that
//     currently enforces it (physical.setOpCheckedDecimalText).
func (v *Vector) SetComputedChecked(i int, val any) error {
	if v == nil || v.Type != TypeDecimal || val == nil {
		return v.SetValueChecked(i, val)
	}
	var n int64
	switch tv := val.(type) {
	case int64:
		n = tv
	case int:
		n = int64(tv)
	case int32:
		n = int64(tv)
	default:
		return v.SetValueChecked(i, val)
	}
	d, ok := Int128From(n).MulPow10(v.DecimalData.Scale)
	if !ok {
		return sqlerr.New("22003",
			"integer value %d has no exact DECIMAL at scale %d",
			n, v.DecimalData.Scale)
	}
	v.Nulls.SetValid(i)
	v.DecimalData.Data[i] = d
	return nil
}
