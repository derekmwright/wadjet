package catalog

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

// HLL implements a HyperLogLog++ sketch sized for catalog use. With 2^14
// registers it has ~0.8% standard error on cardinality estimates from
// thousands up to billions of distinct values, using 16 KB per sketch.
// That's plenty cheap to persist on every column's catalog entry and
// gives the planner reliable NDV for join-order decisions and
// dynamic-filter eligibility.
//
// Wire format: a one-byte version (1) followed by the 16384 register
// bytes. Merge is byte-wise max across two sketches.

const (
	hllPrecision  = 14
	hllRegisters  = 1 << hllPrecision // 16384
	hllMask       = hllRegisters - 1
	hllVersion    = 1
)

// HLL is a fixed-size HyperLogLog++ sketch. Zero value is a valid empty
// sketch; use Add to insert hashed values.
type HLL struct {
	registers [hllRegisters]uint8
}

// Add inserts a value's hash into the sketch.
func (h *HLL) Add(hash uint64) {
	idx := hash & hllMask
	w := hash >> hllPrecision
	// rho = position of the leftmost 1-bit in w (1-indexed). For a
	// 64-precision hash and precision p, w has 64-p bits, so the max
	// rho is 64-p+1 = 51 for p=14.
	rho := uint8(bits.LeadingZeros64(w<<hllPrecision) + 1)
	if rho > h.registers[idx] {
		h.registers[idx] = rho
	}
}

// AddBytes hashes b and inserts it.
func (h *HLL) AddBytes(b []byte) { h.Add(xxhash.Sum64(b)) }

// AddInt64 hashes a uint64 representation of v and inserts it.
func (h *HLL) AddInt64(v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	h.Add(xxhash.Sum64(buf[:]))
}

// Estimate returns the approximate cardinality.
func (h *HLL) Estimate() int64 {
	if h == nil {
		return 0
	}
	const m = float64(hllRegisters)
	alpha := 0.7213 / (1 + 1.079/m)
	sum := 0.0
	zeros := 0
	for _, r := range h.registers {
		sum += 1.0 / math.Pow(2, float64(r))
		if r == 0 {
			zeros++
		}
	}
	raw := alpha * m * m / sum
	// Small-range correction: linear counting when many registers
	// haven't seen a hash yet.
	if zeros > 0 && raw <= 2.5*m {
		raw = m * math.Log(m/float64(zeros))
	}
	return int64(math.Round(raw))
}

// Merge takes the byte-wise max of the two sketches. Equivalent to
// computing a sketch over the union of inserted values.
func (h *HLL) Merge(o *HLL) {
	if h == nil || o == nil {
		return
	}
	for i, r := range o.registers {
		if r > h.registers[i] {
			h.registers[i] = r
		}
	}
}

// Encode writes the sketch to w in version-1 format.
func (h *HLL) Encode(w io.Writer) error {
	if _, err := w.Write([]byte{hllVersion}); err != nil {
		return err
	}
	_, err := w.Write(h.registers[:])
	return err
}

// DecodeHLL reads a sketch in version-1 format.
func DecodeHLL(r io.Reader) (*HLL, error) {
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("hll: read header: %w", err)
	}
	if hdr[0] != hllVersion {
		return nil, fmt.Errorf("hll: unsupported version %d", hdr[0])
	}
	out := &HLL{}
	if _, err := io.ReadFull(r, out.registers[:]); err != nil {
		return nil, fmt.Errorf("hll: read registers: %w", err)
	}
	return out, nil
}

// HLLBytes returns the serialized form, suitable for catalog persistence.
func (h *HLL) Bytes() []byte {
	buf := make([]byte, 1+hllRegisters)
	buf[0] = hllVersion
	copy(buf[1:], h.registers[:])
	return buf
}

// HLLFromBytes parses a sketch from its serialized form. Returns nil on
// any error (corrupt bytes, wrong version) — callers fall back to the
// heuristic NDV path.
func HLLFromBytes(b []byte) *HLL {
	if len(b) != 1+hllRegisters || b[0] != hllVersion {
		return nil
	}
	out := &HLL{}
	copy(out.registers[:], b[1:])
	return out
}
