package catalog

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// AddValueToHLL hashes a value according to its declared parquet type
// and inserts the hash into the sketch.
//
// Shared between the ingest path (which receives raw map[string]any
// rows pre-write) and the ANALYZE path (which decodes parquet rows
// post-write). Both produce hash-compatible HLLs that the catalog
// can merge.
//
// Canonical encodings:
//   - integer/temporal types → 8-byte LE of int64 representation
//   - float32/float64 → 8-byte LE of math.Float64bits
//   - bool → single byte 0/1
//   - string/bytes/network-id types → raw bytes
//   - timestamp → UnixMilli (time.Time) or int64 (raw)
//   - duration → nanoseconds
//
// The encoding intentionally normalizes int32 and int64 to the same
// byte stream so a column widened post-write hashes identically.
func AddValueToHLL(h *HLL, v any, t parquet.TypeID) {
	if h == nil || v == nil {
		return
	}
	switch t {
	case parquet.TypeBool:
		b := byte(0)
		if bv, ok := v.(bool); ok && bv {
			b = 1
		}
		h.AddBytes([]byte{b})
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeDate,
		parquet.TypePort, parquet.TypeProtocol:
		h.AddInt64(asInt64(v))
	case parquet.TypeFloat32, parquet.TypeFloat64:
		bits := math.Float64bits(asFloat64(v))
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], bits)
		h.AddBytes(buf[:])
	case parquet.TypeTimestamp:
		switch tv := v.(type) {
		case time.Time:
			h.AddInt64(tv.UnixMilli())
		default:
			h.AddInt64(asInt64(v))
		}
	case parquet.TypeDuration:
		switch tv := v.(type) {
		case time.Duration:
			h.AddInt64(int64(tv))
		default:
			h.AddInt64(asInt64(v))
		}
	case parquet.TypeString, parquet.TypeBytes,
		parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR,
		parquet.TypeMAC, parquet.TypeUUID, parquet.TypeDecimal:
		switch sv := v.(type) {
		case string:
			h.AddBytes([]byte(sv))
		case []byte:
			h.AddBytes(sv)
		default:
			h.AddBytes([]byte(fmt.Sprintf("%v", v)))
		}
	default:
		h.AddBytes([]byte(fmt.Sprintf("%v", v)))
	}
}

// IsHLLSupportedType reports whether the column type is one we collect
// HLL sketches for. Returns false for nested types whose "value" isn't a
// single scalar.
func IsHLLSupportedType(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap:
		return false
	}
	return true
}

func asInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int32:
		return int64(tv)
	case int:
		return int64(tv)
	case uint64:
		return int64(tv)
	case uint32:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	case bool:
		if tv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func asFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int32:
		return float64(tv)
	case int:
		return float64(tv)
	default:
		return 0
	}
}
