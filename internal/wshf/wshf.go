// Package wshf owns shuffle magics/envelopes and the ONE bounds-checked decoder
// for worker file/stream/pread and coordinator inline consumers (#422).
// Preserve column order, per-file schema, DECIMAL scale/precision and per-chunk
// row/null/data framing; the complete wire layout is in the design.
// The writer stays in internal/worker for batch gather/views and WIDX footer.
// All reads use Cursor: malformed or short untrusted payloads return errors,
// never panic in a coordinator decode goroutine.
// See docs/internals/wshf-shared-wire-decoder.md for the design.
package wshf

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Wire magics. These four-byte constants ARE the wire contract (ADR-0010):
// MagicWSHF is the raw payload, MagicWSHC an s2 stream of it, MagicWSHZ a
// zstd stream of it (docs/design/exchange-zstd-wire.md).
var (
	MagicWSHF = [4]byte{'W', 'S', 'H', 'F'}
	MagicWSHC = [4]byte{'W', 'S', 'H', 'C'}
	MagicWSHZ = [4]byte{'W', 'S', 'H', 'Z'}
)

// Codec identifies the envelope around a WSHF payload.
type Codec uint8

const (
	CodecNone Codec = iota // plain WSHF
	CodecS2                // WSHC: s2 stream of the WSHF bytes
	CodecZstd              // WSHZ: zstd stream of the WSHF bytes
)

// CodecForMagic maps a 4-byte magic to its codec. ok=false means the
// payload is not a shuffle format at all (e.g. parquet).
func CodecForMagic(magic [4]byte) (Codec, bool) {
	switch magic {
	case MagicWSHF:
		return CodecNone, true
	case MagicWSHC:
		return CodecS2, true
	case MagicWSHZ:
		return CodecZstd, true
	default:
		return CodecNone, false
	}
}

// IsShuffleFormat reports whether data starts with any shuffle magic.
func IsShuffleFormat(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	_, ok := CodecForMagic([4]byte{data[0], data[1], data[2], data[3]})
	return ok
}

// Plausibility ceilings. A length field is not a promise: these bound what
// a header may claim before the decoder allocates or skips by it. They are
// far above anything the writer emits (2048-row batches, engine schemas)
// and far below anything that would exhaust memory on a corrupt field.
const (
	MaxCols     = 1 << 12
	MaxNameLen  = 1 << 12
	MaxRows     = 1 << 26
	MaxBytesLen = 1 << 31
)

// HeaderLen is the smallest possible header: magic + NumChunks + NumCols.
const HeaderLen = 10

// Sentinels returned by FixedTypeLen for the two classes whose byte count
// is not a function of the row count.
const (
	// LenBytes: [dataLen u32][data][numRows × u32 end offsets].
	LenBytes = -1
	// LenContainer: [payloadLen u32][payload]. The payload is
	// self-describing (batch.EncodeContainerColumn) and the walk skips it
	// whole — ARRAY/ROW/MAP/VECTOR have no per-row width at all.
	LenContainer = -2
)

// FixedTypeLen returns the exact payload byte length for fixed-width
// shuffle types, or one of the sentinels above for the variable-length
// classes. Shared by the decoder, the streaming stage walk and the
// index-mode extent validation so the three cannot diverge.
func FixedTypeLen(typ parquet.TypeID, numRows int) (int, error) {
	switch typ {
	case parquet.TypeBool:
		return (numRows + 7) / 8, nil
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return numRows * 4, nil
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return numRows * 8, nil
	case parquet.TypeFloat32:
		return numRows * 4, nil
	case parquet.TypeFloat64:
		return numRows * 8, nil
	case parquet.TypeDecimal:
		return numRows * 16, nil
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		return LenBytes, nil
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
		return LenContainer, nil
	default:
		return 0, fmt.Errorf("unsupported shuffle type %v", typ)
	}
}
