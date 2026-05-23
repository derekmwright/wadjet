package distributed

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Dynamic-filter sideband artifact wire format. Each build-scan task uploads
// one blob per FilterID; the coordinator unions partials, then either
// inlines the result in OpSpec.DynamicFilters or re-stages the unioned blob.
//
// Layout (little-endian):
//
//	[4]   magic "WDF1"
//	[1]   version (1)
//	[1]   key_type_code (0=int32, 1=int64, 2=date)
//	[2]   reserved
//	[8]   row_count (int64)
//	[8]   min (int64)
//	[8]   max (int64)
//	[1]   has_range
//	[7]   reserved
//	[8]   bloom_words (uint64)
//	[8*N] bloom data (uint64 little-endian)
//
// Header is 48 bytes + 8*N for the bloom. Compact, no reflection, easy to
// version forward (the next byte after magic is `version`).

const (
	dfMagic   = "WDF1"
	dfVersion = 1

	dfKeyTypeInt32 = 0
	dfKeyTypeInt64 = 1
	dfKeyTypeDate  = 2

	dfHeaderSize = 48
)

// DynamicFilterArtifact is the on-wire form of one filter's stats. Used for
// both per-task partials (sized to BloomBits, may be sparse) and the
// coordinator-unioned final (same size, populated bits ORed across all
// partials).
type DynamicFilterArtifact struct {
	FilterID  string
	KeyType   string
	HasRange  bool
	Min, Max  int64
	RowCount  int64
	Bloom     []uint64
	BloomMask uint64
}

func keyTypeCode(t string) (uint8, error) {
	switch t {
	case "int32":
		return dfKeyTypeInt32, nil
	case "int64":
		return dfKeyTypeInt64, nil
	case "date":
		return dfKeyTypeDate, nil
	default:
		return 0, fmt.Errorf("dynamic-filter: unsupported key_type %q", t)
	}
}

func keyTypeFromCode(c uint8) (string, error) {
	switch c {
	case dfKeyTypeInt32:
		return "int32", nil
	case dfKeyTypeInt64:
		return "int64", nil
	case dfKeyTypeDate:
		return "date", nil
	default:
		return "", fmt.Errorf("dynamic-filter: unknown key_type_code %d", c)
	}
}

// EncodeDynamicFilterArtifact writes the artifact to w in WDF1 format.
// FilterID is NOT part of the payload — it's carried out-of-band in the
// upload key + partial ref, so multiple filters share the same wire codec.
func EncodeDynamicFilterArtifact(w io.Writer, a *DynamicFilterArtifact) error {
	code, err := keyTypeCode(a.KeyType)
	if err != nil {
		return err
	}
	hdr := make([]byte, dfHeaderSize)
	copy(hdr[0:4], dfMagic)
	hdr[4] = dfVersion
	hdr[5] = code
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(a.RowCount))
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(a.Min))
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(a.Max))
	if a.HasRange {
		hdr[32] = 1
	}
	binary.LittleEndian.PutUint64(hdr[40:48], uint64(len(a.Bloom)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	buf := make([]byte, 8)
	for _, word := range a.Bloom {
		binary.LittleEndian.PutUint64(buf, word)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// DecodeDynamicFilterArtifact reads a WDF1-format artifact from r. The
// caller supplies the FilterID separately (see comment on Encode).
func DecodeDynamicFilterArtifact(r io.Reader) (*DynamicFilterArtifact, error) {
	hdr := make([]byte, dfHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("dynamic-filter: read header: %w", err)
	}
	if string(hdr[0:4]) != dfMagic {
		return nil, fmt.Errorf("dynamic-filter: bad magic %q (want %q)", string(hdr[0:4]), dfMagic)
	}
	if hdr[4] != dfVersion {
		return nil, fmt.Errorf("dynamic-filter: unsupported version %d", hdr[4])
	}
	kt, err := keyTypeFromCode(hdr[5])
	if err != nil {
		return nil, err
	}
	a := &DynamicFilterArtifact{
		KeyType:  kt,
		RowCount: int64(binary.LittleEndian.Uint64(hdr[8:16])),
		Min:      int64(binary.LittleEndian.Uint64(hdr[16:24])),
		Max:      int64(binary.LittleEndian.Uint64(hdr[24:32])),
		HasRange: hdr[32] != 0,
	}
	nWords := binary.LittleEndian.Uint64(hdr[40:48])
	if nWords > 1<<25 { // 256 MiB safety cap
		return nil, fmt.Errorf("dynamic-filter: implausibly large bloom (%d words)", nWords)
	}
	if nWords > 0 {
		a.Bloom = make([]uint64, nWords)
		buf := make([]byte, 8)
		for i := range a.Bloom {
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, fmt.Errorf("dynamic-filter: read bloom[%d]: %w", i, err)
			}
			a.Bloom[i] = binary.LittleEndian.Uint64(buf)
		}
		a.BloomMask = uint64(nWords - 1)
	}
	return a, nil
}
