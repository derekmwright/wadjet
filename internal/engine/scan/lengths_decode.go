package scan

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Lengths-only column decode — the scan half of the offsets-shape
// evaluation class.
//
// When the planner can prove that every use of a byte-array column in the
// whole plan is a SHAPE use — LENGTH()/octet_length()/bit_length(), IS
// [NOT] NULL, a comparison against the empty string, COUNT(col) — the
// column's bytes are never read. Decoding it in full still pays: the
// dictionary gather, the arena growth, and the per-page BulkSet memcpy.
// ClickBench Q28 (AVG(LENGTH(URL)) ... GROUP BY CounterID) materializes
// ~9 GB of URL bytes for lengths that are already sitting in the
// dictionary offsets and the PLAIN length prefixes.
//
// readColumnNativeLengths walks exactly the same page structure the full
// decoder walks but writes only offsets: Offsets[i+1] = Offsets[i] + len_i,
// with Data left empty and BytesColumn.ShapeOnly set. Nulls flow through
// the definition levels exactly as they do in the full decode, so
// LENGTH(NULL) stays NULL rather than becoming 0.
//
// Correctness net: a shape-only column that reaches a VALUE consumer
// panics at BytesColumn.Value with a precise diagnosis instead of
// returning a wrong answer. The planner analysis
// (internal/planner/logical/shape_only_columns.go) is conservative — any
// use it cannot classify, and any plan shape it does not fully understand,
// leaves the column on the full decode.
var lengthsOnlyToggle = optswitch.Register("lengths-only-decode", "WADJET_LENGTHS_ONLY_DECODE",
	"decode shape-only byte-array scan columns as lengths, never materializing values")

// LengthsOnlyDecodeOn reports whether lengths-only column decode is enabled.
func LengthsOnlyDecodeOn() bool { return lengthsOnlyToggle.On() }

// SetLengthsOnlyDecodeForTest flips the kill switch and returns its previous
// value. Test-only: production reads the env var once at Register time.
func SetLengthsOnlyDecodeForTest(on bool) bool { return lengthsOnlyToggle.Set(on) }

// LengthsOnlyColumnDecodes counts column chunks decoded as lengths. Tests
// assert engagement with it — a suite that never takes the path proves
// nothing about it.
var LengthsOnlyColumnDecodes atomic.Int64

// readColumnNativeLengths decodes a byte-array column's per-row lengths into
// vec.BytesData.Offsets without copying any value bytes. Eligibility is the
// same flat-byte-array-leaf test the sel path uses (selEligibleLeaf).
//
// Failure contract matches the full and sel decoders: every dictionary index
// dereferenced is bounds-checked and truncated pages stop early with the
// remaining offsets closed out; corrupt files error, never panic.
func readColumnNativeLengths(vec *batch.Vector, fr *pqt.FileReader, rgIdx, colIdx, numRows int) error {
	LengthsOnlyColumnDecodes.Add(1)
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return fmt.Errorf("column %d not found in row group %d", colIdx, rgIdx)
	}
	defer pr.Close()
	scr := colReadScratchPool.Get().(*colReadScratch)
	pr.SeedScratch(scr.def, scr.idx)
	defer func() {
		scr.def, scr.idx = pr.TakeScratch()
		colReadScratchPool.Put(scr)
	}()

	maxDefLevel := int32(0)
	if leaves := fr.Leaves(); colIdx < len(leaves) {
		maxDefLevel = int32(leaves[colIdx].MaxDefLevel)
	}

	dict, err := pr.NextDictionary()
	if err != nil {
		return fmt.Errorf("reading dictionary: %w", err)
	}
	var dictOffs []uint32
	if dict != nil {
		_, dictOffs = dict.Data.ByteArray()
	}

	bd := &vec.BytesData
	// A pooled vector arrives with its arena reset but non-nil; the arena
	// must stay empty for the ShapeOnly contract to hold.
	bd.Data = bd.Data[:0]
	bd.ShapeOnly = true
	if len(bd.Offsets) > 0 {
		bd.Offsets[0] = 0
	}

	offset := 0
	for {
		page, err := pr.NextPage()
		if err != nil {
			return fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}
		pageRows := page.NumValues
		if pageRows == 0 {
			page.Release()
			continue
		}
		defLevels := page.DefinitionLevels
		hasNulls := page.NumNulls > 0 && defLevels != nil

		perr := func() error {
			if page.IsDictEncoded() {
				if dict == nil {
					return fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
				}
				indices := page.Data.Int32()
				numVals := len(dictOffs) - 1
				lenAt := func(vi int) (int, error) {
					if vi >= len(indices) {
						return 0, fmt.Errorf("dictionary page: value %d beyond %d indices", vi, len(indices))
					}
					idx := indices[vi]
					if uint(idx) >= uint(numVals) {
						return 0, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, numVals)
					}
					return int(dictOffs[idx+1] - dictOffs[idx]), nil
				}
				return lengthsCopyPage(vec, offset, pageRows, defLevels, maxDefLevel, hasNulls, lenAt)
			}
			rawData, offs := page.Data.ByteArray()
			if offs != nil {
				lenAt := func(vi int) (int, error) {
					if vi+1 >= len(offs) {
						return 0, fmt.Errorf("byte-array page: value %d beyond %d offsets", vi, len(offs))
					}
					return int(offs[vi+1] - offs[vi]), nil
				}
				return lengthsCopyPage(vec, offset, pageRows, defLevels, maxDefLevel, hasNulls, lenAt)
			}
			// PLAIN raw length-prefixed fallback: lengths are only
			// discoverable by walking the prefixes — which is all this path
			// does anyway, at zero copy cost.
			return lengthsFromRawPrefixes(vec, offset, pageRows, defLevels, maxDefLevel, hasNulls, rawData)
		}()
		page.Release()
		if perr != nil {
			return perr
		}
		offset += pageRows
	}
	// Trailing rows past the last page (schema-evolution short chunks keep
	// the full reader's leave-as-empty behavior): close out the offsets.
	cur := uint32(0)
	if offset < numRows && offset < len(bd.Offsets) {
		cur = bd.Offsets[offset]
	}
	for j := offset; j < numRows; j++ {
		bd.Offsets[j+1] = cur
	}
	return nil
}

// lengthsCopyPage writes one page's per-row lengths given random access to
// the length of each dense (non-null) value. Non-null pages index values by
// row; null-bearing pages walk defLevels to maintain the dense value cursor.
func lengthsCopyPage(vec *batch.Vector, offset, pageRows int, defLevels []int32, maxDefLevel int32, hasNulls bool, lenAt func(int) (int, error)) error {
	bd := &vec.BytesData
	cur := bd.Offsets[offset]
	if !hasNulls {
		for i := 0; i < pageRows; i++ {
			l, err := lenAt(i)
			if err != nil {
				return err
			}
			cur += uint32(l)
			bd.Offsets[offset+i+1] = cur
		}
		return nil
	}
	if len(defLevels) < pageRows {
		return fmt.Errorf("byte-array page: %d definition levels for %d rows", len(defLevels), pageRows)
	}
	valIdx := 0
	for i := 0; i < pageRows; i++ {
		row := offset + i
		if defLevels[i] == maxDefLevel {
			l, err := lenAt(valIdx)
			if err != nil {
				return err
			}
			cur += uint32(l)
			valIdx++
		} else {
			vec.Nulls.SetNull(row)
		}
		bd.Offsets[row+1] = cur
	}
	return nil
}

// lengthsFromRawPrefixes handles the PLAIN length-prefixed layout. Truncated
// pages stop early (mirroring the full decoder) with the remaining offsets
// closed out so the column stays monotonic.
func lengthsFromRawPrefixes(vec *batch.Vector, offset, pageRows int, defLevels []int32, maxDefLevel int32, hasNulls bool, rawData []byte) error {
	bd := &vec.BytesData
	cur := bd.Offsets[offset]
	pos := 0
	written := offset
	for i := 0; i < pageRows; i++ {
		row := offset + i
		isVal := true
		if hasNulls {
			if i >= len(defLevels) {
				return fmt.Errorf("byte-array page: %d definition levels for %d rows", len(defLevels), pageRows)
			}
			isVal = defLevels[i] == maxDefLevel
		}
		if isVal {
			if pos+4 > len(rawData) {
				break // truncated page: full decoder tolerates, stops early
			}
			length := int(binary.LittleEndian.Uint32(rawData[pos:]))
			pos += 4
			if length < 0 || pos+length > len(rawData) {
				return fmt.Errorf("byte-array page: value at %d overruns page (%d bytes)", pos, length)
			}
			cur += uint32(length)
			pos += length
		} else {
			vec.Nulls.SetNull(row)
		}
		bd.Offsets[row+1] = cur
		written = row + 1
	}
	for j := written; j < offset+pageRows; j++ {
		bd.Offsets[j+1] = cur
	}
	return nil
}
