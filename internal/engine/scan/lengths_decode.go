package scan

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Lengths-only decode applies only when ALL uses need byte shape: octet/bit
// length, NULL tests, empty-string comparison or COUNT; text length needs runes.
// Write cumulative byte offsets with empty Data and ShapeOnly set; preserve
// NULL through definition levels, never turn NULL length into zero.
// BytesColumn.Value must panic on a shape-only VALUE read rather than invent data.
// logical/shape_only_columns.go must leave every unclassified use or unknown
// plan shape on full decode.
// See docs/internals/scan-lengths-only-decode.md for the design.
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
		// Same bound as the full decode: the page headers' row counts are
		// the file's claim, numRows is what the offsets array was sized for.
		if pageRows < 0 || offset+pageRows > numRows {
			page.Release()
			return fmt.Errorf("column %d: page declares %d values at row %d but the row group holds %d rows",
				colIdx, pageRows, offset, numRows)
		}
		defLevels := page.DefinitionLevels
		hasNulls := page.NumNulls > 0 && defLevels != nil

		perr := func() error {
			if page.IsDictEncoded() {
				if dict == nil {
					return fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
				}
				indices := page.Data.Int32()
				numVals := dictEntryCount(dictOffs)
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
			rawData, offs, err := byteArraySrc(page.Data, pqt.TypeString)
			if err != nil {
				return err
			}
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

// lengthsFromRawPrefixes handles the PLAIN length-prefixed layout.
//
// A page that ends mid-prefix is an ERROR here too — see selCopyRawLengths
// for why the three walks have to agree.
func lengthsFromRawPrefixes(vec *batch.Vector, offset, pageRows int, defLevels []int32, maxDefLevel int32, hasNulls bool, rawData []byte) error {
	bd := &vec.BytesData
	cur := bd.Offsets[offset]
	pos := 0
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
				return truncatedPlainPageErr(i, pageRows, pos+4, len(rawData))
			}
			length := int(binary.LittleEndian.Uint32(rawData[pos:]))
			pos += 4
			if length < 0 || pos+length > len(rawData) {
				return truncatedPlainPageErr(i, pageRows, pos+length, len(rawData))
			}
			cur += uint32(length)
			pos += length
		} else {
			vec.Nulls.SetNull(row)
		}
		bd.Offsets[row+1] = cur
	}
	return nil
}
