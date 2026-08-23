package scan

import (
	"encoding/binary"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Sel-aware materialization (#299).
//
// When the scan-level filter returns a partial selection, every column in
// the read schema still materialized in full — for a 0.1%-selective
// predicate over a wide row, >99% of the byte-array copy work built values
// no operator would ever read (Sel semantics: unselected slots are dead,
// exactly as they are downstream of any exec filter). readColumnNativeSel
// decodes a byte-array column against the selection instead: offsets are
// written for every row (BytesColumn offsets must stay monotonic, so dead
// rows become zero-length slots), but dictionary gathers and value copies
// run only for selected rows. Dictionary pages additionally skip the
// resolve-to-scratch + BulkSet double copy — selected entries gather
// straight from the dictionary into the vector arena.
//
// Scope: flat (non-nested) String/Bytes leaves, which is where the copy
// cost lives; fixed-width columns keep the straight memmove that already
// beats any per-row selection test. Sel-decoded columns are never offered
// to the decoded-chunk cache (the cache key has no selection component; a
// partial column entering it would corrupt later full reads).
//
// Failure contract matches the full decoder: every dictionary index
// actually dereferenced is bounds-checked; corrupt or hostile files error,
// never panic or mis-slice.
var selDecodeToggle = optswitch.Register("sel-decode", "WADJET_SEL_DECODE",
	"materialize only filter-selected rows for byte-array scan columns")

// SelDecodeOn reports whether sel-aware materialization is enabled; the
// physical planner keys the LIKE selected-column pushdown gate off it
// (pattern columns in SELECT only pay a mask-eval + selected-only copy
// when this path is live).
func SelDecodeOn() bool { return selDecodeToggle.On() }

// selEligibleLeaf reports whether a column can take the sel-aware path:
// byte-array storage on both sides (no coercion) and a flat leaf.
func selEligibleLeaf(fr *pqt.FileReader, colIdx int, catalogType pqt.TypeID) bool {
	if catalogType != pqt.TypeString && catalogType != pqt.TypeBytes {
		return false
	}
	leaves := fr.Leaves()
	if colIdx >= len(leaves) {
		return false
	}
	leaf := leaves[colIdx]
	if leaf.MaxDefLevel > 1 || leaf.MaxRepLevel > 0 {
		return false
	}
	ft := pqt.TypeIDFromSchemaNode(leaf)
	if ft != pqt.TypeString && ft != pqt.TypeBytes {
		return false
	}
	// The recovered type comes from the ANNOTATIONS, and an annotation can
	// sit over any physical at all: a LogicalString on an INT64 leaf
	// recovers as STRING while the pages carry eight-byte integers. This
	// path walks per-value byte offsets, so it needs the PHYSICAL to be one
	// that has them; the full decode refuses such a page outright
	// (byteArraySrc), and eligibility must not disagree with it.
	if leaf.Type == nil {
		return false
	}
	switch *leaf.Type {
	case pqt.PhysicalByteArray, pqt.PhysicalFixedLenByteArray:
		return true
	}
	return false
}

// readColumnNativeSel reads a byte-array column materializing only the
// rows in sel (ascending row indices within this row group). All other
// rows get zero-length non-null slots that Sel semantics guarantee are
// never read.
func readColumnNativeSel(vec *batch.Vector, fr *pqt.FileReader, rgIdx, colIdx, numRows int, sel []uint32) error {
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
	var dictData []byte
	var dictOffs []uint32
	if dict != nil {
		dictData, dictOffs = dict.Data.ByteArray()
	}

	bd := &vec.BytesData
	offset := 0 // row cursor across pages
	sc := 0     // sel cursor
	for {
		// Pages holding no selected row skip decompress+decode entirely:
		// the header's row count places the page in row space, and sel is
		// ascending so sel[sc] is the next candidate. At sparse
		// selectivities this is most pages of most columns.
		page, err := pr.NextPageMaybeSkip(func(n int) bool {
			return sc >= len(sel) || int(sel[sc]) >= offset+n
		})
		if err != nil {
			return fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}
		pageRows := page.NumValues
		if pageRows == 0 {
			continue
		}
		// Same bound as the full decode: the page headers' row counts are
		// the file's claim, numRows is what the offsets array was sized for.
		if pageRows < 0 || offset+pageRows > numRows {
			page.Release()
			return fmt.Errorf("column %d: page declares %d values at row %d but the row group holds %d rows",
				colIdx, pageRows, offset, numRows)
		}
		end := offset + pageRows
		if page.Skipped {
			cur := uint32(len(bd.Data))
			for j := offset; j < end; j++ {
				bd.Offsets[j+1] = cur
			}
			offset = end
			continue
		}
		se := sc
		for se < len(sel) && int(sel[se]) < end {
			se++
		}
		pageSel := sel[sc:se]
		sc = se

		defLevels := page.DefinitionLevels
		hasNulls := page.NumNulls > 0 && defLevels != nil

		perr := func() error {
			if page.IsDictEncoded() {
				if dict == nil {
					return fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
				}
				indices := page.Data.Int32()
				numVals := dictEntryCount(dictOffs)
				valueAt := func(vi int) ([]byte, error) {
					if vi >= len(indices) {
						return nil, fmt.Errorf("dictionary page: value %d beyond %d indices", vi, len(indices))
					}
					idx := indices[vi]
					if uint(idx) >= uint(numVals) {
						return nil, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, numVals)
					}
					return dictData[dictOffs[idx]:dictOffs[idx+1]], nil
				}
				return selCopyPage(vec, pageSel, offset, pageRows, defLevels, maxDefLevel, hasNulls, valueAt)
			}
			rawData, offs, err := byteArraySrc(page.Data, pqt.TypeString)
			if err != nil {
				return err
			}
			if offs != nil {
				valueAt := func(vi int) ([]byte, error) {
					if vi+1 >= len(offs) {
						return nil, fmt.Errorf("byte-array page: value %d beyond %d offsets", vi, len(offs))
					}
					return rawData[offs[vi]:offs[vi+1]], nil
				}
				return selCopyPage(vec, pageSel, offset, pageRows, defLevels, maxDefLevel, hasNulls, valueAt)
			}
			// PLAIN raw length-prefixed fallback: positions are only
			// discoverable sequentially.
			return selCopyRawLengths(vec, pageSel, offset, pageRows, defLevels, maxDefLevel, hasNulls, rawData)
		}()
		page.Release()
		if perr != nil {
			return perr
		}
		offset = end
	}
	// Trailing rows past the last page (schema-evolution short chunks
	// keep the full reader's leave-as-empty behavior): fill offsets so
	// the column stays monotonic.
	cur := uint32(len(bd.Data))
	for j := offset; j < numRows; j++ {
		bd.Offsets[j+1] = cur
	}
	return nil
}

// selCopyPage materializes one page's selected rows given random access to
// its dense (non-null) value stream. Non-null pages index values by row;
// null-bearing pages walk defLevels to maintain the dense value cursor.
func selCopyPage(vec *batch.Vector, pageSel []uint32, offset, pageRows int, defLevels []int32, maxDefLevel int32, hasNulls bool, valueAt func(int) ([]byte, error)) error {
	bd := &vec.BytesData
	if !hasNulls {
		// Exact-size the arena for this page's selected bytes so the
		// appends below never grow it mid-page.
		total := 0
		for _, r := range pageSel {
			v, err := valueAt(int(r) - offset)
			if err != nil {
				return err
			}
			total += len(v)
		}
		bd.PreAllocBytes(len(bd.Data) + total)
		cur := uint32(len(bd.Data))
		prev := offset
		for _, r := range pageSel {
			ri := int(r)
			for j := prev; j < ri; j++ {
				bd.Offsets[j+1] = cur
			}
			v, _ := valueAt(ri - offset) // validated above
			bd.Data = append(bd.Data, v...)
			cur = uint32(len(bd.Data))
			bd.Offsets[ri+1] = cur
			prev = ri + 1
		}
		for j := prev; j < offset+pageRows; j++ {
			bd.Offsets[j+1] = cur
		}
		return nil
	}
	if len(defLevels) < pageRows {
		return fmt.Errorf("byte-array page: %d definition levels for %d rows", len(defLevels), pageRows)
	}
	cur := uint32(len(bd.Data))
	si, valIdx := 0, 0
	for i := 0; i < pageRows; i++ {
		row := offset + i
		isVal := defLevels[i] == maxDefLevel
		if si < len(pageSel) && int(pageSel[si]) == row {
			if isVal {
				v, err := valueAt(valIdx)
				if err != nil {
					return err
				}
				bd.Data = append(bd.Data, v...)
				cur = uint32(len(bd.Data))
			} else {
				vec.Nulls.SetNull(row)
			}
			si++
		}
		bd.Offsets[row+1] = cur
		if isVal {
			valIdx++
		}
	}
	return nil
}

// selCopyRawLengths handles the PLAIN length-prefixed fallback, where value
// positions require a sequential walk regardless of selection; only the
// value memcpy is skipped for unselected rows.
//
// A page that ends mid-prefix is an ERROR, the same as it is in the full
// decoder (scan/columnar_native.go copyNativeDataDirect) and in the
// lengths-only decoder. It used to `break`, with a comment saying the full
// decoder tolerated truncation and stopped early — that stopped being true
// when the full decoder was fixed, and the comment stayed. The rows after a
// truncated page are not empty, they are unknown: the offsets get closed out
// at whatever the walk reached, and every remaining row reads as the empty
// string. Three decoders over one page must not disagree about whether the
// page is readable.
func selCopyRawLengths(vec *batch.Vector, pageSel []uint32, offset, pageRows int, defLevels []int32, maxDefLevel int32, hasNulls bool, rawData []byte) error {
	bd := &vec.BytesData
	cur := uint32(len(bd.Data))
	pos, si := 0, 0
	for i := 0; i < pageRows; i++ {
		row := offset + i
		isVal := true
		if hasNulls {
			if i >= len(defLevels) {
				return fmt.Errorf("byte-array page: %d definition levels for %d rows", len(defLevels), pageRows)
			}
			isVal = defLevels[i] == maxDefLevel
		}
		selected := si < len(pageSel) && int(pageSel[si]) == row
		if isVal {
			if pos+4 > len(rawData) {
				return truncatedPlainPageErr(i, pageRows, pos+4, len(rawData))
			}
			length := int(binary.LittleEndian.Uint32(rawData[pos:]))
			pos += 4
			if length < 0 || pos+length > len(rawData) {
				return truncatedPlainPageErr(i, pageRows, pos+length, len(rawData))
			}
			if selected {
				bd.Data = append(bd.Data, rawData[pos:pos+length]...)
				cur = uint32(len(bd.Data))
			}
			pos += length
		} else if selected {
			vec.Nulls.SetNull(row)
		}
		if selected {
			si++
		}
		bd.Offsets[row+1] = cur
	}
	return nil
}

// truncatedPlainPageErr is the one wording the three PLAIN byte-array walks
// share, so a file that is unreadable through one of them is unreadable
// through all three and says the same thing.
func truncatedPlainPageErr(i, pageRows, need, have int) error {
	return fmt.Errorf("byte-array page: PLAIN page ends at value %d of %d — the value needs %d bytes but the page body holds %d",
		i, pageRows, need, have)
}
