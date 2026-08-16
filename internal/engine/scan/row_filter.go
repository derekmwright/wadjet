package scan

import (
	"fmt"
	"sync"
	"sync/atomic"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Scan-level row filtering (Level 3 of the pushdown ladder, completed):
// eligible `col <op> literal` conjuncts are evaluated by the scan itself,
// straight off column pages, before any value is materialized into a
// vector. Two structural wins over the decode-then-filter pipeline:
//
//   - Dictionary-encoded pages evaluate the predicate ONCE per dictionary
//     (a few thousand entries) and map the resulting mask over the
//     indices — no value gather, no per-row typed compare.
//   - Filter-only columns (referenced by the filter and nothing else)
//     are never materialized at all; the scan reads their pages here and
//     the projected read schema excludes them.
//
// Semantics parity with the expression layer: NULL rows never match a
// comparison; the planner only pushes conjuncts whose literal/column type
// pair compares exactly (integral literals on int-class columns, numeric
// on float, string on byte-array) — anything else stays in the residual
// exec filter.

// RowPred is one pushed conjunct.
type RowPred struct {
	Col   string
	Op    string // =, !=, <, <=, >, >=
	Value any    // int64, float64, or string (planner-normalized)
}

// FilterDecision summarizes a row group evaluation.
type FilterDecision int

const (
	FilterNone    FilterDecision = iota // no row matches: skip the row group
	FilterAll                           // every row matches: no selection needed
	FilterPartial                       // some rows match: apply Sel
)

// Engagement counters (wlog / test markers).
var (
	scanFilterRowGroups atomic.Int64 // row groups evaluated
	scanFilterSkipped   atomic.Int64 // row groups skipped entirely (FilterNone)
)

// ScanFilterStatsSnapshot returns (evaluated, skipped) row-group counts.
func ScanFilterStatsSnapshot() (int64, int64) {
	return scanFilterRowGroups.Load(), scanFilterSkipped.Load()
}

var rowBitsPool = sync.Pool{New: func() any { return &[]bool{} }}

// EvalRowGroupPreds evaluates the AND of preds over one row group and
// returns the matching selection. sel is only meaningful for
// FilterPartial and holds row indices in ascending order.
func EvalRowGroupPreds(fr *pqt.FileReader, rgIdx int, preds []RowPred, numRows int) ([]uint32, FilterDecision, error) {
	scanFilterRowGroups.Add(1)
	// bits[i] = row i still matching. Pooled: a fresh 1M-bool slice per
	// row group was ~100MB of allocation zeroing per 100-part query.
	bp := rowBitsPool.Get().(*[]bool)
	defer rowBitsPool.Put(bp)
	if cap(*bp) < numRows {
		*bp = make([]bool, numRows)
	}
	bits := (*bp)[:numRows]
	for i := range bits {
		bits[i] = true
	}
	leaves := fr.Leaves()
	for _, p := range preds {
		colIdx := -1
		for i, leaf := range leaves {
			if leaf.Name == p.Col {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return nil, FilterNone, fmt.Errorf("scan filter: column %q not in file", p.Col)
		}
		if err := andPredOverColumn(fr, rgIdx, colIdx, p, bits); err != nil {
			return nil, FilterNone, err
		}
	}
	matched := 0
	for _, b := range bits {
		if b {
			matched++
		}
	}
	switch matched {
	case 0:
		scanFilterSkipped.Add(1)
		return nil, FilterNone, nil
	case numRows:
		return nil, FilterAll, nil
	}
	sel := make([]uint32, 0, matched)
	for i, b := range bits {
		if b {
			sel = append(sel, uint32(i))
		}
	}
	return sel, FilterPartial, nil
}

// andPredOverColumn walks the column's pages and clears bits for rows
// that fail the predicate (NULL rows always fail a comparison).
func andPredOverColumn(fr *pqt.FileReader, rgIdx, colIdx int, p RowPred, bits []bool) error {
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return fmt.Errorf("scan filter: column %d pages missing", colIdx)
	}
	defer pr.Close()
	scr := colReadScratchPool.Get().(*colReadScratch)
	pr.SeedScratch(scr.def, scr.idx)
	defer func() {
		scr.def, scr.idx = pr.TakeScratch()
		colReadScratchPool.Put(scr)
	}()

	dict, err := pr.NextDictionary()
	if err != nil {
		return fmt.Errorf("scan filter: reading dictionary: %w", err)
	}
	// Dictionary mask, computed lazily on the first dict-encoded page.
	var dictMask []bool

	row := 0
	for {
		page, err := pr.NextPage()
		if err != nil {
			return fmt.Errorf("scan filter: reading page: %w", err)
		}
		if page == nil {
			break
		}
		n := page.NumValues
		if n == 0 {
			continue
		}
		if row+n > len(bits) {
			return fmt.Errorf("scan filter: page rows overflow row group (%d+%d > %d)", row, n, len(bits))
		}

		if page.IsDictEncoded() {
			if dict == nil {
				page.Release()
				return fmt.Errorf("scan filter: dictionary page missing for dict-encoded page")
			}
			if dictMask == nil {
				dictMask, err = evalPredOnDict(dict, p)
				if err != nil {
					page.Release()
					return err
				}
			}
			indices := page.Data.Int32()
			nv := page.Data.Count()
			if err := andDictPage(bits[row:row+n], indices, nv, dictMask, page.DefinitionLevels, pageMaxDef(page)); err != nil {
				page.Release()
				return err
			}
		} else {
			if err := andPlainPage(bits[row:row+n], page, p); err != nil {
				page.Release()
				return err
			}
		}
		row += n
		page.Release()
	}
	// Rows beyond the pages we saw (shouldn't happen) fail closed.
	for i := row; i < len(bits); i++ {
		bits[i] = false
	}
	return nil
}

// pageMaxDef infers the max definition level from the page's levels: a
// nil DefinitionLevels slice means a REQUIRED column (no nulls).
func pageMaxDef(page *pqt.PageData) int32 {
	if page.DefinitionLevels == nil {
		return 0
	}
	// Flat schemas: maxDefLevel is 1; a level below max marks NULL. The
	// native reader only serves flat schemas (nested types take the
	// row-based fallback), so 1 is exact here.
	return 1
}

// andDictPage ANDs a dictionary page into bits: value present and
// non-null and dictMask[idx] true.
func andDictPage(bits []bool, indices []int32, numValues int, dictMask []bool, defLevels []int32, maxDef int32) error {
	if defLevels == nil {
		// No nulls: one index per row.
		if numValues < len(bits) {
			return fmt.Errorf("scan filter: %d indices for %d rows", numValues, len(bits))
		}
		for i := range bits {
			if bits[i] {
				idx := indices[i]
				if uint(idx) >= uint(len(dictMask)) {
					return fmt.Errorf("scan filter: dictionary index %d out of range [0,%d)", idx, len(dictMask))
				}
				bits[i] = dictMask[idx]
			}
		}
		return nil
	}
	// Nulls present: values are dense over non-null rows only.
	vi := 0
	for i := range bits {
		if i >= len(defLevels) {
			return fmt.Errorf("scan filter: definition levels short (%d < %d)", len(defLevels), len(bits))
		}
		if defLevels[i] < maxDef {
			bits[i] = false // NULL never matches a comparison
			continue
		}
		if vi >= numValues {
			return fmt.Errorf("scan filter: non-null values exhausted at row %d", i)
		}
		if bits[i] {
			idx := indices[vi]
			if uint(idx) >= uint(len(dictMask)) {
				return fmt.Errorf("scan filter: dictionary index %d out of range [0,%d)", idx, len(dictMask))
			}
			bits[i] = dictMask[idx]
		}
		vi++
	}
	return nil
}

// evalPredOnDict evaluates the predicate against every dictionary entry.
func evalPredOnDict(dict *pqt.DictionaryData, p RowPred) ([]bool, error) {
	n := dict.NumValues
	mask := make([]bool, n)
	d := dict.Data
	switch d.PhysType() {
	case pqt.PhysicalInt64:
		want, ok := p.Value.(int64)
		if !ok {
			return nil, fmt.Errorf("scan filter: non-int64 literal for INT64 column")
		}
		src := d.Int64()
		for i := 0; i < n && i < len(src); i++ {
			mask[i] = cmpMatch(compareI64(src[i], want), p.Op)
		}
	case pqt.PhysicalInt32:
		want, ok := p.Value.(int64)
		if !ok {
			return nil, fmt.Errorf("scan filter: non-int64 literal for INT32 column")
		}
		src := d.Int32()
		for i := 0; i < n && i < len(src); i++ {
			mask[i] = cmpMatch(compareI64(int64(src[i]), want), p.Op)
		}
	case pqt.PhysicalDouble:
		want, ok := p.Value.(float64)
		if !ok {
			return nil, fmt.Errorf("scan filter: non-float literal for DOUBLE column")
		}
		src := d.Double()
		for i := 0; i < n && i < len(src); i++ {
			mask[i] = cmpMatch(compareF64(src[i], want), p.Op)
		}
	case pqt.PhysicalFloat:
		want, ok := p.Value.(float64)
		if !ok {
			return nil, fmt.Errorf("scan filter: non-float literal for FLOAT column")
		}
		src := d.Float()
		for i := 0; i < n && i < len(src); i++ {
			mask[i] = cmpMatch(compareF64(float64(src[i]), want), p.Op)
		}
	case pqt.PhysicalByteArray:
		want, ok := p.Value.(string)
		if !ok {
			return nil, fmt.Errorf("scan filter: non-string literal for BYTE_ARRAY column")
		}
		data, offs := d.ByteArray()
		for i := 0; i < n && i+1 < len(offs); i++ {
			s := string(data[offs[i]:offs[i+1]])
			mask[i] = cmpMatch(compareStr(s, want), p.Op)
		}
	default:
		return nil, fmt.Errorf("scan filter: unsupported dictionary type %v", d.PhysType())
	}
	return mask, nil
}

// andPlainPage ANDs a PLAIN-encoded page into bits with per-value
// comparison (the fallback-page case; still saves materialization).
func andPlainPage(bits []bool, page *pqt.PageData, p RowPred) error {
	d := page.Data
	nv := d.Count()
	match := func(vi int) (bool, error) {
		switch d.PhysType() {
		case pqt.PhysicalInt64:
			want, ok := p.Value.(int64)
			if !ok {
				return false, fmt.Errorf("scan filter: non-int64 literal for INT64 column")
			}
			return cmpMatch(compareI64(d.Int64At(vi), want), p.Op), nil
		case pqt.PhysicalInt32:
			want, ok := p.Value.(int64)
			if !ok {
				return false, fmt.Errorf("scan filter: non-int64 literal for INT32 column")
			}
			return cmpMatch(compareI64(int64(d.Int32At(vi)), want), p.Op), nil
		case pqt.PhysicalDouble:
			want, ok := p.Value.(float64)
			if !ok {
				return false, fmt.Errorf("scan filter: non-float literal for DOUBLE column")
			}
			return cmpMatch(compareF64(d.DoubleAt(vi), want), p.Op), nil
		case pqt.PhysicalFloat:
			want, ok := p.Value.(float64)
			if !ok {
				return false, fmt.Errorf("scan filter: non-float literal for FLOAT column")
			}
			return cmpMatch(compareF64(float64(d.FloatAt(vi)), want), p.Op), nil
		case pqt.PhysicalByteArray:
			want, ok := p.Value.(string)
			if !ok {
				return false, fmt.Errorf("scan filter: non-string literal for BYTE_ARRAY column")
			}
			data, offs := d.ByteArray()
			if vi+1 >= len(offs) {
				return false, fmt.Errorf("scan filter: value index %d out of range", vi)
			}
			s := string(data[offs[vi]:offs[vi+1]])
			return cmpMatch(compareStr(s, want), p.Op), nil
		default:
			return false, fmt.Errorf("scan filter: unsupported plain type %v", d.PhysType())
		}
	}
	defLevels := page.DefinitionLevels
	if defLevels == nil {
		if nv < len(bits) {
			return fmt.Errorf("scan filter: %d values for %d rows", nv, len(bits))
		}
		for i := range bits {
			if bits[i] {
				m, err := match(i)
				if err != nil {
					return err
				}
				bits[i] = m
			}
		}
		return nil
	}
	maxDef := pageMaxDef(page)
	vi := 0
	for i := range bits {
		if i >= len(defLevels) {
			return fmt.Errorf("scan filter: definition levels short")
		}
		if defLevels[i] < maxDef {
			bits[i] = false
			continue
		}
		if vi >= nv {
			return fmt.Errorf("scan filter: non-null values exhausted")
		}
		if bits[i] {
			m, err := match(vi)
			if err != nil {
				return err
			}
			bits[i] = m
		}
		vi++
	}
	return nil
}

func compareI64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareF64(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareStr(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpMatch(c int, op string) bool {
	switch op {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	default:
		return false
	}
}
