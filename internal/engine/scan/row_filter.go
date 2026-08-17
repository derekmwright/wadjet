package scan

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/optswitch"
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
//   - RUN-GRANULARITY evaluation (WADJET_RLE_RUN_PREDS): a dictionary
//     page's index stream is RLE — columns like ClickBench's EventDate
//     are ONE run per row group, CounterID three — so the mask is applied
//     once per run over a SPAN of rows rather than once per row, and the
//     indices are never expanded to an int32 array at all.
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

// RLERunPreds gates run-granularity predicate evaluation over
// dictionary-index pages. With it off, dictionary pages expand their index
// stream and the mask is applied per row, exactly as before. Kill switch:
// WADJET_RLE_RUN_PREDS=0.
var RLERunPreds = optswitch.Register("rle-run-preds", "WADJET_RLE_RUN_PREDS",
	"run-granularity predicate evaluation over RLE dictionary-index pages in the scan filter")

// runPathMinRunRatio: the run path is taken when a dictionary page's run
// census comes in under numValues/runPathMinRunRatio. Both paths cost
// ~one dictionary-mask lookup per run; the expand path additionally pays
// the full RLE materialization (one int32 write per row) plus a per-row
// mask test, so the true break-even sits well above 1/8. Eight is the
// conservative side of it: a page has to be genuinely run-structured
// before the census is worth acting on, and the census itself short-
// circuits at the limit so a bit-packed page costs numValues/64 header
// reads to reject.
const runPathMinRunRatio = 8

// runPathPages / runPathRows count what the run path actually handled
// (test engagement markers — a threshold that silently stops firing is
// otherwise invisible).
var (
	runPathPages atomic.Int64
	runPathRows  atomic.Int64
)

// RunPathStatsSnapshot returns (pages, rows) evaluated run-wise.
func RunPathStatsSnapshot() (int64, int64) {
	return runPathPages.Load(), runPathRows.Load()
}

var rowBitsPool = sync.Pool{New: func() any { return &rowBitmap{} }}

// EvalRowGroupPreds evaluates the AND of preds over one row group and
// returns the matching selection. sel is only meaningful for
// FilterPartial and holds row indices in ascending order.
func EvalRowGroupPreds(fr *pqt.FileReader, rgIdx int, preds []RowPred, numRows int) ([]uint32, FilterDecision, error) {
	scanFilterRowGroups.Add(1)
	// bm bit i = row i still matching. Pooled: a fresh mask per row group
	// was ~100MB of allocation zeroing per 100-part query even at one
	// byte per row.
	bm := rowBitsPool.Get().(*rowBitmap)
	defer rowBitsPool.Put(bm)
	bm.reset(numRows)
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
		if err := andPredOverColumn(fr, rgIdx, colIdx, p, bm); err != nil {
			return nil, FilterNone, err
		}
	}
	matched := bm.count()
	switch matched {
	case 0:
		scanFilterSkipped.Add(1)
		return nil, FilterNone, nil
	case numRows:
		return nil, FilterAll, nil
	}
	return bm.appendSel(make([]uint32, 0, matched)), FilterPartial, nil
}

// andPredOverColumn walks the column's pages and clears bits for rows
// that fail the predicate (NULL rows always fail a comparison).
func andPredOverColumn(fr *pqt.FileReader, rgIdx, colIdx int, p RowPred, bm *rowBitmap) error {
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

	if RLERunPreds.On() {
		// Keep dictionary pages' index streams in RLE form. Pages that
		// end up on the expand path below get them back through
		// DecodeDeferredIndices, using the very scratch buffer the reader
		// would have used, so nothing is decoded twice or allocated extra.
		pr.DeferDictIndices()
	}

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
		if row+n > bm.n {
			return fmt.Errorf("scan filter: page rows overflow row group (%d+%d > %d)", row, n, bm.n)
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
			if err := andDictPageAny(bm, row, n, page, pr, dictMask); err != nil {
				page.Release()
				return err
			}
		} else {
			if err := andPlainPage(bm, row, n, page, p); err != nil {
				page.Release()
				return err
			}
		}
		row += n
		page.Release()
	}
	// Rows beyond the pages we saw (shouldn't happen) fail closed.
	bm.clearRange(row, bm.n)
	return nil
}

// andDictPageAny picks between run-granularity and per-row evaluation for
// one dictionary-encoded page and applies the chosen one.
func andDictPageAny(bm *rowBitmap, base, n int, page *pqt.PageData, pr *pqt.ColumnPageReader, dictMask []bool) error {
	// Census first, eligibility second: the census short-circuits at the
	// limit, so a bit-packed page is rejected in numValues/64 header reads,
	// before dictRunPathEligible's (possibly per-row) null verification.
	if page.DictIndexRLE != nil && page.NumNulls == 0 {
		limit := n / runPathMinRunRatio
		runs, err := pqt.CountRLERuns(page.DictIndexRLE, page.DictIndexBitWidth, n, limit)
		if err == nil && runs <= limit && dictRunPathEligible(page, n) {
			runPathPages.Add(1)
			runPathRows.Add(int64(n))
			return andDictPageRuns(bm, base, n, page.DictIndexRLE, page.DictIndexBitWidth, dictMask)
		}
		// A census error is not reported here: the expand path decodes the
		// same bytes and surfaces the identical error with page context.
	}
	indices := page.Data.Int32()
	nv := page.Data.Count()
	if page.DictIndexRLE != nil {
		idx, err := pr.DecodeDeferredIndices(page)
		if err != nil {
			return fmt.Errorf("scan filter: %w", err)
		}
		indices, nv = idx, len(idx)
	}
	return andDictPage(bm, base, n, indices, nv, dictMask, page.DefinitionLevels, pageMaxDef(page))
}

// dictRunPathEligible reports whether a run of k dictionary indices covers
// exactly k consecutive ROWS on this page, which is the whole premise of
// the run path.
//
// That holds only when the page has no nulls: values are stored dense over
// non-null rows, so with nulls present a run of k values spans k *non-null*
// rows and locating it means walking the definition levels row by row —
// the very work the run path exists to skip. Such pages take the expand
// path unchanged, keeping "a NULL never matches a comparison" implemented
// in exactly one place.
//
// "No nulls" has to be established from the levels, not asserted by the
// file. Three cases:
//
//   - no definition levels at all: a REQUIRED column, nothing to check;
//   - NullsFromLevels: the reader counted the nulls off these levels
//     (v1 pages always; v2 pages with no level data);
//   - otherwise a v2 header's num_nulls claim, which is verified here with
//     a compare-only pass. That pass costs far less than the RLE expansion
//     plus per-row mask work it buys, and the caller runs it only after
//     the census has already accepted the page.
func dictRunPathEligible(page *pqt.PageData, n int) bool {
	if page.DictIndexRLE == nil || page.NumNulls != 0 {
		return false
	}
	def := page.DefinitionLevels
	if def == nil {
		return true
	}
	if len(def) < n {
		return false // truncated levels: let the expand path report it
	}
	if page.NullsFromLevels {
		return true
	}
	maxDef := pageMaxDef(page)
	for i := 0; i < n; i++ {
		if def[i] < maxDef {
			return false
		}
	}
	return true
}

// andDictPageRuns ANDs a dictionary page into the mask run-wise: the
// predicate has already been evaluated per dictionary entry, so a run
// resolves to one dictMask lookup and — only when the run fails — one
// span clear. A page of all-matching runs costs nothing at all.
//
// The page must have no nulls (see andDictPageAny), so run positions and
// row positions coincide.
func andDictPageRuns(bm *rowBitmap, base, n int, rle []byte, bitWidth int, dictMask []bool) error {
	it := pqt.NewRLERunIterator(rle, bitWidth, n)
	row, end := base, base+n
	for {
		val, runLen, ok := it.Next()
		if !ok {
			break
		}
		// Span arithmetic against the page window: a run reaching past it
		// is a corrupt stream, never something to clamp into place.
		if runLen <= 0 || runLen > end-row {
			return fmt.Errorf("scan filter: RLE run overruns page (row %d + %d > %d)", row, runLen, end)
		}
		if uint(val) >= uint(len(dictMask)) {
			return fmt.Errorf("scan filter: dictionary index %d out of range [0,%d)", val, len(dictMask))
		}
		if !dictMask[val] {
			bm.clearRange(row, row+runLen)
		}
		row += runLen
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("scan filter: dictionary indices: %w", err)
	}
	if row != end {
		return fmt.Errorf("scan filter: %d indices for %d rows", row-base, n)
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

// andDictPage ANDs a dictionary page into the mask per row: value present
// and non-null and dictMask[idx] true. Rows [base, base+n) of bm.
func andDictPage(bm *rowBitmap, base, n int, indices []int32, numValues int, dictMask []bool, defLevels []int32, maxDef int32) error {
	if defLevels == nil {
		// No nulls: one index per row.
		if numValues < n {
			return fmt.Errorf("scan filter: %d indices for %d rows", numValues, n)
		}
		// A still-untouched mask makes the per-row test dead weight; each
		// row index is visited once, so nothing this loop clears is read
		// back by it.
		allSet := bm.allSet
		for i := 0; i < n; i++ {
			if !allSet && !bm.get(base+i) {
				continue
			}
			idx := indices[i]
			if uint(idx) >= uint(len(dictMask)) {
				return fmt.Errorf("scan filter: dictionary index %d out of range [0,%d)", idx, len(dictMask))
			}
			if !dictMask[idx] {
				bm.clear(base + i)
			}
		}
		return nil
	}
	// Nulls present: values are dense over non-null rows only.
	vi := 0
	for i := 0; i < n; i++ {
		if i >= len(defLevels) {
			return fmt.Errorf("scan filter: definition levels short (%d < %d)", len(defLevels), n)
		}
		if defLevels[i] < maxDef {
			bm.clear(base + i) // NULL never matches a comparison
			continue
		}
		if vi >= numValues {
			return fmt.Errorf("scan filter: non-null values exhausted at row %d", i)
		}
		if bm.get(base + i) {
			idx := indices[vi]
			if uint(idx) >= uint(len(dictMask)) {
				return fmt.Errorf("scan filter: dictionary index %d out of range [0,%d)", idx, len(dictMask))
			}
			if !dictMask[idx] {
				bm.clear(base + i)
			}
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
	if isLikeOp(p.Op) && d.PhysType() != pqt.PhysicalByteArray {
		return nil, fmt.Errorf("scan filter: LIKE on non-string dictionary type %v", d.PhysType())
	}
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
		if isLikeOp(p.Op) {
			match := compileLike(want, p.Op == OpNotLike)
			for i := 0; i < n && i+1 < len(offs); i++ {
				mask[i] = match(data[offs[i]:offs[i+1]])
			}
			break
		}
		for i := 0; i < n && i+1 < len(offs); i++ {
			s := string(data[offs[i]:offs[i+1]])
			mask[i] = cmpMatch(compareStr(s, want), p.Op)
		}
	default:
		return nil, fmt.Errorf("scan filter: unsupported dictionary type %v", d.PhysType())
	}
	return mask, nil
}

// andPlainPage ANDs a PLAIN-encoded page into the mask with per-value
// comparison (the fallback-page case; still saves materialization).
// Rows [base, base+n) of bm.
func andPlainPage(bm *rowBitmap, base, n int, page *pqt.PageData, p RowPred) error {
	d := page.Data
	nv := d.Count()
	var likeFn func([]byte) bool
	if isLikeOp(p.Op) {
		if d.PhysType() != pqt.PhysicalByteArray {
			return fmt.Errorf("scan filter: LIKE on non-string page type %v", d.PhysType())
		}
		if pat, ok := p.Value.(string); ok {
			likeFn = compileLike(pat, p.Op == OpNotLike)
		}
	}
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
			if likeFn != nil {
				return likeFn(data[offs[vi]:offs[vi+1]]), nil
			}
			s := string(data[offs[vi]:offs[vi+1]])
			return cmpMatch(compareStr(s, want), p.Op), nil
		default:
			return false, fmt.Errorf("scan filter: unsupported plain type %v", d.PhysType())
		}
	}
	defLevels := page.DefinitionLevels
	if defLevels == nil {
		if nv < n {
			return fmt.Errorf("scan filter: %d values for %d rows", nv, n)
		}
		allSet := bm.allSet
		for i := 0; i < n; i++ {
			if !allSet && !bm.get(base+i) {
				continue
			}
			m, err := match(i)
			if err != nil {
				return err
			}
			if !m {
				bm.clear(base + i)
			}
		}
		return nil
	}
	maxDef := pageMaxDef(page)
	vi := 0
	for i := 0; i < n; i++ {
		if i >= len(defLevels) {
			return fmt.Errorf("scan filter: definition levels short")
		}
		if defLevels[i] < maxDef {
			bm.clear(base + i)
			continue
		}
		if vi >= nv {
			return fmt.Errorf("scan filter: non-null values exhausted")
		}
		if bm.get(base + i) {
			m, err := match(vi)
			if err != nil {
				return err
			}
			if !m {
				bm.clear(base + i)
			}
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
