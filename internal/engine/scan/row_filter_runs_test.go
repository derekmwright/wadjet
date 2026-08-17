package scan

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Coverage for run-granularity predicate evaluation (WADJET_RLE_RUN_PREDS).
//
// The run path and the expanding path are two implementations of the same
// function, so every test here is differential: they must produce the SAME
// mask, and — where an independent answer is available — the right one.

// --- synthetic RLE stream builders (mirror the parquet-side encoder) ---

func putVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func rleGroup(dst []byte, val int32, count, bitWidth int) []byte {
	dst = putVarint(dst, uint64(count)<<1)
	for i := 0; i < (bitWidth+7)/8; i++ {
		dst = append(dst, byte(uint32(val)>>(8*uint(i))))
	}
	return dst
}

func packGroup(dst []byte, vals []int32, bitWidth int) []byte {
	if len(vals)%8 != 0 {
		panic("bit-packed runs hold whole groups of 8")
	}
	dst = putVarint(dst, uint64(len(vals)/8)<<1|1)
	if bitWidth == 0 {
		return dst
	}
	nbytes := len(vals) * bitWidth / 8
	buf := make([]byte, nbytes+8)
	bitPos := 0
	for _, v := range vals {
		for b := 0; b < bitWidth; b++ {
			if uint32(v)>>uint(b)&1 == 1 {
				buf[(bitPos+b)/8] |= 1 << uint((bitPos+b)%8)
			}
		}
		bitPos += bitWidth
	}
	return append(dst, buf[:nbytes]...)
}

// TestAndDictPageRunsMatchesExpandPath is the core differential: for
// randomized index streams, dictionary masks, page offsets and prior mask
// state, evaluating run-wise must leave the bitmap bit-for-bit identical
// to evaluating row-wise off the expanded indices.
func TestAndDictPageRunsMatchesExpandPath(t *testing.T) {
	for seed := int64(0); seed < 500; seed++ {
		rng := rand.New(rand.NewSource(seed))
		bitWidth := 1 + rng.Intn(10)
		dictSize := 1 + rng.Intn(1<<uint(minInt(bitWidth, 8)))

		// Build a stream with a random mix of long runs and bit-packed
		// groups, so both iterator branches feed the evaluator.
		var rle []byte
		var values []int32
		for len(values) < 64 || rng.Intn(4) != 0 {
			if rng.Intn(2) == 0 {
				n := 1 + rng.Intn(200)
				v := int32(rng.Intn(dictSize))
				rle = rleGroup(rle, v, n, bitWidth)
				for i := 0; i < n; i++ {
					values = append(values, v)
				}
			} else {
				k := 8 * (1 + rng.Intn(6))
				vals := make([]int32, k)
				for i := range vals {
					vals[i] = int32(rng.Intn(dictSize))
				}
				rle = packGroup(rle, vals, bitWidth)
				values = append(values, vals...)
			}
			if len(values) > 2000 {
				break
			}
		}
		n := len(values)

		dictMask := make([]bool, dictSize)
		for i := range dictMask {
			dictMask[i] = rng.Intn(3) != 0
		}

		// Page at an arbitrary offset inside a larger row group, with a
		// randomly pre-cleared mask — the AND has to compose.
		base := rng.Intn(200)
		total := base + n + rng.Intn(200)

		var runBM, expandBM rowBitmap
		runBM.reset(total)
		expandBM.reset(total)
		for i := 0; i < total; i++ {
			if rng.Intn(8) == 0 {
				runBM.clear(i)
				expandBM.clear(i)
			}
		}

		if err := andDictPageRuns(&runBM, base, n, rle, bitWidth, dictMask); err != nil {
			t.Fatalf("seed %d: run path: %v", seed, err)
		}
		if err := andDictPage(&expandBM, base, n, values, len(values), dictMask, nil, 0); err != nil {
			t.Fatalf("seed %d: expand path: %v", seed, err)
		}
		for i := 0; i < total; i++ {
			if runBM.get(i) != expandBM.get(i) {
				t.Fatalf("seed %d: bit %d: run=%v expand=%v (base=%d n=%d bw=%d)",
					seed, i, runBM.get(i), expandBM.get(i), base, n, bitWidth)
			}
		}
		if runBM.count() != expandBM.count() {
			t.Fatalf("seed %d: count run=%d expand=%d", seed, runBM.count(), expandBM.count())
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestAndDictPageRunsRejectsCorruptStreams: a stream that does not cover
// the page exactly, or names a dictionary entry that does not exist, is a
// corrupt file. Both must be errors — never a silently clamped span, which
// would drop or invent matching rows.
func TestAndDictPageRunsRejectsCorruptStreams(t *testing.T) {
	dictMask := []bool{true, false, true, false}

	t.Run("stream short of the page", func(t *testing.T) {
		var bm rowBitmap
		bm.reset(100)
		err := andDictPageRuns(&bm, 0, 100, rleGroup(nil, 0, 40, 8), 8, dictMask)
		if err == nil {
			t.Fatal("a stream covering 40 of 100 rows must error")
		}
	})

	t.Run("index past the dictionary", func(t *testing.T) {
		var bm rowBitmap
		bm.reset(50)
		err := andDictPageRuns(&bm, 0, 50, rleGroup(nil, 99, 50, 8), 8, dictMask)
		if err == nil {
			t.Fatal("dictionary index 99 against a 4-entry dictionary must error")
		}
	})

	t.Run("negative index", func(t *testing.T) {
		var bm rowBitmap
		bm.reset(50)
		err := andDictPageRuns(&bm, 0, 50, rleGroup(nil, -1, 50, 32), 32, dictMask)
		if err == nil {
			t.Fatal("a negative dictionary index must error")
		}
	})

	t.Run("truncated stream", func(t *testing.T) {
		var bm rowBitmap
		bm.reset(50)
		err := andDictPageRuns(&bm, 0, 50, []byte{0x08}, 16, dictMask)
		if err == nil {
			t.Fatal("a truncated stream must error")
		}
	})

	t.Run("empty stream over a non-empty page", func(t *testing.T) {
		var bm rowBitmap
		bm.reset(10)
		if err := andDictPageRuns(&bm, 0, 10, nil, 8, dictMask); err == nil {
			t.Fatal("no indices for 10 rows must error")
		}
	})
}

// TestAndDictPageRunsAllMatchingIsFree pins the mechanism: when every run
// resolves to a matching dictionary entry, the mask is untouched.
func TestAndDictPageRunsAllMatchingIsFree(t *testing.T) {
	var bm rowBitmap
	bm.reset(1000)
	if err := andDictPageRuns(&bm, 0, 1000, rleGroup(nil, 0, 1000, 8), 8, []bool{true}); err != nil {
		t.Fatal(err)
	}
	if bm.count() != 1000 || !bm.allSet {
		t.Fatalf("count=%d allSet=%v, want 1000/true", bm.count(), bm.allSet)
	}
	// And the mirror: one non-matching run clears the whole span.
	bm.reset(1000)
	if err := andDictPageRuns(&bm, 0, 1000, rleGroup(nil, 0, 1000, 8), 8, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if bm.count() != 0 {
		t.Fatalf("count=%d, want 0", bm.count())
	}
}

// TestDictRunPathEligible covers the rule that decides whether a run of k
// indices may be treated as k consecutive rows. Getting it wrong in the
// permissive direction misattributes every value after the first null, so
// each "false" here is load-bearing.
func TestDictRunPathEligible(t *testing.T) {
	const n = 8
	present := []int32{1, 1, 1, 1, 1, 1, 1, 1}
	withNull := []int32{1, 1, 1, 0, 1, 1, 1, 1}
	rle := rleGroup(nil, 0, n, 8)

	cases := []struct {
		name string
		page pqt.PageData
		want bool
	}{
		{"no deferred payload", pqt.PageData{NumValues: n}, false},
		{"required column, no levels",
			pqt.PageData{NumValues: n, DictIndexRLE: rle}, true},
		{"nulls present",
			pqt.PageData{NumValues: n, NumNulls: 1, DictIndexRLE: rle,
				DefinitionLevels: withNull, NullsFromLevels: true}, false},
		{"optional column with zero nulls, counted from levels",
			pqt.PageData{NumValues: n, DictIndexRLE: rle,
				DefinitionLevels: present, NullsFromLevels: true}, true},
		{"header claims zero nulls and the levels agree",
			pqt.PageData{NumValues: n, DictIndexRLE: rle,
				DefinitionLevels: present, NullsFromLevels: false}, true},
		// The dangerous one: a v2 header claiming no nulls over levels
		// that contain one. Trusting it would shift every later value by
		// a row. The verification pass must catch it.
		{"header claims zero nulls but the levels disagree",
			pqt.PageData{NumValues: n, DictIndexRLE: rle,
				DefinitionLevels: withNull, NullsFromLevels: false}, false},
		{"definition levels shorter than the page",
			pqt.PageData{NumValues: n, DictIndexRLE: rle,
				DefinitionLevels: present[:3], NullsFromLevels: false}, false},
		{"short levels are not excused by a counted null count",
			pqt.PageData{NumValues: n, DictIndexRLE: rle,
				DefinitionLevels: present[:3], NullsFromLevels: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dictRunPathEligible(&tc.page, n); got != tc.want {
				t.Fatalf("dictRunPathEligible = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- end-to-end over a pyarrow-written pure-dictionary fixture ---------

const dictRunsFixture = "../../storage/parquet/testdata/dict_runs.parquet"

// openDictRuns loads the fixture and asserts it is still what the test
// needs: pure-dictionary chunks. A fixture that regressed to PLAIN
// fallback would move this coverage silently onto the wrong path.
func openDictRuns(t *testing.T) (*pqt.Reader, *pqt.FileReader) {
	t.Helper()
	data, err := os.ReadFile(dictRunsFixture)
	if err != nil {
		t.Skipf("fixture missing (regen with testdata/gen_dict_runs.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	fr := r.FileReader()
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		for col := range fr.Leaves() {
			pr := fr.ColumnPages(rg, col)
			_, pure, err := pr.DictionaryIfPure()
			pr.Close()
			if err != nil {
				t.Fatalf("rg%d col%d: %v", rg, col, err)
			}
			if !pure {
				t.Fatalf("rg%d col%d is not pure-dictionary — regenerate the fixture with gen_dict_runs.py", rg, col)
			}
		}
	}
	return r, fr
}

// evalWithToggle runs EvalRowGroupPreds with the run path forced on or off
// and returns the decision plus the selection.
func evalWithToggle(t *testing.T, fr *pqt.FileReader, rg int, preds []RowPred, numRows int, on bool) ([]uint32, FilterDecision) {
	t.Helper()
	prev := RLERunPreds.Set(on)
	defer RLERunPreds.Set(prev)
	sel, dec, err := EvalRowGroupPreds(fr, rg, preds, numRows)
	if err != nil {
		t.Fatalf("EvalRowGroupPreds(runPath=%v): %v", on, err)
	}
	return sel, dec
}

// referenceSel computes the expected selection straight from the decoded
// rows, independently of either evaluation path — so a bug shared by both
// still fails.
func referenceSel(t *testing.T, rows []map[string]any, from, n int, preds []RowPred) []uint32 {
	t.Helper()
	out := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		row := rows[from+i]
		ok := true
		for _, p := range preds {
			if !rowMatches(t, row[p.Col], p) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, uint32(i))
		}
	}
	return out
}

func rowMatches(t *testing.T, v any, p RowPred) bool {
	t.Helper()
	if v == nil {
		return false // NULL never matches a comparison
	}
	switch want := p.Value.(type) {
	case int64:
		var got int64
		switch x := v.(type) {
		case int32:
			got = int64(x)
		case int64:
			got = x
		default:
			t.Fatalf("column %s: value %T is not integral", p.Col, v)
		}
		return cmpMatch(compareI64(got, want), p.Op)
	case float64:
		got, ok := v.(float64)
		if !ok {
			t.Fatalf("column %s: value %T is not a double", p.Col, v)
		}
		return cmpMatch(compareF64(got, want), p.Op)
	case string:
		var got string
		switch x := v.(type) {
		case string:
			got = x
		case []byte:
			got = string(x)
		default:
			t.Fatalf("column %s: value %T is not a string", p.Col, v)
		}
		return cmpMatch(compareStr(got, want), p.Op)
	}
	t.Fatalf("unhandled literal %T", p.Value)
	return false
}

// TestEvalRowGroupPredsRunPathDifferential is the end-to-end gate: over a
// real pure-dictionary file spanning the run spectrum (one run per row
// group through fully bit-packed, with and without nulls), the run path
// must agree with the expanding path AND with a reference computed from
// the decoded rows.
func TestEvalRowGroupPredsRunPathDifferential(t *testing.T) {
	r, fr := openDictRuns(t)
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	preds := [][]RowPred{
		// flat: ONE run per row group — the EventDate shape.
		{{Col: "flat", Op: "=", Value: int64(0)}},
		{{Col: "flat", Op: "!=", Value: int64(0)}},
		{{Col: "flat", Op: ">=", Value: int64(1)}},
		// runs: long runs, every operator.
		{{Col: "runs", Op: "=", Value: int64(3)}},
		{{Col: "runs", Op: "!=", Value: int64(3)}},
		{{Col: "runs", Op: "<", Value: int64(20)}},
		{{Col: "runs", Op: "<=", Value: int64(20)}},
		{{Col: "runs", Op: ">", Value: int64(20)}},
		{{Col: "runs", Op: ">=", Value: int64(20)}},
		{{Col: "runs", Op: ">", Value: int64(1 << 40)}},   // matches nothing
		{{Col: "runs", Op: "<", Value: int64(1 << 40)}},   // matches everything
		{{Col: "short", Op: "=", Value: int64(5)}},        // short runs
		{{Col: "packed", Op: "<", Value: int64(2000)}},    // bit-packed: expand path
		{{Col: "packed", Op: "=", Value: int64(1234)}},    //
		{{Col: "s_runs", Op: "=", Value: "cat003"}},       // string dictionary
		{{Col: "s_runs", Op: ">=", Value: "cat010"}},      //
		{{Col: "f_runs", Op: "<=", Value: float64(7)}},    // double dictionary
		{{Col: "opt", Op: "=", Value: int64(3)}},          // OPTIONAL, zero nulls
		{{Col: "opt", Op: ">=", Value: int64(20)}},        //   (the real hits shape)
		{{Col: "nulls", Op: "=", Value: int64(3)}},        // nulls: expand path
		{{Col: "nulls", Op: ">=", Value: int64(0)}},       //
		{{Col: "nulls", Op: "!=", Value: int64(1 << 20)}}, // every non-null row
		// Conjunctions across shapes: run path ANDed with expand path.
		{{Col: "runs", Op: ">=", Value: int64(10)}, {Col: "packed", Op: "<", Value: int64(2000)}},
		{{Col: "flat", Op: "=", Value: int64(1)}, {Col: "runs", Op: "<", Value: int64(60)}},
		{{Col: "s_runs", Op: "<", Value: "cat005"}, {Col: "nulls", Op: "=", Value: int64(2)}},
	}

	engaged := int64(0)
	rowStart := 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		numRows := int(fr.RowGroupNumRows(rg))
		for _, ps := range preds {
			name := fmt.Sprintf("rg%d/%s%s%v", rg, ps[0].Col, ps[0].Op, ps[0].Value)
			t.Run(name, func(t *testing.T) {
				p0, _ := RunPathStatsSnapshot()
				runSel, runDec := evalWithToggle(t, fr, rg, ps, numRows, true)
				p1, _ := RunPathStatsSnapshot()
				engaged += p1 - p0
				expandSel, expandDec := evalWithToggle(t, fr, rg, ps, numRows, false)

				if runDec != expandDec {
					t.Fatalf("decision run=%v expand=%v", runDec, expandDec)
				}
				if len(runSel) != len(expandSel) {
					t.Fatalf("sel length run=%d expand=%d", len(runSel), len(expandSel))
				}
				for i := range expandSel {
					if runSel[i] != expandSel[i] {
						t.Fatalf("sel[%d] run=%d expand=%d", i, runSel[i], expandSel[i])
					}
				}

				want := referenceSel(t, rows, rowStart, numRows, ps)
				var got []uint32
				switch runDec {
				case FilterNone:
					got = nil
				case FilterAll:
					got = make([]uint32, numRows)
					for i := range got {
						got[i] = uint32(i)
					}
				default:
					got = runSel
				}
				if len(got) != len(want) {
					t.Fatalf("matched %d rows, reference says %d", len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("row %d: got %d, reference %d", i, got[i], want[i])
					}
				}
			})
		}
		rowStart += numRows
	}
	if engaged == 0 {
		t.Fatal("the run path never engaged — the threshold or the fixture stopped covering it")
	}
	t.Logf("run path engaged on %d pages", engaged)
}

// TestEvalRowGroupPredsRunPathRandomized sweeps random predicates across
// every column of the fixture with both paths, so the differential is not
// limited to the hand-picked list above.
func TestEvalRowGroupPredsRunPathRandomized(t *testing.T) {
	_, fr := openDictRuns(t)
	ops := []string{"=", "!=", "<", "<=", ">", ">="}
	intCols := []string{"flat", "runs", "short", "packed", "opt", "nulls"}

	for seed := int64(0); seed < 120; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rg := rng.Intn(fr.NumRowGroups())
		numRows := int(fr.RowGroupNumRows(rg))

		var ps []RowPred
		for k := 0; k < 1+rng.Intn(3); k++ {
			switch rng.Intn(3) {
			case 0:
				ps = append(ps, RowPred{
					Col:   intCols[rng.Intn(len(intCols))],
					Op:    ops[rng.Intn(len(ops))],
					Value: int64(rng.Intn(200) - 20),
				})
			case 1:
				ps = append(ps, RowPred{
					Col:   "s_runs",
					Op:    ops[rng.Intn(len(ops))],
					Value: fmt.Sprintf("cat%03d", rng.Intn(80)),
				})
			default:
				ps = append(ps, RowPred{
					Col:   "f_runs",
					Op:    ops[rng.Intn(len(ops))],
					Value: float64(rng.Intn(160)),
				})
			}
		}

		runSel, runDec := evalWithToggle(t, fr, rg, ps, numRows, true)
		expandSel, expandDec := evalWithToggle(t, fr, rg, ps, numRows, false)
		if runDec != expandDec || len(runSel) != len(expandSel) {
			t.Fatalf("seed %d preds %v: run=(%v,%d) expand=(%v,%d)",
				seed, ps, runDec, len(runSel), expandDec, len(expandSel))
		}
		for i := range expandSel {
			if runSel[i] != expandSel[i] {
				t.Fatalf("seed %d preds %v: sel[%d] run=%d expand=%d", seed, ps, i, runSel[i], expandSel[i])
			}
		}
	}
}

// TestEvalRowGroupPredsMixedEncodingFixture drives both paths over the
// dictionary-FALLBACK fixture, where chunks mix dictionary and PLAIN data
// pages. The run path must handle a chunk that switches encoding mid-way
// without corrupting the row offset.
func TestEvalRowGroupPredsMixedEncodingFixture(t *testing.T) {
	data, err := os.ReadFile("../../storage/parquet/testdata/dict_fallback.parquet")
	if err != nil {
		t.Skipf("fixture missing: %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}

	preds := [][]RowPred{
		{{Col: "i32", Op: "<", Value: int64(70000)}},
		{{Col: "i64", Op: ">=", Value: int64(1000003 * 100)}},
		{{Col: "f64", Op: "<=", Value: float64(500)}},
		{{Col: "s", Op: "=", Value: "s0000042"}},
		{{Col: "sn", Op: ">", Value: "n0019000"}}, // nullable
	}
	rowStart := 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		numRows := int(fr.RowGroupNumRows(rg))
		for _, ps := range preds {
			runSel, runDec := evalWithToggle(t, fr, rg, ps, numRows, true)
			expandSel, expandDec := evalWithToggle(t, fr, rg, ps, numRows, false)
			if runDec != expandDec || len(runSel) != len(expandSel) {
				t.Fatalf("rg%d %v: run=(%v,%d) expand=(%v,%d)", rg, ps, runDec, len(runSel), expandDec, len(expandSel))
			}
			for i := range expandSel {
				if runSel[i] != expandSel[i] {
					t.Fatalf("rg%d %v: sel[%d] run=%d expand=%d", rg, ps, i, runSel[i], expandSel[i])
				}
			}
			if runDec == FilterPartial {
				want := referenceSel(t, rows, rowStart, numRows, ps)
				if len(runSel) != len(want) {
					t.Fatalf("rg%d %v: matched %d rows, reference %d", rg, ps, len(runSel), len(want))
				}
				for i := range want {
					if runSel[i] != want[i] {
						t.Fatalf("rg%d %v: row %d got %d want %d", rg, ps, i, runSel[i], want[i])
					}
				}
			}
		}
		rowStart += numRows
	}
}

func BenchmarkEvalRowGroupPredsDictRuns(b *testing.B) {
	data, err := os.ReadFile(dictRunsFixture)
	if err != nil {
		b.Skipf("fixture missing: %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		b.Fatal(err)
	}
	fr := r.FileReader()
	numRows := int(fr.RowGroupNumRows(0))

	cases := []struct {
		name  string
		preds []RowPred
	}{
		{"one-run-per-rg", []RowPred{{Col: "flat", Op: "=", Value: int64(0)}}},
		{"long-runs", []RowPred{{Col: "runs", Op: "<", Value: int64(20)}}},
		// OPTIONAL column with zero nulls — what real hits parts look like.
		{"long-runs-optional", []RowPred{{Col: "opt", Op: "<", Value: int64(20)}}},
		{"short-runs", []RowPred{{Col: "short", Op: "=", Value: int64(5)}}},
		{"bit-packed", []RowPred{{Col: "packed", Op: "<", Value: int64(2000)}}},
	}
	for _, c := range cases {
		for _, on := range []bool{false, true} {
			name := c.name + "/expand"
			if on {
				name = c.name + "/runs"
			}
			b.Run(name, func(b *testing.B) {
				prev := RLERunPreds.Set(on)
				defer RLERunPreds.Set(prev)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := EvalRowGroupPreds(fr, 0, c.preds, numRows); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
