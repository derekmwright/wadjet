package scan

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE SCAN AND THE EXPRESSION LAYER AGREE ON EVERY MASK.
//
// `flagMatch` here and `expr.TCPFlagsMatch` there are two implementations of
// one predicate, kept apart because internal/engine/scan does not depend on
// the expression engine — the same arrangement `compileLike` documents against
// `expr.matchLikeRecur`. Duplication like that is only safe with a gate that
// is TOTAL over the domain rather than a handful of examples: a pushed
// conjunct and its residual twin answering differently is a row set that
// depends on whether the pushdown happened to fire.
//
// Total here means every mask the fold can produce (0..511, the nine-bit
// field) against a flags value spanning the field, the byte above it, the sign
// bit and the extremes.
func TestTheScanAndTheExpressionAgreeOnEveryMask(t *testing.T) {
	values := make([]int64, 0, 1100)
	for v := int64(0); v < 1024; v++ {
		values = append(values, v)
	}
	values = append(values,
		-1, -18, 1<<62|18, 1<<62, 1<<31, -(1 << 31),
		int64(^uint64(0)>>1), -int64(^uint64(0)>>1)-1)

	pairs := []struct {
		op   string
		mode expr.TCPFlagPredicateMode
	}{
		{OpFlagsAll, expr.TCPFlagsAll},
		{OpFlagsAny, expr.TCPFlagsAny},
		{OpFlagsNone, expr.TCPFlagsNone},
	}
	for _, p := range pairs {
		for mask := int64(0); mask <= 511; mask++ {
			for _, v := range values {
				got := flagMatch(v, mask, p.op)
				want := expr.TCPFlagsMatch(v, mask, p.mode)
				if got != want {
					t.Fatalf("%s(%d, mask %d): scan %v, expression %v", p.op, v, mask, got, want)
				}
			}
		}
	}
}

// A FLAG OP IS NOT A COMPARISON, AND THE TWO SETS STAY DISJOINT.
//
// A min/max range cannot prove anything about a BIT: `min=0, max=511` admits
// every mask, and even `min=2, max=16` admits a row holding 18. So a flag
// conjunct never becomes a StatsPredicate and never becomes an EqProbe — the
// planner builds only a scan.RowPred from one (makeFlagRowPred), and the two
// prune layers are reached from `structuredConjuncts`, which yields nothing
// but `column <cmp> literal`.
//
// What is checkable HERE is the seam that would let one leak: the row filter
// dispatches on Op, and a flag op falling through to cmpMatch would compare
// the mask as if it were a bound. cmpMatch answers false for every op it does
// not know, so such a leak would be a silently EMPTY row set rather than an
// error — which is why it is asserted rather than assumed.
//
// The end-to-end half — that turning pruning on and off leaves a flag query's
// answer and the pruned-row-group counter unchanged — is
// wadjet.TestAFlagPredicateNeverPrunesARowGroup.
func TestAFlagOpIsNotAComparison(t *testing.T) {
	for _, op := range []string{OpFlagsAll, OpFlagsAny, OpFlagsNone} {
		if isLikeOp(op) {
			t.Errorf("%s reads as a LIKE op", op)
		}
		if !isFlagOp(op) {
			t.Errorf("%s does not read as a flag op", op)
		}
		for _, c := range []int{-1, 0, 1} {
			if cmpMatch(c, op) {
				t.Errorf("%s matched as a comparison (compare result %d); a flag op "+
					"reaching the comparison path would silently select nothing", op, c)
			}
		}
	}
	for _, op := range []string{"=", "!=", "<", "<=", ">", ">=", OpLike, OpNotLike} {
		if isFlagOp(op) {
			t.Errorf("%q reads as a flag op", op)
		}
	}
}

// EvalRowGroupPreds over a PLAIN file: the flag arm of andPlainPage, against a
// reference computed from the values themselves, including NULLs (a NULL never
// matches, the same rule every other predicate here follows) and both integer
// widths.
func TestAFlagPredicateOnAPlainPageMatchesTheReference(t *testing.T) {
	vals := []any{
		int64(0), int64(2), int64(18), int64(16), int64(4), int64(511),
		int64(24), nil, int64(20), int64(256), int64(-1), int64(1)<<62 | 18,
	}
	raw := writeFlagFile(t, vals, true)
	r, err := pqt.NewReaderFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()

	for _, tc := range []struct {
		op   string
		mask int64
	}{
		{OpFlagsAll, 18}, {OpFlagsAny, 18}, {OpFlagsNone, 18},
		{OpFlagsAll, 2}, {OpFlagsAny, 4}, {OpFlagsNone, 256},
		{OpFlagsAll, 511}, {OpFlagsNone, 0}, {OpFlagsAll, 0},
	} {
		t.Run(fmt.Sprintf("%s_%d", tc.op, tc.mask), func(t *testing.T) {
			want := make([]uint32, 0, len(vals))
			for i, v := range vals {
				iv, ok := v.(int64)
				if !ok {
					continue // NULL never matches
				}
				if flagMatch(iv, tc.mask, tc.op) {
					want = append(want, uint32(i))
				}
			}
			sel, dec, err := EvalRowGroupPreds(fr, 0,
				[]RowPred{{Col: "flags", Op: tc.op, Value: tc.mask}}, len(vals))
			if err != nil {
				t.Fatalf("EvalRowGroupPreds: %v", err)
			}
			got := materializeSel(sel, dec, len(vals))
			if len(got) != len(want) {
				t.Fatalf("selected %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("selected %v, want %v", got, want)
				}
			}
		})
	}
}

// The DICTIONARY arm, over the pure-dictionary fixture the run-path gate uses.
// Two properties, and the second is the one that cannot be seen from rows:
// the answer matches an independent reference, AND the predicate was evaluated
// ONCE PER DICTIONARY ENTRY rather than once per row.
func TestAFlagPredicateOnADictionaryPageIsEvaluatedPerEntry(t *testing.T) {
	data, err := os.ReadFile(dictRunsFixture)
	if err != nil {
		t.Skipf("fixture missing (regen with testdata/gen_dict_runs.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	fr := r.FileReader()
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	cases := []struct {
		col  string
		op   string
		mask int64
	}{
		{"flat", OpFlagsAny, 1}, {"flat", OpFlagsNone, 1},
		{"runs", OpFlagsAll, 3}, {"runs", OpFlagsAny, 6}, {"runs", OpFlagsNone, 7},
		{"short", OpFlagsAny, 5}, {"packed", OpFlagsAll, 12},
		{"opt", OpFlagsAny, 2}, {"nulls", OpFlagsAny, 3}, {"nulls", OpFlagsNone, 3},
	}
	rowStart, totalEntries := 0, int64(0)
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		numRows := int(fr.RowGroupNumRows(rg))
		for _, tc := range cases {
			t.Run(fmt.Sprintf("rg%d/%s/%s/%d", rg, tc.col, tc.op, tc.mask), func(t *testing.T) {
				e0, m0, _ := FlagPushdownStatsSnapshot()
				sel, dec, err := EvalRowGroupPreds(fr, rg,
					[]RowPred{{Col: tc.col, Op: tc.op, Value: tc.mask}}, numRows)
				if err != nil {
					t.Fatalf("EvalRowGroupPreds: %v", err)
				}
				e1, m1, _ := FlagPushdownStatsSnapshot()
				if m1 == m0 {
					t.Fatalf("no dictionary mask was built — the flag predicate did not "+
						"take the per-entry path (%s %s %d)", tc.col, tc.op, tc.mask)
				}
				entries := e1 - e0
				totalEntries += entries
				if entries >= int64(numRows) {
					t.Errorf("evaluated %d dictionary entries for %d rows — the per-entry "+
						"path is the whole point of pushing this predicate", entries, numRows)
				}

				want := make([]uint32, 0, numRows)
				for i := 0; i < numRows; i++ {
					v, ok := flagRowValue(rows[rowStart+i][tc.col])
					if !ok {
						continue // NULL never matches
					}
					if flagMatch(v, tc.mask, tc.op) {
						want = append(want, uint32(i))
					}
				}
				got := materializeSel(sel, dec, numRows)
				if len(got) != len(want) {
					t.Fatalf("%s %s %d: selected %d rows, reference says %d",
						tc.col, tc.op, tc.mask, len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s %s %d: sel[%d] = %d, reference %d",
							tc.col, tc.op, tc.mask, i, got[i], want[i])
					}
				}
			})
		}
		rowStart += numRows
	}
	if totalEntries == 0 {
		t.Fatal("no dictionary entry was ever evaluated; this gate passed vacuously")
	}
	t.Logf("dictionary entries evaluated across the sweep: %d", totalEntries)
}

// A mask predicate over a column whose physical type is not an integer is an
// ERROR naming the type, not a silent mismatch: the planner only pushes over
// INT32/INT64 catalog columns, so reaching this arm means the file disagrees
// with the catalog and nothing here may guess which is right.
func TestAFlagPredicateOnANonIntegerColumnIsAnError(t *testing.T) {
	data, err := os.ReadFile(dictRunsFixture)
	if err != nil {
		t.Skipf("fixture missing: %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	numRows := int(fr.RowGroupNumRows(0))
	_, _, err = EvalRowGroupPreds(fr, 0,
		[]RowPred{{Col: "s_runs", Op: OpFlagsAll, Value: int64(2)}}, numRows)
	if err == nil {
		t.Fatal("a flag mask over a BYTE_ARRAY column answered; it must be an error")
	}
}

// --- helpers ---

func materializeSel(sel []uint32, dec FilterDecision, n int) []uint32 {
	switch dec {
	case FilterNone:
		return []uint32{}
	case FilterAll:
		out := make([]uint32, n)
		for i := range out {
			out[i] = uint32(i)
		}
		return out
	default:
		return sel
	}
}

func flagRowValue(v any) (int64, bool) {
	switch x := v.(type) {
	case int32:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

// writeFlagFile writes a one-column INT64 file with a single uncompressed page
// so the whole row group is one plain page.
func writeFlagFile(t testing.TB, vals []any, nullable bool) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "flags", Type: pqt.TypeInt64, Nullable: nullable},
	}}
	cfg := pqt.DefaultWriterConfig()
	cfg.Compression = pqt.CompressionNone
	cfg.PageBufferSize = 0
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"flags": v}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}
