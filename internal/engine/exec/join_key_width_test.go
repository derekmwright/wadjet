package exec

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ADR-0023 invariant for a cross-WIDTH join key, stated over the encoder
// rather than over a query (#615).
//
// A key only has to be injective and order-stable, and it is allowed to fold
// whatever the comparator calls equal. What it is NOT allowed to do is fold
// DIFFERENTLY on the two sides of one comparison: the join matches by BYTE
// equality of the two keys, so "these bytes are equal" has to be exactly
// "PostgreSQL says these two values are equal at their common type", in both
// directions, for every ordered pair of numeric types.
//
// The oracle below is written from the type rules, not from the encoder: it
// reads each cell as an exact rational (math/big.Rat, which holds every
// int64, every float64 and every scaled Int128 exactly), decides the common
// type from a table transcribed from PostgreSQL 17.11's EXPLAIN VERBOSE, and
// compares AT that type — exact for the integer and DECIMAL rungs, and
// through the correctly-rounded float64 of each side for the float rungs.
// A bug shared between the encoder and the oracle is the one thing a
// same-source test cannot catch, so the two share nothing.

// jkwCell is one fixture value: a vector of length 1 plus the exact rational
// it holds.
type jkwCell struct {
	name string
	typ  batch.TypeID
	vec  *batch.Vector
	rat  *big.Rat // nil for NaN, which is not a rational
	nan  bool
}

func jkwInt32(v int32) jkwCell {
	vec := batch.NewVector(batch.TypeInt32, 1)
	vec.Int32Data[0] = v
	return jkwCell{fmt.Sprintf("int32(%d)", v), batch.TypeInt32, vec, new(big.Rat).SetInt64(int64(v)), false}
}

func jkwInt64(v int64) jkwCell {
	vec := batch.NewVector(batch.TypeInt64, 1)
	vec.Int64Data[0] = v
	return jkwCell{fmt.Sprintf("int64(%d)", v), batch.TypeInt64, vec, new(big.Rat).SetInt64(v), false}
}

func jkwFloat32(v float32) jkwCell {
	vec := batch.NewVector(batch.TypeFloat32, 1)
	vec.Float32Data[0] = v
	c := jkwCell{fmt.Sprintf("float32(%v)", v), batch.TypeFloat32, vec, nil, false}
	if math.IsNaN(float64(v)) {
		c.nan = true
		return c
	}
	c.rat = new(big.Rat).SetFloat64(float64(v))
	return c
}

func jkwFloat64(v float64) jkwCell {
	vec := batch.NewVector(batch.TypeFloat64, 1)
	vec.Float64Data[0] = v
	c := jkwCell{fmt.Sprintf("float64(%v)", v), batch.TypeFloat64, vec, nil, false}
	if math.IsNaN(v) {
		c.nan = true
		return c
	}
	c.rat = new(big.Rat).SetFloat64(v)
	return c
}

// jkwDecimal builds a DECIMAL cell from its UNSCALED integer and scale, the
// carrier's own terms — so the fixture states 12.7501 as (127501, 4) and no
// float ever touches it.
func jkwDecimal(unscaled int64, scale int) jkwCell {
	vec := batch.NewVectorWithScale(batch.TypeDecimal, 1, scale)
	vec.DecimalData.Scale = scale
	vec.DecimalData.Data[0] = batch.Int128From(unscaled)
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	r := new(big.Rat).SetFrac(big.NewInt(unscaled), den)
	return jkwCell{fmt.Sprintf("decimal(%d,%d)", unscaled, scale), batch.TypeDecimal, vec, r, false}
}

// jkwCells is the fixture. Every value that is exactly representable in more
// than one of the five types appears in each of them, so the ordered pairs
// are not vacuous in the "nothing can ever match" direction; and every value
// that is representable in one and NOT the other appears too, which is the
// direction a key at the narrow side's width gets wrong.
func jkwCells() []jkwCell {
	return []jkwCell{
		jkwInt32(0), jkwInt32(2), jkwInt32(3), jkwInt32(-20),
		jkwInt32(16777217), jkwInt32(2147483647),

		jkwInt64(0), jkwInt64(2), jkwInt64(3), jkwInt64(-20),
		jkwInt64(16777217), jkwInt64(2147483648),
		// 2^53+1, exact as an int64 and not as a float64. Against
		// float64(9007199254740992) PostgreSQL says EQUAL, because the pair
		// resolves to float8 and the bigint rounds on the way in.
		jkwInt64(9007199254740993),

		jkwFloat32(0), jkwFloat32(float32(math.Copysign(0, -1))),
		jkwFloat32(2), jkwFloat32(3), jkwFloat32(-20), jkwFloat32(1.5),
		// 0.1 is exact in NEITHER float, and float32(0.1) widened is a
		// DIFFERENT double from 0.1 — the pair the whole float rung turns on.
		jkwFloat32(0.1), jkwFloat32(12.75),
		// 2^24, the last integer float32 can follow; 16777217 is not.
		jkwFloat32(16777216),
		jkwFloat32(float32(math.NaN())),

		jkwFloat64(0), jkwFloat64(math.Copysign(0, -1)),
		jkwFloat64(2), jkwFloat64(3), jkwFloat64(-20), jkwFloat64(1.5),
		jkwFloat64(0.1), jkwFloat64(float64(float32(0.1))),
		jkwFloat64(12.75), jkwFloat64(12.7501),
		jkwFloat64(16777216), jkwFloat64(16777217),
		jkwFloat64(9007199254740992),
		jkwFloat64(math.NaN()),

		// The same quantities at two scales, plus one that differs only at
		// the wider one: 12.75 against 12.7501.
		jkwDecimal(0, 2), jkwDecimal(200, 2), jkwDecimal(300, 2),
		jkwDecimal(-2000, 2), jkwDecimal(10, 2), jkwDecimal(1275, 2), jkwDecimal(150, 2),
		jkwDecimal(0, 4), jkwDecimal(20000, 4), jkwDecimal(30000, 4),
		jkwDecimal(-200000, 4), jkwDecimal(1000, 4), jkwDecimal(127500, 4),
		jkwDecimal(127501, 4), jkwDecimal(167772170000, 4),
		// A value past 2^53 at a non-zero scale, which is where the
		// DECIMAL → float64 conversion leaves the exactly-representable band
		// and has to go through the value's own digits to stay correctly
		// rounded.
		jkwDecimal(900719925474099300, 2),
	}
}

// jkwCommonType is PostgreSQL 17.11's OPERATOR resolution for an equality's
// operand pair, transcribed from EXPLAIN VERBOSE over a table with one column
// of each type (see physical.joinKeyCommonType's comment for the transcript).
// It is written here independently so this test does not assert the
// implementation against itself.
func jkwCommonType(a, b batch.TypeID) batch.TypeID {
	if a == b {
		return a
	}
	isInt := func(t batch.TypeID) bool { return t == batch.TypeInt32 || t == batch.TypeInt64 }
	switch {
	case isInt(a) && isInt(b):
		return batch.TypeInt64
	case a == batch.TypeFloat64 || b == batch.TypeFloat64:
		return batch.TypeFloat64
	case a == batch.TypeFloat32 || b == batch.TypeFloat32:
		// float4 against anything that is not another float4 is float8:
		// PostgreSQL has no float4 = int4 and no float4 = numeric operator,
		// so both go up.
		return batch.TypeFloat64
	default:
		// numeric ⊕ integer.
		return batch.TypeDecimal
	}
}

// jkwEqualAt is "does PostgreSQL call these two values equal at `common`".
func jkwEqualAt(a, b jkwCell, common batch.TypeID) bool {
	switch common {
	case batch.TypeFloat64, batch.TypeFloat32:
		// PostgreSQL's float order (ADR-0012 item 8): NaN equals itself and
		// nothing else, and -0.0 equals +0.0. Every non-NaN cell converts to
		// the correctly-rounded float64 of its exact value, which is what
		// `::float8` does to an int, a real and a numeric alike.
		if a.nan || b.nan {
			return a.nan && b.nan
		}
		fa, _ := a.rat.Float64()
		fb, _ := b.rat.Float64()
		return fa == fb
	default:
		// INT64 and DECIMAL are both EXACT rungs: the comparison is over the
		// values themselves, at whatever precision they need.
		if a.nan || b.nan {
			return false
		}
		return a.rat.Cmp(b.rat) == 0
	}
}

// jkwKey is the bytes one side of a join would put in the hash table for this
// cell, at the pair's resolved type — the exact call buildKeyFromBatch and
// buildProbeKey make.
func jkwKey(c jkwCell, target batch.TypeID) string {
	return string(AppendWidenedKeyValue(nil, c.vec, 0, target))
}

// TestCrossWidthJoinKeyBytesAgreeWithPostgresEquality is the gate: over every
// ordered pair of the fixture's cells, the two sides' key bytes are equal if
// and only if PostgreSQL calls the values equal at their common type.
func TestCrossWidthJoinKeyBytesAgreeWithPostgresEquality(t *testing.T) {
	cells := jkwCells()
	var mismatches int
	for _, a := range cells {
		for _, b := range cells {
			common := jkwCommonType(a.typ, b.typ)
			want := jkwEqualAt(a, b, common)
			got := jkwKey(a, common) == jkwKey(b, common)
			if got == want {
				continue
			}
			mismatches++
			if mismatches <= 40 {
				t.Errorf("%s vs %s at %s: key bytes equal=%v, PostgreSQL equality=%v\n"+
					"  %s key %x\n  %s key %x",
					a.name, b.name, common, got, want,
					a.name, jkwKey(a, common), b.name, jkwKey(b, common))
			}
		}
	}
	if mismatches > 40 {
		t.Errorf("... and %d more", mismatches-40)
	}
}

// TestCrossWidthJoinKeyFixtureIsNotVacuous asserts the fixture can actually
// tell the two rules apart. Without it the gate above passes for a fixture
// whose every cross-type pair is unequal (nothing to fold wrongly) or whose
// every pair is equal (nothing to split) — the shape that made #615 invisible
// for two years.
//
// Both directions are required PER RUNG, so a rung that quietly stops
// matching cannot hide behind another rung's pairs.
func TestCrossWidthJoinKeyFixtureIsNotVacuous(t *testing.T) {
	cells := jkwCells()
	type counts struct{ eq, ne int }
	byRung := map[string]*counts{}
	for _, a := range cells {
		for _, b := range cells {
			if a.typ == b.typ {
				continue
			}
			common := jkwCommonType(a.typ, b.typ)
			rung := fmt.Sprintf("%s/%s->%s", a.typ, b.typ, common)
			c := byRung[rung]
			if c == nil {
				c = &counts{}
				byRung[rung] = c
			}
			if jkwEqualAt(a, b, common) {
				c.eq++
			} else {
				c.ne++
			}
		}
	}
	if len(byRung) != 20 {
		t.Errorf("%d ordered cross-type rungs, want 20 (5 numeric types)", len(byRung))
	}
	for rung, c := range byRung {
		if c.eq == 0 || c.ne == 0 {
			t.Errorf("rung %s is vacuous: %d equal / %d unequal pairs — a fixture that "+
				"cannot distinguish a key at the common type from a key at each side's "+
				"own width passes for the wrong reason", rung, c.eq, c.ne)
		}
	}
}

// TestSameTypeJoinKeyBytesAreUnchanged is the performance and blast-radius
// claim, asserted rather than argued: for a pair whose two sides already
// agree, AppendWidenedKeyValue produces byte-for-byte what appendColumnValue
// always produced, and KeyTypeAt returns the column's own type so nothing
// downstream sees a difference. Every TPC-H join is this case.
func TestSameTypeJoinKeyBytesAreUnchanged(t *testing.T) {
	for _, c := range jkwCells() {
		old := string(appendColumnValue(nil, c.vec, 0, c.vec.Type))
		for _, target := range []batch.TypeID{c.typ, KeyTypeUnresolved} {
			got := string(AppendWidenedKeyValue(nil, c.vec, 0, KeyTypeAt([]batch.TypeID{target}, 0, c.typ)))
			if got != old {
				t.Errorf("%s at target %v: %x, want the unwidened %x", c.name, target, got, old)
			}
		}
	}
}

// TestJoinKeyIntPathIsGatedOnTheResolvedType pins the gate the panic came
// through: an integer BUILD column whose pair resolves to FLOAT64 or DECIMAL
// must NOT take the integer fast path, because the probe column then has no
// Int32Data/Int64Data for those loops to index.
func TestJoinKeyIntPathIsGatedOnTheResolvedType(t *testing.T) {
	for _, tc := range []struct {
		own      batch.TypeID
		resolved batch.TypeID
		want     bool
	}{
		{batch.TypeInt64, KeyTypeUnresolved, true},    // same-type int join: unchanged
		{batch.TypeInt32, KeyTypeUnresolved, true},    //
		{batch.TypeInt32, batch.TypeInt64, true},      // int4 ⊕ int8 stays on the int path
		{batch.TypeInt64, batch.TypeFloat64, false},   // #615's inlineIntProbe panic
		{batch.TypeInt32, batch.TypeFloat64, false},   //
		{batch.TypeInt64, batch.TypeDecimal, false},   // #615's executeSemiAntiJoin panic
		{batch.TypeInt32, batch.TypeDecimal, false},   //
		{batch.TypeFloat64, KeyTypeUnresolved, false}, // never was an int path
		{batch.TypeDecimal, batch.TypeDecimal, false}, //
		{batch.TypeDate, KeyTypeUnresolved, true},     // the non-numeric int-class types are untouched
		{batch.TypeIPv4, KeyTypeUnresolved, true},
	} {
		got := joinKeyUsesIntPath([]batch.TypeID{tc.resolved}, 0, tc.own)
		if got != tc.want {
			t.Errorf("joinKeyUsesIntPath(own=%v, resolved=%v) = %v, want %v",
				tc.own, tc.resolved, got, tc.want)
		}
	}
	// An absent list is every pre-#615 caller and must answer from the
	// column alone.
	if !joinKeyUsesIntPath(nil, 0, batch.TypeInt64) {
		t.Error("a nil KeyTypes must leave the integer fast path exactly where it was")
	}
}

// TestCrossWidthJoinKeySurvivesASpill is the spill boundary of #615.
//
// A grace-partitioned build replays each spilled partition through a TEMPORARY
// HashJoin assembled from the spilled batches (buildTempJoinFromBatches), and
// that temporary has to inherit the pair's resolved key type along with the
// keys. Without it the replayed partition rebuilds its index at each column's
// OWN encoding — a DECIMAL key against a BIGINT one — and the join stops
// matching past the spill boundary ONLY, which is the class of wrong answer
// that never shows up in a small test.
//
// The build is BIGINT and the probe DECIMAL, so the pair resolves to DECIMAL:
// the shape that also drove the integer fast path over a column with no
// integer storage. Both halves are asserted here — the row count, and that
// the spill actually happened.
func TestCrossWidthJoinKeySurvivesASpill(t *testing.T) {
	const buildN = 5000
	buildSchema := []parquet.Column{
		{Name: "bk", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	buildRows := make([]map[string]any, 0, buildN)
	for i := 0; i < buildN; i++ {
		buildRows = append(buildRows, map[string]any{"bk": int64(i), "val": "b"})
	}

	// The probe is DECIMAL(18,2), and every value is an exact integer at that
	// scale — so PostgreSQL's `numeric = bigint` matches exactly the probe
	// rows whose value is in [0, buildN). Half of them are deliberately
	// FRACTIONAL, which no bigint can equal: a key that silently compared
	// the unscaled carrier instead of the value would match those too.
	const probeN = 1000
	probeSchema := []parquet.Column{
		{Name: "pk", Type: parquet.TypeDecimal, Precision: 18, Scale: 2},
		{Name: "label", Type: parquet.TypeString},
	}
	// A FRESH probe batch per run: the probe operator writes a selection
	// vector into the batch it is handed, so reusing one across the two arms
	// would make the second arm read the first one's leftovers.
	wantMatches := 0
	for i := 0; i < probeN; i += 2 {
		if i < buildN {
			wantMatches++
		}
	}
	newProbeBatch := func() *batch.RecordBatch {
		b := batch.NewRecordBatch(probeSchema, probeN)
		b.Len = probeN
		b.Columns[0].DecimalData.Scale = 2
		for i := 0; i < probeN; i++ {
			unscaled := int64(i) * 100 // i.00
			if i%2 == 1 {
				unscaled += 50 // i.50 — exact at scale 2, equal to no bigint
			}
			b.Columns[0].DecimalData.Data[i] = batch.Int128From(unscaled)
			b.Columns[1].BytesData.SetString(i, "p")
		}
		return b
	}

	run := func(t *testing.T, budget int64) (rows int, spilled bool) {
		t.Helper()
		tmpDir := t.TempDir()
		tracker := memory.NewTracker("crosswidth-spill", budget)
		sm, err := memory.NewSpillManager(tmpDir, tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Cleanup()

		hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
		// What the planner resolves for (DECIMAL pk, BIGINT bk): numeric.
		hj.KeyTypes = []batch.TypeID{batch.TypeDecimal}
		hj.Spill = sm
		hj.MemTracker = tracker
		if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
			t.Fatalf("build: %v", err)
		}
		spilled = hj.spillState != nil && len(hj.spillState.spilledParts) > 0

		sink := &CollectSink{}
		pipe := &Pipeline{
			Source: NewBatchSource([]*batch.RecordBatch{newProbeBatch()}),
			Ops:    []UnaryOperator{hj.Probe()},
			Sink:   sink,
		}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatalf("probe: %v", err)
		}
		return len(sink.Rows), spilled
	}

	// A budget that cannot hold the build forces the grace partitioning; a
	// generous one keeps it in memory. The two must agree, and both must
	// agree with the arithmetic.
	spilledRows, didSpill := run(t, 250_000)
	if !didSpill {
		t.Fatal("the build did not spill; this gate cannot see the boundary it exists for")
	}
	memRows, _ := run(t, 1<<30)
	if memRows != wantMatches {
		t.Errorf("in-memory join matched %d rows, want %d", memRows, wantMatches)
	}
	if spilledRows != wantMatches {
		t.Errorf("spilled join matched %d rows, want %d — a spilled partition is "+
			"rebuilding its index at the column's own encoding instead of the "+
			"pair's resolved type", spilledRows, wantMatches)
	}
}

// TestKeyEncodingClassGroupsWhatEncodesAlike pins the grouping the runtime
// backstop reads. It is coarser than the type on purpose: PORT, PROTOCOL and
// DATE key as an INT32's four little-endian bytes and IPv4, MAC, TIMESTAMP
// and DURATION as an INT64's eight, so a join between one of those and its
// carrier integer has always keyed alike and must not start erroring.
func TestKeyEncodingClassGroupsWhatEncodesAlike(t *testing.T) {
	same := [][]batch.TypeID{
		{batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate},
		{batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration},
		{batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID},
	}
	for _, grp := range same {
		for _, a := range grp {
			for _, b := range grp {
				if keyEncodingClass(a) != keyEncodingClass(b) {
					t.Errorf("%v and %v encode alike but are in different classes", a, b)
				}
			}
		}
	}
	// …and the pairs that genuinely differ must NOT collapse.
	differ := [][2]batch.TypeID{
		{batch.TypeInt32, batch.TypeInt64},
		{batch.TypeInt64, batch.TypeFloat64},
		{batch.TypeFloat32, batch.TypeFloat64},
		{batch.TypeInt64, batch.TypeDecimal},
		{batch.TypeDecimal, batch.TypeFloat64},
		{batch.TypeString, batch.TypeCIDR},
	}
	for _, p := range differ {
		if keyEncodingClass(p[0]) == keyEncodingClass(p[1]) {
			t.Errorf("%v and %v encode differently but share a class", p[0], p[1])
		}
	}
	// Every one of the 22 types has a class; 0 is the "unknown" hole and
	// nothing may fall into it.
	for id := batch.TypeBool; id <= batch.TypeVector; id++ {
		if keyEncodingClass(id) == 0 {
			t.Errorf("%v has no key encoding class", id)
		}
	}
}

// TestUnresolvedCrossWidthKeyRaisesInsteadOfMatching is the runtime backstop
// (#615 review). The planner resolves a key pair from DECLARED types, and a
// side it cannot type resolves to KeyTypeUnresolved — correct for a pair whose
// sides agree, silently wrong for one whose sides do not. Here is where that
// is caught, because here is where both sides' ACTUAL encodings are known.
//
// Two conditions and no others: the integer fast path over a non-integer
// probe (the panic), and a numeric pair whose encodings differ with nothing
// resolving them (the silent miss). A pair carrying a resolved type, and a
// pair whose two types encode alike, both go through untouched.
func TestUnresolvedCrossWidthKeyRaisesInsteadOfMatching(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "bk", Type: parquet.TypeInt64}}
	buildRows := []map[string]any{{"bk": int64(2)}, {"bk": int64(7)}}

	newProbe := func(typ parquet.TypeID) *batch.RecordBatch {
		b := batch.NewRecordBatch([]parquet.Column{
			{Name: "pk", Type: typ, Precision: 18, Scale: 2}}, 2)
		b.Len = 2
		switch typ {
		case parquet.TypeDecimal:
			b.Columns[0].DecimalData.Scale = 2
			b.Columns[0].DecimalData.Data[0] = batch.Int128From(200)
			b.Columns[0].DecimalData.Data[1] = batch.Int128From(700)
		case parquet.TypeInt64, parquet.TypeIPv4:
			b.Columns[0].Int64Data[0], b.Columns[0].Int64Data[1] = 2, 7
		case parquet.TypeFloat64:
			b.Columns[0].Float64Data[0], b.Columns[0].Float64Data[1] = 2, 7
		}
		return b
	}
	run := func(t *testing.T, probeType parquet.TypeID, keyTypes []batch.TypeID) (int, error) {
		t.Helper()
		hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
		hj.KeyTypes = keyTypes
		if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
			t.Fatalf("build: %v", err)
		}
		sink := &CollectSink{}
		pipe := &Pipeline{
			Source: NewBatchSource([]*batch.RecordBatch{newProbe(probeType)}),
			Ops:    []UnaryOperator{hj.Probe()},
			Sink:   sink,
		}
		err := pipe.Run(context.Background())
		return len(sink.Rows), err
	}

	t.Run("UnresolvedDecimalAgainstBigintRaises", func(t *testing.T) {
		_, err := run(t, parquet.TypeDecimal, nil)
		if err == nil {
			t.Fatal("an unresolved DECIMAL/INT64 key pair answered instead of raising — " +
				"that is the silent miss #615 is about, and the integer fast path over " +
				"it is the panic")
		}
		if !strings.Contains(err.Error(), "#615") {
			t.Errorf("the error does not name the mechanism: %v", err)
		}
	})
	t.Run("UnresolvedFloatAgainstBigintRaises", func(t *testing.T) {
		if _, err := run(t, parquet.TypeFloat64, nil); err == nil {
			t.Fatal("an unresolved FLOAT64/INT64 key pair answered instead of raising")
		}
	})
	t.Run("ResolvedPairAnswers", func(t *testing.T) {
		n, err := run(t, parquet.TypeDecimal, []batch.TypeID{batch.TypeDecimal})
		if err != nil {
			t.Fatalf("a RESOLVED pair must answer, not raise: %v", err)
		}
		if n != 2 {
			t.Errorf("resolved DECIMAL/INT64 join matched %d rows, want 2", n)
		}
	})
	t.Run("SameEncodingClassIsUntouched", func(t *testing.T) {
		// IPv4 stores in Int64Data and keys as an INT64's eight bytes, so a
		// join against a BIGINT has always matched and must keep matching —
		// the backstop is about ENCODINGS, not about type names.
		n, err := run(t, parquet.TypeIPv4, nil)
		if err != nil {
			t.Fatalf("IPv4 against BIGINT must not raise: %v", err)
		}
		if n != 2 {
			t.Errorf("IPv4/BIGINT join matched %d rows, want 2", n)
		}
	})
	t.Run("SameTypeIsUntouched", func(t *testing.T) {
		n, err := run(t, parquet.TypeInt64, nil)
		if err != nil {
			t.Fatalf("a same-type join must not raise: %v", err)
		}
		if n != 2 {
			t.Errorf("INT64/INT64 join matched %d rows, want 2", n)
		}
	})
}

// TestIntTargetKeyRefusesAVectorWithNoIntegerReading is the silent fan-out
// the re-review found. appendCoercedKeyValue's INT64 arm discarded
// intKeyFromVector's ok, and that function answers (0, false) for every type
// it has no integer reading of — so a DECIMAL or a STRING vector keyed as
// eight ZERO bytes for EVERY row. Not a wrong row here and there: 12.75, 2.00
// and -20.00 became ONE key, the whole build side landing in one bucket.
//
// The three arms are asserted together, because the fan-out is the difference
// between them: the DECIMAL and FLOAT arms already raised.
func TestIntTargetKeyRefusesAVectorWithNoIntegerReading(t *testing.T) {
	dec := batch.NewVectorWithScale(batch.TypeDecimal, 3, 2)
	dec.DecimalData.Scale = 2
	dec.DecimalData.Data[0] = batch.Int128From(1275)
	dec.DecimalData.Data[1] = batch.Int128From(200)
	dec.DecimalData.Data[2] = batch.Int128From(-2000)

	// The bug, stated as the property it broke: three distinct values must
	// not produce one key.
	t.Run("DecimalAtIntTargetRaises", func(t *testing.T) {
		keys := map[string]bool{}
		for row := 0; row < 3; row++ {
			func() {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("row %d keyed at INT64 without raising", row)
					}
				}()
				keys[string(AppendWidenedKeyValue(nil, dec, row, batch.TypeInt64))] = true
			}()
		}
		if len(keys) != 0 {
			t.Errorf("a DECIMAL keyed at INT64 produced %d key(s); it must produce none, "+
				"and before the fix it produced ONE for all three values", len(keys))
		}
	})
	t.Run("StringAtIntTargetRaises", func(t *testing.T) {
		sv := batch.NewVector(batch.TypeString, 1)
		sv.BytesData.SetString(0, "x")
		defer func() {
			if r := recover(); r == nil {
				t.Error("a STRING keyed at INT64 did not raise")
			}
		}()
		_ = AppendWidenedKeyValue(nil, sv, 0, batch.TypeInt64)
	})
	t.Run("IntAtIntTargetStillWorks", func(t *testing.T) {
		iv := batch.NewVector(batch.TypeInt32, 2)
		iv.Int32Data[0], iv.Int32Data[1] = 7, 8
		a := string(AppendWidenedKeyValue(nil, iv, 0, batch.TypeInt64))
		b := string(AppendWidenedKeyValue(nil, iv, 1, batch.TypeInt64))
		if a == b || len(a) != 8 {
			t.Errorf("INT32 at INT64 target: keys %x / %x, want two distinct 8-byte keys", a, b)
		}
	})
}

// TestCanEncodeKeyAtIsTheEncodersOwnTable keeps the backstop's admission test
// and the encoder's arms from drifting: every (vector, target) pair
// canEncodeKeyAt admits must actually encode, and every pair it refuses must
// actually raise.
func TestCanEncodeKeyAtIsTheEncodersOwnTable(t *testing.T) {
	mk := func(tp batch.TypeID) *batch.Vector {
		v := batch.NewVectorWithScale(tp, 1, 2)
		switch tp {
		case batch.TypeDecimal:
			v.DecimalData.Scale = 2
			v.DecimalData.Data[0] = batch.Int128From(200)
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID, batch.TypeCIDR:
			v.BytesData.SetString(0, "1")
		}
		return v
	}
	vecs := []batch.TypeID{
		batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64,
		batch.TypeDecimal, batch.TypeString, batch.TypeBool, batch.TypeDate,
		batch.TypeIPv4, batch.TypeUUID,
	}
	targets := []batch.TypeID{batch.TypeInt64, batch.TypeFloat64, batch.TypeDecimal}
	for _, vt := range vecs {
		for _, target := range targets {
			admitted := canEncodeKeyAt(vt, target)
			raised := func() (r bool) {
				defer func() {
					if recover() != nil {
						r = true
					}
				}()
				_ = AppendWidenedKeyValue(nil, mk(vt), 0, target)
				return false
			}()
			if admitted == raised {
				t.Errorf("canEncodeKeyAt(%v, %v)=%v but the encoder raised=%v — the "+
					"backstop's table and the encoder's arms have drifted", vt, target, admitted, raised)
			}
		}
	}
}

// TestResolvedKeyPairChecksTheACTUALVectors is the second half of the
// re-review finding: the backstop short-circuited on "is the pair resolved"
// and never compared the resolved type to the vectors that actually arrived.
// A pair declared INT64 whose build vector is really a DECIMAL was therefore
// ACCEPTED — and then keyed as zeroes by the arm above.
func TestResolvedKeyPairChecksTheACTUALVectors(t *testing.T) {
	decBuild := []parquet.Column{{Name: "bk", Type: parquet.TypeDecimal, Precision: 18, Scale: 2}}
	strBuild := []parquet.Column{{Name: "bk", Type: parquet.TypeString}}

	buildBatch := func(schema []parquet.Column) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 2)
		b.Len = 2
		switch schema[0].Type {
		case parquet.TypeDecimal:
			b.Columns[0].DecimalData.Scale = 2
			b.Columns[0].DecimalData.Data[0] = batch.Int128From(1275)
			b.Columns[0].DecimalData.Data[1] = batch.Int128From(200)
		case parquet.TypeString:
			b.Columns[0].BytesData.SetString(0, "a")
			b.Columns[0].BytesData.SetString(1, "b")
		}
		return b
	}
	probeBatch := func(tp parquet.TypeID) *batch.RecordBatch {
		b := batch.NewRecordBatch([]parquet.Column{{Name: "pk", Type: tp}}, 2)
		b.Len = 2
		switch tp {
		case parquet.TypeInt64:
			b.Columns[0].Int64Data[0], b.Columns[0].Int64Data[1] = 12, 2
		case parquet.TypeFloat64:
			b.Columns[0].Float64Data[0], b.Columns[0].Float64Data[1] = 12, 2
		}
		return b
	}
	run := func(t *testing.T, build []parquet.Column, probeType parquet.TypeID,
		keyTypes []batch.TypeID) error {
		t.Helper()
		hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
		hj.KeyTypes = keyTypes
		if err := hj.Build(context.Background(),
			NewBatchSource([]*batch.RecordBatch{buildBatch(build)})); err != nil {
			return err
		}
		pipe := &Pipeline{
			Source: NewBatchSource([]*batch.RecordBatch{probeBatch(probeType)}),
			Ops:    []UnaryOperator{hj.Probe()},
			Sink:   &CollectSink{},
		}
		return pipe.Run(context.Background())
	}

	t.Run("DeclaredIntOverADecimalBuildRaises", func(t *testing.T) {
		err := run(t, decBuild, parquet.TypeInt64, []batch.TypeID{batch.TypeInt64})
		if err == nil {
			t.Fatal("a pair declared INT64 over a DECIMAL build vector was ACCEPTED; " +
				"that is the declared-vs-actual drift, and the INT64 arm then keys " +
				"every row as eight zero bytes")
		}
		if !strings.Contains(err.Error(), "#615") {
			t.Errorf("the error does not name the mechanism: %v", err)
		}
	})
	t.Run("DeclaredFloatOverAStringBuildIsAnErrorReturn", func(t *testing.T) {
		// The build is keyed FIRST, so this has to be caught before the
		// encoder raises mid-build — an error the build path returns, not a
		// recovered panic.
		err := run(t, strBuild, parquet.TypeFloat64, []batch.TypeID{batch.TypeFloat64})
		if err == nil {
			t.Fatal("a pair declared FLOAT64 over a STRING build vector was accepted")
		}
		if strings.Contains(err.Error(), "internal error") || IsQueryPanicMessage(err.Error()) {
			t.Errorf("this must be an error RETURN from the build, not a recovered panic: %v", err)
		}
	})
	t.Run("DeclaredDecimalOverADecimalBuildAnswers", func(t *testing.T) {
		if err := run(t, decBuild, parquet.TypeInt64,
			[]batch.TypeID{batch.TypeDecimal}); err != nil {
			t.Errorf("a correctly resolved DECIMAL pair must answer, not raise: %v", err)
		}
	})
}
