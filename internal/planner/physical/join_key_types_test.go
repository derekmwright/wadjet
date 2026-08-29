package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestJoinKeyLadderMatchesPostgresOperatorResolution pins joinKeyCommonType
// against a transcript taken live off postgres:17.11's EXPLAIN VERBOSE, over
// a table with one column of each numeric type. The transcript, verbatim:
//
//	ON x.a = y.b  (int4, int8)     Merge Cond: (x.a = y.b)
//	ON x.a = y.c  (int4, float4)   Merge Cond: (((x.a)::double precision) = y.c)
//	ON x.a = y.e  (int4, numeric)  Merge Cond: (((x.a)::numeric) = y.e)
//	ON x.b = y.d  (int8, float8)   Merge Cond: (((x.b)::double precision) = y.d)
//	ON x.c = y.d  (float4, float8) Merge Cond: (x.c = y.d)            [float48eq]
//	ON x.c = y.e  (float4, numeric) Merge Cond: (x.c = ((y.e)::double precision))
//	ON x.d = y.f  (float8, numeric) Merge Cond: (x.d = ((y.f)::double precision))
//	ON x.e = y.f  (numeric(9,2), numeric(18,4)) Merge Cond: (x.e = y.f)
//
// Two readings need stating because the cast printed is not always the whole
// answer. `((x.a)::double precision) = y.c` is float8 = float4, which
// PostgreSQL resolves with float84eq — the float4 side widens too, so the
// comparison happens at float8. `x.c = y.d` prints no cast at all and is
// float48eq, which likewise compares at float8. Both are float8 rungs here.
//
// This is the OPERATOR ladder. The SET-OPERATION ladder (setOpWiden,
// TestSetOpWidenLadder) is different where float4 is involved — `numeric ∪
// real` is real and `int ∪ real` is real — and the two must not be merged.
func TestJoinKeyLadderMatchesPostgresOperatorResolution(t *testing.T) {
	const (
		i32 = parquet.TypeInt32
		i64 = parquet.TypeInt64
		f32 = parquet.TypeFloat32
		f64 = parquet.TypeFloat64
		dec = parquet.TypeDecimal
	)
	cases := []struct {
		a, b parquet.TypeID
		want parquet.TypeID
		ok   bool
	}{
		// The diagonal: nothing to widen, and the KEY must not move.
		{i32, i32, 0, false}, {i64, i64, 0, false}, {f32, f32, 0, false},
		{f64, f64, 0, false}, {dec, dec, 0, false},

		{i32, i64, i64, true}, {i64, i32, i64, true},
		{i32, f32, f64, true}, {f32, i32, f64, true},
		{i64, f32, f64, true}, {f32, i64, f64, true},
		{i32, f64, f64, true}, {f64, i32, f64, true},
		{i64, f64, f64, true}, {f64, i64, f64, true},
		{f32, f64, f64, true}, {f64, f32, f64, true},
		{i32, dec, dec, true}, {dec, i32, dec, true},
		{i64, dec, dec, true}, {dec, i64, dec, true},
		{f32, dec, f64, true}, {dec, f32, f64, true},
		{f64, dec, f64, true}, {dec, f64, f64, true},

		// Off the ladder entirely: a STRING key, a DATE against a TIMESTAMP,
		// an IPv4 against a BIGINT. Declining leaves the encoding exactly
		// where it was — widening them is a different question with a
		// different authority, and there is no PostgreSQL transcript here to
		// answer it with.
		{parquet.TypeString, parquet.TypeString, 0, false},
		{parquet.TypeDate, parquet.TypeTimestamp, 0, false},
		{parquet.TypeIPv4, i64, 0, false},
		{i64, parquet.TypeIPv4, 0, false},
		{parquet.TypeBool, i32, 0, false},
	}
	for _, c := range cases {
		got, ok := joinKeyCommonType(c.a, c.b)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("joinKeyCommonType(%v, %v) = (%v, %v), want (%v, %v)",
				c.a, c.b, got, ok, c.want, c.ok)
		}
	}
	// The ladder must be SYMMETRIC: which side of the `=` a column is
	// spelled on cannot change the type both sides key at, or
	// assignJoinKeySides' swap would change the answer.
	for _, a := range []parquet.TypeID{i32, i64, f32, f64, dec} {
		for _, b := range []parquet.TypeID{i32, i64, f32, f64, dec} {
			ab, aok := joinKeyCommonType(a, b)
			ba, bok := joinKeyCommonType(b, a)
			if aok != bok || ab != ba {
				t.Errorf("asymmetric: (%v,%v)=(%v,%v) but (%v,%v)=(%v,%v)",
					a, b, ab, aok, b, a, ba, bok)
			}
		}
	}
}

// TestResolveJoinKeyTypesDeclinesWhatItCannotType covers the answers that are
// deliberately "leave it alone": the pre-#615 behaviour, which is correct for
// every pair whose two sides already agree and is the only safe answer for a
// pair neither side can be typed for.
func TestResolveJoinKeyTypesDeclinesWhatItCannotType(t *testing.T) {
	scan := func(alias string, cols map[string]parquet.TypeID) *logical.Node {
		n := &logical.Node{Type: logical.NodeScan, TableName: alias}
		n.ScanColTypes = map[string]parquet.TypeID{}
		for c, tp := range cols {
			n.ScanColumns = append(n.ScanColumns, c)
			n.ScanColTypes[c] = tp
		}
		return n
	}
	join := func(l, r *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{l, r}}
	}

	t.Run("SameTypeIsNil", func(t *testing.T) {
		n := join(
			scan("a", map[string]parquet.TypeID{"x": parquet.TypeInt64}),
			scan("b", map[string]parquet.TypeID{"y": parquet.TypeInt64}))
		if got := resolveJoinKeyTypes(n, []string{"a.x"}, []string{"b.y"}); got != nil {
			t.Errorf("a same-type pair resolved %v, want nil — the operator must keep "+
				"the exact path it had before", got)
		}
	})
	t.Run("CrossWidthResolves", func(t *testing.T) {
		n := join(
			scan("a", map[string]parquet.TypeID{"x": parquet.TypeInt64}),
			scan("b", map[string]parquet.TypeID{"y": parquet.TypeDecimal}))
		got := resolveJoinKeyTypes(n, []string{"a.x"}, []string{"b.y"})
		if len(got) != 1 || got[0] != parquet.TypeDecimal {
			t.Errorf("int64 vs DECIMAL resolved %v, want [DECIMAL]", got)
		}
	})
	t.Run("UntypedKeyDeclines", func(t *testing.T) {
		n := join(
			scan("a", map[string]parquet.TypeID{"x": parquet.TypeInt64}),
			scan("b", map[string]parquet.TypeID{"y": parquet.TypeDecimal}))
		if got := resolveJoinKeyTypes(n, []string{"a.nosuch"}, []string{"b.y"}); got != nil {
			t.Errorf("an unresolvable key resolved %v, want nil", got)
		}
	})
	t.Run("AmbiguousNameDeclines", func(t *testing.T) {
		// One SIDE carrying the name at two different types cannot decide
		// the pair: resolving it against whichever scan the walk reached
		// first is a silently different join. Deleting the name is the same
		// answer inputColTypes gives for the same situation.
		inner := join(
			scan("a1", map[string]parquet.TypeID{"x": parquet.TypeInt64}),
			scan("a2", map[string]parquet.TypeID{"x": parquet.TypeFloat32}))
		n := join(inner, scan("b", map[string]parquet.TypeID{"y": parquet.TypeDecimal}))
		if got := resolveJoinKeyTypes(n, []string{"x"}, []string{"b.y"}); got != nil {
			t.Errorf("an ambiguous key resolved %v, want nil", got)
		}
	})
	t.Run("MixedPairsResolvePerPair", func(t *testing.T) {
		// Two key columns, one pair needing widening and one not: the
		// unmoved pair must come back KeyTypeUnresolved, not the other
		// pair's type.
		n := join(
			scan("a", map[string]parquet.TypeID{"x": parquet.TypeInt64, "s": parquet.TypeString}),
			scan("b", map[string]parquet.TypeID{"y": parquet.TypeFloat64, "t": parquet.TypeString}))
		got := resolveJoinKeyTypes(n, []string{"a.x", "a.s"}, []string{"b.y", "b.t"})
		if len(got) != 2 || got[0] != parquet.TypeFloat64 || got[1] != exec.KeyTypeUnresolved {
			t.Errorf("resolved %v, want [FLOAT64 unresolved]", got)
		}
	})
	t.Run("SemiAntiBuildThroughAProjectResolves", func(t *testing.T) {
		// `x IN (SELECT y FROM t)` narrows its build side to
		// Project(keys) → Distinct (dedupSemiAntiBuildSide), which is the
		// shape inputColTypes cannot see through — and the one #615's
		// executeSemiAntiJoin panic came out of.
		build := &logical.Node{
			Type: logical.NodeProject,
			Projections: []logical.Projection{
				{Column: "y", Alias: "y"},
			},
			Children: []*logical.Node{
				scan("b", map[string]parquet.TypeID{"y": parquet.TypeInt64}),
			},
		}
		n := join(scan("a", map[string]parquet.TypeID{"x": parquet.TypeDecimal}), build)
		got := resolveJoinKeyTypes(n, []string{"a.x"}, []string{"b.y"})
		if len(got) != 1 || got[0] != parquet.TypeDecimal {
			t.Errorf("a DECIMAL probe against a projected BIGINT build resolved %v, "+
				"want [DECIMAL] — a nil here is the panic coming back", got)
		}
	})
}

// TestKeyTypeUnresolvedSentinelsAgree holds the two spellings of "no
// widening" together. distribution.go keeps its own copy so the distribution
// algebra does not import the execution package for one constant, and the two
// disagreeing would make an exchange look interchangeable with one that
// hashes differently.
func TestKeyTypeUnresolvedSentinelsAgree(t *testing.T) {
	if keyTypeUnresolved != exec.KeyTypeUnresolved {
		t.Fatalf("keyTypeUnresolved = %v, exec.KeyTypeUnresolved = %v",
			keyTypeUnresolved, exec.KeyTypeUnresolved)
	}
	// And neither may collide with a real type.
	for i := 0; i <= int(parquet.TypeVector); i++ {
		if parquet.TypeID(i) == keyTypeUnresolved {
			t.Fatalf("the unresolved sentinel collides with %v", parquet.TypeID(i))
		}
	}
}
