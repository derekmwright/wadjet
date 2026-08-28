package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// boolFixture builds a 3-row BOOL column {TRUE, TRUE, FALSE} — the fixture
// #574 reports against.
func boolFixture(tb testing.TB) *batch.Vector {
	tb.Helper()
	b := batch.NewRecordBatch([]parquet.Column{{Name: "bo", Type: parquet.TypeBool}}, 3)
	b.Columns[0].SetValue(0, true)
	b.Columns[0].SetValue(1, true)
	b.Columns[0].SetValue(2, false)
	return b.Columns[0]
}

// selectCount runs a resolved kernel over the whole vector and reports how
// many rows it kept.
func selectCount(tb testing.TB, kern FilterKernel, vec *batch.Vector) int {
	tb.Helper()
	return len(kern(vec, nil, vec.Len, make([]uint32, 0, vec.Len)))
}

// TestBoolFilterKernelReadsBooleanTextGrammar is the vectorized half of #574:
// a BOOL column compared against a quoted text literal read every string as
// FALSE (toBool's default arm), so `bo = 't'` matched the one FALSE row
// instead of the two TRUE rows. Every accepted spelling of TRUE must now
// select both TRUE rows, every spelling of FALSE the one FALSE row.
func TestBoolFilterKernelReadsBooleanTextGrammar(t *testing.T) {
	vec := boolFixture(t)

	for _, tc := range []struct {
		lit  string
		want int // matches for `bo = lit` over {TRUE, TRUE, FALSE}
	}{
		// TRUE spellings, case-insensitive, with word prefixes and padding.
		{"t", 2}, {"true", 2}, {"TRUE", 2}, {"True", 2}, {"tr", 2},
		{"yes", 2}, {"y", 2}, {"on", 2}, {"1", 2}, {"  t  ", 2},
		// FALSE spellings.
		{"f", 1}, {"false", 1}, {"FALSE", 1}, {"fals", 1},
		{"no", 1}, {"n", 1}, {"off", 1}, {"0", 1},
	} {
		t.Run("eq_"+tc.lit, func(t *testing.T) {
			kern := ResolveFilterKernel(batch.TypeBool, OpEq, tc.lit)
			if kern == nil {
				t.Fatalf("ResolveFilterKernel returned nil for a valid literal %q", tc.lit)
			}
			if got := selectCount(t, kern, vec); got != tc.want {
				t.Errorf("bo = %q matched %d rows, want %d", tc.lit, got, tc.want)
			}
			// `<>` is the complement over the two non-NULL classes.
			neKern := ResolveFilterKernel(batch.TypeBool, OpNe, tc.lit)
			if neKern == nil {
				t.Fatalf("ResolveFilterKernel returned nil for a valid literal %q (OpNe)", tc.lit)
			}
			if got := selectCount(t, neKern, vec); got != vec.Len-tc.want {
				t.Errorf("bo <> %q matched %d rows, want %d", tc.lit, got, vec.Len-tc.want)
			}
		})
	}

	// A genuine Go bool constant (a bound parameter) still works.
	if kern := ResolveFilterKernel(batch.TypeBool, OpEq, true); kern == nil || selectCount(t, kern, vec) != 2 {
		t.Errorf("bo = TRUE (Go bool) did not select the two TRUE rows")
	}
}

// TestBoolFilterKernelRefusesNonBooleanLiteral: a literal that names no
// boolean returns no kernel, the "nil kernel, caller raises 22P02"
// convention exec.boolConstError implements — never a silent FALSE match.
func TestBoolFilterKernelRefusesNonBooleanLiteral(t *testing.T) {
	for _, lit := range []string{"maybe", "tru3", "o", "", "2", "10", "yesno"} {
		if kern := ResolveFilterKernel(batch.TypeBool, OpEq, lit); kern != nil {
			t.Errorf("ResolveFilterKernel returned a kernel for non-boolean literal %q; want nil", lit)
		}
	}
}

// TestInFilterBoolReadsGrammarAndRefuses covers the IN arm sharing the same
// binding: `bo IN ('t','no')` reads {TRUE, FALSE}, and an unparseable member
// refuses the whole list.
func TestInFilterBoolReadsGrammarAndRefuses(t *testing.T) {
	vec := boolFixture(t)

	kern := ResolveInFilterKernel(batch.TypeBool, []any{"t", "no"}, false)
	if kern == nil {
		t.Fatal("ResolveInFilterKernel returned nil for a valid bool IN list")
	}
	if got := selectCount(t, kern, vec); got != 3 {
		t.Errorf("bo IN ('t','no') matched %d rows, want all 3", got)
	}

	trueOnly := ResolveInFilterKernel(batch.TypeBool, []any{"yes"}, false)
	if trueOnly == nil || selectCount(t, trueOnly, vec) != 2 {
		t.Errorf("bo IN ('yes') did not select the two TRUE rows")
	}

	if kern := ResolveInFilterKernel(batch.TypeBool, []any{"t", "maybe"}, false); kern != nil {
		t.Error("ResolveInFilterKernel returned a kernel for an IN list holding a non-boolean; want nil")
	}
}

// TestParseBoolTextGrammar pins the shared binding directly against the
// spellings PostgreSQL's parse_bool accepts and the one prefix it refuses.
func TestParseBoolTextGrammar(t *testing.T) {
	for _, tc := range []struct {
		in       string
		val, ok  bool
	}{
		{"t", true, true}, {"true", true, true}, {"TrUe", true, true},
		{"tr", true, true}, {"yes", true, true}, {"y", true, true},
		{"on", true, true}, {"1", true, true}, {" \tYES\n", true, true},
		{"f", false, true}, {"false", false, true}, {"FALSE", false, true},
		{"fals", false, true}, {"no", false, true}, {"n", false, true},
		{"off", false, true}, {"0", false, true},
		// Refused: "o" cannot choose on/off; the rest name no boolean.
		{"o", false, false}, {"maybe", false, false}, {"", false, false},
		{"2", false, false}, {"10", false, false}, {"truer", false, false},
	} {
		val, ok := ParseBoolText(tc.in)
		if ok != tc.ok || (ok && val != tc.val) {
			t.Errorf("ParseBoolText(%q) = (%v,%v), want (%v,%v)", tc.in, val, ok, tc.val, tc.ok)
		}
	}
}
