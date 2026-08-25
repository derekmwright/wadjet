package catalog

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCompareStatValuesCidrInetBoundOrdersByKey pins the AggregateColumnStats
// cross-file merge to PostgreSQL's inet order for a confirmed CIDR bound
// (parquet.CidrInetBound), not to a text comparison and not to "equal".
//
// Before the fix, compareStatValues had no arm for CidrInetBound and fell
// through to `return 0` — every pair of CIDR bounds compared EQUAL there,
// so AggregateColumnStats' merge (`cur.MinValue == nil ||
// compareStatValues(...) < 0`) kept whichever file's bound happened to
// arrive first and silently discarded every other file's, for MIN and MAX
// alike. "9.0.0.0/8" sorts ABOVE "10.0.0.0/24" as TEXT and BELOW it as an
// ADDRESS (the same #565/#546 shape one layer up), so this pair also proves
// the fix reads Key (inet order), not Text.
func TestCompareStatValuesCidrInetBoundOrdersByKey(t *testing.T) {
	lowText, highText := "9.0.0.0/8", "10.0.0.0/24"
	if !(highText < lowText) {
		t.Fatalf("test setup: %q must sort below %q as TEXT for this pair to distinguish the bug", highText, lowText)
	}
	lowKey, ok := kernel.CidrSortKey(lowText)
	if !ok {
		t.Fatalf("kernel.CidrSortKey(%q) refused", lowText)
	}
	highKey, ok := kernel.CidrSortKey(highText)
	if !ok {
		t.Fatalf("kernel.CidrSortKey(%q) refused", highText)
	}
	if !(lowKey < highKey) {
		t.Fatalf("test setup: %q's inet key must sort below %q's for this pair to distinguish the bug", lowText, highText)
	}

	low := parquet.CidrInetBound{Key: lowKey, Text: lowText}
	high := parquet.CidrInetBound{Key: highKey, Text: highText}

	if got := compareStatValues(low, high); got != -1 {
		t.Errorf("compareStatValues(%v, %v) = %d, want -1 (inet order, %q below %q)", low, high, got, lowText, highText)
	}
	if got := compareStatValues(high, low); got != 1 {
		t.Errorf("compareStatValues(%v, %v) = %d, want 1", high, low, got)
	}
	if got := compareStatValues(low, low); got != 0 {
		t.Errorf("compareStatValues(%v, %v) = %d, want 0 (a bound equals itself)", low, low, got)
	}

	// Two DIFFERENT stored spellings of the SAME address ("10.0.0.1" and
	// "10.0.0.1/32") are one value in inet order (#546's own example) — the
	// bug this guards against would have called this pair equal too, but
	// for the wrong reason (compareStatValues never looked at either
	// value), which is indistinguishable from the correct answer without a
	// case that actually disagrees, like the pair above.
	bare, _ := kernel.CidrSortKey("10.0.0.1")
	host, _ := kernel.CidrSortKey("10.0.0.1/32")
	if got := compareStatValues(
		parquet.CidrInetBound{Key: bare, Text: "10.0.0.1"},
		parquet.CidrInetBound{Key: host, Text: "10.0.0.1/32"},
	); got != 0 {
		t.Errorf("compareStatValues found two spellings of one address unequal: got %d, want 0", got)
	}
}

// TestCompareStatValuesUnrecognizedPairPanics pins the loud default: a
// stat-value pair no arm recognizes must panic rather than silently answer
// "equal" the way the CIDR case did before it had its own arm. Exercised
// with a type that has genuinely never had a case here (not CidrInetBound,
// which now does) so the panic path itself stays covered as new boxed
// stat-value types are added in the future.
func TestCompareStatValuesUnrecognizedPairPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("compareStatValues did not panic on an unrecognized stat-value pair")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "compareStatValues") {
			t.Fatalf("panic value = %#v, want a compareStatValues-attributed message", r)
		}
	}()
	compareStatValues(true, true)
}

// TestCompareStatValuesMismatchedTypesPanics: a CIDR bound on one side and a
// plain string on the other is a type mismatch between what should be the
// same column's stats across two files — also loud, not "equal", and not
// silently read as a same-type pair.
func TestCompareStatValuesMismatchedTypesPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("compareStatValues did not panic on a CIDR-bound-vs-string mismatch")
		}
	}()
	compareStatValues(parquet.CidrInetBound{Key: "k", Text: "10.0.0.1"}, "10.0.0.1")
}
