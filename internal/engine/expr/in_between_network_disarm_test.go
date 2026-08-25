package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInWithCidrColumnStaysArmed guards the interaction between the In/
// Between disarm fast-path (pairSetState) and the network cluster's own
// boxed-pair rule for CIDR (#565, boxCidr): a CIDR column against a quoted
// literal ALWAYS carries a declaration-driven rule (pairApplies's
// `lk == boxCidr || rk == boxCidr` case has no other-side condition), so a
// list built entirely of CIDR members must never reach the fully-disarmed
// fast path — doing so would silently fall back to compare()'s plain text
// equality, which is exactly the #565 defect the boxed CIDR rule exists to
// close ("10.0.0.1" and "10.0.0.1/32" are one address in inet order and two
// different strings in text order).
func TestInWithCidrColumnStaysArmed(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeCIDR}}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "10.0.0.1")  // bare address == its own /32 route
	b.Columns[0].SetValue(1, "9.0.0.0/8") // text-sorts ABOVE "10.0.0.0/24", inet-sorts below
	b.Columns[0].SetValue(2, "192.0.2.5")
	b.Len = 3

	e := compileExprSQL(t, "c IN ('10.0.0.1/32', '10.0.0.0/24')")
	in, ok := e.(*In)
	if !ok {
		t.Fatalf("compiled to %T, want *In", e)
	}

	// Row 0: "10.0.0.1" (a /32 host route in every way PostgreSQL reads it)
	// equals the literal "10.0.0.1/32" by INET order, not by TEXT order —
	// the two strings are not equal characters, so a text-comparing fallback
	// would wrongly answer false here.
	// Row 1: "9.0.0.0/8" is not in the address range either list member names.
	// Row 2: "192.0.2.5" is outside "10.0.0.0/24" and is not "10.0.0.1".
	want := []bool{true, false, false}
	for pass := 0; pass < 3; pass++ {
		for row := 0; row < b.Len; row++ {
			if got := in.EvalBool(b, row); got != want[row] {
				t.Fatalf("pass %d row %d: IN = %v, want %v", pass, row, got, want[row])
			}
		}
	}
	if in.disarm.disarmed(in.pairs) {
		t.Error("a CIDR-column IN list reported fully disarmed — the boxed CIDR rule (#565) would be silently skipped")
	}
}

// TestBetweenWithIPv6ColumnStaysArmed is the BETWEEN counterpart: an IPv6
// column's bounds must keep using the address's own order (kernel.IPv6RowKey/
// IPv6LitKey), not the rendered text's, and the node must never settle to the
// disarmed fast path.
func TestBetweenWithIPv6ColumnStaysArmed(t *testing.T) {
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeIPv6}}
	b := batch.NewRecordBatch(schema, 3)
	// Text order would put "2001:db8::10" BELOW "2001:db8::9" ('1' < '9'),
	// the opposite of address order — exactly the split #565 fixed.
	b.Columns[0].SetValue(0, "2001:db8::9")
	b.Columns[0].SetValue(1, "2001:db8::10")
	b.Columns[0].SetValue(2, "2001:db8::20")
	b.Len = 3

	e := compileExprSQL(t, "v BETWEEN '2001:db8::9' AND '2001:db8::10'")
	bt, ok := e.(*Between)
	if !ok {
		t.Fatalf("compiled to %T, want *Between", e)
	}

	want := []bool{true, true, false}
	for pass := 0; pass < 3; pass++ {
		for row := 0; row < b.Len; row++ {
			if got := bt.EvalBool(b, row); got != want[row] {
				t.Fatalf("pass %d row %d: BETWEEN = %v, want %v", pass, row, got, want[row])
			}
		}
	}
	if bt.disarm.disarmed(bt.pairs) {
		t.Error("an IPv6-column BETWEEN reported fully disarmed — the boxed IPv6 rule (#565) would be silently skipped")
	}
}
