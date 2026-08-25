package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestWindowRowNumberOrdersCidrByInetOrder is the regression for
// compareVectorValues' TypeCIDR case: before this fix TypeCIDR fell into the
// String/Bytes/IPv6/UUID group and ordered by raw stored TEXT, so an
// in-memory window's ROW_NUMBER (which decides its ORDER BY position through
// this comparator, not the sort kernel's) put "10.0.0.0/8" ahead of
// "9.0.0.0/8" — text order, and the opposite of what `ORDER BY c_cidr`
// already answers (kernel.CidrOrderKey, #520) and what `=`/`<`/`>` already
// answer (#492, ADR-0012 item 10). A window whose partition boundaries and
// peer groups disagree with the ORDER BY that sorted the same rows is
// exactly the class of bug #446 named for VECTOR/ARRAY(FLOAT) NaN ordering.
func TestWindowRowNumberOrdersCidrByInetOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_cidr", Type: parquet.TypeCIDR},
	}
	rows := []map[string]any{
		// Common bits under the smaller mask decide before the mask length:
		// 9.0.0.0/8 sorts BELOW 10.0.0.0/8 in PostgreSQL's inet order, and
		// ABOVE it in plain text order ('9' > '1').
		{"id": int64(1), "c_cidr": "10.0.0.0/8"},
		{"id": int64(2), "c_cidr": "9.0.0.0/8"},
		// A bare address and its own /32 are one inet value — a stable sort
		// must not reorder them relative to each other by TEXT, since the
		// comparator says they tie.
		{"id": int64(3), "c_cidr": "10.0.0.1/32"},
		{"id": int64(4), "c_cidr": "10.0.0.1"},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinRowNumber,
			OutputCol:  "rn",
			OutputType: parquet.TypeInt64,
			OrderBy:    []SortKey{{Column: "c_cidr", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := win.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	rnByID := make(map[int64]int64)
	for _, row := range b.ToRows() {
		rnByID[row["id"].(int64)] = row["rn"].(int64)
	}

	// Inet order: 9.0.0.0/8 (id=2) sorts first (first octet 9 < 10); then
	// 10.0.0.0/8 (id=1) — its masked network agrees with the /32 pair's on
	// their common (first-octet) bits, and the SHORTER prefix sorts first on
	// that tie; then the tied 10.0.0.1 pair (id=3, id=4) last.
	if rnByID[2] != 1 {
		t.Errorf("id=2 (9.0.0.0/8) rn = %d, want 1 (inet order sorts it below 10.0.0.0/8)", rnByID[2])
	}
	if rnByID[1] != 2 {
		t.Errorf("id=1 (10.0.0.0/8) rn = %d, want 2", rnByID[1])
	}
	tied := map[int64]bool{rnByID[3]: true, rnByID[4]: true}
	if !tied[3] || !tied[4] {
		t.Errorf("id=3/id=4 (10.0.0.1/32 and 10.0.0.1) rn = %d/%d, want {3,4} in some order "+
			"(they are one inet value)", rnByID[3], rnByID[4])
	}
}
