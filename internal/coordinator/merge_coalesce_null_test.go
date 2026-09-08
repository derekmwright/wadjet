package coordinator

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A NULL ROW IS STILL A WRITE — #1007.
//
// A variable-length column's value at row i is `Data[Offsets[i]:Offsets[i+1]]`.
// A row that writes NOTHING leaves `Offsets[i+1]` at zero, so the NEXT non-null
// value is read from the START of the arena: every byte written so far,
// concatenated, under one row's name. `coalesceForOrdering` set the null bit
// and `continue`d, which is exactly that.
//
// Reached through SQL by any coordinator merge over MORE THAN ONE batch — the
// coalesce is what a >2048-row result needs and what nothing under 2048 rows
// exercises, which is why the corruption survived every 8-row cell in this
// package (`TestM1AMergedOrderIsTheQuerysOrderAtScale` is the SQL half).
//
// The matrix is per CARRIER, not per type name, and only ONE carrier cascades:
// `BytesColumn` (STRING, BYTES, IPv6, CIDR, UUID) writes only the CLOSING
// offset of each row, so a skipped row moves every value after it. ROW carries
// that family in its children and cascades with it. ARRAY and MAP write BOTH
// ends of their span from the child's length, so a skipped row leaves a stale
// pair and self-heals at the next write — measured, and they are here as the
// carriers a "fix the offsets" change could break rather than as reproductions.
// The fixed-width cells are the third class: a null writes no offset at all and
// cannot cascade, and a failure there would mean the fix broke something.
func TestCoalescingAMergeAdvancesAVarlenNullsOffset(t *testing.T) {
	rowType := parquet.Column{Name: "c", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	for _, tc := range []struct {
		name string
		col  parquet.Column
		// vals are three rows: two in the first batch (the second NULL) and
		// one in the second batch. The third is what a mis-advanced offset
		// corrupts, because it is the first non-null value AFTER the null.
		v0, v2 any
		// want2 is the third row's value read back out of the coalesced batch.
		want2 any
	}{
		{"string", parquet.Column{Name: "c", Type: parquet.TypeString, Nullable: true},
			"aaa", "ccc", "ccc"},
		{"bytes", parquet.Column{Name: "c", Type: parquet.TypeBytes, Nullable: true},
			[]byte("aaa"), []byte("ccc"), []byte("ccc")},
		{"ipv6", parquet.Column{Name: "c", Type: parquet.TypeIPv6, Nullable: true},
			"2001:db8::1", "2001:db8::2", "2001:db8::2"},
		{"cidr", parquet.Column{Name: "c", Type: parquet.TypeCIDR, Nullable: true},
			"10.0.0.0/8", "192.168.0.0/16", "192.168.0.0/16"},
		{"uuid", parquet.Column{Name: "c", Type: parquet.TypeUUID, Nullable: true},
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000002",
			"00000000-0000-0000-0000-000000000002"},
		{"array", parquet.Column{Name: "c", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
			[]any{"a"}, []any{"c"}, []any{"c"}},
		{"map", parquet.Column{Name: "c", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
			// A MAP is stored as ARRAY(ROW(key,value)), and `GetValue` reads
			// back that storage shape — the entry list, not the Go map the
			// fixture writes. Asserted as stored, because what this cell is
			// about is the OFFSET, and the offset belongs to the entry list.
			map[string]any{"k": int64(1)}, map[string]any{"k": int64(3)},
			[]any{map[string]any{"key": "k", "value": int64(3)}}},
		{"row", rowType, map[string]any{"s": "a"}, map[string]any{"s": "c"},
			map[string]any{"s": "c"}},
		// CONTROLS: fixed-width carriers, where a null writes no offset and so
		// cannot cascade. They must not move.
		{"int64", parquet.Column{Name: "c", Type: parquet.TypeInt64, Nullable: true},
			int64(1), int64(3), int64(3)},
		{"float64", parquet.Column{Name: "c", Type: parquet.TypeFloat64, Nullable: true},
			1.0, 3.0, 3.0},
		{"decimal", parquet.Column{Name: "c", Type: parquet.TypeDecimal, Nullable: true,
			Precision: 18, Scale: 4}, "1.0000", "3.0000", "3.0000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, tc.col}
			first := batch.FromRows(schema, []map[string]any{
				{"id": int64(1), "c": tc.v0},
				{"id": int64(2), "c": nil},
			})
			second := batch.FromRows(schema, []map[string]any{
				{"id": int64(3), "c": tc.v2},
			})
			out := coalesceForOrdering([]*batch.RecordBatch{first, second})
			if len(out) != 1 {
				t.Fatalf("coalesceForOrdering returned %d batches, want 1", len(out))
			}
			b := out[0]
			if b.Len != 3 {
				t.Fatalf("coalesced Len = %d, want 3", b.Len)
			}
			if !b.Columns[1].Nulls.IsNullFast(1) {
				t.Errorf("row 1 is not NULL after the coalesce")
			}
			if b.Columns[1].Nulls.IsNullFast(2) {
				t.Fatalf("row 2 came back NULL")
			}
			got := b.Columns[1].GetValue(2)
			if fmt.Sprint(got) != fmt.Sprint(tc.want2) {
				t.Errorf("the row AFTER the null reads %v, want %v — the null row did not "+
					"advance the offset, so this value starts at the arena's origin (#1007)",
					got, tc.want2)
			}
			// The row BEFORE the null must be untouched, which is what says
			// the fix advanced the offset rather than moved the arena. Read
			// against the coalesce's own INPUT rather than the fixture literal,
			// so a carrier whose stored shape differs from the written one (a
			// MAP's entry list) is compared like for like.
			want0 := first.Columns[1].GetValue(0)
			if got0 := b.Columns[1].GetValue(0); fmt.Sprint(got0) != fmt.Sprint(want0) {
				t.Errorf("the row BEFORE the null reads %v, want %v", got0, want0)
			}
		})
	}
}

// TWO nulls in a row, and a null as the FIRST row of a batch, are the two
// positions a single-null fixture cannot reach: the first tests that the offset
// advances by an EMPTY span each time rather than once, and the second that the
// carry across a batch boundary starts from the arena's current end.
func TestCoalescingAdvancesEveryNullsOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}
	first := batch.FromRows(schema, []map[string]any{
		{"id": int64(1), "s": "aaa"},
		{"id": int64(2), "s": nil},
		{"id": int64(3), "s": nil},
	})
	second := batch.FromRows(schema, []map[string]any{
		{"id": int64(4), "s": nil},
		{"id": int64(5), "s": "eee"},
		{"id": int64(6), "s": "fff"},
	})
	out := coalesceForOrdering([]*batch.RecordBatch{first, second})
	if len(out) != 1 || out[0].Len != 6 {
		t.Fatalf("coalesced %d batches / %d rows, want 1 / 6", len(out), out[0].Len)
	}
	b := out[0]
	want := []any{"aaa", nil, nil, nil, "eee", "fff"}
	for i, w := range want {
		if w == nil {
			if !b.Columns[1].Nulls.IsNullFast(i) {
				t.Errorf("row %d is not NULL", i)
			}
			continue
		}
		if got := b.Columns[1].GetValue(i); fmt.Sprint(got) != fmt.Sprint(w) {
			t.Errorf("row %d = %v, want %v", i, got, w)
		}
	}
}
