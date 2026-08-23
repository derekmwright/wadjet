package exec

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestWritePartialKeyDisplayForm is the unit-level regression test for #395.
//
// The consume-time generic GROUP BY path boxes a key through
// Vector.GetValue, which FORMATS every type whose storage is not its text.
// writePartialKeyToColumn used to dispatch on the destination type alone and
// wrote those display bytes straight into raw storage: 11 bytes of
// "2001:db8::1" into an IPV6 column whose contract is exactly 16, which
// GetValue then rendered as the EMPTY STRING; and kv.I64 — zero for a
// string-tagged kv — into an int-backed DATE/IPV4/MAC column.
//
// The assertion is a round trip: the display form written in must be the
// display form read back.
func TestWritePartialKeyDisplayForm(t *testing.T) {
	cases := []struct {
		name string
		typ  parquet.TypeID
		disp string
	}{
		{"ipv6", parquet.TypeIPv6, "2001:db8::1"},
		{"ipv6-zero", parquet.TypeIPv6, "2001:db8::"},
		{"uuid", parquet.TypeUUID, "00000000-0000-4000-8000-00000000002a"},
		{"ipv4", parquet.TypeIPv4, "10.0.0.5"},
		{"mac", parquet.TypeMAC, "aa:bb:cc:00:00:05"},
		{"date", parquet.TypeDate, "2021-03-04"},
		{"cidr", parquet.TypeCIDR, "10.0.0.0/8"},
		{"string", parquet.TypeString, "plain text"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Two rows: a NULL first, so the bytes-backed offsets have to
			// advance before the value lands, then the value.
			dst := batch.NewVector(tc.typ, 2)
			var null partialKeyValue
			setPartialKeyFromAny(&null, nil)
			writePartialKeyToColumn(dst, 0, &null)

			var kv partialKeyValue
			setPartialKeyFromAny(&kv, tc.disp)
			if kv.Tag != partialTagString {
				t.Fatalf("setPartialKeyFromAny(%q) tagged %d, want partialTagString", tc.disp, kv.Tag)
			}
			writePartialKeyToColumn(dst, 1, &kv)

			if !dst.Nulls.IsNullFast(0) {
				t.Errorf("row 0 should still be NULL")
			}
			if got := fmt.Sprint(dst.GetValue(1)); got != tc.disp {
				t.Errorf("round trip through the partial-merge emit: got %q, want %q", got, tc.disp)
			}
		})
	}
}

// TestWritePartialKeyRawBytesForm covers the other tag on the same
// destinations: a []byte-boxed key already holds the vector's STORAGE form
// and must be written verbatim, not re-parsed.
func TestWritePartialKeyRawBytesForm(t *testing.T) {
	raw := []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for _, typ := range []parquet.TypeID{parquet.TypeIPv6, parquet.TypeUUID, parquet.TypeBytes} {
		dst := batch.NewVector(typ, 1)
		var kv partialKeyValue
		setPartialKeyFromAny(&kv, raw)
		if kv.Tag != partialTagBytes {
			t.Fatalf("%s: []byte tagged %d, want partialTagBytes", typ, kv.Tag)
		}
		writePartialKeyToColumn(dst, 0, &kv)
		if got := string(dst.BytesData.Value(0)); got != string(raw) {
			t.Errorf("%s: raw storage bytes were not written verbatim: got %x want %x", typ, got, raw)
		}
	}
}

// TestPartialMergeKeepsIPv6AndUUIDKeys drives the whole path #395 travels: a
// HashAggregate under a budget tight enough that every clone's state drains
// to a partial run, so Finalize answers through finalizeViaPartialMerge
// rather than the in-memory emit. Before the fix, the counts and the group
// count were right and every key came back as the empty string.
func TestPartialMergeKeepsIPv6AndUUIDKeys(t *testing.T) {
	schema := []parquet.Column{
		{Name: "addr", Type: parquet.TypeIPv6},
		{Name: "id", Type: parquet.TypeUUID},
		{Name: "amount", Type: parquet.TypeFloat64},
	}

	// Group ordinals start at 1: net.IP.String() renders the all-zero
	// suffix as "2001:db8::", so a "2001:db8::0" spelling would be the
	// test's own canonicalization artifact rather than the property here.
	const groups = 12
	addrOf := func(g int) string { return fmt.Sprintf("2001:db8::%x", g+1) }
	uuidOf := func(g int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", g+1) }

	want := make(map[string]float64, groups)
	var batches []*batch.RecordBatch
	for bi := 0; bi < 10; bi++ {
		rows := make([]map[string]any, 0, groups)
		for g := 0; g < groups; g++ {
			amt := float64(bi*100 + g)
			rows = append(rows, map[string]any{
				"addr": addrOf(g), "id": uuidOf(g), "amount": amt,
			})
			want[addrOf(g)+"|"+uuidOf(g)] += amt
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)

	h := &HashAggregate{
		GroupByCols: []string{"addr", "id"},
		Aggs: []AggColumn{
			{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
		},
		Spill: sm,
	}
	rows := runHashAggToMap(t, h, batches)

	if len(rows) != groups {
		t.Fatalf("group count: got %d want %d", len(rows), groups)
	}
	got := make(map[string]float64, len(rows))
	for _, r := range rows {
		addr := fmt.Sprint(r["addr"])
		id := fmt.Sprint(r["id"])
		if addr == "" || id == "" {
			t.Fatalf("empty group key in %v — the display form was written into raw storage (#395)", r)
		}
		got[addr+"|"+id] = r["total"].(float64)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("group %s: got %v want %v", k, got[k], w)
		}
	}
}
