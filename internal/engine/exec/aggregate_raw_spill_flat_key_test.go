package exec

import (
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Every FLAT type must survive the legacy raw-row spill as a GROUP BY key
// (#632).
//
// The sibling of TestRawRowContainerSpillMatchesMemory: that one covers the
// four container types, and this one covers the eighteen flat ones, on the
// same path — a non-simple aggregate (COUNT(DISTINCT)) makes
// canUseExternalMerge false, so group keys go to disk as boxed values through
// memory.SpillManager.SpillRows and come back through batch.FromRows.
//
// The one that failed was BYTES: SpillRows had typed arms for
// bool/int/float/string only, and a []byte box fell to the default arm, which
// renders it with fmt.Sprintf as the DISPLAY text "[104 105]". BYTES SetValue
// accepts a string carrier, so the text was written back into the column as if
// it were the value, and the group came back keyed by nine ASCII bytes instead
// of by its two. Silent in both directions — nothing rejected anything.
//
// Note the renderer: it prints the Go TYPE beside the value, because "%v" of a
// []byte and "%v" of its own rendered text are the SAME STRING. A gate keyed
// on the display form passes with the defect present.
func TestRawRowSpillPreservesEveryFlatGroupKeyType(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  parquet.Column
		val  func(g int64) any
	}{
		{"bool", parquet.Column{Name: "k", Type: parquet.TypeBool, Nullable: true},
			func(g int64) any { return g%2 == 0 }},
		{"int32", parquet.Column{Name: "k", Type: parquet.TypeInt32, Nullable: true},
			func(g int64) any { return int32(g) }},
		{"int64", parquet.Column{Name: "k", Type: parquet.TypeInt64, Nullable: true},
			func(g int64) any { return g * 1_000_003 }},
		{"float32", parquet.Column{Name: "k", Type: parquet.TypeFloat32, Nullable: true},
			func(g int64) any { return float32(g) / 7 }},
		{"float64", parquet.Column{Name: "k", Type: parquet.TypeFloat64, Nullable: true},
			func(g int64) any { return float64(g) / 3 }},
		{"string", parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
			func(g int64) any { return fmt.Sprintf("s-%04d", g) }},
		{"bytes", parquet.Column{Name: "k", Type: parquet.TypeBytes, Nullable: true},
			// Values that are NOT valid UTF-8 and that vary in length, so a
			// path that round-trips them through display text or through a
			// string cannot come back equal by luck.
			func(g int64) any { return []byte{0xff, byte(g), 0x00, byte(g >> 8), 0xfe} }},
		{"timestamp", parquet.Column{Name: "k", Type: parquet.TypeTimestamp, Nullable: true},
			func(g int64) any { return 1_700_000_000_000 + g*61_000 }},
		{"ipv4", parquet.Column{Name: "k", Type: parquet.TypeIPv4, Nullable: true},
			func(g int64) any { return fmt.Sprintf("10.%d.%d.%d", (g/65536)%256, (g/256)%256, g%256) }},
		{"ipv6", parquet.Column{Name: "k", Type: parquet.TypeIPv6, Nullable: true},
			func(g int64) any { return fmt.Sprintf("2001:db8::%x", g) }},
		{"cidr", parquet.Column{Name: "k", Type: parquet.TypeCIDR, Nullable: true},
			func(g int64) any { return fmt.Sprintf("192.168.%d.0/24", g%256) }},
		{"mac", parquet.Column{Name: "k", Type: parquet.TypeMAC, Nullable: true},
			func(g int64) any { return fmt.Sprintf("aa:bb:cc:%02x:%02x:%02x", (g/65536)%256, (g/256)%256, g%256) }},
		{"port", parquet.Column{Name: "k", Type: parquet.TypePort, Nullable: true},
			func(g int64) any { return int32(1024 + g) }},
		{"protocol", parquet.Column{Name: "k", Type: parquet.TypeProtocol, Nullable: true},
			func(g int64) any { return int32(g % 256) }},
		{"duration", parquet.Column{Name: "k", Type: parquet.TypeDuration, Nullable: true},
			func(g int64) any { return g * 1_000_000 }},
		{"uuid", parquet.Column{Name: "k", Type: parquet.TypeUUID, Nullable: true},
			func(g int64) any { return fmt.Sprintf("00000000-0000-4000-8000-%012x", g) }},
		{"date", parquet.Column{Name: "k", Type: parquet.TypeDate, Nullable: true},
			func(g int64) any { return fmt.Sprintf("20%02d-%02d-%02d", 10+g%15, 1+g%12, 1+g%28) }},
		{"decimal", parquet.Column{Name: "k", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
			func(g int64) any { return fmt.Sprintf("%d.%04d", g, g%9973) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const numBatches, rowsPerBatch, numGroups = 25, 200, 200
			schema := []parquet.Column{tc.col, {Name: "v", Type: parquet.TypeInt64}}
			batches := make([]*batch.RecordBatch, 0, numBatches)
			for bi := 0; bi < numBatches; bi++ {
				rows := make([]map[string]any, 0, rowsPerBatch)
				for ri := 0; ri < rowsPerBatch; ri++ {
					g := int64((bi*rowsPerBatch + ri) % numGroups)
					var k any
					if g%17 != 3 { // every 17th group key is NULL
						k = tc.val(g)
					}
					rows = append(rows, map[string]any{"k": k, "v": int64(bi*1000 + ri + 1)})
				}
				batches = append(batches, batch.FromRows(schema, rows))
			}
			mk := func(spill *memory.SpillManager) *HashAggregate {
				// COUNT(DISTINCT) is non-simple, so canUseExternalMerge is
				// false and this is the legacy raw-row path.
				h := NewHashAggregate([]string{"k"}, []AggColumn{
					{Func: AggCountDistinct, InputCol: "v", OutputCol: "nd", OutputType: parquet.TypeInt64},
				})
				h.Spill = spill
				return h
			}

			want := typedAggRows(runHashAggToMap(t, mk(nil), batches))

			defer func(v int64) { spillFileTargetBytes = v }(spillFileTargetBytes)
			spillFileTargetBytes = 4000
			tracker := memory.NewTracker("test", 1_024)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			tracker.ForceReserve(900)

			got := typedAggRows(runRawRowSpillChecked(t, mk(sm), batches))
			if len(got) != len(want) {
				t.Fatalf("spilled run: %d groups, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("row %d differs between the spilled and in-memory runs\n  spilled: %s\n  memory:  %s",
						i, got[i], want[i])
				}
			}
		})
	}
}

// typedAggRows renders each output row with the Go TYPE of the group key
// beside its value. The type is what makes the BYTES defect visible: a []byte
// and the display text a lossy encoder turns it into print identically under
// "%v", so a value-only renderer passes with the group mis-keyed.
func typedAggRows(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("k=%T:%v nd=%v", r["k"], r["k"], r["nd"]))
	}
	sort.Strings(out)
	return out
}
