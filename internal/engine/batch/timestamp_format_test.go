package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestFormatTimestamp pins the rendered form of the engine's timestamp unit.
// The text has to be what a client expects for a column the wire declared as
// PostgreSQL `timestamp` (OID 1114): UTC, space-separated, no zone suffix.
func TestFormatTimestamp(t *testing.T) {
	cases := []struct {
		name string
		ms   int64
		want string
	}{
		{"issue 321 value", 826727136000, "1996-03-13 14:25:36"},
		{"epoch", 0, "1970-01-01 00:00:00"},
		{"whole second", 1755000000000, "2025-08-12 12:00:00"},
		{"sub-second", 1755000000123, "2025-08-12 12:00:00.123"},
		{"one ms after epoch", 1, "1970-01-01 00:00:00.001"},
		{"one ms before epoch", -1, "1969-12-31 23:59:59.999"},
		{"pre-epoch whole second", -14182940000, "1969-07-20 20:17:40"},
		{"pre-epoch sub-second", -14182939877, "1969-07-20 20:17:40.123"},
	}
	for _, tc := range cases {
		if got := FormatTimestamp(tc.ms); got != tc.want {
			t.Errorf("%s: FormatTimestamp(%d) = %q, want %q", tc.name, tc.ms, got, tc.want)
		}
	}
}

// TestTimestampBoxingStaysNumeric is the guard on the other half of the
// display/compute split. Vector.GetValue's TIMESTAMP box is shared with the
// GROUP BY key path, the aggregate/window spill codecs, the window
// comparator and the UPDATE read-modify-write path, all of which type-switch
// on int64 and degrade SILENTLY on anything else (a distinct-timestamp
// GROUP BY collapses to one group; an updated row is rewritten as epoch 0).
//
// Rendering belongs at the renderers, which still hold the declared type.
// If someone later moves formatting into GetValue to "fix display", this
// fails and points at the reason.
func TestTimestampBoxingStaysNumeric(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "d", Type: parquet.TypeDate},
	}
	b := NewRecordBatch(schema, 1)
	b.Len = 1
	b.Columns[0].Int64Data[0] = 826727136000
	b.Columns[1].Int32Data[0] = 9569

	v := b.Columns[0].GetValue(0)
	ms, ok := v.(int64)
	if !ok {
		t.Fatalf("TIMESTAMP GetValue boxed as %T, want int64 — this breaks GROUP BY keys, "+
			"spill codecs, the window comparator and UPDATE rewrite; render at the renderer instead", v)
	}
	if ms != 826727136000 {
		t.Errorf("TIMESTAMP GetValue = %d, want 826727136000", ms)
	}

	// The round-trip those compute paths depend on: box a value out and
	// write it back into a TIMESTAMP column without loss.
	dst := NewVector(TypeTimestamp, 1)
	dst.SetValue(0, v)
	if got := dst.Int64Data[0]; got != 826727136000 {
		t.Errorf("GetValue -> SetValue round-trip wrote %d, want 826727136000", got)
	}

	// And the rendered form is reachable from the same value.
	if got := FormatTimestamp(ms); got != "1996-03-13 14:25:36" {
		t.Errorf("FormatTimestamp(%d) = %q, want %q", ms, got, "1996-03-13 14:25:36")
	}
}
