package parquet

import (
	"bytes"
	"fmt"
	"testing"
)

// The writer's DECIMAL path, per box type. Ingest of a decimal column is text
// or floats row after row, so the box → unscaled conversion is the whole cost
// of the column on the way in.

func benchDecimalWrite(b *testing.B, precision, scale int, vals []any) {
	b.Helper()
	schema := Schema{Columns: []Column{
		{Name: "d", Type: TypeDecimal, Nullable: true, Precision: precision, Scale: scale},
	}}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"d": v}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, schema, DefaultWriterConfig())
		if err != nil {
			b.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

const decimalBenchRows = 10000

func BenchmarkDecimalWriteTextBox(b *testing.B) {
	vals := make([]any, decimalBenchRows)
	for i := range vals {
		vals[i] = fmt.Sprintf("%d.%02d", i, i%100)
	}
	benchDecimalWrite(b, 18, 2, vals)
}

func BenchmarkDecimalWriteFloatBox(b *testing.B) {
	vals := make([]any, decimalBenchRows)
	for i := range vals {
		vals[i] = float64(i) + float64(i%100)/100
	}
	benchDecimalWrite(b, 18, 2, vals)
}

func BenchmarkDecimalWriteUnscaledBox(b *testing.B) {
	vals := make([]any, decimalBenchRows)
	for i := range vals {
		vals[i] = int64(i)*100 + int64(i%100)
	}
	benchDecimalWrite(b, 18, 2, vals)
}

func BenchmarkDecimalWriteWideTextBox(b *testing.B) {
	vals := make([]any, decimalBenchRows)
	for i := range vals {
		vals[i] = fmt.Sprintf("%d%d.%010d", i, i, i)
	}
	benchDecimalWrite(b, 38, 10, vals)
}

// The conversion on its own, with no file writing around it.
func BenchmarkDecimalValueFromBox(b *testing.B) {
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"text", "12345.67"},
		{"text_wide", "12345678901234567890.1234567891"},
		{"float", 12345.67},
		{"float32", float32(12345.67)},
		{"unscaled", int64(1234567)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p, s := 18, 2
			if tc.name == "text_wide" {
				p, s = 38, 10
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := DecimalValueFromBox(tc.v, p, s); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
