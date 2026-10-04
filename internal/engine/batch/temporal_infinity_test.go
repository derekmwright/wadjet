// SPDX-License-Identifier: MIT

package batch

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcTIThePrinterAndTheDateConversion: the TIMESTAMP printer writes the
// carrier's extremes as PostgreSQL prints the infinite values; the one
// DATE→TIMESTAMP conversion maps an infinite DATE to the infinite TIMESTAMP
// of its sign (`DATE 'infinity' = TIMESTAMP 'infinity'`); a vector reads the
// two words into the extremes and prints them back.
func TestArcTIThePrinterAndTheDateConversion(t *testing.T) {
	if got := FormatTimestamp(math.MaxInt64) + "|" + FormatTimestamp(math.MinInt64); got != "infinity|-infinity" {
		t.Errorf("FormatTimestamp of the extremes: %s", got)
	}
	if got := FormatTimestamp(math.MaxInt64 - 1); got == "infinity" {
		t.Errorf("FormatTimestamp(MaxInt64-1) printed infinity")
	}
	for days, want := range map[int64]int64{
		math.MaxInt32: math.MaxInt64, math.MinInt32: math.MinInt64, 1: MillisPerDay, -1: -MillisPerDay,
	} {
		if got := DateMidnightMillis(days); got != want {
			t.Errorf("DateMidnightMillis(%d) = %d, want %d", days, got, want)
		}
	}
	b := NewRecordBatch([]parquet.Column{{Name: "ts", Type: TypeTimestamp}, {Name: "d", Type: TypeDate}}, 2)
	b.Columns[0].SetValue(0, " Infinity ")
	b.Columns[0].SetValue(1, "-infinity")
	b.Columns[1].SetValue(0, "+infinity")
	b.Columns[1].SetValue(1, "- infinity")
	if b.Columns[0].Int64Data[0] != math.MaxInt64 || b.Columns[0].Int64Data[1] != math.MinInt64 ||
		b.Columns[1].Int32Data[0] != math.MaxInt32 || b.Columns[1].Int32Data[1] != math.MinInt32 {
		t.Errorf("stored %v %v", b.Columns[0].Int64Data, b.Columns[1].Int32Data)
	}
	if got := b.Columns[1].GetValue(0); got != "infinity" {
		t.Errorf("DATE GetValue %v", got)
	}
}
