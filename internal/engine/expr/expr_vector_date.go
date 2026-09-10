// This file holds expr vector date; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Vectorized date/time functions ---

func vecYear(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Year())
	}
}

func vecMonth(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Month())
	}
}

func vecDay(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Day())
	}
}

func vecHour(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Hour())
	}
}

func vecExtract(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 2 {
		return
	}
	// First arg is the unit string (constant in practice)
	unit := strings.ToLower(args[0].BytesData.StringValue(0))
	src := args[1]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		switch unit {
		case "year":
			out.Float64Data[i] = float64(t.Year())
		case "quarter":
			out.Float64Data[i] = float64((t.Month()-1)/3 + 1)
		case "month":
			out.Float64Data[i] = float64(t.Month())
		case "week":
			_, week := t.ISOWeek()
			out.Float64Data[i] = float64(week)
		case "day":
			out.Float64Data[i] = float64(t.Day())
		case "hour":
			out.Float64Data[i] = float64(t.Hour())
		case "minute":
			out.Float64Data[i] = float64(t.Minute())
		case "second":
			out.Float64Data[i] = float64(t.Second())
		case "dow", "dayofweek":
			out.Float64Data[i] = float64(t.Weekday())
		case "doy", "dayofyear":
			out.Float64Data[i] = float64(t.YearDay())
		case "epoch":
			out.Float64Data[i] = float64(t.Unix())
		default:
			// fnExtract returns nil for an unrecognized unit. Leaving the
			// pooled vector's stale contents in place instead answered with
			// whatever the previous batch wrote there.
			out.Nulls.SetNull(i)
		}
	}
}
