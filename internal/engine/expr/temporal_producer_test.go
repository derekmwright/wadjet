// SPDX-License-Identifier: MIT

package expr

import "github.com/derekmwright/wadjet/internal/engine/batch"

// shownBox is what a client sees for a row-path value: a temporal box
// rendered in the unit its producer names (producedTemporal), anything else
// as it is. Tests that pin the TEXT of a date or an instant compare this, so
// they state the value rather than the carrier.
func shownBox(e Expr, b *batch.RecordBatch, v any) any {
	if s, ok := renderTemporalBox(e, b, v); ok {
		return s
	}
	return v
}

// producedVectorType is the output vector a row-path value of e belongs in:
// its produced temporal type when it has one, else the registry's declaration.
func producedVectorType(e Expr, b *batch.RecordBatch, declared batch.TypeID) batch.TypeID {
	switch producedTemporal(e, b) {
	case castToDateKind:
		return batch.TypeDate
	case castToTimestampKind:
		return batch.TypeTimestamp
	}
	return declared
}

// shownVec is the vector-path twin of shownBox: a TIMESTAMP vector hands its
// epoch milliseconds back from GetValue, a DATE vector its text.
func shownVec(out *batch.Vector, row int) any {
	v := out.GetValue(row)
	if ms, ok := v.(int64); ok && out.Type == batch.TypeTimestamp {
		return batch.FormatTimestamp(ms)
	}
	return v
}

// tsText renders a kernel's TIMESTAMP box; dateText its DATE box. Kernel-level
// tests call the registry function directly, with no producer in hand.
func tsText(v any) any {
	if ms, ok := v.(int64); ok {
		return batch.FormatTimestamp(ms)
	}
	return v
}

func dateText(v any) any {
	if d, ok := v.(int64); ok {
		return batch.FormatDate(int32(d))
	}
	return v
}
