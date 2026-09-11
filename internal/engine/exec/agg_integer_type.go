package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// IntegerAccOutputType is shared by grouped/window declarations, window runtime
// correction and grouped exact-carrier selection (#784, #813, #953; ADR-0012).
// SUM(int4) is bigint; SUM(int8) and AVG of either are numeric. Overflow must
// raise 22003 rather than wrap (ADR-0024 item 4).
// AVG scale is batch.AvgScale(0)=4, stable across row counts rather than
// magnitude-dependent PostgreSQL scale (ADR-0024 item 2).
// PORT (0..65535) and PROTOCOL (0..255) are int4-domain/wire inputs (#834).
// DATE/TIMESTAMP/DURATION stay float64 in both spellings. Other types return
// ok=false and callers preserve their existing declaration.
// See docs/internals/integer-accumulator-result-types.md for the design.
func IntegerAccOutputType(avg bool, in parquet.TypeID) (out parquet.TypeID, precision, scale int, ok bool) {
	switch in {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		if avg {
			return parquet.TypeDecimal, batch.MaxDecimalPrecision, batch.AvgScale(0), true
		}
		return parquet.TypeInt64, 0, 0, true
	case parquet.TypeInt64:
		if avg {
			return parquet.TypeDecimal, batch.MaxDecimalPrecision, batch.AvgScale(0), true
		}
		return parquet.TypeDecimal, batch.MaxDecimalPrecision, 0, true
	}
	return 0, 0, 0, false
}

// integerAccInput reports whether a vector of this type is summed EXACTLY by
// the rules above — the input side of IntegerAccOutputType, asked where only
// the input is in hand.
func integerAccInput(in parquet.TypeID) bool {
	_, _, _, ok := IntegerAccOutputType(false, in)
	return ok
}
