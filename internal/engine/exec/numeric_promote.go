package exec

import "github.com/derekmwright/wadjet/internal/engine/batch"

// One promotion table for "read this column cell as a float64".
//
// Three places needed it and each had grown its own list, so the same query
// answered differently depending on which one it reached:
//
//	resolveFloat64Extractor (aggregate.go) — STDDEV/VARIANCE/MEDIAN/PERCENTILE/
//	    MODE/CORR/COVAR, and MIN_BY/MAX_BY's ordering key. Its default returned
//	    nil, which updateGroup reads as "skip every row", so the aggregate
//	    answered NULL.
//	vecFloat64 (window.go) — window SUM/AVG. Its default returned 0 AND marked
//	    the row valid, so the window answered a wrong number rather than no
//	    number. It now returns whether a numeric reading EXISTS and the window
//	    writes NULL when one does not (#412) — sharing the table was not enough
//	    on its own, because the caller was free to throw the answer away.
//	kernel.ResolveRowSum (kernel/agg.go) — the grouped SUM/AVG. Its list is the
//	    one this table matches, because it is the one TPC-H exercises and the
//	    one the two-path and DuckDB gates have always compared.
//
// What is IN, and on what grounds:
//
//	INT32/INT64/FLOAT32/FLOAT64/DECIMAL  numeric, obviously.
//	PORT, PROTOCOL                       integer-backed quantities; PostgreSQL
//	                                     sums and averages smallint/integer.
//	DURATION                             int64; PostgreSQL has sum(interval)
//	                                     and avg(interval).
//	DATE, TIMESTAMP                      NOT on PostgreSQL's authority: it has
//	                                     no sum(date), avg(date), sum(timestamp)
//	                                     or avg(timestamp) either. They are in
//	                                     because ResolveRowSum already had them
//	                                     and the TPC-H and DuckDB gates run over
//	                                     them, so removing them changes shipped
//	                                     answers; this table's job is to make the
//	                                     three readers AGREE, not to relitigate
//	                                     what is summable. Keeping them is
//	                                     status-quo preservation and is stated as
//	                                     such rather than dressed as a semantic.
//	                                     Their honest home is orderKeyFloat64
//	                                     below, where an ORDERING over a date is
//	                                     meaningful and a SUM of one is not.
//
// What is OUT: IPV4 and MAC. PostgreSQL has no sum, avg or stddev over inet or
// macaddr, so producing a number would be inventing a semantic; producing NULL
// is what the grouped path already does — and, since #412, what the window
// does too. Making it a plan-time ERROR is the right answer for the whole
// group (DATE and TIMESTAMP included, which is why they cannot simply be
// dropped here first) and is left open: it needs the same output-type decision
// #392 needs. BYTES, STRING, IPV6, CIDR, UUID, BOOL and the containers are out
// on PostgreSQL's authority, with no such caveat.
func numericPromotable(typ batch.TypeID) bool {
	switch typ {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64,
		batch.TypeDecimal, batch.TypePort, batch.TypeProtocol, batch.TypeDuration,
		batch.TypeDate, batch.TypeTimestamp:
		return true
	}
	return false
}

// numericFloat64 reads one cell as a float64. The second result is false when
// the type has no numeric reading — the caller must then produce NULL, never
// a zero.
func numericFloat64(v *batch.Vector, i int) (float64, bool) {
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i], true
	case batch.TypeFloat32:
		return float64(v.Float32Data[i]), true
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return float64(v.Int64Data[i]), true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return float64(v.Int32Data[i]), true
	case batch.TypeDecimal:
		return v.DecimalData.Data[i].ToFloat64(v.DecimalData.Scale), true
	}
	return 0, false
}

// orderKeyFloat64 is the wider table for an ORDERING key rather than a value:
// MIN_BY/MAX_BY pick a row by comparing this column, and the value they return
// comes from the other one. Ordering is meaningful for every type whose stored
// integer form orders like the value it represents, which adds IPV4, MAC and
// BOOL to the numeric set — an address compares as its integer, a MAC as its
// 48 bits, false before true.
//
// Left out: the byte-backed types (STRING, BYTES, IPV6, CIDR, UUID) and the
// containers. Those order fine, but not through a float64, so MIN_BY over such
// an ordering key still answers NULL. Tracked separately.
func orderKeyFloat64(v *batch.Vector, i int) (float64, bool) {
	switch v.Type {
	case batch.TypeIPv4, batch.TypeMAC:
		return float64(v.Int64Data[i]), true
	case batch.TypeBool:
		if v.BoolData[i] {
			return 1, true
		}
		return 0, true
	}
	return numericFloat64(v, i)
}

func orderKeyPromotable(typ batch.TypeID) bool {
	switch typ {
	case batch.TypeIPv4, batch.TypeMAC, batch.TypeBool:
		return true
	}
	return numericPromotable(typ)
}
