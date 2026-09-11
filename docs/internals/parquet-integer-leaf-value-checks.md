# Parquet integer leaf value checks

Source: internal/storage/parquet/leaf_value.go — func int32LeafValue(colType TypeID, v any) (int32, error) {, moved 2026-09-11 (#1026)

int32LeafValue and int64LeafValue resolve a caller's box into the value an
INT32 / INT64 leaf stores, or refuse it.

Their predecessors (toInt32, toInt64) narrowed with a bare Go conversion and
had no way to say no. Measured on the tree before this file existed, writing
one row through NativeWriter.WriteMapRows and reading it back:

	INT32 leaf, int64(3000000000)   stored -1294967296   wrapped
	INT32 leaf, int64(-2147483649)  stored  2147483647   wrapped
	INT32 leaf, int(3000000000)     stored -1294967296   wrapped
	INT32 leaf, float64(1e10)       stored -2147483648   implementation-defined
	INT32 leaf, math.NaN()          stored -2147483648   implementation-defined
	INT32 leaf, float64(2.5)        stored  2            truncated
	INT32 leaf, int8/int16/uint8/uint16/uint32   stored 0
	INT64 leaf, uint64(5), uint8(6)              stored 0
	INT64 leaf, math.NaN(), float64(1e30)        stored -9223372036854775808

The last two rows of each block are the ones no reviewer expects: those five
integer boxes are ADMITTED by ingest.checkType for an INT32 / PORT /
PROTOCOL column and uint64 is admitted for an INT64 one, and every one of
them fell through a `default: return 0` arm and stored a zero nobody wrote.
Go's float→int conversion is IMPLEMENTATION-DEFINED outside the
destination's range and for a NaN, which is why the check has to happen on
the FLOAT and not on whatever integer the conversion happened to produce.

PostgreSQL raises 22003 for the same assignment — `INSERT INTO t(int4col)
VALUES (3000000000)`, `'NaN'::float8::int4` and `1e10::float8::int4` are all
`22003 integer out of range` on postgres:17-alpine — and batch's
IntegerRangeError is the engine's carrier for it. This package cannot use
that type (batch imports parquet, not the other way round), so it raises the
same SQLSTATE through sqlerr, the way DecimalRescale already does.

A FRACTIONAL float is refused rather than rounded. PostgreSQL rounds at the
CAST (float8→int4 half-to-even, numeric→int4 half-away-from-zero) and
wadjet's DML door implements both rules in assignIntegerValue; by the time a
value reaches a leaf the assignment cast has already happened, so a float
with a fraction here means nobody applied one — and storing 2 for 2.5 is
exactly the "reads back as a different number" ADR-0018 forbids.
