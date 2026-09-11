# Set operation type categories

Source: internal/planner/physical/set_op_stages.go — setOpCategory, moved 2026-09-11 (#1026)

```go
// setOpCategory is PostgreSQL's TYPE CATEGORY, which is what its
// UNION/CASE type-resolution algorithm asks about ("Type Conversion → UNION,
// CASE, and Related Constructs", step 4): arms whose types are in different
// categories have no common type and the statement is refused; arms within one
// category are resolved to the type they all implicitly cast to.
//
// The mapping is wadjet's declared type to the category PostgreSQL puts its
// WIRE type in, and every row of it was measured live on 17.11:
//
//	N numeric   INT32 INT64 FLOAT32 FLOAT64 DECIMAL — and PORT, PROTOCOL,
//	            DURATION, which declare int4/int4/int8 on the wire (#834), so
//	            `SELECT c_port … UNION ALL SELECT c_i64 …` is bigint ∪ integer
//	            there and answers.
//	S string    STRING (text).
//	B boolean   BOOL.
//	D datetime  DATE, TIMESTAMP. `date ∪ timestamp` → timestamp, both orders.
//	I network   IPV4, IPV6, CIDR. `inet ∪ inet` → inet, `inet ∪ cidr` → inet,
//	            both orders, values preserved.
//	U other     BYTES, UUID, MAC, ARRAY, ROW, MAP, VECTOR. PostgreSQL puts
//	            bytea, uuid and macaddr in one category too, and with no
//	            implicit conversion between them its step 6 still fails —
//	            `uuid ∪ bytea` is "UNION could not convert type bytea to uuid",
//	            the same SQLSTATE — so each of these matches only itself here.
```
