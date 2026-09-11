# Set operation equality key encoding

Source: internal/planner/physical/set_op_key.go — setOpKeyer, moved 2026-09-11 (#1026)

```go
// setOpKeyer builds the dedup key for the single-process set-operation path,
// from the arms' DECLARED column types rather than from the boxes their values
// arrive in.
//
// UNION, INTERSECT and EXCEPT decide membership by EQUALITY, so their key must
// agree with the comparator: two values `=` calls equal have to produce one
// key, or the same value appears twice in a UNION and matches nothing in an
// INTERSECT. `rowHashKey` used `fmt.Sprintf("%v", ...)` on the boxed value,
// which breaks that for three type families (#499, #546; ADR-0012 items 8
// and 10):
//
//   - DECIMAL boxes as its RENDERED TEXT (Vector.GetValue), so "12.75" from a
//     DECIMAL(9,2) and "12.7500" from a DECIMAL(18,4) were two keys for one
//     number: `UNION` of the two answered 4 rows where PostgreSQL answers 2,
//     and `INTERSECT` answered 0 where PostgreSQL answers 2. `GROUP BY` over
//     the same concatenation was already right, because it keys through the
//     COLUMNAR encoding — so the two halves of one engine disagreed.
//   - FLOAT renders -0.0 as "-0" and +0.0 as "0", which the comparator calls
//     equal (PostgreSQL's float order, ADR-0012 item 8), and every NaN payload
//     as "NaN", which it also calls equal — but the rendering is the only
//     thing keeping those two agreeing, by accident rather than by rule.
//   - CIDR boxes as its stored TEXT, and PostgreSQL's inet calls a bare
//     address and its own /32 host route ONE value, so "10.0.0.1" and
//     "10.0.0.1/32" were two keys for one member: UNION answered 4 rows here
//     and 3 on the stage DAG, whose set-op-as-aggregate lowering keys through
//     kernel.CidrOrderKey already (#546, #520).
//
// The issue filing ruled out "normalize the DECIMAL text before hashing" on
// the grounds that at `rowHashKey` the value is an untyped string,
// indistinguishable from a STRING column holding the same characters. That is
// exactly right, and it is why this keyer takes the SCHEMA: the declaration is
// what says which of the two a given string is.
//
// The bytes are the ones `exec.appendColumnValue` produces for the same value,
// so the local set operation, the aggregate's GROUP BY and the DAG's
// set-op-as-aggregate lowering all key one value one way.
```
