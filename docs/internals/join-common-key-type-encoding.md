# Join common key type encoding

Source: internal/engine/exec/join_key_width.go — KeyTypeUnresolved, moved 2026-09-11 (#1026)

A join key is built at the pair's COMMON type, not at each side's own
storage type (#615, #650, #663).

ADR-0023's rule is that a key and the comparator name ONE relation: "these
two rows compare equal" and "these two rows key alike" have to be the same
statement, or the join answers something the WHERE spelling of it does not.
A comparison already resolves its operand pair to PostgreSQL's common type
before comparing; the key did not. It called appendColumnValue with
`v.Type` on each side independently, so `a.i = b.d` (INT64 against
DECIMAL) built eight little-endian bytes on one side and a canonical
decimal key on the other, and matched only where those byte strings
coincided by accident — one pair over a ten-row fixture, not zero, which is
why the failure reads as a wrong answer rather than as an empty one.

The resolved type is decided at PLAN time (physical.resolveJoinKeyTypes,
which is where the two sides' declared types are both in hand) and carried
into the operator, on BOTH execution paths and into the shuffle's partition
hash — a repartition that routes at the column's own width sends equal
values to different partitions, which is the same defect one layer down.

KeyTypeUnresolved means "this pair needs no widening": either the planner
could not type one of the sides, or the two sides already agree. It is the
value every existing caller gets, and it takes exactly the code path the
key had before, byte for byte.
