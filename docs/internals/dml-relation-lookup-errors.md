# Dml relation lookup errors

Source: wadjet/dml.go — func (db *DB) dmlRelationError(name string, err error) error {, moved 2026-09-11 (#1026)

dmlRelationError is a DML door's table lookup reported the way the SELECT
door already reports it.

#719: all four doors wrapped catalog.GetTable's miss with %w and handed the
client `table "x": table "x" not found` and NO SQLSTATE, while
`MERGE ... USING nosuchtable` — which reaches the relation through db.Query
and therefore through the planner — answered 42P01 with PostgreSQL's own
wording. One statement class, two dispositions, and they disagreed on the
MESSAGE as well as the class. PostgreSQL 17 says
`relation "nosuchtable" does not exist`; so does this now, on every door.

A transport failure is deliberately NOT 42P01: the table's existence is
unknown then, which is the same distinction physical.validate makes on the
read path.

AMBIGUITY is part of the same message. A reference matching two registered
tables case-insensitively and neither byte-exact is one ResolveTableName
declines to answer, so GetTable reports the plain miss — and the writer got
`relation "mytab" does not exist` while the reader got that plus the two
candidates and how to disambiguate. One statement class, two messages again.
catalog.AmbiguousTableError is the ONE builder both doors raise now (#858).
