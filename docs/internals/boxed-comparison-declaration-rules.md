# Boxed comparison declaration rules

Source: internal/engine/expr/boxed_pair.go — type boxKind int32, moved 2026-09-11 (#1026)

This file is the declaration-driven half of the boxed comparison path.

ADR-0012 item 8's rule is that a boxed value's comparison order follows the
COLUMN'S DECLARATION, not the Go type its box happens to be. Two shapes make
that rule impossible to follow from the box alone, because both arrive as a
plain Go `string`:

  - a DECIMAL column, which Vector.GetValue renders as its decimal TEXT, and
  - a STRING column, whose value may look exactly like that text.

`expr.decimalColCmp` already answers one of those pairs from the two
columns' declarations — but it is bound in `NewCmp` alone, so the SAME two
DECIMAL columns at a BOXED site (a simple `CASE d1 WHEN d2`, `d1 IS DISTINCT
FROM d2`, `GREATEST(d1, d2)`) fell through `compare()`'s two-rendered-strings
path and compared LEXICOGRAPHICALLY, where "10.001" sorts below "2.0002"
(#506).

The other direction was worse. `compare()` used to tell the two apart by
SNIFFING: any string operand that PARSED as a number was read numerically
against the other side. That made a genuine STRING column compare
NUMERICALLY on the row path — `WHERE s = 1.5` found the row holding "1.50" —
while the vectorized kernel compared the same predicate as text, so one
query had two answers depending on which path it took (#504). The sniff is
gone: a STRING column is classified from its declaration and compares as
text on both paths.

A boxedPair is armed from the operand EXPRESSIONS at construction, resolves
each operand's DECLARED kind on the first batch that can answer it, and then
applies one rule per kind pair. Nothing here reads a value's box to decide
which RULE applies — only to decide whether the rule's text arm or its
numeric arm is the one holding this row's value.
