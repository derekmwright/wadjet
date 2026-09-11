# Sql set arm window discovery

Source: internal/planner/sql/parser.go — func collectWindowSpecs(info *SelectInfo) []WindowSpec {, moved 2026-09-11 (#1026)

collectWindowSpecs records each SelectInfo's own window columns on that
SelectInfo, through the whole set-operation tree, and returns the statement's
specs for the ParsedQuery.

A SET OPERATION's arms are SelectInfos of their own, and this pass used to
read `info.Columns` at the OUTERMOST level only — which for a set operation
is empty, since the columns live on the arms. `SelectInfo.Windows` was
therefore always nil for an arm, and it is the flag the logical builder
gates window planning on: an arm whose SELECT list is a BARE window
(`SUM(a) OVER () AS s`) got NO Window node, its projection was left reading
`s` off the arm's INPUT, and the query answered the input column of that
name (`decpair.s`, a TEXT column, #733), or failed with `column "s2" does
not exist in the input schema` when the input had no such column (#746), on
every path. A window nested inside a larger expression
(`SUM(a) OVER () + 1`) was unaffected, because the builder extracts those
from the column's own AST rather than from this list — which is why the
class was invisible to the arithmetic shapes in the corpus.

The two other post-parse passes over a set operation already descend into
the arms (CoerceBooleanLiterals recursively, resolvePositionalRefs through
resolveSetOpOrderBy). This one is the omission.
