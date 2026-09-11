# Lateral count default expression walk

Source: internal/planner/logical/lateral_empty_input.go — coalesceLateralCountRefs, moved 2026-09-11 (#1026)

coalesceLateralCountRefs returns node with every reference to one of the
lateral's COUNT outputs wrapped in COALESCE(…, 0). It returns the SAME node
when nothing matched, so a caller can tell a rewrite from a no-op.

The arm list is the whole contract, and a MISSING arm is silent: the
default case returns the node unwalked, so a reference under it keeps
reading the LEFT join's NULL. `WHERE s.n IN (0, 2)` dropped the unmatched
outer row for PostgreSQL's three, because InExpr had no arm while
BetweenExpr and IsExpr did.

Every plansql node that can CONTAIN a column reference is here:
ColRef, ParenNode, NotNode, UnaryOp, AndNode, OrNode, BinaryOp, CmpExpr,
IsExpr, LikeExpr, BetweenExpr, InExpr, AnyAllExpr, CastNode, FuncCallNode,
CaseNode, ArrayLitNode, TupleNode and WindowFuncNode — every node type in
internal/planner/sql that holds another node, StarNode and the two
text-carrying ones excepted.
SubqueryNode and ExistsNode are NOT walked because they carry SQL TEXT
rather than a tree — and NOT because a lateral output is out of their scope.
It is not: PostgreSQL resolves `(SELECT … WHERE i.amount > s.n * 40)`
against the lateral and applies the default there, where this engine
substitutes the LEFT pad's NULL per row through the re-run and answers 0
for PostgreSQL's 4. Both spellings are pinned in the correlation census and
ADR-0021 §1h states the boundary positionally: every position in the
enclosing query's own expression trees, no position inside a subquery's
text.
