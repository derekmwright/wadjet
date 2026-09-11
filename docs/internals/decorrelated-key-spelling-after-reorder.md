# Decorrelated key spelling after reorder

Source: internal/planner/logical/inner_key_spelling.go — repairDecorrelatedSpelling, moved 2026-09-11 (#1026)

Spelling a decorrelated subquery's own column references
------------------------------------------------------------------

decorrelateInSubqueries and decorrelateExists lower an IN / EXISTS to a
semi/anti join whose BUILD side is the subquery's own plan —
Scan → [Join …] → [Filter] → [Aggregate], and never a Project. That side
therefore carries the SOURCE column names of the relations it reads, and
the rewrites have to name their build-side keys the way it emits them.

With ONE inner relation that is knowable on the spot: the bottom Scan
emits every column bare, so the key is the source column with any
qualifier stripped (#516). With a JOIN it is not knowable on the spot at
all. A join emits its PROBE side's columns bare and qualifies a BUILD
column only where the bare name collides (exec.joinOutputSchemaWithMapping),
and which side is which is decided by reorderJoins from estimated row
counts at Optimize step 73 — long after the rewrites run at steps 35/36.
Naming the key from write order then answers over whichever relation the
estimator happened to put on the probe (#526), and correlating on a
stripped column correlates on whichever relation the estimator put there
(#527). Both are silent: the physical planner splits the condition
literally and exec.HashJoin's key repair swaps the pair.

So the rewrites record what they MEAN — the relation qualifier and the
source column, as the subquery wrote them — and repairDecorrelatedSpelling
settles the TEXT after reorderJoins has made the join order final, by
modelling what each build subtree actually emits.
