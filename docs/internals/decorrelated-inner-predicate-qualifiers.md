# Decorrelated inner predicate qualifiers

Source: internal/planner/logical/inner_key_spelling.go — innerOnlyPredicate, moved 2026-09-11 (#1026)

innerOnlyPredicate turns one of a decorrelated subquery's own WHERE
conditions into a plan Predicate, and reports ok=false when the rewrite
must DECLINE rather than produce one.

The decorrelations strip table qualifiers here, for the same reason they
strip them off a key: the inner plan is Scan → [Join …] → [Filter] and
carries SOURCE column names, which a single bottom Scan emits bare. Over a
JOINED inner that reasoning fails the same way #526's did, and worse: the
stripped predicate is pushed to whichever side of the join owns a column of
that bare name, so `WHERE c.n_nationkey < 3` over `nation c JOIN nation b`
filtered on b instead of c and the membership set became a different set
entirely — a silent wrong answer with no key involved.

Three outcomes over a joined inner, decided by how many of its relations the
condition names:

  - ONE, fully qualified: keep the qualifiers. pushFilterThroughJoin
    attributes it by exactly the refs collected here and lands it on that
    relation's own Scan, where the executor resolves the qualified name to
    the bare column it stores.
  - MORE THAN ONE: DECLINE the whole rewrite. There is no spelling that
    works: stripped, pushdown puts `c.x > b.x` on ONE scan as `x > x`
    (which evaluates against that relation's own column twice — the
    membership set collapses); qualified, it stays above the join, where
    the join emits one side's column bare and the qualified spelling names
    nothing. Declining leaves the IN a subquery predicate, executed as
    written — which the stage DAG can now do too (#524).
  - Unattributable — a bare reference, or a subquery inside the condition:
    stripped, exactly as before. A bare reference over a joined inner is
    ambiguous SQL unless one relation owns the name, and in that case the
    strip names it correctly.

A single-relation inner is unchanged: strip, and the bottom Scan emits it.
