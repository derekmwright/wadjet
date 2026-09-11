# Oracle query determinism modes

Source: internal/oracle/compare.go — type CmpMode int, moved 2026-09-11 (#1026)

This file carries the determinism policy the generated-query differentials
(#288 follow-on) compare under. A generated query is only worth running if a
mismatch always means a defect, so every shape declares up front how much of
its answer SQL actually pins down:

  - no ORDER BY            → the row SEQUENCE is not part of the answer, so
                             compare the multiset (CmpUnordered).
  - ORDER BY, total order  → the sequence IS the answer and no two rows tie,
                             so compare positionally (CmpOrdered).
  - ORDER BY, ties possible→ compare the multiset AND the sequence of the
                             ORDER BY KEY values (OrderKeys). Tied rows carry
                             equal keys, so tie order can differ freely while
                             a dropped ORDER BY still shows up.
  - bare LIMIT             → which rows come back is genuinely arbitrary, so
                             compare row counts only (CmpCount).

CheckOrder is the absolute arm: it re-derives the ordering from the returned
key columns and fails a result that is not actually sorted. It needs no
second engine, so it catches an ORDER BY that both comparison arms drop the
same way — the shape that made today's ORDER BY defects invisible to a
two-arm compare.
