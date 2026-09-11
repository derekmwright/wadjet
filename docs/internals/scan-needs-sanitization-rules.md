# Scan needs sanitization rules

Source: internal/planner/logical/optimizer.go — sanitizeScanNeeds, moved 2026-09-11 (#1026)

sanitizeScanNeeds turns the ancestor-accumulated needs set into a clean
RequiredColumns list for one scan. The accumulated set carries junk the
scan can never produce — alias-qualified duplicates ("l1.l_receiptdate"
next to "l_receiptdate") and the OTHER side's join-key columns
("s_suppkey" landing on a lineitem scan via "s_suppkey = l1.l_suppkey").
Any such name trips the worker's all-or-nothing parquet projection guard
(cachedFileStreamSource.projectColumns) and silently reverts the scan —
and every shuffle fed by it — to full width: Q21's l1 leg measured
143 B/row against the 25 B/row its clean sibling leg achieves
(docs/design/exchange-reuse.md §2 A1).

Rules, conservative toward keeping:
  - "alias.col" where alias is THIS scan (TableAlias or TableName):
    rewritten to bare col. Other aliases: dropped — provably another
    relation's column.
  - bare names when ScanColumns (catalog schema, AnnotateScanColumns) is
    known: kept iff in the schema, EXCEPT "__"-prefixed derived names
    (e.g. __having_0), which are kept so the worker guard's
    derived-column semantics are preserved exactly.
  - bare names when ScanColumns is empty (no catalog at plan time):
    kept — we cannot judge, and full width is the safe failure mode.

Output is sorted for deterministic plans.
