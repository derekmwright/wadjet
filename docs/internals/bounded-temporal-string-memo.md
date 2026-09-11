# Bounded temporal string memo

Source: internal/engine/expr/expr_helpers.go — const temporalMemoCap = 4096, moved 2026-09-11 (#1026)

Deterministic temporal-string parsers are called row-by-row from
Cmp.EvalBool whenever a date/timestamp value is compared against a string.
At SF100 the 22Q suite spent 4.24% of worker CPU (236s cum) re-parsing
strings — every row of every filter re-walked the layout list — so the
result is memoized.

The memo is BOUNDED, and the reason is that its stated rationale was
wrong about its own population. The original argument was "SQL queries
have a fixed, tiny set of date literals … so a memoization cache stays
trivially small and never grows unbounded". But the LITERAL shape never
reaches here: compileCmp specializes a bare column against a string
literal into CmpTemporalLit, which pre-parses once at compile time
through the UNCACHED entry points, so `ts <= '1998-09-02'` adds zero
entries. What does reach the memo is, by construction, the shape that
specialization declined — a temporal value against another COLUMN's text
— and those strings are DATA. The population is unbounded and the map is
process-global with no eviction, so a query over a text column of
timestamps added one entry per distinct value for the process's lifetime
(#619).

The bound is a generational reset rather than an LRU: the memo exists to
collapse repetition WITHIN a scan, so dropping the whole generation when
it fills costs a re-parse of the current working set and nothing else,
for one counter and no eviction bookkeeping.
