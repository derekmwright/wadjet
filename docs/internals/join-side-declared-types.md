# Join side declared types

Source: internal/planner/physical/join_key_types.go — joinSideColTypes, moved 2026-09-11 (#1026)

```go
// joinSideColTypes reports the declared type of every column ONE SIDE of a
// join can offer a key, keyed by the name a key may SPELL it with.
//
// It is the shared declared-type layer, not a walk of its own. The first
// version of this function WAS a walk of its own — scans and rename
// projections only — and it answered nothing for a side rooted at an
// aggregate, a window or a set operation, and dropped every computed
// projection. resolveJoinKeyTypes then emitted KeyTypeUnresolved and
// joinKeyUsesIntPath fell back to isIntKeyColumn(own), which is the exact
// gate #615 replaces: `a.w_d2 = b.k` over `(SELECT w_i64 AS k … GROUP BY
// w_i64)` answered 0 where PostgreSQL answers 3, and the CAST spelling of it
// panicked on the DAG.
//
// Two maps, merged, because a key can be spelled either way:
//
//  1. What the side EMITS, under the names it emits them: emittedColTypes
//     for an aggregate / window / projection / DISTINCT chain (its Project
//     arm types a CAST through declaredProjectionType, which is where
//     inferCastType lives), and setOpDeclaredOutputSchema for a side that IS
//     a set operation — the arms reconciled through setOpWiden, the same
//     ladder the executed schema uses. This is the spelling a derived
//     table's key actually takes (`b.k`).
//  2. The SOURCE names still visible below a RENAME, for the spelling
//     resolveShuffleKey produces when it resolves an alias back to the
//     column the shuffle reads. A rename carries its source's values, so
//     both names describe one type; a COMPUTED projection binds only its
//     alias, and its inputs keep their own types under their own names,
//     which is correct — a key spelled with the input name is keyed on the
//     input.
//
// A name the two disagree about is DELETED rather than picked between: a key
// resolved against the wrong column is a silently different join, and
// declining leaves the runtime backstop (exec.joinKeyEncodingMismatch) to
// raise if the two sides really do disagree at run time.
```
