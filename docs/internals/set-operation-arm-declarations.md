# Set operation arm declarations

Source: internal/planner/physical/set_op_arm_decls.go — setOpArmDecls, moved 2026-09-11 (#1026)

```go
// setOpArmDecls describes the columns a set-operation ARM's SELECT list can
// name. It is inputColDecls' job with the two differences #551 and #554 turn
// on, and it exists separately because both differences are wrong for
// inputColDecls' other callers.
//
//  1. A JOIN keeps a PER-SIDE answer. inputColTypes and inputColDecimal merge
//     a join's two sides and DELETE any name they disagree on — right for a
//     TypeID, because two tables genuinely have two `dx` columns and picking
//     a side would answer about the wrong one. For a set operation that
//     disagreement IS the fact being reconciled: `a.dx DECIMAL(9,2)` beside
//     `b.dx DECIMAL(18,4)` resolved to DECIMAL with no (p,s), no coercion was
//     emitted, and each arm's .wshf file kept its own scale — the wider arm's
//     unscaled integer read at the narrower arm's scale, 100x out, silently
//     (#551). The projection names the column QUALIFIED, so the two sides are
//     told apart by keying each side's columns under its own relation names
//     as well as bare; the BARE name still merges-and-deletes, which remains
//     the honest answer for an unqualified reference two sides disagree on.
//
//  2. A PROJECT is descended INTO rather than stopped at, so a DERIVED-TABLE
//     arm resolves through the names its subplan EMITS (#554). The nested
//     set-operation arm already reads itself that way; a derived table is the
//     same shape one node down.
//
// The walk is deliberately STRICTER than emittedColTypes about what it will
// claim: a projection it cannot resolve produces NO entry, where
// declaredProjectionDecl answers STRING. That fallback is right for advisory
// wire metadata and poisonous here — a confidently-wrong arm type makes the
// ladder cast the column, which moves values.
```
