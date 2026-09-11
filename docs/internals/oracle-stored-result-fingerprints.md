# Oracle stored result fingerprints

Source: internal/oracle/fingerprint.go — type Fingerprint struct {, moved 2026-09-11 (#1026)
Superseded: Dual precision is a tolerance policy, not a proof that every floating accumulation perturbation must match at one precision.

Fingerprint is a stored-comparable digest of a whole result: the row
count plus one digest per precision of the canonical row rendering. It
exists so a reference engine's answer can be committed to a file and
compared later without that engine on the machine — the shape the
DuckDB ground-truth gate needs.

Three properties the gate depends on, in the order they were learned the
hard way:

  - Every column is covered, strings and NULLs included. A per-column
    numeric sum (internal/harness's value signature) skips string columns
    entirely, which is how a NULLed name column (#314) shipped green.
    Here a NULL renders "<null>", distinct from the empty string, and a
    string cell contributes its bytes.

  - Order sensitivity is the CALLER's decision, per query. With ordered
    set, the row sequence is part of the digest, so a dropped ORDER BY
    (#313/#316/#320) changes it; without, rows are sorted first, so an
    engine free to return them in any order is not held to one. Passing
    ordered for a query with no top-level ORDER BY would manufacture
    failures; passing it false for one that has an ORDER BY is the blind
    spot those three bugs walked through.

  - Float summation order does not move it. The rows are rendered at two
    precisions and a match at EITHER counts, which is the same
    dual-precision policy Canon.Diff applies and for the same reason (see
    the Canon doc comment): one ULP of accumulation noise can flip a
    rendered digit at a rounding boundary, but not at two independent
    quanta at once.
