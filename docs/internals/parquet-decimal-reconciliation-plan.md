# Parquet decimal reconciliation plan

Source: internal/storage/parquet/decimal_reconcile.go — func DecimalRescalePlan(leaf *SchemaNode, want Column) (fromScale int, need bool) {, moved 2026-09-11 (#1026)

DecimalRescalePlan is what one column read needs to know to reconcile a
file's DECIMAL declaration with the catalog's: the scale to move FROM and
whether any move is needed.

need=false covers both no-op cases — the leaf is not a decimal, or it
declares the catalog's own (p, s) — so a caller writes one branch and the
ordinary file pays two integer comparisons per column chunk.

The boundary, stated because it is a claim and the corpus attempts it from
both sides: this fires on a DECLARATION disagreement, and a file that agrees
with the catalog is read exactly as before. The two halves of the declaration
are treated alike and BOTH are reconciled, which is the round-0 review's P2:

  - a SCALE disagreement moves the carrier (the file holds the right number
    under a different half of the declaration, so the number survives);
  - a PRECISION disagreement moves nothing, but the value is held to the
    catalog's band, because a file declaring `(38,2)` under a catalog column
    of `(15,2)` can carry a value that column promises not to hold. Before
    this, the native scan ANSWERED such a value — a 20-digit number in a
    column whose wire declaration says at most 15 digits, which PostgreSQL
    cannot reach (`…::numeric(15,2)` is 22003) — while the row reader
    refused the same bytes with a different message. One disposition on both
    paths, and it is PostgreSQL's (ADR-0013's two-path property, ADR-0024).

The cost stays off the ordinary path: this writer writes the catalog's
declaration, so both halves match and the check is two comparisons per column
chunk. Only a file some other producer wrote pays anything per value.
