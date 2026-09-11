# Oracle decimal text canonicalization

Source: internal/oracle/fingerprint.go — func canonicalDecimalCell(s string) string {, moved 2026-09-11 (#1026)
Superseded: The example ending in .7160549350 loses its final fractional zero; significant digits and numeric value are unchanged, not the original byte spelling.

canonicalDecimalCell renders a DECIMAL's text cell in the one spelling both
sides of a comparison reach it in: trailing FRACTION zeros removed, and no
decimal point at all when nothing is left after them.

A wadjet DECIMAL boxes as its rendered text at the column's DECLARED scale,
so a set operation whose common type is DECIMAL(12,1) renders 100.0 where
the value is a whole 100. The reference engine's cell has already been
through TextCell, which reads numeric-looking text as a float64 — and
fingerprintFloat renders a whole number as its digits, "100". The two are
the same number hashed to different digests, which is a fingerprint
artifact rather than an answer: it is the seam the exact literal arms of
#555 met, where `SELECT n_regionkey + 100 INTERSECT SELECT r_regionkey +
100.0` matches live DuckDB cell for cell and missed the stored digest.

Trimming, not floating: a wide DECIMAL keeps every digit it holds, so this
cannot weaken the exactness #455 established — 493827160549382.7160549350
is untouched, where reading it as a float64 would quantize it to six
significant digits.

A genuine TEXT column holding "1.50" now hashes with one holding "1.5".
That is narrow, and it is an ASYMMETRY being removed rather than a new
blind spot: TextCell already collapses both on the reference side.
