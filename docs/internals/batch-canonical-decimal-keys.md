# Batch canonical decimal keys

Source: internal/engine/batch/decimal.go — const decimalKeyNegative = 0x80, moved 2026-09-11 (#1026)

--- Canonical DECIMAL key encoding ---

A DECIMAL group / DISTINCT / join / bloom key used to be
`math.Float64bits(v.ToFloat64(scale))`. A float64 carries ~16 significant
decimal digits and a DECIMAL(38,10) carries 38, so every pair of values that
agrees to 16 digits shared one key: GROUP BY collapsed them into one group,
COUNT(DISTINCT) counted them once, and a hash join matched each against the
other (#474).

Keying on the raw 16 bytes of the unscaled Int128 is the wrong repair: it
makes the key depend on the SCALE, so 12.75 stored in a DECIMAL(9,2)
(unscaled 1275) would stop matching 12.75 stored in a DECIMAL(18,4)
(unscaled 127500) — and a join between two tables that declare the same
quantity at different scales is exactly the shape that breaks. The
comparator (kernel.CompareDecimalAt) calls those two equal, and ADR-0012
item 8's invariant is that two values the comparator calls equal must also
SERIALIZE alike.

So the key is the value's canonical form: the unique (unscaled, scale) pair
with scale >= 0 minimal, i.e. trailing zero digits stripped from the
fraction. 12.75 is (1275, 2) from either column; 12.7500 normalizes to it;
1200 at scale 2 (unscaled 120000) and 1200 at scale 0 both normalize to
(1200, 0); zero is (0, 0) at every scale. That form is unique per VALUE, so
the encoding is injective by construction — different values cannot collide,
whatever their declared precision.
