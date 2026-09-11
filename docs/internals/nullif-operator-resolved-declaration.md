# Nullif operator resolved declaration

Source: internal/engine/expr/rettype.go — func (r Ret) operatorResolvedType(d DeclType, seen []DeclType, conf []Confidence, nargs int) (DeclType, bool) {, moved 2026-09-11 (#1026)

operatorResolvedType is NULLIF's own rule, and it is not select_common_type
(#757).

PostgreSQL types NULLIF from the `=` OPERATOR its two arguments select, and
the answer is that operator's LEFT input type. Measured live on 17, every
row of it:

	NULLIF(int4,  int8)     integer            <- argument 0, not the common type
	NULLIF(int8,  int4)     bigint
	NULLIF(int2,  int8)     smallint
	NULLIF(float4,float8)   real               <- argument 0 again
	NULLIF(float4,int4)     real
	NULLIF(float4,numeric)  real
	NULLIF(numeric,int4)    numeric
	NULLIF(int4,  numeric)  numeric
	NULLIF(int8,  numeric)  numeric
	NULLIF(int4,  float4)   double precision   <- NOT real, and NOT argument 0
	NULLIF(int4,  float8)   double precision
	NULLIF(numeric,float4)  double precision   <- NOT real

The last three are what makes this a separate rule rather than a fold:
GREATEST and COALESCE over `(numeric, float4)` are BOTH `real` on the same
server, because they run select_common_type and float4 wins that ladder.
NULLIF has to find an operator, there is no `int4 = float4` or
`numeric = float4`, so both sides coerce to the preferred type in the
category — float8 — and the operator's left input is float8.

So: within the integer family and within the float family, and whenever
argument 0 is itself a float, the cross-type operator exists and argument 0's
own width is the answer. An integer or a numeric compared against a FLOAT
resolves to float8. Everything else is the ordinary ladder.

Wadjet answered argument 0's type for ALL of these, because NULLIF's
candidate list is [0]. The values agree on the census fixture, so this is an
OID and typmod divergence today — and a wrong answer waiting, since a value
only representable at the wider type would be narrowed into the output
vector on the way out.

It fires only for a declaration that names an operator-resolved pair
(Ret.opResolved, set by NULLIF's registration alone) with exactly two
arguments, both Decided, both numeric.
