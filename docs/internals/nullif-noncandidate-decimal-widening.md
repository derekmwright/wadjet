# Nullif noncandidate decimal widening

Source: internal/engine/expr/rettype.go — Ret.widenToDecimalBeyondCandidates, moved 2026-09-11 (#1026)

widenToDecimalBeyondCandidates is the NULLIF correction, and it is
deliberately the narrowest form of it.

PostgreSQL resolves NULLIF's TYPE with select_common_type over BOTH
arguments — they have to be comparable — while the RESULT is argument 0's
value and the TYPMOD is argument 0's. Wadjet folded the type over the
candidate list alone, so `NULLIF(0, numeric(9,2))` declared INT64 where
PostgreSQL says numeric, and the integer 0 went out as an integer column.

Folding EVERY argument into the type instead would be the general rule and
it is not taken here, for two reasons that both cost answers. It would widen
`NULLIF(numeric(9,2), numeric(18,4))` to (18,4), where the result is
argument 0's value and (9,2) holds it exactly — a rendering of 12.7500 for a
column that holds 12.75, which the corpus pins the other way. And it would
re-open the Guessed/Decided contract of #331/#333, where a non-candidate
argument deciding a type is exactly what must NOT displace the candidate's
answer.

So the widening fires only when the candidates produced a NON-DECIMAL type
and some other evaluated argument DECIDED a DECIMAL: that is the one case
where the candidate answer cannot represent the value the pair is compared
at, and it is the case PostgreSQL's numeric ladder is about.
