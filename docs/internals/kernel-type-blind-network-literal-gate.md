# Kernel type blind network literal gate

Source: internal/engine/exec/kernel/compare.go — func CidrAddressText(s string) bool {, moved 2026-09-11 (#1026)

CidrAddressText reports whether s names an address WITHOUT PostgreSQL's
abbreviated cidr grammar — the question a site asks when it does NOT know
the column's type, and the accept-set CidrSortKey had before #627 widened it.

The two callers are `expr.tryNetworkLit` and `expr.firstNonAddressLit`, and
both are type-blind by construction: the column's declared type is not known
at compile time, so they ask "could this literal be an address in ANY
family" and let the column's real type pick the branch at eval time.

Handing them the widened grammar was a silent wrong answer, caught by the
PostgreSQL oracle: the first version of #627's fix read the CIDR type's
grammar, under which `'3.1'` and `'2'` ARE addresses (3.1.0.0/16 and
2.0.0.0/8), so `CASE WHEN d_val < '3.1' THEN 1 ELSE 0 END = 1` compiled to a
network comparison over a DOUBLE column and answered 15 rows where
PostgreSQL answers 8 — the literal ordered as an ADDRESS where the column
wanted a number. The grammar is inet's now and no bare number reaches this
question, but the MASKED abbreviations still do: `'10/8'` is an address
beside a cidr column and a 22P02 beside a numeric one, which is exactly the
knowledge these two sites lack.

Every CIDR-TYPED site keeps the wide grammar: the kernel's TypeCIDR arm, the
IN set, the row-group bound, the boxed-pair key and the plan-time refusal
all know the column is a cidr, and `cd = '10/8'` finds its row through them.
