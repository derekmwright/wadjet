# Network literal binding and fallback

Source: internal/engine/expr/compile.go — func tryNetworkLit(col *ColRef, other Expr, op CmpOp, flip bool) *CmpNetworkLit {, moved 2026-09-11 (#1026)

tryNetworkLit builds a CmpNetworkLit when other is a string literal that
parses as an IPv4 address, a MAC address, an IPv6 address, or a CIDR
network; nil otherwise. Mirrors tryTemporalLit for the network-typed
columns: see CmpNetworkLit for why they need it. UUID does not go through
this path: ColRef.Eval already renders it as its zero-padded hex TEXT
(Vector.GetValue's default case), and lexical order of that fixed-width
text happens to equal the UUID's own byte order, which is enough for
ordering too, not just equality — an accident that does NOT generalize to
IPv6 (variable-width `::`-compressed hex) or CIDR (variable-width prefix
notation), which is why those two DO need a typed comparator (#492): the
column renders as text there too, but comparing that text lexically (<, >,
<=, >=) is not the address's numeric/structural order, is not even
consistent between this expr path's WHERE and SELECT evaluation, and used
to disagree outright with the stage DAG for IPv6 (both compile predicates
through this same function; before this fix, an IPv6/CIDR literal made
tryNetworkLit return nil, so the predicate fell to a plain *expr.Cmp and
its generic per-row path, which is where the lexical comparison happened).

Column type is unknown at compile time (see tryTemporalLit's own comment),
so this cannot be, and does not need to be, restricted to columns that are
ACTUALLY network-typed: a STRING column whose literal happens to parse as
an address (`s = '10.1.2.3'`) gets wrapped the same way, but
extractFilterOps' *expr.CmpNetworkLit case (internal/planner/physical/
plan.go) and CmpNetworkLit.EvalBoolNull's genericFallback both defer
entirely to the column's REAL type at kernel-build/eval time — a STRING
column takes its ordinary compareFilterString/lexical-compare path either
way, never one of the typed branches.
The IPv6 key comes from kernel.IPv6LitKey, which — unlike the local parse
this used to do — accepts a v4 literal too, keying it BELOW every v6 row
(PostgreSQL compares the address FAMILY first). The two encodings no longer
have to be mutually exclusive because the branch is chosen by the COLUMN's
resolved type, never by which parses the literal accepted: a v4 literal
legitimately keys as an IPv4 int64, as a v6 family sentinel, and as a /32
CIDR key all at once, and exactly one of those is read.
