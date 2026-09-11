# Boxed text operand rendering

Source: internal/engine/expr/expr_cast.go — func boxedTextOperand(b *batch.RecordBatch, row int, operand Expr, v any) any {, moved 2026-09-11 (#1026)
Superseded: boxedTextOperand also resolves temporal CASTs and ROW field paths; the bare-column-only boundary below predates those cases.

boxedTextOperand renders a bare-column operand as the text the column's
own value PRINTS as — which is, by construction, the text the vectorized
kernel matches/renders against (kernel.likeTextRenderer's default arm is
fmt.Sprint(Vector.GetValue(i)), and its per-type arms were written to agree
with that rendering; CAST AS STRING's other arms and every scalar function
argument already use the same GetValue rendering for every OTHER type,
via ColRef.Eval's own default case). Mirrors temporalOperand's contract:
only a bare column reference is resolved, and every other operand shape
(an already-string value, a nested expression, a literal) passes v through
unchanged.

ColRef.Eval boxes four types differently from GetValue, for speed on the
numeric paths that dominate it, and all four made a caller here match or
render a DIFFERENT STRING from the one the scan's kernel or the plain
projection would — the same query answering two ways depending on which
evaluator reached the column:

	IPv4, MAC  the raw encoded int64, so `ipv4_col LIKE '10.%'` matched the
	           digits of that integer instead of the address text, and
	           `CAST(ipv4_col AS STRING)` stringified the number
	DATE       the epoch DAY, so `c_date LIKE '20%'` was false for
	           2011-02-02 and true for the day number 20123, and
	           `CAST(c_date AS STRING)` answered "15007" instead of the date
	FLOAT32    widened to float64, so 1/7 printed 0.1428571492433548 here and
	           0.14285715 through the kernel or a bare projection

The IPv4/MAC LIKE pair was fixed with #497; DATE and FLOAT32's LIKE
rendering were found by the review of it. This function used to be two
near-identical copies — likeOperand (LIKE's call site, all four types) and
networkOperand (Cast's, IPv4/MAC only) — which is exactly the two-
implementation drift ADR-0012 keeps calling out elsewhere (CidrSortKey,
appendColumnValue): CAST(date_col AS STRING) and CAST(f32_col AS STRING)
were still wrong through networkOperand's narrower list (#521) after LIKE
had already been fixed for the identical types. One function, every
caller, closes both at once: `wadjet.TestLikeAnswersTheSameAtBothSites`
sweeps every flat type through the LIKE call site so a fifth type that
starts boxing differently is a failing test rather than another quiet
divergence.
