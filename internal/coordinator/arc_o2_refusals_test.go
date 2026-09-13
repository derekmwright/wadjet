package coordinator

// THE SHAPES THIS ENGINE REFUSES WHERE POSTGRESQL ANSWERS, with the mechanism
// that keeps each loud.
//
// A refusal is not a pin. A pin records a WRONG VALUE this arc did not close;
// a refusal records the stand-in rule 11 asks for — "too structural → DEFER
// with the mechanism written down and a LOUD refusal in its place, never a
// plausible wrong value". Each entry is asserted on EVERY arm, so a spelling
// that starts answering (wrongly or not) fails here rather than drifting, and
// each is recorded in ADR-0012's divergence list because a client can hit it.
var o2Refuses = map[string]string{
	// A CORRELATED LATERAL WHOSE BODY CARRIES ITS OWN `LIMIT` (#1079).
	// PostgreSQL evaluates a LATERAL body once per OUTER ROW, so its bound
	// applies to each row's own result; the decorrelation makes the body ONE
	// relation joined once and the bound then applies to the whole of it —
	// `rows=3` where PostgreSQL answers 4, silently, on five arms and in every
	// spelling of the consumer. Honouring it means the bound travelling WITH
	// the correlation key as a per-key top-N, which is ADR-0021's territory;
	// until then the shape is loud. Arc N1 deferred the same defect with a
	// pin on the row count; this arc replaces the pin with the refusal,
	// because a plausible row count is the one thing a client cannot detect.
	"lateral/inner-order-pub-limit/star":     "inside a LATERAL subquery correlated on",
	"lateral/inner-order-pub-limit/qstar":    "inside a LATERAL subquery correlated on",
	"lateral/inner-order-pub-limit/list":     "inside a LATERAL subquery correlated on",
	"lateral/inner-order-hidden-limit/star":  "inside a LATERAL subquery correlated on",
	"lateral/inner-order-hidden-limit/qstar": "inside a LATERAL subquery correlated on",
	"lateral/inner-order-hidden-limit/list":  "inside a LATERAL subquery correlated on",

	// A BLOCK THAT PUBLISHES TWO COLUMNS OF ONE NAME, read by a QUALIFIED star
	// (#1076). The expansion emits one reference per published column, by
	// NAME, and two references spelled alike both bind the FIRST column of
	// that name: `SELECT x.* FROM (SELECT a.id, b.id …) x` published the first
	// `id` twice where PostgreSQL publishes the pair, and
	// `(SELECT order_id AS k, amount AS k …)` published a wrong TYPE with it.
	// Closing it means the block's published list travelling by POSITION —
	// `ProjectExprSpec.SourceSlot` one relation out. The BARE star over the
	// same blocks reads the relation positionally and is right, which is what
	// makes this the qualified spelling's own refusal.
	"dupname/qstar-join-body":   `column "x.*" does not exist in the input schema`,
	"dupname/qstar-two-tables":  `column "x.*" does not exist in the input schema`,
	"dupname/qstar-two-aliases": `column "x.*" does not exist in the input schema`,
	"dupname/qstar-cte":         `column "x.*" does not exist in the input schema`,
}
