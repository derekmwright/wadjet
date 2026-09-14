package coordinator

// THE SHAPES THIS ENGINE REFUSES WHERE POSTGRESQL ANSWERS, with the mechanism
// that keeps each loud.
//
// A refusal is not a pin. A pin records a WRONG VALUE this arc did not close;
// a refusal records the stand-in rule 11 asks for — "too structural → DEFER
// with the mechanism written down and a LOUD refusal in its place, never a
// plausible wrong value". Each is asserted on EVERY arm, and each is recorded
// in ADR-0012's divergence list because a client can hit it.
//
// MEASURED AT BASE: four of the six ANSWERED WRONGLY at `0193c4e9` (a
// WRONG → LOUD move) and two were already LOUD there (LOUD → LOUD). None was
// RIGHT at base — a refusal that replaces a right answer is a regression, and
// round 2 shipped one by triggering on a bound's EXISTENCE rather than on the
// defect.
var o2Refuses = map[string]string{
	// A BLOCK THAT PUBLISHES TWO COLUMNS OF ONE NAME, read by a QUALIFIED star
	// (#1076). The expansion emits one reference per published column, by
	// NAME, and two references spelled alike both bind the FIRST column of
	// that name: `SELECT x.* FROM (SELECT a.id, b.id …) x` published the first
	// `id` twice where PostgreSQL publishes the pair, and `(SELECT order_id AS
	// k, amount AS k …)` published a wrong TYPE with it. Closing it means the
	// block's published list travelling by POSITION —
	// `ProjectExprSpec.SourceSlot` one relation out. The BARE star over the
	// same blocks reads the relation positionally and is right, which is what
	// makes this the qualified spelling's own refusal.
	"dupname/qstar-join-body":   `column "x.*" does not exist in the input schema`,
	"dupname/qstar-two-tables":  `column "x.*" does not exist in the input schema`,
	"dupname/qstar-two-aliases": `column "x.*" does not exist in the input schema`,
	"dupname/qstar-cte":         `column "x.*" does not exist in the input schema`,

	// A QUALIFIED STAR OVER A CORRELATED LATERAL WHOSE OWN BOUND IS NOT
	// APPLIED PER OUTER ROW (#1019). The body's COLUMNS are knowable; its ROW
	// COUNT is not the one the query wrote, because the decorrelation applies
	// the bound to the whole inner relation once. A star publishes a RELATION,
	// so this is a relation this pass cannot state — the same rule as the four
	// above, one property over — and the spelling keeps the refusal it had at
	// base.
	//
	// Only this consumer declines. The named spellings (`SELECT *`, an
	// explicit list) keep the disposition they had, with the row count PINNED
	// and PostgreSQL's answer beside it: whether a bound BINDS is a property
	// of the DATA, so refusing on a bound's existence turned `LIMIT 10` over a
	// body that never yields ten rows for one key — right on five arms at base
	// — into an error, which is the regression round 2 shipped and round 3
	// took out.
	"lateral/inner-order-pub-limit/qstar":    `column "x.*" does not exist in the input schema`,
	"lateral/inner-order-hidden-limit/qstar": `column "x.*" does not exist in the input schema`,
}
