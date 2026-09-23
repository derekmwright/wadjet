// SPDX-License-Identifier: AGPL-3.0-only

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

	// The QUALIFIED star over a correlated LATERAL with its own bound REFUSED
	// here while #1019 was open — the body's row count was not the one the
	// query wrote. Arc LT applies the bound per outer row (ADR-0021 §1s), the
	// `lateral/inner-order-*-limit/qstar` refusals are deleted and both cells
	// assert PostgreSQL's four rows; the deletion is the proof.
}
