package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Shared producers, and why nothing consumer-specific may ride on one.
//
// walkStages dedups a CTE referenced more than once: the FIRST reference
// walked emits the body's real stages, and every later one becomes a
// `cte-alias` phantom pointing at that body's terminal. The two references
// therefore READ ONE STAGE, which is the whole point — Q15's dual-chain float
// drift is what the dedup exists to prevent.
//
// It also means that stage's output belongs to every reference equally. A
// WHERE above ONE reference is not a property of the CTE; attaching it to the
// shared terminal filters the stream every other reference reads. #656 named
// that hazard for the deduped ALIAS ("its target is SHARED … must not be
// filtered for it") and closed it in one direction only: the alias got a
// StageProject, while the FIRST reference — whose producer IS the shared
// terminal — kept attaching straight onto it. A CTE referenced twice with the
// WHERE on the first reference answered 18 rows where PostgreSQL answers 109,
// silently, and three references answered 27 where PostgreSQL answers 119.
//
// countCTEReferences is what makes the shared case knowable BEFORE the second
// reference is walked; sharedStageTerminals is the answer per stage.

// countCTEReferences counts how many times each CTE name appears in the
// logical plan, keyed lowercased. Every reference is a subtree tagged with
// CTEName, so the count is the number of tagged nodes.
func countCTEReferences(root *logical.Node) map[string]int {
	out := map[string]int{}
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.CTEName != "" {
			out[strings.ToLower(n.CTEName)]++
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)
	return out
}

// assertNoConsumerScopedFilterOnSharedStage reports the first stage that has
// more than one consumer AND carries a filter or a projection marked as
// belonging to a single consumer.
//
// It is the structural half of the guard above: the attach-time refusal is
// what prevents the defect, and this is what notices if a future pass
// reintroduces it — including passes that MERGE two stages into one and so
// create a second consumer after the fact.
func assertNoConsumerScopedFilterOnSharedStage(stages []Stage) error {
	consumers := map[string]int{}
	for i := range stages {
		for _, dep := range stages[i].Dependencies {
			consumers[dep]++
		}
	}
	for i := range stages {
		s := &stages[i]
		if consumers[s.ID] < 2 {
			continue
		}
		// The question is OWNERSHIP, not presence, and the marker is the
		// only thing that knows it — in BOTH directions.
		//
		// A shared stage may perfectly well carry a filter or a projection
		// that belongs to the RELATION every consumer reads: a scan's own
		// pushed-down predicate (`WITH c AS (SELECT … WHERE id < 100)`
		// referenced twice), and — the case that made this concrete — the
		// aggregate-output projection absorbAggregateOutputProjection puts
		// on a CTE body whose group key is computed. `WITH a AS (SELECT g+1
		// AS gk, COUNT(*) AS n FROM t GROUP BY g+1) SELECT gk FROM a UNION
		// ALL SELECT gk FROM a` has no filter anywhere and PostgreSQL
		// answers 16 rows; a rule that refused any projection on a shared
		// stage refused the query outright.
		//
		// What makes an attachment unsafe is that it belongs to ONE
		// consumer, and stage emission is where that is knowable: it
		// distinguishes a predicate attached INSIDE a CTE body from one
		// attached ABOVE a reference.
		//
		// filterCarrierIndex sets the marker and this walk is its LIVE
		// trigger: it attaches a consumer's filter to a CTE terminal that is
		// not yet known to be shared and marks it, and #876's own residual —
		// two scalar producers that each filter one reference of one CTE —
		// is refused here when the second producer is pointed at that stage
		// (TestArcH1TwoFilteredProducersOverOneCTEAreRefusedLoudly).
		//
		// #876 tried the stronger rule — never attach to any recorded CTE
		// terminal, give the consumer its own StageProject — and WITHDREW it:
		// the project that then carries an outer WHERE over a shared CTE
		// answered ZERO for PostgreSQL's 4838 on Q15's own shape. So the
		// marker stays, and so does this assert.
		//
		// Deriving ownership structurally instead would mean asking whether
		// every consumer's LOGICAL ancestry carries the same predicate, and
		// this pass is handed only []Stage.
		if !s.ConsumerScoped {
			continue
		}
		return &sharedProducerError{stage: s.ID, consumers: consumers[s.ID]}
	}
	return nil
}

// sharedProducerError is the refusal a consumer-scoped filter on a shared
// producer earns.
type sharedProducerError struct {
	stage     string
	consumers int
}

func (e *sharedProducerError) Error() string {
	return fmt.Sprintf("native-DAG: stage %s has %d consumers and carries a filter or "+
		"projection scoped to ONE of them; every other consumer would read the filtered "+
		"stream (#656)", e.stage, e.consumers)
}
