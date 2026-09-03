package wadjet

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The spill sweep: every type, through every spilling operator, must answer
// what the same query answers with memory to spare.
//
// Why a sweep and not a test per defect. The four defects the spill arc fixed
// (#782's split groups and its DECIMAL twin, #779, #632) are all
// CONDITION-triggered: the query is correct, the plan is correct, and the
// answer is wrong only when a drain lands on a particular batch. No workload
// discipline avoids them and no shape-based corpus finds them, because the
// shape is not what is wrong. What finds them is running the same query on
// both sides of a memory budget and comparing every row.
//
// REPLICATION IS THE POINT. One passing spilled run proves nothing here: the
// budgeted arm of #782's DECIMAL twin answered correctly on three runs out of
// five, and #788 — a DATE group key that keyed as digits from one producer and
// as its display text from another — was wrong on nine runs out of twelve.
// Each cell runs spillMxRuns times (5, or 1 under -short) and every run is
// compared, so a cell that is wrong one time in five fails.
//
// Coverage: every one of the 18 flat types as a GROUP BY key (on the
// partial-state path AND on the legacy raw-row path), as a DISTINCT value, a
// sort key, a window PARTITION BY key and a window ORDER BY key; as MIN/MAX
// input; the numeric ones as SUM/AVG input; and each type again under a join,
// which is what produces the grace-partitioned probe replay #782 lived on.
// The four container types are covered by
// TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget in this package, which
// carries the drain-engagement assertion for them.
//
// What this gate deliberately tolerates, and why:
//
//   - FLOAT values are compared to 6 significant digits. Parallel partial
//     aggregation sums in a different order under a budget than without one,
//     which moves a float64 sum in its last digits — ADR-0013's
//     nondeterminism class 9, not a defect. Every other type, DECIMAL
//     included, is compared exactly.
//   - A cell marked budgetMayRefuse accepts "memory budget exceeded" as an
//     outcome, because a loud refusal is the right answer to a budget too
//     small for the shape (ADR-0006); a WRONG answer is not, so the comparison
//     still runs on the runs that answered, and a cell where NO run answered
//     fails as vacuous. This tolerance RATCHETS like the other two (#824): a
//     cell that ANSWERS on every replication fails, naming the tolerance to
//     delete. NO CELL CARRIES IT TODAY — the eighteen join cells that did were
//     tolerating #789's nondeterministic refusal, and every one of them answers
//     on every run at the budget it runs at. What they still carry is the
//     BUDGET that tolerance came with, and that is ratcheted separately at the
//     end of this test. The field and its ratchet stay for the next shape that
//     needs one.
//   - A cell with a knownBug is compared and its divergence logged instead of
//     failed, and it FAILS IF IT STARTS AGREEING — the ADR-0013 ratchet, so
//     the pin cannot outlive its bug.
func TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget(t *testing.T) {
	ctx := context.Background()
	// The REFERENCE is taken before either forcing knob is armed, so the two
	// sides of every comparison differ in the spill and in nothing else.
	ref := spillMxOpen(t, 0)
	refs := make(map[string][]string, 200)
	for _, cell := range spillMxCells() {
		want, err := tmRun(ctx, ref, cell.sql)
		if err != nil {
			t.Fatalf("unbudgeted %s: %v\n  SQL: %s", cell.name, err, cell.sql)
		}
		if len(want.Rows) == 0 {
			t.Fatalf("the unbudgeted run of %s returned no rows — this cell would compare nothing\n  SQL: %s",
				cell.name, cell.sql)
		}
		refs[cell.name] = spillMxRender(want.Columns, want.Rows, cell.ordered)
	}

	// Reaching the sort, window and raw-row paths at all is not a budget
	// question. minSortRunBytes and spillFileTargetBytes are 64 MiB floors that
	// a 1.2 MB fixture cannot cross at ANY budget, so without this seam those
	// three families compare two in-memory runs — 54 of this file's cells did
	// exactly that, and memory.SpillManager.SpillRows (the encoder #632 IS) was
	// never invoked by the sweep at all. Lowering the floor changes how BIG a
	// run is, not what the operator computes, so the arm still means "under a
	// memory budget".
	//
	// The aggregate's drain is deliberately NOT forced here. ForceAggDrainEvery(1)
	// bypasses the #325 productivity gate, and doing that with a DECIMAL group
	// key collapses 4952 groups to ~1090 (#790) — a defect of its own, and one
	// production's gate prevents. Forcing it would make this arm test a
	// configuration nothing runs in. The aggregate cells reach their drain
	// through real pressure, which is what the engagement counter checks.
	defer exec.ForceSmallSpillRuns(4096)()

	arms := map[int64]*DB{}
	for _, b := range []int64{spillMxBudget, spillMxJoinBudget} {
		arms[b] = spillMxOpen(t, b)
	}
	engagedCells, engagedTotal, cellsByFamily := map[string]int{}, map[string]int64{}, map[string]int{}

	for _, cell := range spillMxCells() {
		t.Run(cell.name, func(t *testing.T) {
			budget := cell.budget()
			arm := arms[budget]
			w := refs[cell.name]
			engagedBefore := cell.engagement()
			// A cell whose defect is CONDITION-triggered arms the drain knob
			// for its own runs, so the condition is reached on EVERY run
			// instead of on a coin flip (ADR-0027 decision 6). The knob is
			// restored before the next cell, and the reference arm was taken
			// with it DISARMED at the top of this test.
			if cell.forceDrainEvery > 0 {
				prevKnob := exec.ForceAggDrainEvery(cell.forceDrainEvery)
				defer exec.ForceAggDrainEvery(prevKnob)
			}
			answered, agreed, refused := 0, 0, 0
			for run := 0; run < spillMxRuns(); run++ {
				got, err := tmRun(ctx, arm, cell.sql)
				if err != nil {
					if cell.budgetMayRefuse && strings.Contains(err.Error(), "memory budget exceeded") {
						refused++
						continue // a loud refusal, see the doc comment
					}
					if cell.knownError != "" && strings.Contains(err.Error(), cell.knownError) {
						continue // the pinned loud failure, see knownError
					}
					t.Fatalf("under a %d KiB budget, run %d: %v\n  SQL: %s", budget/1024, run, err, cell.sql)
				}
				answered++
				g := spillMxRender(got.Columns, got.Rows, cell.ordered)
				diff := spillMxDiff(g, w)
				if diff == "" {
					agreed++
					continue
				}
				if cell.knownBug != "" {
					t.Logf("known divergence (%s), run %d: %s\n  SQL: %s", cell.knownBug, run, diff, cell.sql)
					continue
				}
				t.Fatalf("under a %d KiB budget, run %d: %s\n  SQL: %s", budget/1024, run, diff, cell.sql)
			}
			if cell.knownError != "" {
				if answered > 0 {
					t.Fatalf("pinned as a loud failure (%s), but %d of %d budgeted runs ANSWERED — "+
						"delete the pin, that answer is the fix's proof\n  SQL: %s",
						cell.knownBug, answered, spillMxRuns(), cell.sql)
				}
				t.Logf("known loud failure (%s) on all %d runs: %q", cell.knownBug, spillMxRuns(), cell.knownError)
				return
			}
			if answered == 0 {
				t.Fatalf("every run refused the budget — this cell compared nothing\n  SQL: %s", cell.sql)
			}
			// ENGAGEMENT, recorded per cell and asserted per family below. A
			// cell that never reached its own operator's spill path compared
			// two in-memory runs and would pass with that path deleted — the
			// anti-pattern TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget
			// records having fallen into once already.
			cellsByFamily[cell.family()]++
			if d := cell.engagement() - engagedBefore; d > 0 {
				engagedCells[cell.family()]++
				engagedTotal[cell.family()] += d
			} else if cell.noSpill != "" {
				t.Logf("no spill expected here (%s)", cell.noSpill)
			} else {
				t.Logf("no %s in %d runs (see the family assertion at the end of this test)",
					cell.engagementName(), answered)
			}
			// The FORCED-DRAIN arm, for the plain GROUP BY shapes. Under a
			// budget the drain lands where tracker timing puts it; here it
			// lands on every batch, which is what exposed #790 (a clone's
			// SpillSome runs orphaned at the merge: 5000 rows in, 1100 out).
			// Two runs, because the knob makes it deterministic. The
			// reference is the same disarmed one — a knob armed on BOTH sides
			// lets a shared defect cancel out, which is how #790 first read as
			// a regression rather than as a pre-existing bug.
			if cell.forcedDrainArm {
				restore := exec.ForceAggDrainEvery(1)
				for run := 0; run < 2; run++ {
					got, err := tmRun(ctx, arm, cell.sql)
					if err != nil {
						exec.ForceAggDrainEvery(restore)
						t.Fatalf("drain-every-batch run %d: %v\n  SQL: %s", run, err, cell.sql)
					}
					if diff := spillMxDiff(spillMxRender(got.Columns, got.Rows, cell.ordered), w); diff != "" {
						exec.ForceAggDrainEvery(restore)
						t.Fatalf("drain-every-batch run %d: %s\n  SQL: %s", run, diff, cell.sql)
					}
				}
				exec.ForceAggDrainEvery(restore)
			}
			// The ratchet: a pin that starts agreeing has outlived its bug and
			// must be deleted. It fires only on a sample that could have SEEN
			// the divergence, which is the same rule the rest of this file
			// applies to the spilled arm — one passing run proves nothing.
			//
			// A pinned defect here may be nondeterministic (#788 was wrong on
			// 9 to 12 runs out of 12), so under the reduced replication of
			// `-short` a single lucky run reads exactly like a fix and turned
			// this gate red at random on CI, on a tree where nothing about the
			// pinned bug had changed. Agreement is a fix's proof at the full
			// run count and a coin toss at one; below the threshold it is
			// logged, and the full-replication arm (CI's Type-Matrix step runs
			// this file without -short) keeps the ratchet honest.
			if cell.knownBug != "" && agreed == answered {
				if answered < spillMxRatchetMinRuns {
					t.Logf("pinned as %s and all %d budgeted run(s) agreed — too few runs to tell a fix "+
						"from a lucky sample of a nondeterministic defect, so the ratchet is not fired here; "+
						"the full-replication arm decides", cell.knownBug, answered)
				} else {
					t.Fatalf("pinned as %s, but all %d budgeted runs agreed — delete the pin, that agreement is the fix's proof\n  SQL: %s",
						cell.knownBug, answered, cell.sql)
				}
			}
			// The SAME ratchet for budgetMayRefuse (#824). knownBug and
			// knownError both fail the cell when the pinned state stops
			// reproducing; budgetMayRefuse did not, so a cell could answer on
			// every replication for release after release with the tolerance —
			// and the budget it was moved to — quietly outliving the defect
			// that justified them. A tolerance nothing checks is indisting-
			// uishable from a tolerance nothing needs.
			//
			// The threshold is the knownBug ratchet's, for the same reason:
			// the refusal being tolerated here is NONDETERMINISTIC (#789 — 2
			// of 20 at 512 KiB), so "no run refused" is evidence at the full
			// replication count and a coin toss at one.
			if cell.budgetMayRefuse && refused == 0 {
				if answered < spillMxRatchetMinRuns {
					t.Logf("marked budgetMayRefuse and all %d budgeted run(s) answered — too few runs to tell a "+
						"stale tolerance from a lucky sample of a nondeterministic refusal, so the ratchet is not "+
						"fired here; the full-replication arm decides", answered)
				} else {
					t.Fatalf("marked budgetMayRefuse, but all %d budgeted runs ANSWERED at %d KiB — delete "+
						"budgetMayRefuse from this cell, and if it also carries joinBudget move it back to "+
						"spillMxBudget; that it answers is the fix's proof\n  SQL: %s",
						answered, budget/1024, cell.sql)
				}
			}
		})
	}

	// The RATCHET on joinBudget, in the shape #824 gave budgetMayRefuse.
	//
	// joinBudget is the surviving half of the tolerance #824 retired: it is a
	// per-family budget RAISE, and a raise nothing checks is indistinguishable
	// from a raise nothing needs. So every cell that carries it is also run at
	// spillMxBudget, and if EVERY one of them answers on EVERY run there, the
	// raise has outlived its reason and the gate fails naming it.
	//
	// The ratchet is on the FAMILY, not the cell, because the raise is one
	// decision for one family — and because a per-cell ratchet cannot be
	// judged here. The shapes this guards are nondeterministic at
	// spillMxBudget by a MINORITY of runs (c_uuid answered 19 of 20 in one
	// sample of three and 20 of 20 in the others), so "this cell answered five
	// times" is a coin toss, while "all eighteen answered every time" is not:
	// it cannot happen while three of them refuse on nineteen runs in twenty.
	// ADR-0027's rule for a condition-triggered pin is to bound the condition
	// and never tolerate a split; the family is the granularity at which this
	// condition can be bounded.
	//
	// Any error counts as "did not answer", not just a budget refusal. That is
	// the correct direction for a ratchet that fires only on UNANIMOUS success:
	// an unrelated breakage keeps it quiet rather than failing it, and the
	// breakage is caught by the cell's own run above. It costs the sweep
	// len(joinBudget cells) x spillMxRuns() extra queries, short-circuited at
	// the first non-answer.
	if smaller := arms[spillMxBudget]; smaller != nil {
		joinCells, allAnswered := 0, true
		for _, cell := range spillMxCells() {
			// The FAMILY, not the flag. The crossjoin cells also carry
			// joinBudget — a cross join cannot spill at all, so its build has
			// to fit, and at spillMxBudget the scan's own 412 KiB file buffer
			// leaves it too little (#832's recorded residual, pinned by
			// join_computed_wide_refuses_under_the_smaller_budget). That is a
			// different reason from #789's, and a ratchet that fires only on
			// UNANIMOUS success would be silenced forever by mixing them.
			if cell.family() != "join" {
				continue
			}
			joinCells++
			for run := 0; run < spillMxRuns() && allAnswered; run++ {
				if _, err := tmRun(ctx, smaller, cell.sql); err != nil {
					allAnswered = false
				}
			}
			if !allAnswered {
				break
			}
		}
		t.Logf("joinBudget ratchet: %d cells probed at %d KiB", joinCells, spillMxBudget/1024)
		if joinCells > 0 && allAnswered {
			t.Fatalf("every one of the %d joinBudget cells answered on all %d runs at %d KiB — the "+
				"budget raise has outlived its reason (#789). Delete joinBudget from the "+
				"join_group_by_* cells, delete the #789 residual pin in "+
				"join_budget_determinism_test.go, and close #789; that they answer is the fix's proof",
				joinCells, spillMxRuns(), spillMxBudget/1024)
		}
	}

	// Per-family engagement. One counter for the whole file is not enough: the
	// first version of this gate asserted only JoinPartitionsEvicted, which the
	// 18 join cells satisfied on their own while 54 sort and window cells — and
	// every raw-row cell — spilled nothing at all, a whole third of the sweep
	// comparing two in-memory runs.
	//
	// The assertion is per FAMILY rather than per CELL on purpose. Whether one
	// particular cell drains is a question about that shape's state size
	// against the budget — GROUP BY g is eight groups and a few hundred bytes,
	// and no budget makes that spill — so a per-cell rule would be flaky rather
	// than strict. A family at ZERO is the real signal, and it is what fires
	// when a threshold, a plan or an operator changes out from under this file.
	// The per-cell counts are logged so the record stays honest either way.
	for _, fam := range []string{"aggregate", "sort", "window", "rawrow", "join", "crossjoin"} {
		t.Logf("engagement: %-9s %2d of %2d cells engaged, %d events",
			fam, engagedCells[fam], cellsByFamily[fam], engagedTotal[fam])
		if engagedCells[fam] == 0 {
			t.Errorf("the %s family spilled NOTHING across %d cells — every one of them compared "+
				"two in-memory runs and would pass with that spill path deleted",
				fam, cellsByFamily[fam])
		}
	}
	// The crossjoin family is asserted per CELL, not per family, and it is the
	// one family where that is right. Its counter is not a spill — a cross
	// join cannot spill, which is #832's whole point — it is the BUILD-PATH
	// DECISION that keeps its build readable, and that decision is a property
	// of the SHAPE, taken on every run, not of how the budget happened to
	// bite. So "some cell in this family engaged" is too weak: a cell that
	// stopped being planned as a cross join, or one whose build stopped
	// reaching the dispatch, is a cell that no longer covers what it was
	// added for, and it must say so rather than ride on its siblings.
	if n := cellsByFamily["crossjoin"]; n > 0 && engagedCells["crossjoin"] != n {
		t.Errorf("only %d of %d crossjoin cells reached the unrouted-probe build dispatch "+
			"(exec.JoinUnroutedProbeFlatBuilds). The others compared two runs of a shape that is "+
			"no longer the one #832 is about — find out what changed: the planner may have stopped "+
			"spelling a computed-key join as a cross join, or the build may no longer be "+
			"spill-eligible on this arm",
			engagedCells["crossjoin"], n)
	}
}

// The two budgets. spillMxBudget is where the arc's four defects live. The
// join family needs its own: at 512 KiB the scan's whole-file buffer alone
// holds ~460 KiB of it, and for the widest keys the build's demand lands on
// the budget rather than under it — so whether the query answers follows how
// far the scan decoded ahead, which is #789 and is OPEN. The raise is not a
// tolerance: at spillMxJoinBudget every cell answers on every run and a
// refusal fails, and the joinBudget ratchet at the end of
// TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget fails the whole family if
// the raise ever stops being needed.
const (
	spillMxBudget     int64 = 512 * 1024
	spillMxJoinBudget int64 = 1024 * 1024
)

func (c spillMxCell) budget() int64 {
	if c.joinBudget {
		return spillMxJoinBudget
	}
	return spillMxBudget
}

// spillMxCell is one (shape, column) pair.
type spillMxCell struct {
	name string
	sql  string
	// ordered: the query's own ORDER BY decides row order, so do not sort.
	ordered bool
	// budgetMayRefuse: a loud "memory budget exceeded" is an accepted outcome
	// for this shape at this budget. Ratchets in the direction a tolerance
	// needs it (#824): every budgeted run ANSWERING fails the cell, because a
	// tolerance that is never exercised is a tolerance nothing needs.
	budgetMayRefuse bool
	// joinBudget: run this cell at spillMxJoinBudget instead.
	joinBudget bool
	// knownBug: the issue this cell's divergence is filed as. Compared and
	// logged rather than failed; agreement on every run fails instead.
	knownBug string
	// noSpill names why this cell cannot reach a spill path even with both
	// forcing knobs armed, for the handful where that is CORRECT rather than a
	// coverage hole. Empty means the family's counter MUST move.
	noSpill string
	// forcedDrainArm adds a second pass with exec.ForceAggDrainEvery(1), so
	// the drain lands on every batch instead of where tracker timing puts it.
	forcedDrainArm bool
	// knownError pins a shape whose KNOWN state is a loud failure: every
	// budgeted run must fail with this substring, and a run that ANSWERS fails
	// the cell. The ratchet in the direction a loud bug needs it — the pin
	// cannot outlive the fix any more than knownBug's can.
	knownError string
	// forceDrainEvery arms exec.ForceAggDrainEvery(N) around THIS cell's
	// runs. A defect whose trigger is a CONDITION is pinned by bounding the
	// condition, never by tolerating an outcome mix: the first draft of this
	// cell pinned "at least one of five runs failed", which demands a
	// particular split from an uncontrolled coin — it could not pass under
	// -short (one run satisfies neither edge) and flaked 5 times in 50 at
	// five runs. ADR-0013's evidence classes and ADR-0027 decision 6 both
	// say the same thing: force the condition, then assert loudly on EVERY
	// run. Measured for #791's third route: 7/12 loud with the knob
	// disarmed, 12/12 with ForceAggDrainEvery(1), and the non-nullable twin
	// answers 12/12 either way.
	forceDrainEvery int64
}

// engagement reads the counter for this cell's operator family, and
// engagementName names it in a failure. One counter per family rather than one
// for the file: the previous version asserted only JoinPartitionsEvicted, which
// the 18 join cells satisfied on their own while 54 sort and window cells — and
// every raw-row cell — spilled nothing at all.
func (c spillMxCell) engagement() int64 {
	switch c.family() {
	case "sort":
		return exec.SortRunsWritten.Load()
	case "window":
		return exec.WindowRunsWritten.Load()
	case "rawrow":
		return exec.RawRowSpillFiles.Load()
	case "join":
		return exec.JoinPartitionsEvicted.Load()
	case "crossjoin":
		// A cross join's engagement is a DECISION, not a file: it cannot
		// spill at all (see the family's cells), so what has to be asserted
		// is that the build reached the path that keeps it readable.
		return exec.JoinUnroutedProbeFlatBuilds.Load()
	default:
		return exec.AggregatePartialDrains.Load()
	}
}

func (c spillMxCell) engagementName() string {
	switch c.family() {
	case "sort":
		return "sorted run written to disk"
	case "window":
		return "window run written to disk"
	case "rawrow":
		return "raw-row spill file written to disk"
	case "join":
		return "join partition evicted to disk"
	case "crossjoin":
		return "unrouted-probe build diverted to the flat path"
	default:
		return "aggregate partial-state drain"
	}
}

// family maps a cell to the operator whose spill path it is supposed to
// exercise. Derived from the name so a new shape cannot forget to declare one.
func (c spillMxCell) family() string {
	switch {
	case strings.HasPrefix(c.name, "order_by_"):
		return "sort"
	case strings.HasPrefix(c.name, "window_"):
		return "window"
	case strings.HasPrefix(c.name, "group_by_distinct_"):
		return "rawrow"
	case strings.HasPrefix(c.name, "join_computed_"):
		return "crossjoin"
	case strings.HasPrefix(c.name, "join_group_by_"):
		return "join"
	default:
		return "aggregate"
	}
}

// spillMxCells builds the sweep from typematrix.Columns, so a 23rd type is
// covered by adding one row there rather than by remembering to write queries.
func spillMxCells() []spillMxCell {
	tbl := typematrix.Table
	var cells []spillMxCell
	add := func(c spillMxCell) { cells = append(cells, c) }
	for _, c := range typematrix.Columns() {
		if !c.Flat {
			continue // containers: see TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget
		}
		n := c.Name
		// HashAggregate, partial-state external merge. The int / packed /
		// compact / string / generic key modes all arrive through this shape,
		// picked by the key column's type.
		add(spillMxCell{name: "group_by_" + n, forcedDrainArm: true, sql: fmt.Sprintf(
			`SELECT %[1]s AS k, COUNT(*) AS n, COUNT(%[1]s) AS nn, SUM(id) AS s FROM %[2]s GROUP BY %[1]s`, n, tbl)})
		// The LEGACY raw-row path: COUNT(DISTINCT) is a non-simple aggregate,
		// so canUseExternalMerge is false and group keys go to disk as boxed
		// values (#632's path).
		add(spillMxCell{name: "group_by_distinct_" + n, sql: fmt.Sprintf(
			`SELECT %[1]s AS k, COUNT(DISTINCT id) AS n FROM %[2]s GROUP BY %[1]s`, n, tbl)})
		// A NULL key on a two-column shape, which is what migrates the int and
		// packed key paths to the generic map mid-consume (#782's twin).
		add(spillMxCell{name: "group_by_pair_" + n, sql: fmt.Sprintf(
			`SELECT g AS gk, %[1]s AS k, COUNT(*) AS n FROM %[2]s GROUP BY g, %[1]s`, n, tbl)})
		add(spillMxCell{name: "distinct_" + n, sql: fmt.Sprintf(
			`SELECT DISTINCT %[1]s AS v FROM %[2]s`, n, tbl)})
		// Sort: external merge over sorted runs. id breaks ties, so the answer
		// is a total order and row ORDER is comparable.
		add(spillMxCell{name: "order_by_" + n, ordered: true, sql: fmt.Sprintf(
			`SELECT id, %[1]s AS v FROM %[2]s ORDER BY %[1]s, id`, n, tbl)})
		// Window: partitioned external merge, then the empty-PARTITION-BY
		// streaming evaluator. Both OVER clauses carry a TOTAL order (id is
		// unique), so neither is ADR-0013's unspecified-window class 10.
		add(spillMxCell{name: "window_partition_" + n, ordered: true, sql: fmt.Sprintf(
			`SELECT %[1]s AS k, id, ROW_NUMBER() OVER (PARTITION BY %[1]s ORDER BY id) AS r FROM %[2]s ORDER BY %[1]s, id`, n, tbl)})
		add(spillMxCell{name: "window_global_" + n, ordered: true, sql: fmt.Sprintf(
			`SELECT id, %[1]s AS v, ROW_NUMBER() OVER (ORDER BY %[1]s, id) AS r FROM %[2]s ORDER BY id`, n, tbl)})
		// HashJoin grace partitioning BELOW the aggregate: the spilled probe
		// partitions replay after the workers finish, which is #782 itself.
		//
		// The family runs at spillMxJoinBudget, and #824 took away the
		// tolerance that used to sit beside it: a refusal at 1 MiB now FAILS.
		// What it may NOT do is claim the budget raise is no longer needed.
		// Measured at spillMxBudget, 20 runs per column, three independent
		// samples on this tree: fourteen columns answer 20/20, c_cidr refuses
		// 20/20, and c_str, c_bytes and c_uuid reach BOTH dispositions on
		// identical data (c_str 0/2/1 answers of 20, c_bytes 0/1/2, c_uuid
		// 19/20/20). At 512 KiB those shapes sit exactly at their demand —
		// the file buffer plus the scan's decoded read-ahead plus the join's
		// unspillable index plus the build — so which side of the budget the
		// build's first Reserve lands on follows how far the scan ran ahead.
		// That is #789, and it is OPEN: two bounds for it were implemented in
		// this arc and both are refused on measurement (ADR-0006's open
		// residual). The budget raise is what the family needs until it is
		// closed, and the joinBudget ratchet at the end of this file's sweep is
		// what forces a fix to come back and delete it.
		add(spillMxCell{name: "join_group_by_" + n, joinBudget: true, sql: fmt.Sprintf(
			`SELECT z.%[1]s AS k, COUNT(*) AS n FROM %[2]s x JOIN %[2]s z ON x.id = z.id GROUP BY z.%[1]s`, n, tbl)})
		// A COMPUTED join key, per type (#832). An ON clause that equates two
		// EXPRESSIONS rather than two bare columns leaves the operator no
		// equi-key, so the planner spells it as a CROSS join with the ON as a
		// filter above — and a cross join's probe reads EVERY build row, which
		// a grace-partitioned build that evicts cannot supply. Every one of
		// these panicked on the spilled arm (`invalid memory address or nil
		// pointer dereference`, join.go's nextCrossChunk reading an evicted
		// nil slot) while single, DAG and DAG-shuffled answered.
		//
		// COALESCE(c, c) is the identity on every one of the 18 types,
		// NULLs included, so the answer is the plain `a.c = b.c` join's and
		// the reference arm computes it — what changes is only that the key
		// is an expression. The id bound keeps the cross product's pair count
		// at 200x200: the pressure that triggered the eviction comes from the
		// SCAN's charge, not from this build, which is why the filed shapes
		// reproduce with a five-row build.
		add(spillMxCell{name: "join_computed_key_" + n, joinBudget: true, sql: fmt.Sprintf(
			`SELECT COUNT(*) AS n, COUNT(a.%[1]s) AS nn FROM %[2]s a JOIN %[2]s b `+
				`ON COALESCE(a.%[1]s, a.%[1]s) = COALESCE(b.%[1]s, b.%[1]s) `+
				`WHERE a.id < 200 AND b.id < 200`, n, tbl)})
		// The same cross join asked to carry a VALUE, not only a count
		// (round-0 review, P4). Every cell above projects `COUNT(*)` and
		// nothing else, so the family could not see what a cross join does to
		// a value it has to materialize and order through its output. Here the
		// keyed column travels as a value on BOTH sides and MIN/MAX read it
		// back, over an ORDERED result so row order is compared too.
		//
		// A GROUP BY over this join is deliberately NOT a cell, and that is a
		// measurement rather than an omission: it is WRONG today for six of
		// the eighteen types (`2 rows, want 3` on c_bool at 1 MiB), it is
		// equally wrong at fd679ae9 with no budget at all, and the mechanism
		// is #847 — a group key that reaches `aggPreProject`'s sparse-write
		// path under a selection vector. A cell whose expected state is
		// "wrong" does not belong in a sweep whose contract is "the budgeted
		// answer equals the unbudgeted one"; it belongs in a pin, and it has
		// one: computed_group_key_over_a_cross_join_pin_test.go, which fails
		// the moment the shape starts answering and says to add the cell here.
		add(spillMxCell{name: "join_computed_value_" + n, joinBudget: true, ordered: true, sql: fmt.Sprintf(
			`SELECT COUNT(*) AS n, MIN(a.%[1]s) AS alo, MAX(a.%[1]s) AS ahi, `+
				`MIN(b.%[1]s) AS blo, MAX(b.%[1]s) AS bhi, MIN(b.id) AS ilo, MAX(b.id) AS ihi `+
				`FROM %[2]s a JOIN %[2]s b `+
				`ON COALESCE(a.%[1]s, a.%[1]s) = COALESCE(b.%[1]s, b.%[1]s) `+
				`WHERE a.id < 200 AND b.id < 200`, n, tbl)})
		// MIN/MAX as an aggregate INPUT for every ordered type, grouped by a
		// nullable key so the key-path migration runs underneath. GROUP BY g is
		// eight groups of a few hundred bytes, which no budget makes spill —
		// what these cells gate is the aggregate INPUT's encoding across a
		// drain that the OTHER cells of the family force.
		if c.Ordered {
			add(spillMxCell{name: "minmax_" + n, noSpill: spillMxTinyKey, sql: fmt.Sprintf(
				`SELECT g AS k, MIN(%[1]s) AS lo, MAX(%[1]s) AS hi, COUNT(%[1]s) AS n FROM %[2]s GROUP BY g`, n, tbl)})
		}
		// SUM/AVG for the types they are defined over. DECIMAL is the one that
		// carries an Int128 plus a scale plus an overflow bit through the run
		// format, and it is where #782's second symptom lived.
		switch n {
		case "c_i32", "c_i64", "c_f32", "c_f64", "c_dec":
			add(spillMxCell{name: "sum_avg_" + n, noSpill: spillMxTinyKey, sql: fmt.Sprintf(
				`SELECT g AS k, SUM(%[1]s) AS s, AVG(%[1]s) AS a, COUNT(%[1]s) AS n FROM %[2]s GROUP BY g`, n, tbl)})
			// A window stage above a grouped SUM, which is the shape the
			// DECIMAL loss first showed on.
			add(spillMxCell{name: "sum_window_" + n, ordered: true, noSpill: spillMxTinyKey, sql: fmt.Sprintf(
				`SELECT g AS k, SUM(%[1]s) AS s, SUM(SUM(%[1]s)) OVER () AS w FROM %[2]s GROUP BY g ORDER BY k`, n, tbl)})
		}
	}
	// The SPELLINGS #832 was filed with, plus the three the filing's shape
	// implies. Each is a different way to arrive at "the ON clause is not an
	// equality of bare columns", and they are named cells rather than per-type
	// ones because each names its own function or operator.
	for _, tc := range []struct{ name, on, where string }{
		// The three filed spellings.
		{"concat", `CONCAT('x', a.g) = CONCAT('x', b.g)`, `a.id < 5 AND b.id < 5`},
		{"pipe", `(a.c_str || 'x') = (b.c_str || 'x')`, `a.id < 5 AND b.id < 5`},
		{"upper", `UPPER(a.c_str) = UPPER(b.c_str)`, `a.id < 5 AND b.id < 5`},
		// A numeric expression key, a CAST key, and a key computed on ONE
		// side only — the last is the boundary: the shape is still keyless to
		// the operator when only one side is an expression.
		{"numeric_expr", `a.id + 1 = b.id + 1`, `a.id < 5 AND b.id < 5`},
		{"cast", `CAST(a.g AS BIGINT) = CAST(b.g AS BIGINT)`, `a.id < 5 AND b.id < 5`},
		{"one_side_only", `a.id + 1 = b.id`, `a.id < 5 AND b.id < 5`},
		// The WIDE arm: no id bound, so the build is the whole fixture rather
		// than five rows. It is what says the fix is not "the build was too
		// small to evict" — this one is 5,000 rows against the same budget.
		{"upper_wide", `UPPER(a.c_str) = UPPER(b.c_str)`, `TRUE`},
	} {
		add(spillMxCell{name: "join_computed_" + tc.name, joinBudget: true, sql: fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %[1]s a JOIN %[1]s b ON %[2]s WHERE %[3]s`, tbl, tc.on, tc.where)})
	}
	// #832's RESIDUAL, pinned as the loud failure it is. A cross join cannot
	// spill — that is the fix, and it is a property of the operator, not a
	// bound on the fix — so its build must fit the budget. At spillMxBudget
	// this one does not: the scan's whole-file buffer alone holds 412 KiB of
	// the 512 KiB, and 5,000 rows of build do not go in what is left. The
	// answer is ADR-0006's loud refusal, on every run, naming the reason.
	//
	// Four of the per-type cells above are in the same position at that budget
	// (c_i64, c_ts, c_ipv6, c_uuid — the wide keys), which is why the whole
	// family runs at spillMxJoinBudget, where every one of them answers on
	// every run. This cell is what keeps the residual visible rather than
	// budgeted away: it FAILS if the shape ever answers here, which is what a
	// blockwise nested-loop join with a spillable build would make it do.
	add(spillMxCell{name: "join_computed_wide_refuses_under_the_smaller_budget",
		knownBug:   "#832 residual: no spilling nested-loop join",
		knownError: "cannot be grace-partitioned and cannot spill",
		sql: fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %[1]s a JOIN %[1]s b ON UPPER(a.c_str) = UPPER(b.c_str)`, tbl)})
	// Scalar (ungrouped) aggregates: no GROUP BY, so no external merge — the
	// shape #779 lives on, where a shape-only column reached the row buffer.
	// One cell over every flat column at once, because the defect was in how
	// the INPUT column was read rather than in the aggregate.
	var counts []string
	for _, c := range typematrix.Columns() {
		if c.Flat {
			counts = append(counts, fmt.Sprintf("COUNT(%s) AS n_%s", c.Name, c.Name))
		}
	}
	// The GROUPED half of #779's branch, which was #791 and is now a fixed
	// shape rather than a pinned one. The pins that stood here — two
	// knownError cells asserting "shape-only BytesColumn" on every budgeted
	// run — are DELETED, and their deletion is the fix's proof: a knownError
	// cell FAILS the moment its shape answers.
	//
	// A shape-only column reaches the raw-row buffer by three routes, and all
	// three are cells below because they are three different ways to make
	// canUseExternalMerge false, not one:
	//
	//	a NON-SIMPLE aggregate beside the count   COUNT(DISTINCT), MEDIAN
	//	GROUPING SETS / ROLLUP
	//	a NULLABLE key that CONTAINS a NULL       migrateToGenericMap
	//
	// The third is DATA-dependent, which is why the plan-time fix the filing
	// preferred cannot be written (ADR-0027's 2026-09-03 amendment) and why
	// the buffer was taught instead: the row boundary hands back
	// batch.ShapeOnlyLen — the length and a refusal of the value — and writes
	// it back as a shape-only column with the same lengths.
	//
	// Every one of these cells asserts its RAW-ROW ENGAGEMENT through the
	// family counter: they are the "rawrow" family, and a cell that answers
	// without writing a spill file would be comparing two in-memory runs.
	// Measured at 512 KiB, five runs each, with the raw-row files each shape
	// actually wrote: str_distinct 23, bytes_distinct 22, str_median 25,
	// rollup 6, nullable-key 140.
	add(spillMxCell{name: "group_by_distinct_shape_only_count", sql: fmt.Sprintf(
		`SELECT g AS k, COUNT(c_str) AS n, COUNT(DISTINCT id) AS d FROM %s GROUP BY g`, tbl)})
	// The BYTES twin. #632's defect was on this same buffer and BYTES was its
	// arm, so the pair says the two encodings stay apart.
	add(spillMxCell{name: "group_by_distinct_shape_only_count_bytes", sql: fmt.Sprintf(
		`SELECT g AS k, COUNT(c_bytes) AS n, COUNT(DISTINCT id) AS d FROM %s GROUP BY g`, tbl)})
	// A different non-simple aggregate, so the route is not COUNT(DISTINCT)'s
	// alone.
	add(spillMxCell{name: "group_by_distinct_shape_only_count_median", sql: fmt.Sprintf(
		`SELECT g AS k, COUNT(c_str) AS n, MEDIAN(c_i64) AS m FROM %s GROUP BY g`, tbl)})
	// The GROUPING SETS route, which needs no second aggregate at all.
	add(spillMxCell{name: "group_by_distinct_shape_only_count_rollup", sql: fmt.Sprintf(
		`SELECT g AS k, COUNT(c_str) AS n FROM %s GROUP BY ROLLUP(g)`, tbl)})
	// A shape use that reads the LENGTH rather than only the null mask: it is
	// what says the box carries the length rather than merely refusing the
	// value. AVG(LENGTH(col)) is the ClickBench Q28 shape the whole shape-only
	// optimization was built for.
	add(spillMxCell{name: "group_by_distinct_shape_only_length", sql: fmt.Sprintf(
		`SELECT g AS k, AVG(LENGTH(c_str)) AS a, COUNT(DISTINCT id) AS d FROM %s GROUP BY g`, tbl)})
	// The THIRD route, and its twin. These two differ ONLY in the key's
	// nullability — the nullable one migrates to the generic map and takes the
	// raw-row buffer, the non-nullable one never does — so a fix that closed
	// one and not the other would be visible here. Both arm
	// ForceAggDrainEvery(1) so the route is taken on every run rather than on
	// a coin flip: before the fix the nullable half failed 7 of 12 runs
	// disarmed and 12 of 12 armed, while the twin answered 12 of 12 either
	// way. The names keep the group_by_distinct_ prefix so the nullable one is
	// counted in the rawrow family it engages.
	add(spillMxCell{name: "group_by_distinct_shape_only_nullable_key",
		forceDrainEvery: 1,
		sql: fmt.Sprintf(
			`SELECT g AS k, COUNT(c_str) AS n FROM %s GROUP BY g`, tbl)})
	add(spillMxCell{name: "grouped_nonnull_key_shape_only_count",
		forceDrainEvery: 1,
		noSpill:         "a non-nullable key never migrates, so this shape never reaches the raw-row buffer",
		sql: fmt.Sprintf(
			`SELECT id AS k, COUNT(c_str) AS n FROM %s GROUP BY id`, tbl)})
	add(spillMxCell{name: "scalar_counts",
		// M3's whole point: an ungrouped aggregate no longer buffers or drains
		// anything, so there is nothing here for a spill counter to see. Its
		// evidence is that it FAILED LOUDLY on e96640c6 with the shape-only
		// guard, and passes here.
		noSpill: "ungrouped: canUseExternalMerge is false and M3 removed the row buffer",
		sql: fmt.Sprintf(
			`SELECT COUNT(*) AS n, %s FROM %s`, strings.Join(counts, ", "), tbl)})
	return cells
}

// spillMxTinyKey is the reason the GROUP BY g shapes cannot reach a spill on
// this fixture: eight groups holding a few hundred bytes, which no budget makes
// the aggregate drain. What those cells gate is the aggregate INPUT's encoding
// (DECIMAL's Int128 + scale + overflow bit, FLOAT's IsFloat) across the drains
// the OTHER cells of the family force — and the answer, which is what caught
// M2 on e96640c6.
const spillMxTinyKey = "GROUP BY g is eight groups: too little state for any budget to drain"

// spillMxRuns is the replication count per spilled cell. One passing spilled
// run proves nothing: which batch drains follows tracker timing, and the
// defects this sweep exists for are wrong on a MINORITY of runs.
// spillMxRatchetMinRuns is the smallest sample on which "every run agreed" is
// evidence that a pinned defect is FIXED rather than evidence that it did not
// fire this time. The pinned defects are nondeterministic, so this is the full
// replication count: below it the ratchet logs instead of failing.
const spillMxRatchetMinRuns = 5

func spillMxRuns() int {
	if testing.Short() {
		return 1
	}
	if v := os.Getenv("WADJET_SPILL_SWEEP_RUNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 5
}

// spillMxDiff returns the first difference between two rendered results, or ""
// when they agree.
func spillMxDiff(got, want []string) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Sprintf("row %d differs\n  budgeted:   %s\n  unbudgeted: %s", i, got[i], want[i])
		}
	}
	return ""
}

// spillMxRender renders a result to comparable text, one string per row, with
// every column named — including the NULL ones, which is where a group that
// lost its rows hides. The Go TYPE rides along because a value and the display
// text a lossy encoder turns it into print identically (#632). An unordered
// query's rows are sorted; an ORDER BY query's are not, so a spill that
// reorders a total order fails.
//
// FLOATs are printed to 6 significant digits: a float sum's last digits move
// with accumulation order, which changes under a budget (ADR-0013 class 9).
// Nothing else is rounded — a DECIMAL is compared digit for digit.
func spillMxRender(columns []string, rows []map[string]any, ordered bool) []string {
	out := make([]string, 0, len(rows))
	var sb strings.Builder
	for _, r := range rows {
		sb.Reset()
		for _, c := range columns {
			v, ok := r[c]
			switch t := v.(type) {
			case nil:
				if !ok {
					sb.WriteString(c + "=<missing>|")
				} else {
					sb.WriteString(c + "=NULL|")
				}
			case float64:
				fmt.Fprintf(&sb, "%s=float:%.6g|", c, t)
			case float32:
				fmt.Fprintf(&sb, "%s=float:%.6g|", c, float64(t))
			default:
				fmt.Fprintf(&sb, "%s=%T:%v|", c, v, v)
			}
		}
		out = append(out, sb.String())
	}
	if !ordered {
		sort.Strings(out)
	}
	return out
}

// spillMxOpen loads the flat type matrix into an embedded DB with the given
// per-query memory budget (0 = unbounded).
func spillMxOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open (budget %d): %v", budget, err)
	}
	t.Cleanup(func() { db.Close() })
	schema := typematrix.Schema()
	if err := db.CreateTable(ctx, typematrix.Table, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Table, err)
	}
	ing := db.NewIngester(typematrix.Table, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, typematrix.Data(typematrix.Rows)); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Table, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Table, err)
	}
	return db
}
