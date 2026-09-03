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
//     outcome, because at 512 KiB the scan's own file load already holds
//     ~460 KiB and the join build legitimately has nowhere to go. A loud
//     refusal is the right answer to a budget that small (ADR-0006); a WRONG
//     answer is not, so the comparison still runs on the runs that answered,
//     and a cell where NO run answered fails as vacuous. That the refusal is
//     nondeterministic at a fixed budget is filed as #789.
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
			answered, agreed := 0, 0
			for run := 0; run < spillMxRuns(); run++ {
				got, err := tmRun(ctx, arm, cell.sql)
				if err != nil {
					if cell.budgetMayRefuse && strings.Contains(err.Error(), "memory budget exceeded") {
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
		})
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
	for _, fam := range []string{"aggregate", "sort", "window", "rawrow", "join"} {
		t.Logf("engagement: %-9s %2d of %2d cells spilled, %d events",
			fam, engagedCells[fam], cellsByFamily[fam], engagedTotal[fam])
		if engagedCells[fam] == 0 {
			t.Errorf("the %s family spilled NOTHING across %d cells — every one of them compared "+
				"two in-memory runs and would pass with that spill path deleted",
				fam, cellsByFamily[fam])
		}
	}
}

// The two budgets. spillMxBudget is where the arc's four defects live. The
// join shape needs its own: at 512 KiB the scan's file load alone holds
// ~460 KiB, so the build has nowhere to go and refuses outright for the wide
// byte-array columns — the query never reaches the replay the cell is for.
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
	// for this shape at this budget.
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
		add(spillMxCell{name: "join_group_by_" + n, budgetMayRefuse: true, joinBudget: true, sql: fmt.Sprintf(
			`SELECT z.%[1]s AS k, COUNT(*) AS n FROM %[2]s x JOIN %[2]s z ON x.id = z.id GROUP BY z.%[1]s`, n, tbl)})
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
	// The GROUPED half of #779's branch, pinned as #791: a shape-only column
	// counted beside a NON-simple aggregate keeps the raw-row buffer, and the
	// buffer still reads the column's values. Loud, on every run, on this
	// commit and on e96640c6. The pin's ratchet runs in the direction a loud
	// bug needs: the cell FAILS if the shape starts answering.
	add(spillMxCell{name: "grouped_shape_only_count", knownBug: "#791",
		knownError: "shape-only BytesColumn",
		noSpill:    "the shape fails before any spill file is written",
		sql: fmt.Sprintf(
			`SELECT g AS k, COUNT(c_str) AS n, COUNT(DISTINCT id) AS d FROM %s GROUP BY g`, tbl)})
	// #791's THIRD ROUTE, found in this arc's round 0 and pinned here so the
	// residual is recorded rather than remembered.
	//
	// The filing describes two ways onto the raw-row path beside a shape-only
	// column: a non-simple aggregate (the cell above) and GROUPING SETS. There
	// is a third, and it is DATA-dependent: a nullable GROUP BY key that
	// actually carries a NULL migrates the int-keyed path to the generic
	// string map (migrateToGenericMap clears useIntGroupKey and sets no
	// replacement flag), so canUseExternalMerge is false from that batch on and
	// the aggregate takes the raw-row buffer with ONE SIMPLE aggregate and
	// nothing else. Measured at 512 KiB, five runs each:
	//
	//	GROUP BY g      (nullable, has NULLs)   5/5 fail
	//	GROUP BY id     (non-nullable)          0/5 fail
	//
	// The pair is the fixture: the two shapes differ ONLY in the key's
	// nullability, so a fix that closes one and not the other is visible.
	//
	// Both halves arm ForceAggDrainEvery(1) so the pressure check lands on
	// every batch and the route is taken every run: 7/12 loud with the knob
	// disarmed against 12/12 with it, while the non-nullable twin answers
	// 12/12 either way. Pinning the un-forced 7-in-12 as "some runs fail"
	// would be a tolerance, which is what ADR-0013 forbids.
	//
	// This is why the plan-time fix the filing prefers is REFUSED (ADR-0027
	// amendment): simpleAggs and the key-mode flags are latched from the first
	// batch's vector types inside resolveIndices, not from the logical plan,
	// and the third route depends on whether a nullable key CONTAINS a NULL.
	// No plan-time fact answers that, so a plan-time decline can only be
	// conservative to any GROUP BY on a nullable key beside a shape-only
	// column — which disables the shape-only optimization for most
	// `GROUP BY … COUNT(col)` shapes, including the ClickBench family it was
	// built for.
	add(spillMxCell{name: "grouped_nullable_key_shape_only_count", knownBug: "#791",
		knownError:      "shape-only BytesColumn",
		forceDrainEvery: 1,
		noSpill:         "the shape fails before any spill file is written",
		sql: fmt.Sprintf(
			`SELECT g AS k, COUNT(c_str) AS n FROM %s GROUP BY g`, tbl)})
	// The twin that must keep ANSWERING: the same shape on a NON-nullable
	// key, which never migrates and so never reaches the raw-row path. If a
	// #791 fix ever makes this one fail, it has widened the defect rather
	// than closed it.
	add(spillMxCell{name: "grouped_nonnull_key_shape_only_count",
		forceDrainEvery: 1,
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
