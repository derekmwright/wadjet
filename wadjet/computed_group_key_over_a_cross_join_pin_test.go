package wadjet

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// #847, PINNED WRONG. A GROUP BY over a join whose ON is an EXPRESSION — so
// the planner spells it as a CROSS join — attributes rows to the wrong group.
//
// It is PRE-EXISTING: it reproduces at fd679ae9 with no memory budget and no
// spill anywhere, and it is pinned here rather than fixed because the site is
// outside this arc's territory. What makes it this arc's business is that
// #832's fix CHANGED ITS REACHABILITY — under a budget the shape used to panic
// on a nil build batch, and now it answers. A panic that becomes a plausible
// wrong number must not land unpinned.
//
// # WHAT THE PIN COVERS, AND WHY IT IS THIS WIDE
//
// The filing names a BARE COLUMN key (`GROUP BY a.g`); round 1 pinned only an
// arithmetic one (`GROUP BY a.id % 7`), which would have let a fix delete the
// pin and leave the filed shape unguarded. Both are entries below now, and so
// is EVERY flat type as a bare-column key.
//
// Eighteen types rather than the six the spill sweep noticed, because the
// sweep cannot see this class: it compares a BUDGETED run against an
// UNBUDGETED one, and for twelve of the eighteen both arms are wrong the SAME
// way, so the comparison agrees. The six it fails are only the ones whose
// wrongness DIVERGES between arms. Measured against the truth instead, all
// twenty entries below are wrong on every run.
//
// # THE TRUTH IS THE ENGINE'S OWN
//
// Each entry derives its expected counts from the join's own projected pair
// list — `SELECT <key> FROM <the join>` — rather than hard-coding numbers, so
// the pin cannot rot into asserting a stale answer and it fails loudly if the
// fixture moves. That derivation is not an assumption: with ONE scan worker
// the engine's grouped answer equals it for all twenty entries, 20 runs of 20
// (TestComputedGroupKeyOverACrossJoinIsRightWithOneScanWorker below). The pair
// list is also deterministic and equal to the equi-join's.
//
// # WHAT IS RULED OUT, so the next arc does not repeat the search
//
//   - The JOIN is right — its pairs are deterministic and equal the
//     equi-join's.
//   - The PROJECTION is right — `SELECT a.id AS i, a.id % 7 AS m` over the same
//     join has zero rows where `m != i % 7`.
//   - The AGGREGATE and its partition ROUTER are right. Instrumented at the
//     sink, each of the seven partition sinks receives exactly ONE distinct key
//     — disjoint, as partitioned aggregation requires — and counts precisely
//     the rows it is handed. The counts are already wrong on arrival:
//     sink(key 0) is handed 58 rows where 28 belong to it.
//
// # WHERE IT IS
//
// Two kill switches mask it and neither is the hash: `WADJET_PARTITIONED_AGG=0`
// makes every entry right, `WADJET_SCAN_WORKERS=1` makes every entry right, and
// `WADJET_HASH_ONCE=0` changes nothing. So it is the multi-producer path into
// the partitioned aggregate.
//
// The group key is computed by `aggPreProject.Execute`
// (`internal/planner/physical/plan.go:11347`), which for a numeric computed
// column takes the `canPassSelThrough` path: it KEEPS the input's selection
// vector and writes at SPARSE physical indices, into vectors it REUSES across
// Execute calls when `shareOutputs` is false (`plan.go:11410`). The partitioned
// aggregate then hands VIEWS over those same vectors to its per-partition
// sinks. Measured on one failing batch: the key vector holds exactly the 138
// non-zero values its 161 active rows should produce, but only 108 sit at
// positions the batch's selection vector names — 30 active rows read a slot
// this batch never wrote and key as 0.
//
// So the fix is a decision about sparse writes into reused buffers under a
// selection vector, shared with concurrent readers, in a hot operator whose own
// comments record a measured perf trade for exactly that choice (0.65s vs
// 0.07s at SF1). That is `internal/planner/physical`, which this arc does not
// own, and it wants its own arc with its own A/B.
//
// # THE PIN RATCHETS ON UNANIMOUS CORRECTNESS
//
// It asserts that the shape is NOT RELIABLY RIGHT, and nothing stronger. An
// entry fails only when ALL twenty runs answer correctly; anything less passes
// with the observed count logged.
//
// The first draft asserted the opposite — wrong on all twenty — and that was a
// TOLERANCE wearing a pin's clothes, of exactly the kind ADR-0027 decision 6
// and ADR-0013 forbid: it demanded a particular outcome from an uncontrolled
// coin. Two knobs make the defect fire on almost every run (eight producers, one
// P), but "almost" is the operative word under load: seven full runs of this
// file on an unchanged tree failed five times, each time with a DIFFERENT single
// entry reporting "1 of 20 runs answered correctly" — c_cidr, then c_ipv6 and
// c_dur, then c_ipv4, then a_g, then c_ts. The interleaving that produces the
// wrong answer is scheduling-dependent, so one lucky run is not news, and a red
// pin is worse than no pin: it tells the next engineer to delete this file and
// close #847 on a defect that is still there.
//
// UNANIMITY is the stable signal, and it is the same ratchet
// `spillMxRatchetMinRuns` applies in the spill sweep for the same reason:
// agreement is a fix's proof at full replication and a coin toss at one. The
// forcing knobs stay — they keep the pin from being vacuous by making the
// defect fire on 19 or 20 runs out of 20 rather than 13 — but the ASSERTION no
// longer rests on them.
//
// GOMAXPROCS is process-global, so it is restored on return; this package
// declares no t.Parallel test, so it cannot leak into a sibling running at the
// same time, because none is.
func TestAComputedGroupKeyOverACrossJoinIsPinnedWrong(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	t.Setenv("WADJET_SCAN_WORKERS", "8")
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	ctx := context.Background()
	db := spillMxOpen(t, 0)
	for _, c := range p847Cells() {
		t.Run(c.name, func(t *testing.T) {
			want := p847Truth(t, ctx, db, c)
			right := 0
			var lastDiff string
			for run := 0; run < p847Runs; run++ {
				got := p847Grouped(t, ctx, db, c)
				if d := p847Diff(got, want); d == "" {
					right++
				} else {
					lastDiff = d
				}
			}
			// THE RATCHET. Only unanimity is evidence: a defect whose
			// trigger is a scheduling interleaving answers correctly now and
			// then, and failing on that would make this pin red on a tree
			// where nothing changed.
			if right == p847Runs {
				t.Fatalf("the pin AGREES — all %d runs answered correctly, so #847 is fixed for "+
					"this entry. Delete it from p847Cells (and this whole file once every entry "+
					"agrees), close #847, and add the shape to the sweep's crossjoin family as a "+
					"value-and-GROUP-BY cell; that it answers is the fix's proof.\n  SQL: %s",
					p847Runs, p847GroupSQL(c))
			}
			t.Logf("still wrong: %d of %d runs answered correctly (last divergence: %s)",
				right, p847Runs, lastDiff)
		})
	}
}

// TestComputedGroupKeyOverACrossJoinIsRightWithOneScanWorker is the positive
// control, and it is what makes the pin above evidence rather than an
// assertion about a number nobody checked: with a SINGLE scan worker the
// engine's own grouped answer equals the truth derived from its own pair list,
// for every entry the pin claims is wrong. So the derivation is the engine's,
// the fixture is sound, and what the pin measures is the multi-producer path
// and nothing else.
//
// It is also half of the boundary claim: a fix must make the pinned entries
// right WITHOUT making these wrong. A control tolerates nothing — it asserts
// correctness on every one of p847Runs runs, and if it ever needed a ratchet
// the pin would have stopped measuring the multi-producer path.
func TestComputedGroupKeyOverACrossJoinIsRightWithOneScanWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	t.Setenv("WADJET_SCAN_WORKERS", "1")
	p847ControlRun(t, "With ONE scan worker")
}

// TestComputedGroupKeyOverACrossJoinIsRightWithoutPartitionedAggregation is the
// second control, and the pair of them is what gives the ratchet above its
// meaning: they are what "fixed" looks like. Both kill switches make every
// pinned entry right on EVERY run — no ratchet, no tolerance — so a fix is
// expected to reach the same place at the default configuration, and the
// ratchet fires when it does.
//
// The switch is flipped through optswitch rather than through the environment,
// and that is not a shortcut: `optswitch.Register` reads its variable ONCE, at
// package init, so a `t.Setenv("WADJET_PARTITIONED_AGG", "0")` here sets a
// string nothing will read again and the control would silently assert the
// DEFAULT configuration — passing only because the pinned shape happened to be
// wrong, which is the opposite of what it claims. The toggle is looked up by
// its registered name and restored on return.
func TestComputedGroupKeyOverACrossJoinIsRightWithoutPartitionedAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	tog := p847Toggle(t, "partitioned-agg")
	defer tog.Set(tog.Set(false))
	p847ControlRun(t, "With partitioned aggregation off")
}

// p847Toggle finds a registered optimization switch by name, failing rather
// than skipping if it is gone: a control that quietly stops controlling
// anything is worse than one that is red.
func p847Toggle(t *testing.T, name string) *optswitch.Toggle {
	t.Helper()
	for _, tog := range optswitch.All() {
		if tog.Name == name {
			return tog
		}
	}
	t.Fatalf("optimization switch %q is not registered — this control cannot set the "+
		"condition it claims to, so it would assert the default configuration", name)
	return nil
}

// p847ControlRun asserts every pinned entry answers its own pair list's counts
// on all p847Runs runs, under whatever condition the caller has established. A
// control tolerates nothing: if it ever needs a ratchet, the pin above has
// stopped measuring the multi-producer path and is measuring something else.
func p847ControlRun(t *testing.T, why string) {
	t.Helper()
	ctx := context.Background()
	db := spillMxOpen(t, 0)
	for _, c := range p847Cells() {
		t.Run(c.name, func(t *testing.T) {
			want := p847Truth(t, ctx, db, c)
			for run := 0; run < p847Runs; run++ {
				if d := p847Diff(p847Grouped(t, ctx, db, c), want); d != "" {
					t.Fatalf("run %d of %d: %s\n  %s this shape must answer what its own pair "+
						"list says.\n  SQL: %s", run, p847Runs, d, why, p847GroupSQL(c))
				}
			}
		})
	}
}

// p847Runs is the replication count for both the pin and its controls. The pin
// needs it high because its ratchet fires on unanimity and a short sample would
// make "all of them agreed" cheap; the controls need it high because they
// assert the opposite unanimity and one passing run would prove nothing.
const p847Runs = 20

// The other route that must keep answering: the expression materialized by an
// inner projection, so the outer aggregate groups by a plain column and the
// sparse write never happens. Unlike the entries above this one is right at
// the DEFAULT worker count, which is what makes it a useful control — a fix
// that broke it would have traded one wrong answer for another.
func TestAMaterializedComputedKeyOverACrossJoinKeepsAnswering(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	ctx := context.Background()
	db := spillMxOpen(t, 0)
	const from = `FROM typemx a JOIN typemx b ON UPPER(a.c_str) = UPPER(b.c_str) ` +
		`WHERE a.id < 200 AND b.id < 200`
	pairs, err := tmRun(ctx, db, `SELECT a.id % 7 AS k `+from)
	if err != nil {
		t.Fatalf("pair list: %v", err)
	}
	want := map[string]int64{}
	for _, r := range pairs.Rows {
		want[fmt.Sprintf("%v", r["k"])]++
	}
	sql := `SELECT k, COUNT(*) AS n FROM (SELECT a.id % 7 AS k ` + from + `) t GROUP BY k`
	for run := 0; run < 5; run++ {
		got, err := tmRun(ctx, db, sql)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		gm := map[string]int64{}
		for _, r := range got.Rows {
			n, _ := r["n"].(int64)
			gm[fmt.Sprintf("%v", r["k"])] += n
		}
		if d := p847Diff(gm, want); d != "" {
			t.Fatalf("run %d: %s\n  SQL: %s", run, d, sql)
		}
	}
}

// p847Cell is one (key spelling, join predicate) pair.
type p847Cell struct{ name, keyExpr, on string }

// p847Cells is the pinned corpus: the arithmetic spelling round 1 pinned, the
// BARE COLUMN spelling #847 was actually filed for, and every flat type as a
// bare-column key over the identity-COALESCE join the sweep's crossjoin family
// uses. Built from typematrix.Columns() so a 23rd type joins the pin by being
// added there.
func p847Cells() []p847Cell {
	const upper = `UPPER(a.c_str) = UPPER(b.c_str)`
	cells := []p847Cell{
		{"arithmetic_key_id_mod_7", `a.id % 7`, upper},
		// The spelling the issue was FILED with.
		{"bare_column_key_a_g", `a.g`, upper},
	}
	for _, c := range typematrix.Columns() {
		if !c.Flat {
			continue
		}
		cells = append(cells, p847Cell{
			name:    "bare_column_key_" + c.Name,
			keyExpr: "a." + c.Name,
			on: fmt.Sprintf(`COALESCE(a.%[1]s, a.%[1]s) = COALESCE(b.%[1]s, b.%[1]s)`,
				c.Name),
		})
	}
	return cells
}

func p847From(c p847Cell) string {
	return fmt.Sprintf(`FROM %[1]s a JOIN %[1]s b ON %[2]s WHERE a.id < 200 AND b.id < 200`,
		typematrix.Table, c.on)
}

func p847GroupSQL(c p847Cell) string {
	return fmt.Sprintf(`SELECT %[1]s AS k, COUNT(*) AS n %[2]s GROUP BY %[1]s`, c.keyExpr, p847From(c))
}

// p847Truth counts the key's multiplicity over the join's own projected pair
// list — the engine's rows, grouped in Go rather than by the operator under
// test.
func p847Truth(t *testing.T, ctx context.Context, db *DB, c p847Cell) map[string]int64 {
	t.Helper()
	pairs, err := tmRun(ctx, db, fmt.Sprintf(`SELECT %s AS k %s`, c.keyExpr, p847From(c)))
	if err != nil {
		t.Fatalf("pair list: %v", err)
	}
	if len(pairs.Rows) == 0 {
		t.Fatal("the pair list is empty — this cell would compare nothing")
	}
	out := map[string]int64{}
	for _, r := range pairs.Rows {
		out[fmt.Sprintf("%v", r["k"])]++
	}
	return out
}

func p847Grouped(t *testing.T, ctx context.Context, db *DB, c p847Cell) map[string]int64 {
	t.Helper()
	got, err := tmRun(ctx, db, p847GroupSQL(c))
	if err != nil {
		t.Fatalf("grouped: %v", err)
	}
	out := map[string]int64{}
	for _, r := range got.Rows {
		n, _ := r["n"].(int64)
		out[fmt.Sprintf("%v", r["k"])] += n
	}
	return out
}

// p847Diff returns the first difference between two key->count maps, or "".
func p847Diff(got, want map[string]int64) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d groups, want %d", len(got), len(want))
	}
	for k, w := range want {
		if g, ok := got[k]; !ok {
			return fmt.Sprintf("group %q missing, want %d", k, w)
		} else if g != w {
			return fmt.Sprintf("group %q counted %d, want %d", k, g, w)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			return fmt.Sprintf("group %q invented", k)
		}
	}
	return ""
}
