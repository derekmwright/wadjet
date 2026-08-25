package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

const q21SQL = `SELECT s_name, COUNT(*) as numwait
	FROM supplier
	JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
	JOIN orders ON o_orderkey = l1.l_orderkey
	JOIN nation ON s_nationkey = n_nationkey
	WHERE o_orderstatus = 'F'
		AND l1.l_receiptdate > l1.l_commitdate
		AND n_name = 'SAUDI ARABIA'
		AND EXISTS (
			SELECT 1 FROM lineitem l2
			WHERE l2.l_orderkey = l1.l_orderkey
				AND l2.l_suppkey != l1.l_suppkey
		)
		AND NOT EXISTS (
			SELECT 1 FROM lineitem l3
			WHERE l3.l_orderkey = l1.l_orderkey
				AND l3.l_suppkey != l1.l_suppkey
				AND l3.l_receiptdate > l3.l_commitdate
		)
	GROUP BY s_name
	ORDER BY numwait DESC, s_name
	LIMIT 100`

const q18SQL = `SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
	FROM customer
	JOIN orders ON c_custkey = o_custkey
	JOIN lineitem ON o_orderkey = l_orderkey
	WHERE o_orderkey IN (
		SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
	)
	GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
	ORDER BY o_totalprice DESC, o_orderdate
	LIMIT 100`

// sqlToStagesShuffled plans SQL with broadcast disabled (pins the
// hash-shuffle regime so semi/anti builds ride exchanges) and the LEGACY
// dynamic-filter pass off, isolating markSemiAntiBuildFilters.
func sqlToStagesShuffled(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) []Stage {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = 3
	planner.BroadcastBytesThreshold = -1
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}

func findSemiAntiMarks(stages []Stage) (emitters, consumers []*Stage) {
	for i := range stages {
		for _, e := range stages[i].EmitDynamicFilters {
			// Scope to THIS pass's filters — the dimension cascade also
			// plants AtOutput emits on Q21-class plans.
			if e.AtOutput && strings.HasPrefix(e.FilterID, "sabf-") {
				emitters = append(emitters, &stages[i])
				break
			}
		}
		for _, c := range stages[i].ConsumeDynamicFilters {
			if strings.HasPrefix(c.FilterID, "sabf-") {
				consumers = append(consumers, &stages[i])
				break
			}
		}
	}
	return emitters, consumers
}

// Q21's raw lineitem exchange feeds an EXISTS semi build and (via the
// subsume flag) a NOT-EXISTS anti build, probed by the filtered join
// chain. The pass must emit at the semi join's probe dependency and
// consume at the raw lineitem scan, with the stat-dep edge installed.
func TestSemiAntiBuildFilter_Q21Marks(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	before := SemiAntiBuildFiltersPlanned.Load()
	stages := sqlToStagesShuffled(t, cat, ctx, q21SQL)

	emitters, consumers := findSemiAntiMarks(stages)
	if len(emitters) == 0 {
		t.Fatalf("no AtOutput emit was planned; stages:\n%s", dumpStageIDs(stages))
	}
	if len(consumers) != 1 {
		t.Fatalf("want exactly 1 consuming scan, got %d; stages:\n%s", len(consumers), dumpStageIDs(stages))
	}
	bscan := consumers[0]
	if bscan.Type != StageScan || bscan.TableName != "lineitem" {
		t.Fatalf("consume attached to %s/%s, want lineitem scan", bscan.Type, bscan.TableName)
	}
	if len(bscan.FilterExprs) != 0 {
		t.Fatalf("consume must attach to the RAW lineitem scan, got filters %v", bscan.FilterExprs)
	}
	con := bscan.ConsumeDynamicFilters[0]
	if base := baseColName(con.TargetColumn); base != "l_orderkey" {
		t.Fatalf("TargetColumn = %q, want l_orderkey", con.TargetColumn)
	}
	// Stat-dep edge: the build scan must wait for the emit source.
	if !containsString(bscan.Dependencies, con.SourceStageID) {
		t.Fatalf("build scan deps %v missing emit source %s", bscan.Dependencies, con.SourceStageID)
	}
	// The emit source stage carries the matching AtOutput emit.
	var matched bool
	for _, em := range emitters {
		if em.ID == con.SourceStageID {
			for _, e := range em.EmitDynamicFilters {
				if e.FilterID == con.FilterID && e.AtOutput && baseColName(e.KeyColumn) == "l_orderkey" {
					matched = true
				}
			}
		}
	}
	if !matched {
		t.Fatalf("no matching AtOutput emit for consume %+v", con)
	}
	if SemiAntiBuildFiltersPlanned.Load() == before {
		t.Fatal("SemiAntiBuildFiltersPlanned did not increment")
	}
}

func TestSemiAntiBuildFilter_KillSwitch(t *testing.T) {
	prev := SemiAntiBuildFilter.Set(false)
	defer SemiAntiBuildFilter.Set(prev)
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStagesShuffled(t, cat, ctx, q21SQL)
	emitters, consumers := findSemiAntiMarks(stages)
	if len(emitters) != 0 || len(consumers) != 0 {
		t.Fatalf("kill switch must leave stages unmarked (emitters=%d consumers=%d)", len(emitters), len(consumers))
	}
}

// Q18's raw lineitem exchange is consumed by a grouped aggregate and an
// inner join — not semi/anti. The pass must not touch it.
func TestSemiAntiBuildFilter_Q18Unmarked(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStagesShuffled(t, cat, ctx, q18SQL)
	emitters, consumers := findSemiAntiMarks(stages)
	if len(emitters) != 0 || len(consumers) != 0 {
		t.Fatalf("Q18 shape must stay unmarked (emitters=%d consumers=%d)", len(emitters), len(consumers))
	}
}

// semiAntiFixture is a hand-built plan that the pass MARKS: a raw lineitem
// scan behind a repartition exchange, feeding a semi and an anti build, both
// probed from a filtered orders scan. Two builds satisfy the cost gate, the
// probe source is a different table, and every consumer of the exchange is a
// semi/anti join from that source.
//
// It marks, and that is the point. Every negative below is this fixture with
// exactly one thing changed, so "did not mark" means the changed thing is what
// stopped it. The negatives used to run against NewPlanner(nil): with no
// catalog, columnIntType returns false and the pass bailed at the type gate
// before reaching a single safety walk, so each of them would have passed with
// the walk it names deleted outright.
func semiAntiFixture(key string) []Stage {
	return []Stage{
		{ID: "scan-raw", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"}, Columns: []string{key, "l_suppkey"}},
		{ID: "rp", Type: StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{key}, Count: 4},
			Dependencies: []string{"scan-raw"}},
		{ID: "probe-src", Type: StageScan, TableName: "orders",
			ScanFiles: []string{"f2"}, Columns: []string{"o_orderkey"},
			FilterExprs: []string{"o_orderstatus = 'F'"}},
		{ID: "semi", Type: StageHashJoin, JoinType: "semi",
			JoinLeftKeys: []string{"o_orderkey"}, JoinRightKeys: []string{key},
			LeftDepStage: "probe-src", RightDepStage: "rp",
			Dependencies: []string{"probe-src", "rp"}},
		{ID: "anti", Type: StageHashJoin, JoinType: "anti",
			JoinLeftKeys: []string{"o_orderkey"}, JoinRightKeys: []string{key},
			LeftDepStage: "probe-src", RightDepStage: "rp",
			Dependencies: []string{"probe-src", "rp"}},
	}
}

// TestSemiAntiBuildFilter_FixturePositive is the control the negatives below
// are differentials against. Without it, a negative proves nothing: a fixture
// that cannot mark for an unrelated reason reports every walk as working.
func TestSemiAntiBuildFilter_FixturePositive(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := semiAntiFixture("l_orderkey")
	NewPlanner(cat).markSemiAntiBuildFilters(ctx, stages)
	emitters, consumers := findSemiAntiMarks(stages)
	if len(emitters) == 0 || len(consumers) == 0 {
		t.Fatalf("control fixture did not mark (emitters=%d consumers=%d) — every negative below is vacuous",
			len(emitters), len(consumers))
	}
}

// Fixture-level negative coverage for the safety walks. A raw scan of the
// same table as the build provides no reduction evidence; a consumer of
// the shared exchange that is NOT a semi/anti join blocks marking; and a
// non-integer join key is refused because the whole path downstream —
// DynamicFilterEmitOp's hash, the bloom's UseIntKey probe — indexes
// Int32Data/Int64Data and a STRING column has neither.
func TestSemiAntiBuildFilter_FixtureNegatives(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	t.Run("same-table raw probe source", func(t *testing.T) {
		stages := semiAntiFixture("l_orderkey")
		stages[2].TableName = "lineitem"
		stages[2].Columns = []string{"l_orderkey"}
		stages[2].FilterExprs = nil
		stages[3].JoinLeftKeys = []string{"l_orderkey"}
		stages[4].JoinLeftKeys = []string{"l_orderkey"}
		NewPlanner(cat).markSemiAntiBuildFilters(ctx, stages)
		if _, consumers := findSemiAntiMarks(stages); len(consumers) != 0 {
			t.Fatal("same-table raw probe source must not mark")
		}
	})
	t.Run("non-semi sibling consumer of exchange", func(t *testing.T) {
		stages := semiAntiFixture("l_orderkey")
		stages = append(stages, Stage{
			ID: "other", Type: "final_aggregate",
			Dependencies: []string{"rp"},
		})
		NewPlanner(cat).markSemiAntiBuildFilters(ctx, stages)
		if _, consumers := findSemiAntiMarks(stages); len(consumers) != 0 {
			t.Fatal("non-semi consumer of the shared exchange must block marking")
		}
	})
	// The two key-class gates are pinned SEPARATELY. There is one gate per
	// side and they are checked in sequence, so a single negative that trips
	// both leaves whichever runs second untested — deleting the probe-side
	// check used to leave the whole suite green.
	t.Run("non-integer BUILD key", func(t *testing.T) {
		// l_shipmode is a real STRING column of the fixture catalog (not a
		// missing one, which would refuse for a different reason). The probe
		// key stays o_orderkey, an INT32, so the build gate is the only one
		// that can be the reason this does not mark.
		stages := semiAntiFixture("l_shipmode")
		NewPlanner(cat).markSemiAntiBuildFilters(ctx, stages)
		if _, consumers := findSemiAntiMarks(stages); len(consumers) != 0 {
			t.Fatal("a STRING build key must not be marked: the emit op indexes Int64Data")
		}
	})
	t.Run("non-integer PROBE key", func(t *testing.T) {
		// The mirror: an INT32 build key, so the build gate passes, and a
		// STRING probe key. The emit runs over S's output, so it is the PROBE
		// column DynamicFilterEmitOp actually indexes — applyDynamicFilters
		// has always checked both sides and this pass checked only one.
		stages := semiAntiFixture("l_orderkey")
		stages[2].Columns = []string{"o_orderstatus"}
		stages[3].JoinLeftKeys = []string{"o_orderstatus"}
		stages[4].JoinLeftKeys = []string{"o_orderstatus"}
		NewPlanner(cat).markSemiAntiBuildFilters(ctx, stages)
		if _, consumers := findSemiAntiMarks(stages); len(consumers) != 0 {
			t.Fatal("a STRING probe key must not be marked: the emit op indexes Int64Data on S's output")
		}
	})
	t.Run("no catalog", func(t *testing.T) {
		// The state the negatives above USED to run in. Asserted rather than
		// stumbled into, so a future fixture that loses its catalog fails
		// here instead of quietly making every other negative vacuous.
		stages := semiAntiFixture("l_orderkey")
		NewPlanner(nil).markSemiAntiBuildFilters(context.Background(), stages)
		if _, consumers := findSemiAntiMarks(stages); len(consumers) != 0 {
			t.Fatal("with no catalog the key class is unknowable and the pass must refuse")
		}
	})
}

func dumpStageIDs(stages []Stage) string {
	var b strings.Builder
	for i := range stages {
		b.WriteString(stages[i].ID)
		b.WriteString(" type=")
		b.WriteString(stages[i].Type)
		b.WriteString(" join=")
		b.WriteString(stages[i].JoinType)
		b.WriteString(" deps=")
		b.WriteString(strings.Join(stages[i].Dependencies, ","))
		b.WriteString("\n")
	}
	return b.String()
}
