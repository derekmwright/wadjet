package physical

import (
	"testing"
)

// The cascade fixture after marking: hop-B's consume on the fact scan is a
// terminal consume fed by a scan-only emitter on a dispatched scan — it
// converts to attach-on-arrival (stat-dep removed, emit LateAttach).
// Hop-A's consume on the supplier converts as a GUARDED RE-EMIT (rule-1
// relaxation): the supplier's emit repositions AtOutput→AtScan and gains a
// GuardConsumes entry so its buffered head rows retro-filter through the
// nation bloom, and the supplier's stat-dep on nation is removed — the
// entire chain dispatches at t=0.
func TestAttachOnArrivalConvertsTerminalScanConsume(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := cascadeFixture()
	p.markDimensionCascade(ctx, stages)
	before := AttachOnArrivalConsumesPlanned.Load()
	stages = applyAttachOnArrival(stages)

	var nation, supp, l1 *Stage
	for i := range stages {
		switch stages[i].ID {
		case "scan-nation":
			nation = &stages[i]
		case "scan-supp":
			supp = &stages[i]
		case "scan-l1":
			l1 = &stages[i]
		}
	}
	// Hop B on the fact scan: converted.
	if !l1.ConsumeDynamicFilters[0].AttachOnArrival {
		t.Fatal("terminal scan consume must convert to attach-on-arrival")
	}
	if containsString(l1.Dependencies, "scan-supp") {
		t.Fatalf("stat-dep edge must be removed on attach: %v", l1.Dependencies)
	}
	// Matching emit on the supplier: LateAttach forces coordinator staging.
	if !supp.EmitDynamicFilters[0].LateAttach {
		t.Fatal("emit matching an attach consume must be LateAttach")
	}
	// Hop A on the re-emitting supplier: converted as a guarded re-emit.
	if !supp.ConsumeDynamicFilters[0].AttachOnArrival {
		t.Fatal("re-emitting consumer must convert under the guarded relaxation")
	}
	if containsString(supp.Dependencies, "scan-nation") {
		t.Fatalf("hop-A stat-dep must be removed on guarded attach: %v", supp.Dependencies)
	}
	if !nation.EmitDynamicFilters[0].LateAttach {
		t.Fatal("hop-A emit must be LateAttach (its consume converted)")
	}
	em := supp.EmitDynamicFilters[0]
	if em.AtOutput {
		t.Fatal("guarded emit must reposition AtOutput→AtScan")
	}
	if len(em.GuardConsumes) != 1 || em.GuardConsumes[0] != supp.ConsumeDynamicFilters[0].FilterID {
		t.Fatalf("guarded emit must reference the consume as its guard: %+v", em.GuardConsumes)
	}
	if AttachOnArrivalConsumesPlanned.Load() != before+2 {
		t.Fatal("counter must count both converted edges")
	}
}

// Guarded-re-emit kill switch: re-emitters revert to the barrier while
// terminal consumes still convert.
func TestAttachOnArrivalGuardedReemitKillSwitch(t *testing.T) {
	DFGuardedReemit.Store(false)
	defer DFGuardedReemit.Store(true)
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := cascadeFixture()
	p.markDimensionCascade(ctx, stages)
	stages = applyAttachOnArrival(stages)
	for i := range stages {
		s := &stages[i]
		switch s.ID {
		case "scan-supp":
			if s.ConsumeDynamicFilters[0].AttachOnArrival {
				t.Fatal("kill switch must keep the re-emitter barrier")
			}
			if !containsString(s.Dependencies, "scan-nation") {
				t.Fatal("kill switch must keep the hop-A stat-dep")
			}
			if !s.EmitDynamicFilters[0].AtOutput || len(s.EmitDynamicFilters[0].GuardConsumes) != 0 {
				t.Fatal("kill switch must leave the emit unrewritten")
			}
		case "scan-l1":
			if !s.ConsumeDynamicFilters[0].AttachOnArrival {
				t.Fatal("terminal consume must still convert with guarded re-emit off")
			}
		}
	}
}

// A re-emitting scan WITH FilterExprs must keep the barrier: repositioning
// its emit to the scan head would observe pre-filter rows and a column-pair
// guard cannot reproduce arbitrary predicate filtering.
func TestAttachOnArrivalGuardedReemitRequiresBareScan(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := cascadeFixture()
	p.markDimensionCascade(ctx, stages)
	for i := range stages {
		if stages[i].ID == "scan-supp" {
			stages[i].FilterExprs = []string{"s_acctbal > 0"}
		}
	}
	stages = applyAttachOnArrival(stages)
	for i := range stages {
		if stages[i].ID == "scan-supp" {
			if stages[i].ConsumeDynamicFilters[0].AttachOnArrival {
				t.Fatal("re-emitter with FilterExprs must keep the barrier")
			}
			if !stages[i].EmitDynamicFilters[0].AtOutput {
				t.Fatal("emit must stay AtOutput when not guarded")
			}
		}
	}
}

// Join-fed emitters (the semi/anti build-filter shape) must keep the
// barrier: the filter lands only after a multi-stage probe chain, far too
// late for opportunistic attach to preserve its shuffle/build reduction.
func TestAttachOnArrivalSkipsJoinFedEmitter(t *testing.T) {
	stages := []Stage{
		{ID: "join-src", Type: StageHashJoin,
			EmitDynamicFilters: []DynamicFilterEmit{{FilterID: "sabf-1", KeyColumn: "l_orderkey", KeyType: "int64", BloomBits: 1024, AtOutput: true}}},
		{ID: "scan-build", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"}, FilterExprs: []string{"x > 1"},
			ConsumeDynamicFilters: []DynamicFilterConsume{{FilterID: "sabf-1", SourceStageID: "join-src", TargetColumn: "l_orderkey", KeyType: "int64"}},
			Dependencies:          []string{"join-src"}},
	}
	stages = applyAttachOnArrival(stages)
	if stages[1].ConsumeDynamicFilters[0].AttachOnArrival {
		t.Fatal("join-fed emitter must keep the barrier (rule 2)")
	}
	if !containsString(stages[1].Dependencies, "join-src") {
		t.Fatal("stat-dep must survive for wait-mode consume")
	}
	if stages[0].EmitDynamicFilters[0].LateAttach {
		t.Fatal("wait-mode emit must not be LateAttach")
	}
}

// Consumers outside the dispatched scan-filter fragment path have no
// runtime pipeline to attach into: pass-through scans (no filters/
// projections/fused exchange) and fused scan-aggregate scans keep the
// barrier.
func TestAttachOnArrivalRequiresDispatchedConsumer(t *testing.T) {
	base := func() []Stage {
		return []Stage{
			{ID: "scan-dim", Type: StageScan, TableName: "nation",
				ScanFiles:          []string{"f"},
				FilterExprs:        []string{"n_name = 'X'"},
				EmitDynamicFilters: []DynamicFilterEmit{{FilterID: "df-1", KeyColumn: "n_nationkey", KeyType: "int32", BloomBits: 1024}}},
			{ID: "scan-fact", Type: StageScan, TableName: "lineitem",
				ScanFiles:             []string{"f"},
				FilterExprs:           []string{"l_qty > 1"},
				ConsumeDynamicFilters: []DynamicFilterConsume{{FilterID: "df-1", SourceStageID: "scan-dim", TargetColumn: "l_nationkey", KeyType: "int32"}},
				Dependencies:          []string{"scan-dim"}},
		}
	}
	cases := map[string]func([]Stage){
		"pass-through consumer": func(s []Stage) { s[1].FilterExprs = nil },
		"fused-agg consumer":    func(s []Stage) { s[1].FusedAggGroupBy = []string{"l_orderkey"} },
	}
	for name, mutate := range cases {
		stages := base()
		mutate(stages)
		stages = applyAttachOnArrival(stages)
		if stages[1].ConsumeDynamicFilters[0].AttachOnArrival {
			t.Errorf("%s: must keep the barrier (rule 3)", name)
		}
		if !containsString(stages[1].Dependencies, "scan-dim") {
			t.Errorf("%s: stat-dep must survive", name)
		}
	}
	// Control: the unmutated fixture converts.
	stages := base()
	stages = applyAttachOnArrival(stages)
	if !stages[1].ConsumeDynamicFilters[0].AttachOnArrival {
		t.Fatal("control fixture must convert")
	}
	if containsString(stages[1].Dependencies, "scan-dim") {
		t.Fatal("control fixture must drop the stat-dep")
	}
}

func TestAttachOnArrivalKillSwitch(t *testing.T) {
	DFAttachOnArrival.Store(false)
	defer DFAttachOnArrival.Store(true)
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := cascadeFixture()
	p.markDimensionCascade(ctx, stages)
	stages = applyAttachOnArrival(stages)
	for i := range stages {
		for _, cf := range stages[i].ConsumeDynamicFilters {
			if cf.AttachOnArrival {
				t.Fatal("kill switch must leave every consume in wait mode")
			}
		}
		for _, e := range stages[i].EmitDynamicFilters {
			if e.LateAttach {
				t.Fatal("kill switch must leave every emit non-LateAttach")
			}
		}
	}
}

// A dependency that also serves a wait-mode consume from the same source
// must survive even when another consume from that source converts.
func TestFilterAttachedStatDepsKeepsMixedSource(t *testing.T) {
	deps := filterAttachedStatDeps([]string{"src", "other"}, []DynamicFilterConsume{
		{FilterID: "a", SourceStageID: "src", AttachOnArrival: true},
		{FilterID: "b", SourceStageID: "src", AttachOnArrival: false},
	})
	if !containsString(deps, "src") || !containsString(deps, "other") {
		t.Fatalf("mixed-mode source and unrelated deps must survive: %v", deps)
	}
	deps = filterAttachedStatDeps([]string{"src", "other"}, []DynamicFilterConsume{
		{FilterID: "a", SourceStageID: "src", AttachOnArrival: true},
	})
	if containsString(deps, "src") || !containsString(deps, "other") {
		t.Fatalf("fully-attached source drops, unrelated dep survives: %v", deps)
	}
}

func TestParseSemiAntiNE(t *testing.T) {
	cases := []struct {
		filter         string
		wantProbe, wantBuild string
		wantOK         bool
	}{
		{"l1.l_suppkey <> l2.l_suppkey", "l_suppkey", "l_suppkey", true},
		{"a.x != b.y", "x", "y", true},
		{"l_suppkey <> s_suppkey", "l_suppkey", "s_suppkey", true},
		{"a.x <> b.y AND a.z > b.w", "", "", false}, // conjunction
		{"a.x = b.y", "", "", false},                // wrong op
		{"p_brand <> 'Brand#45'", "", "", false},    // literal
		{"a.x <> 42", "", "", false},                // numeric literal
		{"", "", "", false},
	}
	for _, c := range cases {
		p, b, ok := ParseSemiAntiNE(c.filter)
		if ok != c.wantOK || p != c.wantProbe || b != c.wantBuild {
			t.Errorf("ParseSemiAntiNE(%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.filter, p, b, ok, c.wantProbe, c.wantBuild, c.wantOK)
		}
	}
	SemiAntiNE.Store(false)
	defer SemiAntiNE.Store(true)
	if _, _, ok := ParseSemiAntiNE("a.x <> b.y"); ok {
		t.Fatal("kill switch must disable recognition")
	}
}
