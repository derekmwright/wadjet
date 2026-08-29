package physical

import (
	"reflect"
	"testing"
)

func stagesByID(stages []Stage) map[string]*Stage {
	m := make(map[string]*Stage, len(stages))
	for i := range stages {
		m[stages[i].ID] = &stages[i]
	}
	return m
}

func countStages(stages []Stage, pred func(*Stage) bool) int {
	n := 0
	for i := range stages {
		if pred(&stages[i]) {
			n++
		}
	}
	return n
}

// Q11's scalar-subquery leg is a stage-for-stage clone of its main leg
// (same joins, the clone's partsupp scan reads a column subset). The dedup
// pass must drop the clone and point the scalar leg's partial aggregate at
// the surviving join.
func TestSharedSubplanDedup_Q11CloneLegDropped(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx, tpchPlanQueryMap[11], 3)

	if got := countStages(stages, func(s *Stage) bool { return s.TableName == "partsupp" }); got != 1 {
		t.Errorf("partsupp scans = %d, want 1 (clone leg not deduped)", got)
	}
	joins := countStages(stages, func(s *Stage) bool {
		return s.Type == StageBroadcastJoin || s.Type == StageHashJoin
	})
	if joins != 1 {
		t.Errorf("join stages = %d, want 1", joins)
	}
	// The scalar leg's partial aggregate must now consume the surviving join.
	byID := stagesByID(stages)
	var mainJoinID string
	for i := range stages {
		if stages[i].Type == StageBroadcastJoin || stages[i].Type == StageHashJoin {
			mainJoinID = stages[i].ID
		}
	}
	scalarPartials := 0
	for i := range stages {
		s := &stages[i]
		if s.Type != StageAggregate {
			continue
		}
		if len(s.Dependencies) != 1 || s.Dependencies[0] != mainJoinID {
			t.Errorf("aggregate %s deps=%v, want [%s]", s.ID, s.Dependencies, mainJoinID)
		}
		scalarPartials++
	}
	if scalarPartials != 2 {
		t.Errorf("partial aggregates = %d, want 2 (grouped + scalar)", scalarPartials)
	}
	// No dangling references to dropped stages.
	for i := range stages {
		for _, ref := range stageEdgeRefs(&stages[i]) {
			if _, ok := byID[ref]; !ok {
				t.Errorf("stage %s references dropped stage %s", stages[i].ID, ref)
			}
		}
	}
}

// Q11 in the shuffle regime (broadcast disabled): the clone legs are
// exchange-repartition + hash_join chains — the SF100 shape. Dedup must
// fire on those fingerprints too.
func TestSharedSubplanDedup_Q11ShuffleShape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, tpchPlanQueryMap[11], 3, 1)

	if got := countStages(stages, func(s *Stage) bool { return s.TableName == "partsupp" }); got != 1 {
		t.Errorf("partsupp scans = %d, want 1 (clone leg not deduped in shuffle shape)", got)
	}
	byID := stagesByID(stages)
	for i := range stages {
		for _, ref := range stageEdgeRefs(&stages[i]) {
			if _, ok := byID[ref]; !ok {
				t.Errorf("stage %s references dropped stage %s", stages[i].ID, ref)
			}
		}
	}
}

// Q17's decorrelated AVG leg computes lineitem⋈part as a SEMI join that is
// row-equivalent to the main leg's INNER join for its
// duplication-invariant consumer (AVG grouped on the probe key). The semi
// leg must ride the inner join's output.
func TestSharedSubplanDedup_Q17SemiRidesInner(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx, tpchPlanQueryMap[17], 3)

	if got := countStages(stages, func(s *Stage) bool { return s.TableName == "lineitem" }); got != 1 {
		t.Errorf("lineitem scans = %d, want 1 (semi leg not deduped)", got)
	}
	for i := range stages {
		if stages[i].JoinType == "semi" {
			t.Errorf("semi join %s survived; want it rewired onto the inner sibling", stages[i].ID)
		}
	}
	// The AVG partial consumes the inner join directly.
	var innerJoinID string
	for i := range stages {
		if stages[i].Type == StageBroadcastJoin && stages[i].JoinType == "inner" {
			innerJoinID = stages[i].ID
		}
	}
	found := false
	for i := range stages {
		s := &stages[i]
		if s.Type == StageAggregate && len(s.AggSpecs) == 1 && s.AggSpecs[0].Func == "avg" {
			found = true
			if len(s.Dependencies) != 1 || s.Dependencies[0] != innerJoinID {
				t.Errorf("avg partial %s deps=%v, want [%s]", s.ID, s.Dependencies, innerJoinID)
			}
		}
	}
	if !found {
		t.Fatalf("no avg partial aggregate found")
	}
}

func TestSharedSubplanDedup_KillSwitch(t *testing.T) {
	prev := SharedSubplanDedup.Load()
	t.Cleanup(func() { SharedSubplanDedup.Store(prev) })
	SharedSubplanDedup.Store(false)

	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx, tpchPlanQueryMap[11], 3)
	if got := countStages(stages, func(s *Stage) bool { return s.TableName == "partsupp" }); got != 2 {
		t.Errorf("partsupp scans with kill switch = %d, want 2 (legacy clone shape)", got)
	}
}

// A duplicated subtree feeding BOTH slots of one consumer (self-join of an
// identical subplan) must not collapse: rewiring would alias the consumer's
// two inputs onto one slot. The guard skips consumers that already depend
// on the keeper.
func TestSharedSubplanDedup_SelfJoinPairUntouched(t *testing.T) {
	mk := func(suffix string) []Stage {
		return []Stage{
			{ID: "scan-a" + suffix, Type: StageScan, TableName: "t", Columns: []string{"k", "v"}, Tasks: 4},
			{ID: "scan-b" + suffix, Type: StageScan, TableName: "u", Columns: []string{"k"}, Tasks: 1},
			{
				ID: "join" + suffix, Type: StageBroadcastJoin, JoinType: "inner",
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
				Dependencies: []string{"scan-a" + suffix, "scan-b" + suffix},
				LeftDepStage: "scan-a" + suffix, RightDepStage: "scan-b" + suffix,
				Tasks: 3,
			},
		}
	}
	stages := append(mk("1"), mk("2")...)
	stages = append(stages, Stage{
		ID: "top", Type: StageHashJoin, JoinType: "inner",
		JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
		Dependencies: []string{"join1", "join2"},
		LeftDepStage: "join1", RightDepStage: "join2",
	})

	out := dedupeSharedSubplans(stages)
	if len(out) != len(stages) {
		t.Fatalf("self-join pair collapsed: %d stages, want %d", len(out), len(stages))
	}
	top := stagesByID(out)["top"]
	if top.LeftDepStage == top.RightDepStage {
		t.Errorf("top join slots aliased onto one input: L=%s R=%s", top.LeftDepStage, top.RightDepStage)
	}
}

// Duplicated subtrees with distinct consumers DO collapse in the hand-built
// shape (sanity check that the self-join guard is the only thing preventing
// the collapse above).
func TestSharedSubplanDedup_HandBuiltPairCollapses(t *testing.T) {
	stages := []Stage{
		{ID: "scan-a1", Type: StageScan, TableName: "t", Columns: []string{"k", "v"}, Tasks: 4},
		{ID: "scan-b1", Type: StageScan, TableName: "u", Columns: []string{"k"}, Tasks: 1},
		{
			ID: "join1", Type: StageBroadcastJoin, JoinType: "inner",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Dependencies: []string{"scan-a1", "scan-b1"},
			LeftDepStage: "scan-a1", RightDepStage: "scan-b1",
			Tasks: 3,
		},
		// Clone leg reads a SUBSET of scan-a1's columns.
		{ID: "scan-a2", Type: StageScan, TableName: "t", Columns: []string{"k"}, Tasks: 4},
		{ID: "scan-b2", Type: StageScan, TableName: "u", Columns: []string{"k"}, Tasks: 1},
		{
			ID: "join2", Type: StageBroadcastJoin, JoinType: "inner",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Dependencies: []string{"scan-a2", "scan-b2"},
			LeftDepStage: "scan-a2", RightDepStage: "scan-b2",
			Tasks: 3,
		},
		{ID: "agg1", Type: StageAggregate, GroupByCols: []string{"k"}, AggSpecs: []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "s"}}, Dependencies: []string{"join1"}},
		{ID: "agg2", Type: StageAggregate, GroupByCols: []string{"k"}, AggSpecs: []AggSpec{{Func: "min", InputCol: "k", OutputCol: "m"}}, Dependencies: []string{"join2"}},
	}
	out := dedupeSharedSubplans(stages)
	if len(out) != 5 {
		t.Fatalf("got %d stages, want 5 (join2 leg dropped)", len(out))
	}
	agg2 := stagesByID(out)["agg2"]
	if len(agg2.Dependencies) != 1 || agg2.Dependencies[0] != "join1" {
		t.Errorf("agg2 deps=%v, want [join1]", agg2.Dependencies)
	}
}

// Extra keeper columns on the BUILD side must not block dedup — the SF100
// Q11 shape: the wider partsupp scan is the hash join's build input; build-
// side extras never rebind a dropped-leg consumer's references. (The
// 2026-08-14 SF100 pair caught the original global-extras check skipping
// exactly this.)
func TestSharedSubplanDedup_BuildSideExtraColsDedup(t *testing.T) {
	mk := func(sfx string, partsuppCols []string) []Stage {
		return []Stage{
			{ID: "supscan" + sfx, Type: StageScan, TableName: "supplier", Columns: []string{"s_suppkey", "s_nationkey"}, Tasks: 2},
			{ID: "psscan" + sfx, Type: StageScan, TableName: "partsupp", Columns: partsuppCols, Tasks: 4},
			{ID: "rpL" + sfx, Type: StageExchangeRepartition, Dependencies: []string{"supscan" + sfx},
				Exchange: &ExchangeStage{Keys: []string{"s_suppkey"}, Count: 8}, Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"s_suppkey"}, Count: 8}},
			{ID: "rpR" + sfx, Type: StageExchangeRepartition, Dependencies: []string{"psscan" + sfx},
				Exchange: &ExchangeStage{Keys: []string{"ps_suppkey"}, Count: 8}, Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"ps_suppkey"}, Count: 8}},
			{ID: "join" + sfx, Type: StageHashJoin, JoinType: "inner",
				JoinLeftKeys: []string{"s_suppkey"}, JoinRightKeys: []string{"ps_suppkey"},
				Dependencies: []string{"rpL" + sfx, "rpR" + sfx},
				LeftDepStage: "rpL" + sfx, RightDepStage: "rpR" + sfx,
				JoinPartitionCount: 8, Tasks: 8},
		}
	}
	// Main leg's partsupp (build side) reads ps_partkey extra — superset.
	stages := append(mk("1", []string{"ps_suppkey", "ps_supplycost", "ps_partkey"}),
		mk("2", []string{"ps_suppkey", "ps_supplycost"})...)
	stages = append(stages,
		Stage{ID: "agg1", Type: StageAggregate, GroupByCols: []string{"ps_partkey"}, AggSpecs: []AggSpec{{Func: "sum", InputCol: "ps_supplycost", OutputCol: "v"}}, Dependencies: []string{"join1"}},
		Stage{ID: "agg2", Type: StageAggregate, AggSpecs: []AggSpec{{Func: "sum", InputCol: "ps_supplycost", OutputCol: "t"}}, Dependencies: []string{"join2"}},
	)
	out := dedupeSharedSubplans(stages)
	if len(out) != 7 {
		t.Fatalf("got %d stages, want 7 (join2 leg dropped; build-side extra must not block)", len(out))
	}
	agg2 := stagesByID(out)["agg2"]
	if len(agg2.Dependencies) != 1 || agg2.Dependencies[0] != "join1" {
		t.Errorf("agg2 deps=%v, want [join1]", agg2.Dependencies)
	}
}

// Extra keeper columns on the PROBE side that collide with a build output
// name MUST still block dedup — the qualification flip would rebind the
// rewired consumer's bare reference.
func TestSharedSubplanDedup_ProbeExtraCollisionSkips(t *testing.T) {
	mk := func(sfx string, probeCols []string) []Stage {
		return []Stage{
			{ID: "probe" + sfx, Type: StageScan, TableName: "t", Columns: probeCols, Tasks: 2},
			{ID: "build" + sfx, Type: StageScan, TableName: "u", Columns: []string{"k", "shared_name"}, Tasks: 1},
			{ID: "join" + sfx, Type: StageBroadcastJoin, JoinType: "inner",
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
				Dependencies: []string{"probe" + sfx, "build" + sfx},
				LeftDepStage: "probe" + sfx, RightDepStage: "build" + sfx, Tasks: 2},
		}
	}
	// Keeper probe reads "shared_name" extra — collides with build's column.
	stages := append(mk("1", []string{"k", "v", "shared_name"}), mk("2", []string{"k", "v"})...)
	stages = append(stages,
		Stage{ID: "agg1", Type: StageAggregate, Dependencies: []string{"join1"}},
		Stage{ID: "agg2", Type: StageAggregate, Dependencies: []string{"join2"}},
	)
	out := dedupeSharedSubplans(stages)
	if len(out) != len(stages) {
		t.Fatalf("probe-extra collision pair collapsed: %d stages, want %d", len(out), len(stages))
	}
}

// When neither leg's scan columns cover the other's, the pair must be left
// alone (v1 has no union-columns merge).
func TestSharedSubplanDedup_IncomparableColumnsSkipped(t *testing.T) {
	stages := []Stage{
		{ID: "scan-a1", Type: StageScan, TableName: "t", Columns: []string{"k", "v"}, Tasks: 4},
		{ID: "scan-b1", Type: StageScan, TableName: "u", Columns: []string{"k"}, Tasks: 1},
		{
			ID: "join1", Type: StageBroadcastJoin, JoinType: "inner",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Dependencies: []string{"scan-a1", "scan-b1"},
			LeftDepStage: "scan-a1", RightDepStage: "scan-b1",
		},
		{ID: "scan-a2", Type: StageScan, TableName: "t", Columns: []string{"k", "w"}, Tasks: 4},
		{ID: "scan-b2", Type: StageScan, TableName: "u", Columns: []string{"k"}, Tasks: 1},
		{
			ID: "join2", Type: StageBroadcastJoin, JoinType: "inner",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Dependencies: []string{"scan-a2", "scan-b2"},
			LeftDepStage: "scan-a2", RightDepStage: "scan-b2",
		},
		{ID: "agg1", Type: StageAggregate, Dependencies: []string{"join1"}},
		{ID: "agg2", Type: StageAggregate, Dependencies: []string{"join2"}},
	}
	out := dedupeSharedSubplans(stages)
	if len(out) != len(stages) {
		t.Fatalf("incomparable pair collapsed: %d stages, want %d", len(out), len(stages))
	}
}

func TestSemiConsumerDuplicationInvariant(t *testing.T) {
	probe := []string{"l_partkey"}
	cases := []struct {
		name string
		c    Stage
		want bool
	}{
		{"avg grouped on key", Stage{Type: StageAggregate, GroupByCols: []string{"l_partkey"}, AggSpecs: []AggSpec{{Func: "avg"}}}, true},
		{"min+max grouped wider", Stage{Type: "final_aggregate", GroupByCols: []string{"l_partkey", "x"}, AggSpecs: []AggSpec{{Func: "min"}, {Func: "max"}}}, true},
		{"sum is duplication-sensitive", Stage{Type: StageAggregate, GroupByCols: []string{"l_partkey"}, AggSpecs: []AggSpec{{Func: "sum"}}}, false},
		{"count is duplication-sensitive", Stage{Type: StageAggregate, GroupByCols: []string{"l_partkey"}, AggSpecs: []AggSpec{{Func: "count"}}}, false},
		{"group keys missing probe key", Stage{Type: StageAggregate, GroupByCols: []string{"other"}, AggSpecs: []AggSpec{{Func: "avg"}}}, false},
		{"ungrouped", Stage{Type: StageAggregate, AggSpecs: []AggSpec{{Func: "avg"}}}, false},
		{"not an aggregate", Stage{Type: StageHashJoin}, false},
		{"chained work disqualifies", Stage{Type: StageAggregate, GroupByCols: []string{"l_partkey"}, AggSpecs: []AggSpec{{Func: "avg"}}, ChainedJoins: []ChainedJoinSpec{{}}}, false},
	}
	for _, tc := range cases {
		if got := semiConsumerDuplicationInvariant(&tc.c, probe); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Every Stage field must be consciously classified for the subtree
// fingerprint. Hashed-by-default is the safe direction (a new field that
// differs between clones just suppresses dedup); the DANGEROUS cases are
// fields that reference other stages by ID — those must be added to
// stageEdgeRefs + rewireEdges + the slot resolution in fingerprintAs, or a
// dedup can leave dangling references / stale fingerprint matches. If this
// test fails, classify the new field below AND audit those three sites.
func TestSharedSubplanDedup_StageFieldCoverage(t *testing.T) {
	classified := map[string]string{
		// Identity / projection fields EXCLUDED from the fingerprint.
		"ID": "excluded", "ScanAlias": "excluded", "Columns": "excluded",
		"OutputColumns": "excluded", "ScanFileSizes": "excluded",
		"EstimatedBytes": "excluded", "EstimatedRows": "excluded",
		// Projection-derived like Columns, and advisory: the declared side
		// schemas are read only when a join side is empty (#348/#352).
		"JoinProbeSchema": "excluded", "JoinBuildSchema": "excluded",
		// Stage-reference fields: slot-resolved in the fingerprint, walked
		// by stageEdgeRefs, rewritten by rewireEdges.
		"Dependencies": "reference", "LeftDepStage": "reference",
		"RightDepStage": "reference", "FusedJoins": "reference",
		"ChainedJoins": "reference", "ScalarDependencies": "reference",
		"ConsumeDynamicFilters": "reference",
		// UnionArms[i].DepStage must stay index-aligned with Dependencies[i]
		// (dispatch pairs them), so rewireEdges rewrites both. It also joins
		// the refuse list in fingerprintAs: StageUnion is not fingerprintable
		// today, and the explicit clause keeps that from becoming a silent
		// gap if the fingerprintable set widens.
		"UnionArms": "reference",
		// Refuse-to-fingerprint fields (later passes / non-whitelisted
		// stage types only).
		// OutputSchema rides with OutputRenames: both are set only on the
		// terminal gather, both describe the RESULT rather than the work,
		// and refusing costs nothing because that stage type is not
		// fingerprintable anyway (#416).
		"OutputRenames": "refused", "OutputSchema": "refused",
		// OutputWireUnconstrainedDecimal rides with OutputSchema: set on
		// the SAME terminal gather stage, from the SAME declaredProjection
		// Inputs gate in declaredWireUnconstrainedDecimal, whenever
		// OutputSchema itself is set (FIX 2, #457/#458 fold-in).
		"OutputWireUnconstrainedDecimal": "refused",
		"EmitDynamicFilters":             "refused",
		"PreComputedAggregates":          "refused", "BuildCachePreScans": "refused",
		// Everything else is hashed structurally via the JSON serialization.
		"Type": "hashed", "ClusterID": "hashed", "Tasks": "hashed",
		"TableName": "hashed", "PartitionFilter": "hashed", "ScanFiles": "hashed",
		// The catalog's declared schema for TableName (#423) is a pure
		// function of a field already hashed, so two clone subtrees carry
		// identical values and hashing costs nothing. It is also empty at
		// this point — annotateScanSchemas runs after this pass — which is
		// why hashing it can never suppress a dedup that used to happen.
		"ScanSchema": "hashed",
		// The table's merge-on-read DELETE state (#491) is, like ScanSchema,
		// a pure function of TableName — walkStages takes it from the same
		// manifest object as ScanFiles, so two clone subtrees over one table
		// carry the identical map and hashing can never suppress a dedup
		// that used to happen. Unlike ScanSchema it IS populated by this
		// point, which is why it is hashed rather than merely harmless:
		// two subtrees that somehow disagreed about which rows exist are
		// not interchangeable.
		"ScanDeletes": "hashed",
		"FilterExprs": "hashed", "GroupByCols": "hashed", "AggSpecs": "hashed",
		// FilterAliases rides with FilterExprs: it is the same predicates'
		// second spelling, index-aligned, and two subtrees whose predicates
		// resolved from different aliases are not interchangeable — the
		// spelling that survives resolveFilterAliasSpelling depends on it.
		"FilterAliases": "hashed",
		// ConsumerScoped says the stage's filter belongs to ONE consumer.
		// Hashed, because merging such a stage with another subtree is
		// exactly how a consumer-scoped filter acquires a second consumer
		// — the defect assertNoConsumerScopedFilterOnSharedStage refuses.
		"ConsumerScoped": "hashed",
		"GroupByAll": "hashed", "SortKeys": "hashed", "Limit": "hashed",
		// HasLimit rides with Limit: without it, two subtrees with a
		// genuine LIMIT 0 vs. no LIMIT at all would hash identically and
		// dedup into one (#481).
		"HasLimit": "hashed",
		// Offset rides with Limit for the same reason: a StageLimit that
		// skips 5 rows and one that skips none emit different rows.
		"Offset": "hashed",
		// RowLimit changes how many rows a stage emits, so two otherwise
		// identical subtrees with different bounds are not interchangeable
		// and must not dedup into one.
		"RowLimit":       "hashed",
		"SortShardLocal": "hashed", "JoinType": "hashed",
		"JoinLeftKeys": "hashed", "JoinRightKeys": "hashed",
		// The key pair's RESOLVED common type (#615) decides what bytes the
		// key is built from, so two joins over the same columns that resolve
		// to different types compute different row sets and are never
		// interchangeable. Hashed for the same reason JoinLeftKeys is.
		"JoinKeyTypes":    "hashed",
		"BuildTableAlias": "hashed", "BuildColOrigins": "hashed",
		"JoinFilter": "hashed", "BuildFilterExprs": "hashed",
		// NOT IN's three-valued rule changes which rows the stage EMITS
		// (#507), so an anti join that owes it is not interchangeable with
		// one that does not.
		"NullAwareAnti":     "hashed",
		"ChainedAggGroupBy": "hashed", "ChainedAggSpecs": "hashed",
		// Derived group-key types (#379) are a pure function of GroupByCols
		// plus the input schema, so identical subtrees carry identical maps
		// and hashing is the safe default (encoding/json sorts map keys).
		"GroupByTypes": "hashed", "GroupByDecimal": "hashed",
		"WindowCols": "hashed",
		// The materialized PARTITION BY / ORDER BY keys (#585) change what
		// the window PARTITIONS ON, so two subtrees whose window keys differ
		// are never interchangeable — and the synthetic names are positional
		// (__winkey_0, __winkey_1), so the expressions have to be part of the
		// fingerprint or two stages keyed on different values would hash
		// alike on the names alone.
		"WindowKeyExprs":     "hashed",
		"JoinPartitionCount": "hashed",
		"FusedAggGroupBy":    "hashed", "FusedAggSpecs": "hashed",
		"RawInputAggregate": "hashed",
		// The set-operation marker changes what the stage EMITS (intersect
		// vs except vs plain aggregate; distinct vs ALL), so two subtrees
		// differing here are never interchangeable.
		"SetOp": "hashed", "SetOpAll": "hashed",
		"ProbeSplitAlias": "hashed",
		"ProbeSplitFiles": "hashed", "MergeGroup": "hashed",
		"MergeGroupCount": "hashed", "Distribution": "hashed",
		"Exchange": "hashed", "ProjectExprs": "hashed",
		"SecurityProjectExprs": "hashed", "QualifyAllBuildCols": "hashed",
	}
	tp := reflect.TypeOf(Stage{})
	for i := 0; i < tp.NumField(); i++ {
		name := tp.Field(i).Name
		if _, ok := classified[name]; !ok {
			t.Errorf("Stage field %q is not classified for the shared-subplan fingerprint; "+
				"classify it in this test and, if it references stage IDs, "+
				"update stageEdgeRefs/rewireEdges/fingerprintAs", name)
		}
	}
	for name := range classified {
		if _, ok := tp.FieldByName(name); !ok {
			t.Errorf("classified field %q no longer exists on Stage", name)
		}
	}
}

// Fingerprints must ignore scan aliases and column order but respect
// filters, keys, and table identity.
func TestSharedSubplanFingerprint_Sensitivity(t *testing.T) {
	base := func() []Stage {
		return []Stage{
			{ID: "s1", Type: StageScan, TableName: "t", ScanAlias: "t", Columns: []string{"a", "b"}, Tasks: 2},
			{ID: "s2", Type: StageScan, TableName: "t", ScanAlias: "t:1", Columns: []string{"b", "a"}, Tasks: 2},
		}
	}
	fpPair := func(stages []Stage) (string, string, bool) {
		d := newSubplanDeduper(stages)
		f1, ok1 := d.fingerprint(stages[0].ID)
		f2, ok2 := d.fingerprint(stages[1].ID)
		return f1, f2, ok1 && ok2
	}

	if f1, f2, ok := fpPair(base()); !ok || f1 != f2 {
		t.Errorf("alias/column differences must not split fingerprints (ok=%v, equal=%v)", ok, f1 == f2)
	}
	withFilter := base()
	withFilter[1].FilterExprs = []string{"a > 1"}
	if f1, f2, ok := fpPair(withFilter); !ok || f1 == f2 {
		t.Errorf("filter difference must split fingerprints (ok=%v)", ok)
	}
	otherTable := base()
	otherTable[1].TableName = "u"
	if f1, f2, ok := fpPair(otherTable); !ok || f1 == f2 {
		t.Errorf("table difference must split fingerprints (ok=%v)", ok)
	}
}
