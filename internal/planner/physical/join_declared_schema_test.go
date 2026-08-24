package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A RIGHT or FULL join must never plan as a broadcast_join: broadcast
// replicates the build side to every task and splits the probe across them,
// and those two join types emit their UNMATCHED build rows — which no task
// can attribute, so every task emits all of them. Observed before this gate:
// 75 rows on a 3-worker cluster for a 25-row answer (#352).
func TestOuterJoinsNeverBroadcast(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	cases := []struct {
		name, sql string
		outer     bool
	}{
		{"right", "SELECT COUNT(*) AS c FROM region r RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey", true},
		{"full", "SELECT COUNT(*) AS c FROM nation n FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey", true},
		// The control: a LEFT join emits no build-side-only rows, so
		// replicating its build is sound and the broadcast plan must stay.
		{"left", "SELECT COUNT(*) AS c FROM nation n LEFT JOIN region r ON n.n_regionkey = r.r_regionkey", false},
		{"inner", "SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var joins, broadcasts int
			for _, s := range sqlToStages(t, cat, ctx, c.sql, 3) {
				switch s.Type {
				case StageBroadcastJoin:
					joins++
					broadcasts++
				case StageHashJoin:
					joins++
				}
			}
			if joins == 0 {
				t.Fatalf("no join stage planned for %s", c.sql)
			}
			if c.outer && broadcasts != 0 {
				t.Errorf("%s planned %d broadcast_join stage(s): a replicated build duplicates "+
					"the unmatched build rows once per task", c.name, broadcasts)
			}
			if !c.outer && broadcasts == 0 {
				t.Errorf("%s planned no broadcast_join: the outer-join gate must not move joins "+
					"that emit no build-side-only rows", c.name)
			}
		})
	}
}

// Every join stage carries its two sides' declared schemas, so a worker whose
// side turns out to be empty can still name that side's columns (#348/#352).
func TestJoinStagesCarryDeclaredSideSchemas(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT n.n_name, r.r_name FROM nation n
		LEFT JOIN region r ON n.n_regionkey = r.r_regionkey`
	var found bool
	for _, s := range sqlToStages(t, cat, ctx, sql, 3) {
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin {
			continue
		}
		found = true
		if !hasDeclaredColumn(s.JoinProbeSchema, "n_name") {
			t.Errorf("probe schema %v does not declare n_name", names(s.JoinProbeSchema))
		}
		if !hasDeclaredColumn(s.JoinBuildSchema, "r_name") {
			t.Errorf("build schema %v does not declare r_name", names(s.JoinBuildSchema))
		}
		// The join keys ride along even when the SELECT list omits them —
		// they are part of what the shuffle carries.
		if !hasDeclaredColumn(s.JoinBuildSchema, "r_regionkey") {
			t.Errorf("build schema %v does not declare the join key r_regionkey", names(s.JoinBuildSchema))
		}
		// Sides stay separate: a probe column must not appear in the build's
		// declaration, or an empty build would claim to produce it.
		if hasDeclaredColumn(s.JoinBuildSchema, "n_name") {
			t.Errorf("build schema %v claims the probe's n_name", names(s.JoinBuildSchema))
		}
	}
	if !found {
		t.Fatal("no join stage planned")
	}
}

// The declared types come from the catalog, not from a guess: a string column
// declared numeric would make an empty side's NULL column the wrong type.
func TestDeclaredJoinSchemaTypes(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT n.n_name, n.n_nationkey, r.r_name FROM nation n
		LEFT JOIN region r ON n.n_regionkey = r.r_regionkey`
	want := map[string]parquet.TypeID{
		"n_name":      parquet.TypeString,
		"n_nationkey": parquet.TypeInt32,
		"r_name":      parquet.TypeString,
	}
	for _, s := range sqlToStages(t, cat, ctx, sql, 3) {
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin {
			continue
		}
		for _, col := range append(append([]parquet.Column(nil), s.JoinProbeSchema...), s.JoinBuildSchema...) {
			if w, ok := want[strings.ToLower(col.Name)]; ok && col.Type != w {
				t.Errorf("column %s declared type %v, catalog says %v", col.Name, col.Type, w)
			}
		}
	}
}

// Regression for #473: absorbComputedSubqueryProjection (join_input_projection.go)
// materializes a join input's computed column into its producing scan
// fragment with the integer-preserving-arithmetic hint (#297, #445),
// declaring `r_regionkey + 1` INT64 — the type the worker actually builds
// the vector as (#333). declaredJoinSchema passed strictInt=nil when
// declaring the SAME column for the join's empty-side hint, so it disagreed
// and declared FLOAT64 instead: a join whose build side turns out empty
// would hand exec.HashJoin a schema at odds with what a full build side
// really produces for that column. This asserts the two declarations agree.
func TestDeclaredJoinSchemaAgreesWithComputedInputProjection(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT n.n_name, r.rk2 FROM nation n
		LEFT JOIN (SELECT r_regionkey, r_regionkey + 1 AS rk2 FROM region) r
		ON n.n_regionkey = r.r_regionkey`

	stages := sqlToStages(t, cat, ctx, sql, 3)

	// The PROJ spec: the materializing projection absorbComputedSubqueryProjection
	// appends to the region scan fragment.
	var scanProjType parquet.TypeID
	var foundScan bool
	for _, s := range stages {
		if s.Type != StageScan || s.TableName != "region" {
			continue
		}
		for _, spec := range s.ProjectExprs {
			if strings.EqualFold(spec.Name, "rk2") {
				foundScan = true
				scanProjType = spec.Type
				if !spec.TypeKnown {
					t.Errorf("scan's materializing projection for rk2 does not set TypeKnown")
				}
			}
		}
	}
	if !foundScan {
		t.Fatal("region scan carries no materialized projection for rk2")
	}
	if scanProjType != parquet.TypeInt64 {
		t.Errorf("materializing projection declares rk2 as %v, want INT64 "+
			"(r_regionkey + 1 over a strict-int column)", scanProjType)
	}

	// The PROBESC spec: the join stage's declared build-side schema
	// (declaredJoinSchema via joinSideSchemas), consulted only when the build
	// side turns out empty.
	var join *Stage
	for i := range stages {
		if stages[i].Type == StageHashJoin || stages[i].Type == StageBroadcastJoin {
			join = &stages[i]
		}
	}
	if join == nil {
		t.Fatal("no join stage planned")
	}
	var buildType parquet.TypeID
	var foundBuild bool
	for _, c := range join.JoinBuildSchema {
		if strings.EqualFold(c.Name, "rk2") {
			foundBuild = true
			buildType = c.Type
		}
	}
	if !foundBuild {
		t.Fatalf("JoinBuildSchema does not declare rk2: %+v", join.JoinBuildSchema)
	}
	if buildType != scanProjType {
		t.Errorf("declaredJoinSchema declares rk2 as %v, but the materializing projection that "+
			"actually produces it (absorbComputedSubqueryProjection) declares %v — an empty build "+
			"side would disagree with a full one about the type of its own column (#473)",
			buildType, scanProjType)
	}
}

func hasDeclaredColumn(cols []parquet.Column, name string) bool {
	for _, c := range cols {
		if strings.EqualFold(c.Name, name) {
			return true
		}
	}
	return false
}

func names(cols []parquet.Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}
