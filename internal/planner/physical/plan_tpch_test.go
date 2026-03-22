package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// SF10-like file counts for realistic distributed planning.
var tpchSF10Files = map[string]int{
	"region":   1,
	"nation":   1,
	"supplier": 1,
	"part":     20,
	"partsupp": 80,
	"customer": 15,
	"orders":   150,
	"lineitem": 600,
}

var tpchSchemas = map[string]parquet.Schema{
	"region": {Columns: []parquet.Column{
		{Name: "r_regionkey", Type: parquet.TypeInt32},
		{Name: "r_name", Type: parquet.TypeString},
		{Name: "r_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"nation": {Columns: []parquet.Column{
		{Name: "n_nationkey", Type: parquet.TypeInt32},
		{Name: "n_name", Type: parquet.TypeString},
		{Name: "n_regionkey", Type: parquet.TypeInt32},
		{Name: "n_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"supplier": {Columns: []parquet.Column{
		{Name: "s_suppkey", Type: parquet.TypeInt32},
		{Name: "s_name", Type: parquet.TypeString},
		{Name: "s_address", Type: parquet.TypeString},
		{Name: "s_nationkey", Type: parquet.TypeInt32},
		{Name: "s_phone", Type: parquet.TypeString},
		{Name: "s_acctbal", Type: parquet.TypeFloat64},
		{Name: "s_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"part": {Columns: []parquet.Column{
		{Name: "p_partkey", Type: parquet.TypeInt32},
		{Name: "p_name", Type: parquet.TypeString},
		{Name: "p_mfgr", Type: parquet.TypeString},
		{Name: "p_brand", Type: parquet.TypeString},
		{Name: "p_type", Type: parquet.TypeString},
		{Name: "p_size", Type: parquet.TypeInt32},
		{Name: "p_container", Type: parquet.TypeString},
		{Name: "p_retailprice", Type: parquet.TypeFloat64},
		{Name: "p_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"partsupp": {Columns: []parquet.Column{
		{Name: "ps_partkey", Type: parquet.TypeInt32},
		{Name: "ps_suppkey", Type: parquet.TypeInt32},
		{Name: "ps_availqty", Type: parquet.TypeInt32},
		{Name: "ps_supplycost", Type: parquet.TypeFloat64},
		{Name: "ps_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"customer": {Columns: []parquet.Column{
		{Name: "c_custkey", Type: parquet.TypeInt32},
		{Name: "c_name", Type: parquet.TypeString},
		{Name: "c_address", Type: parquet.TypeString},
		{Name: "c_nationkey", Type: parquet.TypeInt32},
		{Name: "c_phone", Type: parquet.TypeString},
		{Name: "c_acctbal", Type: parquet.TypeFloat64},
		{Name: "c_mktsegment", Type: parquet.TypeString},
		{Name: "c_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"orders": {Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt32},
		{Name: "o_custkey", Type: parquet.TypeInt32},
		{Name: "o_orderstatus", Type: parquet.TypeString},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
		{Name: "o_orderdate", Type: parquet.TypeString},
		{Name: "o_orderpriority", Type: parquet.TypeString},
		{Name: "o_clerk", Type: parquet.TypeString},
		{Name: "o_shippriority", Type: parquet.TypeInt32},
		{Name: "o_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"lineitem": {Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt32},
		{Name: "l_partkey", Type: parquet.TypeInt32},
		{Name: "l_suppkey", Type: parquet.TypeInt32},
		{Name: "l_linenumber", Type: parquet.TypeInt32},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
		{Name: "l_extendedprice", Type: parquet.TypeFloat64},
		{Name: "l_discount", Type: parquet.TypeFloat64},
		{Name: "l_tax", Type: parquet.TypeFloat64},
		{Name: "l_returnflag", Type: parquet.TypeString},
		{Name: "l_linestatus", Type: parquet.TypeString},
		{Name: "l_shipdate", Type: parquet.TypeString},
		{Name: "l_commitdate", Type: parquet.TypeString},
		{Name: "l_receiptdate", Type: parquet.TypeString},
		{Name: "l_shipinstruct", Type: parquet.TypeString},
		{Name: "l_shipmode", Type: parquet.TypeString},
		{Name: "l_comment", Type: parquet.TypeString, Nullable: true},
	}},
}

func setupTPCHCatalog(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	for tableName, schema := range tpchSchemas {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("create table %s: %v", tableName, err)
		}
		nFiles := tpchSF10Files[tableName]
		files := make([]catalog.FileEntry, nFiles)
		for i := range files {
			files[i] = catalog.FileEntry{
				Path:      fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, i),
				SizeBytes: 10 * 1024 * 1024, // 10MB per file
				NumRows:   100000,
			}
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{},
			"tables/"+tableName+"/", files); err != nil {
			t.Fatalf("add files for %s: %v", tableName, err)
		}
	}

	return cat, ctx
}

func sqlToStages(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string, workerCount int) []Stage {
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
	planner.WorkerCount = workerCount
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}

// validateStageGraph checks structural properties of the distributed stage graph.
func validateStageGraph(t *testing.T, stages []Stage, queryName string) {
	t.Helper()

	stageIDs := make(map[string]*Stage, len(stages))
	for i := range stages {
		s := &stages[i]
		if _, dup := stageIDs[s.ID]; dup {
			t.Errorf("%s: duplicate stage ID: %s", queryName, s.ID)
		}
		stageIDs[s.ID] = s
	}

	for _, s := range stages {
		// Every dependency must exist
		for _, dep := range s.Dependencies {
			if _, ok := stageIDs[dep]; !ok {
				t.Errorf("%s: stage %s depends on non-existent stage %s", queryName, s.ID, dep)
			}
		}

		// No stage depends on itself
		for _, dep := range s.Dependencies {
			if dep == s.ID {
				t.Errorf("%s: stage %s depends on itself", queryName, s.ID)
			}
		}

		// Scan stages should have no dependencies
		if s.Type == "scan" && len(s.Dependencies) > 0 {
			t.Errorf("%s: scan stage %s has dependencies: %v", queryName, s.ID, s.Dependencies)
		}

		// Join stages should have BuildTableAlias for self-join scenarios
		if (s.Type == "hash_join" || s.Type == "broadcast_join") && s.BuildTableAlias != "" {
			t.Logf("%s: stage %s has BuildTableAlias=%q", queryName, s.ID, s.BuildTableAlias)
		}

		// Shuffle stages should have valid keys
		if s.Type == "shuffle" {
			if len(s.ShuffleKeys) == 0 {
				t.Errorf("%s: shuffle stage %s has no shuffle keys", queryName, s.ID)
			}
			if s.NumPartitions < 2 {
				t.Errorf("%s: shuffle stage %s has <2 partitions: %d", queryName, s.ID, s.NumPartitions)
			}
		}

		// Join stages with shuffle should have matching partition counts
		if (s.Type == "hash_join" || s.Type == "broadcast_join") && s.NumPartitions > 1 {
			if s.LeftDepStage == "" || s.RightDepStage == "" {
				t.Errorf("%s: partitioned join %s missing dep stages (left=%q, right=%q)",
					queryName, s.ID, s.LeftDepStage, s.RightDepStage)
			}
		}
	}

	// Detect cycles
	visited := map[string]bool{}
	inStack := map[string]bool{}
	var hasCycle func(id string) bool
	hasCycle = func(id string) bool {
		if inStack[id] {
			return true
		}
		if visited[id] {
			return false
		}
		visited[id] = true
		inStack[id] = true
		s := stageIDs[id]
		if s != nil {
			for _, dep := range s.Dependencies {
				if hasCycle(dep) {
					return true
				}
			}
		}
		inStack[id] = false
		return false
	}
	for id := range stageIDs {
		if hasCycle(id) {
			t.Errorf("%s: dependency cycle detected involving stage %s", queryName, id)
		}
	}
}

// validateSelfJoinAliases checks that self-join queries (same table used multiple
// times with different aliases) have BuildTableAlias set on the appropriate join stages.
func validateSelfJoinAliases(t *testing.T, stages []Stage, queryName string, requiredAliases []string) {
	t.Helper()

	foundAliases := map[string]bool{}
	for _, s := range stages {
		if s.BuildTableAlias != "" {
			foundAliases[s.BuildTableAlias] = true
		}
	}

	for _, alias := range requiredAliases {
		if !foundAliases[alias] {
			t.Errorf("%s: expected BuildTableAlias=%q on a join stage, but not found. Found: %v",
				queryName, alias, foundAliases)
		}
	}
}

// TPC-H queries for plan validation (subset most relevant to distributed correctness).
var tpchPlanQueries = map[string]string{
	"Q02": `SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
		FROM part JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE' AND p_size = 15 AND p_type LIKE '%BRASS'
		AND ps_supplycost = (
			SELECT MIN(ps_supplycost) FROM partsupp
			JOIN supplier ON s_suppkey = ps_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			JOIN region ON n_regionkey = r_regionkey
			WHERE ps_partkey = p_partkey AND r_name = 'EUROPE'
		)
		ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100`,

	"Q07": `SELECT n1.n_name as supp_nation, n2.n_name as cust_nation,
		SUBSTR(l_shipdate, 1, 4) as l_year,
		SUM(l_extendedprice * (1 - l_discount)) as revenue
		FROM supplier JOIN lineitem ON s_suppkey = l_suppkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN customer ON c_custkey = o_custkey
		JOIN nation n1 ON s_nationkey = n1.n_nationkey
		JOIN nation n2 ON c_nationkey = n2.n_nationkey
		WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
			AND l_shipdate >= '1995-01-01' AND l_shipdate <= '1996-12-31'
		GROUP BY n1.n_name, n2.n_name, SUBSTR(l_shipdate, 1, 4)
		ORDER BY supp_nation, cust_nation, l_year`,

	"Q08": `SELECT SUBSTR(o_orderdate, 1, 4) as o_year,
		SUM(CASE WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) as brazil_revenue,
		SUM(l_extendedprice * (1 - l_discount)) as total_revenue
		FROM part JOIN lineitem ON p_partkey = l_partkey
		JOIN supplier ON s_suppkey = l_suppkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN customer ON c_custkey = o_custkey
		JOIN nation n1 ON c_nationkey = n1.n_nationkey
		JOIN region ON n1.n_regionkey = r_regionkey
		JOIN nation n2 ON s_nationkey = n2.n_nationkey
		WHERE r_name = 'AMERICA'
			AND o_orderdate >= '1995-01-01' AND o_orderdate <= '1996-12-31'
			AND p_type = 'ECONOMY ANODIZED STEEL'
		GROUP BY SUBSTR(o_orderdate, 1, 4) ORDER BY o_year`,

	"Q21": `SELECT s_name, COUNT(*) as numwait
		FROM supplier JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
			AND EXISTS (
				SELECT 1 FROM lineitem l2
				WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey
			)
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
					AND l3.l_receiptdate > l3.l_commitdate
			)
		GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100`,
}

func TestTPCHDistributedPlans(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for name, sql := range tpchPlanQueries {
		t.Run(name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, sql, 3)
			if len(stages) == 0 {
				t.Fatal("no stages generated")
			}

			// Validate structural properties
			validateStageGraph(t, stages, name)

			// Log stage summary for debugging
			for _, s := range stages {
				extra := ""
				if s.BuildTableAlias != "" {
					extra = fmt.Sprintf(" buildAlias=%s", s.BuildTableAlias)
				}
				if len(s.ShuffleKeys) > 0 {
					extra += fmt.Sprintf(" shuffleKeys=%v", s.ShuffleKeys)
				}
				if len(s.JoinLeftKeys) > 0 {
					extra += fmt.Sprintf(" leftKeys=%v rightKeys=%v", s.JoinLeftKeys, s.JoinRightKeys)
				}
				if len(s.FilterExprs) > 0 {
					extra += fmt.Sprintf(" filters=%v", s.FilterExprs)
				}
				if s.JoinType != "" {
					extra += fmt.Sprintf(" joinType=%s", s.JoinType)
				}
				if s.JoinFilter != "" {
					extra += fmt.Sprintf(" joinFilter=%q", s.JoinFilter)
				}
				t.Logf("  %-20s type=%-16s tasks=%d deps=%v%s",
					s.ID, s.Type, s.Tasks, s.Dependencies, extra)
			}
		})
	}
}

func TestTPCHSelfJoinAliases(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	tests := []struct {
		name     string
		sql      string
		aliases  []string
	}{
		{
			name:    "Q07_nation_self_join",
			sql:     tpchPlanQueries["Q07"],
			aliases: []string{"n2"},
		},
		{
			name:    "Q08_nation_self_join",
			sql:     tpchPlanQueries["Q08"],
			aliases: []string{"n2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			validateSelfJoinAliases(t, stages, tc.name, tc.aliases)
		})
	}
}

func TestTPCHShuffleKeysResolvable(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	// Build column name set from all tables
	allCols := map[string]bool{}
	for _, schema := range tpchSchemas {
		for _, col := range schema.Columns {
			allCols[col.Name] = true
		}
	}

	for name, sql := range tpchPlanQueries {
		t.Run(name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, sql, 3)

			for _, s := range stages {
				if s.Type != "shuffle" {
					continue
				}
				for _, key := range s.ShuffleKeys {
					// Strip any table qualifier
					cleanKey := key
					if dot := strings.IndexByte(key, '.'); dot >= 0 {
						cleanKey = key[dot+1:]
					}
					if !allCols[cleanKey] {
						t.Errorf("%s: shuffle stage %s has key %q which doesn't match any table column",
							name, s.ID, key)
					}
				}
			}
		})
	}
}
