package physical

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

var updateGolden = flag.Bool("update-golden", false, "update TPC-H ensure-distribution golden snapshots")

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
		if s.Type == StageExchangeRepartition {
			if s.Exchange == nil {
				t.Errorf("%s: repartition stage %s has nil Exchange payload", queryName, s.ID)
			} else {
				if len(s.Exchange.Keys) == 0 {
					t.Errorf("%s: shuffle stage %s has no shuffle keys", queryName, s.ID)
				}
				if s.Exchange.Count < 2 {
					t.Errorf("%s: shuffle stage %s has <2 partitions: %d", queryName, s.ID, s.Exchange.Count)
				}
			}
		}

		// Join stages with shuffle should have matching partition counts
		if (s.Type == "hash_join" || s.Type == "broadcast_join") && s.JoinPartitionCount > 1 {
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
				if s.Exchange != nil && len(s.Exchange.Keys) > 0 {
					extra += fmt.Sprintf(" shuffleKeys=%v", s.Exchange.Keys)
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

// tpchPlanQueryMap contains all 22 TPC-H queries for routing validation.
var tpchPlanQueryMap = map[int]string{
	1: `SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, AVG(l_quantity) as avg_qty, AVG(l_extendedprice) as avg_price, AVG(l_discount) as avg_disc, COUNT(*) as count_order FROM lineitem WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus`,
	2: tpchPlanQueries["Q02"],
	3: `SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey WHERE c_mktsegment = 'BUILDING' AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10`,
	4: `SELECT o_orderpriority, COUNT(*) as order_count FROM orders WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01' AND EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority`,
	5: `SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey WHERE c_nationkey = s_nationkey AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01' GROUP BY n_name ORDER BY revenue DESC`,
	6: `SELECT SUM(l_extendedprice * l_discount) as revenue FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24`,
	7: tpchPlanQueries["Q07"],
	8: tpchPlanQueries["Q08"],
	9: `SELECT n_name as nation, SUBSTR(o_orderdate, 1, 4) as o_year, SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit FROM part JOIN lineitem ON p_partkey = l_partkey JOIN supplier ON s_suppkey = l_suppkey JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey JOIN orders ON o_orderkey = l_orderkey JOIN nation ON s_nationkey = n_nationkey WHERE p_name LIKE '%green%' GROUP BY n_name, SUBSTR(o_orderdate, 1, 4) ORDER BY nation, o_year DESC`,
	10: `SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey JOIN nation ON c_nationkey = n_nationkey WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' AND l_returnflag = 'R' GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20`,
	11: `SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'GERMANY') ORDER BY value DESC`,
	12: `SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count, SUM(CASE WHEN o_orderpriority != '1-URGENT' AND o_orderpriority != '2-HIGH' THEN 1 ELSE 0 END) as low_line_count FROM orders JOIN lineitem ON o_orderkey = l_orderkey WHERE l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode`,
	13: `SELECT c_custkey, COUNT(o_orderkey) as c_count FROM customer LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey ORDER BY c_count DESC, c_custkey LIMIT 100`,
	14: `SELECT SUM(CASE WHEN p_type LIKE 'PROMO%%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) as promo_revenue, SUM(l_extendedprice * (1 - l_discount)) as total_revenue FROM lineitem JOIN part ON l_partkey = p_partkey WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'`,
	15: `WITH revenue AS (SELECT l_suppkey as supplier_no, SUM(l_extendedprice * (1 - l_discount)) as total_revenue FROM lineitem WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01' GROUP BY l_suppkey) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier JOIN revenue ON s_suppkey = supplier_no WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue) ORDER BY s_suppkey`,
	16: `SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) as supplier_cnt FROM partsupp JOIN part ON p_partkey = ps_partkey WHERE p_brand != 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size`,
	17: `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)`,
	18: `SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) as total_qty FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100`,
	19: `SELECT SUM(l_extendedprice * (1 - l_discount)) as revenue FROM lineitem JOIN part ON p_partkey = l_partkey WHERE (p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 11 AND p_size >= 1 AND p_size <= 5 AND l_shipmode IN ('AIR', 'REG AIR') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG') AND l_quantity >= 10 AND l_quantity <= 20 AND p_size >= 1 AND p_size <= 10 AND l_shipmode IN ('AIR', 'REG AIR') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 30 AND p_size >= 1 AND p_size <= 15 AND l_shipmode IN ('AIR', 'REG AIR') AND l_shipinstruct = 'DELIVER IN PERSON')`,
	20: `SELECT s_name, s_address FROM supplier JOIN nation ON s_nationkey = n_nationkey WHERE n_name = 'CANADA' AND s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%%') AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01')) ORDER BY s_name`,
	21: tpchPlanQueries["Q21"],
	22: `SELECT SUBSTR(c_phone, 1, 2) as cntrycode, COUNT(*) as numcust, SUM(c_acctbal) as totacctbal FROM customer WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey) GROUP BY SUBSTR(c_phone, 1, 2) ORDER BY cntrycode`,
}

// TestTPCHRoutingDecisions verifies that every TPC-H query routes to a sane
// execution path at SF10 scale. This catches silent regressions where routing
// simplifications cause queries to take a worse path (e.g., single-worker for
// a 6GB multi-join query, or distributed for a 50MB scan).
//
// The test generates physical stages for each query and simulates the
// coordinator's three-way routing decision:
//   1. Probe-split pipeline (preferred for join-heavy queries)
//   2. Single-worker pipeline (small data or high shuffle overhead)
//   3. Full distributed multi-stage (large data with shuffles)
func TestTPCHRoutingDecisions(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	workerCount := 3

	// routingPath describes which execution path the coordinator would choose.
	type routingPath string
	const (
		probeSplit   routingPath = "probe-split"
		singleWorker routingPath = "single-worker"
	)

	// classifyRoute simulates the coordinator's routing decision.
	// All queries are either probe-split or single-worker pipeline.
	classifyRoute := func(stages []Stage, logicalPlan *logical.Node) routingPath {
		_, _, canProbe := CanProbeSplit(stages, workerCount)
		mergeInfo := logical.ExtractMergeInfo(logicalPlan)

		if canProbe && mergeInfo != nil {
			return probeSplit
		}
		return singleWorker
	}

	// Each test case specifies the expected routing path and constraints.
	tests := []struct {
		qNum     int
		sql      string
		expected routingPath
		reason   string // why this routing is correct
	}{
		// No joins — single scan + aggregate. Probe-split partitions lineitem
		// files across workers, each aggregates its chunk, coordinator re-aggregates.
		{1, "", probeSplit, "scan-aggregate: partition lineitem, re-aggregate partials"},
		{6, "", probeSplit, "scan-aggregate: partition lineitem, re-aggregate partials"},

		// 1-2 joins with large probe table → probe-split
		{3, "", probeSplit, "lineitem-orders join, lineitem as probe"},
		{5, "", probeSplit, "5-way join, largest table as probe"},
		{10, "", probeSplit, "customer-orders-lineitem, lineitem as probe"},
		{12, "", probeSplit, "lineitem-orders join, lineitem as probe"},
		{14, "", probeSplit, "lineitem-part join, lineitem as probe"},

		// Semi/anti joins — build side excluded from probe, but outer can still probe
		{4, "", probeSplit, "orders with EXISTS lineitem semi-join, orders as probe"},

		// Multi-way joins (3+) with large data → distributed or probe-split
		{2, "", probeSplit, "5-way join with correlated subquery"},
		{7, "", probeSplit, "self-join on nation, large join chain"},
		{8, "", probeSplit, "8-way join, largest scan as probe"},
		{9, "", probeSplit, "5-way join, lineitem as probe"},

		// Queries that may route to probe-split or distributed depending on stage shape
		{11, "", probeSplit, "partsupp-supplier-nation join"},
		{13, "", probeSplit, "customer LEFT JOIN orders"},
		{15, "", probeSplit, "lineitem aggregate with supplier join"},
		{16, "", probeSplit, "part-partsupp with NOT IN subquery"},
		{17, "", probeSplit, "lineitem-part with scalar subquery"},
		{18, "", probeSplit, "customer-orders-lineitem with IN subquery"},
		{19, "", probeSplit, "lineitem-part with OR conditions"},
		{20, "", probeSplit, "supplier with EXISTS on partsupp-lineitem"},

		// Complex semi/anti join chains
		{21, "", probeSplit, "supplier-lineitem with EXISTS+NOT EXISTS"},
		{22, "", probeSplit, "anti-join excludes orders as build, customer is probe"},
	}

	for _, tc := range tests {
		name := fmt.Sprintf("Q%02d", tc.qNum)
		t.Run(name, func(t *testing.T) {
			// Get the actual TPC-H SQL
			sql := tc.sql
			if sql == "" {
				qDef, ok := tpchPlanQueryMap[tc.qNum]
				if !ok {
					t.Skipf("Q%02d not in plan query map", tc.qNum)
					return
				}
				sql = qDef
			}

			// Build logical plan
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

			// Generate physical stages
			planner := NewPlanner(cat)
			planner.WorkerCount = workerCount
			stages, err := planner.PlanDistributed(ctx, logicalPlan)
			if err != nil {
				t.Fatalf("plan distributed: %v", err)
			}

			// Classify routing
			got := classifyRoute(stages, logicalPlan)

			// Log stage summary for debugging failures
			var totalScanBytes int64
			joinCount := 0
			shuffleCount := 0
			for _, s := range stages {
				if s.Type == "scan" {
					totalScanBytes += s.EstimatedBytes
				}
				if s.Type == "hash_join" || s.Type == "broadcast_join" {
					joinCount++
				}
				joinCount += len(s.FusedJoins)
				if s.Type == StageExchangeRepartition {
					shuffleCount++
				}
			}
			t.Logf("route=%-14s joins=%d shuffles=%d scanMB=%d stages=%d",
				got, joinCount, shuffleCount, totalScanBytes>>20, len(stages))

			if got != tc.expected {
				// Log full stage detail on mismatch
				for _, s := range stages {
					extra := ""
					if s.JoinType != "" {
						extra = fmt.Sprintf(" joinType=%s", s.JoinType)
					}
					if s.ScanAlias != "" {
						extra += fmt.Sprintf(" alias=%s", s.ScanAlias)
					}
					t.Logf("  %-20s type=%-16s bytes=%dMB files=%d%s",
						s.ID, s.Type, s.EstimatedBytes>>20, len(s.ScanFiles), extra)
				}
				t.Errorf("Q%02d: got %s, want %s (%s)", tc.qNum, got, tc.expected, tc.reason)
			}
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
				if s.Type != StageExchangeRepartition {
					continue
				}
				if s.Exchange == nil {
					continue
				}
				for _, key := range s.Exchange.Keys {
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

// TestTPCHDistributionConsistency is the acceptance gate for the
// distribution-property pass on the production planning path
// (UseEnsureDistribution=true). For every Q1-Q22, runs PlanDistributed with
// WorkerCount=4 in strict mode (BehaviorPreservingMode=false) and asserts:
//  1. Every stage has a populated Distribution (shuffle stages explicitly
//     DistHashPartitioned with non-zero Count).
//  2. AssertExchangeConsistency(stages) == nil.
//
// Failure on any query means either the OutputDistribution /
// RequiredChildDistribution rules are wrong or PlanDistributed emits an
// inconsistent plan. Either way, the spec's load-bearing invariant is
// broken.
//
// Native-DAG unification (2026-04-25) made UseEnsureDistribution the only
// runtime path, so the acceptance gate runs against that path. The base
// planner's pre-rewrite output is no longer a runtime configuration.
//
// Spec: docs/archive/specs/2026-04-20-distribution-property-phase-1.md
func TestTPCHDistributionConsistency(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	// Strict mode: any consistency violation must surface as an error.
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = false
	defer func() { BehaviorPreservingMode = prev }()

	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			t.Logf("Q%02d not in plan query map; skipping", qNum)
			continue
		}
		name := fmt.Sprintf("Q%02d", qNum)
		t.Run(name, func(t *testing.T) {
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
			planner.WorkerCount = 4
			stages, err := planner.PlanDistributed(ctx, logicalPlan)
			if err != nil {
				// In strict mode, exchange-consistency violations come back
				// as errors prefixed "exchange consistency: ...".
				t.Fatalf("PlanDistributed failed: %v", err)
			}

			// Assertion 1: every stage has a populated Distribution. The
			// zero value DistSingleton is "populated" for stages that emit
			// singleton output, but shuffle stages must carry the
			// hash-partitioned label with non-zero Count and non-empty Keys.
			for _, s := range stages {
				if s.Type == StageExchangeRepartition {
					if s.Distribution.Kind != DistHashPartitioned {
						t.Errorf("%s shuffle stage %s: Distribution.Kind = %v, want DistHashPartitioned",
							name, s.ID, s.Distribution.Kind)
					}
					if s.Distribution.Count == 0 {
						t.Errorf("%s shuffle stage %s: Distribution.Count = 0 (not populated)",
							name, s.ID)
					}
					if len(s.Distribution.Keys) == 0 {
						t.Errorf("%s shuffle stage %s: Distribution.Keys empty (not populated)",
							name, s.ID)
					}
				}
			}

			// Assertion 2: in strict mode, AssertExchangeConsistency
			// already ran inside PlanDistributed — if it returned an
			// error it would have aborted above. Re-run defensively to
			// log per-stage detail on failure.
			if err := AssertExchangeConsistency(stages); err != nil {
				for _, s := range stages {
					t.Logf("  %-24s type=%-16s dist=%+v",
						s.ID, s.Type, s.Distribution)
				}
				t.Fatalf("%s: %v", name, err)
			}
		})
	}
}

// TestPlanDistributed_InsertsExchanges verifies that EnsureDistribution runs
// during PlanDistributed (always-on under native-DAG). Q01 is the simplest
// wiring correctness gate; Q05 is then checked to confirm at least one
// StageExchange* stage is present, since it has joins large enough to
// require exchange insertion.
func TestPlanDistributed_InsertsExchanges(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	buildLogicalPlan := func(t *testing.T, sql string) *logical.Node {
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
		return logicalPlan
	}

	t.Run("Q01_wiring", func(t *testing.T) {
		// Simple wiring test: PlanDistributed must not error on Q01.
		planner := NewPlanner(cat)
		planner.WorkerCount = 4

		node := buildLogicalPlan(t, tpchPlanQueryMap[1])
		_, err := planner.PlanDistributed(ctx, node)
		if err != nil {
			t.Fatalf("PlanDistributed Q01 failed: %v", err)
		}
	})

	t.Run("Q05_exchange_inserted", func(t *testing.T) {
		// Q05 has multiple joins large enough to trigger exchange insertion.
		planner := NewPlanner(cat)
		planner.WorkerCount = 4

		node := buildLogicalPlan(t, tpchPlanQueryMap[5])
		stages, err := planner.PlanDistributed(ctx, node)
		if err != nil {
			t.Fatalf("PlanDistributed Q05 failed: %v", err)
		}
		var sawExchange bool
		for _, s := range stages {
			if s.Type == StageExchangeRepartition || s.Type == StageExchangeReplicate || s.Type == StageExchangeGather {
				sawExchange = true
				break
			}
		}
		if !sawExchange {
			t.Fatal("expected at least one StageExchange* stage")
		}
	})
}

// TestTPCH_EnsureDistribution_PlannerParity is the Phase 2 acceptance gate.
// For every Q01-Q22, it plans with UseEnsureDistribution=true and asserts:
//  1. Plan succeeds (no error).
//  2. AssertExchangeConsistency passes (guaranteed by PlanDistributed in strict
//     mode, verified defensively here).
//  3. Every StageExchangeRepartition has Distribution.Kind == DistHashPartitioned,
//     every StageExchangeReplicate has DistBroadcast, every StageExchangeGather
//     has DistSingleton.
//
// Spec: docs/archive/specs/2026-04-20-distribution-property-phase-2.md
func TestTPCH_EnsureDistribution_PlannerParity(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	buildLogicalPlanEnsure := func(t *testing.T, sql string) *logical.Node {
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
		return logicalPlan
	}

	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			t.Logf("Q%02d not in plan query map; skipping", qNum)
			continue
		}
		name := fmt.Sprintf("Q%02d", qNum)
		t.Run(name, func(t *testing.T) {
			node := buildLogicalPlanEnsure(t, sql)

			planner := NewPlanner(cat)
			planner.WorkerCount = 4

			// Assertion 1: plan succeeds.
			stages, err := planner.PlanDistributed(ctx, node)
			if err != nil {
				t.Fatalf("PlanDistributed with UseEnsureDistribution=true failed: %v", err)
			}

			// Assertion 2: AssertExchangeConsistency passes (strict mode).
			// PlanDistributed already runs this in strict mode when
			// UseEnsureDistribution=true; we re-run defensively to emit
			// per-stage detail on any failure.
			if err := AssertExchangeConsistency(stages); err != nil {
				for _, s := range stages {
					t.Logf("  %-24s type=%-20s dist=%+v", s.ID, s.Type, s.Distribution)
				}
				t.Fatalf("AssertExchangeConsistency failed: %v", err)
			}

			// Assertion 3: exchange stage Distribution.Kind must match the
			// stage type exactly.
			var exchangeCount int
			for _, s := range stages {
				switch s.Type {
				case StageExchangeRepartition:
					exchangeCount++
					if s.Distribution.Kind != DistHashPartitioned {
						t.Errorf("StageExchangeRepartition %s: Distribution.Kind = %v, want DistHashPartitioned",
							s.ID, s.Distribution.Kind)
					}
				case StageExchangeReplicate:
					exchangeCount++
					if s.Distribution.Kind != DistBroadcast {
						t.Errorf("StageExchangeReplicate %s: Distribution.Kind = %v, want DistBroadcast",
							s.ID, s.Distribution.Kind)
					}
				case StageExchangeGather:
					exchangeCount++
					if s.Distribution.Kind != DistSingleton {
						t.Errorf("StageExchangeGather %s: Distribution.Kind = %v, want DistSingleton",
							s.ID, s.Distribution.Kind)
					}
				}
			}

			// Log summary for debugging.
			t.Logf("stages=%d exchanges=%d", len(stages), exchangeCount)
			for _, s := range stages {
				if s.Type == StageExchangeRepartition || s.Type == StageExchangeReplicate || s.Type == StageExchangeGather {
					t.Logf("  %-24s type=%-20s dist=%+v deps=%v", s.ID, s.Type, s.Distribution, s.Dependencies)
				}
			}
		})
	}
}

// distSummary returns a stable single-word description of a Distribution for
// use in golden snapshot files.
func distSummary(d Distribution) string {
	switch d.Kind {
	case DistSingleton:
		return "Singleton"
	case DistBroadcast:
		return "Broadcast"
	case DistHashPartitioned:
		return fmt.Sprintf("Hash(%s)/%d", strings.Join(d.Keys, ","), d.Count)
	case DistRoundRobin:
		return "RoundRobin"
	}
	return "Unknown"
}

// TestTPCH_EnsureDistribution_Snapshot records the (stageID, type,
// distribution, dependencies) sequence for every Q01-Q22 plan produced by
// EnsureDistribution. Each query's snapshot is written to
// internal/planner/physical/testdata/ensure_distribution/q<NN>.golden.
//
// Run with -update-golden to regenerate the golden files after intentional
// plan changes.
func TestTPCH_EnsureDistribution_Snapshot(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	buildLogicalPlanSnap := func(t *testing.T, sql string) *logical.Node {
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
		return logicalPlan
	}

	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			t.Logf("Q%02d not in plan query map; skipping", qNum)
			continue
		}
		name := fmt.Sprintf("Q%02d", qNum)
		t.Run(name, func(t *testing.T) {
			node := buildLogicalPlanSnap(t, sql)

			planner := NewPlanner(cat)
			planner.WorkerCount = 4

			stages, err := planner.PlanDistributed(ctx, node)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}

			var buf strings.Builder
			for _, s := range stages {
				fmt.Fprintf(&buf, "%s\t%s\t%s", s.ID, s.Type, distSummary(s.Distribution))
				if len(s.Dependencies) > 0 {
					fmt.Fprintf(&buf, "\tdeps=%s", strings.Join(s.Dependencies, ","))
				}
				buf.WriteByte('\n')
			}

			goldenPath := filepath.Join("testdata", "ensure_distribution", strings.ToLower(name)+".golden")
			if *updateGolden {
				if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(goldenPath, []byte(buf.String()), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update-golden to create)", err)
			}
			if got := buf.String(); got != string(want) {
				t.Errorf("snapshot diff for %s:\n--- want\n%s--- got\n%s", name, want, got)
			}
		})
	}
}
