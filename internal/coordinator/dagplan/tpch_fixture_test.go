package dagplan

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The TPC-H-shaped catalog these tests plan against, mirroring
// internal/planner/physical/plan_tpch_test.go.
//
// It is a COPY rather than a shared fixture package because a shared one
// would have to name physical.Stage, and physical's own in-package tests
// would then import a package that imports physical — an import cycle the
// toolchain refuses in a test binary. The fixture is table sizes and
// schemas; it is compared against nothing, so a drift between the two costs
// a different plan shape here, never a wrong assertion about one.

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

func sqlToStages(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string, workerCount int) []physical.Stage {
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
		physical.NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := physical.NewPlanner(cat)
	planner.WorkerCount = workerCount
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}
