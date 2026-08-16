package tpch_test

import (
	"context"
	"testing"

	tpch "github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestSelfJoinDisambiguation verifies that self-joins (same table with different
// aliases) correctly disambiguate columns. This was broken by three bugs:
//
//  1. cleanExpr in the plan builder stripped table qualifiers from GROUP BY
//     expressions, so "n1.n_name" and "n2.n_name" both became "n_name" —
//     causing the aggregate to group on one column instead of two.
//
//  2. collectASTColumnRefs stripped table qualifiers from ColRefs, so
//     NeededColumns only had unqualified names, causing OutputFilter to
//     drop qualified columns like "n2.n_name" from join output.
//
//  3. flattenJoinChain matched right-side column refs against left-side
//     relations, misassigning edges in self-join scenarios.
func TestSelfJoinDisambiguation(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	sf := tpch.ScaleFactor(0.01)
	for name, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
	}
	ingesters := make(map[string]*ingest.Ingester)
	for name, schema := range tpch.AllTables {
		ingesters[name] = db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: 100000,
			RowGroupSize:  65536,
		})
	}
	tpch.GenerateChunked(sf, 50000, func(table string, chunk []map[string]any) error {
		return ingesters[table].Ingest(ctx, chunk)
	})
	for name, ing := range ingesters {
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
	}

	t.Run("cross_join_distinct_aliases", func(t *testing.T) {
		result, err := db.Query(ctx,
			"SELECT n1.n_name as sn, n2.n_name as cn FROM nation n1 CROSS JOIN nation n2 "+
				"WHERE n1.n_nationkey = 6 AND n2.n_nationkey = 7")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		sn, cn := result.Rows[0]["sn"], result.Rows[0]["cn"]
		if sn != "FRANCE" || cn != "GERMANY" {
			t.Errorf("expected sn=FRANCE cn=GERMANY, got sn=%v cn=%v", sn, cn)
		}
	})

	t.Run("cross_join_group_by", func(t *testing.T) {
		// GROUP BY on self-join aliases must produce distinct groups
		result, err := db.Query(ctx,
			"SELECT n1.n_name as sn, n2.n_name as cn, COUNT(*) as cnt "+
				"FROM nation n1 CROSS JOIN nation n2 "+
				"GROUP BY n1.n_name, n2.n_name "+
				"ORDER BY cnt DESC LIMIT 5")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(result.Rows) < 2 {
			t.Fatalf("expected multiple groups, got %d", len(result.Rows))
		}
		// With 25 nations × 25 nations, each group has 1 row
		for _, row := range result.Rows {
			cnt := row["cnt"]
			if cnt != int64(1) {
				t.Errorf("expected cnt=1, got cnt=%v (sn=%v cn=%v)", cnt, row["sn"], row["cn"])
			}
		}
	})

	t.Run("multi_way_self_join", func(t *testing.T) {
		result, err := db.Query(ctx,
			"SELECT n1.n_name as sn, n2.n_name as cn, COUNT(*) as cnt "+
				"FROM supplier "+
				"JOIN lineitem ON s_suppkey = l_suppkey "+
				"JOIN orders ON o_orderkey = l_orderkey "+
				"JOIN customer ON c_custkey = o_custkey "+
				"JOIN nation n1 ON s_nationkey = n1.n_nationkey "+
				"JOIN nation n2 ON c_nationkey = n2.n_nationkey "+
				"WHERE n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY' "+
				"GROUP BY n1.n_name, n2.n_name")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		sn, cn := result.Rows[0]["sn"], result.Rows[0]["cn"]
		if sn != "FRANCE" || cn != "GERMANY" {
			t.Errorf("expected sn=FRANCE cn=GERMANY, got sn=%v cn=%v", sn, cn)
		}
	})

	t.Run("Q07_volume_shipping", func(t *testing.T) {
		result, err := db.Query(ctx, tpch.TPCHQueries[7].SQL)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("Q07 returned 0 rows — self-join disambiguation is broken")
		}
		t.Logf("Q07: %d rows", len(result.Rows))
	})
}
