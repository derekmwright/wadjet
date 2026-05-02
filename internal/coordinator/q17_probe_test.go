//go:build !race

// This file uses setupTPCHDistributedAtScale which lives in
// distributed_tpch_test.go and is gated by the same !race build tag —
// the embedded NATS + heavy data generation makes -race time out.
// Mirror the constraint here.

package coordinator

import (
	"context"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestQ17DistributedProbe is diagnostic infrastructure for the long-
// standing TestDistributedTPCHQ17AggregateShuffleCorrectness failure:
// distributed Q17 at SF0.1 returns avg_yearly = 476997 (in-plan) or
// 518515 (aggregate-shuffle), neither matching the post-fix single-
// process value of 552517.40 (see TestQ17SingleProcessSF01).
//
// What this probe pins down (gated by WADJET_Q17_PROBE=1):
//
//   - Plain SELECT COUNT(*) FROM lineitem and SELECT SUM(l_quantity)
//     return identical totals in single-process and distributed mode
//     (599,513 rows / 15,304,695 quantity). Scan reads all rows.
//
//   - But SELECT l_partkey, COUNT(*) FROM lineitem GROUP BY l_partkey
//     loses ~4% of rows in distributed: SUM(cnt) over the per-partkey
//     groups drops from 599,513 (single-process) to ~572K-577K
//     (distributed; varies ~5K row-to-row). The 20,000 distinct
//     partkeys are all present, but per-partkey counts are short.
//
//   - Looking up a SINGLE partkey with WHERE l_partkey = K returns the
//     correct count — so scan + filter is fine. The bug only surfaces
//     when GROUP BY l_partkey runs through the full
//     scan → partial-aggregate → shuffle → final-aggregate pipeline.
//
//   - ORDER BY cnt LIMIT 5 is also wrong in distributed (returns
//     partkeys with cnt in the 20s/30s instead of the global minimum
//     12). Probably a separate top-K-per-partition merge issue.
//
// Next-session investigation should look at the partial-aggregate stage
// (per-chunk HashAggregate inside each scan task) and the shuffle path
// — whether some partial rows are dropped before reaching final, or
// whether the partial HashAggregate's parallel-merge has a bug similar
// to the in-process ones fixed earlier in this session that doesn't
// affect single-task / single-key paths.
func TestQ17DistributedProbe(t *testing.T) {
	if os.Getenv("WADJET_Q17_PROBE") != "1" {
		t.Skip("set WADJET_Q17_PROBE=1 to enable (heavy: SF0.1 datagen)")
	}

	// Single-process at SF0.1 — ground truth.
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "sp"})
	if err != nil {
		t.Fatal(err)
	}
	data := tpch.Generate(tpch.SF01)
	for tableName, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatal(err)
		}
		rows := data[tableName]
		ing := db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1, RowGroupSize: max(100, len(rows)/4),
		})
		_ = ing.Ingest(ctx, rows)
		_ = ing.FlushAll(ctx)
	}

	probes := []struct {
		name string
		sql  string
	}{
		// Sanity: lineitem total
		{"li_count", `SELECT COUNT(*) AS n FROM lineitem`},
		{"li_sum_qty", `SELECT SUM(l_quantity) AS s, COUNT(*) AS n FROM lineitem`},
		// Per-partkey count + sum (no AVG — just SUM and COUNT)
		{"per_pk_summary", `SELECT MIN(c) AS min_c, MAX(c) AS max_c, SUM(c) AS sum_c, MIN(s) AS min_s, MAX(s) AS max_s, SUM(s) AS sum_s FROM (SELECT l_partkey, COUNT(*) AS c, SUM(l_quantity) AS s FROM lineitem GROUP BY l_partkey) sub`},
		// Inner aggregate alone — the decorrelated subquery target.
		{"inner_agg_count", `SELECT COUNT(*) AS n FROM (SELECT l_partkey, AVG(l_quantity) AS avg FROM lineitem GROUP BY l_partkey) sub`},
		{"inner_agg_summary", `SELECT MIN(avg) AS min_avg, MAX(avg) AS max_avg, SUM(avg) AS sum_avg FROM (SELECT l_partkey, AVG(l_quantity) AS avg FROM lineitem GROUP BY l_partkey) sub`},
		// One specific partkey
		{"pk_5_avg", `SELECT l_partkey, AVG(l_quantity) AS avg, SUM(l_quantity) AS sum, COUNT(*) AS cnt FROM lineitem WHERE l_partkey = 5 GROUP BY l_partkey`},
		// Show partkeys with smallest counts
		{"pk_low_cnt", `SELECT l_partkey, COUNT(*) AS cnt FROM lineitem GROUP BY l_partkey ORDER BY cnt LIMIT 5`},
		// Distinct partkey counts (histogram)
		{"distinct_cnts", `SELECT cnt, COUNT(*) AS num_pks FROM (SELECT l_partkey, COUNT(*) AS cnt FROM lineitem GROUP BY l_partkey) sub GROUP BY cnt ORDER BY cnt LIMIT 10`},
		// Lookup a few specific partkeys
		{"pk_18978", `SELECT l_partkey, COUNT(*) AS cnt FROM lineitem WHERE l_partkey = 18978 GROUP BY l_partkey`},
		{"pk_11904", `SELECT l_partkey, COUNT(*) AS cnt FROM lineitem WHERE l_partkey = 11904 GROUP BY l_partkey`},
		// Total via per-partkey nested
		{"sum_via_pk_groups", `SELECT SUM(cnt) AS total_cnt FROM (SELECT l_partkey, COUNT(*) AS cnt FROM lineitem GROUP BY l_partkey) sub`},
		// Different group: nation_count via SUPPLIER
		{"supplier_check", `SELECT COUNT(*) AS n FROM (SELECT l_suppkey, COUNT(*) AS c FROM lineitem GROUP BY l_suppkey) sub`},
		// Without a GROUP BY, aggregate works:
		{"li_count_again", `SELECT COUNT(*) AS n FROM lineitem`},
	}
	for _, p := range probes {
		res, err := db.Query(ctx, p.sql)
		if err != nil {
			t.Logf("ERR sp/%s: %v", p.name, err)
			continue
		}
		for i, r := range res.Rows {
			if i >= 5 {
				break
			}
			t.Logf("sp/%s[%d]: %v", p.name, i, r)
		}
	}
	db.Close()

	// Distributed at SF0.1 — same probes against the coordinator.
	dctx, coord := setupTPCHDistributedAtScale(t, tpch.SF01)
	for _, p := range probes {
		res, err := coord.ExecuteSQL(dctx, p.sql)
		if err != nil {
			t.Logf("ERR dist/%s: %v", p.name, err)
			continue
		}
		if res.Error != "" {
			t.Logf("ERR dist/%s: %s", p.name, res.Error)
			continue
		}
		rows := res.Rows()
		for i, r := range rows {
			if i >= 5 {
				break
			}
			t.Logf("dist/%s[%d]: %v", p.name, i, r)
		}
	}
}
