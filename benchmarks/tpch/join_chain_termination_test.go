package tpch

import (
	"context"
	"strings"
	"testing"
	"time"
)

// f2ChainSQL is the fuzzer's seed-686 shape: a SIX-WAY aggregate-free join
// chain, `extra` conjuncts folded into its WHERE.
func f2ChainSQL(extra string) string {
	return `SELECT t3.l_receiptdate AS c1, LENGTH(l_comment) AS c2, "l_commitdate" AS c3, ` +
		`t0.ps_partkey AS c4, t0.ps_suppkey AS c5, t1.s_suppkey AS c6, t2.n_nationkey AS c7, ` +
		`t3.l_orderkey AS c8, t3.l_linenumber AS c9, t4.c_custkey AS c10, t5.o_orderkey AS c11 ` +
		`FROM partsupp t0 JOIN supplier t1 ON t0.ps_suppkey = t1.s_suppkey ` +
		`JOIN nation t2 ON t1.s_nationkey = t2.n_nationkey ` +
		`JOIN lineitem t3 ON t1.s_suppkey = t3.l_suppkey ` +
		`JOIN customer t4 ON t2.n_nationkey = t4.c_nationkey ` +
		`JOIN orders t5 ON t4.c_custkey = t5.o_custkey ` +
		`WHERE t3.l_returnflag LIKE 'N%'` + extra + ` ` +
		`ORDER BY LENGTH(l_comment), t0.ps_partkey, t0.ps_suppkey, t1.s_suppkey, t2.n_nationkey, ` +
		`t3.l_orderkey, t3.l_linenumber, t4.c_custkey, t5.o_orderkey LIMIT 15 OFFSET 2`
}

// A six-way aggregate-free join chain TERMINATES, and while it runs it is
// CANCELLABLE (#624).
//
// #624 was filed as "stalls/deadlocks" from a 2,000-seed fuzz run, and a later
// comment on the filing downgraded it to CPU contention. Neither reading is
// what this measures. On a quiet box the seed-686 query does not finish in 60
// seconds — three runs of three — and it is not blocked either: two goroutine
// dumps eight seconds apart show ONE query goroutine, RUNNING, inside
// `batch.(*Vector).SetValue` under `exec.(*Project).Execute` at the top of an
// eleven-deep `ChainDriver.push`, on a DIFFERENT row index in each dump.
// Nothing is parked on a channel or a mutex.
//
// What it is, measured (`SELECT COUNT(*)` over each prefix of the chain, SF0.01):
//
//	partsupp x supplier                    8,000        9 ms
//	  + nation                             8,000        8 ms
//	  + lineitem   (on s_suppkey)      2,431,760       34 ms
//	  + customer   (on n_nationkey)  144,180,080      978 ms
//	  + orders     (on c_custkey)     ~1.4 billion
//
// Every join key in the tail is weak — a supplier has ~600 lineitems, a nation
// ~60 customers, a customer ~10 orders — so the chain's output before the
// LIMIT is about 1.4e9 rows, each of them projected (`LENGTH(l_comment)` per
// row) and fed to a nine-key top-K sort. No join ORDER avoids it: the final
// cardinality is a property of the predicates, not of the plan. So there is no
// stall to fix and no threshold to tune, and this gate deliberately asserts
// neither a wall clock nor a row rate.
//
// What it does assert is the pair that a deadlock would break and a slow query
// would not:
//
//   - the same six-way chain, bounded so its output is small, ANSWERS;
//   - the unbounded one, given a deadline, comes back WITH that deadline
//     instead of hanging — which is what a parked pipeline could not do.
func TestASixWayJoinChainTerminatesAndCancels(t *testing.T) {
	db := fuzzSetupEmbedded(t)

	t.Run("bounded/answers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		// One supplier, an order-key window and a customer-key window: the
		// same six relations, the same eleven output columns, the same
		// nine-key ORDER BY, an output the engine can finish. A deadlock does
		// not care how many rows there are.
		sql := f2ChainSQL(` AND t1.s_suppkey = 1 AND t3.l_orderkey < 500 AND t4.c_custkey < 30`)
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("the bounded six-way chain failed: %v\n  SQL: %s", err, sql)
		}
		if len(res.Rows) == 0 {
			t.Fatalf("the bounded six-way chain returned no rows, so this cell asserts nothing"+
				"\n  SQL: %s", sql)
		}
		// The chain really is six relations deep: every relation contributes
		// an output column, and each one must carry a value.
		for _, col := range []string{"c4", "c6", "c7", "c8", "c10", "c11"} {
			if res.Rows[0][col] == nil {
				t.Errorf("output column %q is NULL on an INNER chain — a relation dropped out",
					col)
			}
		}
	})

	t.Run("unbounded/cancels", func(t *testing.T) {
		// The filing's own query. It asks for ~1.4e9 join rows, so it is not
		// expected to finish; what is asserted is that the deadline reaches
		// the operator chain and the call RETURNS. A parked pipeline would
		// still be parked when this test's own timeout fired.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := db.Query(ctx, f2ChainSQL(""))
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				// A legal outcome if the engine ever gets fast enough. It is
				// not a failure; it is the day this cell stops being about
				// cancellation, and it says so rather than passing silently.
				t.Log("the unbounded chain ANSWERED inside 5s — the cardinality this cell " +
					"is about has changed; re-measure the prefix counts in this file's comment")
				return
			}
			if !strings.Contains(err.Error(), "context deadline exceeded") {
				t.Fatalf("the unbounded chain failed with something other than its deadline: %v", err)
			}
		case <-time.After(90 * time.Second):
			t.Fatal("the unbounded six-way chain did not return 90s after a 5s deadline: " +
				"the pipeline is not observing cancellation")
		}
	})
}
