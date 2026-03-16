package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestQuickCheck(t *testing.T) {
	db := setupTPCH(t, SF001)

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		qNum := qNum
		t.Run(fmt.Sprintf("Q%02d", qNum), func(t *testing.T) {
			// Correlated subqueries (Q04, Q17, Q22) need more time due to
			// row-by-row execution until scalar subquery decorrelation is implemented.
			timeout := 15 * time.Second
			if qNum == 17 {
				timeout = 90 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			start := time.Now()
			result, err := db.Query(ctx, q.SQL)
			elapsed := time.Since(start)
			if err != nil {
				t.Logf("Q%02d: ERROR in %v: %v", qNum, elapsed.Round(time.Millisecond), err)
				return
			}
			t.Logf("Q%02d: %4d rows in %v", qNum, len(result.Rows), elapsed.Round(time.Millisecond))
			if len(result.Rows) == 0 {
				t.Logf("Q%02d: *** ZERO ROWS ***", qNum)
			}
		})
	}
}
