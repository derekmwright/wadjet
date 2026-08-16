package clickbench

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestHitsProfileOne runs a single ClickBench query repeatedly over the
// staged part so `go test -cpuprofile/-memprofile` captures a clean
// hotspot picture for that query alone. Query selection and repetition
// come from env so the harness needs no code change per investigation:
//
//	WADJET_HITS_PART=hits_0.parquet WADJET_HITS_QN=19 WADJET_HITS_REPS=20 \
//	  go test -run TestHitsProfileOne -cpuprofile q19.pprof ./benchmarks/clickbench/
//
// Skips when WADJET_HITS_QN is unset so the normal suite ignores it.
func TestHitsProfileOne(t *testing.T) {
	qnStr := os.Getenv("WADJET_HITS_QN")
	if qnStr == "" {
		t.Skip("WADJET_HITS_QN not set")
	}
	qn, err := strconv.Atoi(qnStr)
	if err != nil {
		t.Fatalf("WADJET_HITS_QN: %v", err)
	}
	reps := 10
	if r := os.Getenv("WADJET_HITS_REPS"); r != "" {
		if reps, err = strconv.Atoi(r); err != nil {
			t.Fatalf("WADJET_HITS_REPS: %v", err)
		}
	}

	ctx := context.Background()
	db, _ := openHitsDB(t, ctx)
	var q string
	if sql := os.Getenv("WADJET_HITS_SQL"); sql != "" {
		q = sql
	} else {
		queries := loadHitsQueries(t)
		if qn < 1 || qn > len(queries) {
			t.Fatalf("query %d out of range 1..%d", qn, len(queries))
		}
		q = queries[qn-1]
	}

	// One untimed warmup so first-touch decode noise stays out of reps.
	res, err := db.Query(ctx, q)
	if err != nil {
		t.Fatalf("warmup: %v", err)
	}
	if len(res.Rows) <= 5 {
		for _, r := range res.Rows {
			t.Logf("row: %v", r)
		}
	}
	start := time.Now()
	for i := 0; i < reps; i++ {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("rep %d: %v", i, err)
		}
	}
	el := time.Since(start)
	t.Logf("Q%02d: %d reps in %s (%.1f ms/query)", qn, reps, el, float64(el.Milliseconds())/float64(reps))
}
