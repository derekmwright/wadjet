// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPoolContentionCompletesBounded is the progress acceptance the
// 2026-11-24 ruling allows (no fairness or winner is claimed): eight
// sessions of one DB run mixed spilling shapes at a pool too small for any
// two of them, some under deadlines that cancel them mid-run. Every
// statement must END — with PostgreSQL's answer, a named budget refusal, or
// its cancellation — within a bound; none may hang on another's bytes, on
// the scan's load wait, or on a relief callback crossing statements (the
// race detector checks the lock order when the package runs under -race).
// The pool is quiescent afterwards.
func TestPoolContentionCompletesBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("8 sessions x 12 statements")
	}
	const sessions, perSession = 8, 12
	db := wideJoinDB(t, 4000, 512, 512, 1<<20)
	g := armPoolGates(1)
	g[0].open()
	// PostgreSQL 17.11 over the same rows (1685e_evidence/pg_oracle.txt).
	shapes := []struct{ sql, want string }{
		{poolJoinSQL(0), poolJoinWant},
		{"SELECT count(*) AS n, sum(c) AS s FROM (SELECT pad, count(k) AS c FROM wide GROUP BY pad) q", "[map[n:4000 s:4000]]"},
		{"SELECT count(*) AS n, max(r) AS m FROM (SELECT row_number() OVER (ORDER BY pad DESC) AS r FROM wide) q", "[map[m:4000 n:4000]]"},
		{"SELECT count(*) AS n, min(k) AS lo FROM (SELECT k FROM wide ORDER BY pad DESC LIMIT 3000) q", "[map[lo:1000 n:3000]]"},
		{"SELECT count(*) AS n FROM wide a CROSS JOIN (SELECT k, pad FROM wide WHERE k < 1000) b WHERE a.k + b.k < 10 AND b.pad > ''", "[map[n:55]]"},
	}
	type outcome struct {
		answered, refused, cancelled int
	}
	var mu sync.Mutex
	out := map[int]*outcome{}
	var wg sync.WaitGroup
	start := time.Now()
	deadline := 5 * time.Minute
	finished := make(chan struct{})
	for s := 0; s < sessions; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < perSession; i++ {
				k := (s + i) % len(shapes)
				ctx := context.Background()
				var cancel context.CancelFunc = func() {}
				if (s+i)%4 == 3 {
					// A deadline that lands mid-run on most shapes.
					ctx, cancel = context.WithTimeout(ctx, time.Duration(5+(s*7+i*3)%40)*time.Millisecond)
				}
				res, err := db.Query(ctx, shapes[k].sql)
				cancel()
				mu.Lock()
				o := out[k]
				if o == nil {
					o = &outcome{}
					out[k] = o
				}
				switch {
				case err == nil && fmt.Sprint(res.Rows) == shapes[k].want:
					o.answered++
				case err == nil:
					t.Errorf("session %d shape %d answered %s, PostgreSQL 17.11 %s", s, k, fmt.Sprint(res.Rows), shapes[k].want)
				case isBudgetRefusal(err):
					o.refused++
				case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
					strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "context canceled"):
					o.cancelled++
				default:
					t.Errorf("session %d shape %d ended with an unnamed error: %v", s, k, err)
				}
				mu.Unlock()
			}
		}(s)
	}
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(deadline):
		t.Fatalf("contention did not complete within %s: a statement hung", deadline)
	}
	for k := range shapes {
		if o := out[k]; o != nil {
			t.Logf("shape %d: answered %d, refused %d, cancelled %d", k, o.answered, o.refused, o.cancelled)
		}
	}
	t.Logf("%d statements in %s", sessions*perSession, time.Since(start).Round(time.Millisecond))
	assertDBPoolQuiescent(t, db)
}
