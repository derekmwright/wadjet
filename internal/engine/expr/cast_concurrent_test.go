// SPDX-License-Identifier: MIT

package expr

import (
	"sync"
	"testing"
)

// TestCastSharedNodeConcurrentEval evaluates ONE Cast node from many
// goroutines at once, the way a parallel filter, a DAG worker or a parallel
// correlated re-run shares one compiled expression. The node's parsed
// destination was resolved lazily by plain field writes published behind an
// atomic.Bool, so two first evaluations wrote the fields concurrently and a
// third read them mid-write — a data race `-race` reports on this test.
//
// Each cell is a destination that carries a cached parse: DECIMAL(p,s), a
// bare NUMERIC and a VARCHAR(n) / CHAR(n) length. Every goroutine must also
// answer the single-threaded value.
func TestCastSharedNodeConcurrentEval(t *testing.T) {
	cells := []struct {
		dest string
		val  any
	}{
		{"decimal(9,2)", "12.7501"},
		{"numeric(18, 4)", int64(42)},
		{"numeric", "3.25"},
		{"varchar(3)", "abcdef"},
		{"char(4)", "ab"},
	}
	const workers = 16
	for _, c := range cells {
		t.Run(c.dest, func(t *testing.T) {
			want := (&Cast{Operand: &Lit{Val: c.val}, DestType: c.dest}).Eval(nil, 0)
			shared := &Cast{Operand: &Lit{Val: c.val}, DestType: c.dest}
			start := make(chan struct{})
			got := make([]any, workers)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					got[w] = shared.Eval(nil, 0)
				}(w)
			}
			close(start)
			wg.Wait()
			for w, g := range got {
				if g != want {
					t.Errorf("worker %d: CAST(%v AS %s) = %#v, want %#v", w, c.val, c.dest, g, want)
				}
			}
		})
	}
}
