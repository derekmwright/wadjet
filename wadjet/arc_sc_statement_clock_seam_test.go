// SPDX-License-Identifier: MIT

package wadjet

import (
	"runtime/debug"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
)

func init() { scUnboundClockGuard = scNoUnboundClock }

// scNoUnboundClock fails t if any clock function is compiled with no
// statement clock bound while the test runs — a compile site no door
// reached, which reads the live clock per evaluation.
func scNoUnboundClock(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var first string
	n := 0
	restore := expr.SetUnboundClockHookForTest(func(name string) {
		mu.Lock()
		defer mu.Unlock()
		if n == 0 {
			first = name + "\n" + string(debug.Stack())
		}
		n++
	})
	t.Cleanup(func() {
		restore()
		if n > 0 {
			t.Errorf("%d clock-function compiles had no statement clock bound; the first:\n%s", n, first)
		}
	})
}
