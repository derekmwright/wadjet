// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A LAG / LEAD OFFSET AT AND PAST THE INPUT'S EDGE ALLOCATES NOTHING BY THE
// OFFSET (#1399). The spilled empty-PARTITION-BY streamer
// (newGlobalWindowStreamer) kept LAG's look-back in a ring of `offset` slots,
// so `LAG(x, 2147483647)` — and, once a negative offset read the other way,
// `LEAD(x, -2147483647)` — asked for a 2^31-slot []any: a 32 GiB allocation,
// which the Go runtime does not recover from (a fatal error, not a panic), so
// the process died. PostgreSQL answers the default on every row. The ring is
// now allocated only for an offset within the input's row count.
//
// The cells run in a CHILD process under a 4 GiB address-space limit, so a
// regression fails this test (the child's allocation fails and it exits
// non-zero) instead of reaching the host's memory.
func TestArcWALagLeadOffsetPastTheInputAllocatesNoRing(t *testing.T) {
	if os.Getenv("WA_RING_CAP_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0],
			"-test.run=^TestArcWALagLeadOffsetPastTheInputAllocatesNoRing$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), "WA_RING_CAP_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child (4 GiB address-space limit) failed: %v\n%s", err, waTail(out, 4000))
		}
		// Non-vacuous: the child ran the cells, the crash cell among them.
		if passed := strings.Count(string(out), "--- PASS: TestArcWALagLeadOffsetPastTheInputAllocatesNoRing/"); passed != 33 ||
			!strings.Contains(string(out), "--- PASS: TestArcWALagLeadOffsetPastTheInputAllocatesNoRing/spilled/lead/negmax") {
			t.Fatalf("child ran %d cells, want 33\n%s", passed, waTail(out, 4000))
		}
		return
	}
	const limit = 4 << 30
	if err := syscall.Setrlimit(syscall.RLIMIT_AS, &syscall.Rlimit{Cur: limit, Max: limit}); err != nil {
		t.Fatalf("setrlimit: %v", err)
	}

	const n = 240
	rows := waSpillRows(n)
	i64 := parquet.TypeInt64
	byTS := []SortKey{{Column: "ts", Order: Ascending}}
	all := make([]int64, n)
	for i := range all {
		all[i] = int64(i)
	}
	for _, c := range []struct {
		name string
		off  int
	}{
		{"max", 2147483647}, {"negmax", -2147483647},
		{"rows_p1", n + 1}, {"neg_rows_p1", -(n + 1)},
		{"rows", n}, {"neg_rows", -n},
		{"rows_m1", n - 1}, {"neg_rows_m1", -(n - 1)},
	} {
		for _, f := range []struct {
			name string
			fn   WindowFunc
			sign int // LAG reads back: row ts answers row ts - off
		}{{"lag", WinLag, -1}, {"lead", WinLead, 1}} {
			want := func(ts int64) any {
				// The target is past either edge for every offset here
				// except ±(n-1), where exactly one row reaches the other end.
				target := int64(f.sign) * int64(c.off)
				if target > n || target < -n {
					return nil
				}
				return waShift(rows, all, ts, int(target))
			}
			col := []WindowColumn{{Func: f.fn, InputCol: "v", OutputCol: "o", OutputType: i64, OrderBy: byTS, LagLeadOffset: c.off}}
			t.Run("in_memory/"+f.name+"/"+c.name, func(t *testing.T) {
				out, _ := runWindowInMemory(t, waSpillSchema(), col, rows)
				got := map[int64]map[string]any{}
				for _, r := range out {
					got[r["ts"].(int64)] = r
				}
				waCheck(t, got, "o", want)
			})
			t.Run("spilled/"+f.name+"/"+c.name, func(t *testing.T) {
				before := WindowRunsWritten.Load()
				got := waRunSpilled(t, col, rows)
				if WindowRunsWritten.Load() == before {
					t.Fatal("WindowRunsWritten did not move: the streamer was not reached")
				}
				waCheck(t, got, "o", want)
			})
		}
	}
	// A default answers on every row past the edge, spilled.
	t.Run("spilled/lag_default/max", func(t *testing.T) {
		got := waRunSpilled(t, []WindowColumn{{Func: WinLag, InputCol: "v", OutputCol: "o", OutputType: i64,
			OrderBy: byTS, LagLeadOffset: 2147483647, LagLeadDefault: float64(7)}}, rows)
		waCheck(t, got, "o", func(int64) any { return int64(7) })
	})
}

// waTail is the child's output from just before its first fatal error or
// failure (a runtime abort prints every goroutine after it), at most n bytes.
func waTail(b []byte, n int) []byte {
	for _, mark := range []string{"fatal error", "--- FAIL"} {
		if i := strings.Index(string(b), mark); i >= 0 {
			b = b[max(0, i-300):] // the cell that was running
			break
		}
	}
	if len(b) > n {
		return b[:n]
	}
	return b
}
