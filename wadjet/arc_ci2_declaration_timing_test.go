// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// THE DECLARATION WALK'S WORK IS BOUNDED AT ITS MEASURED DEGREE (ADR-0047
// stage 2; RISKS R5; #1688): the walk was 2^depth and then 5.5^depth in
// derived-table depth before its per-node memo (#1034: 248 s at depth 16), and
// stage 2 re-keyed that memo by position. The walk is NOT linear in depth: it
// is polynomial, measured here at 7,164 / 230,548 / 1,611,956 memo misses at
// depth 16 / 64 / 128 (fitted exponent 2.5 from 16 to 64, 2.8 from 64 to 128;
// each package-level spelling starts a fresh walk, so the misses are summed
// over every walk of the statement). The gate counts those misses (the walk's
// unit of work: one miss is one node's whole declaration; a hit is a lookup)
// through physical.SetDeclWalkProbe and bounds each shape at its measured
// count plus about 5%, so a memo that stopped caching, or a rise in degree,
// fails; elapsed time is logged and never asserted (a 9x wall-clock ratio
// bound sat inside the measured 7x-9.3x noise of the same walk). Each shape
// is planned and answered on the embedded engine, the door that binds: a bound
// expression over a join, read through derived tables nested 16, 64 and 128
// deep; 200 items over 50 grouped keys (339 misses) read through a derived
// table; and a twelve-way join (879 misses) whose items are qualified
// arithmetic over every arm.
func TestArcCI2DeclarationPlanningBound(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	setup := []string{
		"CREATE TABLE ci2_d (id BIGINT, i INT, f DOUBLE)",
		"INSERT INTO ci2_d VALUES (1, 1, 0.5), (2, 2, 1.5)",
	}
	var cols, vals []string
	for i := 0; i < 50; i++ {
		cols = append(cols, fmt.Sprintf("c%d INT", i))
		vals = append(vals, fmt.Sprint(i))
	}
	setup = append(setup, "CREATE TABLE ci2_w ("+strings.Join(cols, ", ")+")",
		"INSERT INTO ci2_w VALUES ("+strings.Join(vals, ", ")+")")
	for j := 0; j < 12; j++ {
		setup = append(setup, fmt.Sprintf("CREATE TABLE ci2_j%d (id INT, v%d DOUBLE)", j, j),
			fmt.Sprintf("INSERT INTO ci2_j%d VALUES (1, %d.5), (2, %d.5)", j, j, j+1))
	}
	for _, q := range setup {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// The walk's work is counted, not timed: every uncached computation of a
	// declaration walk (a memo miss) is one unit, read through
	// physical.SetDeclWalkProbe over the whole Query (plan and answer).
	var mu sync.Mutex
	var misses int
	restore := physical.SetDeclWalkProbe(func(string) { mu.Lock(); misses++; mu.Unlock() })
	t.Cleanup(restore)
	counted := func(name, q, want string, bound int) int {
		t.Helper()
		mu.Lock()
		misses = 0
		mu.Unlock()
		start := time.Now()
		res, err := db.Query(ctx, q)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		mu.Lock()
		n := misses
		mu.Unlock()
		t.Logf("%s misses=%d elapsed=%s (elapsed is logged, never asserted)", name, n, elapsed)
		if n > bound {
			t.Errorf("%s: the declaration walk computed %d times, want <= %d: its cost rose past its measured degree", name, n, bound)
		}
		if got := ssEmbeddedRender(res); want != "" && got != want {
			t.Errorf("%s\n  got  %s\n  want %s", name, got, want)
		}
		return n
	}
	depthBound := map[int]int{16: 7500, 64: 242000, 128: 1700000}
	for _, depth := range []int{16, 64, 128} {
		q := "SELECT a.id, x + a.i AS v FROM ci2_d a JOIN (SELECT id, f AS x FROM ci2_d) b ON a.id = b.id"
		for i := 0; i < depth; i++ {
			q = fmt.Sprintf("SELECT q%d.id, q%d.v FROM (%s) q%d", i, i, q, i)
		}
		counted(fmt.Sprintf("derivedDepth=%d", depth), q+" ORDER BY 1", "{int,float} 1,1.5 | 2,3.5", depthBound[depth])
	}
	var keys, items []string
	for k := 0; k < 50; k++ {
		keys = append(keys, fmt.Sprintf("w.c%d + 1", k))
	}
	for i := 0; i < 200; i++ {
		items = append(items, fmt.Sprintf("g.k%d * 2", i%50))
	}
	var inner []string
	for k := 0; k < 50; k++ {
		inner = append(inner, fmt.Sprintf("w.c%d + 1 AS k%d", k, k))
	}
	counted("items=200 keys=50", "SELECT "+strings.Join(items, ", ")+" FROM (SELECT "+strings.Join(inner, ", ")+
		" FROM ci2_w w GROUP BY "+strings.Join(keys, ", ")+") g", "", 360)
	from := "ci2_j0 j0"
	sel := []string{"j0.v0 + j0.id"}
	for j := 1; j < 12; j++ {
		from += fmt.Sprintf(" JOIN ci2_j%d j%d ON j%d.id = j0.id", j, j, j)
		sel = append(sel, fmt.Sprintf("j%d.v%d + j%d.id", j, j, j))
	}
	counted("join=12", "SELECT "+strings.Join(sel, ", ")+" FROM "+from+" ORDER BY 1", "", 920)
}
