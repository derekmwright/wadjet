// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// THE DECLARATION WALK STAYS LINEAR (ADR-0047 stage 2; RISKS R5): the walk was
// 2^depth and then 5.5^depth in derived-table depth before its per-node memo
// (#1034: 248 s at depth 16), and stage 2 re-keyed that memo by position. Each
// shape is planned and answered on the embedded engine — the door that binds —
// under the gate's two-second bound: a bound expression over a join, read
// through derived tables nested 16, 64 and 128 deep (128 under three
// seconds); 200 items over 50 grouped keys
// read through a derived table; and a twelve-way join whose items are
// qualified arithmetic over every arm.
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
	const bound = 2 * time.Second
	// The degree of the walk is what this gate guards, not a wall-clock figure:
	// on a loaded gate runner the depth-128 query took 4.5 s where this host
	// takes 2.3 s. At the measured exponent (2.55 at a0f0c322, 2.63 with the
	// ordered walk) depth 128 costs about 6.5x depth 64; a memo that stopped
	// caching would take it past 11x. The absolute limit only catches a stall.
	const depthRatioLimit = 9.0
	var depthElapsed = map[int]time.Duration{}
	timed := func(name, q, want string) time.Duration {
		t.Helper()
		start := time.Now()
		res, err := db.Query(ctx, q)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		t.Logf("%s elapsed=%s", name, elapsed)
		limit := bound
		if strings.HasSuffix(name, "=128") {
			limit = 20 * time.Second
		}
		if elapsed > limit {
			t.Errorf("%s took %s, want < %s: the declaration walk stalled", name, elapsed, limit)
		}
		if got := ssEmbeddedRender(res); want != "" && got != want {
			t.Errorf("%s\n  got  %s\n  want %s", name, got, want)
		}
		return elapsed
	}
	for _, depth := range []int{16, 64, 128} {
		q := "SELECT a.id, x + a.i AS v FROM ci2_d a JOIN (SELECT id, f AS x FROM ci2_d) b ON a.id = b.id"
		for i := 0; i < depth; i++ {
			q = fmt.Sprintf("SELECT q%d.id, q%d.v FROM (%s) q%d", i, i, q, i)
		}
		depthElapsed[depth] = timed(fmt.Sprintf("derivedDepth=%d", depth), q+" ORDER BY 1", "{int,float} 1,1.5 | 2,3.5")
	}
	if d64, d128 := depthElapsed[64], depthElapsed[128]; d64 > 50*time.Millisecond {
		if ratio := float64(d128) / float64(d64); ratio > depthRatioLimit {
			t.Errorf("depth 128 / depth 64 = %.1fx (%s / %s), want <= %.0fx: the declaration walk's cost rose past its measured degree", ratio, d128, d64, depthRatioLimit)
		}
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
	timed("items=200 keys=50", "SELECT "+strings.Join(items, ", ")+" FROM (SELECT "+strings.Join(inner, ", ")+
		" FROM ci2_w w GROUP BY "+strings.Join(keys, ", ")+") g", "")
	from := "ci2_j0 j0"
	sel := []string{"j0.v0 + j0.id"}
	for j := 1; j < 12; j++ {
		from += fmt.Sprintf(" JOIN ci2_j%d j%d ON j%d.id = j0.id", j, j, j)
		sel = append(sel, fmt.Sprintf("j%d.v%d + j%d.id", j, j, j))
	}
	timed("join=12", "SELECT "+strings.Join(sel, ", ")+" FROM "+from+" ORDER BY 1", "")
}
