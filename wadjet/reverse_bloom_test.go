package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// The reverse bloom runs the probe side of a large semi/anti join first,
// builds a bloom from its join keys, and filters the BUILD side's scan with
// it. Rows the filter rejects never reach the hash table, so a bloom whose two
// sides encode keys differently does not slow the query down — it changes the
// answer, silently, by the whole build side (#543).
//
// The gate is a differential: the same query with the optimization on and off
// must return the same rows. It is only worth anything if the optimization
// actually engaged, so it also asserts on physical.ReverseBloomsInstalled —
// otherwise a threshold change would quietly turn this file into a no-op.

// rbLowerThreshold makes the reverse bloom engage on a fixture that is five
// thousand rows rather than the ten million the production threshold wants.
func rbLowerThreshold(t *testing.T) {
	t.Helper()
	prevSemi, prevInner := physical.ReverseBloomThreshold, physical.ReverseBloomInnerThreshold
	physical.ReverseBloomThreshold = 100
	physical.ReverseBloomInnerThreshold = 100
	t.Cleanup(func() {
		physical.ReverseBloomThreshold = prevSemi
		physical.ReverseBloomInnerThreshold = prevInner
	})
}

// rbToggle returns the reverse-bloom kill switch, failing if it went missing:
// the invariance oracle enumerates it by name, so losing it loses coverage
// everywhere, not just here.
func rbToggle(t *testing.T) *optswitch.Toggle {
	t.Helper()
	for _, tg := range optswitch.All() {
		if tg.Name == "reverse-bloom" {
			return tg
		}
	}
	t.Fatal(`no "reverse-bloom" toggle registered — the optimization has no kill switch`)
	return nil
}

// rbAnswer runs one query with the reverse bloom in the given state and
// reports the result plus how many filters were installed while it ran.
func rbAnswer(t *testing.T, db *DB, sql string, on bool) (*oracle.Result, int64) {
	t.Helper()
	tg := rbToggle(t)
	prev := tg.Set(on)
	defer tg.Set(prev)
	before := physical.ReverseBloomsInstalled.Load()
	res, err := tmRun(context.Background(), db, sql)
	if err != nil {
		t.Fatalf("query %q (reverse bloom on=%v): %v", sql, on, err)
	}
	return res, physical.ReverseBloomsInstalled.Load() - before
}

// TestReverseBloomAnswersTheSameWithAndWithout is #543's end-to-end gate: a
// semi join keyed on every wide type in the matrix, each answered twice.
//
// Before the encoders were unified, the STRING/BYTES/UUID/IPv6/CIDR arms
// answered zero rows with the filter engaged — every build row rejected — and
// the arms whose storage is not a BytesColumn at all (TIMESTAMP, IPv4, MAC,
// DURATION, FLOAT, DECIMAL) took the process down reading offsets a vector
// does not have.
func TestReverseBloomAnswersTheSameWithAndWithout(t *testing.T) {
	// No -short skip. ci.yml's Unit Tests step runs with -short and its
	// TypeMatrix step's pattern does not match this name, so a skip here
	// would mean this gate never runs in CI at all — the exact trap ci.yml
	// documents around TestQueryPanicBoundary. It costs about a second.
	rbLowerThreshold(t)
	db := tmOpen(t)

	engagedAnywhere := int64(0)
	for _, c := range typematrix.Columns() {
		if !c.Flat || !c.Wide {
			continue // a join key needs distinct values, and a scalar type
		}
		col := c.Name
		t.Run("semi_"+col, func(t *testing.T) {
			sql := fmt.Sprintf(
				`SELECT COUNT(*) AS n, MIN(a.id) AS lo, MAX(a.id) AS hi, SUM(a.id) AS s `+
					`FROM %s a WHERE a.%s IN (SELECT b.%s FROM %s b WHERE b.id < 2500)`,
				typematrix.Table, col, col, typematrix.Table)

			off, _ := rbAnswer(t, db, sql, false)
			on, installed := rbAnswer(t, db, sql, true)
			engagedAnywhere += installed

			if diff := oracle.Compare(off, on, oracle.CompareSpec{Mode: oracle.CmpUnordered}); diff != "" {
				t.Fatalf("reverse bloom changed the answer on a %s key (installed=%d): %s",
					c.Type.String(), installed, diff)
			}
			// A semi join that matched nothing would make the comparison
			// above vacuous — both arms would be zero.
			if len(on.Rows) != 1 {
				t.Fatalf("expected one aggregate row, got %d", len(on.Rows))
			}
			t.Logf("%s: %v (bloom filters installed: %d)", col, on.Rows[0], installed)
		})
	}

	if engagedAnywhere == 0 {
		t.Fatal("no reverse bloom was installed by any arm — the gate compared two identical code paths")
	}
	t.Logf("reverse blooms installed across the matrix: %d", engagedAnywhere)
}

// TestReverseBloomStringSemiJoinKeepsItsRows is the headline shape stated
// plainly, and it checks ROWS rather than an aggregate: a string-keyed semi
// join whose answer is known independently of the join.
func TestReverseBloomStringSemiJoinKeepsItsRows(t *testing.T) {
	rbLowerThreshold(t)
	db := tmOpen(t)

	// Every non-NULL c_str in the left half of the table also appears in the
	// right half's subquery range or it does not; either way both arms must
	// agree, and the count must not be zero.
	sql := fmt.Sprintf(
		`SELECT a.id FROM %s a WHERE a.c_str IN (SELECT b.c_str FROM %s b WHERE b.id < 1000) ORDER BY a.id`,
		typematrix.Table, typematrix.Table)

	off, _ := rbAnswer(t, db, sql, false)
	on, installed := rbAnswer(t, db, sql, true)

	if installed == 0 {
		t.Fatal("the reverse bloom never engaged — this test proves nothing as written")
	}
	if len(off.Rows) == 0 {
		t.Fatal("the semi join matched no rows with the optimization off; the fixture cannot show row loss")
	}
	if diff := oracle.Compare(off, on, oracle.CompareSpec{Mode: oracle.CmpOrdered}); diff != "" {
		t.Fatalf("reverse bloom dropped rows from a string-keyed semi join (installed=%d): %s", installed, diff)
	}
	t.Logf("string-keyed semi join: %d rows, %d bloom filters installed", len(on.Rows), installed)
}

// TestReverseBloomNeverAnswersFromABrokenFilter asserts the counters stay at
// zero across the matrix. A self-check failure or a key-type mismatch is not a
// slow query, it is a filter that was about to drop rows — so the run that
// exercises every type must record neither.
func TestReverseBloomNeverAnswersFromABrokenFilter(t *testing.T) {
	// No -short skip: see TestReverseBloomAnswersTheSameWithAndWithout.
	rbLowerThreshold(t)
	db := tmOpen(t)

	selfCheck := exec.BloomSelfCheckFailures.Load()
	mismatch := exec.BloomKeyTypeMismatches.Load()

	tg := rbToggle(t)
	prev := tg.Set(true)
	defer tg.Set(prev)

	for _, c := range typematrix.Columns() {
		if !c.Flat || !c.Wide {
			continue
		}
		sql := fmt.Sprintf(
			`SELECT COUNT(*) FROM %s a WHERE a.%s IN (SELECT b.%s FROM %s b WHERE b.id < 2500)`,
			typematrix.Table, c.Name, c.Name, typematrix.Table)
		if _, err := tmRun(context.Background(), db, sql); err != nil {
			t.Fatalf("query on %s: %v", c.Name, err)
		}
	}

	if got := exec.BloomSelfCheckFailures.Load() - selfCheck; got != 0 {
		t.Errorf("%d bloom self-check failures across the type matrix — a filter that cannot match its own keys", got)
	}
	if got := exec.BloomKeyTypeMismatches.Load() - mismatch; got != 0 {
		t.Errorf("%d bloom key-type mismatches across the type matrix", got)
	}
}
