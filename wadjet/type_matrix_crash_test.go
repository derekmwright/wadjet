package wadjet

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	execpkg "github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The process-killer gate.
//
// Some defects do not return a wrong answer — they take the process down in a
// way no caller can recover from: a panic raised on a worker goroutine (the
// parallel aggregate emitter, the partitioned-aggregation workers), or a Go
// runtime fatal error such as a concurrent map write. #334 was exactly that
// shape and shipped, because a harness that runs every query in one process
// dies at the first one and reports nothing about the rest.
//
// This gate runs the type-matrix corpus in a CHILD process that names each
// entry on stderr BEFORE running it. When the child dies, the last name it
// printed is the killer; the parent records it and restarts the child at the
// next entry. What comes back is the complete set of corpus entries that kill
// a process, and it is compared against the declared set:
//
//   - an UNDECLARED killer fails the gate — that is a new defect;
//   - a DECLARED killer that no longer kills fails the gate — the pin has
//     outlived its bug and deleting it is the proof the fix landed
//     (ADR-0013 §Pins).
//
// So a crash pin is a real pin, not a skip: the entry is still executed, in a
// process that is allowed to die.

// tmChildEnv, when set, makes the test binary run as the child: it executes
// corpus entries from index N onward instead of driving children.
const tmChildEnv = "WADJET_TYPEMATRIX_CRASH_FROM"

// tmCrashMarker prefixes the child's per-entry announcement, printed BEFORE
// the query runs. tmCrashDone is printed after it returns and after a settle
// window, so the parent can tell "died running entry i" from "died after
// entry i finished".
//
// Both are needed because the fatal this gate hunts is raised on a goroutine
// the query does not join: #392's mismatch panic fires in
// aggregate_parallel_emit.go's emit goroutine, which can outlive the
// db.Query call that started it. With only the ENTRY marker, the process
// sometimes died after the loop had already announced entry i+1, and the
// parent blamed the WRONG entry — observed as `maxby_c_uuid` reported "no
// longer kills, so #392 is FIXED" while `minby_scalar_c_uuid`, the next
// entry, was reported as a NEW PROCESS KILLER, in the same run. Both claims
// were false and both fail the gate, so the race was noisy rather than
// dangerous, but a gate that names the wrong entry cannot be read.
const tmCrashMarker = "TYPEMATRIX-ENTRY "

// tmCrashDone marks an entry the child finished without the process dying.
// Its second field is how many panics exec's query boundary converted while
// that entry ran — see tmPanicPins.
const tmCrashDone = "TYPEMATRIX-DONE "

// tmCrashSettle is how long the child waits after each query before declaring
// the entry survived. It bounds the window in which an escaped goroutine's
// panic is still attributed to the entry that started it. 270 entries x 5ms
// is under two seconds on a gate that spawns one process per killer.
const tmCrashSettle = 5 * time.Millisecond

// tmCrashPins are the corpus entries known to kill the process.
//
// Every other gate in this file SKIPS these entries, because a dead process
// compares nothing — this gate is the one that keeps them honest.
//
// #393 (MAP scan fallback) and #392 (minMaxDeclaredType's FLOAT64 fallthrough)
// are both fixed at the root — see the closing commits for the mechanism.
// Independently, #400 wrapped the two goroutines that had no recover — the
// parallel aggregate emitter (internal/engine/exec/aggregate_parallel_emit.go)
// and the scan workers (internal/planner/physical/plan.go, util.go) — so a
// FatalEvalPanic raised there is delivered as a query error the way
// Pipeline.Run has always delivered its own, rather than taking the process
// down. That recover is a safety net for the NEXT type-coverage defect that
// reaches one of those goroutines, not the reason #392/#393 are gone from
// this map: both were verified fixed at the value level (dbf6031) before
// their pins were deleted.
//
// The map is empty rather than removed: the mechanism (crash-pin the entries
// a fix hasn't reached yet, delete the pin the moment
// TestTypeMatrixNoProcessKillers says the entry no longer kills the process)
// stays in place for the next type-coverage defect that takes the process
// down.
var tmCrashPins = map[string]typematrix.Pin{}

// tmPanicPins are the corpus entries known to raise an INTERNAL PANIC that
// the query-scoped boundary recovers (#511).
//
// The boundary is what keeps the crash gate above honest now that it exists:
// once every goroutine converts any panic into a query error, a brand-new
// index-out-of-range no longer kills a process, so no child dies and the
// crash arm sees nothing. That is the right production behaviour and the
// wrong gate. exec.QueryPanicsRecovered is the seam — a recovered panic is
// still a defect, so the child reports the count around every entry and an
// unpinned nonzero delta fails CI exactly as a killer does.
//
// Empty, and gating: every panic the SQLancer soak reached (#508, #509, #510,
// #512) is fixed at the root, not merely recovered.
var tmPanicPins = map[string]typematrix.Pin{}

// TestTypeMatrixNoProcessKillers drives child processes over the corpus and
// gates the set of entries that kill one.
func TestTypeMatrixNoProcessKillers(t *testing.T) {
	if from, ok := os.LookupEnv(tmChildEnv); ok {
		tmCrashChild(t, from)
		return
	}
	if testing.Short() {
		t.Skip("-short: the process-killer gate re-execs the test binary once per killer")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	corpus := typematrix.Corpus()

	found := map[string]bool{}
	hung := map[string]bool{}
	panicked := map[string]int{}
	start := 0
	// One child per killer, plus one that reaches the end. Every round
	// advances start by at least one entry, so len(corpus)+1 rounds is a
	// termination bound, not a budget: sizing it to the number of PINS
	// instead would cap discovery at the killers already known, which is
	// exactly the set a discovery pass must not assume.
	for round := 0; round <= len(corpus) && start < len(corpus); round++ {
		last, died, timedOut, panics, detail := tmRunCrashChild(t, self, start)
		for idx, n := range panics {
			if idx >= 0 && idx < len(corpus) {
				panicked[corpus[idx].Name] += n
			}
		}
		if last < 0 {
			if died {
				t.Fatalf("the child died before naming an entry (start=%d), so no entry can be "+
					"blamed:\n%s", start, detail)
			}
			break // child ran the whole remaining corpus
		}
		if !died {
			break // child finished; `last` is just the final entry it ran
		}
		found[corpus[last].Name] = true
		if timedOut {
			// ctx.Err() fired: WE killed the child after its 10m deadline
			// because it never exited on its own. That is a HANG, not a
			// crash — a crash at least ends, a hang holds a slot forever —
			// and reporting it as a generic kill would send the wrong
			// investigation down the wrong path.
			hung[corpus[last].Name] = true
			t.Logf("entry %q (index %d) HUNG — the harness's 10m per-child deadline killed it; "+
				"the process never exited on its own:\n%s", corpus[last].Name, last, detail)
		} else {
			t.Logf("entry %q (index %d) killed the process:\n%s", corpus[last].Name, last, detail)
		}
		start = last + 1
	}
	tmGatePanics(t, corpus, panicked)

	names := make([]string, 0, len(found))
	for n := range found {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if p, ok := tmCrashPins[n]; ok {
			t.Logf("known process killer %q, tracked in %s — NOT gated:\n  %s", n, p.Issue, p.Reason)
			continue
		}
		if hung[n] {
			t.Errorf("NEW HANG: corpus entry %q never exits — the harness's 10m per-child deadline "+
				"is what ended it, not the process itself. A query that hangs holds its slot forever, "+
				"which no timeout in the engine ends — file it, then pin it in tmCrashPins with the "+
				"issue number.", n)
			continue
		}
		t.Errorf("NEW PROCESS KILLER: corpus entry %q takes the process down. A query that kills "+
			"the server is the worst failure mode in the engine — file it, then pin it in "+
			"tmCrashPins with the issue number.", n)
	}
	pinned := make([]string, 0, len(tmCrashPins))
	for n := range tmCrashPins {
		pinned = append(pinned, n)
	}
	sort.Strings(pinned)
	byName := map[string]bool{}
	for _, q := range corpus {
		byName[q.Name] = true
	}
	for _, n := range pinned {
		p := tmCrashPins[n]
		switch {
		case !byName[n]:
			t.Errorf("crash pin %q (%s) names no corpus entry — it exempts nothing. "+
				"Delete it or fix the name.", n, p.Issue)
		case !found[n]:
			t.Errorf("crash pin %q no longer kills the process, so %s is FIXED:\n  %s\n"+
				"Delete the pin so the entry is gated again.", n, p.Issue, p.Reason)
		}
	}
	t.Logf("process-killer gate: %d corpus entries, %d killers (%d pinned)",
		len(corpus), len(found), len(tmCrashPins))
}

// tmGatePanics compares the set of corpus entries that raised a recovered
// internal panic against tmPanicPins, on the same terms as the crash arm: an
// undeclared one is a new defect, a declared one that no longer panics is a
// pin that has outlived its bug.
func tmGatePanics(t *testing.T, corpus []typematrix.Query, panicked map[string]int) {
	t.Helper()
	names := make([]string, 0, len(panicked))
	for n := range panicked {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if p, ok := tmPanicPins[n]; ok {
			t.Logf("known internal panic in %q, tracked in %s — NOT gated:\n  %s", n, p.Issue, p.Reason)
			continue
		}
		t.Errorf("NEW INTERNAL PANIC: corpus entry %q raised %d panic(s) that the query "+
			"boundary had to recover. The server survived, which is the whole point of the "+
			"boundary — but a query that panics is still a defect, and the boundary exists "+
			"so it fails HERE instead of in production. File it, then pin it in tmPanicPins.",
			n, panicked[n])
	}
	byName := map[string]bool{}
	for _, q := range corpus {
		byName[q.Name] = true
	}
	pinned := make([]string, 0, len(tmPanicPins))
	for n := range tmPanicPins {
		pinned = append(pinned, n)
	}
	sort.Strings(pinned)
	for _, n := range pinned {
		p := tmPanicPins[n]
		switch {
		case !byName[n]:
			t.Errorf("panic pin %q (%s) names no corpus entry — it exempts nothing. "+
				"Delete it or fix the name.", n, p.Issue)
		case panicked[n] == 0:
			t.Errorf("panic pin %q no longer panics, so %s is FIXED:\n  %s\n"+
				"Delete the pin so the entry is gated again.", n, p.Issue, p.Reason)
		}
	}
	t.Logf("internal-panic gate: %d entries panicked (%d pinned)", len(panicked), len(tmPanicPins))
}

// tmRunCrashChild runs one child from index start. It returns the index of the
// last entry the child announced (-1 if it announced none), whether the child
// died, the recovered-panic count per entry index, and the tail of its output.
// timedOut reports whether ctx's own 10m deadline is what ended the child —
// that is a HANG (the harness killed it because it never exited on its own),
// a different defect than a process that crashed or panicked unrecoverably
// well inside the deadline, and the caller reports it distinctly.
func tmRunCrashChild(t *testing.T, self string, start int) (last int, died, timedOut bool, panics map[int]int, detail string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-test.run=TestTypeMatrixNoProcessKillers", "-test.v")
	cmd.Env = append(os.Environ(), tmChildEnv+"="+strconv.Itoa(start))
	out, err := cmd.CombinedOutput()

	last, lastDone := -1, -1
	panics = map[int]int{}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	fieldsAfter := func(line, marker string) []string {
		i := strings.Index(line, marker)
		if i < 0 {
			return nil
		}
		return strings.Fields(line[i+len(marker):])
	}
	num := func(line, marker string) (int, bool) {
		fields := fieldsAfter(line, marker)
		if len(fields) == 0 {
			return 0, false
		}
		n, cerr := strconv.Atoi(fields[0])
		return n, cerr == nil
	}
	for sc.Scan() {
		line := sc.Text()
		if n, ok := num(line, tmCrashDone); ok {
			lastDone = n
			if f := fieldsAfter(line, tmCrashDone); len(f) > 1 {
				if p, cerr := strconv.Atoi(f[1]); cerr == nil && p > 0 {
					panics[n] += p
				}
			}
			continue
		}
		if n, ok := num(line, tmCrashMarker); ok {
			last = n
		}
	}
	if err == nil {
		return last, false, false, panics, ""
	}
	// Attribute to the entry that STARTED and never finished. If every
	// announced entry reported DONE, the fatal escaped from an already-
	// finished one and the last of those owns it — not the entry whose
	// marker happened to be printed next.
	if lastDone >= last {
		last = lastDone
	}
	// ctx.Err() is non-nil only when OUR deadline elapsed and
	// exec.CommandContext killed the child for us — the child never chose to
	// exit. Any other death (a panic escaping past recover, a Go runtime
	// fatal like a concurrent map write, an OS-level crash) leaves ctx.Err()
	// nil because the process ended well within the 10m window.
	return last, true, ctx.Err() == context.DeadlineExceeded, panics, tmTail(string(out), 24)
}

// tmCrashChild is the child half: run corpus entries from start, naming each
// on stderr before it runs so a fatal error is attributable.
func tmCrashChild(t *testing.T, from string) {
	start, err := strconv.Atoi(from)
	if err != nil {
		t.Fatalf("bad %s=%q: %v", tmChildEnv, from, err)
	}
	ctx := context.Background()
	db := tmOpen(t)
	corpus := typematrix.Corpus()
	for i := start; i < len(corpus); i++ {
		// Unbuffered and BEFORE the query: a runtime fatal error is not
		// recoverable and no buffered log survives it.
		fmt.Fprintf(os.Stderr, "%s%d %s\n", tmCrashMarker, i, corpus[i].Name)
		before := execpkg.QueryPanicsRecovered()
		func() {
			defer func() { _ = recover() }() // a recoverable panic is not this gate's business
			_, _ = db.Query(ctx, corpus[i].SQL)
		}()
		// Give a goroutine the query did not join its chance to take the
		// process down while this entry is still the one on the record.
		runtime.Gosched()
		time.Sleep(tmCrashSettle)
		// The DONE line carries the entry index and the number of panics the
		// query boundary converted while it ran. Zero is the normal case; a
		// nonzero count is a defect the boundary kept out of production, and
		// the parent gates it against tmPanicPins.
		fmt.Fprintf(os.Stderr, "%s%d %d\n", tmCrashDone, i,
			execpkg.QueryPanicsRecovered()-before)
	}
}

// tmTail returns the last n lines of s.
func tmTail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return "    " + strings.Join(lines, "\n    ")
}
