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
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The panic-shape gate.
//
// TestTypeMatrixNoProcessKillers gates the TYPE corpus, which is where
// type-coverage defects live. This gate covers the other axis the SQLancer
// soak exercised: query SHAPES that reached a panic — a text function over a
// non-text column, a failing cast on a join's build side, an outer join under
// a set operation, a parallel aggregate whose workers race to report errors.
//
// Both arms of the contract are asserted, and they pull in opposite
// directions, which is the point:
//
//   - the process must SURVIVE every shape (the child is allowed to die, and
//     a death is attributed to the entry that was running);
//   - no shape may raise an INTERNAL PANIC that the boundary merely recovered
//     (execpkg.QueryPanicsRecovered must not move). Recovery keeps the server
//     up; it does not make the defect acceptable, and without this arm every
//     future panic would pass CI silently.
//
// Entries that must fail as QUERIES (an invalid cast is a client error, not a
// bug) declare wantErr, so "the server survived by refusing everything" fails
// too.

// pbChildEnv, when set, makes the test binary run as the child.
const pbChildEnv = "WADJET_PANIC_SHAPES_FROM"

const (
	pbMarker = "PANICSHAPE-ENTRY "
	pbDone   = "PANICSHAPE-DONE " // index, recovered-panic delta
)

// pbSettle gives a goroutine the query did not join its chance to take the
// process down while its entry is still the one on the record.
const pbSettle = 10 * time.Millisecond

// pbPins are shapes known to kill the process or to raise a recovered
// internal panic. Empty: #508, #509, #510 and #512 are fixed at the root.
var pbPins = map[string]string{}

type pbEntry struct {
	name string
	sql  string
	// wantErr, when non-empty, is a substring the query's error must carry.
	// It pins the shapes whose right answer is a CLIENT ERROR, so a boundary
	// that turned a real answer into an internal error would fail here.
	wantErr string
	// wantRows, when > 0, is how many rows the query must return.
	wantRows int
}

// pbCorpus is the shape corpus. Every entry names the issue it came from.
func pbCorpus() []pbEntry {
	return []pbEntry{
		// #509: a vec text kernel handed a column with no BytesData.
		{name: "concat_text_int", sql: `SELECT CONCAT(c0, c1) AS v FROM t0`, wantRows: 2},
		{name: "concat_int_text", sql: `SELECT CONCAT(c1, c0) AS v FROM t0`, wantRows: 2},
		{name: "concat_pipes", sql: `SELECT c0 || c1 AS v FROM t0`, wantRows: 2},
		{name: "concat_three", sql: `SELECT CONCAT(c0, c1, c0) AS v FROM t0`, wantRows: 2},
		{name: "upper_int", sql: `SELECT UPPER(c1) AS v FROM t0`, wantRows: 2},
		{name: "starts_with_int", sql: `SELECT starts_with(c0, c1) AS v FROM t0`, wantRows: 2},
		{name: "replace_int", sql: `SELECT REPLACE(c0, c1, 'z') AS v FROM t0`, wantRows: 2},
		{name: "substr_keeps_vec", sql: `SELECT SUBSTR(c0, 1, 1) AS v FROM t0`, wantRows: 2},
		{name: "concat_in_where", sql: `SELECT c0 FROM t0 WHERE CONCAT(c0, c1) = 'x5'`, wantRows: 1},
		{name: "concat_in_group_by", sql: `SELECT CONCAT(c0, c1) AS v, COUNT(*) AS n FROM t0 GROUP BY CONCAT(c0, c1)`, wantRows: 2},

		// #508: a failing cast evaluated on a hash join's BUILD side.
		{
			name:    "join_build_bad_cast",
			sql:     `SELECT * FROM t0 JOIN t2 ON TRUE WHERE (t0.c1) BETWEEN (CAST(t0.c0 AS INT)) AND (t0.c1)`,
			wantErr: "invalid input syntax for type integer",
		},
		{
			name:    "join_build_div_zero",
			sql:     `SELECT * FROM t0 JOIN t2 ON t0.c1 = t2.c0 WHERE t0.c1 / (t0.c1 - t0.c1) > 0`,
			wantErr: "division by zero",
		},
		{
			// No wantErr: CAST AS DATE does not currently validate its input
			// (#340, open on its own terms). This entry is here for the
			// SURVIVAL arm — whatever the cast decides, the build side must
			// not take the process with it.
			name: "join_build_date_cast",
			sql:  `SELECT * FROM t0 JOIN t2 ON TRUE WHERE CAST(t0.c0 AS DATE) > DATE '1970-01-01'`,
		},
		{
			name:     "concat_on_join_build",
			sql:      `SELECT CONCAT(t0.c0, t0.c1) AS v FROM t0 JOIN t2 ON TRUE`,
			wantRows: 4,
		},

		// #510: outer-join flush sources torn down under a set operation.
		{name: "right_join_union", sql: `SELECT c0 FROM t0 UNION SELECT CAST(c0 AS TEXT) FROM t2`, wantRows: 4},
		{
			name: "full_outer_under_union",
			sql: `SELECT t0.c0 AS v FROM t0 FULL OUTER JOIN t2 ON t0.c1 = t2.c0 ` +
				`UNION ALL SELECT c0 AS v FROM t0`,
		},
		{
			name: "right_outer_under_union",
			sql: `SELECT t0.c0 AS v FROM t0 RIGHT JOIN t2 ON t0.c1 = t2.c0 ` +
				`UNION SELECT c0 AS v FROM t0`,
		},
		{name: "full_outer_empty_probe", sql: `SELECT t0.c0 AS v FROM t0 FULL OUTER JOIN t2 ON FALSE`},

		// #512: parallel workers racing to report differently-shaped errors.
		{
			name:    "parallel_agg_bad_cast",
			sql:     `SELECT CAST(c0 AS INT) AS k, COUNT(*) AS n FROM wide GROUP BY CAST(c0 AS INT)`,
			wantErr: "invalid input syntax for type integer",
		},
		{
			name:    "parallel_filter_div_zero",
			sql:     `SELECT COUNT(*) AS n FROM wide WHERE c1 / (c1 - c1) > 0`,
			wantErr: "division by zero",
		},
		{name: "parallel_concat", sql: `SELECT CONCAT(c0, c1) AS v, COUNT(*) AS n FROM wide GROUP BY CONCAT(c0, c1)`},
	}
}

// pbOpen builds the fixture both halves of the gate share: the two tables
// from #508/#509's repro, plus a wider one that forces parallel workers.
func pbOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ddl := []string{
		`CREATE TABLE t0 (c0 TEXT, c1 BIGINT)`,
		`CREATE TABLE t2 (c0 BIGINT)`,
		`CREATE TABLE wide (c0 TEXT, c1 BIGINT)`,
	}
	for _, s := range ddl {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	dml := []string{
		`INSERT INTO t0 (c0, c1) VALUES ('x', 5)`,
		`INSERT INTO t0 (c0, c1) VALUES ('9', 7)`,
		`INSERT INTO t2 (c0) VALUES (1), (2)`,
	}
	// Enough rows in `wide` that runParallel actually forks workers.
	var b strings.Builder
	b.WriteString(`INSERT INTO wide (c0, c1) VALUES `)
	for i := 0; i < 6000; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "('%s', %d)", []string{"x", "9", "7"}[i%3], i)
	}
	dml = append(dml, b.String())
	for _, s := range dml {
		if _, err := db.Execute(ctx, s); err != nil {
			t.Fatalf("%s: %v", truncSQL(s), err)
		}
	}
	return db
}

func truncSQL(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}

// TestQueryPanicBoundaryHoldsForEveryShape drives the shape corpus in child
// processes and gates both arms: nothing kills a process, and nothing raises
// a panic the boundary had to recover.
func TestQueryPanicBoundaryHoldsForEveryShape(t *testing.T) {
	if from, ok := os.LookupEnv(pbChildEnv); ok {
		pbChild(t, from)
		return
	}
	if testing.Short() {
		t.Skip("-short: the panic-shape gate re-execs the test binary once per killer")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	corpus := pbCorpus()

	killers := map[string]bool{}
	panicked := map[string]int{}
	start := 0
	for round := 0; round <= len(corpus) && start < len(corpus); round++ {
		last, died, panics, detail := pbRunChild(t, self, start)
		for idx, n := range panics {
			if idx >= 0 && idx < len(corpus) {
				panicked[corpus[idx].name] += n
			}
		}
		if last < 0 {
			if died {
				t.Fatalf("the child died before naming a shape (start=%d):\n%s", start, detail)
			}
			break
		}
		if !died {
			break
		}
		killers[corpus[last].name] = true
		t.Logf("shape %q (index %d) killed the process:\n%s", corpus[last].name, last, detail)
		start = last + 1
	}

	bad := make([]string, 0, len(killers)+len(panicked))
	for n := range killers {
		if _, ok := pbPins[n]; !ok {
			bad = append(bad, fmt.Sprintf("%s: KILLED THE PROCESS", n))
		}
	}
	for n, c := range panicked {
		if _, ok := pbPins[n]; !ok {
			bad = append(bad, fmt.Sprintf("%s: raised %d internal panic(s) the boundary recovered", n, c))
		}
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Errorf("panic-shape gate: %s\n"+
			"A query that panics is a defect whether or not the server survives it — the "+
			"boundary exists so this fails HERE. File it, then pin it in pbPins.", b)
	}
	for n := range pbPins {
		if !killers[n] && panicked[n] == 0 {
			t.Errorf("pin %q neither kills nor panics any more — delete it, that is the proof "+
				"the fix landed", n)
		}
	}
	t.Logf("panic-shape gate: %d shapes, %d killers, %d panickers (%d pinned)",
		len(corpus), len(killers), len(panicked), len(pbPins))
}

// pbRunChild runs one child from index start, mirroring tmRunCrashChild.
func pbRunChild(t *testing.T, self string, start int) (last int, died bool, panics map[int]int, detail string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, self,
		"-test.run=TestQueryPanicBoundaryHoldsForEveryShape", "-test.v")
	cmd.Env = append(os.Environ(), pbChildEnv+"="+strconv.Itoa(start))
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
	for sc.Scan() {
		line := sc.Text()
		if f := fieldsAfter(line, pbDone); len(f) > 0 {
			if n, cerr := strconv.Atoi(f[0]); cerr == nil {
				lastDone = n
				if len(f) > 1 {
					if p, perr := strconv.Atoi(f[1]); perr == nil && p > 0 {
						panics[n] += p
					}
				}
			}
			continue
		}
		if f := fieldsAfter(line, pbMarker); len(f) > 0 {
			if n, cerr := strconv.Atoi(f[0]); cerr == nil {
				last = n
			}
		}
	}
	if err == nil {
		// The child ran to the end AND its own assertions passed. A failing
		// child that did not die is a value assertion, not a crash — surface
		// it rather than reporting a clean gate.
		return last, false, panics, ""
	}
	if strings.Contains(string(out), "--- FAIL") {
		t.Errorf("the panic-shape child reported failures (start=%d):\n%s", start, pbTail(string(out), 40))
		return last, false, panics, ""
	}
	if lastDone >= last {
		last = lastDone
	}
	return last, true, panics, pbTail(string(out), 30)
}

// pbChild is the child half: run shapes from start, naming each on stderr
// before it runs so a fatal is attributable, and asserting the shape's own
// contract (error or row count) so "survived by refusing everything" fails.
func pbChild(t *testing.T, from string) {
	start, err := strconv.Atoi(from)
	if err != nil {
		t.Fatalf("bad %s=%q: %v", pbChildEnv, from, err)
	}
	ctx := context.Background()
	db := pbOpen(t)
	corpus := pbCorpus()
	for i := start; i < len(corpus); i++ {
		e := corpus[i]
		// Unbuffered and BEFORE the query: a runtime fatal is not
		// recoverable and no buffered log survives it.
		fmt.Fprintf(os.Stderr, "%s%d %s\n", pbMarker, i, e.name)
		before := execpkg.QueryPanicsRecovered()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: the panic escaped to the caller: %v", e.name, r)
				}
			}()
			res, qErr := db.Query(ctx, e.sql)
			switch {
			case e.wantErr != "":
				if qErr == nil {
					t.Errorf("%s: got no error, want one carrying %q — the server survived by "+
						"answering, but this shape must fail as a QUERY", e.name, e.wantErr)
					return
				}
				if !strings.Contains(qErr.Error(), e.wantErr) {
					t.Errorf("%s: error %q does not carry %q", e.name, qErr, e.wantErr)
				}
			case qErr != nil:
				t.Errorf("%s: %v", e.name, qErr)
			case e.wantRows > 0 && len(res.Rows) != e.wantRows:
				t.Errorf("%s: %d rows, want %d", e.name, len(res.Rows), e.wantRows)
			}
		}()
		runtime.Gosched()
		time.Sleep(pbSettle)
		fmt.Fprintf(os.Stderr, "%s%d %d\n", pbDone, i, execpkg.QueryPanicsRecovered()-before)
	}
}

func pbTail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return "    " + strings.Join(lines, "\n    ")
}
