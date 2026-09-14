package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC NV — the numeric-VALUE seam, enumerated once and anchored to live
// PostgreSQL 17.11.
//
// One rule: a number has ONE value in every spelling and on every path, and it
// is the server's. An overflow is 22003, never ±Infinity or a wrapped or
// narrowed number; a real accumulates as a real; a DECIMAL keeps every digit
// its declaration admits; an integer is never read through a float64.
//
// Every `want` below is what psql printed on a PostgreSQL 17.11 container over
// the SAME rows, rendered the way nvRender writes a wadjet result. Where the
// two engines' TEXT differs while the NUMBER does not, the cell carries the
// server's own spelling in its comment and says which class the difference is.
//
// FIVE arms, the five an answer can differ between (ADR-0018 §3, ADR-0027):
// the embedded engine; the same engine at 512 KiB with the forced aggregate
// drain armed; the stage DAG; the DAG with the broadcast threshold at one
// byte, which forces the shuffle; and the DAG with four morsel workers, which
// is the only width that reaches CloneSink's second call site.
//
// Issues: #1082 (float arithmetic and the float SUM answering ±Infinity where
// the server raises), #950 (SUM(real) accumulated at float8's width in three
// spellings and answered three numbers), #1037 (a wide DECIMAL literal read
// through the float64 box a CAST was handed), #1000 (PORT and PROTOCOL
// arithmetic on the float path, so `c_proto / 2` was 127.5).

type nvCell struct {
	name, sql string
	// want is the rendered result; wantState is a SQLSTATE the query must
	// raise instead. Exactly one is set.
	want      string
	wantState string
	// pin overrides want for the named arm, with the mechanism in why.
	pin map[string]string
	// digits rounds this cell's float renderings to that many significant
	// figures. A cell sets it when its answer is a float whose LAST digits
	// move with the order the rows reach the accumulator — ADR-0013's
	// nondeterminism class 9 — and the cell is about something else.
	digits int
	why    string
}

func TestNumericValuesMatchPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(string) (string, error)
	}{
		{"single", func(s string) (string, error) { return nvRender(tmdRunSingle(ctx, single, s)) }},
		{"budget", func(s string) (string, error) { return nvRender(tmdRunSingle(ctx, spilled, s)) }},
		{"dag", func(s string) (string, error) { return nvRender(tmdRunDAG(ctx, coord, s)) }},
		{"dagshuf", func(s string) (string, error) { return nvRender(tmdRunDAG(ctx, coordB, s)) }},
		{"morsel", func(s string) (string, error) { return nvRender(tmdRunDAG(ctx, coordM, s)) }},
	}

	for _, tc := range nvCells() {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				nvDigits = tc.digits
				got, err := arm.run(tc.sql)
				nvDigits = 0
				if tc.wantState != "" {
					if err == nil {
						t.Errorf("%s: answered %s; PostgreSQL 17.11 raises %s\n  SQL: %s%s",
							arm.name, got, tc.wantState, tc.sql, nvWhy(tc.why))
						continue
					}
					if s := sqlerr.StateOf(err); s != tc.wantState {
						t.Errorf("%s: raised SQLSTATE %s, want %s (%v)\n  SQL: %s%s",
							arm.name, s, tc.wantState, err, tc.sql, nvWhy(tc.why))
					}
					continue
				}
				if err != nil {
					t.Errorf("%s: %v — PostgreSQL 17.11 answers %s\n  SQL: %s%s",
						arm.name, err, tc.want, tc.sql, nvWhy(tc.why))
					continue
				}
				want := tc.want
				if p, ok := tc.pin[arm.name]; ok {
					want = p
				}
				if got != want {
					t.Errorf("%s\n  got  %s\n  want %s\n  SQL: %s%s",
						arm.name, got, want, tc.sql, nvWhy(tc.why))
				}
			}
		})
	}
}

func nvWhy(why string) string {
	if why == "" {
		return ""
	}
	return "\n  " + why
}

// nvRender writes a result the way the seam has to be read: VERBATIM, with the
// Go BOX named.
//
// It does NOT round to six significant digits the way the older censuses in
// this package do. Half of this arc is a last digit — 1.6777224e+07 against
// 1.6777226e+07 for SUM(real), 9007199254740993.25 against
// 9007199254740994.00 for a wide DECIMAL literal — and a rounded rendering
// cannot see either. The box is part of the answer for the same reason a
// declaration is: an int64 and a float64 holding the same integer print
// identically under %v, and telling those apart is what #1000 is.
func nvRender(res *oracle.Result, err error) (string, error) {
	if err != nil {
		return "", err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			parts = append(parts, c+"="+nvBox(r[c]))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return strings.Join(out, ";"), nil
}

// nvDigits is the current cell's float rounding, set around each arm's run.
// A package-level knob rather than a parameter because nvRender's shape is the
// oracle.Result → text contract every arm shares.
var nvDigits int

func nvFloat(f float64, tag string) string {
	if nvDigits > 0 {
		return fmt.Sprintf("%.*g<%s>", nvDigits, f, tag)
	}
	return fmt.Sprintf("%v<%s>", f, tag)
}

func nvBox(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case string:
		return t
	case float64:
		return nvFloat(t, "f8")
	case float32:
		return nvFloat(float64(t), "f4")
	case int64:
		return fmt.Sprintf("%d<i8>", t)
	case int32:
		return fmt.Sprintf("%d<i4>", t)
	default:
		return fmt.Sprintf("%v<%T>", v, v)
	}
}
