package coordinator

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// ARC ND — the numeric DECLARATION seam, enumerated once and anchored to live
// PostgreSQL 17.11.
//
// One rule: the DECLARED TYPE of a numeric expression is PostgreSQL's, on
// every arm, and the two paths never disagree about the type of one
// expression. Arc NV settled the VALUES; this gate is what a value oracle
// cannot see — a right number under a wrong type — so every cell carries the
// declaration AND the Go box beside it.
//
// FIVE arms, the five a declaration can differ between: the embedded engine;
// the same engine at 512 KiB, where the pipeline breakers spill and re-declare
// from their run format; the stage DAG, whose worker rebuilds an aggregate's
// input projection from expression TEXT; the DAG with the broadcast threshold
// at one byte, which forces the shuffle and its .wshf header; and the DAG with
// four morsel workers.
//
// Every `want` below is what PostgreSQL 17.11 declares (`pg_typeof`) over the
// same rows, written in this engine's own type spelling: bigint is INT64,
// integer INT32, real FLOAT32, double precision FLOAT64, numeric
// DECIMAL(p,s). A cell with a `why` is a DIVERGENCE this arc PINS rather than
// closes, and it names what the server answers, so the day it closes the cell
// FAILS and deleting it is the proof.
//
// Issues: #813 and #954 (the window declaration arm), #1011 (a ROW field path
// through a window), #951 (grouped MIN/MAX of an int4), #1018 (the bitwise
// family), #1029 (an integer width above an aggregate's own outputs), #1070
// (an integer literal and int4 arithmetic), #952 (AVG's scale), #992 (an
// ARRAY's wire OID — gated in pgwire, where the wire is), #1117 (real
// arithmetic), #1118 (SUM(real) windowed), #1119 (a bare wide literal).

type ndCell struct {
	name, sql string
	want      string
	// pin overrides want for the named arm, with the mechanism in why.
	pin map[string]string
	// digits rounds this cell's float renderings to that many significant
	// figures, borrowing arc NV's own knob. A cell sets it when its answer is
	// a float whose LAST digits move with the ORDER the rows reach the
	// accumulator — ADR-0013's nondeterminism class 9 — and the cell is about
	// the DECLARATION rather than about those digits.
	digits int
	why    string
}

func TestNDDeclarationsMatchPostgres(t *testing.T) {
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
		{"single", func(s string) (string, error) { return ndRenderSingle(ctx, single, s) }},
		{"budget", func(s string) (string, error) { return ndRenderSingle(ctx, spilled, s) }},
		{"dag", func(s string) (string, error) { return ndRenderDAG(ctx, coord, s) }},
		{"dagshuf", func(s string) (string, error) { return ndRenderDAG(ctx, coordB, s) }},
		{"morsel", func(s string) (string, error) { return ndRenderDAG(ctx, coordM, s) }},
	}

	for _, tc := range ndCells() {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				nvDigits = tc.digits
				got, err := arm.run(tc.sql)
				nvDigits = 0
				if err != nil {
					got = "ERR " + ndFirstLine(err.Error())
				}
				if ndDiscover {
					fmt.Printf("NDC\t%s\t%s\t%s\n", tc.name, arm.name, got)
					continue
				}
				want := tc.want
				if p, ok := tc.pin[arm.name]; ok {
					want = p
				}
				if got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s",
						tc.sql, arm.name, got, want, ndWhy(tc.why))
				}
			}
		})
	}
}

func ndWhy(why string) string {
	if why == "" {
		return ""
	}
	return "\n  pinned: " + why
}

func ndFirstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// ndRender writes a result as its DECLARATION followed by its rows' BOXES.
//
// Both halves are load-bearing and neither can stand in for the other. The
// declaration is what a client binds on — `v:INT64` and `v:INT32` carry the
// same digits under OIDs 20 and 23 — and the box is what says the engine
// actually produced a value of the type it declared, which is the failure a
// declaration-only census cannot see (a FLOAT32 column holding a float64, or
// an INT32 declaration over an int64 the store then wrapped).
func ndRender(cols []ndCol, rows [][]any) string {
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(c.name + ":" + c.typ)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		parts := make([]string, 0, len(r))
		for _, v := range r {
			parts = append(parts, nvBox(v))
		}
		out = append(out, strings.Join(parts, ","))
	}
	sort.Strings(out)
	if len(out) > 0 {
		b.WriteString(" | " + strings.Join(out, " | "))
	}
	return b.String()
}

type ndCol struct{ name, typ string }

func ndRenderSingle(ctx context.Context, db *wadjet.DB, sql string) (s string, err error) {
	defer func() {
		if r := recover(); r != nil {
			s, err = "", fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := db.Query(ctx, sql)
	if qerr != nil {
		return "", qerr
	}
	cols := make([]ndCol, len(out.Columns))
	for i, name := range out.Columns {
		typ := "?"
		if i < len(out.ColumnMetas) {
			typ = ndTypeName(out.ColumnMetas[i].TypeName,
				out.ColumnMetas[i].Precision, out.ColumnMetas[i].Scale)
		}
		cols[i] = ndCol{name, typ}
	}
	rows := make([][]any, 0, len(out.Rows))
	for i := range out.Rows {
		rows = append(rows, out.Cells(i))
	}
	return ndRender(cols, rows), nil
}

func ndRenderDAG(ctx context.Context, c *Coordinator, sql string) (s string, err error) {
	defer func() {
		if r := recover(); r != nil {
			s, err = "", fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := c.ExecuteSQL(ctx, sql)
	if qerr != nil {
		return "", qerr
	}
	if out.Error != "" {
		return "", fmt.Errorf("%s", out.Error)
	}
	sc := out.OutputSchema()
	cols := make([]ndCol, len(out.Columns))
	for i, name := range out.Columns {
		typ := "?"
		if i < len(sc) {
			typ = ndTypeName(sc[i].Type.String(), sc[i].Precision, sc[i].Scale)
		}
		cols[i] = ndCol{name, typ}
	}
	var rows [][]any
	st := out.Stream()
	defer st.Close()
	for {
		bb, berr := st.Next(ctx)
		if berr != nil {
			return "", berr
		}
		if bb == nil {
			break
		}
		rows = append(rows, bb.ToRowValues()...)
	}
	return ndRender(cols, rows), nil
}

func ndTypeName(name string, precision, scale int) string {
	if precision > 0 {
		return fmt.Sprintf("%s(%d,%d)", name, precision, scale)
	}
	return name
}

// ndDiscover prints each cell on each arm instead of asserting, which is how
// the table below was MEASURED before it was written.
var ndDiscover = os.Getenv("ND_DISCOVER") != ""
