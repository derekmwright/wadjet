// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// R0's differential over EXISTING coordinator arm tables (epic #1678,
// ADR-0051, deliverable 3), report mode only.
//
// With R0_ARMDIFF=1 (and R0_ARMDIFF_TABLE=<name>), every cell an arm
// table runs through armDumpRecord (tbRunDAG, ssRunDAG and the tables that
// call it directly) or reRunDAG is ALSO run once more on the same coordinator,
// on the stage DAG and on localD, and the pair is classified: agree, differ,
// routed (the DAG answered from L: not DAG evidence), localD not established,
// or both refused alike. The arm tables' own PostgreSQL oracles are in each
// table's own format and are not read here: this is DAG-versus-localD only.
// The answers are compared as bags (an arm table's ORDER BY is not known here),
// floats to a relative 1e-9. Off by default; it never changes what a table
// asserts. Like every R0 writer it takes no path (r0WriterOn): the report goes
// to r0_armdiff.tsv in ONE fresh directory per test process, made under
// os.TempDir() at the first recorded cell and printed once on stderr; any
// value other than 1 panics with the usage.

var (
	r0ArmDiffMu   sync.Mutex
	r0ArmDiffOnce sync.Once
	r0ArmDiffFile *os.File
	r0ArmDiffErr  error
)

func r0ArmDiffRecord(c *Coordinator, sql string) {
	// The hook has no *testing.T: a refused switch value or a failed write
	// panics, which fails the arm table that called it, rather than being
	// ignored.
	on, err := r0WriterOn("R0_ARMDIFF")
	if err != nil {
		panic(err)
	}
	if !on || c == nil {
		return
	}
	if os.Getenv("ARM_DUMP") != "" {
		return // the arm dump attributes route counters per cell; do not disturb it
	}
	r0ArmDiffMu.Lock()
	defer r0ArmDiffMu.Unlock()
	r0ArmDiffOnce.Do(func() {
		var path string
		r0ArmDiffFile, path, r0ArmDiffErr = r0NewArtifact("R0_ARMDIFF", "r0_armdiff.tsv")
		if r0ArmDiffErr == nil {
			fmt.Fprintf(os.Stderr, "R0_ARMDIFF: writing the artifact %s (review it; nothing is copied into testdata/)\n", path)
		}
	})
	if r0ArmDiffErr != nil {
		panic(r0ArmDiffErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	before, _ := r0RouteSum(c)
	dres, derr := c.ExecuteSQL(ctx, sql)
	after, routes := r0RouteSum(c)
	dag := r0Render(dres, derr)
	lres, rec, lerr := r0LocalD(c, ctx, sql)
	ld := r0Render(lres, lerr)
	cmp := r0SameRendered(dag, ld, false)
	var class string
	switch {
	case after != before:
		class = "routed"
	case rec.Status != "established" && rec.Frozen == nil:
		class = "not-planned(" + rec.Status + ")"
	case rec.Status != "established":
		class = "localD-not-established(" + rec.Status + ")"
	case cmp == "" && dag.err != "":
		class = "both-refuse-alike"
	case cmp == "":
		class = "agree"
	default:
		class = "differ(" + cmp + ")"
	}
	flat := strings.NewReplacer("\t", " ", "\n", " ")
	line := fmt.Sprintf("%s\t%s\t%s\troutes=%s\tdag_err=%s\tlocalD_err=%s\n", os.Getenv("R0_ARMDIFF_TABLE"), class,
		flat.Replace(sql), routes, flat.Replace(dag.err), flat.Replace(ld.err))
	// The file stays open for the process (unbuffered writes): it was created
	// O_EXCL in a fresh directory, and is never reopened.
	if _, err := r0ArmDiffFile.WriteString(line); err != nil {
		panic(fmt.Errorf("R0_ARMDIFF: %w", err))
	}
}
