// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The arm-table dump: with ARM_DUMP=<path>, every coordinator cell an arm
// table runs is appended to <path> as arm<TAB>sql<TAB>columns<TAB>answer —
// the declared column names and types, the rendered rows, or the error's
// SQLSTATE and full text. It exists to compare a whole arm table at two
// commits line for line (ADR-0050 Acceptance 4: with the fast path off, every
// DAG-arm cell identical at base and tip), which the tables' own assertions
// do not do: they compare against PostgreSQL's text or a kept pin, an error by
// SQLSTATE and a fragment of its message.

var (
	armDumpNames sync.Map // *Coordinator -> arm name
	armDumpMu    sync.Mutex
	armDumpOut   *os.File
	armDumpErr   error
	armDumpOnce  sync.Once
	// armDumpRoutes is each coordinator's route-counter total at its last
	// dumped cell, under armDumpMu.
	armDumpRoutes = map[*Coordinator]int64{}
)

// armDumpName names c's arm in the dump.
func armDumpName(c *Coordinator, name string) { armDumpNames.Store(c, name) }

// armDumpNamed is armDumpName returning c, for an arm built inline.
func armDumpNamed(name string, c *Coordinator) *Coordinator {
	armDumpName(c, name)
	return c
}

// armDumpRecord appends one cell when ARM_DUMP is set.
func armDumpRecord(c *Coordinator, sql string, schema []parquet.Column, answer string, err error) {
	path := os.Getenv("ARM_DUMP")
	if path == "" {
		return
	}
	armDumpOnce.Do(func() {
		armDumpOut, armDumpErr = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	})
	if armDumpErr != nil {
		return
	}
	name := "unnamed"
	if v, ok := armDumpNames.Load(c); ok {
		name = v.(string)
	}
	cols := make([]string, len(schema))
	for i, col := range schema {
		cols[i] = fmt.Sprintf("%s:%d", col.Name, col.Type)
		if col.ElementType != nil {
			cols[i] += fmt.Sprintf("/%d", col.ElementType.Type)
		}
	}
	if err != nil {
		answer = "ERR " + sqlerr.StateOf(err) + " " + err.Error()
	}
	flat := func(s string) string { return strings.NewReplacer("\n", " ", "\t", " ").Replace(s) }
	// Whether a refused-plan route answered: the coordinator's route counters
	// against what they read at its previous cell (one coordinator runs its
	// cells one at a time).
	routes := c.CorrelatedLocalRoutes() + c.DistinctLocalRoutes() + c.GroupingSetsLocalRoutes() +
		c.GroupKeyLocalRoutes() + c.InSubqueryLocalRoutes() + c.ScalarProjectionLocalRoutes() +
		c.NullAwareAntiLocalRoutes() + c.UnreachableOutputLocalRoutes() +
		c.LateralProjectionLocalRoutes() + c.TableLessLocalRoutes() +
		c.UnbuildableStageLocalRoutes() + c.ResidualSidesLocalRoutes() +
		c.PolicedWindowLocalRoutes() + c.WindowOverLateralLocalRoutes() +
		c.LateralIdentityLocalRoutes()
	armDumpMu.Lock()
	defer armDumpMu.Unlock()
	routed := "direct"
	if routes > armDumpRoutes[c] { // a fresh coordinator's counters start at 0
		routed = "routed"
	}
	armDumpRoutes[c] = routes
	fmt.Fprintf(armDumpOut, "%s\t%s\t%s\t%s\t%s\n", name, routed, flat(sql), strings.Join(cols, ","), flat(answer))
}
