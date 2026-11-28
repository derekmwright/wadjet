// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1677 line r on every door: a join's build-key scan filters never prune
// through a TYPE change. The grid is the embedded test's
// (wadjet/testdata/arc_1677r_cells.tsv, PostgreSQL 17.11's answers; see
// wadjet.TestArc1677rScanFiltersNeverPruneThroughATypeChange), over the same
// fixture written as tmdTables, on ejArmsOver's arms — one process unbudgeted
// and under 512 KiB, the stage DAG broadcast / shuffled / morsel / skew-split,
// the small-query fast path, the asynchronous door with and without probe
// split — plus the DAG with its own dynamic filters on
// (Config.DynamicFilters, the opt-in WADJET_DYNAMIC_FILTERS consumer). Every
// arm runs with the hash-join build forced onto the one-CPU routes that record
// the build keys' min/max (exec.ForceSerialJoinBuildForTest; the in-process
// workers take it too), so a many-core host exercises what `taskset -c 0`
// does.
func TestArc1677rScanFilterKeyTypesEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("every arm over 432 cells")
	}
	ctx := context.Background()
	cells := arc1677rCells(t)
	defer exec.ForceSerialJoinBuildForTest()()
	arms := ejArmsOver(t, ctx, arc1677rTables())
	stand := func() *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, arc1677rTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, nil, func(c *Config) { c.DynamicFilters = true })
	}
	dyn := stand()
	arms = append(arms, ejArm{"dag-dynfilters", func(s string) string {
		res, err := dyn.ExecuteSQL(ctx, s)
		if err != nil {
			return tbErr(err)
		}
		if res.Error != "" {
			return "ERR (uncoded) " + res.Error
		}
		rows, err := tbdCollect(ctx, res)
		if err != nil {
			return tbErr(err)
		}
		return ejRender(rows)
	}})
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			for _, c := range cells {
				want := "rows=1 " + c[1]
				if got := arm.run(c[2]); got != want {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c[0], c[2], got, want)
				}
			}
		})
	}
}

func arc1677rCells(t *testing.T) [][3]string {
	t.Helper()
	raw, err := os.ReadFile("../../wadjet/testdata/arc_1677r_cells.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var cells [][3]string
	for _, l := range strings.Split(string(raw), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.SplitN(l, "\t", 3)
		if len(f) != 3 {
			t.Fatalf("malformed cell %q", l)
		}
		cells = append(cells, [3]string{f[0], f[1], f[2]})
	}
	return cells
}

// arc1677rTables is wadjet/testdata/arc_1677r_fixture.sql as tmdTables: xp
// holds one value of every key type per row, xb's 'k' row matches xp row 3.
func arc1677rTables() []tmdTable {
	schema := func(first parquet.Column) parquet.Schema {
		return parquet.Schema{Columns: []parquet.Column{first,
			{Name: "i4", Type: parquet.TypeInt32, Nullable: true},
			{Name: "i8", Type: parquet.TypeInt64, Nullable: true},
			{Name: "f8", Type: parquet.TypeFloat64, Nullable: true},
			{Name: "f4", Type: parquet.TypeFloat32, Nullable: true},
			{Name: "m", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
			{Name: "w", Type: parquet.TypeDecimal, Precision: 12, Scale: 4, Nullable: true},
			{Name: "d", Type: parquet.TypeDate, Nullable: true},
			{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
			{Name: "s", Type: parquet.TypeString, Nullable: true},
		}}
	}
	row := func(i int, day string, ts string) map[string]any {
		return map[string]any{"i4": int32(i), "i8": int64(i), "f8": float64(i), "f4": float32(i),
			"m": float64(i), "w": float64(i), "d": day, "ts": ssMS(ts), "s": fmt.Sprintf("s%02d", i)}
	}
	var xp []map[string]any
	for i := 1; i <= 10; i++ {
		day := fmt.Sprintf("2024-01-%02d", i)
		r := row(i, day, day+" 00:00:00")
		r["id"] = int32(i)
		xp = append(xp, r)
	}
	k := row(3, "2024-01-03", "2024-01-03 00:00:00")
	k["tag"] = "k"
	z := row(50, "2025-06-01", "2025-06-01 12:00:00")
	z["tag"] = "z"
	return []tmdTable{
		{"xp", schema(parquet.Column{Name: "id", Type: parquet.TypeInt32}), xp},
		{"xb", schema(parquet.Column{Name: "tag", Type: parquet.TypeString}), []map[string]any{k, z}},
	}
}
