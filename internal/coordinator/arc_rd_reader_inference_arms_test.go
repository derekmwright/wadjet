// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC RD ON FIVE ARMS (#1242) — the readers' position past the inference
// sample on single / single+budget / dag / dag-shuffled / dag+morsel4: by
// default a value of another type is 22P02 and a key first seen past the
// sample 22P04; `sample_size = -1` answers the same files with the column
// widened and the key a column. Each cell runs with the plan-time schema read
// (the execution reuses the plan's whole-input inference) and without it
// (WADJET_TEST_NO_READER_SCHEMA=1: the execution infers from every row
// itself). At 9420d256 the key cells answered the row without the key and
// the -1 cells did not parse.
func TestArcRDReaderPastSampleOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	dir := t.TempDir()
	const R = 2200
	var key, float strings.Builder
	float.WriteString("id,a\n")
	for i := 1; i <= R; i++ {
		if i == R {
			fmt.Fprintf(&key, "{\"id\":%d,\"a\":%d,\"k\":7}\n", i, i)
			fmt.Fprintf(&float, "%d,0.75\n", i)
			continue
		}
		fmt.Fprintf(&key, "{\"id\":%d,\"a\":%d}\n", i, i)
		fmt.Fprintf(&float, "%d,%d\n", i, i)
	}
	kp, fp := filepath.Join(dir, "key.json"), filepath.Join(dir, "float.csv")
	if err := os.WriteFile(kp, []byte(key.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fp, []byte(float.String()), 0o644); err != nil {
		t.Fatal(err)
	}

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
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return ptArmRun(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget", func(sql string) ([]string, error) { return ptArmRun(tmdRunSingle(ctx, spilled, sql)) }},
		{"dag", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coordM, sql)) }},
	}
	S := (R - 1) * R / 2
	cells := []struct {
		name, sql, state, want string
	}{
		{"value_default", fmt.Sprintf("SELECT SUM(a) AS s FROM read_csv('%s')", fp), "22P02",
			fmt.Sprintf(`row %d column "a": value "0.75" (double precision) is not of type bigint`, R)},
		{"key_default", fmt.Sprintf("SELECT COUNT(*) AS n FROM read_json('%s')", kp), "22P04",
			fmt.Sprintf(`row %d: key "k" is not a column of the relation`, R)},
		{"value_whole", fmt.Sprintf("SELECT SUM(a) AS s FROM read_csv('%s', sample_size = -1)", fp), "",
			fmt.Sprintf("s=float64:%v", float64(S)+0.75)},
		{"key_whole", fmt.Sprintf("SELECT k, id FROM read_json('%s', sample_size = -1) WHERE k IS NOT NULL", kp), "",
			fmt.Sprintf("k=int64:7|id=int64:%d", R)},
		{"value_whole_join", fmt.Sprintf("SELECT COUNT(*) AS n FROM typemx t JOIN read_csv('%s', sample_size = -1) r ON r.id = t.id WHERE r.a > 0", fp), "",
			"n=int64:"},
	}
	for _, planRead := range []bool{true, false} {
		mode := "plan_read"
		if !planRead {
			mode = "no_plan_read"
		}
		t.Run(mode, func(t *testing.T) {
			if !planRead {
				t.Setenv("WADJET_TEST_NO_READER_SCHEMA", "1")
			}
			for _, c := range cells {
				t.Run(c.name, func(t *testing.T) {
					answers := map[string]string{}
					for _, arm := range arms {
						got, err := arm.run(c.sql)
						if c.state != "" {
							if sqlerr.StateOf(err) != c.state || !strings.Contains(err.Error(), c.want) {
								t.Errorf("%s: want %s %q, got %v %v", arm.name, c.state, c.want, got, err)
							}
							continue
						}
						if err != nil {
							t.Errorf("%s: %v", arm.name, err)
							continue
						}
						answers[arm.name] = strings.Join(got, ";")
						if !strings.HasPrefix(answers[arm.name], c.want) {
							t.Errorf("%s: got %v, want %s", arm.name, got, c.want)
						}
					}
					// Every arm answers alike.
					for name, a := range answers {
						if a != answers["single"] {
							t.Errorf("%s answered %s, single %s", name, a, answers["single"])
						}
					}
				})
			}
		})
	}
}
