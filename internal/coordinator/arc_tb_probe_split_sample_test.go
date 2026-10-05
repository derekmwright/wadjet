// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcTBAsyncProbeSplitDrawsEachSampleOnce: on the async door a probe split
// hands the statement's TEXT to every task, each re-plans it over its share
// of the probe's files and reads every other relation whole — so a sampled
// build side, or a sampled IN / EXISTS / scalar subquery, was drawn once PER
// TASK, and one table's rows were joined against three different samples
// (#1411 measured case P1: ~740 of 1000 keys answered a count PostgreSQL cannot
// produce; base 9420d256 the same). A statement with a TABLESAMPLE runs as one
// task and draws each sample once.
//
// The fixture discriminates: pb_dup holds ids 1..1000 once in EACH of eight
// files, so with one draw of pb_k every id answers 0 or 8 rows, and with a
// draw per task it answers whatever the tasks holding its files drew.
func TestArcTBAsyncProbeSplitDrawsEachSampleOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the async door stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	orig := dagplan.ProbeSplitMinBytes
	dagplan.ProbeSplitMinBytes = 1
	t.Cleanup(func() { dagplan.ProbeSplitMinBytes = orig })

	infra := tmdInfra(t, ctx)
	var dup []map[string]any
	for f := 0; f < 8; f++ {
		dup = append(dup, tbRows(1000)...)
	}
	pbWrite(t, ctx, infra, "pb_dup", dup, 8)
	pbWrite(t, ctx, infra, "pb_k", tbRows(1000), 4)
	coord := tmdCoordinatorWithWorkers(t, ctx, infra, nil)

	// Non-vacuous: the same join WITHOUT a sample is probe-split here.
	if tasks := pbAsyncTasks(t, ctx, coord, "SELECT pb_dup.id FROM pb_dup JOIN pb_k ON pb_dup.id = pb_k.id"); tasks < 2 {
		t.Fatalf("the unsampled control ran as %d task(s); the fixture must probe-split", tasks)
	}

	for _, sql := range []string{
		"SELECT pb_dup.id FROM pb_dup JOIN pb_k TABLESAMPLE BERNOULLI (50) ON pb_dup.id = pb_k.id",
		"SELECT id FROM pb_dup WHERE id IN (SELECT id FROM pb_k TABLESAMPLE BERNOULLI (50))",
		"SELECT id FROM pb_dup WHERE id < (SELECT count(*) FROM pb_k TABLESAMPLE BERNOULLI (50))",
		"SELECT id FROM pb_dup WHERE EXISTS (SELECT 1 FROM pb_k TABLESAMPLE BERNOULLI (50) WHERE pb_k.id = 7)",
	} {
		for rep := 0; rep < 3; rep++ {
			rows := pbAsyncRows(t, ctx, coord, sql)
			per := map[string]int{}
			for _, r := range rows {
				per[fmt.Sprint(r[0])]++
			}
			notEight := 0
			for _, n := range per {
				if n != 8 {
					notEight++
				}
			}
			if notEight != 0 {
				t.Errorf("%s (run %d): %d of %d ids answer a count other than 0 or 8 (%d rows): each task drew its own sample",
					sql, rep, notEight, len(per), len(rows))
			}
		}
	}
	// The sampled PROBE still answers a sample of every file.
	for rep := 0; rep < 3; rep++ {
		rows := pbAsyncRows(t, ctx, coord, "SELECT count(*) FROM pb_dup TABLESAMPLE BERNOULLI (50) JOIN pb_k ON pb_dup.id = pb_k.id")
		if len(rows) != 1 {
			t.Fatalf("sampled probe: %d rows", len(rows))
		}
		if n, ok := rows[0][0].(int64); !ok || n < 3500 || n > 4500 {
			t.Errorf("sampled probe (run %d): count %v, want 3500..4500 (half of 8000)", rep, rows[0][0])
		}
	}
}

func pbWrite(t *testing.T, ctx context.Context, infra tmdInfraT, name string, rows []map[string]any, chunks int) {
	t.Helper()
	if err := infra.cat.CreateTable(ctx, name, tbSchema(), nil); err != nil {
		t.Fatal(err)
	}
	per := (len(rows) + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, min(c*per+per, len(rows))
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, tbSchema(), parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, c)
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, catalog.FileEntry{Path: path, SizeBytes: int64(buf.Len()), NumRows: int64(hi - lo), CreatedAt: time.Now()})
	}
	if err := infra.cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", entries); err != nil {
		t.Fatal(err)
	}
}

// pbAsyncTasks submits a statement on the async door and returns how many
// tasks its pipeline stage runs.
func pbAsyncTasks(t *testing.T, ctx context.Context, c *Coordinator, sql string) int {
	t.Helper()
	qid, _, err := c.SubmitSQL(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	pbAwait(t, c, qid)
	c.mu.Lock()
	defer c.mu.Unlock()
	meta := c.queryMetas[qid]
	if meta == nil || len(meta.stages) != 1 {
		t.Fatalf("%s: no single pipeline stage recorded", sql)
	}
	return meta.stages[0].Tasks
}

func pbAwait(t *testing.T, c *Coordinator, qid string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if info := c.tracker.Get(qid); info != nil &&
			(info.State == QueryStateCompleted || info.State == QueryStateFailed) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("query %s did not finish", qid)
}

func pbAsyncRows(t *testing.T, ctx context.Context, c *Coordinator, sql string) [][]any {
	t.Helper()
	qid, _, err := c.SubmitSQL(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	pbAwait(t, c, qid)
	res, err := c.GetQueryResults(ctx, qid)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if res.Error != "" {
		t.Fatalf("%s: %s", sql, res.Error)
	}
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				t.Fatalf("%s: %v", sql, berr)
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
		return cells
	}
	rows, err := res.Rows()
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	cols := res.OutputSchema()
	for _, r := range rows {
		row := make([]any, len(cols))
		for j, col := range cols {
			row[j] = r[col.Name]
		}
		cells = append(cells, row)
	}
	return cells
}
