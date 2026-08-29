package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
	"github.com/derekmwright/wadjet/wadjet"
)

// The STRUCTURAL half of #685, and the ADR-0010 amendment it forces.
//
// A `.wshf` header declares its schema ONCE and every chunk is read under it,
// so for a DECIMAL the header carries half of every value in the file: the
// chunk holds the unscaled integer and the header holds the scale. That makes
// the header a claim about the file's meaning, and a partial task that writes
// a header its stage's other partials contradict has written a file that means
// something other than what it holds.
//
// The reachable producer of such a file is the identity row: an ungrouped
// aggregate that consumed no rows still owes one row, and that row has no
// input vector to take a DECIMAL scale from. It shipped DECIMAL(0,0) for a
// column every other partial of the same stage declared DECIMAL(38,s) (#685).
//
// This gate reads the BYTES rather than the answer. It runs a query whose
// filter empties whole tasks, then parses every stage-output header the query
// left behind and asserts that the files of one stage agree, column for
// column, on what they declare — and that the DECIMAL columns declare the (p,s)
// the aggregate's rules say they should, over the whole legal (p,s) range
// rather than the one pair the failing query happened to use.
//
// The value half of #685 lives in TestFilteredDecimalAggregateTwoPath. Both
// are needed: the accumulator fix makes the ANSWER right even under a lying
// header, and this one keeps the header from lying.

const ephTable = "decempty"

// ephDecCols is the (p,s) matrix: the two bounds of the declaration
// (ADR-0024's 1 <= p <= 38, 0 <= s <= p), a scale-0 DECIMAL (whose bug would
// be invisible to a scale>0-only check), the two the decpair fixture uses, and
// the all-fraction corner where AVG's scale increment saturates at the cap.
var ephDecCols = []struct {
	name             string
	precision, scale int
}{
	{"d_1_0", 1, 0},
	{"d_9_2", 9, 2},
	{"d_18_4", 18, 4},
	{"d_38_0", 38, 0},
	{"d_38_10", 38, 10},
	{"d_38_38", 38, 38},
}

func ephSchema() parquet.Schema {
	cols := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	for _, c := range ephDecCols {
		cols = append(cols, parquet.Column{
			Name: c.name, Type: parquet.TypeDecimal,
			Precision: c.precision, Scale: c.scale, Nullable: true,
		})
	}
	// The other two PARAMETERIZED types, for the same question one type over:
	// a VECTOR's dimension and an ARRAY's element declaration are parameters a
	// bare TypeID cannot carry either, so an identity row could lose them the
	// same way. They ride here rather than in a test of their own because the
	// answer turns on the same file.
	cols = append(cols,
		parquet.Column{Name: "v4", Type: parquet.TypeVector, Dimension: 4, Nullable: true},
		parquet.Column{Name: "arrd", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeDecimal,
				Precision: 9, Scale: 2, Nullable: true}})
	return parquet.Schema{Columns: cols}
}

// ephRows is nine rows, so tmdWriteTables' four-chunk split makes three files
// of three — and `WHERE id < 4` then matches only the first, leaving TWO
// partial tasks with no input at all.
func ephData() []map[string]any {
	rows := make([]map[string]any, 0, 9)
	for i := 1; i <= 9; i++ {
		r := map[string]any{"id": int64(i)}
		for _, c := range ephDecCols {
			// A value whose unscaled integer differs per row and per column,
			// small enough for every precision in the matrix.
			r[c.name] = parquet.Decimal128{Hi: 0, Lo: uint64(i)}
		}
		r["v4"] = []float32{float32(i), 0.5, -0.25, 1}
		r["arrd"] = []any{parquet.Decimal128{Hi: 0, Lo: uint64(100 + i)}}
		rows = append(rows, r)
	}
	return rows
}

func TestEmptyPartialHeaderCarriesTheDeclaredDecimalParams(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	ephWriteTable(t, ctx, infra)
	coord := tmdCoordinator(t, ctx, infra)

	for _, c := range ephDecCols {
		t.Run(c.name, func(t *testing.T) {
			// One aggregate per rule, so every declaration the identity row
			// can make is covered: MIN/MAX keep the input's (p,s), SUM widens
			// the precision to the carrier's and keeps the scale, AVG
			// additionally widens the scale (ADR-0024 item 2).
			sql := fmt.Sprintf(
				"SELECT SUM(%[1]s) AS s, MIN(%[1]s) AS lo, MAX(%[1]s) AS hi, AVG(%[1]s) AS av "+
					"FROM %[2]s WHERE id < 4", c.name, ephTable)
			headers := ephRunAndReadHeaders(t, ctx, infra, coord, sql)
			if len(headers) == 0 {
				// Not a skip: a gate that quietly passes when it found
				// nothing to look at is a gate that cannot fail. The
				// coordinator leaves its stage outputs in the result bucket
				// under this configuration; if that changes, this test needs
				// a different way to reach the bytes, not a green tick.
				t.Fatal("the query left no stage-output .wshf files to inspect — this gate " +
					"reads the bytes a partial task wrote, and has nothing to read")
			}
			ephAssertStagesAgree(t, headers)
			want := map[string][2]int{
				"s":  {batch.MaxDecimalPrecision, c.scale},
				"lo": {c.precision, c.scale},
				"hi": {c.precision, c.scale},
				// The AVG leg travels as __avg_sum#av, at SUM's declaration.
				"__avg_sum#av": {batch.MaxDecimalPrecision, c.scale},
			}
			ephAssertDeclared(t, headers, want)
		})
	}

	// The parameterized non-DECIMAL types, asked the same question through the
	// only observable they have: a VECTOR's dimension and an ARRAY's element
	// type are NOT in the .wshf header at all — the container payload is
	// self-describing and batch.SyncContainerSchema rebuilds the shape on
	// read. So there is nothing for an identity row to lose there, and the way
	// to prove it is the answer, not the header.
	t.Run("parameterized_containers", func(t *testing.T) {
		single := ephStandalone(t, ctx)
		for _, sql := range []string{
			"SELECT MIN(v4) AS v FROM " + ephTable + " WHERE id < 4",
			"SELECT MAX(v4) AS v FROM " + ephTable + " WHERE id < 4",
			"SELECT MIN(arrd) AS v FROM " + ephTable + " WHERE id < 4",
			"SELECT MAX(arrd) AS v FROM " + ephTable + " WHERE id < 4",
			// And with NO task matching, where every partial is an identity
			// row and the merge has nothing but them.
			"SELECT MIN(v4) AS v FROM " + ephTable + " WHERE id < 0",
			"SELECT MIN(arrd) AS v FROM " + ephTable + " WHERE id < 0",
		} {
			sRes, sErr := tmdRunSingle(ctx, single, sql)
			dRes, dErr := tmdRunDAG(ctx, coord, sql)
			if sErr != nil || dErr != nil {
				t.Errorf("%s: single err=%v, DAG err=%v", sql, sErr, dErr)
				continue
			}
			if fmt.Sprintf("%v", sRes.Rows) != fmt.Sprintf("%v", dRes.Rows) {
				t.Errorf("%s: single %v vs DAG %v — a parameterized container lost its "+
					"declaration on one path", sql, sRes.Rows, dRes.Rows)
			}
		}
	})
}

// ephWriteTable writes the fixture as several parquet chunks, the way
// tmdWriteTables does for the shared corpus. Written here rather than added to
// tmdTables() because this gate reads the OBJECT STORE and wants nothing in it
// but its own query's leavings.
func ephWriteTable(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	schema, rows := ephSchema(), ephData()
	if err := infra.cat.CreateTable(ctx, ephTable, schema, nil); err != nil {
		t.Fatalf("create %s: %v", ephTable, err)
	}
	const chunks = 4
	per := (len(rows) + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c*per < len(rows); c++ {
		lo, hi := c*per, min(c*per+per, len(rows))
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", ephTable, c)
		payload := buf.Bytes()
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(payload)),
			NumRows: int64(hi - lo), CreatedAt: time.Now(),
		})
	}
	if len(entries) != 3 {
		t.Fatalf("the fixture wrote %d chunks, not 3 — `WHERE id < 4` no longer leaves a "+
			"partial task with no input and this gate would prove nothing", len(entries))
	}
	if err := infra.cat.AddFiles(ctx, ephTable, map[string]string{}, "tables/"+ephTable+"/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}

// ephStandalone is the single-process arm over the same rows.
func ephStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema, rows := ephSchema(), ephData()
	if err := db.CreateTable(ctx, ephTable, schema, nil); err != nil {
		t.Fatalf("create %s: %v", ephTable, err)
	}
	ing := db.NewIngester(ephTable, schema, nil, ingest.Config{
		MaxBufferRows: len(rows) + 1, RowGroupSize: 4,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest %s: %v", ephTable, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", ephTable, err)
	}
	return db
}

// ephStageHeader is one stage-output file's declared schema.
type ephStageHeader struct {
	stage  string // the key's directory, i.e. queries/<qid>/<stage>
	key    string
	schema []parquet.Column
}

// ephRunAndReadHeaders runs sql on the stage DAG and parses the header of every
// .wshf object THIS query added to the result bucket. Scoped by a before/after
// key diff rather than by prefix: earlier sub-tests leave their own stage
// outputs behind, and mixing two queries' files into one comparison would
// report a disagreement between two different relations.
func ephRunAndReadHeaders(t *testing.T, ctx context.Context, infra tmdInfraT, coord *Coordinator, sql string) []ephStageHeader {
	t.Helper()
	before := ephStageKeys(t, ctx, infra)
	if _, err := tmdRunDAG(ctx, coord, sql); err != nil {
		t.Fatalf("stage DAG refused %q: %v", sql, err)
	}
	return ephReadStageHeaders(t, ctx, infra, before)
}

// ephStageKeys is the set of stage-output object keys currently in the bucket.
func ephStageKeys(t *testing.T, ctx context.Context, infra tmdInfraT) map[string]bool {
	t.Helper()
	objs, err := infra.store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		t.Fatalf("listing stage outputs: %v", err)
	}
	keys := make(map[string]bool, len(objs))
	for _, o := range objs {
		keys[o.Key] = true
	}
	return keys
}

// ephReadStageHeaders parses the header of every .wshf object in the result
// bucket that is not in skip.
func ephReadStageHeaders(t *testing.T, ctx context.Context, infra tmdInfraT, skip map[string]bool) []ephStageHeader {
	t.Helper()
	objs, err := infra.store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		t.Fatalf("listing stage outputs: %v", err)
	}
	var out []ephStageHeader
	for _, o := range objs {
		if !strings.HasSuffix(o.Key, ".wshf") || skip[o.Key] {
			continue
		}
		rc, _, err := infra.store.Get(ctx, "test", o.Key)
		if err != nil {
			t.Fatalf("get %s: %v", o.Key, err)
		}
		payload, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", o.Key, err)
		}
		raw, err := wshf.Decompress(payload)
		if err != nil {
			t.Fatalf("decompress %s: %v", o.Key, err)
		}
		cur := wshf.NewCursor(raw)
		schema, _, err := wshf.ParseHeader(&cur)
		if err != nil {
			t.Fatalf("parse header %s: %v", o.Key, err)
		}
		dir := o.Key
		if i := strings.LastIndexByte(dir, '/'); i >= 0 {
			dir = dir[:i]
		}
		out = append(out, ephStageHeader{stage: dir, key: o.Key, schema: schema})
	}
	return out
}

// ephAssertStagesAgree is the ADR-0010 amendment as a gate: the files of ONE
// stage must declare the same DECIMAL parameters for the same column. This is
// the check the writer's single-writer guard cannot make — each file here is
// internally consistent, and only the SET of them is contradictory.
func ephAssertStagesAgree(t *testing.T, headers []ephStageHeader) {
	t.Helper()
	byStage := map[string][]ephStageHeader{}
	for _, h := range headers {
		byStage[h.stage] = append(byStage[h.stage], h)
	}
	stages := make([]string, 0, len(byStage))
	for s := range byStage {
		stages = append(stages, s)
	}
	sort.Strings(stages)
	for _, st := range stages {
		files := byStage[st]
		first := files[0]
		for _, f := range files[1:] {
			if len(f.schema) != len(first.schema) {
				t.Errorf("stage %s: %s has %d columns, %s has %d",
					st, f.key, len(f.schema), first.key, len(first.schema))
				continue
			}
			for i := range f.schema {
				a, b := first.schema[i], f.schema[i]
				if a.Type != parquet.TypeDecimal || b.Type != parquet.TypeDecimal {
					continue
				}
				if a.Name != b.Name {
					t.Errorf("stage %s column %d: %q in %s, %q in %s", st, i, a.Name, first.key, b.Name, f.key)
					continue
				}
				if a.Precision != b.Precision || a.Scale != b.Scale {
					t.Errorf("stage %s: column %q is DECIMAL(%d,%d) in %s but DECIMAL(%d,%d) in %s — "+
						"one stage's files declare two different meanings for one column, and the "+
						"consumer reads them all under whichever it saw first (ADR-0010, #685)",
						st, a.Name, b.Precision, b.Scale, f.key, a.Precision, a.Scale, first.key)
				}
			}
		}
		// Nothing may declare a zero precision: that is the "unconstrained"
		// sentinel, and a column that can hold no digits is not a description
		// of a column that holds values.
		for _, f := range files {
			for _, c := range f.schema {
				if c.Type == parquet.TypeDecimal && c.Precision <= 0 {
					t.Errorf("%s declares column %q as DECIMAL(%d,%d) — precision 0 is the "+
						"undeclared sentinel, which is exactly what #685's identity row wrote",
						f.key, c.Name, c.Precision, c.Scale)
				}
			}
		}
	}
}

// ephAssertDeclared checks the (p,s) each named output column carries, in
// EVERY file that has it — the identity row included.
func ephAssertDeclared(t *testing.T, headers []ephStageHeader, want map[string][2]int) {
	t.Helper()
	seen := map[string]int{}
	for _, h := range headers {
		for _, c := range h.schema {
			w, named := want[c.Name]
			if !named {
				continue
			}
			seen[c.Name]++
			if c.Type != parquet.TypeDecimal {
				t.Errorf("%s: column %q is %v, want DECIMAL", h.key, c.Name, c.Type)
				continue
			}
			if c.Precision != w[0] || c.Scale != w[1] {
				t.Errorf("%s: column %q is DECIMAL(%d,%d), want DECIMAL(%d,%d)",
					h.key, c.Name, c.Precision, c.Scale, w[0], w[1])
			}
		}
	}
	for name := range want {
		if seen[name] == 0 {
			t.Errorf("no stage-output file declared column %q at all — this gate inspected "+
				"%d files and none of them is the aggregate's output", name, len(headers))
		}
	}
}
