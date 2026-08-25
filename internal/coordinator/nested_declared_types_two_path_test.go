package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #589 across the two execution paths.
//
// An IPv6 or a UUID inside a ROW, an ARRAY or a MAP read back as the EMPTY
// STRING, because the reader's declared-schema overlay — the mechanism that
// restores the nine types parquet cannot annotate — walked only the top-level
// columns. Both paths read the same files through the same parquet reader, so
// both were wrong in the same way and a differential ALONE could not see it.
//
// This gate therefore has two arms of its own:
//
//   - the DIFFERENTIAL: every corpus entry answered by the single-process
//     engine and by the three-worker stage DAG, compared. That is what catches
//     a fix that lands on one path and not the other — the DAG takes a
//     column's type from the FILE (#396/#423) where the single-process engine
//     takes it from the CATALOG, so a container leaf is exactly the place the
//     two can disagree.
//   - the VALUE ANCHOR, run on BOTH arms: the fixture writes the same value to
//     a top-level column and into every container position, so a path that
//     lost the nested one disagrees with its own flat column. This is the half
//     that fails when both engines are wrong together.
func TestNestedDeclaredTypesTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the #589 two-path gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	ndeclWriteTable(t, ctx, infra)
	coord := tmdCoordinator(t, ctx, infra)
	single, err := wadjet.Open(ctx, wadjet.Config{
		Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
	})
	if err != nil {
		t.Fatalf("open single-process arm over the shared catalog: %v", err)
	}
	t.Cleanup(func() { single.Close() })

	t.Run("value_anchor", func(t *testing.T) {
		const sql = `SELECT id, flat_ipv6, flat_uuid, flat_cidr, rw, arr_ipv6, arr_uuid, m_ipv6
			FROM ` + typematrix.NestedDeclared + ` ORDER BY id`
		aRes, aErr := tmdRunSingle(ctx, single, sql)
		if aErr != nil {
			t.Fatalf("single-process arm: %v", aErr)
		}
		bRes, bErr := tmdRunDAG(ctx, coord, sql)
		if bErr != nil {
			t.Fatalf("stage DAG arm: %v", bErr)
		}
		ndeclAssertAnchored(t, "single-process", aRes.Rows)
		ndeclAssertAnchored(t, "stage DAG", bRes.Rows)
	})

	ratchet := newTmdRatchet()
	var compared, diverged int
	for _, q := range typematrix.NestedDeclaredCorpus() {
		q := q
		t.Run(q.Name, func(t *testing.T) {
			aRes, aErr := tmdRunSingle(ctx, single, q.SQL)
			bRes, bErr := tmdRunDAG(ctx, coord, q.SQL)

			pin, pinned := ndeclPins[q.Name]
			fail := func(format string, args ...any) {
				if pinned {
					ratchet.observe(q.Name, true)
					t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  "+format,
						append([]any{pin.Issue, pin.Reason}, args...)...)
					return
				}
				t.Errorf(format, args...)
			}

			if aErr != nil && bErr != nil {
				t.Fatalf("both arms refuse this shape — it can say nothing about #589 and "+
					"should not be in the corpus: single=%v dag=%v\n  SQL: %s", aErr, bErr, q.SQL)
			}
			if aErr != nil {
				fail("the single-process arm FAILED on a query the stage DAG answered (%d rows): %v\n  SQL: %s",
					len(bRes.Rows), aErr, q.SQL)
				return
			}
			if bErr != nil {
				fail("the stage DAG FAILED on a query the single-process arm answered (%d rows): %v\n  SQL: %s",
					len(aRes.Rows), bErr, q.SQL)
				return
			}
			compared++
			if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: q.Mode}); diff != "" {
				diverged++
				fail("TWO-PATH DIVERGENCE over a parquet-inexpressible type inside a "+
					"container (#589)\n  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					q.SQL, diff, tmdRender(aRes, 3), tmdRender(bRes, 3))
				return
			}
			if pinned {
				ratchet.observe(q.Name, false)
			}
		})
	}
	ratchet.finish(t, ndeclPins, "#589 two-path")
	t.Logf("#589 two-path gate: %d shapes compared over IPv6/UUID/CIDR below ROW, ARRAY and MAP, "+
		"%d diverged, %d pinned", compared, diverged, len(ndeclPins))
	if compared == 0 {
		t.Fatal("no entry was compared on both arms — the gate proves nothing")
	}
}

// ndeclPins are the divergences this corpus found that are NOT #589 and are
// tracked on their own issues. Same two-way ratchet as every other gate here
// (ADR-0013 §Pins): a pinned entry is still RUN and still COMPARED — the
// divergence is logged instead of failed — and the ratchet FAILS when the two
// arms start agreeing. Deleting a pin is the fix's proof, and a pin naming an
// entry the corpus does not contain fails too.
//
// ndeclPins is empty. Two issues this corpus found are now both fixed and
// their pins deleted, which is the ratchet's proof (ADR-0013 §Pins):
//
//   - #606 (a container GROUP BY divergence) — fixed by #566 (ADR-0023).
//   - #605 (the stage DAG dropped or refused a ROW field path: a projected
//     field came back missing, a WHERE on one answered zero rows, an ORDER BY
//     on one failed with `column "rw.f_uuid" does not exist in the input
//     schema`) — fixed by #568, whose DAG half is exactly this: the planner
//     keeps the parent column alive through pruning, exec.ColumnRef resolves
//     the field, and the vectorized filters carry a row-at-a-time fallback.
//     All five #605 pins agreed once #568 landed and are gone.
var ndeclPins = map[string]typematrix.Pin{}

// ndeclAssertAnchored is the half a differential cannot supply: within ONE
// arm's own answer, every container position must hold what the flat column
// beside it holds. Before the fix every one of them held "".
func ndeclAssertAnchored(t *testing.T, arm string, rows []map[string]any) {
	t.Helper()
	if len(rows) != typematrix.NestedDeclaredRows {
		t.Fatalf("%s: read %d rows, the fixture has %d", arm, len(rows), typematrix.NestedDeclaredRows)
	}
	empties, checked := 0, 0
	for _, r := range rows {
		id := r["id"]
		anchors := map[string]any{
			"ipv6": r["flat_ipv6"], "uuid": r["flat_uuid"], "cidr": r["flat_cidr"],
		}
		eq := func(where, anchor string, have any) {
			t.Helper()
			checked++
			want := anchors[anchor]
			if !reflect.DeepEqual(have, want) {
				t.Errorf("%s: id=%v %s read back %#v; the same value in flat_%s reads back %#v",
					arm, id, where, have, anchor, want)
			}
			if s, ok := have.(string); ok && s == "" && want != nil {
				empties++
			}
		}
		row, _ := r["rw"].(map[string]any)
		eq("rw.f_ipv6", "ipv6", ndeclField(row, "f_ipv6"))
		eq("rw.f_uuid", "uuid", ndeclField(row, "f_uuid"))
		eq("rw.f_cidr", "cidr", ndeclField(row, "f_cidr"))
		nested, _ := ndeclField(row, "f_nested").(map[string]any)
		eq("rw.f_nested.n_ipv6", "ipv6", ndeclField(nested, "n_ipv6"))
		eq("rw.f_nested.n_uuid", "uuid", ndeclField(nested, "n_uuid"))
		eq("arr_ipv6[0]", "ipv6", ndeclFirst(r["arr_ipv6"]))
		eq("arr_uuid[0]", "uuid", ndeclFirst(r["arr_uuid"]))
		eq("m_ipv6[k]", "ipv6", ndeclMapValue(r["m_ipv6"]))
	}
	if empties > 0 {
		t.Errorf("%s: %d container positions read back the empty string — #589's signature", arm, empties)
	}
	t.Logf("%s: %d container positions compared against their flat anchor", arm, checked)
}

func ndeclField(row map[string]any, field string) any {
	if row == nil {
		return nil
	}
	return row[field]
}

func ndeclFirst(v any) any {
	a, ok := v.([]any)
	if !ok || len(a) == 0 {
		return nil
	}
	return a[0]
}

func ndeclMapValue(v any) any {
	a, ok := v.([]any)
	if !ok || len(a) == 0 {
		return nil
	}
	e, ok := a[0].(map[string]any)
	if !ok {
		return nil
	}
	return e["value"]
}

// ndeclWriteTable writes the #589 fixture into infra's store and catalog,
// several files so the DAG really fans the scan out across tasks.
func ndeclWriteTable(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	schema := typematrix.NestedDeclaredSchema()
	rows := typematrix.NestedDeclaredData(typematrix.NestedDeclaredRows)
	if err := infra.cat.CreateTable(ctx, typematrix.NestedDeclared, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.NestedDeclared, err)
	}
	const chunks = 4
	n := len(rows)
	per := (n + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, min(c*per+per, n)
		if lo >= hi {
			break
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", typematrix.NestedDeclared, c)
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
	if err := infra.cat.AddFiles(ctx, typematrix.NestedDeclared, map[string]string{},
		"tables/"+typematrix.NestedDeclared+"/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}
