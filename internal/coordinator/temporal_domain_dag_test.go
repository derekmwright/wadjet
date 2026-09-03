package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #826's DAG arm: the temporal DOMAIN is a property of the DECLARATION, so
// it cannot depend on which engine path evaluated the predicate.
//
// The single-process gate lives in wadjet/temporal_domain_test.go and
// carries the base-FAIL evidence. This one asks the narrower question
// position 5 named — "all arms" — because the fix lands in a comparison
// layer the DAG reaches through a different builder (the worker's fragment
// executor compiles its own operators from the stage spec), and "the
// expression layer is below the distribution decision" is a claim about the
// code rather than something a gate had established.
//
// Both spellings of each predicate must agree with each other AND with the
// single-process answer.
func TestTemporalDomainAgreesOnTheDistributedArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	writeTemporalDomainTable(t, ctx, infra)
	coord := tmdCoordinator(t, ctx, infra)

	// The same fixture the single-process gate uses: s and ds carry the
	// literal each column arm is paired with, on every non-NULL row.
	const nearEpoch = "1970-01-01T00:00:01Z"
	const farDate = "3500-01-01"

	for _, tc := range []struct {
		name             string
		literal, colPair string
		want             int64
	}{
		{"near_epoch_ts_equality",
			`SELECT COUNT(*) AS n FROM tdom WHERE ts = '` + nearEpoch + `'`,
			`SELECT COUNT(*) AS n FROM tdom WHERE ts = s`, 2},
		{"near_epoch_ts_less_than",
			`SELECT COUNT(*) AS n FROM tdom WHERE ts < '` + nearEpoch + `'`,
			`SELECT COUNT(*) AS n FROM tdom WHERE ts < s`, 1},
		{"far_date_equality",
			`SELECT COUNT(*) AS n FROM tdom WHERE d = '` + farDate + `'`,
			`SELECT COUNT(*) AS n FROM tdom WHERE d = ds`, 2},
		{"far_date_greater_than",
			`SELECT COUNT(*) AS n FROM tdom WHERE d > '` + farDate + `'`,
			`SELECT COUNT(*) AS n FROM tdom WHERE d > ds`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lit := dagCount(t, ctx, coord, tc.literal)
			col := dagCount(t, ctx, coord, tc.colPair)
			if lit != col {
				t.Fatalf("on the DAG, one predicate has two answers: literal %d, column %d\n"+
					"  literal: %s\n  column:  %s\n"+
					"  The temporal domain comes from the DECLARED type on every arm (#826).",
					lit, col, tc.literal, tc.colPair)
			}
			if lit != tc.want {
				t.Fatalf("the DAG answers %d, want %d (the single-process arm's answer)", lit, tc.want)
			}
		})
	}
}

func dagCount(t *testing.T, ctx context.Context, coord *Coordinator, sql string) int64 {
	t.Helper()
	res, err := tmdRunDAG(ctx, coord, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s returned %d rows, want 1", sql, len(res.Rows))
	}
	switch v := res.Rows[0]["n"].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		t.Fatalf("%s: n is %T", sql, v)
		return 0
	}
}

// writeTemporalDomainTable writes the #826 fixture across several files so
// the DAG really fans the scan out; a single-file table would hide a
// per-task difference by accident.
func writeTemporalDomainTable(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	const nearEpoch = "1970-01-01T00:00:01Z"
	const farDate = "3500-01-01"
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ds", Type: parquet.TypeString, Nullable: true},
	}}
	rows := []map[string]any{
		{"id": int64(1), "ts": nearEpoch, "s": nearEpoch, "d": farDate, "ds": farDate},
		{"id": int64(2), "ts": nearEpoch, "s": nearEpoch, "d": farDate, "ds": farDate},
		{"id": int64(3), "ts": "2024-06-01T12:00:00Z", "s": nearEpoch, "d": "2024-06-01", "ds": farDate},
		{"id": int64(4), "ts": "1999-01-01T00:00:00Z", "s": nearEpoch, "d": "1999-01-01", "ds": farDate},
		{"id": int64(5), "ts": nil, "s": nil, "d": nil, "ds": nil},
		{"id": int64(6), "ts": "1970-01-01T00:00:00.500Z", "s": nearEpoch, "d": "4000-01-01", "ds": farDate},
	}
	if err := infra.cat.CreateTable(ctx, "tdom", schema, nil); err != nil {
		t.Fatalf("create tdom: %v", err)
	}
	var entries []catalog.FileEntry
	for c := 0; c < 3; c++ {
		lo, hi := c*2, c*2+2
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
		path := fmt.Sprintf("tables/tdom/chunk_%04d.parquet", c)
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
	if err := infra.cat.AddFiles(ctx, "tdom", map[string]string{}, "tables/tdom/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}
