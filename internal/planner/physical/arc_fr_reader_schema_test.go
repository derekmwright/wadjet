// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC FR — THE PLAN-TIME SCHEMA IS ASKED FOR ONLY UNDER AN AUTHORIZED
// CONTEXT, AND THE BATCHES ARE HELD TO IT.
//
// The SQL-door gates measure what a statement ANSWERS. These two measure the
// seam itself, which no statement can reach on purpose: the context test
// because the fail-closed direction has no visible symptom (the relation just
// keeps the behaviour it had), and the backstop because the condition it
// guards is the INPUT CHANGING between the plan's open and the execution's —
// a file replaced or truncated in between — which cannot be forced from the
// SQL door inside one statement.
//
// WHAT THE BACKSTOP DOES NOT COVER, said here so this file claims nothing it
// does not hold: both readers infer their schema ONCE per file, from a
// 100-row sample, so the later rows of ONE file never carry a different batch
// schema. A row past that sample whose value does not fit it reads NULL with
// the row still counted, and this wrapper never sees it. That is the readers'
// own pre-existing behaviour, identical at 0c0d33b6, stated on
// docs/sql-reference.md and filed as a priority:high candidate.
func TestArcFRAReaderSchemaIsReadOnlyUnderAnAuthorizedContext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fr.json")
	if err := os.WriteFile(path, []byte("{\"a\":1,\"b\":\"p\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{path}

	t.Run("a_bare_context_reads_nothing", func(t *testing.T) {
		before := ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(context.Background(), "read_json", args, nil); ok {
			t.Errorf("a context with no authorization record published %v", cols)
		}
		if after := ReaderSchemaReads.Load(); after != before {
			t.Errorf("the planner opened the input %d time(s) with no authorization record",
				after-before)
		}
	})

	t.Run("an_authorized_context_reads_once_per_call", func(t *testing.T) {
		ctx := ContextWithReaderSchemaProbe(context.Background())
		before := ReaderSchemaReads.Load()
		cols, ok := readerPlanTimeSchema(ctx, "read_json", args, nil)
		if !ok || len(cols) != 2 || cols[0].Name != "a" || cols[1].Name != "b" {
			t.Fatalf("published %v (ok=%v), want columns a and b", cols, ok)
		}
		if cols[0].Type != parquet.TypeInt64 {
			t.Errorf("column a is %s, want INT64 — the type the reader infers", cols[0].Type)
		}
		if got := ReaderSchemaReads.Load() - before; got != 1 {
			t.Errorf("opened the input %d time(s) for one call, want 1", got)
		}
		// The SAME call again is the cached answer: a reader named twice in
		// one statement is read once.
		if _, ok := readerPlanTimeSchema(ctx, "read_json", args, nil); !ok {
			t.Fatal("the cached answer declined")
		}
		if got := ReaderSchemaReads.Load() - before; got != 1 {
			t.Errorf("opened the input %d time(s) for two asks of one call, want 1", got)
		}
	})

	t.Run("a_guard_that_refuses_reads_nothing", func(t *testing.T) {
		ctx := ContextWithReaderSchemaProbe(context.Background())
		ctx = logical.ContextWithTableFuncGuard(ctx,
			func(string, []string, map[string]string) error {
				return sqlerr.New("42501", "permission denied for table function")
			})
		before := ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(ctx, "read_json", args, nil); ok {
			t.Errorf("a refused identity's file was read: %v", cols)
		}
		if after := ReaderSchemaReads.Load(); after != before {
			t.Errorf("the planner opened the input %d time(s) for a refused identity",
				after-before)
		}
	})

	t.Run("the_kill_switch_forces_the_untyped_path", func(t *testing.T) {
		t.Setenv("WADJET_TEST_NO_READER_SCHEMA", "1")
		ctx := ContextWithReaderSchemaProbe(context.Background())
		before := ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(ctx, "read_json", args, nil); ok {
			t.Errorf("the kill switch did not force the untyped path: %v", cols)
		}
		if after := ReaderSchemaReads.Load(); after != before {
			t.Errorf("the planner opened the input %d time(s) under the kill switch",
				after-before)
		}
	})

	t.Run("an_input_that_can_be_read_once_is_declined_but_counted", func(t *testing.T) {
		// A FIFO is ONE stream: sampling it here would leave the execution
		// with a different one, or with an open(2) that never returns. The
		// SQL-door cells are wadjet.TestArcFRAnInputThatCanBeReadOnceIsReadOnce
		// and server.TestArcFRAFifoFedReaderAnswersOnTheWire.
		//
		// The schema is DECLINED and the counter MOVES. That pairing is the
		// point: deciding to decline means stat'ing the path, and a counter
		// that stayed at zero here would read zero for exactly the inputs the
		// resolver touched — so a door gate asserting "zero for a refused
		// identity" would pass over a FIFO path that had been stat'd anyway.
		fifo := filepath.Join(dir, "fr.fifo")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Skipf("this platform has no FIFO: %v", err)
		}
		ctx := ContextWithReaderSchemaProbe(context.Background())
		before := ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(ctx, "read_csv", []string{fifo}, nil); ok {
			t.Errorf("a FIFO was sampled at plan time: %v", cols)
		}
		if got := ReaderSchemaReads.Load() - before; got != 1 {
			t.Errorf("the counter moved by %d for a FIFO the resolver stat'd, want 1 — "+
				"it counts a path TOUCHED, not a schema sampled", got)
		}
		// And a GLOB is judged by EVERY match, because the source
		// concatenates all of them.
		if err := os.WriteFile(filepath.Join(dir, "g1.csv"), []byte("a\n1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(filepath.Join(dir, "g2.csv"), 0o600); err != nil {
			t.Skipf("this platform has no FIFO: %v", err)
		}
		before = ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(ctx, "read_csv",
			[]string{filepath.Join(dir, "g*.csv")}, nil); ok {
			t.Errorf("a glob holding a FIFO was sampled at plan time: %v", cols)
		}
		if got := ReaderSchemaReads.Load() - before; got != 1 {
			t.Errorf("the counter moved by %d for a glob the resolver expanded and stat'd, "+
				"want 1", got)
		}
	})

	t.Run("a_refused_identity_does_not_even_stat_a_fifo", func(t *testing.T) {
		// The cell the counter move above makes meaningful: the guard is
		// asked BEFORE the path is expanded or stat'd, so a refused identity
		// moves it by zero for an input the resolver would have declined
		// anyway. Without the guard check this reads 1.
		fifo := filepath.Join(dir, "fr_denied.fifo")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Skipf("this platform has no FIFO: %v", err)
		}
		ctx := ContextWithReaderSchemaProbe(context.Background())
		ctx = logical.ContextWithTableFuncGuard(ctx,
			func(string, []string, map[string]string) error {
				return sqlerr.New("42501", "permission denied for table function")
			})
		before := ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(ctx, "read_csv", []string{fifo}, nil); ok {
			t.Errorf("a refused identity's FIFO was sampled: %v", cols)
		}
		if after := ReaderSchemaReads.Load(); after != before {
			t.Errorf("the planner touched a refused identity's FIFO path %d time(s)",
				after-before)
		}
	})

	t.Run("an_http_source_reads_nothing_at_plan_time", func(t *testing.T) {
		ctx := ContextWithReaderSchemaProbe(context.Background())
		before := ReaderSchemaReads.Load()
		if cols, ok := readerPlanTimeSchema(ctx, "read_json",
			[]string{"http://127.0.0.1:1/x.json"}, nil); ok {
			t.Errorf("an HTTP source was fetched at plan time: %v", cols)
		}
		if after := ReaderSchemaReads.Load(); after != before {
			t.Errorf("the planner opened an HTTP source %d time(s) at plan time", after-before)
		}
	})

	t.Run("a_database_connector_reads_nothing_at_plan_time", func(t *testing.T) {
		ctx := ContextWithReaderSchemaProbe(context.Background())
		before := ReaderSchemaReads.Load()
		if _, ok := readerPlanTimeSchema(ctx, "postgres_scan",
			[]string{"postgres://u:p@127.0.0.1:1/d", "t"}, nil); ok {
			t.Error("a database connector was dialled at plan time")
		}
		if after := ReaderSchemaReads.Load(); after != before {
			t.Errorf("the planner dialled %d time(s) at plan time", after-before)
		}
	})
}

// driftSource publishes one batch at the plan-time schema and then one whose
// column `a` carries another type — the input that changed between the plan
// and the run. No file this engine's readers produce behaves this way, which
// is why the source is synthetic and why the doc above says so.
type driftSource struct{ n int }

func (s *driftSource) Init(context.Context) error { return nil }

func (s *driftSource) Next(context.Context) (*batch.RecordBatch, error) {
	s.n++
	switch s.n {
	case 1:
		b := batch.NewRecordBatch([]parquet.Column{
			{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString},
		}, 1)
		b.Len = 1
		return b, nil
	case 2:
		b := batch.NewRecordBatch([]parquet.Column{
			{Name: "a", Type: parquet.TypeString}, {Name: "b", Type: parquet.TypeString},
		}, 1)
		b.Len = 1
		return b, nil
	}
	return nil, nil
}

func (s *driftSource) Close() error { return nil }

// goneSource publishes a batch that no longer carries a column the plan read.
type goneSource struct{ done bool }

func (s *goneSource) Init(context.Context) error { return nil }

func (s *goneSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.done {
		return nil, nil
	}
	s.done = true
	b := batch.NewRecordBatch([]parquet.Column{{Name: "b", Type: parquet.TypeString}}, 1)
	b.Len = 1
	return b, nil
}

func (s *goneSource) Close() error { return nil }

func TestArcFRAnInputThatChangesBetweenThePlanAndTheRunIsLoud(t *testing.T) {
	planned := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString},
	}
	ctx := context.Background()

	t.Run("a_type_that_changed", func(t *testing.T) {
		src := withPlanTimeSchema(&driftSource{}, planned, "f")
		if _, err := src.Next(ctx); err != nil {
			t.Fatalf("the first batch, which agrees, was refused: %v", err)
		}
		_, err := src.Next(ctx)
		if err == nil {
			t.Fatal("a batch whose column carries another type was accepted; every consumer " +
				"above the relation was built from the declaration the plan read")
		}
		if st := sqlerr.StateOf(err); st != "42804" {
			t.Errorf("SQLSTATE %q, want 42804: %v", st, err)
		}
		if !strings.Contains(err.Error(), `"a"`) {
			t.Errorf("the refusal does not name the column: %v", err)
		}
	})

	t.Run("a_column_that_is_gone", func(t *testing.T) {
		src := withPlanTimeSchema(&goneSource{}, planned, "f")
		_, err := src.Next(ctx)
		if err == nil {
			t.Fatal("a batch missing a column the plan read was accepted; the reference to it " +
				"would answer NULL for every row")
		}
		if st := sqlerr.StateOf(err); st != "42703" {
			t.Errorf("SQLSTATE %q, want 42703: %v", st, err)
		}
		if !strings.Contains(err.Error(), `"a"`) {
			t.Errorf("the refusal does not name the column: %v", err)
		}
	})

	t.Run("a_batch_that_agrees_passes_through", func(t *testing.T) {
		src := withPlanTimeSchema(&driftSource{}, planned, "f")
		b, err := src.Next(ctx)
		if err != nil || b == nil {
			t.Fatalf("an agreeing batch was refused: %v", err)
		}
	})
}
