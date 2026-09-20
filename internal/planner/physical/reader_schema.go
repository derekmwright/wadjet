// SPDX-License-Identifier: MIT

// This file holds the plan-time schema of a FILE READER, governed by ADR-0039
// §3 and ADR-0034.
package physical

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// readerSchemaProbe is one statement's record that its door AUTHORIZED the
// table-function capability before anything bound, and the cache of what the
// readers it names publish.
//
// Its PRESENCE is the authorization order, made structural. A reader's
// columns are its INPUT's, so learning them means opening the input — and
// opening the input for an identity that may not be allowed to is what
// ADR-0034 forbids and what #943's gate asserts. So the only place the probe
// is installed is `auth.AuthorizeTableFunctions`, which every statement door
// calls BEFORE `ValidateStatementColumns` and before `AnnotateScanColumns`;
// a context with no probe reads nothing and the relation keeps the open scope
// and the first-batch refusal it had through v0.23.0.
//
// The per-call decision is still asked, every time, through the same
// `logical.TableFuncGuard` the source build asks: a call the early pass could
// not see — inside a scalar subquery, an IN list, a CTE body, a scan the
// optimizer minted — is decided here before its input is touched.
type readerSchemaProbe struct {
	mu    sync.Mutex
	cache map[string]readerSchemaResult
}

// readerSchemaResult is one call's answer, cached for the statement: a reader
// named twice in a statement is read once. `ok=false` records a DECLINE (the
// input is unreachable, the identity is not authorized, the shape is one this
// resolver does not read) so the second ask does not repeat the work.
type readerSchemaResult struct {
	cols []parquet.Column
	ok   bool
}

type readerSchemaProbeKey struct{}

// ContextWithReaderSchemaProbe records that this statement's table-function
// capability has been authorized, so the planner may read a reader's schema
// before execution. `auth.AuthorizeTableFunctions` is its one caller.
func ContextWithReaderSchemaProbe(ctx context.Context) context.Context {
	if ctx.Value(readerSchemaProbeKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, readerSchemaProbeKey{}, &readerSchemaProbe{
		cache: make(map[string]readerSchemaResult),
	})
}

func readerSchemaProbeFromContext(ctx context.Context) *readerSchemaProbe {
	p, _ := ctx.Value(readerSchemaProbeKey{}).(*readerSchemaProbe)
	return p
}

// readerPlanTimeSchema is the column list a FILE READER publishes, read from
// its input at PLAN time — the half of "a table function in FROM is a
// relation" that ADR-0039 §3 deferred (#1230).
//
// With it the reader is an ordinary relation: the binder closes its scope, so
// an unknown column is 42703 at plan time through any path (#1231); `f.*`
// expands; and the aggregate result-type rules read the column's real type,
// so an integer SUM is bigint and a MAX keeps the input's width (ADR-0024).
//
// THE BOUND. Nothing here reads more than it must:
//
//   - `read_parquet` reads the FOOTER and no page. It is exact — the file
//     declares its own schema — and it is the same `reader.Schema().Columns`
//     the source itself publishes.
//   - `read_json` and `read_csv` read ONE BATCH through the very reader the
//     source uses, and take that batch's schema. Taking it through the same
//     inference is what makes the plan-time answer and the run-time answer
//     the same answer rather than two guesses about one file — and that
//     inference is the readers' own 100-ROW SAMPLE (csv.sampleSize,
//     json.defaultSampleSize), which describes the whole file. A row past it
//     that does not fit is NOT refused: the value reads NULL with the row
//     still counted, a key first seen there is not a column at all, and a
//     JSON number that becomes a string fails as a recovered panic. That is
//     the readers' pre-existing behaviour, identical at 0c0d33b6, stated on
//     docs/sql-reference.md and filed rather than claimed as handled.
//   - an HTTP(S) source reads NOTHING here and keeps the first-batch stance.
//     A plan-time fetch would be a second request for every statement and
//     would make EXPLAIN reach the network, which is a cost and a surprise
//     this schema is not worth.
//   - an input that can be read ONCE reads nothing here either
//     (readerInputIsRereadable): this read opens the input and the execution
//     opens it AGAIN.
//
// An input that CHANGES between this read and the execution's — a file
// replaced or truncated in between — is caught by withPlanTimeSchema, loudly.
//
// ok=false means "not knowable here", the caller's signal to keep the open
// scope and the first-batch refusal.
func readerPlanTimeSchema(ctx context.Context, funcName string, args []string,
	namedArgs map[string]string,
) ([]parquet.Column, bool) {
	if !readerSchemaEnabled() {
		return nil, false
	}
	name := strings.ToLower(strings.TrimSpace(funcName))
	if !readerSchemaFuncs[name] || len(args) == 0 {
		return nil, false
	}
	probe := readerSchemaProbeFromContext(ctx)
	if probe == nil {
		// No door authorized this statement's capability, so nothing here may
		// open anything. This is the fail-CLOSED direction: the relation
		// keeps the behaviour it had before a plan-time schema existed.
		return nil, false
	}
	// THE CAPABILITY, before the file. A denied identity gets its 42501 from
	// the enforcement pass; what matters here is that the decision is asked
	// before `openData`, so the refused identity's file is never opened.
	if guard := logical.TableFuncGuardFromContext(ctx); guard != nil {
		if err := guard(funcName, args, namedArgs); err != nil {
			return nil, false
		}
	}
	key := readerSchemaKey(name, args, namedArgs)
	probe.mu.Lock()
	if r, ok := probe.cache[key]; ok {
		probe.mu.Unlock()
		return r.cols, r.ok
	}
	probe.mu.Unlock()

	cols, ok := readReaderSchema(name, args, namedArgs)
	probe.mu.Lock()
	probe.cache[key] = readerSchemaResult{cols: cols, ok: ok}
	probe.mu.Unlock()
	return cols, ok
}

// readerSchemaFuncs are the table functions whose columns are a FILE's and
// which this resolver reads. The database connectors are deliberately absent:
// their schema is a remote query's, reading it is a round trip to a server
// over a connection string, and the first-batch stance is the honest one
// until that has its own bound.
var readerSchemaFuncs = map[string]bool{
	"read_json": true, "read_json_auto": true,
	"read_csv": true, "read_csv_auto": true,
	"read_parquet": true,
}

// readerSchemaEnabled is the kill switch, and the way a gate FORCES the
// untyped path. `WADJET_TEST_NO_READER_SCHEMA=1` makes every reader publish
// no plan-time schema, which is exactly what every reader did through
// v0.23.0 — so the join-key repair (#1229), which must hold for a relation
// whose column list is unknown, keeps being exercised after this schema
// exists rather than being hidden by it.
func readerSchemaEnabled() bool {
	return os.Getenv("WADJET_TEST_NO_READER_SCHEMA") != "1"
}

func readerSchemaKey(name string, args []string, namedArgs map[string]string) string {
	var b strings.Builder
	b.WriteString(name)
	for _, a := range args {
		b.WriteByte(0)
		b.WriteString(a)
	}
	keys := make([]string, 0, len(namedArgs))
	for k := range namedArgs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(namedArgs[k])
	}
	return b.String()
}

// ReaderSchemaReads counts the times the planner has TOUCHED a path a caller
// named, to learn a reader's columns. It is the capability order made
// measurable: a gate asserts it does not move for an identity the policy
// refuses, on any door, which is the property #943's own gate states with a
// path that would error if opened and a loopback server that must never be
// hit (ADR-0034).
//
// TOUCHED, not sampled: it goes up before the rereadable check stats the
// path, so an input this resolver ends up DECLINING — a FIFO, a device, a
// glob holding one — still moves it. A counter that only counted successful
// samples would read zero for exactly the inputs whose paths were stat'd
// anyway, which is the measurement the door gates depend on.
//
// A URL, a connector, a cached answer and a refused identity leave it alone:
// none of them reaches this body.
var ReaderSchemaReads atomic.Int64

// readReaderSchema is the read itself, with no cache and no authorization: a
// PRIVATE body whose one caller has already asked both questions.
func readReaderSchema(name string, args []string, namedArgs map[string]string) (cols []parquet.Column, ok bool) {
	// A reader's input is whatever the caller wrote. A malformed file, a
	// truncated footer, a decoder that raises rather than returns — none of
	// them is a reason to fail the STATEMENT here, because the statement's
	// own execution will reach the same input and report it with the source's
	// own error. Declining leaves that path exactly as it was.
	defer func() {
		if r := recover(); r != nil {
			cols, ok = nil, false
		}
	}()
	path := expandHome(args[0])
	if isURL(path) {
		return nil, false
	}
	// THE COUNTER GOES UP HERE, before anything below touches the path.
	//
	// It means "the plan-time reader touched a path the caller named", not
	// "it sampled one". The rereadable check below expands a glob and stats
	// every match, and a stat IS a touch: counting after it would let a
	// FIFO or a device path be stat'd for an identity nobody authorized while
	// every capability-order gate still read zero. The instrument has to
	// move for every path this body reaches, or it measures the wrong thing.
	ReaderSchemaReads.Add(1)
	// A plan-time read OPENS the input and the execution opens it AGAIN, so it
	// is only sound for an input that reads the same bytes twice. A FIFO, a
	// character device (`/dev/stdin`), a socket or a process substitution
	// (`/dev/fd/63`) is ONE stream: consuming its first batch here leaves the
	// execution reading a different one — zero rows for a shape that answered
	// three — and a second open no writer will meet BLOCKS FOREVER, because
	// `open(2)` is not interruptible by the statement's context. Nothing in
	// the reader or its documentation says an input must be seekable, so the
	// unseekable ones keep the first-batch stance every reader had through
	// v0.23.0: no plan-time schema, and exactly ONE open.
	//
	// This runs AFTER the capability decision in readerPlanTimeSchema, so it
	// names no path on behalf of an identity that has not been authorized.
	if !readerInputIsRereadable(path) {
		return nil, false
	}
	if name == "read_parquet" {
		return parquetFooterSchema(path)
	}
	src, err := buildTableFunctionSource(name, args, namedArgs)
	if err != nil {
		return nil, false
	}
	defer src.Close()
	if err := src.Init(context.Background()); err != nil {
		return nil, false
	}
	b, err := src.Next(context.Background())
	if err != nil {
		return nil, false
	}
	if b != nil {
		out := make([]parquet.Column, len(b.Schema))
		copy(out, b.Schema)
		return out, true
	}
	// The input produced NO batch. That is not the same as publishing no
	// columns: a CSV whose header row is its only row declares its columns
	// and has no rows, exactly as an empty table does, so the reader's own
	// inferred schema is asked before the relation is called empty.
	switch r := src.(type) {
	case *csvTableFuncSource:
		if r.reader != nil {
			if cols := r.reader.Schema(); len(cols) > 0 {
				out := make([]parquet.Column, len(cols))
				copy(out, cols)
				return out, true
			}
		}
	case *jsonTableFuncSource:
		if r.reader != nil {
			if cols := r.reader.Schema(); len(cols) > 0 {
				out := make([]parquet.Column, len(cols))
				copy(out, cols)
				return out, true
			}
		}
	}
	// The input is REACHABLE and declares nothing: an empty file. That is
	// knowledge, not a decline — the relation publishes nothing — and it is
	// reported as a zero-column schema so the caller can refuse it by name
	// rather than as the door's "the result has no columns at all" (#1230).
	return []parquet.Column{}, true
}

// readerInputIsRereadable reports whether opening this input twice reads the
// same bytes twice. Only a REGULAR file does.
//
// A GLOB is judged by EVERY match, not by the first: the JSON and CSV sources
// concatenate all of them (`multiFileReadCloser`), so one FIFO anywhere in the
// expansion is a stream the execution cannot read again. A match that cannot
// be stat'd declines too — an input this cannot describe is one it must not
// consume.
func readerInputIsRereadable(path string) bool {
	paths := []string{path}
	if isGlob(path) {
		matches, err := filepath.Glob(path)
		if err != nil || len(matches) == 0 {
			return false
		}
		sort.Strings(matches)
		paths = matches
	}
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil || !fi.Mode().IsRegular() {
			return false
		}
	}
	return true
}

// parquetFooterSchema reads a Parquet file's own declaration and no page of
// its data. A GLOB takes the FIRST match in sorted order, which is the file
// the source's own concatenation starts with.
func parquetFooterSchema(path string) ([]parquet.Column, bool) {
	if isGlob(path) {
		matches, err := filepath.Glob(path)
		if err != nil || len(matches) == 0 {
			return nil, false
		}
		sort.Strings(matches)
		path = matches[0]
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, false
	}
	r, err := parquet.NewReader(f, fi.Size())
	if err != nil {
		return nil, false
	}
	cols := r.Schema().Columns
	out := make([]parquet.Column, len(cols))
	copy(out, cols)
	return out, true
}

// withPlanTimeSchema is the BACKSTOP under the plan-time schema: the batch
// that actually arrives is held to what the plan read.
//
// The condition it guards is the INPUT CHANGING between the two opens — the
// plan reads a file's schema, the file is replaced or truncated, and the
// execution opens a relation with another one. Read through a projection
// built for the schema the plan saw, that is a NULL for a column that is
// really there under another type, or a value read at the wrong width; so it
// is an error naming the column, the type the plan declared and the type that
// came.
//
// It is NOT a guard on a value inside a batch. Both readers infer their
// schema ONCE per file from a 100-row sample, so the later rows of ONE file
// never carry a different schema; what happens to a row that does not fit the
// sample is the readers' own behaviour, recorded in readerPlanTimeSchema's
// header and on docs/sql-reference.md. The gate drives this wrapper directly
// for that reason: the condition cannot be forced from the SQL door inside
// one statement.
func withPlanTimeSchema(src exec.Source, cols []parquet.Column, relName string) exec.Source {
	if len(cols) == 0 {
		return src
	}
	return &planTimeSchemaSource{src: src, cols: cols, relName: relName}
}

type planTimeSchemaSource struct {
	src     exec.Source
	cols    []parquet.Column
	relName string
}

func (s *planTimeSchemaSource) Init(ctx context.Context) error { return s.src.Init(ctx) }

func (s *planTimeSchemaSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	b, err := s.src.Next(ctx)
	if err != nil || b == nil {
		return b, err
	}
	have := make(map[string]parquet.TypeID, len(b.Schema))
	for _, c := range b.Schema {
		have[strings.ToLower(c.Name)] = c.Type
	}
	for _, want := range s.cols {
		got, present := have[strings.ToLower(want.Name)]
		if !present {
			return nil, sqlerr.New("42703",
				"the table function %q read column %q at plan time and a later batch does not "+
					"publish it: the relation's column list changed while it was being read",
				s.relName, want.Name)
		}
		if got != want.Type && !readerTypeCompatible(want.Type) {
			return nil, sqlerr.New("42804",
				"the table function %q declared column %q as %s at plan time and a later batch "+
					"carries %s: the declaration every consumer above this relation was built "+
					"from and the vector that arrived describe one column two ways",
				s.relName, want.Name, want.Type, got)
		}
	}
	return b, nil
}

func (s *planTimeSchemaSource) Close() error { return s.src.Close() }

// readerTypeCompatible reports the ONE disagreement that is not one: a column
// whose plan-time sample held only NULLs has no type to declare, and both
// readers spell that as a STRING. Whatever type it really turns out to be,
// nothing is read at the wrong width through it, because every consumer above
// the relation was built from the STRING declaration and reads a string.
//
// The condition is the DECLARED side alone. Requiring the arrived side to be
// STRING too made the function unreachable — its one call site is already
// guarded by `got != want.Type` — so the exemption this comment describes was
// refused with 42804 instead of allowed (the review's P1).
func readerTypeCompatible(declared parquet.TypeID) bool {
	return declared == parquet.TypeString
}

// emptyReaderRefusal is what a reader whose input is EMPTY answers.
//
// A relation publishes a column list; this one has none to publish, because
// its columns are its input's and its input has no rows. Through v0.23.0 that
// reached the door as `XX000 the result has no columns at all, which is never
// an answer` — the engine reporting an internal invariant for a file the
// caller can see is empty (#1230). It is a named refusal now, in the class of
// a shape this engine does not have: PostgreSQL permits a relation with zero
// columns and this engine does not, at any door.
func emptyReaderRefusal(funcName string, args []string) error {
	target := ""
	if len(args) > 0 {
		target = args[0]
	}
	return sqlerr.New("0A000",
		"the table function %q published no columns: its input %q is empty, so the relation "+
			"has no column list, and a result with no columns is not an answer this engine "+
			"has — point it at an input that declares its columns (a Parquet file carries its "+
			"schema in the footer even with no rows, and a CSV carries it in the header row)",
		funcName, target)
}
