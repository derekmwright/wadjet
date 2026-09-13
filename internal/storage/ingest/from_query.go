package ingest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file is the plan-to-writer seam (#1024): the two things a statement
// whose SOURCE is a query needs that an `INSERT … VALUES` does not — a schema
// taken from the plan's declared output, and a write that publishes all of its
// files or none of them.
//
// It lives here, beside the Ingester, because both doors that execute such a
// statement must reach ONE implementation. The embedded API runs the query on
// the single-process pipeline; the coordinator runs it on the arm its planner
// picks and hands over the merged result. If each wrote its own writer the two
// would disagree about the schema, the commit or the reclaim, which is the
// split #815 closed for the four DML verbs.

// TableSchemaForQuery turns a statement's DECLARED OUTPUT into the schema of
// the table a `CREATE TABLE … AS SELECT` creates.
//
// The declared output is the one column list every arm agrees on (ADR-0026 §8):
// the names, the types and the DECIMAL (p, s) come from the single inference
// the planner already carries, so the table a CTAS creates has exactly the
// columns the identical bare SELECT returns.
//
// Three rules are PostgreSQL 17.11's, measured:
//
//   - NOT NULL is never inferred. Every column of a CTAS result is nullable
//     there (`is_nullable = YES`), including one copied from a NOT NULL source
//     column, so it is nullable here.
//   - `renames` renames POSITIONALLY and may be SHORTER than the query's
//     output: `CREATE TABLE t (a) AS SELECT id, n FROM src` is accepted and
//     gives `(a, n)`. A LONGER list is 42601, `too many column names were
//     specified`.
//   - Two output columns that answer to one name is 42701 — the same refusal
//     `CREATE TABLE t (a int, a int)` gets — because a relation cannot hold
//     both. `SELECT id, n+1, s||'x', 42` is that shape on PostgreSQL: the two
//     unaliased expressions are both called `?column?`.
func TableSchemaForQuery(declared []parquet.Column, renames []string) (parquet.Schema, error) {
	if len(declared) == 0 {
		// The declared output is what this statement writes; an empty one is
		// the engine failing to describe its own query, never an answer.
		return parquet.Schema{}, sqlerr.New("0A000",
			"CREATE TABLE AS: this engine cannot name the query's output columns, "+
				"so it cannot declare the table; alias the SELECT list explicitly")
	}
	if len(renames) > len(declared) {
		return parquet.Schema{}, sqlerr.New("42601", "too many column names were specified")
	}

	cols := make([]parquet.Column, len(declared))
	for i, c := range declared {
		cols[i] = c
		if i < len(renames) {
			cols[i].Name = renames[i]
		}
		// PostgreSQL does not infer NOT NULL for a CTAS column; neither does
		// this. A declaration copied from the source would refuse rows the
		// query legitimately produces (an outer join's pad, a CASE with no
		// ELSE) with 23502.
		cols[i].Nullable = true
	}

	seen := make(map[string]string, len(cols))
	for _, c := range cols {
		if c.Name == "" {
			return parquet.Schema{}, sqlerr.New("42601",
				"CREATE TABLE AS: an output column of the query has no name; alias it")
		}
		k := parquet.FoldName(c.Name)
		if prev, dup := seen[k]; dup {
			// PostgreSQL's own wording for this statement, measured:
			// `column "?column?" specified more than once`.
			return parquet.Schema{}, sqlerr.New("42701",
				"column %q specified more than once", prev)
		}
		seen[k] = c.Name
	}

	schema := parquet.Schema{Columns: cols}
	// The writer's own rules, asked at CREATE rather than at the first flush
	// (ADR-0018 §14): a declared type no writer can store must not become a
	// table whose first write fails.
	if err := parquet.ValidateWriteSchema(schema); err != nil {
		return parquet.Schema{}, fmt.Errorf("CREATE TABLE AS: %w", err)
	}
	return schema, nil
}

// QueryWrite names the table a query's result is written into, and how the
// write is committed.
type QueryWrite struct {
	// Table is the CATALOG-RESOLVED name for an append, and the name as the
	// statement minted it for a create.
	Table string
	// Schema is the table's schema: TableSchemaForQuery's answer for a create,
	// the target's own stored schema for an append.
	Schema parquet.Schema
	// Columns names the target column each of the query's output columns
	// feeds, IN QUERY ORDER. It is how a write whose source is a query is
	// positional: PostgreSQL matches the query's items to the target's columns
	// by position — by the explicit `INSERT INTO t (b, a)` list where one is
	// written, by the table's own column order otherwise — and never by the
	// names the query happened to publish. It may be SHORTER than Schema: the
	// columns it does not name are absent from every row and take NULL, which
	// is what PostgreSQL 17.11 does (measured).
	Columns []string
	// PartitionKeys is the target's partitioning. A CTAS creates an
	// unpartitioned table — `CREATE TABLE … AS SELECT` has no PARTITION BY in
	// PostgreSQL's grammar and none here.
	PartitionKeys []string
	// Create is true for `CREATE TABLE … AS SELECT`: the catalog entry is
	// created holding the files, so the name never resolves to an empty table.
	Create bool
	// Incarnation is the table identity an APPEND read (ADR-0030/#919). Empty
	// for a create, and empty for a manifest written before the field existed.
	Incarnation string
	// Config bounds the writer's buffers. Zero fields take DefaultConfig.
	Config Config
}

// queryWriteChunk is how many boxed rows are handed to the Ingester at a time.
//
// The ingester buffers per partition and flushes on its own thresholds, so the
// peak this bounds is the map boxing, not the buffer: a result of ten million
// rows is boxed 8192 at a time rather than all at once. It is the batch size
// the engine already works in, doubled twice, because a smaller chunk buys
// nothing here — Ingest validates and copies per row either way.
const queryWriteChunk = 8192

// WriteQueryRows writes a query's gathered rows into the target table and
// publishes them in ONE catalog write, or none.
//
// `rows` are POSITIONAL — cell j of every row is the query's output column j,
// which w.Columns maps onto a target column, and they are read ONE AT A TIME
// (RowSource) so the result is never copied whole a second time. A map keyed by the query's own
// names could not carry the answer: a result may legally publish two columns
// of one name (`SELECT abs(a), abs(b)`), and the second would overwrite the
// first.
//
// The rows travel through the ordinary Ingester — validate, partition, flush,
// parquet, per-file statistics and HLL sketches — so a table a query wrote is
// indistinguishable from one an INSERT wrote, which is what makes the
// compaction gate's rule ("every read→write asymmetry is data loss") cover this
// door too. What is different is the COMMIT: DeferManifestCommit holds every
// flushed file out of the manifest until the whole result is written, so a
// statement that fails on its second file has published neither.
//
// A failure RECLAIMS: the objects this statement uploaded are named by nothing
// — the table does not exist for a create, and an append's files are not in the
// manifest — so they are retired through catalog.RetireObjects, which deletes
// only what no live manifest references. That is stricter than the residual
// ADR-0030 accepts for a refused DML retry ("bytes, never rows"), and it can
// be: a DML retry may legitimately re-run and needs its window, while this
// statement is over.
// RowSource is a statement's result, read one row at a time.
//
// It is an accessor and not a `[][]any` because the caller already holds the
// rows and a slice would be a second full copy of the result — the allocation
// pattern CollectSink's own comment records as having held 21 GB of live heap
// at SF10 Q18. `At` returns row i POSITIONALLY, its cells aligned with the
// query's declared output, and may convert as it goes: the assignment
// conversion a query-sourced append applies is per row, so doing it here keeps
// exactly one converted row alive at a time.
//
// `At` returning an error stops the write; the statement reports it.
type RowSource struct {
	N  int
	At func(i int) ([]any, error)
}

// sliceRows is a RowSource over rows a caller already has as a slice. It is
// unexported because no door has such a slice — every production caller reads
// its rows one at a time out of a result — and it exists for the tests that
// drive this seam directly.
func sliceRows(rows [][]any) RowSource {
	return RowSource{N: len(rows), At: func(i int) ([]any, error) { return rows[i], nil }}
}

func WriteQueryRows(ctx context.Context, cat *catalog.Catalog, w QueryWrite, rows RowSource) (n int64, err error) {
	cfg := w.Config
	if cfg == (Config{}) {
		cfg = DefaultConfig()
	}
	ing := New(cat, w.Table, w.Schema, w.PartitionKeys, cfg)
	ing.DeferManifestCommit()

	defer func() {
		if err == nil {
			return
		}
		// Everything this statement uploaded, including the files an earlier
		// flush landed before the one that failed.
		//
		// WithoutCancel, and it is the whole of the reclaim: the commonest
		// reason to be in this defer is that the statement's OWN context died
		// — a client cancel, a deadline — and RetireObjects reads the live
		// catalog state and issues its deletes through the context it is
		// given. On a dead one both fail, the retirement is logged as
		// deferred, and the bytes stay forever. MemStore ignores ctx entirely,
		// which is why no fixture over it can see this; every real store
		// honours it (#1024 review B4).
		reclaim, cancel := context.WithTimeout(context.WithoutCancel(ctx), reclaimTimeout)
		defer cancel()
		reclaimPendingObjects(reclaim, cat, ing.PendingFiles())
	}()

	boxed := make([]map[string]any, 0, min(queryWriteChunk, rows.N))
	flushChunk := func() error {
		if len(boxed) == 0 {
			return nil
		}
		if err := ing.Ingest(ctx, boxed); err != nil {
			return err
		}
		boxed = boxed[:0]
		return nil
	}
	for i := 0; i < rows.N; i++ {
		cells, err := rows.At(i)
		if err != nil {
			return 0, err
		}
		row := make(map[string]any, len(w.Columns))
		for j, name := range w.Columns {
			if j >= len(cells) {
				break
			}
			row[name] = cells[j]
		}
		boxed = append(boxed, row)
		if len(boxed) == queryWriteChunk {
			if err := flushChunk(); err != nil {
				return 0, err
			}
		}
	}
	if err := flushChunk(); err != nil {
		return 0, err
	}
	if err := ing.FlushAll(ctx); err != nil {
		return 0, fmt.Errorf("writing %q: %w", w.Table, err)
	}
	pending := ing.PendingFiles()

	if w.Create {
		if err := cat.CreateTableWithFiles(ctx, w.Table, w.Schema, w.PartitionKeys, pending); err != nil {
			// The reclaim in the defer needs the list back: PendingFiles
			// cleared it.
			ing.RestorePendingFiles(pending)
			return 0, err
		}
		return int64(rows.N), nil
	}
	if err := cat.CommitIngest(ctx, w.Table, w.Incarnation, pending); err != nil {
		ing.RestorePendingFiles(pending)
		return 0, err
	}
	return int64(rows.N), nil
}

// reclaimTimeout bounds the reclaim of a failed statement's objects. It is a
// bound and not a deadline anyone waits on: the statement has already failed
// and its error is the answer, so a store that has stopped answering must not
// hold the caller open — it costs the bytes instead, which is the residual
// ADR-0030 already accepts.
const reclaimTimeout = 30 * time.Second

// reclaimPendingObjects retires the objects a failed statement uploaded.
//
// It never reports: the statement already has an error to return and that
// error is the answer. A path RetireObjects declines to delete — because some
// live manifest names it, or because a sweep holds it — is left as the
// unreferenced bytes ADR-0030 accepts, not as a second error over the first.
func reclaimPendingObjects(ctx context.Context, cat *catalog.Catalog, pending []catalog.PendingFile) {
	if len(pending) == 0 {
		return
	}
	reqs := make([]catalog.RetireRequest, 0, 2*len(pending))
	for _, pf := range pending {
		reqs = append(reqs, catalog.RetireRequest{Path: pf.Entry.Path})
		// The per-file statistics object the flush uploaded beside the data
		// file. It is referenced only from the manifest entry this statement
		// never published, so leaving it behind would be the same leak one
		// layer over.
		if pf.Entry.SketchesKey != "" {
			reqs = append(reqs, catalog.RetireRequest{Path: pf.Entry.SketchesKey})
		}
	}
	cat.RetireObjects(ctx, reqs)
}

// AssignableToColumn reports whether a query's output column may be written
// into a target column by an `INSERT INTO … <select>`, and says why not.
//
// PostgreSQL 17.11 inserts an ASSIGNMENT CAST here — `transformAssignedExpr` —
// so it accepts more pairs than this does: `INSERT INTO t (text_col) SELECT
// bigint_col` and `INSERT INTO t (bigint_col) SELECT numeric_col` both succeed
// there. Measured, along with the two it refuses: a boolean or a timestamp into
// bigint is 42804, `column "id" is of type bigint but expression is of type
// boolean`.
//
// This engine takes the LOUD side of that line wherever a conversion would have
// to be invented at the writer, and the reason is not caution. A DECIMAL box
// carries an unscaled integer and no scale: assigning a DECIMAL(12,3) value to
// a DECIMAL(18,4) column without rescaling stores 1.500 as 0.1500 — a silent
// wrong number, which is the one outcome that is never acceptable (ADR-0012).
// The pairs below are exactly the ones where the box the query produces is a
// box the writer already stores correctly for the target's declaration, which
// is why they need no conversion at all:
//
//   - the same declared type, including (p, s) for DECIMAL, the dimension for
//     VECTOR and the whole shape for ARRAY / ROW / MAP;
//   - any integer declaration into any other (INT32, INT64, PORT, PROTOCOL) —
//     the writer's own leaf check refuses a value the target cannot hold, with
//     22003;
//   - an integer into FLOAT32, FLOAT64 or DECIMAL, and a float into a float.
//
// Everything else is 42804 carrying PostgreSQL's message and its hint, so the
// statement's answer is "write the CAST" rather than a number nobody can
// audit. The difference is in ADR-0012's divergence list.
func AssignableToColumn(from, to parquet.Column) error {
	if sameDeclaredType(from, to) {
		return nil
	}
	if numericDeclaration(from.Type) && numericDeclaration(to.Type) {
		return nil
	}
	return sqlerr.New("42804",
		"column %q is of type %s but expression is of type %s; "+
			"you will need to rewrite or cast the expression",
		to.Name, declaredTypeText(to), declaredTypeText(from))
}

// numericDeclaration is the family the assignment converter covers: every
// declaration `assignEvaluatedValue` has a rule for. PostgreSQL assigns freely
// within it — an integer into a numeric, a double into a bigint (rounded), a
// numeric into a real — and so does this engine's VALUES door, which reaches
// that converter for every literal it writes.
func numericDeclaration(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol,
		parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal:
		return true
	}
	return false
}

// sameDeclaredType compares two columns' TYPES and nothing else — not the name
// a column carries and not whether it is nullable, neither of which an
// assignment has to agree about.
//
// It recurses, and inside a container the field NAMES are part of the type: a
// ROW(a INT64, b STRING) and a ROW(x INT64, y STRING) are two composite types
// on PostgreSQL and reading one as the other renames every field. ARRAY's
// element and MAP's entry carry structural names the schema builders mint
// ("element", "key", "value"), so those are compared the same way and agree by
// construction.
func sameDeclaredType(a, b parquet.Column) bool {
	if a.Type != b.Type || a.Precision != b.Precision || a.Scale != b.Scale || a.Dimension != b.Dimension {
		return false
	}
	if (a.ElementType == nil) != (b.ElementType == nil) {
		return false
	}
	if a.ElementType != nil {
		if !strings.EqualFold(a.ElementType.Name, b.ElementType.Name) ||
			!sameDeclaredType(*a.ElementType, *b.ElementType) {
			return false
		}
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if !strings.EqualFold(a.Fields[i].Name, b.Fields[i].Name) ||
			!sameDeclaredType(a.Fields[i], b.Fields[i]) {
			return false
		}
	}
	return true
}

// declaredTypeText renders a column's declaration the way a refusal has to
// name it: a bare TypeID is not a type for a DECIMAL or a VECTOR, and a
// message that says "DECIMAL" about a (18,4) column tells the reader nothing
// about why their (12,3) value was refused.
func declaredTypeText(c parquet.Column) string {
	switch c.Type {
	case parquet.TypeDecimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", c.Precision, c.Scale)
	case parquet.TypeVector:
		return fmt.Sprintf("VECTOR(%d)", c.Dimension)
	case parquet.TypeArray:
		if c.ElementType != nil {
			return "ARRAY(" + declaredTypeText(*c.ElementType) + ")"
		}
	case parquet.TypeMap:
		if c.ElementType != nil && len(c.ElementType.Fields) == 2 {
			return "MAP(" + declaredTypeText(c.ElementType.Fields[0]) + ", " +
				declaredTypeText(c.ElementType.Fields[1]) + ")"
		}
	case parquet.TypeRow:
		parts := make([]string, 0, len(c.Fields))
		for _, f := range c.Fields {
			parts = append(parts, f.Name+" "+declaredTypeText(f))
		}
		return "ROW(" + strings.Join(parts, ", ") + ")"
	}
	return c.Type.String()
}
