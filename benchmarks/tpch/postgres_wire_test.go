package tpch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
)

// The PROTOCOL arm: the same SQL through the same PostgreSQL client library
// against Wadjet's pgwire endpoint and against a real PostgreSQL, comparing
// what the WIRE carries rather than what the values mean.
//
// This is the arm DuckDB cannot provide at all, and the gap is not theoretical.
// Eight defects were found in this layer BY HAND on 2026-08-18, every one of
// them invisible to a value oracle because the values were right:
//
//   - RowDescription declared OID 25 (text) for every column, so DataGrip read
//     an int4 as text and reported "Bad value for type int : f";
//   - a DATE column under a BINARY result format wrote its rendered TEXT bytes
//     beneath the declared OID 1082, whose value is a 4-byte day count;
//   - a cancelled statement reported SQLSTATE 42000 (syntax error) instead of
//     57014 (query_canceled), so a client could not tell "you cancelled this"
//     from "your SQL is wrong".
//
// Nothing tested any of it. A value comparison cannot: the rows were correct in
// every one of those cases.
//
// Comparison is per PROPERTY, not per query, so a pin says exactly which
// property is not gated and the rest of the query stays gated. See wireCase.
func runPostgresWireArm(t *testing.T, ctx context.Context, o *postgresOracle) {
	// Wadjet's pgwire, over the same *wadjet.DB the semantics arm queried, so
	// both arms are looking at the same rows.
	srv := pgwire.NewServer(o.db, pgwire.Config{}, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("starting wadjet pgwire: %v", err)
	}
	// Shutdown is BOUNDED, and the bound is not defensive padding: Server.
	// Shutdown does wg.Wait() over its connection handlers, and the
	// cancellation subtest once left one running a statement the server would
	// not cancel (#368, fixed — the executor now polls the statement context
	// between batches). The bound stays so a regression fails the suite
	// instead of hanging it on the very defect it would be reporting.
	t.Cleanup(func() {
		stopped := make(chan struct{})
		go func() { srv.Shutdown(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(30 * time.Second):
			t.Logf("wadjet pgwire Shutdown did not return within 30s: a connection handler is still executing " +
				"a statement a CancelRequest should have stopped (#368 regressed?). Not waiting further.")
		}
	})

	wadjetDSN := fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", srv.Addr())

	wConn, err := pgconn.Connect(ctx, wadjetDSN)
	if err != nil {
		t.Fatalf("pgconn to wadjet pgwire at %s: %v", srv.Addr(), err)
	}
	t.Cleanup(func() { wConn.Close(context.Background()) })

	pConn, err := pgconn.Connect(ctx, o.dsn)
	if err != nil {
		t.Fatalf("pgconn to PostgreSQL at %s: %v", redactDSN(o.dsn), err)
	}
	t.Cleanup(func() { pConn.Close(context.Background()) })

	t.Run("Metadata", func(t *testing.T) { runWireMetadata(t, ctx, wConn, pConn) })
	t.Run("Errors", func(t *testing.T) { runWireErrors(t, ctx, wConn, pConn) })
	t.Run("CommandTags", func(t *testing.T) { runWireCommandTags(t, ctx, wConn, pConn) })
	t.Run("Cancellation", func(t *testing.T) { runWireCancellation(t, ctx, wadjetDSN, o.dsn) })
}

// wireCase is one statement compared at the protocol level.
type wireCase struct {
	name string
	sql  string
	// pgSQL is the PostgreSQL-dialect spelling of the same statement, for the
	// shapes the two engines write differently. It must ask the identical
	// question — see pgCase.pgSQL, whose one use this shares: PostgreSQL
	// requires parentheses around a composite before a field access,
	// `(rw).b`, because bare `rw.b` is read as table.column (#568).
	pgSQL string
	// paramOIDs / params bind a parameter by its DECLARED type. Empty means an
	// unparameterized statement.
	paramOIDs []uint32
	params    [][]byte
	// pins maps a PROPERTY name (one of the wireProp* constants) to the reason
	// it is not gated. Every other property of the same statement stays gated,
	// and a pinned property that starts AGREEING fails the subtest — deleting
	// the entry is the proof the fix landed.
	pins map[string]string
}

// The properties compared. Each is something a PostgreSQL client reads and
// acts on; naming them individually is what lets a pin be narrow.
const (
	wirePropFieldCount   = "field_count"        // number of columns in RowDescription
	wirePropFieldNames   = "field_names"        // the names a client shows and binds by
	wirePropTypeOIDs     = "type_oids"          // the OID a driver picks its Java/Go/Python type from
	wirePropTypeSizes    = "type_sizes"         // the declared size, which must agree with the OID
	wirePropTypeMods     = "type_modifiers"     // atttypmod (precision/scale/length)
	wirePropTextFormat   = "text_format_codes"  // the server must honour a text result format request
	wirePropBinFormat    = "binary_format_code" // ... and a binary one, or say it did not
	wirePropValuesText   = "values_text"        // the cell bytes under a text result format
	wirePropFloatRender  = "float_text_render"  // same number, different spelling
	wirePropNullRep      = "null_representation"
	wirePropBinaryDecode = "binary_decode" // binary bytes must decode under the declared OID
	wirePropCommandTag   = "command_tag"
	wirePropParamOIDs    = "param_oids" // ParameterDescription
	wirePropSQLState     = "sqlstate"
)

// runWireMetadata compares RowDescription, result-format handling, cell bytes
// and ParameterDescription for every statement in the wire corpus.
func runWireMetadata(t *testing.T, ctx context.Context, wConn, pConn *pgconn.PgConn) {
	for _, c := range wireCorpus() {
		t.Run(c.name, func(t *testing.T) {
			cmp := &wireComparison{t: t, c: c}

			// --- Describe: what the server PROMISES before any row moves ----
			wDesc, wErr := wConn.Prepare(ctx, "", c.sql, c.paramOIDs)
			pDesc, pErr := pConn.Prepare(ctx, "", c.oracleSQL(), c.paramOIDs)
			if pErr != nil {
				t.Fatalf("the ORACLE refused to describe this statement: %v\n  SQL: %s", pErr, c.sql)
			}
			if wErr != nil {
				cmp.diverged(wirePropFieldCount, fmt.Sprintf("wadjet cannot describe the statement: %v", wErr))
				return
			}
			cmp.compareFields(wDesc.Fields, pDesc.Fields)
			cmp.compareParamOIDs(wDesc.ParamOIDs, pDesc.ParamOIDs)

			// --- Text result format -----------------------------------------
			wText := readOne(ctx, wConn, c, c.sql, textFormats(len(pDesc.Fields)))
			pText := readOne(ctx, pConn, c, c.oracleSQL(), textFormats(len(pDesc.Fields)))
			if pText.Err != nil {
				t.Fatalf("the ORACLE refused to execute this statement: %v\n  SQL: %s", pText.Err, c.sql)
			}
			if wText.Err != nil {
				cmp.diverged(wirePropValuesText, fmt.Sprintf("wadjet cannot execute the statement: %v", wText.Err))
				return
			}
			cmp.compareFormats(wirePropTextFormat, wText.FieldDescriptions, 0)
			cmp.compareCells(wText, pText)
			cmp.compareTag(wText.CommandTag, pText.CommandTag)

			// --- Binary result format ---------------------------------------
			//
			// The half that found the DATE defect. Asking for binary and being
			// handed the text rendering under a fixed-width OID is not a value
			// error — every value is "right" — and it makes a typed client
			// decode whatever the string happened to contain.
			wBin := readOne(ctx, wConn, c, c.sql, binaryFormats(len(pDesc.Fields)))
			pBin := readOne(ctx, pConn, c, c.oracleSQL(), binaryFormats(len(pDesc.Fields)))
			if pBin.Err != nil {
				t.Fatalf("the ORACLE refused a binary-format execute: %v\n  SQL: %s", pBin.Err, c.sql)
			}
			if wBin.Err != nil {
				cmp.diverged(wirePropBinaryDecode, fmt.Sprintf("wadjet cannot execute under a binary result format: %v", wBin.Err))
				return
			}
			// Sanity: the expectation being held against Wadjet is one
			// PostgreSQL itself meets, or the check is measuring the harness.
			for i, f := range pBin.FieldDescriptions {
				if f.Format != 1 {
					t.Fatalf("the ORACLE answered field %d with format code %d after a binary request, so "+
						"the expectation this arm holds Wadjet to is wrong", i, f.Format)
				}
			}
			cmp.compareFormats(wirePropBinFormat, wBin.FieldDescriptions, 1)
			cmp.compareBinaryDecode(wBin, pBin)

			cmp.finish()
		})
	}
}

// wireComparison accumulates per-property verdicts so a pin can be narrow and
// so a pin that stopped being true fails.
type wireComparison struct {
	t             *testing.T
	c             wireCase
	divergedProps map[string]bool
	compared      map[string]bool
}

func (w *wireComparison) note(prop string) {
	if w.compared == nil {
		w.compared = map[string]bool{}
	}
	w.compared[prop] = true
}

// diverged records that prop differs. A pinned property is logged; an unpinned
// one fails.
func (w *wireComparison) diverged(prop, detail string) {
	w.t.Helper()
	w.note(prop)
	if w.divergedProps == nil {
		w.divergedProps = map[string]bool{}
	}
	w.divergedProps[prop] = true
	if reason, pinned := w.c.pins[prop]; pinned {
		w.t.Logf("known divergence, NOT gated [%s]: %s\n  %s", prop, detail, reason)
		return
	}
	w.t.Errorf("wire divergence [%s]: %s\n  SQL: %s", prop, detail, w.c.sql)
}

// finish fails any pin whose property agreed after all — the pin's deletion is
// the fix's proof, so an outdated pin has to be loud.
func (w *wireComparison) finish() {
	w.t.Helper()
	for prop, reason := range w.c.pins {
		if !w.compared[prop] {
			w.t.Errorf("pin on [%s] names a property this statement never compares — "+
				"either the property name is wrong or the pin belongs on another entry:\n  %s", prop, reason)
			continue
		}
		if !w.divergedProps[prop] {
			w.t.Errorf("wadjet now agrees with PostgreSQL on [%s], so this known divergence is FIXED:\n  %s\n"+
				"Delete the pin on %s in wireCorpus so the property is gated again.", prop, reason, w.c.name)
		}
	}
}

func (w *wireComparison) compareFields(got, want []pgconn.FieldDescription) {
	w.t.Helper()
	w.note(wirePropFieldCount)
	if len(got) != len(want) {
		w.diverged(wirePropFieldCount, fmt.Sprintf("%d fields, PostgreSQL %d", len(got), len(want)))
		return
	}
	w.note(wirePropFieldNames)
	w.note(wirePropTypeOIDs)
	w.note(wirePropTypeSizes)
	w.note(wirePropTypeMods)
	for i := range want {
		if got[i].Name != want[i].Name {
			w.diverged(wirePropFieldNames, fmt.Sprintf("field %d named %q, PostgreSQL %q", i, got[i].Name, want[i].Name))
		}
		if got[i].DataTypeOID != want[i].DataTypeOID {
			w.diverged(wirePropTypeOIDs, fmt.Sprintf("field %d (%s) declared OID %d (%s), PostgreSQL %d (%s)",
				i, want[i].Name, got[i].DataTypeOID, oidName(got[i].DataTypeOID),
				want[i].DataTypeOID, oidName(want[i].DataTypeOID)))
		}
		if got[i].DataTypeSize != want[i].DataTypeSize {
			w.diverged(wirePropTypeSizes, fmt.Sprintf("field %d (%s) declared size %d, PostgreSQL %d",
				i, want[i].Name, got[i].DataTypeSize, want[i].DataTypeSize))
		}
		if got[i].TypeModifier != want[i].TypeModifier {
			w.diverged(wirePropTypeMods, fmt.Sprintf("field %d (%s) type modifier %d, PostgreSQL %d",
				i, want[i].Name, got[i].TypeModifier, want[i].TypeModifier))
		}
	}
	// TableOID and TableAttributeNumber are NOT compared. The protocol defines
	// 0 as "this column is not a simple reference to a table column", and
	// Wadjet answers 0 everywhere because its tables have no pg_class row a
	// client could look up. That is a legal and deliberate difference; a client
	// uses the pair only for updatable result sets, which Wadjet does not offer.
}

func (w *wireComparison) compareParamOIDs(got, want []uint32) {
	w.t.Helper()
	if len(w.c.paramOIDs) == 0 && len(want) == 0 {
		return
	}
	w.note(wirePropParamOIDs)
	if len(got) != len(want) {
		w.diverged(wirePropParamOIDs, fmt.Sprintf("ParameterDescription has %d OIDs %v, PostgreSQL %d %v",
			len(got), got, len(want), want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			w.diverged(wirePropParamOIDs, fmt.Sprintf("parameter $%d declared OID %d (%s), PostgreSQL %d (%s)",
				i+1, got[i], oidName(got[i]), want[i], oidName(want[i])))
		}
	}
}

// compareFormats checks the server honoured the result format the client asked
// for. A server that ignores the request and sends text under a binary format
// code is the DATE defect's shape.
func (w *wireComparison) compareFormats(prop string, fields []pgconn.FieldDescription, want int16) {
	w.t.Helper()
	w.note(prop)
	for i, f := range fields {
		if f.Format != want {
			w.diverged(prop, fmt.Sprintf("field %d (%s) came back with format code %d after the client asked for %d",
				i, f.Name, f.Format, want))
			return
		}
	}
}

// compareCells compares the text-format bytes cell by cell, separating three
// things a single "values differ" would blur: a different VALUE, a different
// SPELLING of the same number, and NULL rendered as something other than a
// negative length.
func (w *wireComparison) compareCells(got, want *pgconn.Result) {
	w.t.Helper()
	w.note(wirePropValuesText)
	w.note(wirePropNullRep)
	w.note(wirePropFloatRender)
	if len(got.Rows) != len(want.Rows) {
		w.diverged(wirePropValuesText, fmt.Sprintf("%d rows, PostgreSQL %d", len(got.Rows), len(want.Rows)))
		return
	}
	for i := range want.Rows {
		if len(got.Rows[i]) != len(want.Rows[i]) {
			w.diverged(wirePropValuesText, fmt.Sprintf("row %d has %d cells, PostgreSQL %d",
				i, len(got.Rows[i]), len(want.Rows[i])))
			return
		}
		for j := range want.Rows[i] {
			g, p := got.Rows[i][j], want.Rows[i][j]
			// NULL is a negative length on the wire, which pgconn surfaces as a
			// nil slice. The empty string is a zero length, a non-nil empty
			// slice. Conflating them is how a NULLed column reads as blank.
			if (g == nil) != (p == nil) {
				w.diverged(wirePropNullRep, fmt.Sprintf("row %d col %d: wadjet sent %s, PostgreSQL sent %s",
					i, j, describeCell(g), describeCell(p)))
				continue
			}
			if g == nil {
				continue
			}
			if string(g) == string(p) {
				continue
			}
			if gf, gok := parseFloat(string(g)); gok {
				if pf, pok := parseFloat(string(p)); pok && nearlyEqual(gf, pf) {
					w.diverged(wirePropFloatRender, fmt.Sprintf("row %d col %d: wadjet spelled %q, PostgreSQL %q (same number)",
						i, j, g, p))
					continue
				}
			}
			w.diverged(wirePropValuesText, fmt.Sprintf("row %d col %d: wadjet %q, PostgreSQL %q", i, j, g, p))
			return
		}
	}
}

// compareBinaryDecode decodes both servers' binary bytes UNDER THE OID EACH
// DECLARED and compares the results. This is the check that catches text bytes
// shipped beneath a fixed-width OID: the value is not wrong, the encoding is,
// and only a decode says so.
func (w *wireComparison) compareBinaryDecode(got, want *pgconn.Result) {
	w.t.Helper()
	w.note(wirePropBinaryDecode)
	m := pgtype.NewMap()
	if len(got.Rows) != len(want.Rows) {
		w.diverged(wirePropBinaryDecode, fmt.Sprintf("%d rows under a binary format, PostgreSQL %d",
			len(got.Rows), len(want.Rows)))
		return
	}
	for i := range want.Rows {
		for j := range want.Rows[i] {
			if j >= len(got.Rows[i]) || j >= len(got.FieldDescriptions) || j >= len(want.FieldDescriptions) {
				return
			}
			gf, pf := got.FieldDescriptions[j], want.FieldDescriptions[j]
			gv, gErr := decodeCell(m, gf, got.Rows[i][j])
			pv, pErr := decodeCell(m, pf, want.Rows[i][j])
			if pErr != nil {
				w.t.Fatalf("the ORACLE's own binary output does not decode under its declared OID %d: %v",
					pf.DataTypeOID, pErr)
			}
			if gErr != nil {
				w.diverged(wirePropBinaryDecode, fmt.Sprintf("row %d col %d (%s): wadjet's bytes %q do not decode "+
					"under the OID %d (%s) it declared, format %d: %v",
					i, j, gf.Name, got.Rows[i][j], gf.DataTypeOID, oidName(gf.DataTypeOID), gf.Format, gErr))
				return
			}
			if !sameDecoded(gv, pv) {
				w.diverged(wirePropBinaryDecode, fmt.Sprintf("row %d col %d (%s): wadjet's binary decodes to %#v, PostgreSQL's to %#v",
					i, j, gf.Name, gv, pv))
				return
			}
		}
	}
}

func (w *wireComparison) compareTag(got, want pgconn.CommandTag) {
	w.t.Helper()
	w.note(wirePropCommandTag)
	if got.String() != want.String() {
		w.diverged(wirePropCommandTag, fmt.Sprintf("CommandComplete %q, PostgreSQL %q", got.String(), want.String()))
	}
}

func decodeCell(m *pgtype.Map, f pgconn.FieldDescription, raw []byte) (any, error) {
	if raw == nil {
		return nil, nil
	}
	var v any
	if err := m.Scan(f.DataTypeOID, f.Format, raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// sameDecoded compares two decoded values across the type each server chose.
// A different OID is reported by compareFields; here only the VALUE matters,
// so both sides render through the same canonicalization the value oracle uses.
func sameDecoded(got, want any) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	gs, ps := canonicalString(normalizePostgresValue(got)), canonicalString(normalizePostgresValue(want))
	if gs == ps {
		return true
	}
	gf, gok := parseFloat(gs)
	pf, pok := parseFloat(ps)
	return gok && pok && nearlyEqual(gf, pf)
}

// nearlyEqual is the relative-difference test the DuckDB arm's cellEqual
// already applies, at the same floatEps: one ULP of accumulation noise is not a
// wire divergence.
func nearlyEqual(a, b float64) bool {
	if a == b {
		return true
	}
	denom := math.Max(math.Abs(a), math.Abs(b))
	if denom == 0 {
		return false
	}
	return math.Abs(a-b)/denom < floatEps
}

func describeCell(b []byte) string {
	if b == nil {
		return "NULL (negative length)"
	}
	if len(b) == 0 {
		return `"" (zero length)`
	}
	return strconv.Quote(string(b))
}

func readOne(ctx context.Context, conn *pgconn.PgConn, c wireCase, sql string, resultFormats []int16) *pgconn.Result {
	paramFormats := make([]int16, len(c.params)) // all text unless a case says otherwise
	return conn.ExecParams(ctx, sql, c.params, c.paramOIDs, paramFormats, resultFormats).Read()
}

// oracleSQL is the spelling to send to PostgreSQL: the entry's own, unless it
// declares a dialect variant.
func (c wireCase) oracleSQL() string {
	if c.pgSQL != "" {
		return c.pgSQL
	}
	return c.sql
}

func textFormats(n int) []int16 { return make([]int16, n) }
func binaryFormats(n int) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = 1
	}
	return out
}

// oidName renders the handful of OIDs this arm can produce, so a failure reads
// as "int4 against text" rather than "23 against 25".
func oidName(oid uint32) string {
	switch oid {
	case 0:
		return "unspecified"
	case 16:
		return "bool"
	case 20:
		return "int8"
	case 21:
		return "int2"
	case 23:
		return "int4"
	case 17:
		return "bytea"
	case 25:
		return "text"
	case 700:
		return "float4"
	case 701:
		return "float8"
	case 705:
		return "unknown"
	case 1042:
		return "bpchar"
	case 1043:
		return "varchar"
	case 1082:
		return "date"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamptz"
	case 1700:
		return "numeric"
	default:
		return "oid " + strconv.FormatUint(uint64(oid), 10)
	}
}

// int4Text encodes an int32 the way a client sends a parameter under the TEXT
// parameter format, which is what pgx sends for a value it did not pre-encode.
func int4Text(v int32) []byte { return []byte(strconv.FormatInt(int64(v), 10)) }

// The pins the wire corpus shares, written once so the same defect cannot
// acquire two descriptions.
const (
	// #454 (type_modifiers) and #453 (float_text_render) were pinned here on
	// both DECIMAL entries. Both are fixed and the pins are gone, which is
	// the proof: ColumnMeta now carries the declared precision and scale and
	// pgTypeMod packs them the way numerictypmodin does, and FormatDecimal
	// renders the fraction at its declared scale instead of trimming it. The
	// two properties are gated again.

	// avgDecimalDigitsPin is a DELIBERATE difference of SCALE, recorded in
	// ADR-0012 item 9: PostgreSQL's numeric division picks a result scale
	// giving at least 16 significant digits, while wadjet widens the input's
	// scale by a fixed 4 (batch.AvgScaleIncrement) so that the same query
	// over more rows cannot silently change the scale of its own output
	// column. Both engines divide EXACTLY and agree to min(both scales);
	// they differ only in how many digits past that they print, which is a
	// difference no value comparison can express.
	//
	// It is pinned on float_text_render ALONE — the property for "same
	// number, different spelling". The OID, the declared size, the modifier
	// and the normalised value all agree, and those are what #586 is about:
	// a right average under a float8 OID is the defect a value oracle cannot
	// see.
	avgDecimalDigitsPin = "DELIBERATE (ADR-0012 item 9): AVG over a numeric widens the input's " +
		"scale by a fixed 4 (batch.AvgScaleIncrement) where PostgreSQL picks a scale giving at " +
		"least 16 significant digits. Both engines divide EXACTLY and agree to min(both scales); " +
		"they differ only in how many digits past that they print"

	// setOpDecimalDigitsPin is a DELIBERATE difference of CARRIER, recorded in
	// ADR-0012 item 12: PostgreSQL's numeric is variable-scale, so a set
	// operation renders each arm's rows at that ARM's original scale
	// ("-6.00" beside "-6.1875"); a wadjet DECIMAL column has ONE declared
	// scale, so every row of the result prints at it. Same number, same row
	// set, same class as AVG's digit count in item 9.
	//
	// The digits differ in the OTHER direction today, for a second and
	// temporary reason: this server takes the single-process path, which
	// builds the result under the FIRST arm's schema and so renders the WIDER
	// arm's rows too narrow (#532). When that lands the direction flips and
	// this pin still holds — which is why it is a deliberate pin and not a bug
	// pin. The typmod pin beside it is the one that must disappear.
	setOpDecimalDigitsPin = "DELIBERATE (ADR-0012 item 12): PostgreSQL's variable-scale numeric renders " +
		"each arm of a set operation at that arm's own scale; a wadjet DECIMAL column has one declared " +
		"scale and renders every row at it. Same number, same row set. Today the difference also " +
		"carries #532's narrowing on the single-process path, which is a defect and is pinned " +
		"separately in the semantics corpus"

	// choiceDecimalDigitsPin is setOpDecimalDigitsPin's twin for a CHOICE
	// expression, and it is deliberate for the same reason: PostgreSQL's
	// numeric carries a per-VALUE dscale, so COALESCE renders whichever
	// branch won at THAT branch's scale, while a wadjet DECIMAL column has
	// ONE declared scale — ADR-0024 item 2's common type — and renders every
	// row at it. Same number, same row set, and the type modifier beside it
	// is compared and agrees. It shows up on COALESCE and not on GREATEST
	// over the same pair only because GREATEST's winner happens to be the
	// wider column on every row of the fixture.
	choiceDecimalDigitsPin = "DELIBERATE (ADR-0012 item 12, ADR-0024 item 2): PostgreSQL renders each " +
		"numeric at the dscale of the value that produced it; a wadjet DECIMAL column has one declared " +
		"scale — the common type of the branches — and renders every row at it. Same number, same rows"

	// noExactNumericPin is a DELIBERATE difference, documented rather than
	// fixed: the engine has one numeric tower and it is float64.
	noExactNumericPin = "DELIBERATE: Wadjet has no exact numeric type. PostgreSQL promotes SUM(int4) to " +
		"int8 and AVG(int4) to numeric so neither can lose a digit; Wadjet computes both in float64 and " +
		"declares OID 701. benchmarks/tpch/schema.go states the position for the fixture itself " +
		"(\"monetary values use FLOAT64\"), and the engine has no DECIMAL kernel to promote into. Pinned, " +
		"not exempted: the day a NUMERIC type lands, this entry fails and says so. The client-visible " +
		"cost is real — a JDBC client reads a BigDecimal column as a Double, and a SUM past 2^53 loses " +
		"low digits"
)

// wireCorpus is the statement set the protocol arm compares. Each entry is
// chosen for the TYPE it forces into the RowDescription, not for its rows: the
// answer is usually two or three rows, because the wire metadata is the subject.
// networkAsTextOIDPin records the standing network-as-text wire choice: every
// IPV4/IPV6/CIDR column is declared OID 25 (text) where PostgreSQL declares
// 869 (inet)/650 (cidr). It is DELIBERATE — the same choice VECTOR takes, so a
// generic client renders the value rather than needing an inet type plan — and
// a difference of TYPE, not of value, which is exactly what the wire arm
// exists to record and a value oracle cannot see. Not #569's doing: #569 only
// made a windowed MIN/MAX over these types produce a column at all.
const networkAsTextOIDPin = "DELIBERATE (network-as-text on the wire, like VECTOR): wadjet declares " +
	"OID 25 (text) for an IPV4/IPV6/CIDR column where PostgreSQL declares 869 (inet). A difference of " +
	"type, not value; the wire arm is the only place it is visible. The pin fails if wadjet ever adopts " +
	"the inet OID"

func wireCorpus() []wireCase {
	base := []wireCase{
		// The shape that broke DataGrip: a plain projection of an int, a text
		// and an int. Every column used to be declared OID 25; the OIDs are
		// right now, and this entry is what keeps them right.
		{name: "IntTextInt", sql: `SELECT n_nationkey, n_name, n_regionkey FROM nation ORDER BY n_nationkey LIMIT 3`},
		// A float column, where the declared OID and the text spelling of the
		// value are separate questions.
		{name: "Float8Column", sql: `SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_orderkey LIMIT 3`},
		// COUNT(*) is int8 in PostgreSQL, and a driver that reads it as int4
		// truncates silently past 2^31. Wadjet agrees on the OID here.
		{name: "CountStar", sql: `SELECT COUNT(*) AS c FROM nation`},
		// SUM over an int4 column is int8 in PostgreSQL and AVG is numeric.
		{name: "SumAvgOverInteger", sql: `SELECT SUM(n_regionkey) AS s, AVG(n_regionkey) AS a FROM nation`,
			pins: map[string]string{
				wirePropTypeOIDs: noExactNumericPin,
				wirePropTypeSizes: noExactNumericPin + " — the declared SIZE follows the OID: 8 for float8, " +
					"-1 for a variable-length numeric",
				wirePropFloatRender: "DELIBERATE, same cause: PostgreSQL renders a NUMERIC average with its " +
					"full scale (\"2.0000000000000000\") and Wadjet renders a float64 (\"2\"). Same number, " +
					"and any client that parses it gets the same value; a client that string-compares does not",
			}},
		// A literal of each basic type, which is how a client probes a server.
		{name: "LiteralTypes", sql: `SELECT 1 AS i, 'x' AS t, TRUE AS b, 1.5 AS f`,
			pins: map[string]string{
				wirePropTypeOIDs: "DELIBERATE for the integer, deliberate-by-consequence for the decimal: " +
					"Wadjet widens every integer literal to INT64 (OID 20) where PostgreSQL types it int4 " +
					"(23), and types 1.5 as float8 (701) where PostgreSQL uses numeric (1700). The integer " +
					"widening is safe in one direction only — a client asking for an Integer column gets a " +
					"Long — and the decimal is the no-exact-numeric position again",
				wirePropTypeSizes: "DELIBERATE, follows the OIDs above (8 for int8/float8, 4 and -1 in PostgreSQL)",
			}},
		// NULL beside the empty string: the wire tells them apart by length,
		// and a renderer that does not is invisible to a value comparison.
		// Wadjet gets this right — the entry exists to keep it right.
		{name: "NullAndEmpty", sql: `SELECT NULL AS nul, '' AS empty, 'x' AS filled`},
		// A NULL in a typed column, rather than a bare NULL literal.
		{name: "NullInTypedColumn", sql: `SELECT n_nationkey, NULLIF(n_regionkey, 1) AS k FROM nation ORDER BY n_nationkey LIMIT 6`},
		// A DATE expression: the declared OID (1082, size 4), the text value,
		// and the 4-byte binary day count are gated since #363.
		{name: "DateColumn", sql: `SELECT CAST('1996-01-10' AS date) AS d`},
		// A TIMESTAMP expression: declared 1114 and rendered, not the boxed
		// epoch-millisecond integer (#363).
		{name: "TimestampColumn", sql: `SELECT CAST('1996-01-10 12:34:56' AS timestamp) AS ts`},
		// A boolean expression, whose PostgreSQL text form is 't'/'f' and
		// whose binary form is one byte (#364).
		{name: "BooleanExpression", sql: `SELECT (n_regionkey = 1) AS is_one FROM nation ORDER BY n_nationkey LIMIT 4`},
		// A DECIMAL column: the declared OID (1700 numeric, since a DECIMAL's
		// values are exact and pgFormatType already answered "numeric" for the
		// same type in pg_attribute), the text rendering, and the base-10000
		// binary digit vector. The binary arm is the one the OID change makes
		// load-bearing: under OID 25 the generic string encoder was right,
		// because the binary form of a text column IS its bytes.
		{name: "DecimalColumn", sql: `SELECT d_2, d_4 FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		// The wide arm, whose values need more than 64 bits — the range no
		// float8 fallback could carry and the one #437's old reader truncates.
		{name: "WideDecimalColumn", sql: `SELECT d_wide FROM dec_probe WHERE d_key IN (1, 100, 150) ORDER BY d_key`},
		// A ZERO-ROW DECIMAL result (#458): d_key never goes negative, so
		// this matches nothing on either engine. RowDescription is sent
		// before any DataRow regardless of row count, so the type modifier
		// is exactly as comparable here as on DecimalColumn above — the
		// difference #458 hid is that Wadjet's zero-row path took a
		// PLAN-DECLARED schema (declaredOutputSchema) rather than one read
		// off a batch, and that path used to carry no precision/scale at
		// all (typmod -1, "unconstrained") where PostgreSQL still declares
		// numeric(9,2)/numeric(18,4).
		{name: "DecimalColumnZeroRows", sql: `SELECT d_2, d_4 FROM dec_probe WHERE d_key = -1`},
		// #534: the same zero-row DECIMAL result, reached through a NaN
		// literal. It belongs on this arm as well as the value one because
		// the failure it replaces was an ERROR on the wire — 22P02 where
		// PostgreSQL sends RowDescription and a command tag — and a value
		// oracle cannot tell "answered zero rows" from "raised" at all.
		{name: "DecimalColumnZeroRowsViaNaN", sql: `SELECT d_2, d_4 FROM dec_probe WHERE d_2 = 'NaN'`},
		{name: "DecimalColumnAllRowsViaNegInfinity",
			sql: `SELECT d_2 FROM dec_probe WHERE d_2 > '-Infinity' AND d_key < 4 ORDER BY d_key`},
		// MIN/SUM over a DECIMAL column (FIX 2, fold-in to #457/#458): live
		// PostgreSQL's \gdesc declares typmod -1 ("unconstrained numeric")
		// for MIN(numeric(p,s)), SUM(numeric(p,s)), and every other
		// aggregate over a numeric column — typmod survives ONLY a bare
		// column reference, never a function call, aggregate or otherwise.
		// DecimalColumn/DecimalColumnZeroRows above cover the bare-column
		// half; these four cover the aggregate half, zero-row and
		// non-zero-row, so a divergence here can no longer hide the way it
		// did before typmod (-1) was compared at all (#454) and then before
		// the zero-row plan-declared path was compared (#458): both
		// entries pass with NO pin, because after FIX 2 wadjet agrees with
		// PostgreSQL outright rather than needing an exemption.
		// A BARE DECIMAL column reference with a SUBQUERY elsewhere in the
		// statement (#697). PostgreSQL keeps the column's typmod for every one
		// of these — verified live on 17.11's \gdesc — and wadjet declared
		// them unconstrained: the declared-type walk had no arm for a JOIN, so
		// a decorrelated correlated subquery (a Join over an Aggregate) or a
		// derived table (a Join over a Project) nilled the whole type map.
		// The ZERO-ROW entries are the stronger half: with no batch to re-type
		// from, that same nil declared the column TEXT (OID 25).
		{name: "DecimalColumnBehindInSubquery",
			sql: `SELECT d_2 FROM dec_probe WHERE d_key IN (SELECT d_key FROM dec_probe WHERE d_key < 4)
				ORDER BY d_key`},
		{name: "DecimalColumnBehindExists",
			sql: `SELECT d_2 FROM dec_probe t WHERE EXISTS
				(SELECT 1 FROM dec_probe u WHERE u.d_key = t.d_key AND u.d_key < 4) ORDER BY t.d_key`},
		{name: "DecimalColumnBehindScalarSubquery",
			sql: `SELECT d_2 FROM dec_probe WHERE d_key = (SELECT MIN(d_key) FROM dec_probe WHERE d_key > 0)`},
		// The TPC-H Q02 shape.
		{name: "DecimalColumnBehindCorrelatedSubquery",
			sql: `SELECT d_2 FROM dec_probe t WHERE t.d_key =
				(SELECT MIN(u.d_key) FROM dec_probe u WHERE u.d_grp = t.d_grp) ORDER BY t.d_key`},
		// The TPC-H Q18 shape: IN over a GROUPED subquery.
		{name: "DecimalColumnBehindGroupedInSubquery",
			sql: `SELECT d_2 FROM dec_probe WHERE d_key IN
				(SELECT d_key FROM dec_probe WHERE d_key < 4 GROUP BY d_key) ORDER BY d_key`},
		// No subquery PREDICATE at all — a derived table is enough, because
		// the Project on the join's other side is what the walk could not
		// cross.
		{name: "DecimalColumnBehindDerivedTableJoin",
			sql: `SELECT t.d_2 FROM dec_probe t JOIN (SELECT d_key AS k FROM dec_probe WHERE d_key < 4) s
				ON t.d_key = s.k ORDER BY t.d_key`},
		{name: "DecimalColumnBehindCorrelatedSubqueryZeroRows",
			sql: `SELECT d_2 FROM dec_probe t WHERE t.d_key < 0 AND t.d_key =
				(SELECT MIN(u.d_key) FROM dec_probe u WHERE u.d_grp = t.d_grp)`},
		{name: "DecimalColumnBehindDerivedTableJoinZeroRows",
			sql: `SELECT t.d_2 FROM dec_probe t JOIN (SELECT d_key AS k FROM dec_probe WHERE d_key < 0) s
				ON t.d_key = s.k`},
		// A CHOICE construct over a DECIMAL branch and a numeric LITERAL —
		// ADR-0024 item 5's select_common_typmod, over the pair #695 made
		// reachable. The literal is a numeric with typmod -1, so it DISAGREES
		// with the column and the result is unconstrained; NULLIF folds over
		// argument 0 alone and therefore keeps numeric(9,2). Both verified
		// live against 17.11's \gdesc, and both are the whole point of reading
		// the candidate list off the registry's declaration rather than
		// hand-listing the constructs.
		{name: "DecimalChoiceIntegerLiteral",
			sql:  `SELECT GREATEST(d_2, 0) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{wirePropFloatRender: choiceDecimalDigitsPin}},
		{name: "DecimalChoiceIntegerLiteralZeroRows",
			sql: `SELECT GREATEST(d_2, 0) AS v FROM dec_probe WHERE d_key = -1`},
		{name: "DecimalChoiceCaseIntegerElse",
			sql: `SELECT CASE WHEN d_grp < 3 THEN d_2 ELSE 0 END AS v FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{wirePropFloatRender: choiceDecimalDigitsPin}},
		{name: "DecimalChoiceFractionalLiteralElse",
			sql: `SELECT CASE WHEN d_grp < 3 THEN d_2 ELSE 0.125 END AS v FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{wirePropFloatRender: choiceDecimalDigitsPin}},
		{name: "DecimalChoiceIntegerColumn",
			sql: `SELECT LEAST(d_2, d_grp) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "DecimalChoiceNullifIntegerLiteral",
			sql: `SELECT NULLIF(d_2, 0) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "DecimalChoiceNullifIntegerLiteralZeroRows",
			sql: `SELECT NULLIF(d_2, 0) AS v FROM dec_probe WHERE d_key = -1`},
		// The INT-FIRST direction, which the three entries above cannot see:
		// they all put the DECIMAL in argument 0, where NULLIF's typmod comes
		// from. With the DECIMAL in argument 1 the TYPE folds to numeric while
		// argument 0 carries no numeric modifier, so PostgreSQL sends -1 —
		// and wadjet sent the folded (p,s) until the modifier was checked
		// against the column's own declaration. Invisible before #695, when
		// the column declared int8 and had no numeric modifier at all.
		{name: "DecimalChoiceNullifIntegerColumnFirst",
			sql:  `SELECT NULLIF(d_key, d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{wirePropFloatRender: choiceDecimalDigitsPin}},
		{name: "DecimalChoiceNullifIntegerLiteralFirst",
			sql:  `SELECT NULLIF(0, d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{wirePropFloatRender: choiceDecimalDigitsPin}},
		{name: "DecimalChoiceNullifIntegerColumnFirstZeroRows",
			sql: `SELECT NULLIF(d_key, d_2) AS v FROM dec_probe WHERE d_key = -1`},
		{name: "MinOverDecimalColumn", sql: `SELECT MIN(d_2) AS lo FROM dec_probe WHERE d_key IN (1, 2, 3)`},
		{name: "MinOverDecimalColumnZeroRows", sql: `SELECT MIN(d_2) AS lo FROM dec_probe WHERE d_key = -1`},
		{name: "SumOverDecimalColumn", sql: `SELECT SUM(d_2) AS s FROM dec_probe WHERE d_key IN (1, 2, 3)`},
		{name: "SumOverDecimalColumnZeroRows", sql: `SELECT SUM(d_2) AS s FROM dec_probe WHERE d_key = -1`},
		// The WINDOWED form of the same aggregate. It is a separate entry
		// because it is a separate declaration path: an aggregate reaches the
		// output projection as an aggregate (proj.IsAgg, which
		// declaredWireUnconstrainedDecimal reads), a window function reaches
		// it as a bare reference to the Window operator's output column.
		//
		// Before #569 there was nothing to compare here at all — a windowed
		// MIN over a DECIMAL column FAILED the query ("cannot store string
		// into FLOAT64 vector"), so no RowDescription was ever sent. The OID
		// and the value are right now; the typmod is #587.
		// It carries NO pin since ADR-0024: declaredWireUnconstrainedDecimal
		// gates on "not a bare column reference", so a window function's
		// DECIMAL output goes out with typmod -1 the way PostgreSQL's does
		// (#587). Deleting the pin is the fix's proof.
		{name: "WindowMinOverDecimalColumn",
			sql: `SELECT d_key, MIN(d_2) OVER (PARTITION BY d_grp) AS lo FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		// The ZERO-ROW form is a DIFFERENT declaration path: it is described
		// from the PLAN (declaredOutputSchema), with no batch to re-type
		// from, so it used to be pinned on the OID as well as the modifier —
		// colRefDeclaredType returned Undecided for every DECIMAL, the
		// column kept windowOutputType's FLOAT64, and the same query went
		// out as float8 when it matched nothing and numeric when it matched
		// rows. Since ADR-0024 a DECIMAL column reference decides its own
		// (p,s) and windowSpecOutputType resolves it, so both halves of #587
		// agree with PostgreSQL and neither is pinned.
		{name: "WindowMinOverDecimalColumnZeroRows",
			sql: `SELECT d_key, MIN(d_2) OVER (PARTITION BY d_grp) AS lo FROM dec_probe WHERE d_key = -1`},
		// The ACCUMULATING windowed aggregates (#586, #475). MIN/MAX above
		// copy an input value; SUM and AVG build one, and until ADR-0024 they
		// built it in float64 and went out under OID 701 where PostgreSQL
		// sends 1700 — a value oracle reading "4126669.6257" against
		// 4.1266696257e+06 sees two spellings of one number and cannot say
		// which type the client will bind. Only this arm can.
		//
		// No pin on either: the OID, the size and the modifier (-1, since a
		// window function is a function call — ADR-0024 item 5) all agree
		// with PostgreSQL now, and the SUM's digits agree too, since
		// sum(numeric(9,2)) keeps scale 2 on both engines.
		{name: "WindowSumOverDecimalColumn",
			sql: `SELECT d_key, SUM(d_2) OVER (PARTITION BY d_grp) AS s FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		// The ZERO-ROW form, described from the PLAN alone with no batch to
		// re-type from — the path that made #587 visible for MIN and would
		// have hidden a FLOAT64 SUM declaration the same way.
		{name: "WindowSumOverDecimalColumnZeroRows",
			sql: `SELECT d_key, SUM(d_2) OVER (PARTITION BY d_grp) AS s FROM dec_probe WHERE d_key = -1`},
		// AVG's printed DIGITS are a deliberate divergence and are pinned
		// (wadjet prints scale+4, PostgreSQL a scale giving 16 significant
		// digits); its OID, size, modifier and normalised value are not, and
		// they are what this entry is for.
		{name: "WindowAvgOverDecimalColumn",
			sql: `SELECT d_key, AVG(d_2) OVER (PARTITION BY d_grp) AS a FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{wirePropFloatRender: avgDecimalDigitsPin}},
		{name: "WindowAvgOverDecimalColumnZeroRows",
			sql: `SELECT d_key, AVG(d_2) OVER (PARTITION BY d_grp) AS a FROM dec_probe WHERE d_key = -1`},
		// The window's OUTPUT NAME spelled like an input column (#694). This
		// is the arm only the WIRE can judge: the value oracle sees a number
		// under the name it asked for and cannot say whether the DECLARATION
		// came from the window or from the column the alias shadows.
		//
		// Live PostgreSQL 17 \gdesc declares all of these `numeric` with
		// typmod -1: a window function is a function call, so no column's
		// modifier survives into it (ADR-0024 item 5).
		//
		// `AS d_wide` shadows numeric(38,10) and `AS d_key` shadows a BIGINT,
		// and both FAIL on the unfixed tree — the first on the modifier, the
		// second on the OID and the binary width. `AS d_2` shadows
		// numeric(9,2) and the zero-row form pass either way, because the
		// shadowed reading and the window's own declaration happen to agree
		// there; they are kept as the controls that say so.
		{name: "WindowSumAliasShadowsAWideDecimalColumn",
			sql: `SELECT d_key, SUM(d_2) OVER (PARTITION BY d_grp) AS d_wide FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "WindowSumAliasShadowsItsOwnArgument",
			sql: `SELECT d_key, SUM(d_2) OVER (PARTITION BY d_grp) AS d_2 FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "WindowSumAliasShadowsABigintColumn",
			sql: `SELECT d_grp, SUM(d_2) OVER (PARTITION BY d_grp) AS d_key FROM dec_probe
				WHERE d_grp IN (0, 1) ORDER BY d_grp LIMIT 4`},
		// The ZERO-ROW form of the shadowing alias, described from the PLAN
		// with no batch to re-type from — the path #587 lived in, asked of the
		// name collision.
		{name: "WindowSumAliasShadowsAWideDecimalColumnZeroRows",
			sql: `SELECT d_key, SUM(d_2) OVER (PARTITION BY d_grp) AS d_wide FROM dec_probe
				WHERE d_key = -1`},
		// ROW_NUMBER shadowing an INTEGER column, which is the sharpest of
		// the set: live PostgreSQL declares `bigint` for row_number() and
		// d_grp is `integer`, so a declaration taken from the shadowed column
		// goes out under OID 23 with a 4-byte binary form where PostgreSQL
		// sends OID 20 and eight bytes. Nothing about the printed digits
		// differs.
		{name: "WindowRowNumberAliasShadowsAnIntColumn",
			sql: `SELECT d_key, ROW_NUMBER() OVER (ORDER BY d_key) AS d_grp FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		// The CONTROL for the two above: MIN/MAX over a window of a
		// non-DECIMAL column, where no typmod is in play. It carries no pin,
		// which is what proves the pinned entries are about the modifier and
		// not about windowed MIN/MAX declaring a wrong type in general.
		{name: "WindowMinMaxOverIntAndText",
			sql: `SELECT o_orderkey, MIN(o_custkey) OVER (PARTITION BY o_orderstatus) AS lo,
				MAX(o_orderstatus) OVER (PARTITION BY o_orderstatus) AS hi
				FROM orders WHERE o_orderkey IN (1, 2, 3) ORDER BY o_orderkey`},
		// Windowed MIN/MAX over the three network types that map onto
		// PostgreSQL's `inet` (#569). Before the fix a windowed MIN/MAX over
		// any of them FAILED the query, so there was no RowDescription to
		// compare at all; now there is, and the wire arm is the only one that
		// can see what OID it carries — a value oracle reads a right address
		// under a wrong type and cannot tell. PostgreSQL has min(inet), so
		// this is gated against a real reference, unlike the four types it has
		// no aggregate for (ADR-0012 §5).
		//
		// wadjet declares OID 25 (text) for every network column — the
		// standing network-as-text choice, the same one VECTOR takes — where
		// PostgreSQL declares 869 (inet). That is a deliberate divergence of
		// TYPE, pinned here on the OID and its dependent size; every other
		// wire property stays gated — the declared SIZE (-1 on both, text and
		// inet are both variable-length), the values on the wire (a rendered
		// address matches inet's text form), the field count, the names, the
		// null representation. The pin fails the day wadjet declares 869,
		// which is the only way network-as-text is ever revisited.
		{name: "WindowMinMaxOverInet",
			sql: `SELECT n_key,
				MIN(n_v4) OVER (PARTITION BY n_grp) AS v4,
				MIN(n_v6) OVER (PARTITION BY n_grp) AS v6,
				MIN(n_cidr) OVER (PARTITION BY n_grp) AS c
				FROM net_probe WHERE n_key IN (1, 2, 3) ORDER BY n_key`,
			pins: map[string]string{
				// type_oids ONLY. The declared SIZE agrees at -1 on both
				// sides — text and inet are each variable-length — so pinning
				// it would be a false pin the ratchet reports as already
				// fixed. The values on the wire agree too (a rendered address
				// matches inet's text form), which is the whole point: right
				// value, wrong OID, and only this arm sees the OID.
				wirePropTypeOIDs: networkAsTextOIDPin,
			}},
		// A SET OPERATION over two DECIMAL columns of different (p,s). Live
		// PostgreSQL's \gdesc declares the result UNCONSTRAINED — plain
		// `numeric`, typmod -1 — where the same column selected on its own is
		// `numeric(9,2)`: a set operation keeps the arms' typmod only when
		// every arm agrees on it, and drops it otherwise. Wadjet declares a
		// real (p,s) either way, which is #542.
		//
		// The VALUES agree (the DAG widens both arms since #533, and this
		// server takes the single-process path where #532 governs the
		// rendering), so a value oracle cannot see this at all — it is exactly
		// the class the wire arm exists for. Pinned per PROPERTY, so every
		// other property of this statement stays gated.
		{name: "SetOpAcrossDecimalScales",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (0, 4, 8)
				UNION ALL SELECT d_4 FROM dec_probe WHERE d_key IN (0, 4, 8) ORDER BY 1`,
			pins: map[string]string{
				wirePropFloatRender: setOpDecimalDigitsPin,
			}},
		// The control for the entry above: BOTH arms are the same column, so
		// PostgreSQL KEEPS numeric(9,2) and wadjet agrees outright. It carries
		// no pin, which is what proves the pinned entry is about the arms
		// disagreeing and not about set operations in general.
		{name: "SetOpSameDecimalScale",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (1, 2)
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key IN (2, 3) ORDER BY 1`},
		// #542's two directions on the shapes the entries above do not
		// reach: a ZERO-ROW set operation, where the answer comes from the
		// plan alone and no batch can correct it, and the other set-op
		// flavours. Both must keep the typmod when the arms agree and drop
		// it when they do not — a rule that answered "always drop" would
		// pass the disagreeing half and fail here, which is why both
		// directions are gated.
		{name: "SetOpAcrossDecimalScalesZeroRows",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key = -1
				UNION ALL SELECT d_4 FROM dec_probe WHERE d_key = -1`},
		{name: "SetOpSameDecimalScaleZeroRows",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key = -1
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key = -1`},
		{name: "IntersectAcrossDecimalScales",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (0, 4, 8)
				INTERSECT SELECT d_4 FROM dec_probe WHERE d_key IN (0, 4, 8) ORDER BY 1`},
		// select_common_typmod, both directions (ADR-0024 item 5). A numeric
		// result KEEPS its inputs' type modifier when every one of them
		// carries the SAME one and is unconstrained otherwise — which is
		// NOT "computed means unconstrained": these seven entries all
		// describe as numeric(9,2) or numeric(18,4) on live PostgreSQL,
		// while the ones below them describe as plain numeric. NULLIF folds
		// argument 0 ALONE, the same candidate list its type resolution
		// folds, which is why the two NULLIF spellings answer differently.
		{name: "GreatestOverOneDecimalColumn",
			sql: `SELECT GREATEST(d_2, d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "GreatestOfASingleDecimalArgument",
			sql: `SELECT GREATEST(d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "CoalesceOverOneDecimalColumn",
			sql: `SELECT COALESCE(d_2, d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "CaseOverOneDecimalColumn",
			sql: `SELECT CASE WHEN d_key > 1 THEN d_2 ELSE d_2 END AS v FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "LeastOverOneWideDecimalColumn",
			sql: `SELECT LEAST(d_4, d_4) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "NullifKeepsItsFirstArgumentsTypmod",
			sql: `SELECT NULLIF(d_2, d_4) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "NullifTheOtherWayRound",
			sql: `SELECT NULLIF(d_4, d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "CoalesceWithANullBranch",
			sql: `SELECT COALESCE(d_2, NULL) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		// The other direction: inputs whose modifiers DIFFER, and inputs
		// that carry none at all.
		{name: "GreatestAcrossDecimalScales",
			sql: `SELECT GREATEST(d_2, d_4) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "CoalesceAcrossDecimalScales",
			sql: `SELECT COALESCE(d_2, d_4) AS v FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY d_key`,
			pins: map[string]string{
				wirePropFloatRender: choiceDecimalDigitsPin,
			}},
		// A CASE with no ELSE: the implicit NULL branch is untyped, so the
		// modifier goes the way CoalesceWithANullBranch's does.
		{name: "CaseWithoutElseOverOneDecimalColumn",
			sql: `SELECT CASE WHEN d_key > 1 THEN d_2 END AS v FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "GreatestAcrossDecimalScalesZeroRows",
			sql: `SELECT GREATEST(d_2, d_4) AS v FROM dec_probe WHERE d_key = -1`},
		// An AGGREGATE whose argument is a choice expression is still an
		// aggregate call, and PostgreSQL gives every one of those typmod -1.
		// The plan cannot type these — aggSpecOutputDecimal declines a
		// non-bare-ColRef input — so gating the wire mark on "the plan says
		// DECIMAL" skipped exactly them while the runtime schema resolved a
		// real (p,s), and the modifier went out on the wire.
		{name: "MaxOverAChoiceOfOneDecimalColumn",
			sql: `SELECT MAX(COALESCE(d_2, d_2)) AS v FROM dec_probe WHERE d_key IN (1, 2, 3)`},
		{name: "MinOverAChoiceOfOneDecimalColumn",
			sql: `SELECT MIN(GREATEST(d_2, d_2)) AS v FROM dec_probe WHERE d_key IN (1, 2, 3)`},
		{name: "SumOverAChoiceAcrossDecimalScales",
			sql: `SELECT SUM(COALESCE(d_2, d_4)) AS v FROM dec_probe WHERE d_key IN (1, 2, 3)`,
			pins: map[string]string{
				// The SUM inherits its argument's rendering: COALESCE takes
				// the narrow column on every row here, which wadjet renders
				// at the common scale and PostgreSQL at the value's own.
				wirePropFloatRender: choiceDecimalDigitsPin,
			}},
		{name: "MinOverACaseOfOneDecimalColumn",
			sql: `SELECT MIN(CASE WHEN d_key > 1 THEN d_2 ELSE d_2 END) AS v FROM dec_probe
				WHERE d_key IN (1, 2, 3)`},
		// A set operation over an arm that carries NO modifier is
		// unconstrained however well the arms' widths line up — the shape
		// "the arms' (p,s) disagree" alone cannot see.
		{name: "SetOpOverAnAggregateArm",
			sql: `SELECT MIN(d_2) AS v FROM dec_probe WHERE d_key IN (1, 2, 3)
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY 1`},
		{name: "SetOpOverAComputedArm",
			sql: `SELECT COALESCE(d_2, d_4) AS v FROM dec_probe WHERE d_key IN (1, 2, 3)
				UNION ALL SELECT d_4 FROM dec_probe WHERE d_key IN (1, 2, 3) ORDER BY 1`,
			pins: map[string]string{
				wirePropFloatRender: setOpDecimalDigitsPin,
			}},
		{name: "ExceptSameDecimalScale",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (1, 2)
				EXCEPT SELECT d_2 FROM dec_probe WHERE d_key IN (2, 3) ORDER BY 1`},
		// A parameter bound by its DECLARED type rather than as a string —
		// the shape of the 4a25af0 fix, and the one that exercises
		// ParameterDescription.
		{name: "ParamInt4Text", sql: `SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Text(7)}},
		// The same statement with the OID NOT declared, which is what a client
		// that lets the server infer parameter types sends — and the shape
		// behind DataGrip's "Bad value for type int : f". The server must
		// infer int4 from the comparison for ParameterDescription AND bind the
		// value to the same row the declared path binds to (#365).
		{name: "ParamInt4Inferred", sql: `SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = $1`,
			params: [][]byte{int4Text(7)}},
		{name: "ParamText", sql: `SELECT n_nationkey FROM nation WHERE n_name = $1`,
			paramOIDs: []uint32{25}, params: [][]byte{[]byte("BRAZIL")}},
		// An aggregate over a parameterized filter, so the parameter has to
		// survive into a plan rather than only into a scan predicate.
		{name: "ParamInAggregate", sql: `SELECT COUNT(*) AS c FROM nation WHERE n_regionkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Text(1)}},

		// --- Output column NAMES for unaliased items (#513) -----------------
		//
		// The name in RowDescription is what a client shows and binds by, and
		// it is the one thing only this arm compares — the semantics arm
		// compares cells POSITIONALLY on purpose. PostgreSQL labels an
		// unaliased function call with the FUNCTION's name; wadjet labelled it
		// from the expression's text with everything up to the first '.'
		// stripped, so `UPPER(supplier.s_name)` came back as `s_name)`,
		// parenthesis included.
		{name: "UnaliasedFuncOverQualifiedColumn",
			sql: `SELECT UPPER(supplier.s_name) FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`},
		{name: "UnaliasedFuncOverBareColumn",
			sql: `SELECT UPPER(s_name) FROM supplier ORDER BY s_suppkey LIMIT 2`},
		{name: "UnaliasedFuncSeveralArgs",
			sql: `SELECT COALESCE(supplier.s_name, 'q') FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`},
		// Two unaliased calls in one list, so the names are compared where
		// PostgreSQL happens to give them DIFFERENT labels. The declared type
		// of the second is a divergence of its own, found by this entry.
		{name: "UnaliasedFuncTwice",
			sql: `SELECT UPPER(s_name), LENGTH(s_name) FROM supplier ORDER BY s_suppkey LIMIT 2`},
		// The two shapes the same change deliberately does NOT touch, pinned
		// so the remaining divergence is recorded rather than forgotten.
		{name: "UnaliasedArithmetic",
			sql: `SELECT supplier.s_acctbal + 1 FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`,
			pins: map[string]string{
				wirePropFieldNames: "#513, not fixed: PostgreSQL labels an operator expression with " +
					"no natural name `?column?`; wadjet uses the expression's own text " +
					"(`supplier.s_acctbal + 1`). Both arms of this engine now agree with each other, " +
					"which they did not before — the single-process path answered `s_acctbal + 1` and " +
					"the stage DAG the full text. Adopting `?column?` is a separate change: it names " +
					"every such column the same thing, which the result-row map cannot represent",
			}},
		// --- DUPLICATE output names, values compared cell by cell ----------
		//
		// PostgreSQL answers `SELECT abs(a), abs(b)` with two columns both
		// called `abs`, and #513 made this engine agree. The DataRow path
		// then looked each cell up in a name-keyed map, so the second column
		// was sent carrying the FIRST column's value — 100|100 where
		// PostgreSQL says 100|200. This arm is where that is caught: it
		// compares cells POSITIONALLY, which is the only comparison a
		// duplicate name leaves meaningful.
		// Text-returning functions, so the VALUE comparison is not masked by
		// the separate integer-declaration divergence (#530).
		{name: "DuplicateNameScalarFuncs",
			sql: `SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_nationkey LIMIT 6`},
		{name: "DuplicateNameCoalesce",
			sql: `SELECT COALESCE(n_name, 'q'), COALESCE(n_comment, 'q') FROM nation ORDER BY n_nationkey LIMIT 3`},
		// A computed column and a plain one that an ALIAS collides with, so
		// the duplicate is not two calls of the same function.
		{name: "DuplicateNameFuncAndAlias",
			sql: `SELECT UPPER(n_name), n_comment AS upper FROM nation ORDER BY n_nationkey LIMIT 6`},
		// The same shape with an ORDER BY term the SELECT list does not
		// carry, which is what puts the hidden-sort-key trim between the
		// projection and the client — the projection that used to do the
		// name-keyed copy.
		{name: "DuplicateNameUnderHiddenSortKey",
			sql: `SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_comment LIMIT 6`},
		// The integer spelling of the same shape, kept because it is the one
		// the issue was reported with. Its VALUES still gate; only the
		// declared type is pinned, and to a different defect.
		{name: "DuplicateNameIntegerFuncs",
			sql: `SELECT ABS(n_nationkey), ABS(n_regionkey) FROM nation ORDER BY n_nationkey LIMIT 6`,
			pins: map[string]string{
				wirePropTypeOIDs: "#530: ABS over an integer is int4 in PostgreSQL and wadjet declares " +
					"float8, because the expr layer computes it as a float64 — the same defect LENGTH " +
					"has, and nothing to do with the duplicate NAME this entry is here for",
				wirePropTypeSizes: "#530, follows the OID: 8 for float8 where PostgreSQL declares 4",
			}},
		// CAST's label, pinned rather than changed: PostgreSQL names a cast
		// after its ARGUMENT (`n_nationkey`), and only after the target type
		// when the argument has no name of its own.
		{name: "UnaliasedCast",
			sql: `SELECT CAST(n_nationkey AS BIGINT) FROM nation ORDER BY n_nationkey LIMIT 2`,
			pins: map[string]string{
				wirePropFieldNames: "#513, deliberately out of scope: PostgreSQL labels `CAST(x AS t)` " +
					"after the ARGUMENT (`n_nationkey`) and only after the TYPE when the argument is " +
					"itself computed. Wadjet uses the expression text (`cast(n_nationkey as bigint)`). " +
					"Unlike the function case this is not a mangled fragment, and getting it right " +
					"means implementing FigureColname's recursion, not a one-line rule",
			}},

		// --- BYTES is bytea (#570) -----------------------------------------
		//
		// The coverage hole this family closes: `bytea` appeared NOWHERE in
		// benchmarks/ or internal/oracle/ before it, so this arm had never
		// compared the OID a BYTES column is declared under, the text body
		// beneath it, or the binary one. It was declaring OID 25 (text) and
		// sending Go's %v of the byte slice ("[255 254 0 65]") in BOTH
		// formats — right values nowhere, and invisible to a value oracle
		// because pgx decoded the text bytes into something.
		//
		// The rows are chosen for what a rendering breaks on: the empty
		// value (`\x`, a ZERO length, not NULL's negative one), four NULs,
		// the invalid-UTF-8-with-embedded-NUL value from the issue, and
		// ASCII.
		{name: "ByteaColumn", sql: `SELECT b_key, b_val FROM bytea_probe WHERE b_key IN (0, 1, 2, 3) ORDER BY b_key`},
		// NULL against the EMPTY value in one result, which is the pair a
		// wrong NULL representation collapses.
		{name: "ByteaNullAndEmpty", sql: `SELECT b_key, b_val FROM bytea_probe WHERE b_key IN (0, 10) ORDER BY b_key`},
		// A zero-row bytea result: RowDescription is sent before any DataRow,
		// so the declaration is exactly as comparable here — and it comes off
		// the PLAN-declared schema rather than a batch, which is the path
		// #458 found a different type's declaration missing from.
		{name: "ByteaColumnZeroRows", sql: `SELECT b_val FROM bytea_probe WHERE b_key = -1`},
		// bytea as an ORDER BY key, so the sort runs on the wire arm too.
		{name: "ByteaOrderBy", sql: `SELECT b_key, b_val FROM bytea_probe ORDER BY b_val, b_key LIMIT 4`},
		// A predicate over bytea: an unknown-typed literal beside a bytea
		// column is coerced by byteain on both engines.
		{name: "ByteaEquality", sql: `SELECT b_key FROM bytea_probe WHERE b_val = 'hi'`},
		// `bytea::text` — the second half of #570, and the one whose old
		// answer put an embedded NUL inside a text-format DataRow field.
		{name: "ByteaCastText", sql: `SELECT b_key, CAST(b_val AS text) AS s FROM bytea_probe WHERE b_key IN (0, 2, 3) ORDER BY b_key`},
		// OCTET_LENGTH over bytea: the value agrees, the declared type does
		// not, and for the reason #530 already records for LENGTH and ABS.
		{name: "ByteaOctetLength", sql: `SELECT b_key, OCTET_LENGTH(b_val) AS n FROM bytea_probe WHERE b_key IN (0, 2, 3) ORDER BY b_key`},
		// A bytea PARAMETER, declared and inferred. The parameter format is
		// TEXT here (readOne sends every parameter as text), so these also
		// cover byteain's escape spelling on the way in — the shape that
		// used to bind the SPELLING of the bytes and match nothing.
		{name: "ByteaParamDeclared", sql: `SELECT b_key FROM bytea_probe WHERE b_val = $1`,
			paramOIDs: []uint32{17}, params: [][]byte{[]byte("hi")}},
		{name: "ByteaParamHexSpelling", sql: `SELECT b_key FROM bytea_probe WHERE b_val = $1`,
			paramOIDs: []uint32{17}, params: [][]byte{[]byte(`\x6869`)}},
		// A DERIVED bytea value. PostgreSQL has `bytea || bytea` and it
		// returns bytea; wadjet's expr layer has no BYTES return type, so
		// every scalar function reads the value through toString and the
		// result is TEXT. Pinned per property so the row COUNT, the field
		// names and the format handling of the same statement stay gated.
		{name: "ByteaConcat", sql: `SELECT b_key, b_val || b_other AS c FROM bytea_probe WHERE b_key IN (2, 3) ORDER BY b_key`,
			pins: map[string]string{
				wirePropTypeOIDs: "#583: `bytea || bytea` is bytea (OID 17) in PostgreSQL and wadjet " +
					"declares text (25), because expr has no BYTES return type — every scalar function " +
					"reads its operand through toString",
				wirePropValuesText: "#583, the same cause seen as bytes: PostgreSQL renders the derived " +
					"bytea as \\x hex and wadjet sends the RAW bytes under OID 25 — which puts #570's " +
					"own hazard back in through a derived value, since those bytes are invalid UTF-8 " +
					"and hold an embedded NUL that libpq truncates at",
			}},
		// --- The SHAPE of a grouped answer (#591) ---------------------------
		//
		// A HAVING over an aggregate the SELECT list does not carry adds a
		// synthetic `__having_N` column to the aggregate's output, and the
		// projection above it used to be elided as redundant — so the column
		// reached RowDescription and psql drew it. The semantics arm cannot
		// see this: it compares cells POSITIONALLY and the extra column is
		// last. Field COUNT and field NAMES are exactly what this arm
		// compares, which makes these entries the gate for the leak.
		{name: "HavingBareAggregateOneColumn",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey
				HAVING BOOL_OR(n_nationkey > 5) ORDER BY n_regionkey`},
		{name: "HavingComparisonOneColumn",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey
				HAVING MAX(n_nationkey) > 5 ORDER BY n_regionkey`},
		{name: "HavingNullCheckOneColumn",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey
				HAVING MAX(n_nationkey) IS NOT NULL ORDER BY n_regionkey`},
		// Two selected columns and a THIRD aggregate only HAVING mentions.
		{name: "HavingUnselectedAggregateTwoColumns",
			sql: `SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey
				HAVING MAX(n_nationkey) > 5 ORDER BY n_regionkey`},
		// The aggregate HAVING names IS the selected one, so nothing
		// synthetic should be created at all — the control for the entries
		// above, and the gate on the reuse that avoids computing it twice.
		{name: "HavingReusesSelectedAggregate",
			sql: `SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey
				HAVING COUNT(*) > 1 ORDER BY n_regionkey`},
		// A grouping key the SELECT list does not ask for is the same leak
		// through a different door: the aggregate emits it, and only the
		// projection can trim it.
		{name: "GroupedKeyNotSelected",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey, n_nationkey
				ORDER BY n_regionkey, n_nationkey`},
		{name: "GroupedNoKeySelected",
			sql: `SELECT COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY c, n_regionkey`},

		// ROW FIELD PATHS on the wire (#568). A field path was declared
		// STRING all the way down, so RowDescription carried OID 25 for a
		// bigint field and a driver bound it as text — the half of the defect
		// only this arm can see, since a value oracle compares the VALUE
		// under whatever OID it arrives with.
		//
		// row_probe is the composite fixture (postgres_oracle_test.go); the
		// pgSQL spelling is the parentheses PostgreSQL requires.
		{name: "RowFieldTypes",
			sql:   `SELECT rw.a AS a, rw.b AS b, rw.f AS f FROM row_probe WHERE k IN (0, 2, 4) ORDER BY k`,
			pgSQL: `SELECT (rw).a AS a, (rw).b AS b, (rw).f AS f FROM row_probe WHERE k IN (0, 2, 4) ORDER BY k`},
		// The zero-row form, where the OID comes from the PLAN rather than
		// from a batch (declaredOutputSchema, #416) — a separate resolution
		// that was STRING for every field path.
		{name: "RowFieldTypesZeroRows",
			sql:   `SELECT rw.a AS a, rw.b AS b, rw.f AS f FROM row_probe WHERE k = -1`,
			pgSQL: `SELECT (rw).a AS a, (rw).b AS b, (rw).f AS f FROM row_probe WHERE k = -1`},
		// An aggregate over a field path: the output OID follows the field's
		// type, and the query could not run at all before the fix.
		{name: "MinOverRowField",
			sql:   `SELECT MIN(rw.b) AS lo, MAX(rw.a) AS hi FROM row_probe`,
			pgSQL: `SELECT MIN((rw).b) AS lo, MAX((rw).a) AS hi FROM row_probe`},
		{name: "UnaliasedAggregate",
			sql: `SELECT COUNT(supplier.s_name) FROM supplier`,
			pins: map[string]string{
				wirePropFieldNames: "#513, deliberately out of scope: PostgreSQL labels an aggregate " +
					"call `count`, wadjet `count(supplier.s_name)`. Unlike the scalar case this is not " +
					"a mangled name, and an aggregate's output name is load-bearing inside the planner " +
					"(it IS the Aggregate node's OutputCol, which GROUP BY, HAVING and ORDER BY resolve " +
					"against), so renaming it is its own change",
			}},
	}
	return append(base, decimalTPCHWireCorpus()...)
}

// decimalTPCHWireCorpus is the 22 TPC-H queries as wire cases, and it is
// EMPTY unless TPCH_DECIMAL=1 (ADR-0024). Under the FLOAT64 schema they would
// say nothing this arm does not already know: every monetary column is float8
// on both sides. Under the DECIMAL(15,2) schema each one declares numeric
// columns, and PostgreSQL — not a table in this repo — is the authority on
// the OID and the typmod each of them carries:
//
//   - a BARE column reference (Q02's s_acctbal, Q10's c_acctbal, Q18's
//     o_totalprice) keeps its column's typmod, numeric(15,2);
//   - everything computed — every SUM, AVG, product and quotient — is
//     unconstrained numeric, typmod -1.
//
// That is ADR-0024 item 5's rule applied to a real corpus, checked against
// the engine that defines it, on the WIRE rather than in ColumnMeta.
func decimalTPCHWireCorpus() []wireCase {
	if FixtureFromEnv() != DecimalFixture {
		return nil
	}
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	out := make([]wireCase, 0, len(nums))
	for _, n := range nums {
		c := wireCase{name: fmt.Sprintf("DecimalTPCH_Q%02d", n), sql: TPCHQueries[n].SQL}
		if p := decimalWirePins(n); len(p) > 0 {
			c.pins = p
		}
		out = append(out, c)
	}
	return out
}

// decimalWirePins records the wire properties these queries diverge on today.
// Each names its issue or the record that makes it deliberate, and a pinned
// property that starts agreeing FAILS.
func decimalWirePins(n int) map[string]string {
	const digitsKept = "DELIBERATE: PostgreSQL's numeric is unbounded, so AVG and `/` keep every digit; " +
		"wadjet's finite carrier keeps the (p,s) ADR-0024 item 3 computes — DECIMAL(38,6) here. Both are " +
		"exact to the digits they keep and agree to min(scale). ADR-0012 item 9's accepted divergence."
	const scalarSubquery = "#696 — a DECIMAL column compared against a SCALAR SUBQUERY's value selects " +
		"the wrong rows, so this group's count is inflated. The VALUE of the subquery is right; the " +
		"substitution into the comparison is not."
	switch n {
	case 1:
		// avg_price and avg_disc only: the three SUMs agree digit for digit.
		return map[string]string{wirePropFloatRender: digitsKept}
	case 17:
		// SUM(l_extendedprice) / 7.0. The quotient terminates inside
		// wadjet's six fraction digits at SF0.01 and repeats past it — see
		// decimalTierPastSF001 — so the divergence is real but only visible
		// on the larger tier.
		if decimalTierPastSF001() {
			return map[string]string{wirePropFloatRender: digitsKept}
		}
		return nil
	case 9:
		// sum_profit is float8 on BOTH sides — l_quantity stays FLOAT64 in
		// the decimal fixture, and a float8 operand takes the whole
		// expression to float8 in either engine. What differs is the last
		// ULP of a float summation, which is accumulation order and not an
		// answer.
		return map[string]string{
			wirePropFloatRender: "DELIBERATE: float64 summation order. sum_profit is float8 in both " +
				"engines (l_quantity is the FLOAT64 column the decimal fixture keeps), and two correct " +
				"engines adding the same values in different orders differ in the last ULP.",
		}
	case 12:
		// SUM over an INTEGER CASE. Not a decimal defect at all — it is the
		// standing SUM(int4) divergence, surfaced here because no wire case
		// carried this shape before. The SIZE agrees (float8 and int8 are
		// both 8 bytes), so only the OID is pinned.
		return map[string]string{wirePropTypeOIDs: noExactNumericPin}
	// Q02 and Q18 were pinned for #697 — a subquery anywhere in the statement
	// dropped the typmod of every BARE DECIMAL output column, so PostgreSQL
	// sent numeric(15,2) and wadjet -1. Q08 and Q14 were pinned for #695, whose
	// face depended on the tier: at SF0.01 Q08 answered under a FLOAT64
	// declaration, and Q14's decimal branch fired so the statement could not be
	// described at all. Every OID, size, modifier and field count agrees now
	// and all four are gated again.
	case 8:
		// What is left on Q08 is the standing one-scale-per-column RENDERING,
		// and it became visible only because brazil_revenue is finally a
		// numeric column instead of a float8 one: its CASE takes no
		// decimal-branch row at SF0.01, so PostgreSQL's per-VALUE dscale
		// prints the sum as "0" while a DECIMAL(38,4) column prints every row
		// at its own scale. Same number, and the OID and modifier beside it
		// are compared and agree.
		return map[string]string{wirePropFloatRender: choiceDecimalDigitsPin}
	case 22:
		return map[string]string{
			wirePropValuesText:   scalarSubquery,
			wirePropBinaryDecode: scalarSubquery,
		}
	}
	return nil
}

// runWireErrors compares the SQLSTATE a client is handed for statements that
// cannot succeed. A client branches on this code: 42P01 sends it to look up a
// table name, 22012 to report a data error, 57014 to say "cancelled" rather
// than "your SQL is broken".
func runWireErrors(t *testing.T, ctx context.Context, wConn, pConn *pgconn.PgConn) {
	// The SQLSTATE granularity defect, shared by every entry that reaches an
	// error at all. 42000 is a CLASS, not a code.
	const sqlstateClassPin = "WADJET BUG (pgwire): every failure is reported as SQLSTATE 42000. That is the " +
		"CLASS \"syntax error or access rule violation\", not a code, and a client branches on the code: " +
		"42P01 sends it to re-resolve a table name, 42703 to re-resolve a column, 42883 to look for a " +
		"function, 22012 to report a data error. Under one blanket code an ORM cannot tell a typo'd column " +
		"from a broken connection, and 57014 — the one that means 'you cancelled this' — is in the same " +
		"blanket. (#366)"
	// The other half, which is not about the code at all: PostgreSQL REFUSES
	// these and Wadjet answers them.
	const missingValidationPin = "WADJET BUG: PostgreSQL refuses this statement and Wadjet ANSWERS it. A " +
		"silent answer to a statement the standard calls invalid is worse than a wrong code: the client " +
		"gets rows it will treat as the truth. (#367)"

	cases := []struct {
		name string
		sql  string
		// pgSQL is the PostgreSQL-dialect spelling of the same refusal, for
		// the entries the two engines write differently — the one use today
		// is ROW field access, where PostgreSQL needs `(rw).f` and bare
		// `rw.f` is read as table.column (see pgCase.pgSQL). Empty means the
		// engines share the spelling.
		pgSQL string
		pin   string
	}{
		{name: "UndefinedTable", sql: `SELECT * FROM no_such_table_here`},
		{name: "UndefinedColumn", sql: `SELECT no_such_column FROM nation`},
		// #604: a field path naming no field of a ROW column. wadjet spells it
		// `rw.nosuch`; PostgreSQL raises 42703 ("could not identify column
		// ... in record data type") only for the parenthesised `(rw).nosuch`,
		// because bare `rw.nosuch` is read as table.column and is 42P01. Both
		// must be undefined_column (42703), not a silent column of NULLs.
		{name: "UndefinedRowField",
			sql:   `SELECT rw.nosuch FROM row_probe`,
			pgSQL: `SELECT (rw).nosuch FROM row_probe`},
		{name: "SyntaxError", sql: `SELECT FROM WHERE`},
		{name: "DivisionByZero", sql: `SELECT 1/0`},
		{name: "InvalidTextRepresentation", sql: `SELECT CAST('abc' AS integer)`},
		// ADR-0024 item 4 at the DECIMAL arithmetic and CAST sites: a value
		// with no carrier at its declared type is 22003 and a zero divisor is
		// 22012, and the two must stay APART — a caller that branched on "did
		// it produce a value" would report a numeric overflow for `x / 0`.
		// All three used to answer a number: the arithmetic in float64, the
		// cast by passing its operand through untouched (#555, #668).
		{name: "DecimalDivisionByZero", sql: `SELECT d_2 / 0 FROM dec_probe`},
		{name: "DecimalModuloByZero", sql: `SELECT d_2 % 0 FROM dec_probe`},
		{name: "DecimalCastPastPrecision", sql: `SELECT CAST(d_2 AS numeric(3,2)) FROM dec_probe`},
		{name: "DecimalCastFromNonNumericText", sql: `SELECT CAST('abc' AS numeric(9,2))`},
		// PostgreSQL has no boolean-to-numeric cast: the TYPE PAIR is wrong,
		// not the text, so it is 42846 cannot_coerce and not 22P02. The BARE
		// spelling is the one that answered 1 — a bare destination declines to
		// name a type before the refusal is reached (#555 review, N3) — so it
		// is the one gated here, with the parameterized twin beside it.
		{name: "DecimalCastFromBoolean", sql: `SELECT CAST(TRUE AS numeric)`},
		{name: "DecimalCastFromBooleanTyped", sql: `SELECT CAST(TRUE AS numeric(9,2))`},
		// A well-formed number too WIDE for the carrier is a RANGE condition,
		// 22003 — not the 22P02 that sends a client hunting a typo in a number
		// it read correctly (#555 review, S1).
		{name: "DecimalCastFromOverWideText", sql: `SELECT CAST('1e40' AS numeric(38,0))`},
		// A DECIMAL past SMALLINT's range: `smallint out of range`, 22003.
		{name: "SmallintOutOfRange", sql: `SELECT CAST('40000' AS numeric(9,0))::smallint`},
		// An integer result with no int64 (#637). PostgreSQL refuses it as
		// `bigint out of range`; wadjet WRAPPED, which is a different number
		// wearing the right type.
		{name: "BigintOutOfRange", sql: `SELECT 9223372036854775807 + n_nationkey FROM nation`},
		// A constant that is not a number, reaching a DECIMAL column. Both
		// engines must REFUSE it: wadjet used to read it as the value zero and
		// answer the rows holding zero (#463), which is worse than a wrong
		// code because the client treats the rows as the truth.
		{name: "DecimalNonNumericConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = 'abc'`},
		// The same constant where NO ROW reaches the comparison, and where
		// which pair gets compared depends on the DATA. PostgreSQL resolves
		// the literal's type from the column's declaration at parse time, so
		// all four are refused there whatever the rows do; wadjet refused
		// them from inside the comparison until #517, so an empty row set
		// answered zero rows and GREATEST refused where LEAST answered — on
		// the same three arguments.
		{name: "DecimalNonNumericConstantNoRowSurvives",
			sql: `SELECT COUNT(*) FROM dec_probe WHERE d_key > 100000 AND d_2 = 'abc'`},
		{name: "DecimalNonNumericConstantShortCircuited",
			sql: `SELECT COUNT(*) FROM dec_probe WHERE 1 = 0 AND d_2 = 'abc'`},
		{name: "DecimalNonNumericConstantGreatest",
			sql: `SELECT COUNT(*) FROM dec_probe WHERE GREATEST(d_key, 'abc', d_2) = 'abc'`},
		{name: "DecimalNonNumericConstantLeast",
			sql: `SELECT COUNT(*) FROM dec_probe WHERE LEAST(d_key, 'abc', d_2) = 'abc'`},
		{name: "DecimalNonNumericConstantIsDistinctFrom",
			sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 IS DISTINCT FROM 'abc'`},
		// #534's boundary, on the arm where an ERROR is the assertion. The
		// accept-set widened by exactly the NaN/±Infinity spellings
		// PostgreSQL's numeric input takes: a SIGNED NaN and every partial
		// spelling of an infinity are 22P02 in PostgreSQL too (verified live
		// on postgres:17-alpine), so they must stay 22P02 here. If the
		// widening over-fired, these would ANSWER on wadjet and RAISE on
		// PostgreSQL, which is exactly what this arm reports and the value
		// arm cannot.
		{name: "DecimalSignedNaNConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = '+NaN'`},
		{name: "DecimalNegatedNaNConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = '-NaN'`},
		{name: "DecimalPartialInfinityConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = 'Infin'`},
		{name: "DecimalOverlongInfinityConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = 'infinityy'`},
		{name: "DecimalTrailingJunkNaNConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = 'NaN0'`},
		{name: "DecimalSpacedSignInfinityConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = '- inf'`},
		// The same rule one type family over (#536, closed): an integer
		// column used to read an unparseable constant as the value ZERO and
		// answer the rows holding zero — #463's failure mode on the family
		// #463 never covered. Both comparison paths now read the integer
		// input grammar (kernel.Int64FilterConst/Int32FilterConst) and raise
		// 22P02 for a literal that names no integer, so this AGREES now.
		// #647: the same accept-set on the INGEST side, where the answer is a
		// STORED VALUE rather than a row set. A DECIMAL literal wider than the
		// column used to WRAP the int64 the writer scaled it through
		// (99999999999999999999.99 into a DECIMAL(9,2) stored
		// -92233720368547758.08), unparseable text stored 0, and NaN and the
		// infinities stored 0 as well — all with no error, so the client's
		// next SELECT read a number nobody wrote.
		//
		// Every entry here is a statement PostgreSQL REFUSES, so neither
		// engine's fixture is mutated by running it. The value/rounding half
		// of the same rule (' 3.50 ' -> 3.50, 1.239 -> 1.24, 5 -> 5.00) is
		// gated by wadjet.TestInsertDecimalLiteralFollowsPostgres, because
		// this corpus has no per-entry setup and cannot compare a statement
		// that SUCCEEDS on the shared fixture.
		{name: "DecimalInsertPastDeclaredPrecision",
			sql: `INSERT INTO dec_probe (d_key, d_grp, d_2) VALUES (900001, 1, 99999999999999999999.99)`},
		{name: "DecimalInsertExponentPastDeclaredPrecision",
			sql: `INSERT INTO dec_probe (d_key, d_grp, d_2) VALUES (900002, 1, 1e40)`},
		{name: "DecimalInsertRoundsIntoOverflow",
			sql: `INSERT INTO dec_probe (d_key, d_grp, d_2) VALUES (900003, 1, 9999999.999)`},
		{name: "DecimalInsertNonNumericText",
			sql: `INSERT INTO dec_probe (d_key, d_grp, d_2) VALUES (900004, 1, 'abc')`},
		{name: "DecimalInsertInfinity",
			sql: `INSERT INTO dec_probe (d_key, d_grp, d_2) VALUES (900005, 1, 'Infinity')`},
		{name: "DecimalInsertWidePastDeclaredPrecision",
			sql: `INSERT INTO dec_probe (d_key, d_grp, d_wide) VALUES (900006, 1, 123456789012345678901234567890.1234567891)`},
		{name: "IntegerNonNumericConstant", sql: `SELECT COUNT(*) FROM nation WHERE n_nationkey = 'abc'`},
		// #536 review: the SQLSTATE for an integer literal that OVERFLOWS the
		// column type is 22003 (numeric_value_out_of_range), a DIFFERENT code
		// from the 22P02 a non-numeric literal earns — PostgreSQL distinguishes
		// them and so must both wadjet comparison paths. d_key is bigint,
		// d_grp is integer, so these cover both widths.
		{name: "BigintOutOfRangeConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_key = '99999999999999999999999'`},
		{name: "IntegerOutOfRangeConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_grp = '3000000000'`},
		// #536 review: the integer input trims only PostgreSQL's whitespace
		// (ASCII), never Unicode — a NBSP (U+00A0) before the digits is a
		// non-whitespace byte PostgreSQL rejects with 22P02. strings.TrimSpace
		// would have stripped it and accepted the value.
		{name: "IntegerNBSPConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_key = ' 42'`},
		// The DECIMAL twin of the entry above (#534 review, R1). The numeric
		// reader trimmed the UNICODE space set (strings.TrimSpace), so a
		// NBSP-prefixed constant resolved to the number and ANSWERED the row
		// PostgreSQL refuses the query for — the integer types had this pinned
		// and `numeric` did not. PostgreSQL's numeric input skips C isspace()
		// and nothing else, exactly as pg_strtoint* does. The literals here are
		// U+00A0 followed by a value dec_probe actually holds, and by a NaN, so
		// a reader that strips it answers rows rather than raising.
		{name: "DecimalNBSPConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 = ' 12.75'`},
		{name: "DecimalNBSPNaNConstant", sql: `SELECT COUNT(*) FROM dec_probe WHERE d_2 < ' NaN'`},
		// The same rule for a DATE column: an unparseable or nonexistent
		// calendar date reaching a DATE comparison must be REFUSED, not read
		// as the epoch (1970-01-01) and answered as the rows holding it.
		// PostgreSQL splits the two — 22008 (datetime_field_overflow) for a
		// well-formed but nonexistent date, 22007 (invalid_datetime_format)
		// for a string that is not a date at all — and a value oracle cannot
		// see the difference between "raised" and "returned the epoch", which
		// is the whole point of gating it here (#560). TPC-H's o_orderdate is
		// a string, not a DATE, so these ask the multikey fixture's mk_outer.dt
		// column, the one real DATE seeded into both engines.
		{name: "DateNonexistentCalendarConstant",
			sql: `SELECT COUNT(*) FROM mk_outer WHERE dt = '2026-02-30'`},
		{name: "DateMonthOutOfRangeConstant",
			sql: `SELECT COUNT(*) FROM mk_outer WHERE dt = '2026-13-01'`},
		{name: "DateUnparseableConstant",
			sql: `SELECT COUNT(*) FROM mk_outer WHERE dt = 'not-a-date'`},
		{name: "DateNonexistentCalendarConstantIn",
			sql: `SELECT COUNT(*) FROM mk_outer WHERE dt IN ('2026-02-30')`},
		// #560 fixed the DATE literal reaching a COMPARISON. The literal
		// reaching a STORED VALUE was a separate path and stayed broken: the
		// SQL INSERT converter took only "2006-01-02", failed with a bare
		// error carrying no SQLSTATE, and — for the spellings it did take —
		// handed the writer a time.Time box no writer arm converted, so
		// `INSERT INTO t VALUES (1, '2020-01-01')` stored the EPOCH while
		// ingest.Ingester with the same text stored the date (#673). Both
		// entries are statements PostgreSQL REFUSES, so neither engine's
		// fixture is mutated by running them; the value half (which
		// spellings STORE, and as what) is gated by
		// wadjet.TestSQLInsertStoresEveryTemporalLiteral, because this corpus
		// has no per-entry setup and cannot compare a statement that succeeds.
		{name: "DateInsertNonexistentCalendar",
			sql: `INSERT INTO mk_outer (id, dt) VALUES (900001, '2026-02-30')`},
		{name: "DateInsertUnparseable",
			sql: `INSERT INTO mk_outer (id, dt) VALUES (900002, 'not-a-date')`},
		// A DML WHERE whose evaluation cannot answer. PostgreSQL raises before
		// touching a row; wadjet must too, and must do it as an ERROR on the
		// wire rather than as a dead connection — the DML predicate closure
		// called Eval with no recover, so over HTTP the client got a transport
		// EOF and a goroutine dump (#677). These are the pgwire door's half of
		// that gate, and neither engine's fixture is mutated because neither
		// engine reaches a row.
		{name: "DeleteWhereDivisionByZero",
			sql: `DELETE FROM mk_outer WHERE 1/0 = 1`},
		{name: "DeleteWhereInvalidCast",
			sql: `DELETE FROM mk_outer WHERE id = CAST('abc' AS integer)`},
		{name: "UpdateWhereDivisionByZero",
			sql: `UPDATE mk_outer SET n = 1 WHERE 1/0 = 1`},
		{name: "UpdateWhereInvalidCast",
			sql: `UPDATE mk_outer SET n = 1 WHERE id = CAST('abc' AS integer)`},
		// A DML statement naming a column that does not exist. The DML doors do
		// not go through the planner, so they had no name-resolution step at
		// all: `SET nosuchcol = 1` answered "UPDATE 1" with the assignment
		// silently dropped, and `WHERE nosuchcol = 1` answered "UPDATE 0"
		// because the reference evaluated to NULL on every row (#678, the #653
		// class one door over). Both are 42703 here. Again neither engine
		// reaches a row, so neither fixture is mutated.
		{name: "UpdateUnknownSetColumn",
			sql: `UPDATE mk_outer SET nosuchcol = 1`},
		{name: "UpdateUnknownWhereColumn",
			sql: `UPDATE mk_outer SET n = 1 WHERE nosuchcol = 1`},
		{name: "DeleteUnknownWhereColumn",
			sql: `DELETE FROM mk_outer WHERE nosuchcol = 1`},
		// An ALIASED DML target. The alias token used to END the statement, so
		// `DELETE FROM t AS a WHERE ...` reached the executor with an EMPTY
		// WHERE — which means "every row" — and reported DELETE <all> as a
		// success (#686). Only the refusals can live in this corpus, since it
		// has no per-entry setup and a successful DELETE would mutate the
		// fixture for every entry after it; the counts and the row sets are
		// gated by wadjet.TestDMLTableAliasMatchesPostgres and
		// pgwire.TestAliasedDMLCommandTagOnTheWire.
		//
		// The first four are the alias-HIDES-the-table rule and the name
		// resolution that rides on it; the last two are statement tails this
		// parser cannot read, which used to be dropped on the floor together
		// with the WHERE that followed them.
		{name: "DeleteAliasedTableQualifiedWhere",
			sql: `DELETE FROM mk_outer AS a WHERE mk_outer.id = 1`},
		{name: "UpdateAliasedTableQualifiedWhere",
			sql: `UPDATE mk_outer AS a SET n = 1 WHERE mk_outer.id = 1`},
		{name: "DeleteAliasedUnknownQualifier",
			sql: `DELETE FROM mk_outer AS a WHERE b.id = 1`},
		{name: "DeleteAliasedUnknownColumn",
			sql: `DELETE FROM mk_outer AS a WHERE a.nosuchcol = 1`},
		{name: "DeleteEmptyWhere",
			sql: `DELETE FROM mk_outer AS a WHERE`},
		{name: "DeleteUnreadableStatementTail",
			sql: `DELETE FROM mk_outer x y`},
		// A short leading field PostgreSQL reads as a MONTH under MDY: '31/1/2'
		// and '13/1/2' are month 31 and month 13, which PostgreSQL rejects
		// with 22008. wadjet refuses the SHAPE (a non-4-digit leading field is
		// not an unambiguous year) with 22007 rather than guess year-first and
		// risk a value that differs from PostgreSQL's (#560). BOTH refuse, so
		// no silent divergence; the SQLSTATE differs only because wadjet has
		// not implemented DateStyle-ordered parsing (#639), which is what
		// would let it see these as an out-of-range MONTH.
		{name: "DateShortMDYLeadingMonth31", sql: `SELECT COUNT(*) FROM mk_outer WHERE dt = '31/1/2'`,
			pin: "WADJET: refuses a non-4-digit leading DATE field as malformed (22007) where PostgreSQL, " +
				"reading it MDY, refuses it as an out-of-range month (22008). Both refuse; the code differs " +
				"until wadjet implements DateStyle-ordered parsing (#639)."},
		{name: "DateShortMDYLeadingMonth13", sql: `SELECT COUNT(*) FROM mk_outer WHERE dt = '13/1/2'`,
			pin: "WADJET: refuses a non-4-digit leading DATE field as malformed (22007) where PostgreSQL, " +
				"reading it MDY, refuses it as an out-of-range month (22008). Both refuse; the code differs " +
				"until wadjet implements DateStyle-ordered parsing (#639)."},
		// A TEXT-only function over bytea. PostgreSQL has no upper(bytea) —
		// 42883, the same shape as min(boolean) — and wadjet answers,
		// because expr reads every operand through toString (#583).
		{name: "ByteaTextFunctionOverBytes", sql: `SELECT UPPER(b_val) FROM bytea_probe WHERE b_key = 3`,
			pin: missingValidationPin + " Specifically: UPPER reads the BYTES operand through " +
				"expr.toString and answers the text those bytes spell. (#583)"},
		// MIN/MAX over bytea, which PostgreSQL genuinely does not have
		// either — verified live, "function min(bytea) does not exist".
		// This one is a DELIBERATE extension rather than a defect, in the
		// same class as MIN/MAX over BOOL, and the entry exists so
		// ADR-0012 item 5's claim about it is checkable rather than
		// remembered.
		{name: "ByteaMinMax", sql: `SELECT MIN(b_val) FROM bytea_probe`,
			pin: "DELIBERATE (ADR-0012 item 5): PostgreSQL has no min(bytea)/max(bytea) at all " +
				"(verified live: \"function min(bytea) does not exist\"), exactly as it has no " +
				"min(boolean). wadjet supports both as an extension over a type whose order it " +
				"defines anyway — bytewise, which is what every bytea comparison uses. Not a " +
				"position against a PostgreSQL answer, because there is none"},
		{name: "UndefinedFunction", sql: `SELECT no_such_function_here(1)`},
		{name: "GroupByMissingColumn", sql: `SELECT n_name, COUNT(*) FROM nation`},
		// #367 fixed the no-GROUP-BY shape above. With a GROUP BY present
		// PostgreSQL's rule does not relax, it NARROWS — every non-aggregated
		// SELECT / HAVING / ORDER BY expression must be one of the grouped
		// ones — and wadjet did not check that case at all. What came out was
		// not a missing error but three silent wrong answers: the SELECT-list
		// column was replaced in place by the grouping key, and a HAVING over
		// a bare ungrouped column excluded EVERY group, so TLP's `p` /
		// `NOT p` / `p IS NULL` partition summed to zero rows (#590).
		{name: "GroupBySelectsUngroupedColumn",
			sql: `SELECT n_regionkey, n_name FROM nation GROUP BY n_regionkey`},
		{name: "GroupBySelectsUngroupedColumnBesideAggregate",
			sql: `SELECT COUNT(*), n_name FROM nation GROUP BY n_regionkey`},
		{name: "GroupBySelectsUngroupedColumnInExpression",
			sql: `SELECT n_regionkey, UPPER(n_name) FROM nation GROUP BY n_regionkey`},
		{name: "GroupByHavingUngroupedColumn",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey HAVING n_nationkey > 5`},
		{name: "GroupByHavingUngroupedColumnIsNull",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey HAVING n_name IS NULL`},
		{name: "GroupByHavingUngroupedColumnNegated",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey HAVING NOT (n_nationkey > 5)`},
		{name: "GroupByOrderByUngroupedColumn",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey ORDER BY n_name`},
		// A HAVING with no GROUP BY makes the whole table one group, so the
		// SELECT list is under the same rule.
		{name: "HavingWithoutGroupBySelectsUngroupedColumn",
			sql: `SELECT n_name FROM nation HAVING COUNT(*) > 1`},
		{name: "AmbiguousColumn", sql: `SELECT n_nationkey FROM nation a JOIN nation b ON a.n_nationkey = b.n_nationkey`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pgSQL := c.sql
			if c.pgSQL != "" {
				pgSQL = c.pgSQL
			}
			pState, pErr := execSQLState(ctx, pConn, pgSQL)
			if pErr == nil {
				t.Fatalf("the ORACLE accepted a statement this entry exists to see refused: %s", pgSQL)
			}
			wState, wErr := execSQLState(ctx, wConn, c.sql)
			if wErr == nil {
				res := wConn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
				detail := fmt.Sprintf("wadjet ACCEPTED the statement and answered %d row(s) %v; PostgreSQL refused it with SQLSTATE %s (%s)",
					len(res.Rows), firstWireRow(res), pState, sqlstateName(pState))
				if c.pin != "" {
					t.Logf("known divergence, NOT gated [%s]: %s\n  %s", wirePropSQLState, detail, c.pin)
					return
				}
				t.Errorf("wire divergence [%s]: %s\n  SQL: %s", wirePropSQLState, detail, c.sql)
				return
			}
			if wState == pState {
				if c.pin != "" {
					t.Errorf("wadjet now reports SQLSTATE %s like PostgreSQL, so this known divergence is FIXED:\n  %s\n"+
						"Delete the pin on %s in runWireErrors.", wState, c.pin, c.name)
				}
				return
			}
			detail := fmt.Sprintf("SQLSTATE %s (%s), PostgreSQL %s (%s)\n  wadjet: %v\n  postgres: %v",
				wState, sqlstateName(wState), pState, sqlstateName(pState), wErr, pErr)
			if c.pin != "" {
				t.Logf("known divergence, NOT gated [%s]: %s\n  %s", wirePropSQLState, detail, c.pin)
				return
			}
			t.Errorf("wire divergence [%s]: %s\n  SQL: %s", wirePropSQLState, detail, c.sql)
		})
	}
}

// firstWireRow renders a result's first row for a failure message, so "wadjet
// accepted it" says WHAT it answered.
func firstWireRow(res *pgconn.Result) []string {
	if len(res.Rows) == 0 {
		return nil
	}
	out := make([]string, len(res.Rows[0]))
	for i, cell := range res.Rows[0] {
		out[i] = describeCell(cell)
	}
	return out
}

// execSQLState runs sql and returns the SQLSTATE of the error it produced.
func execSQLState(ctx context.Context, conn *pgconn.PgConn, sql string) (string, error) {
	res := conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	if res.Err == nil {
		return "", nil
	}
	var pgErr *pgconn.PgError
	if errors.As(res.Err, &pgErr) {
		return pgErr.Code, res.Err
	}
	return "<not a PostgreSQL error>", res.Err
}

func sqlstateName(code string) string {
	switch code {
	case "":
		return "no error"
	case "22012":
		return "division_by_zero"
	case "22P02":
		return "invalid_text_representation"
	case "42601":
		return "syntax_error"
	case "42702":
		return "ambiguous_column"
	case "42703":
		return "undefined_column"
	case "42883":
		return "undefined_function"
	case "42P01":
		return "undefined_table"
	case "42803":
		return "grouping_error"
	case "42000":
		return "syntax_error_or_access_rule_violation, the CLASS not a code"
	case "57014":
		return "query_canceled"
	case "XX000":
		return "internal_error"
	default:
		return "unclassified"
	}
}

// runWireCommandTags compares the CommandComplete tag, which a client uses for
// its "n rows affected" report and, for some drivers, to decide whether a
// result set follows at all.
func runWireCommandTags(t *testing.T, ctx context.Context, wConn, pConn *pgconn.PgConn) {
	cases := []struct {
		name string
		sql  string
		pin  string
	}{
		{name: "SelectRows", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3`},
		{name: "SelectZeroRows", sql: `SELECT n_nationkey FROM nation WHERE n_nationkey < 0`},
		{name: "SelectOneRow", sql: `SELECT COUNT(*) FROM nation`},
		{name: "Begin", sql: `BEGIN`},
		{name: "Commit", sql: `COMMIT`},
		{name: "Set", sql: `SET extra_float_digits = 3`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pRes := pConn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
			if pRes.Err != nil {
				t.Fatalf("the ORACLE refused %q: %v", c.sql, pRes.Err)
			}
			wRes := wConn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
			if wRes.Err != nil {
				detail := fmt.Sprintf("wadjet refused a statement PostgreSQL tagged %q: %v", pRes.CommandTag.String(), wRes.Err)
				if c.pin != "" {
					t.Logf("known divergence, NOT gated [%s]: %s\n  %s", wirePropCommandTag, detail, c.pin)
					return
				}
				t.Errorf("wire divergence [%s]: %s\n  SQL: %s", wirePropCommandTag, detail, c.sql)
				return
			}
			got, want := wRes.CommandTag.String(), pRes.CommandTag.String()
			if got == want {
				if c.pin != "" {
					t.Errorf("wadjet now sends the tag %q like PostgreSQL, so this known divergence is FIXED:\n  %s\n"+
						"Delete the pin on %s in runWireCommandTags.", got, c.pin, c.name)
				}
				return
			}
			detail := fmt.Sprintf("CommandComplete %q, PostgreSQL %q", got, want)
			if c.pin != "" {
				t.Logf("known divergence, NOT gated [%s]: %s\n  %s", wirePropCommandTag, detail, c.pin)
				return
			}
			t.Errorf("wire divergence [%s]: %s\n  SQL: %s", wirePropCommandTag, detail, c.sql)
		})
	}
}

// runWireCancellation sends a CancelRequest mid-query on a dedicated connection
// per server and compares the SQLSTATE the cancelled statement reports.
//
// PostgreSQL answers 57014 (query_canceled), and a client shows "cancelled"
// only because of that code. Wadjet reported 42000 — the syntax-error class —
// so a cancelled query was indistinguishable from broken SQL. Nothing tested
// it, and no value oracle could: a cancelled query has no values.
func runWireCancellation(t *testing.T, ctx context.Context, wadjetDSN, pgDSN string) {
	// A query slow enough to still be running after the cancel is sent: a
	// keyless self-join whose predicate cannot become a hash key, so both
	// engines walk the cross product, with per-pair string work on top.
	//
	// Two properties it has to have, and both are about the BUGGY outcome
	// rather than the correct one:
	//
	//   - the o_orderkey bound caps the row set at 15,000 on every tier, so
	//     the work does not grow with the fixture. A server that stops for the
	//     cancel never notices; a server that does not runs the whole thing.
	//   - it must still TERMINATE on its own in tens of seconds, because
	//     pgwire's Shutdown waits on its connection handlers (see the cleanup
	//     in runPostgresWireArm). Unbounded, the abandoned statement at SF0.1
	//     held the suite for minutes after the finding was already reported.
	//
	// The test itself never waits for it to finish: the statement is given a
	// grace window after the cancel, and "still running" IS the finding.
	const slow = `SELECT COUNT(*) AS c FROM orders a, orders b
		WHERE a.o_orderkey <= 15000 AND b.o_orderkey <= 15000 AND a.o_orderkey < b.o_orderkey
		  AND LENGTH(a.o_comment || b.o_comment) > 0`

	// The cancel is sent this long after the statement starts. A statement that
	// finishes sooner says nothing either way and is skipped.
	const cancelAfter = 300 * time.Millisecond

	// How long a correct server may take to act on the cancel. PostgreSQL
	// answers within milliseconds; anything past this window has not acted.
	const graceAfterCancel = 20 * time.Second

	// No pins: both servers answer a cancelled statement with 57014. The Wadjet
	// pin (#368 — a CancelRequest was accepted and the statement ran 11 more
	// seconds to a normal completion) was deleted when the executor learned to
	// poll the statement context between output batches
	// (exec.ChainDriver.push); PostgreSQL stays unpinned on purpose: it is the
	// reference, and a reference that stopped answering 57014 would mean the
	// probe had stopped measuring cancellation at all.
	pins := map[string]string{}

	for _, s := range []struct{ name, dsn string }{{"PostgreSQL", pgDSN}, {"Wadjet", wadjetDSN}} {
		t.Run(s.name, func(t *testing.T) {
			runCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()

			conn, err := pgconn.Connect(runCtx, s.dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			// The close is DEADLINED, which matters only here: pgconn.Close
			// sends Terminate and waits for the server to close the socket, and
			// a server still grinding through the abandoned statement never
			// reads it. With context.Background() that wait is unbounded, so
			// the subtest that reports "the cancel did nothing" would then hang
			// on the same defect it just reported. The deadline makes the
			// context watcher drop the socket instead.
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				conn.Close(closeCtx)
			}()

			type outcome struct {
				state   string
				err     error
				elapsed time.Duration
			}
			done := make(chan outcome, 1)
			start := time.Now()
			go func() {
				state, err := execSQLState(runCtx, conn, slow)
				done <- outcome{state, err, time.Since(start)}
			}()

			time.Sleep(cancelAfter)
			if err := conn.CancelRequest(runCtx); err != nil {
				t.Fatalf("CancelRequest: %v", err)
			}
			sentAt := time.Since(start)

			report := func(format string, args ...any) {
				detail := fmt.Sprintf(format, args...)
				if pin, ok := pins[s.name]; ok {
					t.Logf("known divergence, NOT gated [%s]: %s\n  %s", wirePropSQLState, detail, pin)
					return
				}
				t.Errorf("wire divergence [%s]: %s\n  SQL: %s", wirePropSQLState, detail, slow)
			}

			var got outcome
			select {
			case got = <-done:
			case <-time.After(graceAfterCancel):
				// Neither an error nor a result within the grace window. The
				// server took the cancel and kept going, which is the finding —
				// waiting for the statement to end would only measure how long
				// a cross product takes at this tier.
				report("%s was still running the statement %s after a CancelRequest, which it accepted without "+
					"error. PostgreSQL aborts within milliseconds and answers 57014. The connection is dropped "+
					"here rather than waited on, because a cancel that does nothing has no end to wait for",
					s.name, graceAfterCancel)
				return
			case <-runCtx.Done():
				t.Fatalf("%s never answered the cancelled statement", s.name)
			}

			switch {
			case got.err == nil && got.elapsed < sentAt:
				t.Skipf("the statement finished in %s, before the cancel was sent at %s, so this run says "+
					"nothing about cancellation on %s", got.elapsed.Round(time.Millisecond),
					sentAt.Round(time.Millisecond), s.name)
			case got.err == nil:
				// Still running when the cancel arrived, and it finished
				// anyway. The request was received (CancelRequest returned no
				// error) and had no effect.
				report("%s ran the statement to COMPLETION in %s despite a CancelRequest sent at %s — the "+
					"cancel was accepted on the wire and did not stop the query. PostgreSQL aborts it and "+
					"answers 57014",
					s.name, got.elapsed.Round(time.Millisecond), sentAt.Round(time.Millisecond))
			case got.state == "57014":
				t.Logf("%s aborted the statement after %s and reported SQLSTATE 57014 (query_canceled), which "+
					"is the answer a client needs", s.name, got.elapsed.Round(time.Millisecond))
				if pin, pinned := pins[s.name]; pinned {
					t.Errorf("%s now answers a cancelled statement with 57014, so this known divergence is "+
						"FIXED:\n  %s\nDelete the pin on %s in runWireCancellation.", s.name, pin, s.name)
				}
			default:
				report("%s answered a cancelled statement with SQLSTATE %s (%s) after %s: %v. PostgreSQL "+
					"defines 57014 query_canceled, and a client tells 'you cancelled this' from 'your SQL is "+
					"wrong' by exactly that code",
					s.name, got.state, sqlstateName(got.state), got.elapsed.Round(time.Millisecond), got.err)
			}
		})
	}
}
