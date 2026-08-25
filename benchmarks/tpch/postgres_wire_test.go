package tpch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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
			pDesc, pErr := pConn.Prepare(ctx, "", c.sql, c.paramOIDs)
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
			wText := readOne(ctx, wConn, c, textFormats(len(pDesc.Fields)))
			pText := readOne(ctx, pConn, c, textFormats(len(pDesc.Fields)))
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
			wBin := readOne(ctx, wConn, c, binaryFormats(len(pDesc.Fields)))
			pBin := readOne(ctx, pConn, c, binaryFormats(len(pDesc.Fields)))
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

func readOne(ctx context.Context, conn *pgconn.PgConn, c wireCase, resultFormats []int16) *pgconn.Result {
	paramFormats := make([]int16, len(c.params)) // all text unless a case says otherwise
	return conn.ExecParams(ctx, c.sql, c.params, c.paramOIDs, paramFormats, resultFormats).Read()
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
func wireCorpus() []wireCase {
	return []wireCase{
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
		{name: "MinOverDecimalColumn", sql: `SELECT MIN(d_2) AS lo FROM dec_probe WHERE d_key IN (1, 2, 3)`},
		{name: "MinOverDecimalColumnZeroRows", sql: `SELECT MIN(d_2) AS lo FROM dec_probe WHERE d_key = -1`},
		{name: "SumOverDecimalColumn", sql: `SELECT SUM(d_2) AS s FROM dec_probe WHERE d_key IN (1, 2, 3)`},
		{name: "SumOverDecimalColumnZeroRows", sql: `SELECT SUM(d_2) AS s FROM dec_probe WHERE d_key = -1`},
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
				wirePropTypeMods: "WADJET BUG (#542): PostgreSQL declares a set operation's numeric " +
					"result unconstrained (typmod -1) unless every arm carries the SAME typmod; " +
					"wadjet declares a real (p,s). declaredWireUnconstrainedDecimal already does this " +
					"for the other shape PostgreSQL drops typmod on (an aggregate call) and needs the " +
					"set-operation case added",
				wirePropFloatRender: setOpDecimalDigitsPin,
			}},
		// The control for the entry above: BOTH arms are the same column, so
		// PostgreSQL KEEPS numeric(9,2) and wadjet agrees outright. It carries
		// no pin, which is what proves the pinned entry is about the arms
		// disagreeing and not about set operations in general.
		{name: "SetOpSameDecimalScale",
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (1, 2)
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key IN (2, 3) ORDER BY 1`},
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
			sql: `SELECT UPPER(s_name), LENGTH(s_name) FROM supplier ORDER BY s_suppkey LIMIT 2`,
			pins: map[string]string{
				wirePropTypeOIDs: "#530: LENGTH is int4 in PostgreSQL (OID 23) and wadjet declares " +
					"float8 (701) — honestly, because the expr layer COMPUTES it as a float64. " +
					"Declaring int4 over a float64 vector would read back NULL through the typed " +
					"getter, so the value and the declaration have to move together",
				wirePropTypeSizes: "#530, follows the OID: 8 for float8 where PostgreSQL declares 4 for int4",
			}},
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
		pin  string
	}{
		{name: "UndefinedTable", sql: `SELECT * FROM no_such_table_here`},
		{name: "UndefinedColumn", sql: `SELECT no_such_column FROM nation`},
		{name: "SyntaxError", sql: `SELECT FROM WHERE`},
		{name: "DivisionByZero", sql: `SELECT 1/0`},
		{name: "InvalidTextRepresentation", sql: `SELECT CAST('abc' AS integer)`},
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
		// The same rule one type family over, and NOT yet implemented: an
		// integer column reads an unparseable constant as the value ZERO and
		// answers the rows holding zero, which is #463's failure mode on the
		// family #463 never covered.
		{name: "IntegerNonNumericConstant", sql: `SELECT COUNT(*) FROM nation WHERE n_nationkey = 'abc'`,
			pin: missingValidationPin + " Specifically: the constant is read as the integer ZERO and " +
				"matches the rows holding 0. (#536)"},
		{name: "UndefinedFunction", sql: `SELECT no_such_function_here(1)`},
		{name: "GroupByMissingColumn", sql: `SELECT n_name, COUNT(*) FROM nation`},
		{name: "AmbiguousColumn", sql: `SELECT n_nationkey FROM nation a JOIN nation b ON a.n_nationkey = b.n_nationkey`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pState, pErr := execSQLState(ctx, pConn, c.sql)
			if pErr == nil {
				t.Fatalf("the ORACLE accepted a statement this entry exists to see refused: %s", c.sql)
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
