package tpch

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
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
	// paramFormats is the format code PER PARAMETER, as Bind carries it: 0
	// text, 1 binary. Nil means all text, which is what every entry sent
	// before #486 — readOne hard-coded a zero-filled slice, so pgwire's whole
	// binary parameter decoder (renderBinaryParam: fifteen OIDs of big-endian
	// integers, IEEE floats, 2000-epoch date/time, raw bytea and PostgreSQL's
	// base-10000 numeric) was reachable from no gate at all, while pgx sends
	// binary by DEFAULT.
	//
	// It is a per-parameter SLICE and not a per-case bool because Bind's is:
	// a driver that pre-encoded one value and left another as text sends
	// {1, 0}, and a server that reads one format code and applies it to every
	// parameter answers that Bind wrong. binaryFormats(n) is the all-binary
	// spelling.
	paramFormats []int16
	// minRows is the number of rows the ORACLE must return for this entry to
	// be worth comparing. A parameterized statement whose bind matches
	// nothing agrees with any other implementation that also matches nothing,
	// so a corpus of them would pass however the parameter was decoded. Every
	// binary-parameter entry declares it; PostgreSQL decides the number.
	minRows int
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
			if c.minRows > 0 && len(pText.Rows) < c.minRows {
				t.Fatalf("the ORACLE returned %d rows and this entry needs at least %d: "+
					"a bind that matches nothing agrees with every decoding of it, so the "+
					"cell would prove nothing\n  SQL: %s", len(pText.Rows), c.minRows, c.oracleSQL())
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
	paramFormats := c.paramFormats
	if paramFormats == nil {
		paramFormats = make([]int16, len(c.params)) // all text
	}
	return conn.ExecParams(ctx, sql, c.params, c.paramOIDs, paramFormats, resultFormats).Read()
}

// --- binary parameter encoders -------------------------------------------
//
// PostgreSQL's network representation, which is what a driver sends: big
// endian throughout, integers two's complement, floats IEEE 754, date and
// timestamp counted from 2000-01-01 UTC, bytea as its own bytes, uuid as its
// sixteen bytes, bool as one byte. These are the inputs pgwire's
// renderBinaryParam reads; writing them here rather than through a driver
// helper is deliberate — the bytes are the thing under test.

func int2Bin(v int16) []byte {
	return binary.BigEndian.AppendUint16(nil, uint16(v))
}

func int4Bin(v int32) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(v))
}

func int8Bin(v int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(v))
}

func float8Bin(v float64) []byte {
	return binary.BigEndian.AppendUint64(nil, math.Float64bits(v))
}

func float4Bin(v float32) []byte {
	return binary.BigEndian.AppendUint32(nil, math.Float32bits(v))
}

// timestampBin is MICROSECONDS from 2000-01-01 UTC, PostgreSQL's timestamp
// epoch — the conversion pgx performs for every time.Time it binds.
func timestampBin(y int, m time.Month, d, hh, mm, ss int) []byte {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	micros := time.Date(y, m, d, hh, mm, ss, 0, time.UTC).Sub(base).Microseconds()
	return int8Bin(micros)
}

// timeBin is microseconds since midnight.
func timeBin(hh, mm, ss int) []byte {
	return int8Bin(int64(hh)*3600e6 + int64(mm)*60e6 + int64(ss)*1e6)
}

func boolBin(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

// dateBin is the day count from 2000-01-01, PostgreSQL's date epoch.
func dateBin(y int, m time.Month, d int) []byte {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	days := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Sub(base) / (24 * time.Hour)
	return int4Bin(int32(days))
}

// uuidBin is a canonical UUID string as its sixteen bytes. It panics on a
// malformed literal: every caller is a constant in the corpus below, so a bad
// one is a typo to fix and not a condition to report.
func uuidBin(s string) []byte {
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		panic(fmt.Sprintf("bad uuid literal %q: %v", s, err))
	}
	return b
}

// numericBin encodes a decimal string in PostgreSQL's base-10000 `numeric`
// wire form, through pgtype — the same encoder a driver uses, so the bytes
// are a driver's bytes and not this file's idea of them.
func numericBin(s string) []byte {
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		panic(fmt.Sprintf("numeric %q: %v", s, err))
	}
	out, err := pgtype.NewMap().Encode(pgtype.NumericOID, pgtype.BinaryFormatCode, n, nil)
	if err != nil {
		panic(fmt.Sprintf("encoding numeric %q: %v", s, err))
	}
	return out
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
	// fixed. It USED to say "Wadjet has no exact numeric type … computes both
	// in float64 and declares OID 701"; #784 made SUM/AVG over a COLUMN follow
	// PostgreSQL exactly, and the pin on that shape is gone. What remains is
	// one step narrower and is the integer WIDTH, not the tower.
	noExactNumericPin = "DELIBERATE: wadjet declares every integer EXPRESSION int8 (ADR-0024's recorded " +
		"divergence, \"every integer spelling computes and is declared INT64\"), so PostgreSQL's " +
		"SUM(int4) -> bigint becomes wadjet's SUM(int8) -> numeric here. Both are EXACT and the values " +
		"agree digit for digit; the client is told a different width. Pinned, not exempted: the day the " +
		"integer widths follow PostgreSQL's, this entry fails and says so"
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
		// IDENTIFIER CASE on the WIRE (#731). The name a client reads a
		// column by is `field_names`, and this arm is the only place that
		// compares it for a #731 shape: an unquoted reference and an unquoted
		// alias FOLD, a delimited one keeps its bytes, and a keyword spelling
		// used as an alias folds like the identifier it is. Measured live —
		// `SELECT g AS Foo` publishes `foo` there and `AS "Foo"` publishes
		// `Foo`.
		{name: "IdentifierCaseUnquotedReference",
			sql: `SELECT N_NAME FROM nation ORDER BY n_nationkey LIMIT 1`},
		{name: "IdentifierCaseDelimitedReference",
			sql: `SELECT "n_name" FROM nation ORDER BY n_nationkey LIMIT 1`},
		{name: "IdentifierCaseUnquotedAlias",
			sql: `SELECT n_name AS Foo FROM nation ORDER BY n_nationkey LIMIT 1`},
		{name: "IdentifierCaseDelimitedAlias",
			sql: `SELECT n_name AS "Foo" FROM nation ORDER BY n_nationkey LIMIT 1`},
		{name: "IdentifierCaseKeywordAlias",
			sql: `SELECT n_name AS Desc FROM nation ORDER BY n_nationkey LIMIT 1`},
		{name: "IdentifierCaseQualifiedReference",
			sql: `SELECT N.N_NAME FROM nation N ORDER BY 1 LIMIT 1`},
		// A float column, where the declared OID and the text spelling of the
		// value are separate questions.
		{name: "Float8Column", sql: `SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_orderkey LIMIT 3`},
		// COUNT(*) is int8 in PostgreSQL, and a driver that reads it as int4
		// truncates silently past 2^31. Wadjet agrees on the OID here.
		{name: "CountStar", sql: `SELECT COUNT(*) AS c FROM nation`},
		// An outer expression over an AGGREGATE whose result is the
		// aggregate's OWN type. The DECLARATION is the whole question: the
		// value is a number either way, and a client reading `text` where
		// PostgreSQL declares `numeric` gets a string. Nothing in this corpus
		// asked it before — every aggregate entry here returns a number
		// directly — which is how a DAG declaring the STRING fallback for a
		// DATE went unseen by both oracle arms (#831 review B1).
		//
		// This arm runs wadjet's pgwire over the EMBEDDED db, so what it pins
		// is the declaration both engines are supposed to produce. That the
		// DAG's column agrees with it is the census's half
		// (coordinator.TestAnOuterExpressionOverAPublishedSlotAgreesOnEveryArm),
		// and the two together are what covers the shape.
		{name: "CaseOverAggregateKeepsItsOwnType",
			sql: `SELECT CASE WHEN MAX(l_quantity) > 0 THEN MAX(l_extendedprice) ELSE NULL END AS v ` +
				`FROM lineitem`},
		{name: "CoalesceOverAggregateKeepsItsOwnType",
			sql: `SELECT COALESCE(MAX(l_extendedprice), MIN(l_extendedprice)) AS v FROM lineitem`},
		// GROUPING(...) is `integer` in PostgreSQL, not bigint and not text.
		// The value arm cannot see a right bitmask under a wrong OID, and a
		// client that reads int4 where int8 is declared reads garbage (#804).
		{name: "GroupingBitmask", sql: `SELECT n_regionkey, GROUPING(n_regionkey) AS g, COUNT(*) AS c ` +
			`FROM nation GROUP BY ROLLUP(n_regionkey) ORDER BY g, n_regionkey`},
		// SUM over an int4 column is int8 in PostgreSQL and AVG is numeric.
		// The OID and SIZE pins are GONE since #784: `pg_typeof(sum(int4))` is
		// bigint and `pg_typeof(avg(int4))` is numeric on the live server, and
		// wadjet declares OID 20 and 1700 for them now instead of 701. This is
		// the wire arm's own proof of that fix — a value oracle cannot see a
		// right number under a wrong OID.
		{name: "SumAvgOverInteger", sql: `SELECT SUM(n_regionkey) AS s, AVG(n_regionkey) AS a FROM nation`,
			pins: map[string]string{
				wirePropFloatRender: "DELIBERATE: the AVG SCALE. PostgreSQL's numeric division picks a " +
					"MAGNITUDE-DEPENDENT scale (\"2.0000000000000000\" — sixteen digits here and eight " +
					"over a wider column) and wadjet answers at the fixed batch.AvgScale(0) = 4 " +
					"(\"2.0000\"), because a scale that depends on the values makes the same query over " +
					"more rows change the type of its own output column — ADR-0024's explicitly rejected " +
					"alternative. Same number, exact on both sides to the digits each keeps, agreeing to " +
					"min(scale): ADR-0012 item 9's class",
			}},
		// Integer arithmetic inside a CASE arm, at the wire. Both engines
		// answer the same digits here and the OID underneath them said
		// float8 (701) where PostgreSQL says an integer — a driver handing
		// the application a Double for a column that is an integer
		// everywhere else in the query. The integer rule reached only the
		// outermost node of a projection, so `SELECT k + 100` already had an
		// integer OID while the identical expression as a CASE arm did not.
		//
		// The OID is PINNED below for the standing int4/int8 width
		// divergence, so this entry does NOT gate the domain — a pin is a
		// reason, and it absorbs 701 as readily as 20. What gates it is
		// coordinator.TestNumericFoldTwoPath, whose CASE|n_i64|ARITH row
		// asserts the box exactly, unpinned, on both execution paths. These
		// two entries carry the field names, cell bytes, formats and command
		// tag of the shape, and record what a client actually reads.
		{name: "IntegerArithmeticInACaseArm",
			sql: `SELECT n_nationkey, CASE WHEN n_nationkey = 0 THEN n_nationkey ELSE n_nationkey + 100 END AS x
				FROM nation ORDER BY n_nationkey LIMIT 3`,
			pins: map[string]string{
				wirePropTypeOIDs: "DELIBERATE, and the same widening LiteralTypes records: integer " +
					"arithmetic is computed in int64 and declared INT64 (OID 20) where PostgreSQL keeps " +
					"the int4 column's width (23). The DOMAIN agrees; it was float8 (701) until the " +
					"declared fold learned this shape",
				wirePropTypeSizes: "same cause — the declared SIZE follows the OID: 8 for int8, 4 for int4",
			}},
		// The aggregate spelling, which is where it was found: MIN over the
		// same CASE described itself float8 on a column of integers.
		{name: "MinOverACaseWithIntegerArithmetic",
			sql: `SELECT MIN(CASE WHEN n_nationkey = 0 THEN n_nationkey ELSE n_nationkey + 100 END) AS x FROM nation`,
			pins: map[string]string{
				wirePropTypeOIDs:  "same int4/int8 widening as IntegerArithmeticInACaseArm above",
				wirePropTypeSizes: "same cause — the declared SIZE follows the OID",
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
		// A UUID expression: OID 2950 and size 16, the canonical lowercase
		// text, and the 16-byte binary form. Until #839 the cast declared
		// OID 25 and returned its operand unchanged, so a driver with a UUID
		// scanner never saw one — and the mixed-case spelling below is the
		// value half: PostgreSQL prints a uuid lowercase whatever was written.
		{name: "UUIDColumn", sql: `SELECT CAST('123E4567-E89B-12D3-A456-426614174000' AS uuid) AS u`},
		// FLOAT(n) resolves by WIDTH (#652): float(1..24) declares real (OID
		// 700, size 4) and float(25..53) double precision (701, size 8). Both
		// declared an unconstrained STRING before, with the double's digits
		// under it — the wire arm is what sees a right number under a wrong
		// OID, and the VALUE moves too: float(1) of 1/3 is 0.33333334.
		{name: "FloatPrecisionNarrow", sql: `SELECT CAST(1.0/3 AS float(1)) AS f`},
		{name: "FloatPrecisionWide", sql: `SELECT CAST(1.0/3 AS float(25)) AS f`},
		// PORT / PROTOCOL / DURATION declare integer OIDs since #834: 23 with
		// size 4 for the first two, 20 with size 8 for the third. The TEXT is
		// unchanged — all three already rendered as plain integers — so this
		// entry exists precisely because a VALUE oracle cannot see the change
		// and a driver can: under OID 25 pgx handed the application a String
		// for a column the engine compares numerically.
		{name: "NetworkIntegerTypes",
			sql: `SELECT n_port, n_proto, n_dur FROM net_probe WHERE n_key < 6 ORDER BY n_key`},
		// The BINARY form under those OIDs. A PORT boxes as an int32 and a
		// DURATION as an int64, so appendBinaryValue's own arms already write
		// the 4 and 8 bytes the OIDs promise — this entry is what says so,
		// the way binary_decode caught the UUID text under OID 2950.
		{name: "NetworkIntegerTypesOrdered",
			sql: `SELECT n_port FROM net_probe WHERE n_port IS NOT NULL ORDER BY n_port LIMIT 4`},
		// An INTEGER BOUND PARAMETER against a PORT column, which is the shape
		// declaring OID 23 invites a client to send: a driver that reads int4
		// from RowDescription binds `WHERE n_port = $1` as an int4, and
		// bindparams' numericOID has to render it as a bare SQL number rather
		// than a quoted string — `port_col = '443'` matches nothing. The
		// inferred twin is DataGrip's shape, where the client lets the server
		// decide the parameter's type.
		{name: "NetworkPortParamDeclared",
			sql:       `SELECT n_key FROM net_probe WHERE n_port = $1 ORDER BY n_key`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Text(631)}},
		{name: "NetworkPortParamInferred",
			sql:    `SELECT n_key FROM net_probe WHERE n_port = $1 ORDER BY n_key`,
			params: [][]byte{int4Text(631)}},
		{name: "NetworkDurationParamDeclared",
			sql:       `SELECT n_key FROM net_probe WHERE n_dur = $1 ORDER BY n_key`,
			paramOIDs: []uint32{20}, params: [][]byte{[]byte("1000000007")}},
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
		// #768 — ABS answers in its argument's OWN domain, and #757 —
		// NULLIF's type comes from the OPERATOR its two arguments select.
		//
		// These belong on the WIRE arm and nowhere else. The values agree on
		// every arm for most of them, so only the declared OID separates a
		// right answer from a wrong one — except `ABS(real)`, where the wrong
		// declaration WAS a wrong number: computing through a double gave
		// 0.1 the digits a float32 never held.
		//
		// The FLOOR entries are the controls that make this a rule rather
		// than a rewrite: `floor(bigint)` IS double precision in PostgreSQL,
		// measured, and so are CEIL, ROUND, TRUNC, SIGN, SQRT, POWER, LN and
		// EXP over an integer. Only ABS and MOD preserve the domain.
		{name: "AbsOverReal", sql: `SELECT ABS(r_val) AS v FROM real_probe WHERE r_key IN (0, 16) ORDER BY r_key`},
		{name: "AbsOverBigint", sql: `SELECT ABS(r_key) AS v FROM real_probe WHERE r_key IN (0, 3) ORDER BY r_key`},
		{name: "AbsOverInteger", sql: `SELECT ABS(r_grp) AS v FROM real_probe WHERE r_key IN (0, 3) ORDER BY r_key`},
		{name: "AbsOverDouble", sql: `SELECT ABS(d_val) AS v FROM real_probe WHERE r_key IN (0, 16) ORDER BY r_key`},
		{name: "ModOverBigint", sql: `SELECT MOD(r_key, 3) AS v FROM real_probe WHERE r_key IN (0, 3, 4) ORDER BY r_key`},
		{name: "ModOverInteger", sql: `SELECT MOD(r_grp, 3) AS v FROM real_probe WHERE r_key IN (0, 3, 4) ORDER BY r_key`},
		// MOD over an int4 column against an int8 COLUMN really is bigint on
		// both engines; the entry above uses a literal, and this one makes
		// the widening rule itself non-vacuous.
		{name: "ModIntegerAgainstBigint",
			sql: `SELECT MOD(r_grp, r_key + 3) AS v FROM real_probe WHERE r_key IN (0, 3, 4) ORDER BY r_key`},
		{name: "CtlFloorOverBigintIsDouble",
			sql: `SELECT FLOOR(r_key) AS v FROM real_probe WHERE r_key IN (0, 3) ORDER BY r_key`},
		{name: "CtlCeilOverBigintIsDouble",
			sql: `SELECT CEIL(r_key) AS v FROM real_probe WHERE r_key IN (0, 3) ORDER BY r_key`},
		{name: "CtlSqrtOverBigintIsDouble",
			sql: `SELECT SQRT(r_key) AS v FROM real_probe WHERE r_key IN (0, 4) ORDER BY r_key`},
		// #757's rows. The first two are argument 0's own width within one
		// family; the third and fourth are the cross-family pairs where
		// PostgreSQL resolves to float8 and a fold over the arguments would
		// answer `real` instead — which is what makes NULLIF a different rule
		// from GREATEST, not a variation on it.
		{name: "NullifWithinTheIntegerFamily",
			sql: `SELECT NULLIF(r_grp, r_key) AS v FROM real_probe WHERE r_key IN (0, 3) ORDER BY r_key`},
		{name: "NullifWithinTheFloatFamily",
			sql: `SELECT NULLIF(r_val, d_val) AS v FROM real_probe WHERE r_key IN (0, 16) ORDER BY r_key`},
		{name: "NullifIntegerAgainstAReal",
			sql: `SELECT NULLIF(r_key, r_val) AS v FROM real_probe WHERE r_key IN (0, 3) ORDER BY r_key`},
		{name: "NullifRealAgainstAnInteger",
			sql: `SELECT NULLIF(r_val, r_key) AS v FROM real_probe WHERE r_key IN (0, 16) ORDER BY r_key`},
		{name: "NullifDecimalAgainstADouble",
			sql: `SELECT NULLIF(d_2, d_wide) AS v FROM dec_probe WHERE d_key IN (1, 2) ORDER BY d_key`},
		// #768's other value half: PostgreSQL's float-to-integer cast is
		// rint(), half to EVEN, and the numeric-to-integer cast is not.
		{name: "CastFloatToIntegerRoundsHalfToEven",
			sql: `SELECT CAST(r_val AS BIGINT) AS v FROM real_probe WHERE r_key IN (16, 17) ORDER BY r_key`},

		// #758 — GREATEST/LEAST hand the winner over at the FOLD's width.
		//
		// The bare call was already right, because the projection narrowed the
		// winner into the fold's vector on the way out; ARITHMETIC over it
		// never reaches a vector before the operator, which is where the
		// integer's own width survived. So the arithmetic entry is the one
		// that gates the value, and the bare one gates the declaration.
		{name: "GreatestRealInteger",
			sql: `SELECT GREATEST(r_val, 16777217) AS v FROM real_probe WHERE r_key IN (16, 20) ORDER BY r_key`,
			pins: map[string]string{
				wirePropFloatRender: "DELIBERATE and pre-existing, independent of #758's narrowing: " +
					"wadjet spells a real 16777216 as \"16777216\" and PostgreSQL as " +
					"\"1.6777216e+07\" — the same number under the same OID, differing only in " +
					"where each engine switches to exponent form. Every other property of this " +
					"statement stays gated, and the arithmetic entry below — where the VALUE " +
					"this issue is about lives — is unpinned",
			}},
		// The integer operand is the LITERAL 16777217 and not `r_grp`, and
		// that is what makes this entry able to fail. `r_grp` is 0..3 on every
		// row of real_probe, so the REAL arm always won and the integer-arm
		// handover #758 is about never happened: the entry PASSED with the
		// fix's source reverted while calling itself "the one that gates the
		// value". 16777217 and the real 16777216 are the SAME real, so either
		// may win the comparison and the value differs by 1 unless the winner
		// is brought to the fold's width — which is the defect exactly.
		//
		// A literal rather than a fixture row because `real_probe.r_grp` is
		// cast to DECIMAL(9,2) by another corpus entry, and an r_grp past
		// 2^24 overflows that: the oracle then refuses the OTHER query and
		// stops being ground truth for it.
		{name: "GreatestRealIntegerArithmetic",
			sql: `SELECT GREATEST(r_val, 16777217) * 2 AS v FROM real_probe WHERE r_key IN (16, 20) ORDER BY r_key`},
		{name: "CtlCoalesceRealIntegerArithmetic",
			sql: `SELECT COALESCE(r_val, 16777217) * 2 AS v FROM real_probe WHERE r_key IN (16, 20) ORDER BY r_key`},

		// #760 — SUM and MIN/MAX over a REAL answer at REAL width.
		//
		// The values agree here, so only the declared OID separates the right
		// answer from the wrong one — except SUM, where the two ENGINES
		// disagreed on the number itself: 16777216 on the single-process path
		// and 16777215.600000001 on the DAG, reproducibly, because the batch
		// sum was float32 and the fold float64.
		//
		// The AVG entry is the control that keeps the two apart:
		// `pg_typeof(avg(real))` is double precision on the same server, so
		// AVG must NOT narrow, and its total widens each value rather than
		// the batch's float32 sum.
		{name: "SumOverReal", sql: `SELECT SUM(r_val) AS v FROM real_probe WHERE r_key IN (0, 16, 17)`},
		{name: "MinOverReal", sql: `SELECT MIN(r_val) AS v FROM real_probe WHERE r_key IN (0, 16, 17)`},
		{name: "MaxOverReal", sql: `SELECT MAX(r_val) AS v FROM real_probe WHERE r_key IN (0, 16, 17)`},
		{name: "CtlAvgOverRealIsDouble", sql: `SELECT AVG(r_val) AS v FROM real_probe WHERE r_key IN (0, 16, 17)`},
		{name: "CtlSumOverDoubleUnchanged", sql: `SELECT SUM(d_val) AS v FROM real_probe WHERE r_key IN (0, 16, 17)`},
		{name: "GroupedSumOverReal",
			sql: `SELECT r_grp, SUM(r_val) AS v FROM real_probe WHERE r_key IN (0, 16, 17) GROUP BY r_grp ORDER BY r_grp`},

		// #708 — a CAST that NAMES a (p,s) carries it. These four replace
		// `wadjet.TestCastTypmodIsUnconstrained`, the pin that recorded the
		// divergence: `declaredTypmod` had no CAST arm and answered -1 for
		// every spelling, while the TYPE side already resolved the
		// destination's (p,s) — so the two halves of one declaration
		// disagreed and a JDBC client's `getPrecision()` read 0 for a column
		// the query itself declares DECIMAL(9,2).
		//
		// The VALUES agree on every arm, which is why this belongs HERE and
		// nowhere else: no value oracle can see a right number under a
		// missing modifier. PostgreSQL 17 sends atttypmod 589830 for
		// numeric(9,2) and 1179656 for numeric(18,4) — ((p<<16)|s)+VARHDRSZ.
		//
		// The BARE spelling is the control and it must keep sending -1: a
		// `CAST(x AS numeric)` with no parameters describes as plain numeric
		// on the live server, so an arm that imposed a modifier for every
		// cast would be a new divergence in the other direction.
		{name: "CastToParameterizedDecimal",
			sql: `SELECT CAST(d_2 AS DECIMAL(9,2)) AS v FROM dec_probe WHERE d_key = 1`},
		{name: "CastToWiderParameterizedDecimal",
			sql: `SELECT CAST(d_2 AS DECIMAL(18,4)) AS v FROM dec_probe WHERE d_key = 1`},
		{name: "CastToParameterizedDecimalColonColon",
			sql: `SELECT d_4::DECIMAL(9,2) AS v FROM dec_probe WHERE d_key = 1`},
		{name: "CastToBareDecimalStaysUnconstrained",
			sql: `SELECT CAST(d_2 AS DECIMAL) AS v FROM dec_probe WHERE d_key = 1`},
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
		// A QUALIFIED reference to a name BOTH join arms carry, at two
		// different scales (#706). The declaration has to come from the arm
		// the qualifier names: PostgreSQL sends numeric(p,2) for `t.d_2` and
		// numeric(p,4) for `u.d_2`, and wadjet sent typmod -1 for both —
		// `mergeJoinSides` DROPS a name the two sides declare differently,
		// which is the honest answer to a BARE reference and the wrong one to
		// a qualified one. This is the arm no value oracle can provide: the
		// digits were right on the DAG the whole time and the OID's modifier
		// was not.
		{name: "DecimalQualifiedAcrossArmsProbeSide",
			sql: `SELECT t.d_2 FROM dec_probe t JOIN (SELECT d_key, d_4 AS d_2 FROM dec_probe) u
				ON t.d_key = u.d_key WHERE t.d_key IN (1, 2, 3) ORDER BY t.d_key`},
		{name: "DecimalQualifiedAcrossArmsBuildSide",
			sql: `SELECT u.d_2 FROM dec_probe t JOIN (SELECT d_key, d_4 AS d_2 FROM dec_probe) u
				ON t.d_key = u.d_key WHERE t.d_key IN (1, 2, 3) ORDER BY t.d_key`},
		{name: "DecimalQualifiedAcrossArmsBothProjected",
			sql: `SELECT t.d_2 AS a, u.d_2 AS b FROM dec_probe t
				JOIN (SELECT d_key, d_4 AS d_2 FROM dec_probe) u
				ON t.d_key = u.d_key WHERE t.d_key IN (1, 2, 3) ORDER BY t.d_key`},
		// The ZERO-ROW variant, which is described from the PLAN alone: with
		// the (p,s) dropped it went out as an unconstrained numeric where a
		// row-bearing result of the same query was right.
		{name: "DecimalQualifiedAcrossArmsZeroRows",
			sql: `SELECT t.d_2 FROM dec_probe t JOIN (SELECT d_key, d_4 AS d_2 FROM dec_probe) u
				ON t.d_key = u.d_key WHERE t.d_key < 0`},
		// Arithmetic OVER a DECIMAL WINDOW output (#729). These two are
		// CONTROLS, not ratchets: the wire server answers through the
		// SINGLE-process path, which typed this shape correctly before the
		// fix, so both PASS on de95b3b5. #729's failing arms were the DAG's
		// and no wire arm reaches them; the value gate that ratchets is
		// `coordinator.TestArithmeticOverADecimalWindowOutputThreeArms`. They
		// earn their place by pinning the declared MODIFIER, which that gate
		// cannot see — a right value under `numeric` with typmod -1 is a
		// divergence a value oracle reports as agreement.
		{name: "DecimalArithmeticOverAWindowOutput",
			sql: `SELECT id, w * 2 AS w2 FROM
				(SELECT d_key AS id, SUM(d_2) OVER () AS w FROM dec_probe WHERE d_key IN (1, 2, 3)) x
				ORDER BY id`},
		{name: "DecimalArithmeticOverAWindowOutputZeroRows",
			sql: `SELECT id, w * 2 AS w2 FROM
				(SELECT d_key AS id, SUM(d_2) OVER () AS w FROM dec_probe WHERE d_key < 0) x
				ORDER BY id`},
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
		// UNALIASED windows, where the NAME is on the wire in
		// RowDescription. PostgreSQL sends `sum` / `row_number` / `min`;
		// wadjet sent the window call's own text, `sum(d_2) OVER (...)`,
		// which a client binds by name and would never find. The corpus had
		// no unaliased window on either arm before this.
		{name: "WindowUnaliasedSum",
			sql: `SELECT d_key, SUM(d_2) OVER () FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "WindowUnaliasedRowNumber",
			sql: `SELECT d_key, ROW_NUMBER() OVER (ORDER BY d_key) FROM dec_probe
				WHERE d_key IN (1, 2, 3) ORDER BY d_key`},
		{name: "WindowUnaliasedMinMax",
			sql: `SELECT d_key, MIN(d_2) OVER (), MAX(d_2) OVER () FROM dec_probe
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
		// The two shapes #513 deliberately left alone, both closed by #732:
		// an operator expression with no natural name is `?column?` and a cast
		// takes its ARGUMENT's name. The pins are GONE; these entries announce
		// a regression by failing.
		{name: "UnaliasedArithmetic",
			sql: `SELECT supplier.s_acctbal + 1 FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`},
		// TWO unnamed items in one list: PostgreSQL calls both `?column?`, so
		// this is where a rule that de-duplicates the name shows.
		{name: "UnaliasedArithmeticTwice",
			sql: `SELECT supplier.s_acctbal + 1, supplier.s_acctbal + 2 FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`},
		// …and two of DIFFERENT TYPES, which is the cell this corpus did not
		// have. Every other duplicate-name entry here pairs two columns of one
		// type, so a declaration resolved by NAME — giving column 0 the LAST
		// column's OID — passed all of them while declaring an integer as TEXT
		// (round-1 review B2).
		{name: "UnaliasedDuplicateNamesOfDifferentTypes",
			sql: `SELECT supplier.s_suppkey + 1, supplier.s_name || 'x' FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`,
			pins: map[string]string{
				wirePropTypeOIDs: "int4 + an integer literal widens to int8 (OID 20) here where " +
					"PostgreSQL stays int4 (23). Not this cell's subject and not #732's: what it " +
					"asserts is that the TWO columns declare DIFFERENT types, which they do — a " +
					"name-keyed declaration gave both the LAST one's. The same integer-width " +
					"divergence is pinned on UnaliasedLiteral and belongs to the literal typing " +
					"layer",
				wirePropTypeSizes: "the size that follows that OID (8, not 4). Same mechanism; a " +
					"size pin without the OID pin would be the wrong half",
			}},
		// The MODIFIER half of the same question: two VARCHAR casts of
		// different lengths both publish `s_name`, and a name-keyed typmod
		// sent the second one's for both.
		{name: "DuplicateNamesOfDifferentModifiers",
			sql: `SELECT CAST(s_name AS VARCHAR(4)), CAST(s_name AS VARCHAR(9)) FROM supplier ORDER BY s_suppkey LIMIT 2`},
		// A literal and a predicate, the other two `?column?` families.
		{name: "UnaliasedLiteral",
			sql: `SELECT 1 FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`,
			pins: map[string]string{
				wirePropTypeOIDs: "an integer LITERAL in a SELECT list declares int8 (OID 20) where " +
					"PostgreSQL declares int4 (OID 23). Not #732's: the NAME is `?column?` on both " +
					"sides, which is what this entry was added for. The declaration is the literal " +
					"typing layer's — a bare integer literal is carried as int64 with no narrowing " +
					"to the smallest type that holds it — and found by this cell because no other " +
					"wire entry selects a bare literal",
				wirePropTypeSizes: "the size that follows that OID (8, not 4). Same mechanism; a " +
					"size pin without the OID pin would be the wrong half",
			}},
		{name: "UnaliasedPredicate",
			sql: `SELECT supplier.s_acctbal > 0 FROM supplier ORDER BY supplier.s_suppkey LIMIT 2`},
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
		// The pins are GONE (#768, 2026-09-03): ABS answers in its argument's
		// own domain now, so the OID and the size agree with PostgreSQL
		// outright. This entry announced the fix by failing, which is what a
		// pin is for — the change was made for #768's own shapes and this
		// one, filed as #530, came along with them.
		{name: "DuplicateNameIntegerFuncs",
			sql: `SELECT ABS(n_nationkey), ABS(n_regionkey) FROM nation ORDER BY n_nationkey LIMIT 6`},
		// CAST's label: PostgreSQL names a cast after its ARGUMENT
		// (`n_nationkey`), and only after the target TYPE when the argument
		// has no name of its own. Both halves are driven, because a rule that
		// reached for the type first would pass the second and fail the first.
		{name: "UnaliasedCast",
			sql: `SELECT CAST(n_nationkey AS BIGINT) FROM nation ORDER BY n_nationkey LIMIT 2`},
		{name: "UnaliasedCastOfALiteral",
			sql: `SELECT CAST(1 AS BIGINT) FROM nation ORDER BY n_nationkey LIMIT 2`},

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
		// A DERIVED bytea value, and no longer a pin: `bytea || bytea` is
		// bytea on both engines since #583, so the wire carries OID 17 and
		// PostgreSQL's own \x rendering rather than the raw bytes under
		// text's 25 — which is where #570's embedded-NUL hazard came back in
		// through a derived value. `substring(bytea, …)` is the sibling, and
		// it is here because it was the one whose VALUE moved: read as text
		// it produced the UTF-8 replacement character.
		{name: "ByteaConcat", sql: `SELECT b_key, b_val || b_other AS c FROM bytea_probe WHERE b_key IN (2, 3) ORDER BY b_key`},
		{name: "ByteaSubstring", sql: `SELECT b_key, substring(b_val from 1 for 1) AS c FROM bytea_probe ORDER BY b_key`},
		{name: "ByteaLength", sql: `SELECT b_key, length(b_val) AS c FROM bytea_probe ORDER BY b_key`},
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
		// An aggregate call is labelled after the FUNCTION (#732). Its output
		// name is still load-bearing INSIDE the planner — it IS the Aggregate
		// node's OutputCol, which GROUP BY, HAVING and ORDER BY resolve
		// against — which is why the published name is a second name applied
		// where the values leave the engine, not a rewrite of that one.
		{name: "UnaliasedAggregate",
			sql: `SELECT COUNT(supplier.s_name) FROM supplier`},
		// TWO unaliased aggregates: PostgreSQL publishes ONE name for both,
		// which is exactly the collapse the resolution spelling may not take.
		{name: "UnaliasedAggregateTwice",
			sql: `SELECT COUNT(supplier.s_name), COUNT(supplier.s_acctbal) FROM supplier`},
		// An unaliased aggregate BESIDE a GROUP BY key, so the published name
		// is compared where a HAVING and an ORDER BY resolve against the
		// planner's own spelling for the same column.
		{name: "UnaliasedAggregateGrouped",
			sql: `SELECT n_regionkey, COUNT(*) FROM nation GROUP BY n_regionkey ` +
				`HAVING COUNT(*) > 0 ORDER BY n_regionkey LIMIT 3`},

		// `SELECT *` with ZERO rows (#846). PostgreSQL always sends a
		// RowDescription for a SELECT; wadjet sent none at all for these,
		// because a bare star builds no Project node and the plan-declared
		// output schema (#416) had nothing to read. psql printed nothing and
		// pgJDBC's executeQuery threw "No results were returned by the query"
		// — and `SELECT * FROM t` is how a BI tool opens a table, so this is
		// the shape that matters most and the one no cell covered.
		//
		// dec_probe is the strong one: a star over it declares five columns
		// including two constrained numerics, so it compares names, OIDs and
		// TYPMODS across the whole row rather than one column at a time. The
		// non-empty control beside it is what says the zero-row answer is the
		// same answer, not merely a self-consistent one.
		{name: "StarOverDecimalProbe",
			sql: `SELECT * FROM dec_probe WHERE d_key IN (1, 2) ORDER BY d_key`},
		{name: "StarOverDecimalProbeZeroRows",
			sql: `SELECT * FROM dec_probe WHERE d_key = -1`},
		{name: "StarZeroRows", sql: `SELECT * FROM nation WHERE n_nationkey < 0`},
		{name: "StarZeroRowsOrdered",
			sql: `SELECT * FROM nation WHERE n_nationkey < 0 ORDER BY n_nationkey`},
		{name: "StarZeroRowsLimit", sql: `SELECT * FROM nation WHERE n_nationkey < 0 LIMIT 5`},
		{name: "StarDistinctZeroRows", sql: `SELECT DISTINCT * FROM nation WHERE n_nationkey < 0`},
		{name: "StarZeroRowsUnionAll",
			sql: `SELECT * FROM nation WHERE n_nationkey < 0 UNION ALL ` +
				`SELECT * FROM nation WHERE n_nationkey < 0`},

		// A PARAMETERIZED star, which is where the extended protocol's
		// Describe cannot measure the shape: this arm's Prepare is exactly
		// that Describe, and wadjet answers it from the plan's declaration
		// because running the statement with a NULL parameter would return
		// the wrong rows. A star with no cell for $1 is how the door came to
		// promise a RowDescription it could not keep (#846 round-1 B1), so
		// the corpus carries both the matching and the non-matching bind.
		{name: "StarParamMatching",
			sql:       `SELECT * FROM nation WHERE n_nationkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Text(3)}},
		{name: "StarParamZeroRows",
			sql:       `SELECT * FROM nation WHERE n_nationkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Text(-1)}},
		{name: "StarParamOverDecimalProbe",
			sql:       `SELECT * FROM dec_probe WHERE d_key = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Text(1)}},
		{name: "StarParamInferred",
			sql:    `SELECT * FROM nation WHERE n_nationkey = $1`,
			params: [][]byte{int4Text(3)}},

		// --- BINARY parameter format (#486) --------------------------------
		//
		// Every entry above sends its parameters as TEXT, because readOne
		// hard-coded a zero-filled format slice. pgx — and therefore every Go
		// program, and by default several other drivers — sends BINARY, so
		// pgwire's whole renderBinaryParam decoder was reachable from no
		// gate: thirteen OIDs of big-endian integers, IEEE floats, a
		// 2000-epoch date, raw bytea bytes, sixteen uuid bytes and
		// PostgreSQL's base-10000 numeric.
		//
		// Each of these is the TEXT entry it sits beside, re-sent as bytes.
		// The answer must be identical, on both servers: a parameter is a
		// value, and the format code is how it travelled.
		{name: "ParamInt4Binary", sql: `SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Bin(7)}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamInt2Binary", sql: `SELECT n_nationkey FROM nation WHERE n_regionkey = $1 ORDER BY n_nationkey`,
			paramOIDs: []uint32{21}, params: [][]byte{int2Bin(1)}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamInt8Binary", sql: `SELECT n_key FROM net_probe WHERE n_dur = $1 ORDER BY n_key`,
			paramOIDs: []uint32{20}, params: [][]byte{int8Bin(1000000007)}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamTextBinary", sql: `SELECT n_nationkey FROM nation WHERE n_name = $1`,
			paramOIDs: []uint32{25}, params: [][]byte{[]byte("BRAZIL")}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamBoolBinary", sql: `SELECT n_nationkey FROM nation WHERE (n_regionkey = 1) = $1 ORDER BY n_nationkey`,
			paramOIDs: []uint32{16}, params: [][]byte{boolBin(true)}, paramFormats: binaryFormats(1), minRows: 1},
		// bytea, whose binary form IS the value's bytes — the shape #570
		// closed on the way in, now asked in the format that carries it.
		{name: "ParamByteaBinary", sql: `SELECT b_key FROM bytea_probe WHERE b_val = $1 ORDER BY b_key`,
			paramOIDs: []uint32{17}, params: [][]byte{{0x68, 0x69}}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamByteaBinaryHighBytes",
			sql:       `SELECT b_key FROM bytea_probe WHERE b_val = $1 ORDER BY b_key`,
			paramOIDs: []uint32{17}, params: [][]byte{{0xff, 0xfe, 0x00, 0x41}}, paramFormats: binaryFormats(1), minRows: 1},
		// The NETWORK types, which declare int4 / int4 / int8 since #834 — so
		// a driver binds them as integers, in binary, and this is the only arm
		// that can see whether the value survived the declaration.
		{name: "ParamPortBinary", sql: `SELECT n_key FROM net_probe WHERE n_port = $1 ORDER BY n_key`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Bin(631)}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamProtocolBinary", sql: `SELECT n_key FROM net_probe WHERE n_proto = $1 ORDER BY n_key`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Bin(6)}, paramFormats: binaryFormats(1), minRows: 1},
		// float8 and numeric: IEEE 754 and PostgreSQL's base-10000 digits.
		{name: "ParamFloat8Binary", sql: `SELECT r_key FROM real_probe WHERE d_val = $1 ORDER BY r_key`,
			paramOIDs: []uint32{701}, params: [][]byte{float8Bin(0.5)}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamNumericBinary", sql: `SELECT d_key FROM dec_probe WHERE d_2 = $1 ORDER BY d_key`,
			paramOIDs: []uint32{1700}, params: [][]byte{numericBin("1.25")}, paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamNumericBinaryNegative", sql: `SELECT d_key FROM dec_probe WHERE d_2 = $1 ORDER BY d_key`,
			paramOIDs: []uint32{1700}, params: [][]byte{numericBin("-1.25")}, paramFormats: binaryFormats(1), minRows: 1},
		// date and uuid, over the multi-key fixture — the only tables in this
		// oracle carrying a real DATE and a real UUID column.
		{name: "ParamDateBinary", sql: `SELECT id FROM mk_outer WHERE dt = $1 ORDER BY id LIMIT 5`,
			paramOIDs: []uint32{1082}, params: [][]byte{dateBin(2024, time.January, 3)},
			paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamUUIDBinary", sql: `SELECT id FROM mk_outer WHERE u = $1 ORDER BY id LIMIT 5`,
			paramOIDs:    []uint32{2950},
			params:       [][]byte{uuidBin("2c5f39cb-3fb2-11d2-883f-0016d3cca427")},
			paramFormats: binaryFormats(1), minRows: 1},
		// A binary parameter that reaches a PLAN rather than a scan predicate.
		{name: "ParamInt4BinaryInAggregate", sql: `SELECT COUNT(*) AS c FROM nation WHERE n_regionkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Bin(1)}, paramFormats: binaryFormats(1), minRows: 1},
		// A binary parameter under SELECT *, so RowDescription is promised
		// before the bind is read (#846's shape, in the other format).
		{name: "StarParamBinary", sql: `SELECT * FROM nation WHERE n_nationkey = $1`,
			paramOIDs: []uint32{23}, params: [][]byte{int4Bin(3)}, paramFormats: binaryFormats(1), minRows: 1},
		// The five OIDs the first pass left ungated (round-1 P1). `timestamp`
		// is the one that matters most: it is what pgx binds a time.Time to
		// by default, and its decode is the 2000-epoch MICROSECOND conversion
		// — the most error-prone arm in renderBinaryParam. None of the three
		// temporal ones has a column in this fixture, so each is compared
		// against the literal of its own type, which is the question anyway:
		// did the eight bytes become the instant they encode.
		{name: "ParamFloat4Binary", sql: `SELECT r_key FROM real_probe WHERE r_val = $1 ORDER BY r_key`,
			paramOIDs: []uint32{700}, params: [][]byte{float4Bin(1.5)},
			paramFormats: binaryFormats(1), minRows: 1},
		// The ANSWER has to depend on the decoded value, or the cell gates
		// nothing. `WHERE $1 = $1` — the first spelling here — is true under
		// every decoding and `COUNT(*)` always returns one row, so a wrong
		// endianness passed it (round-2 B3). PostgreSQL resolves
		// `integer = oid` and `integer < oid` through its implicit int4→oid
		// cast, verified live, so the column comparison works on both sides.
		{name: "ParamOIDBinary",
			sql:       `SELECT n_nationkey FROM nation WHERE n_nationkey = $1`,
			paramOIDs: []uint32{26}, params: [][]byte{int4Bin(7)},
			paramFormats: binaryFormats(1), minRows: 1},
		// And a RANGE, so a decode that is merely off rather than byte-swapped
		// changes the row COUNT rather than only missing an equality.
		{name: "ParamOIDBinaryRange",
			sql:       `SELECT n_nationkey FROM nation WHERE n_nationkey < $1 ORDER BY n_nationkey`,
			paramOIDs: []uint32{26}, params: [][]byte{int4Bin(3)},
			paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamTimestampBinary",
			sql:       `SELECT COUNT(*) AS c FROM nation WHERE CAST('1996-01-10 12:34:56' AS timestamp) = $1`,
			paramOIDs: []uint32{1114}, params: [][]byte{timestampBin(1996, time.January, 10, 12, 34, 56)},
			paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamTimestampTZBinary",
			sql:       `SELECT COUNT(*) AS c FROM nation WHERE CAST('1996-01-10 12:34:56' AS timestamp) = $1`,
			paramOIDs: []uint32{1184}, params: [][]byte{timestampBin(1996, time.January, 10, 12, 34, 56)},
			paramFormats: binaryFormats(1), minRows: 1},
		{name: "ParamTimeBinary",
			sql:       `SELECT COUNT(*) AS c FROM nation WHERE CAST('12:34:56' AS time) = $1`,
			paramOIDs: []uint32{1083}, params: [][]byte{timeBin(12, 34, 56)},
			paramFormats: binaryFormats(1), minRows: 1},
		// A MIXED Bind (round-1 P2): one parameter binary, one text, in the
		// format array Bind actually carries. A server that reads the FIRST
		// format code and applies it to every parameter answers this wrong,
		// and no all-binary or all-text cell can see that.
		{name: "ParamMixedFormatsBinaryThenText",
			sql:       `SELECT n_nationkey FROM nation WHERE n_regionkey = $1 AND n_name = $2`,
			paramOIDs: []uint32{23, 25}, params: [][]byte{int4Bin(1), []byte("BRAZIL")},
			paramFormats: []int16{1, 0}, minRows: 1},
		{name: "ParamMixedFormatsTextThenBinary",
			sql:       `SELECT n_nationkey FROM nation WHERE n_name = $1 AND n_regionkey = $2`,
			paramOIDs: []uint32{25, 23}, params: [][]byte{[]byte("BRAZIL"), int4Bin(1)},
			paramFormats: []int16{0, 1}, minRows: 1},
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
		// Q12 sums a CASE, not a column, and it is GATED again since #784:
		// PostgreSQL types the CASE `integer` and its SUM `bigint` (OID 20),
		// and a computed integer argument declares bigint here too — the one
		// narrowing aggOutputFromInputDecl makes, precisely so this shape
		// keeps PostgreSQL's width.
		return nil
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
		// Q22 was pinned for #696 — `c_acctbal > (SELECT AVG(c_acctbal) …)` selected
		// the wrong rows, so the group's values and their binary decodings both
		// diverged. Both halves of the substitution are fixed and every wire
		// property agrees, so the query is gated again.
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
		// The temporal casts, whose refusal is the wire arm's business because
		// the two codes are DIFFERENT ANSWERS to a client: 22007 says the
		// literal is malformed, 22008 says the calendar has no such day. Both
		// were a NULL row until #836/#840 — a value oracle sees nothing wrong
		// with a NULL, which is precisely why these live here.
		{name: "CastTextToDateInvalidSyntax", sql: `SELECT CAST('not-a-date' AS date)`},
		{name: "CastTextToDateFieldOutOfRange", sql: `SELECT CAST('2020-02-30' AS date)`},
		{name: "CastTextToTimestampInvalidSyntax", sql: `SELECT CAST('not-a-timestamp' AS timestamp)`},
		{name: "CastTextToTimestampFieldOutOfRange", sql: `SELECT CAST('2020-02-30 12:00:00' AS timestamp)`},
		// A zero month, a zero day and a negative year — the field-range and
		// syntax classes over the same destination, so a client can tell "the
		// literal is malformed" from "the calendar has no such day".
		{name: "CastTextToDateMonthZero", sql: `SELECT CAST('2024-00-01' AS date)`},
		{name: "CastTextToDateDayZero", sql: `SELECT CAST('2024-01-00' AS date)`},
		{name: "CastTextToDateNegativeYear", sql: `SELECT CAST('-0001-01-01' AS date)`},
		{name: "CastTextToTimestampThreeDigitMonth", sql: `SELECT CAST('2024-001-01' AS timestamp)`},
		// The math DOMAIN refusals (#840). PostgreSQL uses four codes here and
		// they are four different answers: 2201E for a logarithm's domain,
		// 2201F for a square root's and for the two undefined powers, 22003
		// for an inverse-trig argument and for a float result that left the
		// type, 22012 for a base-1 logarithm and a zero modulus. Every one of
		// them was a NULL or an infinity before, which a value oracle reads as
		// an ordinary row.
		{name: "LogarithmOfZero", sql: `SELECT LN(0)`},
		{name: "LogarithmOfANegativeNumber", sql: `SELECT LN(-1)`},
		{name: "Log10OfZero", sql: `SELECT LOG(0)`},
		{name: "LogarithmBaseOne", sql: `SELECT LOG(1, 8)`},
		{name: "SquareRootOfANegativeNumber", sql: `SELECT SQRT(-1)`},
		{name: "ZeroToANegativePower", sql: `SELECT POWER(0, -1)`},
		{name: "NegativeToANonIntegerPower", sql: `SELECT POWER(-1, 0.5)`},
		{name: "PowerOverflow", sql: `SELECT POWER(2, 10000)`},
		{name: "ExpOverflow", sql: `SELECT EXP(1000)`},
		{name: "ExpUnderflow", sql: `SELECT EXP(-1000)`},
		{name: "ArcSineOutOfRange", sql: `SELECT ASIN(2)`},
		{name: "ArcCosineOutOfRange", sql: `SELECT ACOS(2)`},
		{name: "ModuloByZero", sql: `SELECT MOD(1, 0)`},
		// #839 and the ZERO half of its census. A CAST that cannot read its
		// text answered the text back (uuid) or the number 0 (the float
		// family) — the second being the one a client cannot detect at all.
		{name: "CastTextToUUIDInvalid", sql: `SELECT CAST('not-a-uuid' AS uuid)`},
		{name: "CastTextToDoubleInvalid", sql: `SELECT CAST('abc' AS double precision)`},
		{name: "CastTextToRealInvalid", sql: `SELECT CAST('abc' AS real)`},
		{name: "CastTextToNumericInvalid", sql: `SELECT CAST('abc' AS numeric)`},
		{name: "CastTextToDoubleOutOfRange", sql: `SELECT CAST('1e400' AS double precision)`},
		// FLOAT(n)'s two ends, which PostgreSQL refuses as a type modifier out
		// of range rather than as a data exception (#652).
		{name: "FloatPrecisionTooSmall", sql: `SELECT CAST(1.0 AS float(0))`},
		{name: "FloatPrecisionTooLarge", sql: `SELECT CAST(1.0 AS float(54))`},
		// A string type modifier the server refuses, on the CAST door. The
		// DDL door gives the identical type name the identical code — one
		// reading, `parquet.StringTypeLength` — which is what review round 0's
		// B3 was about.
		{name: "VarcharZeroLength", sql: `SELECT CAST('x' AS varchar(0))`},
		{name: "CharZeroLength", sql: `SELECT CAST('x' AS char(0))`},
		{name: "VarcharLengthTooLarge", sql: `SELECT CAST('x' AS varchar(10485761))`},
		// A float past an integer type's range CONVERTED with a wrap before
		// the range check ran, so `CAST(1e30 AS bigint)` answered
		// -9223372036854775808 while the int4 spelling raised — one
		// destination family, two answers (review round 0, P2).
		{name: "CastFloatPastBigintRange", sql: `SELECT CAST(1e30 AS bigint)`},
		{name: "CastFloatPastIntegerRange", sql: `SELECT CAST(1e30 AS integer)`},
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
		// #855: four scalar functions that MANUFACTURED a value where
		// PostgreSQL raises. Each entry is here rather than in the value arm
		// because a NULL — which is what three of them answered — is an
		// ordinary row to a value oracle, and because the SQLSTATEs are three
		// different answers to a client: 22023 says the argument names
		// nothing, 2201G is width_bucket's own condition, 54000 says the
		// request exceeds a program limit.
		{name: "DateTruncUnrecognizedUnit",
			sql: `SELECT DATE_TRUNC('bogus', CAST('2023-01-02 03:04:05' AS timestamp))`},
		{name: "WidthBucketZeroCount", sql: `SELECT WIDTH_BUCKET(1.0, 0.0, 10.0, 0)`},
		{name: "WidthBucketNegativeCount", sql: `SELECT WIDTH_BUCKET(1.0, 0.0, 10.0, -1)`},
		{name: "WidthBucketEqualBounds", sql: `SELECT WIDTH_BUCKET(1.0, 5.0, 5.0, 3)`},
		{name: "SplitPartZeroPosition", sql: `SELECT SPLIT_PART('a,b,c', ',', 0)`},
		// CHR's three, which carry TWO different SQLSTATEs on the server. The
		// zero is the operationally sharpest of the four functions: a NUL
		// cannot travel in a text-format DataRow and libpq truncates at one,
		// so before this the same query answered two lengths to two clients —
		// #570's shape reached through a function.
		{name: "ChrNul", sql: `SELECT CHR(0)`},
		{name: "ChrNegative", sql: `SELECT CHR(-1)`},
		{name: "ChrPastUnicodeRange", sql: `SELECT CHR(1114112)`},
		// #652's second half: a CAST to a name no type answers to. It declared
		// a STRING column over the operand and published a number as text
		// (the #310/#443 shape), which is a value a client treats as the
		// truth; PostgreSQL says 42704 `type "bogustype" does not exist`.
		// Both spellings, because the FLOAT operand is the one whose
		// pass-through published a MEASUREMENT.
		{name: "CastToUndefinedType", sql: `SELECT CAST(1 AS bogustype)`},
		{name: "CastFloatToUndefinedType", sql: `SELECT CAST(1.5 AS bogustype)`},
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

// wireTagCase is one statement whose CommandComplete tag is compared.
type wireTagCase struct {
	name string
	sql  string
	// setup runs on BOTH servers before the case, in dialect-neutral SQL.
	// The DML cases use it to restore the scratch table, so each one's count
	// is what it says it is regardless of what ran before it.
	setup []string
	pin   string
}

// runWireCommandTags compares the CommandComplete tag, which a client uses for
// its "n rows affected" report and, for some drivers, to decide whether a
// result set follows at all.
func runWireCommandTags(t *testing.T, ctx context.Context, wConn, pConn *pgconn.PgConn) {
	cases := []wireTagCase{
		{name: "SelectRows", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3`},
		{name: "SelectZeroRows", sql: `SELECT n_nationkey FROM nation WHERE n_nationkey < 0`},
		{name: "SelectOneRow", sql: `SELECT COUNT(*) FROM nation`},
		{name: "Begin", sql: `BEGIN`},
		{name: "Commit", sql: `COMMIT`},
		{name: "Set", sql: `SET extra_float_digits = 3`},
	}
	cases = append(cases, wireDMLTagCases(t, ctx, wConn, pConn)...)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, stmt := range c.setup {
				execBothOrFail(t, ctx, wConn, pConn, stmt, stmt)
			}
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

// wireDMLTagCases builds a scratch table on both servers and returns one case
// per DML verb × {0 rows, 1 row, N rows}.
//
// The CommandTags gate had six cases and NOT ONE of them was DML, so the one
// wire defect it existed to catch was invisible to it: over the EXTENDED
// protocol — the protocol pgx, JDBC, psycopg and every ORM use — wadjet
// reported `SELECT 1` for every INSERT, UPDATE, DELETE and MERGE (#816). The
// gate used the right door and had no fixture that attempted the shape, which
// is correctness-fix-protocol method 10 in its purest form. This is the
// fixture.
//
// The tag is not decoration: for a write it IS the statement's whole answer.
// An ORM's optimistic-concurrency check — `UPDATE … WHERE version = ?`, then
// "if 0 rows affected, someone else won" — cannot detect a conflict when
// every write reports 1.
func wireDMLTagCases(t *testing.T, ctx context.Context, wConn, pConn *pgconn.PgConn) []wireTagCase {
	t.Helper()

	// A leftover from a crashed run must not decide this run's counts.
	execBoth(ctx, wConn, pConn, `DROP TABLE wire_dml`, `DROP TABLE IF EXISTS wire_dml`)
	execBoth(ctx, wConn, pConn, `DROP TABLE wire_dml_src`, `DROP TABLE IF EXISTS wire_dml_src`)

	execBothOrFail(t, ctx, wConn, pConn,
		`CREATE TABLE wire_dml (id INT64, n INT64)`,
		`CREATE TABLE wire_dml (id bigint, n bigint)`)
	execBothOrFail(t, ctx, wConn, pConn,
		`CREATE TABLE wire_dml_src (id INT64, n INT64)`,
		`CREATE TABLE wire_dml_src (id bigint, n bigint)`)
	execBothOrFail(t, ctx, wConn, pConn,
		`INSERT INTO wire_dml_src (id, n) VALUES (1, 100), (9, 900)`,
		`INSERT INTO wire_dml_src (id, n) VALUES (1, 100), (9, 900)`)
	t.Cleanup(func() {
		bg := context.Background()
		execBoth(bg, wConn, pConn, `DROP TABLE wire_dml`, `DROP TABLE IF EXISTS wire_dml`)
		execBoth(bg, wConn, pConn, `DROP TABLE wire_dml_src`, `DROP TABLE IF EXISTS wire_dml_src`)
	})

	// Every case restores the same three rows first, so its count is what its
	// name says regardless of what ran before it.
	reset := []string{
		`DELETE FROM wire_dml WHERE id > -1000000`,
		`INSERT INTO wire_dml (id, n) VALUES (1, 10), (2, 20), (3, 30)`,
	}
	c := func(name, sql string) wireTagCase {
		return wireTagCase{name: name, sql: sql, setup: reset}
	}
	return []wireTagCase{
		c("InsertOneRow", `INSERT INTO wire_dml (id, n) VALUES (4, 40)`),
		c("InsertManyRows", `INSERT INTO wire_dml (id, n) VALUES (5, 50), (6, 60)`),
		c("UpdateOneRow", `UPDATE wire_dml SET n = 99 WHERE id = 1`),
		c("UpdateZeroRows", `UPDATE wire_dml SET n = 99 WHERE id = 999`),
		c("UpdateManyRows", `UPDATE wire_dml SET n = 0 WHERE id > 0`),
		c("DeleteOneRow", `DELETE FROM wire_dml WHERE id = 1`),
		c("DeleteZeroRows", `DELETE FROM wire_dml WHERE id = 999`),
		c("DeleteManyRows", `DELETE FROM wire_dml WHERE id > 0`),
		c("MergeOneRow", `MERGE INTO wire_dml AS t USING wire_dml_src AS s ON t.id = s.id `+
			`WHEN MATCHED THEN UPDATE SET n = s.n`),
		c("MergeZeroRows", `MERGE INTO wire_dml AS t USING wire_dml_src AS s ON t.id = s.id `+
			`WHEN MATCHED AND s.n > 100000 THEN DELETE`),
		c("MergeManyRows", `MERGE INTO wire_dml AS t USING wire_dml_src AS s ON t.id = s.id `+
			`WHEN MATCHED THEN UPDATE SET n = s.n `+
			`WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n)`),
	}
}

// execBoth runs a statement on each server and IGNORES failures: it is for the
// pre-run cleanup, where "the table was not there" is the expected outcome.
func execBoth(ctx context.Context, wConn, pConn *pgconn.PgConn, wSQL, pSQL string) {
	wConn.ExecParams(ctx, wSQL, nil, nil, nil, nil).Read()
	pConn.ExecParams(ctx, pSQL, nil, nil, nil, nil).Read()
}

func execBothOrFail(t *testing.T, ctx context.Context, wConn, pConn *pgconn.PgConn, wSQL, pSQL string) {
	t.Helper()
	if res := wConn.ExecParams(ctx, wSQL, nil, nil, nil, nil).Read(); res.Err != nil {
		t.Fatalf("wadjet fixture %q: %v", wSQL, res.Err)
	}
	if res := pConn.ExecParams(ctx, pSQL, nil, nil, nil, nil).Read(); res.Err != nil {
		t.Fatalf("PostgreSQL fixture %q: %v", pSQL, res.Err)
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

	// The CONTROL arm runs that statement behind a sleep floor, and only the
	// control arm does.
	//
	// Wadjet walks the cross product for over a minute; PostgreSQL finishes the
	// same 15,000-row nested loop in a few seconds (3.66s measured at SF0.01),
	// and a few seconds is not a safe margin over a cancel sent at 300ms. Under
	// a loaded machine the backend reached the end of the join before its next
	// CHECK_FOR_INTERRUPTS, and the probe reported the REFERENCE as having
	// accepted a CancelRequest and ignored it (#748) — a timing property of the
	// control, asserted as if it were a property of cancellation.
	//
	// Enlarging the shared statement is not the fix: the o_orderkey bound
	// exists to keep the work identical at every tier, and the smallest tier
	// has only 15,000 orders to bound, so there is nothing to enlarge it with.
	// The floor makes early completion impossible instead of unlikely. The
	// uncorrelated subquery plans as an InitPlan under a One-Time Filter ABOVE
	// the nested loop (EXPLAIN, postgres 17.11), so PostgreSQL runs it before
	// the join can produce anything and the cancel always lands inside the
	// sleep — an interruptible wait that answers 57014 in milliseconds.
	// Uncancelled the statement still ends on its own in well under a minute,
	// which the abandoned-statement handling below depends on.
	const pgSleepFloorSeconds = 30
	pgSlow := fmt.Sprintf("%s\n\t\t  AND (SELECT COUNT(*) FROM (SELECT pg_sleep(%d)) z) = 1", slow, pgSleepFloorSeconds)

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

	for _, s := range []struct{ name, dsn, sql string }{
		{"PostgreSQL", pgDSN, pgSlow},
		{"Wadjet", wadjetDSN, slow},
	} {
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
				state, err := execSQLState(runCtx, conn, s.sql)
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
				t.Errorf("wire divergence [%s]: %s\n  SQL: %s", wirePropSQLState, detail, s.sql)
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
