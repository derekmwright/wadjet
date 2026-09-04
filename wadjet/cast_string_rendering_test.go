package wadjet

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file gates #521: CAST(<col> AS STRING) must render the same text the
// column's own projection does — the claim ADR-0012 item 11 makes for LIKE
// and every scalar function argument, which CAST itself did not keep for
// two types.
//
// DATE: Cast.Eval's string-family case read a DATE operand through
// ColRef.Eval's raw epoch-day int32 fast path instead of the column's own
// rendering, so `CAST(c_date AS STRING)` answered the epoch day ("15007")
// where the projection and LIKE (both already fixed, #497) answer the date
// ("2011-02-02"). Verified against live PostgreSQL 17: `date '2011-02-02'::
// text` answers "2011-02-02".
//
// FLOAT32: the same operand-boxing gap, found while fixing DATE.
// ColRef.Eval widens a FLOAT32 column to float64, so
// `CAST(c_f32 AS STRING)` printed the float64-widened digits
// ("0.1428571492433548") instead of the float32-shortest-round-trip form
// ("0.14285715") the projection and LIKE use. Verified against live
// PostgreSQL 17: a `real` column holding 1.0::real/7::real answers
// "0.14285715" for `::text`.
//
// Both are now fixed through one resolver (expr.boxedTextOperand), shared
// with LIKE's operand rendering instead of a second, narrower copy
// (networkOperand, IPv4/MAC only) — the two-implementation drift ADR-0012
// calls out elsewhere (CidrSortKey, appendColumnValue) for exactly this
// class of bug.

// TestCastStringPinsPostgresRenderingPerType pins one literal answer per
// fixed type, so a CAST implementation and a comparator that agreed on the
// same wrong answer would still fail the test. Every value here was read
// off live PostgreSQL 17, which is the authority ADR-0012 item 1 names.
func TestCastStringPinsPostgresRenderingPerType(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	cases := []struct {
		name string
		sql  string
		want any
	}{
		{"date", fmt.Sprintf("SELECT CAST(c_date AS STRING) AS v FROM %s WHERE id = 1", typematrix.Table), "2011-02-02"},
		{"float32", fmt.Sprintf("SELECT CAST(c_f32 AS STRING) AS v FROM %s WHERE id = 7", typematrix.Table), "1"},
		// BYTES (#570): PostgreSQL's `bytea::text` under the default
		// bytea_output = hex. Verified against live PostgreSQL 17 —
		// `'bytes-000001-x'::bytea::text` answers
		// "\x62797465732d3030303030312d78". The literal spelling is pinned
		// here rather than derived, because deriving it from the same
		// hex.EncodeToString the fix uses would only prove the function is
		// deterministic.
		{"bytes", fmt.Sprintf("SELECT CAST(c_bytes AS STRING) AS v FROM %s WHERE id = 1", typematrix.Table),
			`\x62797465732d3030303030312d78`},
		// TIMESTAMP (#544): the fixture's c_ts at id 0 is epoch ms
		// 1700000000000. Live PostgreSQL 17:
		// `SELECT (to_timestamp(1700000000000/1000.0) AT TIME ZONE 'UTC')::text`
		// answers "2023-11-14 22:13:20". CAST answered the epoch number.
		{"timestamp", fmt.Sprintf("SELECT CAST(c_ts AS STRING) AS v FROM %s WHERE id = 0", typematrix.Table),
			"2023-11-14 22:13:20"},
		// The LIKE half of the same defect, at the OTHER implementation: the
		// single-process filter runs kernel.ResolveLikeFilterKernel, whose
		// renderer read TIMESTAMP through Vector.GetValue's raw int64, so
		// this predicate matched the digits of the epoch and was false for a
		// 2023 timestamp.
		{"timestamp_like", fmt.Sprintf(
			"SELECT COUNT(*) AS v FROM %s WHERE c_ts LIKE '2023-11-14 22:13:20' AND id < 5",
			typematrix.Table), int64(1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmRun(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(res.Rows))
			}
			got := res.Rows[0]["v"]
			if got != tc.want {
				t.Errorf("%s = %#v, want %#v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestCastStringAgreesWithPostgresAcrossFixture sweeps every row of the
// type-matrix fixture (multiple scan batches — the vectorized EvalVec path
// falls back to per-row Eval for every scalar function including CAST, so
// this also exercises the batch boundary) and checks CAST(<col> AS STRING)
// against POSTGRESQL'S rendering of the same value, for every FLAT type —
// not only the two #521 fixed (DATE, FLOAT32): boxedTextOperand is the one
// resolver both LIKE and CAST share for all 18, so a divergence anywhere in
// that set is one implementation disagreeing with itself, and every flat
// type belongs in the sweep that would catch a sixth one (item 11's own
// point about "types that box differently" needing a checkable list, not an
// enumeration someone has to remember to extend).
//
// THE REFERENCE USED TO BE THE PROJECTION'S OWN TEXT, and that is why this
// test could not fail for #544. The embedded API boxes a TIMESTAMP as its
// raw epoch-millisecond int64 — deliberately, and with five compute paths
// that need it — so `fmt.Sprint(projection)` was "1700000000000" and CAST
// answered "1700000000000" and the two AGREED, both wrong. A gate whose
// reference is one half of the bug passes for the wrong reason (protocol
// method 2). The reference is PostgreSQL's now, per type, written out here
// rather than taken from the implementation being tested:
//
//	TIMESTAMP  UTC "2006-01-02 15:04:05", with .000 only when the
//	           millisecond part is non-zero — PostgreSQL 17's `timestamp`
//	           output, and what the pgwire send path already puts on the
//	           wire for this column under OID 1114 (#321)
//	BYTES      `\x` plus lowercase hex, PostgreSQL's default bytea_output
//	           (#570) — built from the projected bytes rather than read
//	           from CAST, so a CAST and a comparator agreeing on the same
//	           wrong hex would still fail
//	everything the projection already renders the way PostgreSQL prints it
//	           (DATE as the date, IPv4 as the address, DECIMAL as its
//	           digits, …) keeps the projection as its reference — for those
//	           types the two references are the same string, and saying so
//	           in one place is what keeps this sweep total.
func TestCastStringAgreesWithPostgresAcrossFixture(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	for _, c := range typematrix.Columns() {
		if !c.Flat {
			continue
		}
		col := c.Name
		typ := c.Type
		t.Run(col, func(t *testing.T) {
			res, err := tmRun(ctx, db, fmt.Sprintf(
				"SELECT id, %s AS v, CAST(%s AS STRING) AS s FROM %s WHERE %s IS NOT NULL ORDER BY id",
				col, col, typematrix.Table, col))
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(res.Rows) == 0 {
				t.Fatalf("no non-NULL rows for %s in the fixture", col)
			}
			mismatches := 0
			for _, r := range res.Rows {
				want, ok := pgTextOf(typ, r["v"])
				if !ok {
					t.Fatalf("id %v: %s projected as %T, which this test has no "+
						"PostgreSQL rendering for — add one rather than falling back "+
						"to the projection's own text, which is what made this gate "+
						"unable to fail for #544", r["id"], col, r["v"])
				}
				got, _ := r["s"].(string)
				if got != want {
					mismatches++
					if mismatches <= 5 {
						t.Errorf("id %v: CAST(%s AS STRING) = %q, PostgreSQL 17 renders %q",
							r["id"], col, got, want)
					}
				}
			}
			if mismatches > 5 {
				t.Errorf("... and %d more mismatches", mismatches-5)
			}
		})
	}
}

// pgTextOf is PostgreSQL 17's `::text` for one projected value of a declared
// type. It is written from PostgreSQL's output rules rather than derived from
// the engine's own renderers (protocol method 5): batch.FormatTimestamp is
// what the fix calls, so calling it here would only prove the function is
// deterministic.
//
// The TIMESTAMP arm is where that discipline actually bites. PostgreSQL prints
// the MINIMAL fraction — `('…20.500'::timestamp)::text` is `…20.5`, verified
// live — and `batch.FormatTimestamp` printed three digits always until #544.
// Writing the three-digit rule here would have re-encoded the implementation as
// the reference in the one branch where the two differed, which is exactly what
// made the OLD version of this sweep unable to fail for #544. It stays written
// from the server now that the two agree, so the reference is independent of
// the function it checks. Every c_ts in the type-matrix fixture is a whole
// second, so the fractional branch needs its own fixture:
// TestCastTimestampSubSecondPrintsTheMinimalFraction below.
func pgTextOf(typ parquet.TypeID, v any) (string, bool) {
	switch typ {
	case parquet.TypeTimestamp:
		ms, ok := v.(int64)
		if !ok {
			return "", false
		}
		t := time.UnixMilli(ms).UTC()
		if frac := ms % 1000; frac != 0 {
			// PostgreSQL trims trailing zeros: .5, .25, .125 — never .500.
			return strings.TrimRight(t.Format("2006-01-02 15:04:05.000"), "0"), true
		}
		return t.Format("2006-01-02 15:04:05"), true
	case parquet.TypeBytes:
		b, ok := v.([]byte)
		if !ok {
			return "", false
		}
		return `\x` + hex.EncodeToString(b), true
	case parquet.TypeDuration:
		// DEFERRED, not agreed (#544, ADR-0012 item 11). PostgreSQL renders
		// the equivalent `interval` as `00:00:00.001`; wadjet renders the raw
		// nanosecond count, HERE AND ON THE WIRE AND IN THE DOCS. Comparing
		// CAST against the projection is the weak reference this test exists
		// to stop using — so the disagreement is stated in one place and
		// ratcheted by TestCastDurationIsPinnedToNanoseconds below, rather
		// than dissolved into the default arm where nobody would see it.
		return fmt.Sprint(v), true
	default:
		// Every other flat type's projection is already the text PostgreSQL
		// prints — that is what makes the box a display boundary for them.
		if b, ok := v.([]byte); ok {
			return `\x` + hex.EncodeToString(b), true
		}
		return fmt.Sprint(v), true
	}
}

// TestCastDurationIsPinnedToNanoseconds is the DEFERRAL of #544's DURATION
// half, in the form protocol method 7 asks for: a pin that FAILS the day the
// deferral is lifted, so the residual cannot survive quietly.
//
// PostgreSQL 17: `CAST(INTERVAL '1 millisecond' AS text)` = "00:00:00.001".
// Wadjet: "1000000", the nanosecond count — and that is what the WIRE sends
// for the same column (pgTypeOID maps DURATION to OID 25, text) and what
// docs/sql-reference.md documents the type as. Rendering only the CAST would
// give the column two answers on one connection, which is the defect #544
// closed for TIMESTAMP.
//
// When DURATION gets a text form — OID 1186, the send path, the row reader,
// the INSERT coercion and the doc, together — this test fails, and the fix is
// to replace it with PostgreSQL's spelling here and in pgTextOf.
func TestCastDurationIsPinnedToNanoseconds(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	res, err := tmRun(ctx, db, fmt.Sprintf(
		"SELECT CAST(c_dur AS STRING) AS v FROM %s WHERE id = 1", typematrix.Table))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	got, _ := res.Rows[0]["v"].(string)
	if got != "1000000" {
		t.Errorf("CAST(c_dur AS STRING) = %q, this pin records \"1000000\" and "+
			"PostgreSQL 17 renders the same interval \"00:00:00.001\".\n"+
			"If DURATION now has a text form, #544's deferred half has moved: check "+
			"that the WIRE (pgTypeOID/OID 1186), the send path, the row reader, the "+
			"INSERT coercion and docs/sql-reference.md moved with it, then replace this "+
			"pin and pgTextOf's DURATION arm with PostgreSQL's spelling (ADR-0012 item 11).", got)
	}
}

// TestCastTimestampSubSecondIsPaddedToMilliseconds pins the ONE branch in
// which wadjet's timestamp rendering and PostgreSQL's differ, on its own
// one-row fixture — the type-matrix `c_ts` is `1_700_000_000_000 + i*61_000`,
// every value a whole second, so the sweep above cannot reach this and would
// have gone on claiming "CAST agrees with PostgreSQL" for a rule it never
// exercised.
//
// PostgreSQL 17 prints the MINIMAL fraction (verified live:
// `('2023-11-14 22:13:20.500'::timestamp)::text` is `2023-11-14 22:13:20.5`,
// and `.250` prints `.25`). `batch.FormatTimestamp` printed THREE digits
// always, and it is the same function pgwire's send path calls, so the padding
// was what a client saw too — the divergence was on the WIRE, not only in
// CAST.
//
// This was a PIN recording the padding, with the note that the day it went the
// test should be deleted. It is an ASSERTION now (#544): every `c_ts` in the
// type-matrix fixture is a whole second, so the sweep above never reaches the
// fractional branch and this two-row fixture is the only thing that does.
// `pgTextOf` above is unchanged and still writes PostgreSQL's rule from the
// server rather than from `FormatTimestamp`, which is what keeps the sweep's
// reference independent.
func TestCastTimestampSubSecondPrintsTheMinimalFraction(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "subsec_ts", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("subsec_ts", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		// 2023-11-14 22:13:20.500 and .250 — the two fractions whose minimal
		// spellings (.5, .25) differ from the three-digit ones.
		{"id": int64(1), "ts": int64(1_700_000_000_500)},
		{"id": int64(2), "ts": int64(1_700_000_000_250)},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for _, tc := range []struct {
		id       int64
		want, pg string
	}{
		{1, "2023-11-14 22:13:20.5", "2023-11-14 22:13:20.5"},
		{2, "2023-11-14 22:13:20.25", "2023-11-14 22:13:20.25"},
	} {
		res, err := tmRun(ctx, db, fmt.Sprintf(
			"SELECT CAST(ts AS STRING) AS v FROM subsec_ts WHERE id = %d", tc.id))
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("id %d: %d rows, want 1", tc.id, len(res.Rows))
		}
		got, _ := res.Rows[0]["v"].(string)
		if got != tc.want {
			t.Errorf("id %d: CAST(ts AS STRING) = %q, want %q — PostgreSQL 17 renders %q "+
				"and batch.FormatTimestamp is what pgwire's send path calls, so a padded "+
				"fraction here is a padded fraction on the wire (#544).",
				tc.id, got, tc.want, tc.pg)
		}
	}
}
