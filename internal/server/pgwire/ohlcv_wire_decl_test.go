package pgwire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// THE WIRE DECLARES THE SAME THING FOR A BAR FIELD WITH ROWS AND WITHOUT
// (#965 round 2, B1; the time_bucket cells are round 2's P2).
//
// The declaration is what a client binds by, and it was arm-dependent: the
// same statement sent OID 1700 from a standalone server and 701 from a
// coordinator, because the DAG's fold invented FLOAT64 for a column of empty
// states. `coordinator.TestTheBarsDeclaredTypeIsTheSameOnEveryArm` holds the
// five execution arms to ONE (type, precision, scale); this holds the WIRE to
// what that declaration MEANS — the OID a client reads, and the typmod beside
// it.
//
// A bar field's typmod is -1 for the reason every aggregate-produced DECIMAL's
// is: live PostgreSQL keeps numeric(p,s)'s typmod only for a bare column
// reference, so an aggregate result is WireUnconstrained here (#457/#458).
// That is exactly why the OID is asserted with rows AND over an EMPTY input:
// the typmod cannot show a lost precision, and over no rows the VALUES cannot
// either — a wrong declaration there is invisible in everything but this.
func TestPGWireDeclaresABarFieldTheSameWithRowsAndWithout(t *testing.T) {
	db := ohlcvWireDB(t)
	srv := startTestServer(t, db)

	const oidNumeric, oidInt8, oidTimestamp = 1700, 20, 1114
	// numeric's typmod is ((precision<<16)|scale) + VARHDRSZ, and it is the
	// ONLY place on the wire the precision appears — which is why B1's
	// numeric(18,4)-against-numeric(0,4) divergence was OID-invisible and
	// typmod-visible. px is DECIMAL(9,2) and vol is int4, so open/high/low/
	// close are numeric(9,2), volume is sum(int4) = bigint, and vwap is AVG's
	// DECIMAL(38,6) — ADR-0024's fixed +4 scale increment.
	tm := func(p, s int) int32 { return int32((p<<16)|(s&0xFFFF)) + 4 }
	barFields := []owField{
		{"o", oidNumeric, tm(9, 2)}, {"h", oidNumeric, tm(9, 2)},
		{"l", oidNumeric, tm(9, 2)}, {"c", oidNumeric, tm(9, 2)},
		{"v", oidInt8, -1}, {"w", oidNumeric, tm(38, 6)},
	}
	// A COMPUTED price declares what the EXPRESSION declares: DECIMAL(9,2) * 2
	// is DECIMAL(11,2) by ADR-0024's multiply rule, and the four price fields
	// follow it the way MIN of that expression would.
	computedFields := []owField{
		{"o", oidNumeric, tm(11, 2)}, {"h", oidNumeric, tm(11, 2)},
		{"l", oidNumeric, tm(11, 2)}, {"c", oidNumeric, tm(11, 2)},
		{"v", oidInt8, -1}, {"w", oidNumeric, tm(38, 6)},
	}
	for _, tc := range []struct {
		name string
		sql  string
		want []owField
	}{
		{"bar_fields_with_rows",
			`SELECT (b).open o, (b).high h, (b).low l, (b).close c, (b).volume v, (b).vwap w
			 FROM (SELECT ohlcv(ts, px, vol) AS b FROM owt) t`,
			barFields},
		{"bar_fields_over_an_empty_input",
			`SELECT (b).open o, (b).high h, (b).low l, (b).close c, (b).volume v, (b).vwap w
			 FROM (SELECT ohlcv(ts, px, vol) AS b FROM owt WHERE id < 0) t`,
			barFields},
		{"bar_fields_over_a_computed_price",
			`SELECT (b).open o, (b).high h, (b).low l, (b).close c, (b).volume v, (b).vwap w
			 FROM (SELECT ohlcv(ts, px * 2, vol) AS b FROM owt) t`,
			computedFields},
		// time_bucket declares `timestamp` — OID 1114 — which is what
		// date_trunc declares beside it. Asserted on the WIRE because this is
		// the door a value oracle cannot see: a right instant under OID 25 is
		// a bucket every client reads as text.
		{"time_bucket_declares_timestamp",
			`SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt FROM owt`,
			[]owField{{"bkt", oidTimestamp, -1}}},
		{"time_bucket_over_an_empty_input",
			`SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt FROM owt WHERE id < 0`,
			[]owField{{"bkt", oidTimestamp, -1}}},
		{"time_bucket_grouped",
			`SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt, COUNT(*) n
			 FROM owt GROUP BY 1 ORDER BY 1`,
			[]owField{{"bkt", oidTimestamp, -1}, {"n", oidInt8, -1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := owRowDescription(t, srv, tc.sql)
			if len(got) != len(tc.want) {
				t.Fatalf("RowDescription has %d fields, want %d: %+v", len(got), len(tc.want), got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("field %d: got %+v, want %+v\n  SQL: %s", i, got[i], tc.want[i], tc.sql)
				}
			}
		})
	}
}

type owField struct {
	name   string
	oid    uint32
	typmod int32
}

// owRowDescription runs one query and returns its RowDescription's per-field
// (name, type OID, type modifier).
func owRowDescription(t *testing.T, srv *Server, sql string) []owField {
	t.Helper()
	client := newPGClient(t, srv.Addr())
	client.startup("test", "wadjet")
	client.writeMsg('Q', append([]byte(sql), 0))
	var desc []byte
	for {
		typ, payload, err := client.readMsg()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch typ {
		case 'T':
			desc = append([]byte(nil), payload...)
		case 'E':
			t.Fatalf("refused: %s", client.parseError(payload))
		}
		if typ == 'Z' {
			break
		}
	}
	client.terminate()
	if len(desc) < 2 {
		t.Fatal("no RowDescription")
	}
	n := int(binary.BigEndian.Uint16(desc[0:2]))
	out := make([]owField, 0, n)
	p := 2
	for i := 0; i < n; i++ {
		nul := bytes.IndexByte(desc[p:], 0)
		if nul < 0 {
			t.Fatal("malformed RowDescription")
		}
		name := string(desc[p : p+nul])
		p += nul + 1
		// tableOID(4) attnum(2) typeOID(4) typlen(2) typmod(4) format(2)
		if p+18 > len(desc) {
			t.Fatal("truncated RowDescription field")
		}
		out = append(out, owField{
			name:   name,
			oid:    binary.BigEndian.Uint32(desc[p+6 : p+10]),
			typmod: int32(binary.BigEndian.Uint32(desc[p+12 : p+16])),
		})
		p += 18
	}
	return out
}
