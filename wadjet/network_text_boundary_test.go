package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file is arc NT's ONE gate: every network-native type has ONE text
// grammar and every boundary that reads text reads THAT one. The table below
// is the seam enumerated once — type × input form × boundary — and each cell's
// expected disposition is PostgreSQL 17.11's own answer, measured in a
// postgres:17-alpine container (inet for IPv4/IPv6/CIDR, macaddr for MAC, uuid
// for UUID; PORT and PROTOCOL have no PostgreSQL type, so their oracle is this
// engine's documented text form — docs/data-types.md).
//
// Before this arc the same literal got three different answers depending on
// which door it arrived at: the writer refused `aa-bb-cc-dd-ee-ff` and `{uuid}`
// that the comparison kernels accepted, the kernels accepted `a-0eebc99…` that
// PostgreSQL refuses, the CAST parsed nothing at all, and a CIDR column
// validated nothing whatsoever (#1092, #627, #986, #1088).

// ntBoundary names one door text can enter through.
type ntBoundary string

const (
	ntIngester   ntBoundary = "ingester"      // db.NewIngester (the embedded writer)
	ntValues     ntBoundary = "insert-values" // INSERT INTO t (c) VALUES ('…')
	ntInsertSel  ntBoundary = "insert-select" // INSERT INTO t (c) SELECT '…'
	ntCastLit    ntBoundary = "cast-literal"  // CAST('…' AS T)
	ntCastCol    ntBoundary = "cast-column"   // CAST(s AS T) over a STRING column
	ntWhereEmpty ntBoundary = "where-empty"   // a literal in WHERE over a scan no row survives
	ntWhereFull  ntBoundary = "where-rows"    // the same over a scan that has rows
)

var ntAllBoundaries = []ntBoundary{
	ntIngester, ntValues, ntInsertSel, ntCastLit, ntCastCol, ntWhereEmpty, ntWhereFull,
}

// ntCell is one row of the coverage table: a type, an input form, the
// disposition every boundary gives it, and the deliberate per-boundary
// exceptions. An exception here is a CLAIM — it says this engine answers
// differently at this door ON PURPOSE — so each one carries its reason in the
// table's comments and in ADR-0012.
type ntCell struct {
	typ  parquet.TypeID
	form string
	want string // "ok" | "22P02" | "22003" | "0A000"
	only map[ntBoundary]string
}

func (c ntCell) at(b ntBoundary) string {
	if v, ok := c.only[b]; ok {
		return v
	}
	return c.want
}

// ntWriterNull is the one exception every type shares, and it is a SPLIT
// between two kinds of door rather than a divergence from PostgreSQL: at the
// embedded INGESTER an empty string is ABSENCE — the empty CSV or JSON field,
// which means NULL — while at every SQL door it is a VALUE the type cannot
// read, which is PostgreSQL's 22P02 for an empty inet literal. Both doors said
// absence until this arc, so `INSERT INTO t (ip) VALUES (”)` stored a NULL
// nobody wrote.
var ntWriterNull = map[ntBoundary]string{ntIngester: "ok"}

func ntCells() []ntCell {
	return []ntCell{
		// --- IPv4 -----------------------------------------------------------
		{typ: parquet.TypeIPv4, form: "10.0.0.1", want: "ok"},
		{typ: parquet.TypeIPv4, form: "010.1.2.3", want: "ok"},   // leading zeros are decimal
		{typ: parquet.TypeIPv4, form: "10.1.2.3.", want: "ok"},   // one trailing dot is ignored
		{typ: parquet.TypeIPv4, form: "10.0.0.1/32", want: "ok"}, /* a HOST-width prefix is the address */
		{typ: parquet.TypeIPv4, form: "192.168", want: "22P02"},  // no mask ⇒ all four octets
		{typ: parquet.TypeIPv4, form: "10.0.0.256", want: "22P02"},
		{typ: parquet.TypeIPv4, form: "zzz", want: "22P02"},
		// A NETWORK: valid inet, and a bare-address column has no room for it.
		// 0A000 rather than 22P02 because the TEXT is not the problem
		// (ADR-0012 item 5).
		{typ: parquet.TypeIPv4, form: "10/8", want: "0A000"},
		{typ: parquet.TypeIPv4, form: "", want: "22P02", only: ntWriterNull},

		// --- IPv6 -----------------------------------------------------------
		{typ: parquet.TypeIPv6, form: "2001:db8::1", want: "ok"},
		{typ: parquet.TypeIPv6, form: "2001:DB8::1", want: "ok"},
		{typ: parquet.TypeIPv6, form: "::1", want: "ok"},
		{typ: parquet.TypeIPv6, form: "::ffff:10.0.0.1", want: "ok"},
		{typ: parquet.TypeIPv6, form: "2001:db8::1/128", want: "ok"},
		{typ: parquet.TypeIPv6, form: "2001:db8::1/64", want: "0A000"},
		// inet6's mask grammar is NOT inet's: no leading zeros, ever.
		{typ: parquet.TypeIPv6, form: "::1/064", want: "22P02"},
		{typ: parquet.TypeIPv6, form: "zzz", want: "22P02"},
		{typ: parquet.TypeIPv6, form: "", want: "22P02", only: ntWriterNull},

		// --- CIDR -----------------------------------------------------------
		{typ: parquet.TypeCIDR, form: "192.168.1.0/24", want: "ok"},
		{typ: parquet.TypeCIDR, form: "10.0.0.1/32", want: "ok"},
		{typ: parquet.TypeCIDR, form: "192.168/16", want: "ok"}, // abbreviated, with a mask
		{typ: parquet.TypeCIDR, form: "10.0.0.1", want: "ok"},   // a bare address is /32
		{typ: parquet.TypeCIDR, form: "2001:db8::1/64", want: "ok"},
		{typ: parquet.TypeCIDR, form: "192.168", want: "22P02"}, // abbreviated, NO mask
		{typ: parquet.TypeCIDR, form: "10.0.0.1/33", want: "22P02"},
		{typ: parquet.TypeCIDR, form: "zzz", want: "22P02"},
		{typ: parquet.TypeCIDR, form: "", want: "22P02", only: ntWriterNull},

		// --- MAC ------------------------------------------------------------
		// macaddr_in is a LIST OF sscanf PATTERNS; all seven spellings are one
		// value and the regroupings between them are 22P02.
		{typ: parquet.TypeMAC, form: "aa:bb:cc:dd:ee:ff", want: "ok"},
		{typ: parquet.TypeMAC, form: "AA:BB:CC:DD:EE:FF", want: "ok"},
		{typ: parquet.TypeMAC, form: "aa-bb-cc-dd-ee-ff", want: "ok"},
		{typ: parquet.TypeMAC, form: "aabbcc:ddeeff", want: "ok"},
		{typ: parquet.TypeMAC, form: "aabbcc-ddeeff", want: "ok"},
		{typ: parquet.TypeMAC, form: "aabb.ccdd.eeff", want: "ok"},
		{typ: parquet.TypeMAC, form: "aabb-ccdd-eeff", want: "ok"},
		{typ: parquet.TypeMAC, form: "aabbccddeeff", want: "ok"},
		{typ: parquet.TypeMAC, form: "a:b:c:d:e:f", want: "ok"}, // %x groups are variable-width
		{typ: parquet.TypeMAC, form: "aabb:ccdd:eeff", want: "22P02"},
		{typ: parquet.TypeMAC, form: "aa.bb.cc.dd.ee.ff", want: "22P02"},
		{typ: parquet.TypeMAC, form: "aa:bb:cc:dd:ee", want: "22P02"},
		{typ: parquet.TypeMAC, form: "zz:bb:cc:dd:ee:ff", want: "22P02"},
		// An octet the type cannot carry is 22003, a DIFFERENT answer from a
		// spelling it cannot read.
		{typ: parquet.TypeMAC, form: "aa:bb:cc:dd:ee:100", want: "22003"},
		{typ: parquet.TypeMAC, form: "", want: "22P02", only: ntWriterNull},

		// --- UUID -----------------------------------------------------------
		{typ: parquet.TypeUUID, form: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", want: "ok"},
		{typ: parquet.TypeUUID, form: "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", want: "ok"},
		{typ: parquet.TypeUUID, form: "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", want: "ok"},
		{typ: parquet.TypeUUID, form: "a0eebc999c0b4ef8bb6d6bb9bd380a11", want: "ok"},
		// A hyphen after ANY group of four, and NOWHERE else.
		{typ: parquet.TypeUUID, form: "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11", want: "ok"},
		{typ: parquet.TypeUUID, form: "a-0eebc999c0b4ef8bb6d6bb9bd380a11", want: "22P02"},
		{typ: parquet.TypeUUID, form: "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", want: "22P02"},
		{typ: parquet.TypeUUID, form: "zzz", want: "22P02"},
		{typ: parquet.TypeUUID, form: "", want: "22P02", only: ntWriterNull},

		// --- PORT -----------------------------------------------------------
		// Numeric only: the type's documented text form names no service, and
		// `port_name()` is the function that does. The CAST's domain is the
		// int4 CARRIER (#901) while a WRITER door holds the type's own
		// 0..65535 — the one deliberate split in this table, recorded in
		// ADR-0012 and in the landing notes as a filing candidate.
		{typ: parquet.TypePort, form: "443", want: "ok"},
		{typ: parquet.TypePort, form: "0", want: "ok"},
		{typ: parquet.TypePort, form: "65535", want: "ok"},
		{typ: parquet.TypePort, form: "65536", want: "22003", only: map[ntBoundary]string{
			ntCastLit: "ok", ntCastCol: "ok", ntWhereEmpty: "ok", ntWhereFull: "ok"}},
		{typ: parquet.TypePort, form: "https", want: "22P02"},
		{typ: parquet.TypePort, form: "zzz", want: "22P02"},
		{typ: parquet.TypePort, form: "", want: "22P02", only: ntWriterNull},

		// --- PROTOCOL -------------------------------------------------------
		{typ: parquet.TypeProtocol, form: "6", want: "ok"},
		{typ: parquet.TypeProtocol, form: "17", want: "ok"},
		// The IANA NAME is the type's own text form: what protocol_name()
		// prints has to read back (#986). It is read at the doors that resolve
		// the literal against the TYPE; a WHERE comparison resolves it against
		// the DECLARED wire type instead, which is integer (OID 23) — the
		// deferral recorded in the landing notes.
		{typ: parquet.TypeProtocol, form: "udp", want: "ok", only: map[ntBoundary]string{
			ntWhereEmpty: "22P02", ntWhereFull: "22P02"}},
		{typ: parquet.TypeProtocol, form: "TCP", want: "ok", only: map[ntBoundary]string{
			ntWhereEmpty: "22P02", ntWhereFull: "22P02"}},
		{typ: parquet.TypeProtocol, form: "ipv6-icmp", want: "ok", only: map[ntBoundary]string{
			ntWhereEmpty: "22P02", ntWhereFull: "22P02"}},
		{typ: parquet.TypeProtocol, form: "255", want: "ok"},
		{typ: parquet.TypeProtocol, form: "256", want: "22003", only: map[ntBoundary]string{
			ntCastLit: "ok", ntCastCol: "ok", ntWhereEmpty: "ok", ntWhereFull: "ok"}},
		{typ: parquet.TypeProtocol, form: "zzz", want: "22P02"},
		{typ: parquet.TypeProtocol, form: "", want: "22P02", only: ntWriterNull},
	}
}

func TestEveryNetworkTypeReadsOneTextGrammarAtEveryBoundary(t *testing.T) {
	cells := ntCells()
	for _, b := range ntAllBoundaries {
		t.Run(string(b), func(t *testing.T) {
			run := ntBoundaryRunner(t, b)
			for _, c := range cells {
				name := c.typ.String() + "/" + ntFormName(c.form)
				t.Run(name, func(t *testing.T) {
					got := run(t, c.typ, c.form)
					if got != c.at(b) {
						t.Errorf("%s at %s = %s, want %s "+
							"(PostgreSQL 17.11, or the type's own documented text form)",
							ntFormName(c.form), b, got, c.at(b))
					}
				})
			}
		})
	}
}

// ntBoundaryRunner builds one door's probe. Each returns the disposition of a
// literal at that door: "ok" or the SQLSTATE of the refusal.
func ntBoundaryRunner(t *testing.T, b ntBoundary) func(*testing.T, parquet.TypeID, string) string {
	t.Helper()
	switch b {
	case ntIngester:
		return func(t *testing.T, typ parquet.TypeID, form string) string {
			t.Helper()
			db, schema := ntOpenTyped(t, typ)
			ing := db.NewIngester("t", schema, nil, ingest.DefaultConfig())
			err := ing.Ingest(context.Background(), []map[string]any{{"id": int64(1), "c": form}})
			if err == nil {
				err = ing.FlushAll(context.Background())
			}
			return ntDisposition(err)
		}
	case ntValues:
		return func(t *testing.T, typ parquet.TypeID, form string) string {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			_, err := db.Query(context.Background(),
				fmt.Sprintf("INSERT INTO t (id, c) VALUES (1, '%s')", form))
			return ntDisposition(err)
		}
	case ntInsertSel:
		return func(t *testing.T, typ parquet.TypeID, form string) string {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			_, err := db.Query(context.Background(),
				fmt.Sprintf("INSERT INTO t (c) SELECT '%s'", form))
			return ntDisposition(err)
		}
	case ntCastLit:
		return func(t *testing.T, typ parquet.TypeID, form string) string {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			_, err := db.Query(context.Background(),
				fmt.Sprintf("SELECT CAST('%s' AS %s) AS v", form, ntCastName(typ)))
			return ntDisposition(err)
		}
	case ntCastCol:
		return func(t *testing.T, typ parquet.TypeID, form string) string {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			if _, err := db.Query(context.Background(),
				fmt.Sprintf("INSERT INTO t (id, s) VALUES (1, '%s')", form)); err != nil {
				t.Fatalf("seeding the string column: %v", err)
			}
			_, err := db.Query(context.Background(),
				fmt.Sprintf("SELECT CAST(s AS %s) AS v FROM t WHERE id = 1", ntCastName(typ)))
			return ntDisposition(err)
		}
	case ntWhereEmpty, ntWhereFull:
		return func(t *testing.T, typ parquet.TypeID, form string) string {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			if b == ntWhereFull {
				// One row whose value is a spelling every type reads, so the
				// scan is non-empty and the literal's disposition cannot be
				// coming from "no row reached the comparison" (#517's shape).
				if _, err := db.Query(context.Background(),
					fmt.Sprintf("INSERT INTO t (id, c) VALUES (1, '%s')", ntSeedValue(typ))); err != nil {
					t.Fatalf("seeding: %v", err)
				}
			}
			_, err := db.Query(context.Background(),
				fmt.Sprintf("SELECT count(*) AS v FROM t WHERE c = '%s'", form))
			return ntDisposition(err)
		}
	}
	t.Fatalf("no runner for boundary %s", b)
	return nil
}

// ntOpenTyped is one table with a column of typ, a STRING column beside it for
// the CAST-over-a-column door, and an id.
func ntOpenTyped(t *testing.T, typ parquet.TypeID) (*DB, parquet.Schema) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c", Type: typ, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	return db, schema
}

func ntSeedValue(typ parquet.TypeID) string {
	switch typ {
	case parquet.TypeIPv4:
		return "10.0.0.1"
	case parquet.TypeIPv6:
		return "2001:db8::1"
	case parquet.TypeCIDR:
		return "192.168.1.0/24"
	case parquet.TypeMAC:
		return "aa:bb:cc:dd:ee:ff"
	case parquet.TypeUUID:
		return "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	case parquet.TypePort:
		return "443"
	}
	return "6"
}

// ntCastName is the SQL spelling of a CAST to this type.
func ntCastName(typ parquet.TypeID) string {
	if typ == parquet.TypeMAC {
		return "MACADDR"
	}
	return typ.String()
}

// ntDisposition reduces an error to the SQLSTATE this table compares, so a
// cell says WHICH refusal and not merely that one happened.
func ntDisposition(err error) string {
	if err == nil {
		return "ok"
	}
	if st := sqlerr.StateOf(err); st != "" && st != "XX000" {
		return st
	}
	// A writer door wraps its refusal in plain context ("schema validation:
	// …"); the SQLSTATE still travels through the wrap, so reaching here means
	// the refusal carries no class at all, which is itself the finding.
	return "no-sqlstate: " + err.Error()
}

// ntFormName keeps a subtest name printable and unique.
func ntFormName(form string) string {
	if form == "" {
		return "<empty>"
	}
	return strings.NewReplacer("/", "_", " ", "_").Replace(form)
}
