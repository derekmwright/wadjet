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
// grammar and every door that reads text reads THAT one.
//
// The table is PostgreSQL 17.11's GRAMMAR, measured from a live server into
// network_text_pgcensus_test.go — every documented family of accepted form and
// every family of rejected one, including whitespace, mask digits, leading
// zeros, trailing dots and the abbreviations. It is not a list of the
// spellings the issues named. That distinction is the round-2 review's
// structural finding: the round-1 table had no whitespace cell, no
// IPv6-abbreviation cell and no CIDR v6-mask cell, so it passed while
// `INSERT … VALUES` trimmed `' 10.0.0.1'`, an IPV6 column refused
// `'010.1.2.3'` that it accepted as `'010.1.2.3/32'`, and a CIDR column stored
// `'::1/064'` — three grammars that were not PostgreSQL's, under a claim that
// there was one that was.
//
// The EXPECTATION is derived, not listed: PostgreSQL's answer plus the type's
// own representability rule (ntExpect below). A form nobody thought of still
// gets a right expectation, which is what makes the table a grammar.

// ntDoor names one place text enters the engine.
type ntDoor string

const (
	ntIngester   ntDoor = "ingester"      // db.NewIngester — the embedded writer
	ntValues     ntDoor = "insert-values" // INSERT INTO t (c) VALUES ('…')
	ntInsertSel  ntDoor = "insert-select" // INSERT INTO t (c) SELECT '…'
	ntCopy       ntDoor = "copy"          // ConvertTextForColumn — the COPY door
	ntUpdate     ntDoor = "update"        // UPDATE t SET c = '…'
	ntCastLit    ntDoor = "cast-literal"  // CAST('…' AS T)
	ntCastCol    ntDoor = "cast-column"   // CAST(s AS T) over a STRING column
	ntWhereEmpty ntDoor = "where-empty"   // a literal in WHERE over a scan no row survives
	ntWhereFull  ntDoor = "where-rows"    // the same over a scan that has rows
)

var ntAllDoors = []ntDoor{
	ntIngester, ntValues, ntInsertSel, ntCopy, ntUpdate,
	ntCastLit, ntCastCol, ntWhereEmpty, ntWhereFull,
}

// ntWriterDoors are the doors that STORE, where the value at rest is
// observable and is asserted beside the disposition.
func ntIsWriterDoor(d ntDoor) bool {
	return d == ntIngester || d == ntValues || d == ntInsertSel || d == ntUpdate
}

// ntInetShape reads PostgreSQL's canonical `inet` output — always
// `address/bits` — into the two facts a bare-address column's representability
// rule needs.
func ntInetShape(canonical string) (family, bits int) {
	addr, mask, _ := strings.Cut(canonical, "/")
	family = 4
	if strings.ContainsRune(addr, ':') {
		family = 6
	}
	fmt.Sscanf(mask, "%d", &bits)
	return family, bits
}

// ntExpect is the RULE, and it is the whole claim: this engine answers what
// PostgreSQL answers, except where its own type has no room for the value,
// and then it says so with 0A000 rather than pretending the text is bad.
//
//   - PostgreSQL refuses → this engine refuses with the SAME class.
//   - PostgreSQL accepts and the column can hold it → a value.
//   - PostgreSQL accepts and the column cannot hold it → 0A000, one class for
//     both of its reasons (a NETWORK where the type holds a bare address, and
//     an address of the other family). ADR-0012 item 5.
func ntExpect(typ parquet.TypeID, c ntPgCell) string {
	if c.state != "" {
		return c.state
	}
	switch typ {
	case parquet.TypeIPv4:
		if family, bits := ntInetShape(c.value); family == 4 && bits == 32 {
			return "ok"
		}
		return "0A000"
	case parquet.TypeIPv6:
		family, bits := ntInetShape(c.value)
		if (family == 6 && bits == 128) || (family == 4 && bits == 32) {
			return "ok"
		}
		return "0A000"
	}
	// CIDR keeps the prefix, and MAC and UUID have no representability
	// question at all: what PostgreSQL reads, this engine reads.
	return "ok"
}

// ntExpectValue is the value at rest for the types whose storage IS the value.
// "" means "do not assert" — CIDR stores its input text verbatim (its storage
// is text, so canonicalizing at one door would split it from the others), and
// a refusal has no value.
func ntExpectValue(typ parquet.TypeID, c ntPgCell) string {
	if c.state != "" || c.value == "" {
		return ""
	}
	switch typ {
	case parquet.TypeIPv4, parquet.TypeIPv6:
		addr, _, _ := strings.Cut(c.value, "/")
		if typ == parquet.TypeIPv6 && !strings.ContainsRune(addr, ':') {
			// An IPV6 column stores a v4 address in its v4-mapped form, which
			// is what every reader renders it back as.
			return "::ffff:" + addr
		}
		return addr
	case parquet.TypeMAC, parquet.TypeUUID:
		return c.value
	}
	return ""
}

// ntNumCell is a PORT/PROTOCOL form. These two types have no PostgreSQL type
// to measure against, so the oracle is the type's own documented text form
// (docs/data-types.md): a DECIMAL number in the type's range, and for PROTOCOL
// the IANA name `protocol_name()` prints. int4's `0x1bb` / `0o17` / `1_000`
// are deliberately NOT part of it — the writer never read them and the CAST
// stopped reading them in round 2 (review NT P4).
type ntNumCell struct {
	form  string
	write string // disposition at a door that STORES (the type's own range)
	cast  string // disposition at the CAST (int4's domain, #901; ADR-0012)
	// cmp is the disposition at a COMPARISON door, where the literal is
	// resolved against the column's DECLARED WIRE TYPE — `integer`, OID 23
	// (#834) — and therefore reads int4's text grammar rather than the type's
	// own. That is why `'udp'` is 22P02 there while `CAST('udp' AS PROTOCOL)`
	// is 17, and why `'0x6'` is the other way round. One deferral, recorded in
	// ADR-0012 and filed as FC-1; "" means "same as cast".
	cmp   string
	value int32 // the stored value when write == "ok"
}

func (c ntNumCell) at(d ntDoor) string {
	switch d {
	case ntWhereEmpty, ntWhereFull:
		if c.cmp != "" {
			return c.cmp
		}
		return c.cast
	case ntCastLit, ntCastCol:
		return c.cast
	}
	return c.write
}

// The `write` / `cast` / `cmp` columns below are the three readings these two
// types get, and every difference between them is recorded rather than
// silent: the type's own form and range at a writer, int4's RANGE at a CAST
// (#901, FC-2), and int4's whole GRAMMAR at a comparison (FC-1).
//
// `'2.5'` and `'443.0'` are the numeric family's boundary, not the network
// one, and they sit here to SHOW it: a quoted fractional literal is 22P02 on
// PostgreSQL and rounds at this engine's cast and INSERT … SELECT doors —
// identically for INTEGER (`CAST('2.5' AS INTEGER)` is 3 here, measured), so
// it is int4's cell and not PORT's. Filed as FC-7.
func ntPortCells() []ntNumCell {
	return []ntNumCell{
		{"443", "ok", "ok", "", 443},
		{"0", "ok", "ok", "", 0},
		{"65535", "ok", "ok", "", 65535},
		{" 443", "ok", "ok", "", 443},
		{"443 ", "ok", "ok", "", 443},
		{"+443", "ok", "ok", "", 443},
		{"-0", "ok", "ok", "", 0},
		{"017", "ok", "ok", "", 17},
		{"65536", "22003", "ok", "", 0},
		{"-1", "22003", "ok", "", 0},
		{"3000000000", "22003", "22003", "", 0},
		{"0x1bb", "22P02", "22P02", "ok", 0},
		{"0o17", "22P02", "22P02", "ok", 0},
		{"0b101", "22P02", "22P02", "ok", 0},
		{"1_000", "22P02", "22P02", "ok", 0},
		{"443.0", "22P02", "ok", "22P02", 0},
		{"2.5", "22P02", "ok", "22P02", 0},
		{"https", "22P02", "22P02", "", 0},
		{"zzz", "22P02", "22P02", "", 0},
		{"", "22P02", "22P02", "", 0},
	}
}

func ntProtocolCells() []ntNumCell {
	return []ntNumCell{
		{"6", "ok", "ok", "", 6},
		{"17", "ok", "ok", "", 17},
		{"255", "ok", "ok", "", 255},
		{"udp", "ok", "ok", "22P02", 17},
		{"TCP", "ok", "ok", "22P02", 6},
		{"icmp", "ok", "ok", "22P02", 1},
		{"icmpv6", "ok", "ok", "22P02", 58},
		{"ipv6-icmp", "ok", "ok", "22P02", 58},
		{" udp", "ok", "ok", "22P02", 17},
		{"256", "22003", "ok", "", 0},
		{"3000000000", "22003", "22003", "", 0},
		{"0x6", "22P02", "22P02", "ok", 0},
		{"nosuchproto", "22P02", "22P02", "", 0},
		{"", "22P02", "22P02", "", 0},
	}
}

// ntCells flattens the PostgreSQL census into (type, form, expected) rows —
// the coverage table, derived from the grammar rather than listed.
type ntCell struct {
	typ   parquet.TypeID
	form  string
	want  string
	value string
	// only holds the doors whose answer differs ON PURPOSE, each with its
	// reason in the comment at the site that fills it in.
	only map[ntDoor]string
}

func (c ntCell) at(d ntDoor) string {
	if v, ok := c.only[d]; ok {
		return v
	}
	return c.want
}

func ntCells() []ntCell {
	var out []ntCell
	add := func(typ parquet.TypeID, cells []ntPgCell) {
		for _, c := range cells {
			cell := ntCell{typ: typ, form: c.form, want: ntExpect(typ, c), value: ntExpectValue(typ, c)}
			if c.form == "" {
				// The ONE door-shaped exception the types share, and it is a
				// split between two kinds of door rather than a divergence:
				// at the embedded INGESTER an empty string is ABSENCE — the
				// empty CSV or JSON field, which means NULL — while at every
				// SQL door it is a value the type cannot read, which is
				// PostgreSQL's 22P02. ADR-0012 records it.
				cell.only = map[ntDoor]string{ntIngester: "ok"}
				cell.value = ""
			}
			out = append(out, cell)
		}
	}
	// The three types that share the `inet` oracle. A wadjet CIDR column is
	// `inet` and not `cidr`: it holds host bits under a mask, which `cidr`
	// refuses outright.
	add(parquet.TypeIPv4, ntPgInet)
	add(parquet.TypeIPv6, ntPgInet)
	add(parquet.TypeCIDR, ntPgInet)
	add(parquet.TypeMAC, ntPgMacaddr)
	add(parquet.TypeUUID, ntPgUuid)
	for _, c := range ntPortCells() {
		out = append(out, ntNumRow(parquet.TypePort, c))
	}
	for _, c := range ntProtocolCells() {
		out = append(out, ntNumRow(parquet.TypeProtocol, c))
	}
	return out
}

func ntNumRow(typ parquet.TypeID, c ntNumCell) ntCell {
	cell := ntCell{typ: typ, form: c.form, want: c.write, only: map[ntDoor]string{}}
	if c.write == "ok" {
		cell.value = fmt.Sprintf("%d", c.value)
	}
	for _, d := range ntAllDoors {
		if c.at(d) != c.write {
			cell.only[d] = c.at(d)
		}
	}
	// INSERT … SELECT carries an unknown-typed literal through the ASSIGNMENT
	// converter, which reaches the same decimal reader the CAST does — so the
	// fractional pair answers there too. Same cell as FC-7, one door over.
	if c.form == "443.0" || c.form == "2.5" {
		cell.only[ntInsertSel] = "ok"
	}
	if c.form == "" {
		cell.only = map[ntDoor]string{ntIngester: "ok"}
		cell.value = ""
	}
	return cell
}

func TestEveryNetworkTypeReadsOneTextGrammarAtEveryBoundary(t *testing.T) {
	cells := ntCells()
	if len(cells) < 320 {
		t.Fatalf("the census has %d rows; the grammar tables are not loaded", len(cells))
	}
	for _, d := range ntAllDoors {
		t.Run(string(d), func(t *testing.T) {
			run := ntDoorRunner(t, d)
			for _, c := range cells {
				t.Run(c.typ.String()+"/"+ntFormName(c.form), func(t *testing.T) {
					got, value := run(t, c.typ, c.form)
					if got != c.at(d) {
						t.Errorf("%s at %s = %s, want %s (PostgreSQL 17.11, or the "+
							"type's own documented text form)", ntFormName(c.form), d, got, c.at(d))
						return
					}
					// The VALUE at rest, where the storage IS the value: a
					// door that TRIMS or canonicalizes differently from the
					// others answers the same disposition and stores a
					// different thing (review NT B1).
					if got == "ok" && c.value != "" && ntIsWriterDoor(d) && value != c.value {
						t.Errorf("%s at %s stored %q, want %q", ntFormName(c.form), d, value, c.value)
					}
				})
			}
		})
	}
}

// ntDoorRunner builds one door's probe: it reports the disposition ("ok" or
// the SQLSTATE) and, at a door that stores, the value read back.
func ntDoorRunner(t *testing.T, d ntDoor) func(*testing.T, parquet.TypeID, string) (string, string) {
	t.Helper()
	ctx := context.Background()
	switch d {
	case ntIngester:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, schema := ntOpenTyped(t, typ)
			ing := db.NewIngester("t", schema, nil, ingest.DefaultConfig())
			err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "c": ntBox(typ, form)}})
			if err == nil {
				err = ing.FlushAll(ctx)
			}
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntReadBack(t, db, typ)
		}
	case ntValues:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			_, err := db.Query(ctx, "INSERT INTO t (id, c) VALUES (1, "+ntQuote(form)+")")
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntReadBack(t, db, typ)
		}
	case ntInsertSel:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			_, err := db.Query(ctx, "INSERT INTO t (c) SELECT "+ntQuote(form))
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntReadBack(t, db, typ)
		}
	case ntCopy:
		// The COPY door is a function, not a statement: pgwire's COPY FROM
		// calls ConvertTextForColumn per field. The round-1 notes excluded it
		// as "the same boundary as the embedded ingester" — it is not, and
		// the two disagreed about every whitespace-padded literal.
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			v, err := ConvertTextForColumn(form, parquet.Column{Name: "c", Type: typ, Nullable: true})
			if err != nil {
				return ntDisposition(err), ""
			}
			db, schema := ntOpenTyped(t, typ)
			ing := db.NewIngester("t", schema, nil, ingest.DefaultConfig())
			if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "c": v}}); err != nil {
				return ntDisposition(err), ""
			}
			if err := ing.FlushAll(ctx); err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntReadBack(t, db, typ)
		}
	case ntUpdate:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			if _, err := db.Query(ctx, "INSERT INTO t (id, c) VALUES (1, "+
				ntQuote(ntSeedValue(typ))+")"); err != nil {
				t.Fatalf("seeding: %v", err)
			}
			_, err := db.Query(ctx, "UPDATE t SET c = "+ntQuote(form)+" WHERE id = 1")
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntReadBack(t, db, typ)
		}
	case ntCastLit:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			res, err := db.Query(ctx, "SELECT CAST("+ntQuote(form)+" AS "+ntCastName(typ)+") AS v")
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntCellText(res)
		}
	case ntCastCol:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			if _, err := db.Query(ctx, "INSERT INTO t (id, s) VALUES (1, "+ntQuote(form)+")"); err != nil {
				t.Fatalf("seeding the string column: %v", err)
			}
			res, err := db.Query(ctx,
				"SELECT CAST(s AS "+ntCastName(typ)+") AS v FROM t WHERE id = 1")
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ntCellText(res)
		}
	case ntWhereEmpty, ntWhereFull:
		return func(t *testing.T, typ parquet.TypeID, form string) (string, string) {
			t.Helper()
			db, _ := ntOpenTyped(t, typ)
			if d == ntWhereFull {
				// One row whose value every type reads, so the scan is
				// non-empty and a literal's disposition cannot be coming from
				// "no row reached the comparison" (#517's shape).
				if _, err := db.Query(ctx, "INSERT INTO t (id, c) VALUES (1, "+
					ntQuote(ntSeedValue(typ))+")"); err != nil {
					t.Fatalf("seeding: %v", err)
				}
			}
			_, err := db.Query(ctx, "SELECT count(*) AS v FROM t WHERE c = "+ntQuote(form))
			if err != nil {
				return ntDisposition(err), ""
			}
			return "ok", ""
		}
	}
	t.Fatalf("no runner for door %s", d)
	return nil
}

// ntBox is the Go value the embedded ingester is handed. PORT and PROTOCOL are
// int4-backed and their map API takes a number OR the type's text; every cell
// here is text, which is the form a CSV or JSON source supplies.
func ntBox(typ parquet.TypeID, form string) any { return form }

// ntQuote is a SQL string literal. No corpus form contains an apostrophe; the
// doubling is here so that adding one cannot silently change the statement.
func ntQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func ntReadBack(t *testing.T, db *DB, typ parquet.TypeID) string {
	t.Helper()
	res, err := db.Query(context.Background(), "SELECT c AS v FROM t WHERE c IS NOT NULL")
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	return ntCellText(res)
}

func ntCellText(res *QueryResult) string {
	if res == nil || len(res.Rows) == 0 {
		return ""
	}
	v := res.Rows[0]["v"]
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
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
	// A refusal that carries no class at all is itself the finding.
	return "no-sqlstate: " + err.Error()
}

// ntFormName keeps a subtest name printable and unique.
func ntFormName(form string) string {
	if form == "" {
		return "<empty>"
	}
	return strings.NewReplacer("/", "_", " ", "_SP_", "\t", "_TAB_").Replace(form)
}
