// Package typematrix is the fixture and query corpus for the type-coverage
// gates: one table carrying all 22 column types, and a generated corpus that
// pushes every type through every consumer that can retain, re-key, re-order
// or re-encode a value.
//
// Why it exists. Every differential corpus in this repo is built on three
// storage types — Int32, Float64 and String (TPC-H schema.go; ClickBench adds
// Int64 and Date). So an entire class of wrong-answer defects is structurally
// invisible: no gate can see a bug that needs a BYTES, IPv4, UUID, DECIMAL or
// nested column to fire. That is not hypothetical. (*Vector).GetValue's
// TypeBytes arm returns a slice ALIASING the column arena, so MIN_BY over a
// BYTES column answers with whatever the pool wrote into those bytes next —
// a silent wrong answer that shipped through TPC-H, ClickBench, the DuckDB
// fingerprint corpus, the PostgreSQL oracle, the two-path suite and the shape
// fuzzer, because not one of them has a top-level BYTES column.
//
// The corpus is GENERATED from a column table rather than hand-written, so a
// 23rd type is covered by adding one row to Columns() instead of by
// remembering to write ten queries.
//
// This package holds no assertions. Three gates consume it, each supplying its
// own reference:
//
//	wadjet.TestTypeMatrixBatchReuse             — poisoned pool vs clean pool
//	wadjet.TestTypeMatrixOptimizationInvariance — each kill switch off vs on
//	coordinator.TestTypeMatrixTwoPath           — stage DAG vs single process
package typematrix

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Table is the fixture table name, Dim the small dimension table the join
// entries build against.
const (
	// Table carries the 18 flat types. Nested carries the four container
	// types. They are SEPARATE tables because readBatchDirect
	// (internal/planner/physical/util.go:151) decides between the columnar
	// decoder and the row fallback on the WHOLE table schema, not on the
	// columns a query projects: one ARRAY column anywhere in a table forces
	// every query on it onto the row reader, which mints unpooled batches.
	// Merging the two would silently disable batch reuse — and therefore the
	// batch-reuse gate — for all 22 types at once.
	Table  = "typemx"
	Nested = "typemx_nested"
	Dim    = "typemx_dim"
)

// Rows is the fixture row count. Past 2×DefaultBatchSize so the scan recycles
// pooled batches several times over: a retention defect that only fires on the
// second batch is the whole point of this corpus.
const Rows = 5000

// RowGroup is the parquet row-group size the loaders should use. Deliberately
// not a multiple of the batch size, so batch boundaries and row-group
// boundaries fall in different places.
const RowGroup = 1100

// Col describes one typed column and what the generator may do with it.
type Col struct {
	Name string
	Type parquet.TypeID
	// Flat marks a scalar type: one that can be a GROUP BY key, a sort key,
	// a join key and a DISTINCT value. ARRAY/ROW/MAP/VECTOR are not.
	Flat bool
	// Wide marks a column with enough distinct values that an equi-self-join
	// on it stays roughly 1:1 instead of exploding into a cross product.
	Wide bool
	// Ordered marks a type MIN/MAX and ORDER BY are meaningful over. Every
	// Flat type qualifies today; the field exists so a future opaque scalar
	// can opt out without opting out of grouping.
	Ordered bool
}

// TableOf reports which fixture table carries this column.
func (c Col) TableOf() string {
	if c.Flat {
		return Table
	}
	return Nested
}

// Columns is the type matrix: one column per wadjet type, all 22, plus the
// one SHAPE that takes a different reader from the type it shares.
//
// Each is nullable and each carries a NULL every so often, at a stride coprime
// with both the batch size and the row-group size so nulls land at interior
// AND boundary positions — the offsets-on-NULL corruption class only fires at
// a boundary.
//
// c_rownest is a second ROW, and it is here because a ROW's field types decide
// which READER answers for it: a ROW of primitive leaves is addressed by leaf
// path in the native columnar decoder, while a ROW whose fields are themselves
// containers has no such path and routes to the row reader
// (scan.HasUnsupportedColumnarTypes, #448). One entry in this list is the
// difference between the two paths being compared and one of them never being
// exercised — the columnar arm read such a field back as all-NULL for as long
// as no corpus column had the shape.
func Columns() []Col {
	return []Col{
		{Name: "c_bool", Type: parquet.TypeBool, Flat: true, Ordered: true},
		{Name: "c_i32", Type: parquet.TypeInt32, Flat: true, Wide: true, Ordered: true},
		{Name: "c_i64", Type: parquet.TypeInt64, Flat: true, Wide: true, Ordered: true},
		{Name: "c_f32", Type: parquet.TypeFloat32, Flat: true, Wide: true, Ordered: true},
		{Name: "c_f64", Type: parquet.TypeFloat64, Flat: true, Wide: true, Ordered: true},
		{Name: "c_str", Type: parquet.TypeString, Flat: true, Wide: true, Ordered: true},
		{Name: "c_bytes", Type: parquet.TypeBytes, Flat: true, Wide: true, Ordered: true},
		{Name: "c_ts", Type: parquet.TypeTimestamp, Flat: true, Wide: true, Ordered: true},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Flat: true, Wide: true, Ordered: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Flat: true, Wide: true, Ordered: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Flat: true, Ordered: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Flat: true, Wide: true, Ordered: true},
		{Name: "c_port", Type: parquet.TypePort, Flat: true, Ordered: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Flat: true, Ordered: true},
		{Name: "c_dur", Type: parquet.TypeDuration, Flat: true, Wide: true, Ordered: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Flat: true, Wide: true, Ordered: true},
		{Name: "c_date", Type: parquet.TypeDate, Flat: true, Ordered: true},
		{Name: "c_dec", Type: parquet.TypeDecimal, Flat: true, Wide: true, Ordered: true},
		{Name: "c_arr", Type: parquet.TypeArray},
		{Name: "c_row", Type: parquet.TypeRow},
		{Name: "c_rownest", Type: parquet.TypeRow},
		{Name: "c_map", Type: parquet.TypeMap},
		{Name: "c_vec", Type: parquet.TypeVector},
	}
}

// Schema is the flat table's schema: an id, a low-cardinality group key, two
// numeric measures for the statistical aggregates, and the 18 flat types.
func Schema() parquet.Schema { return schemaFor(true) }

// NestedSchema is the nested table's schema: the same id and group key plus
// the four container types.
func NestedSchema() parquet.Schema { return schemaFor(false) }

func schemaFor(flat bool) parquet.Schema {
	cols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
	}
	if flat {
		cols = append(cols,
			parquet.Column{Name: "m1", Type: parquet.TypeFloat64, Nullable: true},
			parquet.Column{Name: "m2", Type: parquet.TypeFloat64, Nullable: true})
	}
	for _, c := range Columns() {
		if c.Flat != flat {
			continue
		}
		pc := parquet.Column{Name: c.Name, Type: c.Type, Nullable: true}
		switch c.Type {
		case parquet.TypeDecimal:
			pc.Precision, pc.Scale = 18, 4
		case parquet.TypeArray:
			pc.ElementType = &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}
		case parquet.TypeRow:
			pc.Fields = rowFields(c.Name)
		case parquet.TypeMap:
			// MAP is ARRAY(ROW("key","value")) in the storage layer, so its
			// ElementType is the entry ROW — the shape ResolveColumn builds
			// for "MAP(STRING, INT64)".
			pc.ElementType = &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}
		case parquet.TypeVector:
			pc.Dimension = 4
		}
		cols = append(cols, pc)
	}
	return parquet.Schema{Columns: cols}
}

// rowFields declares the two ROW columns' field lists.
//
// c_row is a struct of primitive LEAVES, which the native columnar reader
// addresses one leaf chunk per field. c_rownest is a struct whose fields are
// themselves containers, which has no such addressing and takes the row
// reader on every path (#448). The gates compare the two paths' answers, so
// which reader a column reaches is part of what the fixture has to cover.
func rowFields(name string) []parquet.Column {
	if name != "c_rownest" {
		return []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}
	}
	return []parquet.Column{
		{Name: "s", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "x", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "l", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "m", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
	}
}

// DimSchema is the join partner: one row per group key.
func DimSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
	}}
}

// Groups is the number of distinct non-NULL group keys.
const Groups = 7

// Data builds the flat table's rows deterministically. Values are derived from
// the row index so the expected answer of any query is recomputable in Go, and
// so the fixture is identical in every process that loads it.
func Data(n int) []map[string]any { return dataFor(n, true) }

// NestedData builds the nested table's rows, over the same row indices, so a
// join between the two tables on id lines up.
func NestedData(n int) []map[string]any { return dataFor(n, false) }

// dataFor projects allData onto one table's schema.
func dataFor(n int, flat bool) []map[string]any {
	all := allData(n)
	schema := schemaFor(flat)
	out := make([]map[string]any, n)
	for i, src := range all {
		row := make(map[string]any, len(schema.Columns))
		for _, c := range schema.Columns {
			row[c.Name] = src[c.Name]
		}
		out[i] = row
	}
	return out
}

func allData(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		id := int64(i)
		r := map[string]any{"id": id}
		r["g"] = orNull(i, 13, int32(i%Groups))
		r["m1"] = orNull(i, 17, float64(i%97)+0.5)
		r["m2"] = orNull(i, 19, float64((i*7)%89)-3.25)

		// Each column nulls on its own stride so no two columns are NULL in
		// lockstep — a group key and its measure nulling together would hide
		// a null-key defect behind an empty aggregate.
		r["c_bool"] = orNull(i, 23, i%3 == 0)
		r["c_i32"] = orNull(i, 29, int32(i*3))
		r["c_i64"] = orNull(i, 31, int64(i)*1_000_003)
		r["c_f32"] = orNull(i, 37, float32(i)/7)
		r["c_f64"] = orNull(i, 41, float64(i)/3)
		r["c_str"] = orNull(i, 43, fmt.Sprintf("s-%06d", i))
		// BYTES values are DISTINCT PER ROW and long enough to span several
		// arena slots. A per-row-unique value is what makes an aliasing read
		// legible: the wrong answer is a different row's bytes, not a
		// coincidentally equal one.
		r["c_bytes"] = orNull(i, 47, []byte(fmt.Sprintf("bytes-%06d-%s", i, strings.Repeat("x", i%11))))
		r["c_ts"] = orNull(i, 53, int64(1_700_000_000_000+int64(i)*61_000))
		r["c_ipv4"] = orNull(i, 59, fmt.Sprintf("10.%d.%d.%d", (i/65536)%256, (i/256)%256, i%256))
		r["c_ipv6"] = orNull(i, 61, fmt.Sprintf("2001:db8::%x", i))
		r["c_cidr"] = orNull(i, 67, cidrValue(i))
		r["c_mac"] = orNull(i, 71, fmt.Sprintf("aa:bb:cc:%02x:%02x:%02x", (i/65536)%256, (i/256)%256, i%256))
		r["c_port"] = orNull(i, 73, int32(1024+i%40000))
		r["c_proto"] = orNull(i, 79, int32(i%256))
		r["c_dur"] = orNull(i, 83, int64(i)*1_000_000)
		r["c_uuid"] = orNull(i, 89, fmt.Sprintf("00000000-0000-4000-8000-%012x", i))
		r["c_date"] = orNull(i, 97, fmt.Sprintf("20%02d-%02d-%02d", 10+i%15, 1+i%12, 1+i%28))
		r["c_dec"] = orNull(i, 101, float64(i)+0.0001*float64(i%9973))
		r["c_arr"] = orNull(i, 103, arrayValue(i))
		r["c_row"] = orNull(i, 107, rowValue(i))
		r["c_rownest"] = orNull(i, 127, nestedRowValue(i))
		r["c_map"] = orNull(i, 109, map[string]any{fmt.Sprintf("k%d", i%5): int64(i)})
		r["c_vec"] = orNull(i, 113, []float32{float32(i), float32(i) + 0.5, -float32(i), 0.25})
		rows[i] = r
	}
	return rows
}

// cidrValue is row i's CIDR text. It cycles through four shapes on purpose:
// a CANONICAL /24, a HOST-BEARING /24, a host-bearing /8 and a /32 host
// route.
//
// The fixture used to be canonical /24s alone ("192.168.<i%256>.0/24"), which
// made three of the four things PostgreSQL's inet order decides invisible to
// this corpus: the mask length never varied, so "the shorter mask sorts
// first" was never exercised; the host bits were always zero, so a key built
// from the MASKED network alone — which is what #492's first CidrSortKey did
// — could throw them away and still agree with itself; and no two rows shared
// a network, so `= '10.0.0.1/8'` could not answer a DIFFERENT address's row.
// Wadjet's CIDR column is unvalidated text (internal/storage/ingest) and
// host-bearing prefixes are ordinary in the network data this type exists
// for, so the canonical-only fixture was not the conservative choice.
//
// id=700 lands on case 0 and keeps its old value, "192.168.188.0/24", so
// networkLit's equality literal is unchanged.
//
// id=298 spells id=299's own /32 address BARE (no "/32") instead of taking
// its own case's shape. Both land inside union_c_cidr's `WHERE id < 300` arm,
// so a query that unions the fixture against itself holds one address two
// ways: PostgreSQL's inet calls a bare address and its own /32 host route ONE
// value (`'10.0.0.1' = '10.0.0.1/32'`), and text order calls them two
// distinct strings. That pair is what made #546 visible: the single-process
// set operation's dedup (`rowHashKey`, keyed on the boxed value's raw text)
// and the stage DAG's (a `GroupByAll` aggregate keyed through
// `kernel.CidrOrderKey`, #520) answered the identical UNION DIFFERENTLY. Both
// now key by inet (`physical.keyValueText`'s TypeCIDR arm), so these two rows
// are what keeps that agreement gated rather than what records its absence.
func cidrValue(i int) string {
	if i == 298 {
		return strings.TrimSuffix(cidrValue(299), "/32")
	}
	switch i % 4 {
	case 0:
		return fmt.Sprintf("192.168.%d.0/24", i%256)
	case 1:
		// (i-1)%256, not i%256: case 0 runs on i%4==0 and this on i%4==1, so
		// sharing the third octet with the PREVIOUS row is what puts a
		// host-bearing address INSIDE a /24 the fixture also holds as its own
		// network row. Without that overlap no query can tell a masked key
		// from a full one — the two spellings would never meet.
		return fmt.Sprintf("192.168.%d.%d/24", (i-1)%256, 1+i%200)
	case 2:
		return fmt.Sprintf("10.%d.%d.%d/8", (i/256)%256, (i/16)%256, i%256)
	default:
		return fmt.Sprintf("172.16.%d.%d/32", (i/256)%256, i%256)
	}
}

// rowValue returns a PRESENT ROW whose fields cycle through present,
// first-NULL, last-NULL and both-NULL.
//
// A present ROW with a NULL field is a value of its own, and the fixture used
// to have none: c_row was all-present or NULL, so the two read paths boxed it
// the same way by accident and #449 — the row reader omitting the null field's
// key where the columnar reader carries it — could not be seen by any gate.
// The stride is 7, coprime with both the batch size and the row-group size, so
// each variant lands at interior and boundary rows.
func rowValue(i int) any {
	v := map[string]any{"a": fmt.Sprintf("r-%05d", i), "b": int64(i) * 11}
	switch i % 7 {
	case 1:
		v["a"] = nil
	case 2:
		v["b"] = nil
	case 3:
		v["a"], v["b"] = nil, nil
	}
	return v
}

// nestedRowValue returns a PRESENT ROW whose three fields are containers: a
// ROW, an ARRAY and a MAP. This is the shape the native columnar reader
// cannot address by leaf path and must hand to the row reader (#448); before
// that routing existed the columnar arm answered every one of these fields
// with NULL and no error.
//
// The cycle covers what a container distinguishes: populated, EMPTY (not the
// same value as NULL), NULL inside a present struct, and a NULL entry inside
// a present container.
func nestedRowValue(i int) any {
	v := map[string]any{
		"s": map[string]any{"x": int64(i)},
		"l": []any{fmt.Sprintf("n%05d", i)},
		"m": map[string]any{fmt.Sprintf("k%d", i%3): int64(i)},
	}
	switch i % 7 {
	case 1: // empty, which is not NULL
		v["s"] = map[string]any{"x": nil}
		v["l"] = []any{}
		v["m"] = map[string]any{}
	case 2: // NULL containers inside a PRESENT struct
		v["s"], v["l"], v["m"] = nil, nil, nil
	case 3: // a NULL element and a NULL map value
		v["l"] = []any{nil, fmt.Sprintf("n%05d", i)}
		v["m"] = map[string]any{"k": nil}
	}
	return v
}

// arrayValue returns an ARRAY(STRING) whose length cycles 0..2 so a
// zero-length array and a NULL array are both present and must stay distinct.
func arrayValue(i int) any {
	n := i % 3
	out := make([]any, n)
	for j := 0; j < n; j++ {
		out[j] = fmt.Sprintf("a%05d-%d", i, j)
	}
	return out
}

// DimData is one row per group key plus one unmatched key, so the outer-join
// entries have an unmatched side to null-pad.
func DimData() []map[string]any {
	out := make([]map[string]any, 0, Groups+1)
	for k := 0; k < Groups+1; k++ {
		out = append(out, map[string]any{"k": int32(k), "label": fmt.Sprintf("grp-%d", k)})
	}
	return out
}

// orNull returns v, or nil every stride-th row.
func orNull(i, stride int, v any) any {
	if i%stride == stride-1 {
		return nil
	}
	return v
}

// Query is one corpus entry.
type Query struct {
	// Name identifies the entry; it is the subtest name.
	Name string
	SQL  string
	// Mode is how strictly two arms' results are compared.
	Mode oracle.CmpMode
	// Col names the type column the entry targets, "" for the entries that
	// target none. Used only for reporting which types a run exercised.
	Col string
	// KnownBug names the open issue for a divergence a gate found and that is
	// not yet fixed. The comparison STILL RUNS: the arm's runner logs the
	// divergence instead of failing, and FAILS when the two arms start
	// agreeing — deleting the field is the whole of "the fix landed"
	// (ADR-0013 §Pins). It is not a skip.
	//
	// Pins are per-GATE because the same query can diverge on one arm and
	// agree on another, so each gate carries its own map (see Pins below)
	// rather than a field here.
	_ struct{}
}

// Pin records one gate's known divergence on one corpus entry.
type Pin struct {
	// Issue is the tracking issue, e.g. "#391".
	Issue string
	// Reason says what diverges and why it is not fixed here.
	Reason string
	// GatedBy names the gate that enforces the "this bug is fixed, delete the
	// pin" half of the ratchet for this issue, when THIS gate cannot.
	//
	// Some defects are nondeterministic by nature: #391 answers with whatever
	// the allocator happened to write over a freed arena, so an arm that does
	// not force the reuse sees it only sometimes. Demanding a divergence every
	// run from such an arm makes the suite flap; dropping the demand silently
	// would let a fixed bug keep its exemption forever. Naming the arm that
	// DOES force it keeps the ratchet exact and keeps it in one place. A pin
	// with GatedBy set is exempt from this arm's must-still-diverge check and
	// from nothing else — the "matches no corpus entry" check still applies.
	GatedBy string
}

// networkFuncArg maps each network-native column to a representative scalar
// function that reads it as network-address TEXT (internal/engine/expr's
// networkTextFuncs table), for the function-argument consumer class below.
// Every one of these is a "(guard)" case in expr/network_typematrix_test.go
// and wadjet/network_typed_column_args_test.go — proven correct at the expr
// layer already — so what THIS corpus checks is new: that the single-
// process and stage-DAG engines still agree once the value has gone through
// a scalar function.
var networkFuncArg = map[string]string{
	"c_ipv4":  "ip_to_string",
	"c_ipv6":  "ip_to_string",
	"c_cidr":  "network_address",
	"c_mac":   "mac_to_string",
	"c_port":  "port_name",
	"c_proto": "protocol_name",
}

// networkLit gives each network-native column a literal EQUAL to id=700's
// value under allData's per-column formula (id=700 lands on a non-NULL
// stride position for all six: 700 mod 59/61/67/71/73/79 never hits the
// stride's NULL slot). EQUALITY only. ORDERING is a separate consumer class
// (networkOrdLit, below): an ORDERING literal comparison (<, >) against
// TypeIPv6 or TypeCIDR used to compare the column's rendered TEXT lexically
// instead of the address's numeric/structural order, and disagreed outright
// between this expr path and the stage DAG (#492, fixed) — this corpus now
// gates it instead of skipping it as a known bug.
//
// c_port/c_proto are UNQUOTED (a bare numeric literal, `c_port = 1724`),
// not a quoted string like the other four: both are Int32-backed, and a
// QUOTED numeric literal against an Int32/Int64/PORT/PROTOCOL column hits a
// different, pre-existing bug (#493) — kernel.toInt64's string case
// calls parseTimestampString, not a plain integer parse, so `c_port =
// '1724'` silently compares against 0 on one engine and something else
// again on the other. That is a real defect (filed, not fixed here — out
// of this corpus's territory), not a reason to leave PORT/PROTOCOL out of
// this consumer class: the unquoted form is how a `PORT = <int>` /
// `PROTOCOL = <int>` predicate is actually written.
var networkLit = map[string]string{
	"c_ipv4":  "'10.0.2.188'",
	"c_ipv6":  "'2001:db8::2bc'",
	"c_cidr":  "'192.168.188.0/24'",
	"c_mac":   "'aa:bb:cc:00:02:bc'",
	"c_port":  "1724",
	"c_proto": "188",
}

// networkOrdLit gives c_ipv6 and c_cidr an ORDERING literal (`<`). Only
// these two: IPv4/MAC/PORT/PROTOCOL already had a correct typed ordering
// comparator before #492 (tryNetworkLit's int64 encodings), so their
// ordering was never at risk of the lexical-text divergence this entry
// exists to gate. The literal is the same one networkLit uses for equality,
// which under allData's per-column formula ("2001:db8::" + hex(i),
// "192.168." + (i%256) + ".0/24") spans a wide range of hex-digit-count and
// decimal-digit-count boundaries across the fixture's 5000 rows — exactly
// where lexical and numeric/structural order disagree.
var networkOrdLit = map[string]string{
	"c_ipv6": networkLit["c_ipv6"],
	"c_cidr": networkLit["c_cidr"],
}

// networkExtraLit adds the literal SHAPES the equality/ordering pair above
// cannot reach, one per column, as (suffix, operator, literal) triples.
//
// Each is a shape #492's first fix answered differently on the two engines,
// and none of them is exotic:
//
//   - c_cidr against a BARE address. PostgreSQL's inet reads "172.16.2.187"
//     as "172.16.2.187/32" and so does wadjet now; the kernel used to answer
//     an unparseable-literal sentinel that matched NOTHING while the
//     row-at-a-time path compared the text, so `WHERE c_cidr = '<a bare
//     address the fixture holds>'` answered zero rows on one engine and the
//     row on the other. `>=` is the ordering half of the same literal.
//   - c_cidr against a HOST-BEARING prefix. Two rows share the /24 network
//     and differ only in host bits, which the masked-network key erased.
//   - c_ipv6 against a v4-shaped literal. Different FAMILIES: PostgreSQL puts
//     every v4 address below every v6 one, so every non-NULL row is `>` it.
//     The kernel read the literal as its v4-MAPPED v6 bytes (mid-range) and
//     the expr path fell through to a lexical text compare — two engines,
//     two answers, neither PostgreSQL's.
//
// The literal shape that is NOT here is the one that must ERROR — `c_cidr <>
// 'garbage'`, which raised zero rows through the scan and every row through
// the row evaluator before #492's second pass. A corpus entry cannot carry it:
// oracle.Run fails the whole run on any query error, so an entry whose correct
// answer IS an error would read as a broken corpus. It lives in
// wadjet.TestNonAddressLiteralAgainstACidrColumnIsAQueryError instead, where
// both sites are asserted to raise the same SQLSTATE.
var networkExtraLit = []struct{ col, suffix, op, lit string }{
	{"c_cidr", "bare", "=", "'172.16.2.187'"},
	{"c_cidr", "bare_ord", ">=", "'172.16.2.187'"},
	{"c_cidr", "hostbits", "=", "'192.168.188.190/24'"},
	{"c_cidr", "hostbits_ord", "<", "'192.168.188.190/24'"},
	{"c_ipv6", "xfamily", ">", "'10.0.0.2'"},
}

// Corpus generates the query corpus: per type-column templates plus the
// entries that need no particular column.
//
// Ordering is deterministic (Columns() order, then template order) so a
// failing entry name is stable across runs and across processes.
func Corpus() []Query {
	var out []Query
	add := func(name, sql string, mode oracle.CmpMode, col string) {
		out = append(out, Query{Name: name, SQL: sql, Mode: mode, Col: col})
	}

	for _, c := range Columns() {
		n := c.Name
		tbl := c.TableOf()
		// The value-boxing aggregates. MIN_BY/MAX_BY retain a boxed value
		// from the input batch across every later batch, which is the
		// retention path GetValue's aliasing arm sits on. Applied to EVERY
		// type, nested included: a nested column whose leaf is BYTES aliases
		// through the same recursion.
		add("minby_"+n,
			fmt.Sprintf(`SELECT g, MIN_BY(%s, id) AS v FROM %s GROUP BY g ORDER BY g`, n, tbl),
			oracle.CmpOrdered, n)
		add("maxby_"+n,
			fmt.Sprintf(`SELECT g, MAX_BY(%s, id) AS v FROM %s GROUP BY g ORDER BY g`, n, tbl),
			oracle.CmpOrdered, n)
		// Scalar (no GROUP BY) form: a different code path — whole-input
		// aggregation, which the planner routes separately (agg_whole_input.go).
		add("minby_scalar_"+n,
			fmt.Sprintf(`SELECT MIN_BY(%s, id) AS v, MAX_BY(%s, id) AS w FROM %s`, n, n, tbl),
			oracle.CmpUnordered, n)
		// Projection passthrough over a filter: the value reaches the result
		// assembler through a selection vector rather than a copy.
		add("project_"+n,
			fmt.Sprintf(`SELECT id, %s AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, n, tbl),
			oracle.CmpOrdered, n)
		// Window frame value functions retain a boxed value per partition.
		add("window_"+n,
			fmt.Sprintf(`SELECT id, FIRST_VALUE(%s) OVER (PARTITION BY g ORDER BY id) AS f, `+
				`LAST_VALUE(%s) OVER (PARTITION BY g ORDER BY id) AS l `+
				`FROM %s WHERE id < 400 ORDER BY id`, n, n, tbl),
			oracle.CmpOrdered, n)
		// Windowed MIN/MAX. Unlike the value functions above, the answer is
		// CHOSEN by a comparison rather than lifted by position, so the
		// entry gates two things at once: the output column's declared type
		// (the input's own, with DECIMAL's scale and a container's shape
		// riding along) and the ORDER the choice is made in.
		//
		// Until #569 twelve of the twenty-two types declined to declare and
		// kept the planner's FLOAT64, which did not degrade the answer — it
		// FAILED the query, "cannot store string into FLOAT64 vector", for
		// CIDR/UUID/IPV6/IPV4/MAC/DECIMAL/BYTES/BOOL and all four
		// containers, while the plain aggregate over the identical column
		// answered correctly. A corpus entry could not have shown that
		// before, because none of the entries above asks a window for a
		// value it has to CHOOSE.
		//
		// Two shapes, because they are two evaluators and the comparison
		// reaches the value through a different door in each:
		//
		//   whole-partition  the partition-at-a-time columnar path, which
		//                    compares kernel.CompareValuesAt on the vector —
		//                    and, on the DAG, a hash-partitioned window
		//                    stage (#349) with its own declared schema.
		//   OVER ()          the streaming empty-PARTITION-BY evaluator
		//                    (window_global.go), which carries its running
		//                    extreme as a BOX across batches and so compares
		//                    through newBoxedCompare — the site where a
		//                    network type's DISPLAY text is not its address
		//                    order (#520/#565's shape, one consumer over).
		//
		// A SLIDING frame is deliberately not here. It exercises the deque's
		// EVICTION, which is worth gating, but it does so identically on
		// every arm this corpus feeds — the DAG's window stage runs the same
		// operator — and each entry here costs a distributed query per type
		// in coordinator.TestTypeMatrixTwoPath. It is gated instead in
		// exec.TestWindowExternalMinMaxEveryType, which compares the
		// in-memory and SPILLED evaluators against each other over all 22
		// types and asserts the spill actually happened.
		add("windowminmax_"+n,
			fmt.Sprintf(`SELECT id, MIN(%s) OVER (PARTITION BY g ORDER BY id `+
				`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo, `+
				`MAX(%s) OVER (PARTITION BY g ORDER BY id `+
				`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi `+
				`FROM %s WHERE id < 400 ORDER BY id`, n, n, tbl),
			oracle.CmpOrdered, n)
		add("windowminmax_global_"+n,
			fmt.Sprintf(`SELECT id, MIN(%s) OVER () AS lo, MAX(%s) OVER () AS hi `+
				`FROM %s WHERE id < 400 ORDER BY id`, n, n, tbl),
			oracle.CmpOrdered, n)

		// Function-argument, CAST-AS-STRING and literal-comparison consumer
		// classes for the six network-native types — the routes #484/#485
		// special-cased (ColRef/FuncCall/Cast/Cmp in internal/engine/expr)
		// and feedback_gate_coverage_of_type_system.md flagged as absent
		// from this corpus: none of the other templates above route a
		// network column through a function call, a STRING cast, or a
		// literal comparison, so a two-path divergence in any of those
		// three would have been invisible here. See networkFuncArg/
		// networkLit for why EQUALITY is the only literal-comparison shape
		// every network type gets; networkOrdLit adds ORDERING for the two
		// types (#492) whose ordering comparator needed a real fix.
		if fn, ok := networkFuncArg[n]; ok {
			add("funcarg_"+n,
				fmt.Sprintf(`SELECT id, %s(%s) AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, fn, n, tbl),
				oracle.CmpOrdered, n)
			add("caststr_"+n,
				fmt.Sprintf(`SELECT id, CAST(%s AS STRING) AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, n, tbl),
				oracle.CmpOrdered, n)
			add("litcmp_"+n,
				fmt.Sprintf(`SELECT id, %s AS v FROM %s WHERE %s = %s ORDER BY id`, n, tbl, n, networkLit[n]),
				oracle.CmpOrdered, n)
			if lit, ok := networkOrdLit[n]; ok {
				add("litcmp_ord_"+n,
					fmt.Sprintf(`SELECT id, %s AS v FROM %s WHERE %s < %s ORDER BY id`, n, tbl, n, lit),
					oracle.CmpOrdered, n)
			}
			for _, x := range networkExtraLit {
				if x.col != n {
					continue
				}
				add("litcmp_"+x.suffix+"_"+n,
					fmt.Sprintf(`SELECT id, %s AS v FROM %s WHERE %s %s %s ORDER BY id`,
						n, tbl, n, x.op, x.lit),
					oracle.CmpOrdered, n)
			}
		}

		// CAST AS STRING for the two non-network types whose ColRef.Eval box
		// isn't their own rendering (#521): DATE boxes as its raw epoch-day
		// int32, and FLOAT32 boxes widened to float64. Both used to answer
		// through that raw box — the epoch day, or the float64-widened
		// digits — instead of the same text the projection and LIKE render.
		// This entry only pins the two wadjet EXECUTION PATHS (single-
		// process vs stage DAG) against each other, which agreed on the
		// wrong answer before the fix (one shared expr.Cast implementation);
		// the independent check is the PostgreSQL oracle, which reads the
		// value cold.
		if n == "c_date" || n == "c_f32" {
			add("caststr_"+n,
				fmt.Sprintf(`SELECT id, CAST(%s AS STRING) AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, n, tbl),
				oracle.CmpOrdered, n)
		}

		if !c.Flat {
			// A plain projection over a range that CERTAINLY contains a NULL
			// container. project_<n> above cannot promise that — its filter
			// (id % 331 = 7) selects no row where the container is null for
			// three of the four periods — and a NULL container that comes
			// back as a PRESENT container of nulls is exactly what #425 was:
			// the stage DAG's columnar ROW reader dropped the group's own
			// definition level while the single-process row reader kept it.
			// The range is wide enough to hold several nulls of every period
			// (103, 107, 109, 113).
			add("project_nulls_"+n,
				fmt.Sprintf(`SELECT id, %s AS v FROM %s WHERE id < 400 ORDER BY id`, n, tbl),
				oracle.CmpOrdered, n)
			// MIN/MAX over a container. The flat arm below gates these on
			// c.Ordered; the containers have no such flag because they had no
			// order until #415 gave them one, and MIN/MAX declined to answer
			// until #426. Both forms, because the scalar one takes the
			// whole-input path and the grouped one the hash path.
			add("minmax_"+n,
				fmt.Sprintf(`SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s`, n, n, tbl),
				oracle.CmpUnordered, n)
			add("minmax_group_"+n,
				fmt.Sprintf(`SELECT g, MIN(%s) AS lo, MAX(%s) AS hi FROM %s GROUP BY g ORDER BY g`, n, n, tbl),
				oracle.CmpOrdered, n)
			continue
		}
		// TEXT FUNCTIONS over a typed column. A vec string kernel reads its
		// argument straight out of BytesData, so it indexes an offsets array
		// that an INT64/DATE/IPv4/... column simply does not have. #509 was
		// exactly that — CONCAT(text, bigint) killed the server on ordinary
		// single-table SQL — and this corpus could not see it, because not
		// one entry above routes a typed column into a text function at all.
		// CONCAT reads EVERY argument as text (the multi-arg form is the one
		// that regressed); UPPER reads its first, which is the #273 shape.
		add("concat_"+n,
			fmt.Sprintf(`SELECT id, CONCAT(c_str, %s) AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, n, tbl),
			oracle.CmpOrdered, n)
		add("upper_"+n,
			fmt.Sprintf(`SELECT id, UPPER(%s) AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, n, tbl),
			oracle.CmpOrdered, n)

		// ARRAY[expr] built from a typed column, with the column referenced
		// NOWHERE else in the query — the exact shape of #596: projection
		// pruning had no case for ArrayLitNode, so the scan dropped the
		// column and every constructed array element read back NULL. The
		// WHERE clause filters on id, never on n, so nothing else keeps the
		// column alive.
		add("array_of_"+n,
			fmt.Sprintf(`SELECT id, ARRAY[%s] AS v FROM %s WHERE id %% 331 = 7 ORDER BY id`, n, tbl),
			oracle.CmpOrdered, n)

		// GROUP BY key: the key is serialized into the hash table and
		// reconstructed on output, a second retention path with its own
		// per-type encoding.
		add("groupby_"+n,
			fmt.Sprintf(`SELECT %s AS k, COUNT(*) AS n FROM %s GROUP BY %s ORDER BY k, n`, n, Table, n),
			oracle.CmpOrdered, n)
		// GROUP BY ARRAY[expr]: #596 also collapsed every row into one group
		// under this shape, because the pruned-away column made every
		// constructed key the same NULL-element array.
		add("groupby_array_"+n,
			fmt.Sprintf(`SELECT ARRAY[%s] AS k, COUNT(*) AS n FROM %s GROUP BY ARRAY[%s] ORDER BY k, n`, n, Table, n),
			oracle.CmpOrdered, n)
		// COUNT(DISTINCT) hashes the value into a per-group set.
		add("countdistinct_"+n,
			fmt.Sprintf(`SELECT g, COUNT(DISTINCT %s) AS n FROM %s GROUP BY g ORDER BY g`, n, tbl),
			oracle.CmpOrdered, n)
		// DISTINCT and the set operators keep a value in a dedup set.
		add("distinct_"+n,
			fmt.Sprintf(`SELECT DISTINCT %s AS v FROM %s ORDER BY v`, n, tbl),
			oracle.CmpOrdered, n)
		add("union_"+n,
			fmt.Sprintf(`SELECT %s AS v FROM %s WHERE id < 300 UNION SELECT %s FROM %s WHERE id >= %d ORDER BY v`,
				n, Table, n, Table, Rows-300),
			oracle.CmpOrdered, n)
		if c.Ordered {
			// Sort keys: the comparator plus, past the memory budget, the
			// sorted-run encoding.
			add("sort_"+n,
				fmt.Sprintf(`SELECT id, %s AS v FROM %s ORDER BY %s, id LIMIT 60`, n, Table, n),
				oracle.CmpOrdered, n)
			add("sort_desc_"+n,
				fmt.Sprintf(`SELECT id, %s AS v FROM %s ORDER BY %s DESC, id LIMIT 60`, n, Table, n),
				oracle.CmpOrdered, n)
			add("minmax_"+n,
				fmt.Sprintf(`SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s`, n, n, tbl),
				oracle.CmpUnordered, n)
		}
		if c.Wide {
			// Equi-join on the typed column: the build side copies (or
			// retains) the key AND the payload per type.
			add("selfjoin_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n, MIN(a.id) AS lo, MAX(b.id) AS hi `+
					`FROM %s a JOIN %s b ON a.%s = b.%s WHERE a.id < 500`, tbl, tbl, n, n),
				oracle.CmpUnordered, n)
			// RIGHT and FULL OUTER on the typed column. Every column here
			// carries NULLs, and a NULL key is unmatched BY DEFINITION — so
			// the outer join owes it a NULL-padded row. The integer build
			// paths used to drop those rows before they reached the arena
			// FlushUnmatched enumerates, while the serialized-key path kept
			// them, which made the answer depend on the key's ENCODING
			// rather than on the query (#496). Per type, because the
			// encoding is what the type decides: single int, two ints,
			// float, and the serialized fallback are four different build
			// loops, and a wide type reaches whichever its storage picks.
			add("rightjoin_null_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a RIGHT JOIN %s b ON a.%s = b.%s `+
					`WHERE b.id < 500`, tbl, tbl, n, n),
				oracle.CmpUnordered, n)
			add("fullouter_null_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a FULL OUTER JOIN %s b ON a.%s = b.%s `+
					`WHERE a.id < 500 OR b.id < 500`, tbl, tbl, n, n),
				oracle.CmpUnordered, n)
			// The two-column key takes its own build loop (dualIntKey when
			// both are integers, the serialized one otherwise), and it had
			// the same missing arena append plus a probe that never marked
			// a build row matched at all.
			add("rightjoin_dualkey_null_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a RIGHT JOIN %s b ON a.%s = b.%s AND a.g = b.g `+
					`WHERE b.id < 500`, tbl, tbl, n, n),
				oracle.CmpUnordered, n)
			// The same join with the typed column on the PAYLOAD side, so a
			// build-side value that is copied rather than compared is read
			// back out.
			add("joinpayload_"+n,
				fmt.Sprintf(`SELECT a.id AS id, b.%s AS v FROM %s a JOIN %s b ON a.id = b.id `+
					`WHERE a.id %% 337 = 11 ORDER BY a.id`, n, tbl, tbl),
				oracle.CmpOrdered, n)
			// SEMI and ANTI joins. Their build side needs only key
			// existence, so HashJoin.Build routes them to
			// buildParallelKeyOnly — a morsel-parallel path with its own
			// goroutines, its own shared source mutex and its own
			// flattenSource wrapper, reached by nothing else in this corpus.
			// A defect that lives only there (a panic under that mutex, a
			// per-type key encoding the local tables get wrong) was
			// invisible to every gate this corpus feeds.
			//
			// All three are written with ALIASES on both relations, which is
			// the OTHER thing they cover: the keys of these joins are named
			// by the OPTIMIZER, not by the user, and decorrelateInSubqueries
			// has to spell the inner one the way the inner plan emits it.
			// When it did not, every `IN (SELECT …)` here answered ZERO rows
			// on the single-process path while the stage DAG answered
			// correctly (#516) — and a bare, unaliased self-IN happened to
			// work, which is why nothing caught it.
			add("semijoin_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s IN `+
					`(SELECT b.%s FROM %s b WHERE b.id < 500)`, tbl, n, n, tbl),
				oracle.CmpUnordered, n)
			add("antijoin_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS `+
					`(SELECT 1 FROM %s b WHERE b.%s = a.%s AND b.id < 500)`, tbl, tbl, n, n),
				oracle.CmpUnordered, n)
			// NOT IN is a THIRD lowering, not a spelling of the one above: a
			// correlated NOT EXISTS decorrelates on its own equality, while
			// an uncorrelated NOT IN takes its key from the subquery's SELECT
			// list — the half #516 got wrong. Both sides are NULL-guarded
			// here so this entry isolates the KEY; the two below take the
			// guards off, one at a time.
			add("notin_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s IS NOT NULL AND a.%s NOT IN `+
					`(SELECT b.%s FROM %s b WHERE b.id < 500 AND b.%s IS NOT NULL)`, tbl, n, n, n, tbl, n),
				oracle.CmpUnordered, n)
			// NOT IN's three-valued rule, per type and per key encoding
			// (#507). An anti join asks "did nothing match", which is a
			// two-valued question; NOT IN's is not, and the difference is
			// decided entirely by NULLs.
			//
			// The list is unguarded here and every column of this fixture
			// carries NULLs, so a NULL reaches the build side and the
			// predicate is UNKNOWN for every probe row it did not otherwise
			// match — the answer is no rows at all. Degenerate on purpose:
			// what it gates is that the poison fires on BOTH arms and for
			// every key encoding, which is exactly where an anti join used
			// to answer with the whole probe side instead.
			add("notin_nulllist_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s NOT IN `+
					`(SELECT b.%s FROM %s b WHERE b.id < 500)`, tbl, n, n, tbl),
				oracle.CmpUnordered, n)
			// The non-degenerate half: a clean list, so the anti join still
			// answers — but the PROBE carries NULLs, and a NULL key compares
			// UNKNOWN against every value whether it matched or not. Those
			// rows must not survive, which is the rule an anti join gets
			// backwards on its own (no match => emit).
			add("notin_nullprobe_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s NOT IN `+
					`(SELECT b.%s FROM %s b WHERE b.id < 500 AND b.%s IS NOT NULL)`, tbl, n, n, tbl, n),
				oracle.CmpUnordered, n)

			// The same three lowerings with a JOIN inside the subquery.
			// Every entry above reads a SINGLE relation, and that is the
			// axis the corpus was blind on: decorrelateInSubqueries names
			// the semi join's inner key from the subquery's SELECT list,
			// and with one relation the bottom Scan's bare column name is
			// provably what the inner plan emits. With a JOIN it is not —
			// which relation's columns come out bare is decided by
			// reorderJoins from estimated row counts, at Optimize step 73,
			// long after decorrelation has named the key at step 36. A
			// rewrite that assumes write order answers over the wrong
			// relation, silently (#526, #527), and a self-IN cannot show
			// it because both sides of a self-IN carry the same values.
			//
			// Both qualifications, because they are different code paths
			// in innerSemiJoinKey: the item may qualify the relation
			// written FIRST or the one written second, and the two are
			// spelled the same way only by accident.
			add("semijoin_join_lead_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s IN `+
					`(SELECT b.%s FROM %s b JOIN %s d ON d.k = b.g WHERE b.id < 500)`,
					tbl, n, n, tbl, Dim),
				oracle.CmpUnordered, n)
			add("semijoin_join_nonlead_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s IN `+
					`(SELECT b.%s FROM %s d JOIN %s b ON d.k = b.g WHERE b.id < 500)`,
					tbl, n, n, Dim, tbl),
				oracle.CmpUnordered, n)
			add("antijoin_join_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS `+
					`(SELECT 1 FROM %s b JOIN %s d ON d.k = b.g WHERE b.%s = a.%s AND b.id < 500)`,
					tbl, tbl, Dim, n, n),
				oracle.CmpUnordered, n)
			add("notin_join_"+n,
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a WHERE a.%s IS NOT NULL AND a.%s NOT IN `+
					`(SELECT b.%s FROM %s d JOIN %s b ON d.k = b.g WHERE b.id < 500 AND b.%s IS NOT NULL)`,
					tbl, n, n, n, Dim, tbl, n),
				oracle.CmpUnordered, n)
		}
	}

	// Entries that target the statistical aggregates rather than a type. The
	// merge path of these was the #339 defect (mergeSinkState never merged
	// extraState, so CORR/COVAR/MEDIAN/PERCENTILE/MODE/MIN_BY/STRING_AGG/
	// BOOL_* all answered from one clone).
	add("stats_by_group",
		fmt.Sprintf(`SELECT g, STDDEV(m1) AS sd, VARIANCE(m1) AS vr, CORR(m1, m2) AS co, `+
			`COVAR_SAMP(m1, m2) AS cs, MEDIAN(m1) AS md, MODE(m1) AS mo `+
			`FROM %s GROUP BY g ORDER BY g`, Table),
		oracle.CmpOrdered, "")
	add("stats_scalar",
		fmt.Sprintf(`SELECT STDDEV(m1) AS sd, VARIANCE(m2) AS vr, CORR(m1, m2) AS co, `+
			`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m1) AS p50 FROM %s`, Table),
		oracle.CmpUnordered, "")
	add("stringagg_bytes",
		fmt.Sprintf(`SELECT g, STRING_AGG(c_str, '|') AS s FROM %s WHERE id < 120 GROUP BY g ORDER BY g`, Table),
		oracle.CmpOrdered, "c_str")
	add("boolagg",
		fmt.Sprintf(`SELECT g, BOOL_AND(c_bool) AS ba, BOOL_OR(c_bool) AS bo FROM %s GROUP BY g ORDER BY g`, Table),
		oracle.CmpOrdered, "c_bool")
	add("dim_left_join",
		fmt.Sprintf(`SELECT d.k AS k, d.label AS label, COUNT(t.id) AS n `+
			`FROM %s d LEFT JOIN %s t ON d.k = t.g GROUP BY d.k, d.label ORDER BY k`, Dim, Table),
		oracle.CmpOrdered, "")
	add("wide_row",
		fmt.Sprintf(`SELECT * FROM %s WHERE id %% 743 = 5 ORDER BY id`, Table),
		oracle.CmpOrdered, "")
	add("wide_row_nested",
		fmt.Sprintf(`SELECT * FROM %s WHERE id %% 743 = 5 ORDER BY id`, Nested),
		oracle.CmpOrdered, "")
	// A join ACROSS the two tables: the flat side's typed columns and the
	// nested side's containers meet in one output batch, which is the shape a
	// build-side copy or a late-materialization view has to carry.
	add("flat_nested_join",
		fmt.Sprintf(`SELECT t.id AS id, t.c_bytes AS b, n.c_arr AS a FROM %s t JOIN %s n ON t.id = n.id `+
			`WHERE t.id %% 613 = 3 ORDER BY t.id`, Table, Nested),
		oracle.CmpOrdered, "")

	return out
}

// ColumnNames returns the type-column names in corpus order, for reporting.
func ColumnNames() []string {
	cols := Columns()
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}
