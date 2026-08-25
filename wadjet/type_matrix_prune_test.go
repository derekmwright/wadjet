package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Row-group pruning must never change an answer. It is an optimization whose
// entire contract is "skip a row group that provably holds no matching row",
// and the row groups it skips are invisible: the query returns fewer rows and
// nothing says a decision was made.
//
// Three columns broke that contract, and all three broke it the same way
// (#442, and #438 which is the same defect seen through a DECIMAL): the prune
// layer compares two `any` values by their Go KIND, the footer's bounds are in
// the FILE's representation and the literal is in the ENGINE's, and for
// DECIMAL, IPV6 and UUID those are different things that land in the same
// kind. `c_ipv6 >= '2001:db8::5'` returned 0 rows against 4914 matching ones,
// because '2' (0x32) sorts above every byte of a 2001:db8:: address.
//
// This is the audit as a gate rather than as prose: every flat type, every
// comparison operator, a literal taken from a row the fixture actually holds,
// answered with pruning on and with pruning off. It is the shape no other gate
// has — the two-path suite and the DuckDB fingerprint both run the same prune
// on both sides and agree on the wrong answer.
func TestTypeMatrixPruningNeverChangesTheAnswer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	ctx := context.Background()
	db := tmOpen(t)

	// A row in the middle of the fixture, so a literal taken from it splits
	// the row groups rather than falling outside every bound.
	const probeRow = 1500
	rows := typematrix.Data(typematrix.Rows)
	if len(rows) <= probeRow {
		t.Fatalf("fixture has %d rows, need more than %d", len(rows), probeRow)
	}
	// Each column NULLs on its own stride, so the probe row is chosen per
	// column: the first row at or after probeRow that actually holds a value.
	valueFor := func(name string) any {
		for i := probeRow; i < len(rows); i++ {
			if v, ok := rows[i][name]; ok && v != nil {
				return v
			}
		}
		return nil
	}

	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})

	count := func(t *testing.T, sql string, prune bool) (int64, bool) {
		t.Helper()
		scan.StatsPrune.Set(prune)
		scan.DictPrune.Set(prune)
		res, err := tmRun(ctx, db, sql)
		if err != nil {
			return 0, false
		}
		if len(res.Rows) != 1 || len(res.Columns) != 1 {
			t.Fatalf("%s: want one row of one column, got %d rows and %d columns",
				sql, len(res.Rows), len(res.Columns))
		}
		n, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
		if !ok {
			t.Fatalf("%s: COUNT(*) came back as %#v", sql, res.Rows[0][res.Columns[0]])
		}
		return n, true
	}

	checked, skipped := 0, 0
	for _, c := range typematrix.Columns() {
		if !c.Flat {
			continue // a container has no scalar bound to prune on
		}
		v := valueFor(c.Name)
		lit, ok := tmSQLLiteral(c.Type, v)
		if !ok {
			t.Fatalf("column %s: the fixture value %#v has no SQL literal form — "+
				"the sweep would silently skip the type", c.Name, v)
		}
		for _, op := range []string{"=", "<>", "<", "<=", ">", ">="} {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s %s %s",
				typematrix.Table, c.Name, op, lit)
			t.Run(c.Name+"_"+tmOpName(op), func(t *testing.T) {
				off, okOff := count(t, sql, false)
				if !okOff {
					skipped++
					t.Skipf("the engine does not answer this shape: %s", sql)
				}
				on, okOn := count(t, sql, true)
				if !okOn {
					t.Fatalf("pruning turned an answerable query into an error: %s", sql)
				}
				checked++
				if on != off {
					t.Errorf("PRUNING CHANGED THE ANSWER\n  SQL: %s\n  prune on  = %d\n  prune off = %d",
						sql, on, off)
				}
			})
		}
	}

	// The six comparisons above are all a *expr.Cmp — the only shape
	// structuredConjuncts (internal/planner/physical) builds a prune
	// predicate from today. A NOT and a `= NULL` are two shapes that AREN'T
	// that today (a NotNode isn't a CmpExpr, and a NULL literal lowers to
	// MatchNothingFilter before reaching the prune layer), so both sides of
	// this sweep currently agree by construction rather than by a pruning
	// decision getting the negation or the three-valued NULL right. That is
	// exactly why they belong here: if a future change teaches the prune
	// layer to push a negated or NULL-literal conjunct into a row-group
	// skip decision, this is where a wrong one shows up, in this file, gated
	// in CI — not only in the two-path or predicate-semantics gates next
	// door, which assert against SQL truth rather than against each other.
	extra := []struct {
		name string
		sql  string
	}{
		// id is int64, monotonic 0..Rows-1, never NULL — a clean column to
		// split row groups on. NOT (id < 2500) is id >= 2500, a genuinely
		// non-trivial row set that lands inside the fixture's five 1100-row
		// groups rather than falling wholly outside every bound.
		{"NotLessThan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE NOT (id < 2500)", typematrix.Table)},
		{"NotGreaterEqual", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE NOT (id >= 2500)", typematrix.Table)},
		// `= NULL` is UNKNOWN for every row, so both sides must answer 0 —
		// but they must both answer it by evaluating the predicate, not by
		// one side crashing on a nil bound while the other quietly matches
		// nothing.
		{"NullLiteralEq", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE id = NULL", typematrix.Table)},
		// CIDR's own shape, spelled out rather than left to the sweep's
		// per-column probe value. The engine orders CIDR by PostgreSQL's inet
		// order and the footer bounds are the address TEXT's, so a literal
		// chosen to sit on the wrong side of that disagreement is what makes
		// a re-engaged prune visible here: "10.0.0.0/16" is BELOW every
		// "192.168.x" row as text and ABOVE the fixture's "10.x" rows as an
		// address. Before kernel.StatsDomainValue withheld TypeCIDR this
		// answered 0 rows pruned-on and a non-zero count pruned-off.
		{"CidrLessThanAcrossTheTextBarrier",
			fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr < '10.0.0.0/16'", typematrix.Table)},
		{"CidrGreaterEqualAcrossTheTextBarrier",
			fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr >= '10.0.0.0/16'", typematrix.Table)},
		// A HOST-BEARING literal: the fixture holds rows that differ from it
		// only in bits the mask covers, which the first CidrSortKey erased.
		{"CidrEqualsAHostBearingPrefix",
			fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr = '192.168.188.190/24'", typematrix.Table)},
		// A BARE address is a /32 host route, as PostgreSQL's inet reads it.
		{"CidrEqualsABareAddress",
			fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr = '172.16.2.187'", typematrix.Table)},
	}
	for _, tc := range extra {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			off, okOff := count(t, tc.sql, false)
			if !okOff {
				skipped++
				t.Skipf("the engine does not answer this shape: %s", tc.sql)
			}
			on, okOn := count(t, tc.sql, true)
			if !okOn {
				t.Fatalf("pruning turned an answerable query into an error: %s", tc.sql)
			}
			checked++
			if on != off {
				t.Errorf("PRUNING CHANGED THE ANSWER\n  SQL: %s\n  prune on  = %d\n  prune off = %d",
					tc.sql, on, off)
			}
		})
	}

	t.Logf("prune sweep: %d predicates compared, %d shapes unanswerable", checked, skipped)
	if checked == 0 {
		t.Fatal("no predicate was compared — the sweep proves nothing")
	}
}

// tmSQLLiteral renders a fixture value as the SQL literal a query would carry.
// A type with no rendering here is a FAILURE, not a skip: the point of the
// sweep is that no type is missing from it.
func tmSQLLiteral(typ parquet.TypeID, v any) (string, bool) {
	switch typ {
	case parquet.TypeBool:
		b, ok := v.(bool)
		return strconv.FormatBool(b), ok
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		switch tv := v.(type) {
		case int32:
			return strconv.FormatInt(int64(tv), 10), true
		case int64:
			return strconv.FormatInt(tv, 10), true
		}
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeDuration:
		n, ok := v.(int64)
		return strconv.FormatInt(n, 10), ok
	case parquet.TypeFloat32:
		f, ok := v.(float32)
		return strconv.FormatFloat(float64(f), 'f', -1, 32), ok
	case parquet.TypeFloat64, parquet.TypeDecimal:
		f, ok := v.(float64)
		return strconv.FormatFloat(f, 'f', -1, 64), ok
	case parquet.TypeString, parquet.TypeIPv4, parquet.TypeIPv6,
		parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID, parquet.TypeDate:
		s, ok := v.(string)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'", ok
	case parquet.TypeBytes:
		b, ok := v.([]byte)
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'", ok
	}
	return "", false
}

func tmOpName(op string) string {
	switch op {
	case "=":
		return "eq"
	case "<>":
		return "ne"
	case "<":
		return "lt"
	case "<=":
		return "le"
	case ">":
		return "gt"
	}
	return "ge"
}

func tmAsInt64(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	case float64:
		return int64(tv), true
	}
	return 0, false
}

// --- the STRUCTURAL arm ----------------------------------------------------

// TestTypeMatrixPruningAnswersTheFixtureBounds is the ground truth the sweep
// above cannot be.
//
// That sweep is a DIFFERENTIAL: the same engine, once with pruning on and once
// with it off. Two failure modes walk straight through it.
//
//  1. BOTH ARMS WRONG. The un-pruned path is not a reference implementation —
//     it is this scanner with one optimization disabled. A defect anywhere
//     else (a decoder, a comparison kernel, a NULL rule) moves both arms
//     together and the differential reports agreement.
//  2. THE PRUNE DISENGAGED. A kernel.StatsDomainValue that converts nothing
//     WITHHOLDS every predicate, the prune layer never sees one, and prune-on
//     is byte-for-byte prune-off. That is never a wrong answer and never a
//     caught regression: the gate goes green precisely because the thing it
//     guards stopped running. (TestTypeMatrixPruneConversionCoversEveryType
//     below is the other half of that one.)
//
// This arm answers from the FIXTURE's construction instead. Every column in
// tmAscendingColumns rises with the row index, so its minimum is its first
// non-NULL value and its maximum is its last, and the three boundary
// predicates have answers that are arithmetic on the generator rather than a
// second run of the engine:
//
//	col >= min   every non-NULL row, and only those
//	col <  min   no row
//	col >  max   no row
//
// A prune that drops a qualifying row group fails here even when the un-pruned
// path drops it too.
//
// The counts are NON-NULL counts, deliberately: a NULL satisfies no
// comparison, so `col >= min` excludes the fixture's null stride (PostgreSQL
// semantics, ADR-0012). They come from typematrix.Data — the generator that
// produced the parquet — and never from a query.
func TestTypeMatrixPruningAnswersTheFixtureBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	asc := tmAscendingColumns()

	// Both directions, so a flat column cannot leave the structural arm by
	// omission: it either ascends, or it is named as cycling.
	inAsc := make(map[string]bool, len(asc))
	for _, c := range asc {
		inAsc[c.name] = true
	}
	typeOf := map[string]parquet.TypeID{}
	for _, c := range typematrix.Columns() {
		typeOf[c.Name] = c.Type
		if !c.Flat {
			continue
		}
		_, cycles := tmNotMonotonic[c.Name]
		switch {
		case inAsc[c.Name] && cycles:
			t.Errorf("column %s is listed as both ascending and cycling — pick one", c.Name)
		case !inAsc[c.Name] && !cycles:
			t.Errorf("flat column %s is in neither list: put it in tmAscendingColumns if its "+
				"values rise with the row index, or in tmNotMonotonic with the reason they do not",
				c.Name)
		}
	}

	ctx := context.Background()
	db := tmOpen(t)
	rows := typematrix.Data(typematrix.Rows)

	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})
	count := func(t *testing.T, sql string, prune bool) int64 {
		t.Helper()
		scan.StatsPrune.Set(prune)
		scan.DictPrune.Set(prune)
		res, err := tmRun(ctx, db, sql)
		if err != nil {
			t.Fatalf("%s (prune %v): %v", sql, prune, err)
		}
		if len(res.Rows) != 1 || len(res.Columns) != 1 {
			t.Fatalf("%s: want one row of one column, got %d rows and %d columns",
				sql, len(res.Rows), len(res.Columns))
		}
		n, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
		if !ok {
			t.Fatalf("%s: COUNT(*) came back as %#v", sql, res.Rows[0][res.Columns[0]])
		}
		return n
	}

	checked := 0
	for _, c := range asc {
		vals := tmNonNullValues(rows, c.name)
		if len(vals) < 2 {
			t.Fatalf("column %s: the fixture holds %d non-NULL values, too few to bound", c.name, len(vals))
		}
		for i := 1; i < len(vals); i++ {
			if c.less(vals[i], vals[i-1]) {
				t.Fatalf("column %s is NOT ascending: value %d (%v) sorts below value %d (%v). "+
					"The structural arm's ground truth IS the fixture's order — restore it, or move "+
					"the column to tmNotMonotonic", c.name, i, vals[i], i-1, vals[i-1])
			}
		}
		lo, hi := vals[0], vals[len(vals)-1]
		loLit, ok := tmSQLLiteral(typeOf[c.name], lo)
		if !ok {
			t.Fatalf("column %s: the fixture minimum %#v has no SQL literal form", c.name, lo)
		}
		hiLit, ok := tmSQLLiteral(typeOf[c.name], hi)
		if !ok {
			t.Fatalf("column %s: the fixture maximum %#v has no SQL literal form", c.name, hi)
		}

		for _, want := range []struct {
			op   string
			lit  string
			rows int64
		}{
			{">=", loLit, int64(len(vals))}, // every non-NULL row qualifies
			{"<", loLit, 0},                 // nothing sorts below the minimum
			{">", hiLit, 0},                 // nothing sorts above the maximum
		} {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s %s %s",
				typematrix.Table, c.name, want.op, want.lit)
			t.Run(c.name+"_"+tmOpName(want.op), func(t *testing.T) {
				on := count(t, sql, true)
				off := count(t, sql, false)
				checked++
				if on != want.rows {
					t.Errorf("WRONG AGAINST THE FIXTURE, pruning ON\n  SQL: %s\n  engine = %d\n  fixture says %d",
						sql, on, want.rows)
				}
				if off != want.rows {
					t.Errorf("WRONG AGAINST THE FIXTURE, pruning OFF\n  SQL: %s\n  engine = %d\n  fixture says %d\n"+
						"  (both arms wrong is the case the prune-on/prune-off differential cannot see)",
						sql, off, want.rows)
				}
				if on != off {
					t.Errorf("PRUNING CHANGED THE ANSWER\n  SQL: %s\n  prune on  = %d\n  prune off = %d", sql, on, off)
				}
			})
		}
	}
	t.Logf("fixture-bound sweep: %d predicates answered against the generator", checked)
	if checked == 0 {
		t.Fatal("no predicate was answered — the sweep proves nothing")
	}
}

// tmOrderedCol pairs a fixture column with the order the ENGINE compares it
// in. That is not always Go's order on the fixture value: an IPv4 is an
// integer and an IPv6 is sixteen raw bytes, and '2001:db8::10' sorts BELOW
// '2001:db8::5' as text while the two addresses sort the other way — which is
// exactly the confusion #442 was.
type tmOrderedCol struct {
	name string
	less func(a, b any) bool
}

// tmAscendingColumns names the fixture columns whose values rise with the row
// index, so the minimum is the first non-NULL value and the maximum the last.
// The claim is re-checked from typematrix.Data on every run.
func tmAscendingColumns() []tmOrderedCol {
	return []tmOrderedCol{
		{"c_i32", tmNumLess},    // i*3
		{"c_i64", tmNumLess},    // i*1_000_003
		{"c_f32", tmNumLess},    // i/7
		{"c_f64", tmNumLess},    // i/3
		{"c_str", tmTextLess},   // s-%06d: fixed width, so text order is index order
		{"c_bytes", tmByteLess}, // bytes-%06d-...: the width-6 index decides before the suffix
		{"c_ts", tmNumLess},     // epoch ms + i*61_000
		{"c_ipv4", tmAddrLess},  // 10.0.0.0 + i, an INTEGER order
		{"c_ipv6", tmAddrLess},  // 2001:db8:: + i, a RAW-BYTE order
		{"c_mac", tmAddrLess},   // aa:bb:cc:00:00:00 + i
		{"c_port", tmNumLess},   // 1024 + i%40000; the fixture stops at 5000 rows
		{"c_dur", tmNumLess},    // i*1_000_000
		{"c_uuid", tmTextLess},  // fixed-width lowercase hex: text order IS raw-byte order
		{"c_dec", tmNumLess},    // i*1.0001, exact at DECIMAL(18,4)
	}
}

// tmNotMonotonic is the other half of that list: the flat columns that cycle,
// each with the reason. Asserted in both directions so coverage cannot be lost
// by omission.
var tmNotMonotonic = map[string]string{
	"c_bool":  "two values, alternating on i%3",
	"c_cidr":  "192.168.(i%256).0/24 wraps every 256 rows",
	"c_proto": "i%256 wraps every 256 rows",
	"c_date":  "year, month and day each cycle on their own modulus",
}

// tmNonNullValues returns a column's non-NULL fixture values in row order.
func tmNonNullValues(rows []map[string]any, name string) []any {
	out := make([]any, 0, len(rows))
	for _, r := range rows {
		if v, ok := r[name]; ok && v != nil {
			out = append(out, v)
		}
	}
	return out
}

func tmNumLess(a, b any) bool {
	switch av := a.(type) {
	case int32:
		bv, ok := b.(int32)
		return ok && av < bv
	case int64:
		bv, ok := b.(int64)
		return ok && av < bv
	case float32:
		bv, ok := b.(float32)
		return ok && av < bv
	case float64:
		bv, ok := b.(float64)
		return ok && av < bv
	}
	return false
}

func tmTextLess(a, b any) bool {
	as, aok := a.(string)
	bs, bok := b.(string)
	return aok && bok && as < bs
}

func tmByteLess(a, b any) bool {
	ab, aok := a.([]byte)
	bb, bok := b.([]byte)
	return aok && bok && bytes.Compare(ab, bb) < 0
}

// tmAddrLess is the order an ADDRESS has, which is the order the engine
// compares IPv4, IPv6 and MAC columns in — not the order their text has.
func tmAddrLess(a, b any) bool {
	as, aok := a.(string)
	bs, bok := b.(string)
	if !aok || !bok {
		return false
	}
	raw := func(s string) []byte {
		if ip := net.ParseIP(s); ip != nil {
			return ip.To16()
		}
		mac, err := net.ParseMAC(s)
		if err != nil {
			return nil
		}
		return mac
	}
	ra, rb := raw(as), raw(bs)
	return ra != nil && rb != nil && bytes.Compare(ra, rb) < 0
}

// --- the ENGAGEMENT gate ---------------------------------------------------

// tmPruneWithheldTypes is the explicit list of type-matrix column types the
// prune layer is handed NOTHING for: a container's statistics belong to its
// LEAVES, which are not the column, so kernel.StatsDomainValue withholds it.
// Asserted in both directions below.
var tmPruneWithheldTypes = map[parquet.TypeID]bool{
	parquet.TypeArray:  true,
	parquet.TypeRow:    true,
	parquet.TypeMap:    true,
	parquet.TypeVector: true,
	// CIDR is no longer withheld (#523): the writer now accumulates a CIDR
	// leaf's row-group min/max by PostgreSQL's inet order rather than the
	// text's own byte order (parquet.CidrStatsOrderKey), and
	// kernel.StatsDomainValue converts a literal to the same order
	// (CidrSortKey) unconditionally, since RowGroupStats only ever hands
	// back a bound in that order — an old-footer file, or one this reader
	// cannot identify as CIDR at all, withholds at RowGroupStats instead
	// (parquet's TestCIDRRowGroupStatsWithheldOnOldFooter /
	// WithheldOnUnparseableValue). Was #492's fix for the same defect one
	// layer up: WITHHOLDING an order it could not repair rather than
	// answering it wrong; this is the write-time repair #492 deferred.
}

// tmPruneMustConvert is the floor: the everyday types whose predicates carry
// most of the row groups this engine ever skips. Naming them separately from
// the matrix sweep means a fixture that lost a column cannot also quietly lose
// the requirement.
var tmPruneMustConvert = []parquet.TypeID{
	parquet.TypeInt32, parquet.TypeInt64,
	parquet.TypeFloat32, parquet.TypeFloat64,
	parquet.TypeString, parquet.TypeDate, parquet.TypeTimestamp,
}

// TestTypeMatrixPruneConversionCoversEveryType is the engagement half of the
// sweeps above, and it is the assertion no differential can make.
//
// kernel.StatsDomainValue is the producer the whole prune layer sits on: a
// predicate it declines is WITHHELD, and a withheld predicate prunes nothing.
// So a regression that makes it decline everything — a reordered switch, a
// type that stopped matching, a `return nil, false` moved up a line — turns
// row-group pruning OFF engine-wide and produces no wrong answer at all. The
// prune-on/prune-off differential goes green on it (both arms are now the
// same arm), the fixture-bound arm above goes green on it (an un-pruned scan
// is correct), and the only visible symptom is a benchmark somewhere getting
// slower.
//
// Every flat type in the matrix must therefore convert a value the fixture
// actually holds, and the types that legitimately do not are the explicit list
// above — checked in both directions, so a type cannot fall out of prune
// coverage by omission.
func TestTypeMatrixPruneConversionCoversEveryType(t *testing.T) {
	scaleOf := map[string]int{}
	for _, s := range []parquet.Schema{typematrix.Schema(), typematrix.NestedSchema()} {
		for _, c := range s.Columns {
			scaleOf[c.Name] = int(c.Scale)
		}
	}
	flatRows := typematrix.Data(typematrix.Rows)
	nestedRows := typematrix.NestedData(typematrix.Rows)

	converts := map[parquet.TypeID]bool{}
	var missing, unexpected []string
	for _, c := range typematrix.Columns() {
		rows := flatRows
		if !c.Flat {
			rows = nestedRows
		}
		vals := tmNonNullValues(rows, c.Name)
		if len(vals) == 0 {
			t.Fatalf("column %s holds no non-NULL fixture value to convert", c.Name)
		}
		got, ok := kernel.StatsDomainValue(c.Type, scaleOf[c.Name], vals[0])
		switch {
		case tmPruneWithheldTypes[c.Type] && ok:
			unexpected = append(unexpected, fmt.Sprintf("%s (%v) converted to %#v", c.Name, c.Type, got))
		case !tmPruneWithheldTypes[c.Type] && !ok:
			missing = append(missing, fmt.Sprintf("%s (%v), fixture value %#v", c.Name, c.Type, vals[0]))
		case ok && got == nil:
			missing = append(missing, fmt.Sprintf("%s (%v) converted to a nil domain value", c.Name, c.Type))
		}
		if ok {
			converts[c.Type] = true
		}
	}
	if len(missing) > 0 {
		t.Errorf("kernel.StatsDomainValue withheld these columns, so NOTHING prunes on them: %v\n"+
			"either the conversion regressed or the type belongs in tmPruneWithheldTypes — say which, deliberately",
			missing)
	}
	if len(unexpected) > 0 {
		t.Errorf("kernel.StatsDomainValue converted types listed as withheld: %v\n"+
			"drop them from tmPruneWithheldTypes", unexpected)
	}
	for _, typ := range tmPruneMustConvert {
		if !converts[typ] {
			t.Errorf("type %v does not convert to the stats domain — row-group pruning is OFF for it", typ)
		}
	}
}
