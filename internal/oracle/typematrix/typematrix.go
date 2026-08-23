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

// Columns is the type matrix: one column per wadjet type, all 22.
//
// Each is nullable and each carries a NULL every so often, at a stride coprime
// with both the batch size and the row-group size so nulls land at interior
// AND boundary positions — the offsets-on-NULL corruption class only fires at
// a boundary.
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
			pc.Fields = []parquet.Column{
				{Name: "a", Type: parquet.TypeString, Nullable: true},
				{Name: "b", Type: parquet.TypeInt64, Nullable: true},
			}
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
		r["c_cidr"] = orNull(i, 67, fmt.Sprintf("192.168.%d.0/24", i%256))
		r["c_mac"] = orNull(i, 71, fmt.Sprintf("aa:bb:cc:%02x:%02x:%02x", (i/65536)%256, (i/256)%256, i%256))
		r["c_port"] = orNull(i, 73, int32(1024+i%40000))
		r["c_proto"] = orNull(i, 79, int32(i%256))
		r["c_dur"] = orNull(i, 83, int64(i)*1_000_000)
		r["c_uuid"] = orNull(i, 89, fmt.Sprintf("00000000-0000-4000-8000-%012x", i))
		r["c_date"] = orNull(i, 97, fmt.Sprintf("20%02d-%02d-%02d", 10+i%15, 1+i%12, 1+i%28))
		r["c_dec"] = orNull(i, 101, float64(i)+0.0001*float64(i%9973))
		r["c_arr"] = orNull(i, 103, arrayValue(i))
		r["c_row"] = orNull(i, 107, map[string]any{"a": fmt.Sprintf("r-%05d", i), "b": int64(i) * 11})
		r["c_map"] = orNull(i, 109, map[string]any{fmt.Sprintf("k%d", i%5): int64(i)})
		r["c_vec"] = orNull(i, 113, []float32{float32(i), float32(i) + 0.5, -float32(i), 0.25})
		rows[i] = r
	}
	return rows
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
			continue
		}
		// GROUP BY key: the key is serialized into the hash table and
		// reconstructed on output, a second retention path with its own
		// per-type encoding.
		add("groupby_"+n,
			fmt.Sprintf(`SELECT %s AS k, COUNT(*) AS n FROM %s GROUP BY %s ORDER BY k, n`, n, Table, n),
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
			// The same join with the typed column on the PAYLOAD side, so a
			// build-side value that is copied rather than compared is read
			// back out.
			add("joinpayload_"+n,
				fmt.Sprintf(`SELECT a.id AS id, b.%s AS v FROM %s a JOIN %s b ON a.id = b.id `+
					`WHERE a.id %% 337 = 11 ORDER BY a.id`, n, tbl, tbl),
				oracle.CmpOrdered, n)
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
