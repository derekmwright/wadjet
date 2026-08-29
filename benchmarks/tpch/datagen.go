package tpch

import (
	"fmt"
	"math/rand"
	"time"
)

// ScaleFactor controls data volume. SF=0.01 is ~10MB, SF=1 is ~1GB.
type ScaleFactor float64

const (
	SF001 ScaleFactor = 0.01
	SF01  ScaleFactor = 0.1
	SF1   ScaleFactor = 1.0
	SF10  ScaleFactor = 10.0
	SF100 ScaleFactor = 100.0
)

// RowCounts returns the row counts for each table at the given scale factor.
func (sf ScaleFactor) RowCounts() TableCounts {
	f := float64(sf)
	return TableCounts{
		Region:   5,  // fixed
		Nation:   25, // fixed
		Supplier: max(10, int(10000*f)),
		Part:     max(20, int(200000*f)),
		PartSupp: max(80, int(800000*f)),
		Customer: max(15, int(150000*f)),
		Orders:   max(60, int(1500000*f)),
		LineItem: max(240, int(6000000*f)),
	}
}

type TableCounts struct {
	Region, Nation, Supplier, Part, PartSupp, Customer, Orders, LineItem int
}

// TPC-H reference data
var regions = []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}

var nations = []struct {
	name      string
	regionKey int
}{
	{"ALGERIA", 0}, {"ARGENTINA", 1}, {"BRAZIL", 1}, {"CANADA", 1}, {"EGYPT", 4},
	{"ETHIOPIA", 0}, {"FRANCE", 3}, {"GERMANY", 3}, {"INDIA", 2}, {"INDONESIA", 2},
	{"IRAN", 4}, {"IRAQ", 4}, {"JAPAN", 2}, {"JORDAN", 4}, {"KENYA", 0},
	{"MOROCCO", 0}, {"MOZAMBIQUE", 0}, {"PERU", 1}, {"CHINA", 2}, {"ROMANIA", 3},
	{"SAUDI ARABIA", 4}, {"VIETNAM", 2}, {"RUSSIA", 3}, {"UNITED KINGDOM", 3}, {"UNITED STATES", 1},
}

var mktSegments = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"}
var priorities = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
var shipModes = []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
var shipInstructs = []string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}
var containers = []string{"SM CASE", "SM BOX", "SM PACK", "SM PKG", "SM JAR", "SM CAN",
	"MED BAG", "MED BOX", "MED PACK", "MED PKG", "MED JAR", "MED CAN",
	"LG CASE", "LG BOX", "LG PACK", "LG PKG", "LG JAR", "LG CAN",
	"JUMBO BAG", "JUMBO BOX", "JUMBO PACK", "JUMBO PKG", "JUMBO JAR", "JUMBO CAN",
	"WRAP CASE", "WRAP BOX", "WRAP PACK", "WRAP PKG", "WRAP JAR", "WRAP CAN"}
var brands []string
var partTypes []string

func init() {
	// Generate brand names: Brand#11 through Brand#55
	for i := 1; i <= 5; i++ {
		for j := 1; j <= 5; j++ {
			brands = append(brands, fmt.Sprintf("Brand#%d%d", i, j))
		}
	}
	// Generate part types
	typeWords := []string{"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"}
	materials := []string{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"}
	items := []string{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}
	for _, tw := range typeWords {
		for _, m := range materials {
			for _, it := range items {
				partTypes = append(partTypes, tw+" "+m+" "+it)
			}
		}
	}
}

// Generate produces all TPC-H tables at the given scale factor in the FLOAT64
// fixture. Returns a map of table name → rows. Deterministic for a given SF.
func Generate(sf ScaleFactor) map[string][]map[string]any {
	return GenerateFor(sf, FloatFixture)
}

// GenerateFor is Generate for a chosen fixture. The RNG draws are identical in
// both, so the two fixtures hold the SAME VALUES and differ only in the carrier
// of the eight monetary columns (see schema_decimal.go).
func GenerateFor(sf ScaleFactor, f Fixture) map[string][]map[string]any {
	rng := rand.New(rand.NewSource(int64(sf * 42_000_000)))
	counts := sf.RowCounts()
	data := make(map[string][]map[string]any, 8)

	data["region"] = genRegion()
	data["nation"] = genNation()
	data["supplier"] = genSupplier(rng, counts.Supplier, f)
	data["part"] = genPart(rng, counts.Part, f)
	data["partsupp"] = genPartSupp(rng, counts.Part, counts.Supplier, counts.PartSupp, f)
	data["customer"] = genCustomer(rng, counts.Customer, f)
	data["orders"] = genOrders(rng, counts.Orders, counts.Customer, f)
	data["lineitem"] = genLineItem(rng, counts.Orders, counts.LineItem, counts.Part, counts.Supplier, f)

	return data
}

// chunkEmitter collects rows and emits them in fixed-size chunks to bound memory.
type chunkEmitter struct {
	buf       []map[string]any
	chunkSize int
	emit      func([]map[string]any) error
	err       error
	total     int
}

func newEmitter(chunkSize int, emit func([]map[string]any) error) *chunkEmitter {
	return &chunkEmitter{
		buf:       make([]map[string]any, 0, chunkSize),
		chunkSize: chunkSize,
		emit:      emit,
	}
}

func (e *chunkEmitter) add(row map[string]any) {
	if e.err != nil {
		return
	}
	e.buf = append(e.buf, row)
	e.total++
	if len(e.buf) >= e.chunkSize {
		e.err = e.emit(e.buf)
		e.buf = e.buf[:0]
	}
}

func (e *chunkEmitter) flush() error {
	if e.err != nil {
		return e.err
	}
	if len(e.buf) > 0 {
		e.err = e.emit(e.buf)
		e.buf = e.buf[:0]
	}
	return e.err
}

// GenerateChunked streams TPC-H data for all tables, calling emit with chunks of rows.
// Memory usage is bounded to O(chunkSize) regardless of scale factor.
func GenerateChunked(sf ScaleFactor, chunkSize int, emit func(table string, rows []map[string]any) error) error {
	return GenerateChunkedFor(sf, chunkSize, FloatFixture, emit)
}

// GenerateChunkedFor is GenerateChunked for a chosen fixture.
func GenerateChunkedFor(sf ScaleFactor, chunkSize int, f Fixture, emit func(table string, rows []map[string]any) error) error {
	rng := rand.New(rand.NewSource(int64(sf * 42_000_000)))
	counts := sf.RowCounts()

	// Region and nation are fixed-size, emit whole
	if err := emit("region", genRegion()); err != nil {
		return fmt.Errorf("region: %w", err)
	}
	if err := emit("nation", genNation()); err != nil {
		return fmt.Errorf("nation: %w", err)
	}

	type tableGen struct {
		name string
		fn   func(*chunkEmitter)
	}
	tables := []tableGen{
		{"supplier", func(e *chunkEmitter) { streamSupplier(rng, counts.Supplier, f, e) }},
		{"part", func(e *chunkEmitter) { streamPart(rng, counts.Part, f, e) }},
		{"partsupp", func(e *chunkEmitter) { streamPartSupp(rng, counts.Part, counts.Supplier, counts.PartSupp, f, e) }},
		{"customer", func(e *chunkEmitter) { streamCustomer(rng, counts.Customer, f, e) }},
		{"orders", func(e *chunkEmitter) { streamOrders(rng, counts.Orders, counts.Customer, f, e) }},
		{"lineitem", func(e *chunkEmitter) {
			streamLineItem(rng, counts.Orders, counts.LineItem, counts.Part, counts.Supplier, f, e)
		}},
	}

	for _, tbl := range tables {
		name := tbl.name
		e := newEmitter(chunkSize, func(rows []map[string]any) error {
			return emit(name, rows)
		})
		tbl.fn(e)
		if err := e.flush(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}

	return nil
}

func genRegion() []map[string]any {
	rows := make([]map[string]any, len(regions))
	for i, name := range regions {
		rows[i] = map[string]any{
			"r_regionkey": int32(i),
			"r_name":      name,
			"r_comment":   fmt.Sprintf("Region %s comment", name),
		}
	}
	return rows
}

func genNation() []map[string]any {
	rows := make([]map[string]any, len(nations))
	for i, n := range nations {
		rows[i] = map[string]any{
			"n_nationkey": int32(i),
			"n_name":      n.name,
			"n_regionkey": int32(n.regionKey),
			"n_comment":   fmt.Sprintf("Nation %s comment", n.name),
		}
	}
	return rows
}

func genSupplier(rng *rand.Rand, count int, f Fixture) []map[string]any {
	rows := make([]map[string]any, count)
	for i := range rows {
		rows[i] = map[string]any{
			"s_suppkey":   int32(i + 1),
			"s_name":      fmt.Sprintf("Supplier#%09d", i+1),
			"s_address":   randString(rng, 10, 25),
			"s_nationkey": int32(rng.Intn(25)),
			"s_phone":     randPhone(rng),
			"s_acctbal":   f.money(randCents(rng, -999.99, 9999.99)),
			"s_comment":   randString(rng, 20, 80),
		}
	}
	return rows
}

func genPart(rng *rand.Rand, count int, f Fixture) []map[string]any {
	rows := make([]map[string]any, count)
	for i := range rows {
		rows[i] = map[string]any{
			"p_partkey":     int32(i + 1),
			"p_name":        randPartName(rng),
			"p_mfgr":        fmt.Sprintf("Manufacturer#%d", rng.Intn(5)+1),
			"p_brand":       brands[rng.Intn(len(brands))],
			"p_type":        partTypes[rng.Intn(len(partTypes))],
			"p_size":        int32(rng.Intn(50) + 1),
			"p_container":   containers[rng.Intn(len(containers))],
			"p_retailprice": f.money(int64(90000 + i*3 + int(rng.Int31n(1000)))),
			"p_comment":     randString(rng, 5, 20),
		}
	}
	return rows
}

func genPartSupp(rng *rand.Rand, numParts, numSupps, count int, f Fixture) []map[string]any {
	rows := make([]map[string]any, 0, count)
	suppPerPart := 4
	if count < numParts*4 {
		suppPerPart = max(1, count/max(1, numParts))
	}
	for i := 0; i < numParts && len(rows) < count; i++ {
		for j := 0; j < suppPerPart && len(rows) < count; j++ {
			suppKey := (i*suppPerPart+j)%numSupps + 1
			rows = append(rows, map[string]any{
				"ps_partkey":    int32(i + 1),
				"ps_suppkey":    int32(suppKey),
				"ps_availqty":   int32(rng.Intn(9999) + 1),
				"ps_supplycost": f.money(randCents(rng, 1.0, 1000.0)),
				"ps_comment":    randString(rng, 20, 120),
			})
		}
	}
	return rows
}

func genCustomer(rng *rand.Rand, count int, f Fixture) []map[string]any {
	rows := make([]map[string]any, count)
	for i := range rows {
		rows[i] = map[string]any{
			"c_custkey":    int32(i + 1),
			"c_name":       fmt.Sprintf("Customer#%09d", i+1),
			"c_address":    randString(rng, 10, 25),
			"c_nationkey":  int32(rng.Intn(25)),
			"c_phone":      randPhone(rng),
			"c_acctbal":    f.money(randCents(rng, -999.99, 9999.99)),
			"c_mktsegment": mktSegments[rng.Intn(len(mktSegments))],
			"c_comment":    randString(rng, 20, 80),
		}
	}
	return rows
}

func genOrders(rng *rand.Rand, count, numCusts int, f Fixture) []map[string]any {
	rows := make([]map[string]any, count)
	statuses := []string{"F", "O", "P"}
	// TPC-H spec: orders only reference the first 2/3 of customers.
	// The remaining 1/3 have no orders, enabling NOT EXISTS queries (Q22).
	custRange := max(1, numCusts*2/3)
	for i := range rows {
		year := 1992 + rng.Intn(6) // 1992-1997
		month := rng.Intn(12) + 1
		day := rng.Intn(28) + 1
		rows[i] = map[string]any{
			"o_orderkey":      int32(i + 1),
			"o_custkey":       int32(rng.Intn(custRange) + 1),
			"o_orderstatus":   statuses[rng.Intn(3)],
			"o_totalprice":    f.money(randCents(rng, 1000.0, 500000.0)),
			"o_orderdate":     fmt.Sprintf("%04d-%02d-%02d", year, month, day),
			"o_orderpriority": priorities[rng.Intn(len(priorities))],
			"o_clerk":         fmt.Sprintf("Clerk#%09d", rng.Intn(max(1, count/1000))+1),
			"o_shippriority":  int32(0),
			"o_comment":       randString(rng, 20, 60),
		}
	}
	return rows
}

func genLineItem(rng *rand.Rand, numOrders, count, numParts, numSupps int, f Fixture) []map[string]any {
	rows := make([]map[string]any, 0, count)
	flags := []string{"N", "R", "A"}
	lineStatuses := []string{"O", "F"}

	// TPC-H spec: 1-7 line items per order (uniform random)
	for orderKey := 1; orderKey <= numOrders && len(rows) < count; orderKey++ {
		nLines := 1 + rng.Intn(7) // 1 to 7 line items
		for ln := 1; ln <= nLines && len(rows) < count; ln++ {
			quantity := float64(rng.Intn(50) + 1)
			price := f.money(randCents(rng, 900.0, 100000.0))
			discount := f.money(int64(rng.Intn(11)))
			tax := f.money(int64(rng.Intn(9)))

			// TPC-H spec date generation:
			// L_COMMITDATE = O_ORDERDATE + random(5, 120) days
			// L_SHIPDATE   = L_COMMITDATE + random(-10, 40) days
			// L_RECEIPTDATE = L_SHIPDATE + random(1, 30) days
			year := 1992 + rng.Intn(6)
			month := rng.Intn(12) + 1
			day := rng.Intn(28) + 1
			orderDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
			commitDate := orderDate.AddDate(0, 0, rng.Intn(116)+5) // +5..120 days
			shipDate := commitDate.AddDate(0, 0, rng.Intn(51)-10)  // -10..+40 days
			receiptDate := shipDate.AddDate(0, 0, rng.Intn(30)+1)  // +1..30 days

			// Derive suppkey from partkey using same formula as partsupp,
			// ensuring lineitem's (l_partkey, l_suppkey) has matching rows in partsupp.
			partKey := rng.Intn(numParts) // 0-based
			suppIdx := rng.Intn(4)        // pick one of 4 suppliers for this part
			suppKey := (partKey*4+suppIdx)%numSupps + 1

			rows = append(rows, map[string]any{
				"l_orderkey":      int32(orderKey),
				"l_partkey":       int32(partKey + 1),
				"l_suppkey":       int32(suppKey),
				"l_linenumber":    int32(ln),
				"l_quantity":      quantity,
				"l_extendedprice": price,
				"l_discount":      discount,
				"l_tax":           tax,
				"l_returnflag":    flags[rng.Intn(3)],
				"l_linestatus":    lineStatuses[rng.Intn(2)],
				"l_shipdate":      shipDate.Format("2006-01-02"),
				"l_commitdate":    commitDate.Format("2006-01-02"),
				"l_receiptdate":   receiptDate.Format("2006-01-02"),
				"l_shipinstruct":  shipInstructs[rng.Intn(len(shipInstructs))],
				"l_shipmode":      shipModes[rng.Intn(len(shipModes))],
				"l_comment":       randString(rng, 10, 40),
			})
		}
	}
	return rows
}

// Helper functions

func randString(rng *rand.Rand, minLen, maxLen int) string {
	n := minLen + rng.Intn(maxLen-minLen+1)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + rng.Intn(26))
	}
	return string(b)
}

func randPhone(rng *rand.Rand) string {
	return fmt.Sprintf("%02d-%03d-%03d-%04d",
		rng.Intn(25)+10, rng.Intn(1000), rng.Intn(1000), rng.Intn(10000))
}

// randCents draws a monetary value in [lo, hi) and returns its EXACT
// hundredths. It replaced randFloat, which drew the same value and then
// divided the same truncated integer by 100 — so the FLOAT64 fixture's values
// are bit-for-bit what they were, and the DECIMAL fixture gets the integer
// before any float64 has touched it.
func randCents(rng *rand.Rand, lo, hi float64) int64 {
	v := lo + rng.Float64()*(hi-lo)
	return int64(v * 100) // 2 decimal places
}

var partNameWords = []string{
	"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
	"blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse",
	"chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan",
	"dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest",
	"frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
	"hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon",
	"light", "lime", "linen", "magenta", "maroon", "medium", "metallic",
	"midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive",
	"orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum",
	"powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle",
	"salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke",
	"snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise",
	"violet", "wheat", "white", "yellow",
}

func randPartName(rng *rand.Rand) string {
	return partNameWords[rng.Intn(len(partNameWords))] + " " +
		partNameWords[rng.Intn(len(partNameWords))] + " " +
		partNameWords[rng.Intn(len(partNameWords))]
}

// Streaming generators — identical logic to gen* but emit rows via chunkEmitter
// to bound memory usage at large scale factors.

func streamSupplier(rng *rand.Rand, count int, f Fixture, e *chunkEmitter) {
	for i := 0; i < count; i++ {
		e.add(map[string]any{
			"s_suppkey":   int32(i + 1),
			"s_name":      fmt.Sprintf("Supplier#%09d", i+1),
			"s_address":   randString(rng, 10, 25),
			"s_nationkey": int32(rng.Intn(25)),
			"s_phone":     randPhone(rng),
			"s_acctbal":   f.money(randCents(rng, -999.99, 9999.99)),
			"s_comment":   randString(rng, 20, 80),
		})
	}
}

func streamPart(rng *rand.Rand, count int, f Fixture, e *chunkEmitter) {
	for i := 0; i < count; i++ {
		e.add(map[string]any{
			"p_partkey":     int32(i + 1),
			"p_name":        randPartName(rng),
			"p_mfgr":        fmt.Sprintf("Manufacturer#%d", rng.Intn(5)+1),
			"p_brand":       brands[rng.Intn(len(brands))],
			"p_type":        partTypes[rng.Intn(len(partTypes))],
			"p_size":        int32(rng.Intn(50) + 1),
			"p_container":   containers[rng.Intn(len(containers))],
			"p_retailprice": f.money(int64(90000 + i*3 + int(rng.Int31n(1000)))),
			"p_comment":     randString(rng, 5, 20),
		})
	}
}

func streamPartSupp(rng *rand.Rand, numParts, numSupps, count int, f Fixture, e *chunkEmitter) {
	suppPerPart := 4
	if count < numParts*4 {
		suppPerPart = max(1, count/max(1, numParts))
	}
	n := 0
	for i := 0; i < numParts && n < count; i++ {
		for j := 0; j < suppPerPart && n < count; j++ {
			suppKey := (i*suppPerPart+j)%numSupps + 1
			e.add(map[string]any{
				"ps_partkey":    int32(i + 1),
				"ps_suppkey":    int32(suppKey),
				"ps_availqty":   int32(rng.Intn(9999) + 1),
				"ps_supplycost": f.money(randCents(rng, 1.0, 1000.0)),
				"ps_comment":    randString(rng, 20, 120),
			})
			n++
		}
	}
}

func streamCustomer(rng *rand.Rand, count int, f Fixture, e *chunkEmitter) {
	for i := 0; i < count; i++ {
		e.add(map[string]any{
			"c_custkey":    int32(i + 1),
			"c_name":       fmt.Sprintf("Customer#%09d", i+1),
			"c_address":    randString(rng, 10, 25),
			"c_nationkey":  int32(rng.Intn(25)),
			"c_phone":      randPhone(rng),
			"c_acctbal":    f.money(randCents(rng, -999.99, 9999.99)),
			"c_mktsegment": mktSegments[rng.Intn(len(mktSegments))],
			"c_comment":    randString(rng, 20, 80),
		})
	}
}

func streamOrders(rng *rand.Rand, count, numCusts int, f Fixture, e *chunkEmitter) {
	statuses := []string{"F", "O", "P"}
	custRange := max(1, numCusts*2/3)
	for i := 0; i < count; i++ {
		year := 1992 + rng.Intn(6)
		month := rng.Intn(12) + 1
		day := rng.Intn(28) + 1
		e.add(map[string]any{
			"o_orderkey":      int32(i + 1),
			"o_custkey":       int32(rng.Intn(custRange) + 1),
			"o_orderstatus":   statuses[rng.Intn(3)],
			"o_totalprice":    f.money(randCents(rng, 1000.0, 500000.0)),
			"o_orderdate":     fmt.Sprintf("%04d-%02d-%02d", year, month, day),
			"o_orderpriority": priorities[rng.Intn(len(priorities))],
			"o_clerk":         fmt.Sprintf("Clerk#%09d", rng.Intn(max(1, count/1000))+1),
			"o_shippriority":  int32(0),
			"o_comment":       randString(rng, 20, 60),
		})
	}
}

func streamLineItem(rng *rand.Rand, numOrders, count, numParts, numSupps int, f Fixture, e *chunkEmitter) {
	flags := []string{"N", "R", "A"}
	lineStatuses := []string{"O", "F"}

	n := 0
	for orderKey := 1; orderKey <= numOrders && n < count; orderKey++ {
		nLines := 1 + rng.Intn(7) // TPC-H spec: 1-7 line items per order
		for ln := 1; ln <= nLines && n < count; ln++ {
			quantity := float64(rng.Intn(50) + 1)
			price := f.money(randCents(rng, 900.0, 100000.0))
			discount := f.money(int64(rng.Intn(11)))
			tax := f.money(int64(rng.Intn(9)))

			// TPC-H spec date generation:
			// L_COMMITDATE = O_ORDERDATE + random(5, 120) days
			// L_SHIPDATE   = L_COMMITDATE + random(-10, 40) days
			// L_RECEIPTDATE = L_SHIPDATE + random(1, 30) days
			year := 1992 + rng.Intn(6)
			month := rng.Intn(12) + 1
			day := rng.Intn(28) + 1
			orderDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
			commitDate := orderDate.AddDate(0, 0, rng.Intn(116)+5) // +5..120 days
			shipDate := commitDate.AddDate(0, 0, rng.Intn(51)-10)  // -10..+40 days
			receiptDate := shipDate.AddDate(0, 0, rng.Intn(30)+1)  // +1..30 days

			// Derive suppkey from partkey using same formula as partsupp
			partKey := rng.Intn(numParts) // 0-based
			suppIdx := rng.Intn(4)        // pick one of 4 suppliers for this part
			suppKey := (partKey*4+suppIdx)%numSupps + 1

			e.add(map[string]any{
				"l_orderkey":      int32(orderKey),
				"l_partkey":       int32(partKey + 1),
				"l_suppkey":       int32(suppKey),
				"l_linenumber":    int32(ln),
				"l_quantity":      quantity,
				"l_extendedprice": price,
				"l_discount":      discount,
				"l_tax":           tax,
				"l_returnflag":    flags[rng.Intn(3)],
				"l_linestatus":    lineStatuses[rng.Intn(2)],
				"l_shipdate":      shipDate.Format("2006-01-02"),
				"l_commitdate":    commitDate.Format("2006-01-02"),
				"l_receiptdate":   receiptDate.Format("2006-01-02"),
				"l_shipinstruct":  shipInstructs[rng.Intn(len(shipInstructs))],
				"l_shipmode":      shipModes[rng.Intn(len(shipModes))],
				"l_comment":       randString(rng, 10, 40),
			})
			n++
		}
	}
}
