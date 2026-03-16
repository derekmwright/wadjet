package tpch

import (
	"fmt"
	"math/rand"
)

// ScaleFactor controls data volume. SF=0.01 is ~10MB, SF=1 is ~1GB.
type ScaleFactor float64

const (
	SF001 ScaleFactor = 0.01
	SF01  ScaleFactor = 0.1
	SF1   ScaleFactor = 1.0
	SF10  ScaleFactor = 10.0
)

// RowCounts returns the row counts for each table at the given scale factor.
func (sf ScaleFactor) RowCounts() TableCounts {
	f := float64(sf)
	return TableCounts{
		Region:   5,           // fixed
		Nation:   25,          // fixed
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
var brands    []string
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

// Generate produces all TPC-H tables at the given scale factor.
// Returns a map of table name → rows. Deterministic for a given SF.
func Generate(sf ScaleFactor) map[string][]map[string]any {
	rng := rand.New(rand.NewSource(int64(sf * 42_000_000)))
	counts := sf.RowCounts()
	data := make(map[string][]map[string]any, 8)

	data["region"] = genRegion()
	data["nation"] = genNation()
	data["supplier"] = genSupplier(rng, counts.Supplier)
	data["part"] = genPart(rng, counts.Part)
	data["partsupp"] = genPartSupp(rng, counts.Part, counts.Supplier, counts.PartSupp)
	data["customer"] = genCustomer(rng, counts.Customer)
	data["orders"] = genOrders(rng, counts.Orders, counts.Customer)
	data["lineitem"] = genLineItem(rng, counts.Orders, counts.LineItem, counts.Part, counts.Supplier)

	return data
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

func genSupplier(rng *rand.Rand, count int) []map[string]any {
	rows := make([]map[string]any, count)
	for i := range rows {
		rows[i] = map[string]any{
			"s_suppkey":   int32(i + 1),
			"s_name":      fmt.Sprintf("Supplier#%09d", i+1),
			"s_address":   randString(rng, 10, 25),
			"s_nationkey": int32(rng.Intn(25)),
			"s_phone":     randPhone(rng),
			"s_acctbal":   randFloat(rng, -999.99, 9999.99),
			"s_comment":   randString(rng, 20, 80),
		}
	}
	return rows
}

func genPart(rng *rand.Rand, count int) []map[string]any {
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
			"p_retailprice": float64(90000+i*3+int(rng.Int31n(1000))) / 100.0,
			"p_comment":     randString(rng, 5, 20),
		}
	}
	return rows
}

func genPartSupp(rng *rand.Rand, numParts, numSupps, count int) []map[string]any {
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
				"ps_supplycost": randFloat(rng, 1.0, 1000.0),
				"ps_comment":    randString(rng, 20, 120),
			})
		}
	}
	return rows
}

func genCustomer(rng *rand.Rand, count int) []map[string]any {
	rows := make([]map[string]any, count)
	for i := range rows {
		rows[i] = map[string]any{
			"c_custkey":    int32(i + 1),
			"c_name":       fmt.Sprintf("Customer#%09d", i+1),
			"c_address":    randString(rng, 10, 25),
			"c_nationkey":  int32(rng.Intn(25)),
			"c_phone":      randPhone(rng),
			"c_acctbal":    randFloat(rng, -999.99, 9999.99),
			"c_mktsegment": mktSegments[rng.Intn(len(mktSegments))],
			"c_comment":    randString(rng, 20, 80),
		}
	}
	return rows
}

func genOrders(rng *rand.Rand, count, numCusts int) []map[string]any {
	rows := make([]map[string]any, count)
	statuses := []string{"F", "O", "P"}
	for i := range rows {
		year := 1992 + rng.Intn(6) // 1992-1997
		month := rng.Intn(12) + 1
		day := rng.Intn(28) + 1
		rows[i] = map[string]any{
			"o_orderkey":     int32(i + 1),
			"o_custkey":      int32(rng.Intn(numCusts) + 1),
			"o_orderstatus":  statuses[rng.Intn(3)],
			"o_totalprice":   randFloat(rng, 1000.0, 500000.0),
			"o_orderdate":    fmt.Sprintf("%04d-%02d-%02d", year, month, day),
			"o_orderpriority": priorities[rng.Intn(len(priorities))],
			"o_clerk":        fmt.Sprintf("Clerk#%09d", rng.Intn(max(1, count/1000))+1),
			"o_shippriority": int32(0),
			"o_comment":      randString(rng, 20, 60),
		}
	}
	return rows
}

func genLineItem(rng *rand.Rand, numOrders, count, numParts, numSupps int) []map[string]any {
	rows := make([]map[string]any, 0, count)
	flags := []string{"N", "R", "A"}
	lineStatuses := []string{"O", "F"}

	// Distribute ~count items across numOrders orders
	linesPerOrder := max(1, count/max(1, numOrders))

	for orderKey := 1; orderKey <= numOrders && len(rows) < count; orderKey++ {
		nLines := linesPerOrder + rng.Intn(3) - 1
		if nLines < 1 {
			nLines = 1
		}
		for ln := 1; ln <= nLines && len(rows) < count; ln++ {
			quantity := float64(rng.Intn(50) + 1)
			price := randFloat(rng, 900.0, 100000.0)
			discount := float64(rng.Intn(11)) / 100.0
			tax := float64(rng.Intn(9)) / 100.0

			// Ship date is order date + 1-120 days (simplified)
			year := 1992 + rng.Intn(6)
			month := rng.Intn(12) + 1
			day := rng.Intn(28) + 1
			shipYear := year
			shipMonth := month + rng.Intn(4)
			if shipMonth > 12 {
				shipYear++
				shipMonth -= 12
			}

			rows = append(rows, map[string]any{
				"l_orderkey":     int32(orderKey),
				"l_partkey":      int32(rng.Intn(numParts) + 1),
				"l_suppkey":      int32(rng.Intn(numSupps) + 1),
				"l_linenumber":   int32(ln),
				"l_quantity":     quantity,
				"l_extendedprice": price,
				"l_discount":     discount,
				"l_tax":          tax,
				"l_returnflag":   flags[rng.Intn(3)],
				"l_linestatus":   lineStatuses[rng.Intn(2)],
				"l_shipdate":     fmt.Sprintf("%04d-%02d-%02d", shipYear, shipMonth, day),
				"l_commitdate":   fmt.Sprintf("%04d-%02d-%02d", year, month+1, day),
				"l_receiptdate":  fmt.Sprintf("%04d-%02d-%02d", shipYear, shipMonth, min(28, day+rng.Intn(10))),
				"l_shipinstruct": shipInstructs[rng.Intn(len(shipInstructs))],
				"l_shipmode":     shipModes[rng.Intn(len(shipModes))],
				"l_comment":      randString(rng, 10, 40),
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

func randFloat(rng *rand.Rand, lo, hi float64) float64 {
	v := lo + rng.Float64()*(hi-lo)
	return float64(int64(v*100)) / 100.0 // 2 decimal places
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
