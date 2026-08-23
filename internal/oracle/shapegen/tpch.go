package shapegen

// TPCH is the generation universe for the TPC-H schema as Wadjet stores it:
// monetary columns are FLOAT64, date columns are ISO-8601 STRINGS (so ordering
// and comparison are lexicographic, which agrees with date semantics), and
// every table declares the column set that makes a row unique.
//
// Literal pools are drawn from the real SF0.01 domains so predicates are
// selective without being always-empty — a generator whose every WHERE returns
// zero rows compares nothing.
func TPCH() *Schema {
	return &Schema{
		Tables: []Table{
			{Name: "region", SelfJoin: true, PK: []string{"r_regionkey"}, Cols: []Column{
				{Name: "r_regionkey", Kind: KindInt, Lits: []string{"0", "2", "4"}},
				{Name: "r_name", Kind: KindText, Lits: []string{"'ASIA'", "'EUROPE'", "'AMERICA'", "'AFRICA'"}},
				{Name: "r_comment", Kind: KindText},
			}},
			{Name: "nation", SelfJoin: true, PK: []string{"n_nationkey"}, Cols: []Column{
				{Name: "n_nationkey", Kind: KindInt, Lits: []string{"3", "10", "20"}},
				{Name: "n_name", Kind: KindText, Lits: []string{"'FRANCE'", "'CHINA'", "'BRAZIL'", "'GERMANY'"}},
				{Name: "n_regionkey", Kind: KindInt, Lits: []string{"0", "1", "3"}},
				{Name: "n_comment", Kind: KindText},
			}},
			{Name: "supplier", SelfJoin: true, PK: []string{"s_suppkey"}, Cols: []Column{
				{Name: "s_suppkey", Kind: KindInt, Lits: []string{"3", "50", "99"}},
				{Name: "s_name", Kind: KindText},
				{Name: "s_address", Kind: KindText},
				{Name: "s_nationkey", Kind: KindInt, Lits: []string{"1", "12", "23"}},
				{Name: "s_phone", Kind: KindText},
				{Name: "s_acctbal", Kind: KindFloat, Lits: []string{"100.0", "4500.0", "9000.0"}},
				{Name: "s_comment", Kind: KindText},
			}},
			{Name: "part", SelfJoin: true, PK: []string{"p_partkey"}, Cols: []Column{
				{Name: "p_partkey", Kind: KindInt, Lits: []string{"5", "500", "1999"}},
				{Name: "p_name", Kind: KindText},
				{Name: "p_mfgr", Kind: KindText, Lits: []string{"'Manufacturer#1'", "'Manufacturer#4'"}},
				{Name: "p_brand", Kind: KindText, Lits: []string{"'Brand#13'", "'Brand#42'", "'Brand#51'"}},
				{Name: "p_type", Kind: KindText, Lits: []string{"'PROMO BURNISHED COPPER'", "'STANDARD ANODIZED TIN'"}},
				{Name: "p_size", Kind: KindInt, Lits: []string{"5", "15", "40"}},
				{Name: "p_container", Kind: KindText, Lits: []string{"'SM BOX'", "'LG CASE'", "'MED BAG'"}},
				{Name: "p_retailprice", Kind: KindFloat, Lits: []string{"950.0", "1500.0", "1900.0"}},
				{Name: "p_comment", Kind: KindText},
			}},
			{Name: "partsupp", PK: []string{"ps_partkey", "ps_suppkey"}, Cols: []Column{
				{Name: "ps_partkey", Kind: KindInt, Lits: []string{"5", "500", "1999"}},
				{Name: "ps_suppkey", Kind: KindInt, Lits: []string{"3", "50", "99"}},
				{Name: "ps_availqty", Kind: KindInt, Lits: []string{"100", "5000", "9000"}},
				{Name: "ps_supplycost", Kind: KindFloat, Lits: []string{"50.0", "500.0", "900.0"}},
				{Name: "ps_comment", Kind: KindText},
			}},
			{Name: "customer", SelfJoin: true, PK: []string{"c_custkey"}, Cols: []Column{
				{Name: "c_custkey", Kind: KindInt, Lits: []string{"10", "150", "1499"}},
				{Name: "c_name", Kind: KindText},
				{Name: "c_address", Kind: KindText},
				{Name: "c_nationkey", Kind: KindInt, Lits: []string{"2", "9", "21"}},
				{Name: "c_phone", Kind: KindText},
				{Name: "c_acctbal", Kind: KindFloat, Lits: []string{"0.0", "3000.0", "8000.0"}},
				{Name: "c_mktsegment", Kind: KindText, Lits: []string{"'BUILDING'", "'AUTOMOBILE'", "'MACHINERY'"}},
				{Name: "c_comment", Kind: KindText},
			}},
			{Name: "orders", PK: []string{"o_orderkey"}, Cols: []Column{
				{Name: "o_orderkey", Kind: KindInt, Lits: []string{"100", "5000", "12000"}},
				{Name: "o_custkey", Kind: KindInt, Lits: []string{"15", "700", "1400"}},
				{Name: "o_orderstatus", Kind: KindText, Lits: []string{"'F'", "'O'", "'P'"}},
				{Name: "o_totalprice", Kind: KindFloat, Lits: []string{"50000.0", "200000.0", "400000.0"}},
				{Name: "o_orderdate", Kind: KindDate, Lits: []string{"'1993-01-01'", "'1994-06-30'", "'1996-12-31'"}},
				{Name: "o_orderpriority", Kind: KindText, Lits: []string{"'1-URGENT'", "'3-MEDIUM'", "'5-LOW'"}},
				{Name: "o_clerk", Kind: KindText},
				{Name: "o_shippriority", Kind: KindInt, Lits: []string{"0"}},
				{Name: "o_comment", Kind: KindText},
			}},
			{Name: "lineitem", PK: []string{"l_orderkey", "l_linenumber"}, Cols: []Column{
				{Name: "l_orderkey", Kind: KindInt, Lits: []string{"100", "5000", "12000"}},
				{Name: "l_partkey", Kind: KindInt, Lits: []string{"5", "500", "1999"}},
				{Name: "l_suppkey", Kind: KindInt, Lits: []string{"3", "50", "99"}},
				{Name: "l_linenumber", Kind: KindInt, Lits: []string{"1", "3", "6"}},
				{Name: "l_quantity", Kind: KindFloat, Lits: []string{"10.0", "25.0", "45.0"}},
				{Name: "l_extendedprice", Kind: KindFloat, Lits: []string{"10000.0", "40000.0", "70000.0"}},
				{Name: "l_discount", Kind: KindFloat, Lits: []string{"0.02", "0.05", "0.08"}},
				{Name: "l_tax", Kind: KindFloat, Lits: []string{"0.02", "0.06"}},
				{Name: "l_returnflag", Kind: KindText, Lits: []string{"'R'", "'N'", "'A'"}},
				{Name: "l_linestatus", Kind: KindText, Lits: []string{"'F'", "'O'"}},
				{Name: "l_shipdate", Kind: KindDate, Lits: []string{"'1993-06-01'", "'1994-09-15'", "'1996-03-31'"}},
				{Name: "l_commitdate", Kind: KindDate, Lits: []string{"'1993-06-01'", "'1995-01-15'", "'1996-06-30'"}},
				{Name: "l_receiptdate", Kind: KindDate, Lits: []string{"'1993-07-01'", "'1995-02-15'", "'1996-07-30'"}},
				{Name: "l_shipinstruct", Kind: KindText, Lits: []string{"'DELIVER IN PERSON'", "'NONE'", "'TAKE BACK RETURN'"}},
				{Name: "l_shipmode", Kind: KindText, Lits: []string{"'AIR'", "'MAIL'", "'SHIP'", "'RAIL'"}},
				{Name: "l_comment", Kind: KindText},
			}},
		},
		Edges: []Edge{
			{LTable: "lineitem", LCol: "l_orderkey", RTable: "orders", RCol: "o_orderkey"},
			{LTable: "lineitem", LCol: "l_partkey", RTable: "part", RCol: "p_partkey"},
			{LTable: "lineitem", LCol: "l_suppkey", RTable: "supplier", RCol: "s_suppkey"},
			{LTable: "orders", LCol: "o_custkey", RTable: "customer", RCol: "c_custkey"},
			{LTable: "customer", LCol: "c_nationkey", RTable: "nation", RCol: "n_nationkey"},
			{LTable: "supplier", LCol: "s_nationkey", RTable: "nation", RCol: "n_nationkey"},
			{LTable: "nation", LCol: "n_regionkey", RTable: "region", RCol: "r_regionkey"},
			{LTable: "partsupp", LCol: "ps_partkey", RTable: "part", RCol: "p_partkey"},
			{LTable: "partsupp", LCol: "ps_suppkey", RTable: "supplier", RCol: "s_suppkey"},
		},
	}
}
