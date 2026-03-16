package tpch

import "github.com/derekmwright/caelum/internal/storage/parquet"

// TPC-H table schemas adapted for Caelum's type system.
// Monetary values use FLOAT64 (DECIMAL would be ideal but Caelum uses float).
// Dates use DATE type (int32 days since epoch).

var RegionSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "r_regionkey", Type: parquet.TypeInt32},
		{Name: "r_name", Type: parquet.TypeString},
		{Name: "r_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var NationSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "n_nationkey", Type: parquet.TypeInt32},
		{Name: "n_name", Type: parquet.TypeString},
		{Name: "n_regionkey", Type: parquet.TypeInt32},
		{Name: "n_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var SupplierSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "s_suppkey", Type: parquet.TypeInt32},
		{Name: "s_name", Type: parquet.TypeString},
		{Name: "s_address", Type: parquet.TypeString},
		{Name: "s_nationkey", Type: parquet.TypeInt32},
		{Name: "s_phone", Type: parquet.TypeString},
		{Name: "s_acctbal", Type: parquet.TypeFloat64},
		{Name: "s_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var PartSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "p_partkey", Type: parquet.TypeInt32},
		{Name: "p_name", Type: parquet.TypeString},
		{Name: "p_mfgr", Type: parquet.TypeString},
		{Name: "p_brand", Type: parquet.TypeString},
		{Name: "p_type", Type: parquet.TypeString},
		{Name: "p_size", Type: parquet.TypeInt32},
		{Name: "p_container", Type: parquet.TypeString},
		{Name: "p_retailprice", Type: parquet.TypeFloat64},
		{Name: "p_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var PartSuppSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "ps_partkey", Type: parquet.TypeInt32},
		{Name: "ps_suppkey", Type: parquet.TypeInt32},
		{Name: "ps_availqty", Type: parquet.TypeInt32},
		{Name: "ps_supplycost", Type: parquet.TypeFloat64},
		{Name: "ps_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var CustomerSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "c_custkey", Type: parquet.TypeInt32},
		{Name: "c_name", Type: parquet.TypeString},
		{Name: "c_address", Type: parquet.TypeString},
		{Name: "c_nationkey", Type: parquet.TypeInt32},
		{Name: "c_phone", Type: parquet.TypeString},
		{Name: "c_acctbal", Type: parquet.TypeFloat64},
		{Name: "c_mktsegment", Type: parquet.TypeString},
		{Name: "c_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var OrdersSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt32},
		{Name: "o_custkey", Type: parquet.TypeInt32},
		{Name: "o_orderstatus", Type: parquet.TypeString},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
		{Name: "o_orderdate", Type: parquet.TypeString}, // YYYY-MM-DD string for easy comparison
		{Name: "o_orderpriority", Type: parquet.TypeString},
		{Name: "o_clerk", Type: parquet.TypeString},
		{Name: "o_shippriority", Type: parquet.TypeInt32},
		{Name: "o_comment", Type: parquet.TypeString, Nullable: true},
	},
}

var LineItemSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt32},
		{Name: "l_partkey", Type: parquet.TypeInt32},
		{Name: "l_suppkey", Type: parquet.TypeInt32},
		{Name: "l_linenumber", Type: parquet.TypeInt32},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
		{Name: "l_extendedprice", Type: parquet.TypeFloat64},
		{Name: "l_discount", Type: parquet.TypeFloat64},
		{Name: "l_tax", Type: parquet.TypeFloat64},
		{Name: "l_returnflag", Type: parquet.TypeString},
		{Name: "l_linestatus", Type: parquet.TypeString},
		{Name: "l_shipdate", Type: parquet.TypeString},
		{Name: "l_commitdate", Type: parquet.TypeString},
		{Name: "l_receiptdate", Type: parquet.TypeString},
		{Name: "l_shipinstruct", Type: parquet.TypeString},
		{Name: "l_shipmode", Type: parquet.TypeString},
		{Name: "l_comment", Type: parquet.TypeString, Nullable: true},
	},
}

// AllTables maps table names to their schemas.
var AllTables = map[string]parquet.Schema{
	"region":   RegionSchema,
	"nation":   NationSchema,
	"supplier": SupplierSchema,
	"part":     PartSchema,
	"partsupp": PartSuppSchema,
	"customer": CustomerSchema,
	"orders":   OrdersSchema,
	"lineitem": LineItemSchema,
}
