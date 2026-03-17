package tpch

// TPCHQueries contains all 22 TPC-H queries adapted for Wadjet's SQL dialect.
// Dates use string comparisons (YYYY-MM-DD format is lexicographically ordered).
// DECIMAL is replaced with FLOAT64. SUBSTRING uses SUBSTR.
//
// Queries that require features not yet supported are marked with a comment
// and provided in a simplified form that exercises the same join/agg patterns.

var TPCHQueries = map[int]QueryDef{

	// Q1: Pricing Summary Report
	// Tests: large scan, filter, GROUP BY, multiple aggregates
	1: {
		Name: "Pricing Summary Report",
		SQL: `SELECT
			l_returnflag,
			l_linestatus,
			SUM(l_quantity) as sum_qty,
			SUM(l_extendedprice) as sum_base_price,
			SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
			SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
			AVG(l_quantity) as avg_qty,
			AVG(l_extendedprice) as avg_price,
			AVG(l_discount) as avg_disc,
			COUNT(*) as count_order
		FROM lineitem
		WHERE l_shipdate <= '1998-09-02'
		GROUP BY l_returnflag, l_linestatus
		ORDER BY l_returnflag, l_linestatus`,
	},

	// Q2: Minimum Cost Supplier
	// Tests: multi-join, correlated subquery, ORDER BY with LIMIT
	2: {
		Name: "Minimum Cost Supplier",
		SQL: `SELECT
			s_acctbal, s_name, n_name, p_partkey, p_mfgr,
			s_address, s_phone, s_comment
		FROM part
		JOIN partsupp ON p_partkey = ps_partkey
		JOIN supplier ON s_suppkey = ps_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE r_name = 'EUROPE'
			AND p_size = 15
			AND p_type LIKE '%BRASS'
			AND ps_supplycost = (
				SELECT MIN(ps_supplycost)
				FROM partsupp
				JOIN supplier ON s_suppkey = ps_suppkey
				JOIN nation ON s_nationkey = n_nationkey
				JOIN region ON n_regionkey = r_regionkey
				WHERE ps_partkey = p_partkey
					AND r_name = 'EUROPE'
			)
		ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
		LIMIT 100`,
	},

	// Q3: Shipping Priority
	// Tests: 3-way join, filter, GROUP BY, ORDER BY
	3: {
		Name: "Shipping Priority",
		SQL: `SELECT
			l_orderkey,
			SUM(l_extendedprice * (1 - l_discount)) as revenue,
			o_orderdate,
			o_shippriority
		FROM customer
		JOIN orders ON c_custkey = o_custkey
		JOIN lineitem ON l_orderkey = o_orderkey
		WHERE c_mktsegment = 'BUILDING'
			AND o_orderdate < '1995-03-15'
			AND l_shipdate > '1995-03-15'
		GROUP BY l_orderkey, o_orderdate, o_shippriority
		ORDER BY revenue DESC, o_orderdate
		LIMIT 10`,
	},

	// Q4: Order Priority Checking
	// Tests: EXISTS subquery, GROUP BY, ORDER BY
	4: {
		Name: "Order Priority Checking",
		SQL: `SELECT
			o_orderpriority,
			COUNT(*) as order_count
		FROM orders
		WHERE o_orderdate >= '1993-07-01'
			AND o_orderdate < '1993-10-01'
			AND EXISTS (
				SELECT 1 FROM lineitem
				WHERE l_orderkey = o_orderkey
					AND l_commitdate < l_receiptdate
			)
		GROUP BY o_orderpriority
		ORDER BY o_orderpriority`,
	},

	// Q5: Local Supplier Volume
	// Tests: 6-way join, GROUP BY, ORDER BY
	5: {
		Name: "Local Supplier Volume",
		SQL: `SELECT
			n_name,
			SUM(l_extendedprice * (1 - l_discount)) as revenue
		FROM customer
		JOIN orders ON c_custkey = o_custkey
		JOIN lineitem ON l_orderkey = o_orderkey
		JOIN supplier ON l_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		JOIN region ON n_regionkey = r_regionkey
		WHERE c_nationkey = s_nationkey
			AND r_name = 'ASIA'
			AND o_orderdate >= '1994-01-01'
			AND o_orderdate < '1995-01-01'
		GROUP BY n_name
		ORDER BY revenue DESC`,
	},

	// Q6: Forecasting Revenue Change
	// Tests: single-table scan, multiple filters, single aggregate
	6: {
		Name: "Forecasting Revenue Change",
		SQL: `SELECT
			SUM(l_extendedprice * l_discount) as revenue
		FROM lineitem
		WHERE l_shipdate >= '1994-01-01'
			AND l_shipdate < '1995-01-01'
			AND l_discount >= 0.05
			AND l_discount <= 0.07
			AND l_quantity < 24`,
	},

	// Q7: Volume Shipping
	// Tests: multi-join, CASE WHEN, date extraction, GROUP BY
	7: {
		Name: "Volume Shipping",
		SQL: `SELECT
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			SUBSTR(l_shipdate, 1, 4) as l_year,
			SUM(l_extendedprice * (1 - l_discount)) as revenue
		FROM supplier
		JOIN lineitem ON s_suppkey = l_suppkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN customer ON c_custkey = o_custkey
		JOIN nation n1 ON s_nationkey = n1.n_nationkey
		JOIN nation n2 ON c_nationkey = n2.n_nationkey
		WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
			AND l_shipdate >= '1995-01-01'
			AND l_shipdate <= '1996-12-31'
		GROUP BY n1.n_name, n2.n_name, SUBSTR(l_shipdate, 1, 4)
		ORDER BY supp_nation, cust_nation, l_year`,
	},

	// Q8: National Market Share
	// Tests: multi-join, CASE WHEN, aggregate expressions
	8: {
		Name: "National Market Share",
		SQL: `SELECT
			SUBSTR(o_orderdate, 1, 4) as o_year,
			SUM(CASE WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) as brazil_revenue,
			SUM(l_extendedprice * (1 - l_discount)) as total_revenue
		FROM part
		JOIN lineitem ON p_partkey = l_partkey
		JOIN supplier ON s_suppkey = l_suppkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN customer ON c_custkey = o_custkey
		JOIN nation n1 ON c_nationkey = n1.n_nationkey
		JOIN region ON n1.n_regionkey = r_regionkey
		JOIN nation n2 ON s_nationkey = n2.n_nationkey
		WHERE r_name = 'AMERICA'
			AND o_orderdate >= '1995-01-01'
			AND o_orderdate <= '1996-12-31'
			AND p_type = 'ECONOMY ANODIZED STEEL'
		GROUP BY SUBSTR(o_orderdate, 1, 4)
		ORDER BY o_year`,
	},

	// Q9: Product Type Profit Measure
	// Tests: multi-join, LIKE, arithmetic expressions
	9: {
		Name: "Product Type Profit Measure",
		SQL: `SELECT
			n_name as nation,
			SUBSTR(o_orderdate, 1, 4) as o_year,
			SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
		FROM part
		JOIN lineitem ON p_partkey = l_partkey
		JOIN supplier ON s_suppkey = l_suppkey
		JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE p_name LIKE '%green%'
		GROUP BY n_name, SUBSTR(o_orderdate, 1, 4)
		ORDER BY nation, o_year DESC`,
	},

	// Q10: Returned Item Reporting
	// Tests: 4-way join, GROUP BY, ORDER BY, LIMIT
	10: {
		Name: "Returned Item Reporting",
		SQL: `SELECT
			c_custkey, c_name,
			SUM(l_extendedprice * (1 - l_discount)) as revenue,
			c_acctbal, n_name, c_address, c_phone, c_comment
		FROM customer
		JOIN orders ON c_custkey = o_custkey
		JOIN lineitem ON l_orderkey = o_orderkey
		JOIN nation ON c_nationkey = n_nationkey
		WHERE o_orderdate >= '1993-10-01'
			AND o_orderdate < '1994-01-01'
			AND l_returnflag = 'R'
		GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
		ORDER BY revenue DESC
		LIMIT 20`,
	},

	// Q11: Important Stock Identification
	// Tests: multi-join, HAVING with correlated subquery
	11: {
		Name: "Important Stock Identification",
		SQL: `SELECT
			ps_partkey,
			SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp
		JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > (
			SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
			FROM partsupp
			JOIN supplier ON ps_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			WHERE n_name = 'GERMANY'
		)
		ORDER BY value DESC`,
	},

	// Q12: Shipping Modes and Order Priority
	// Tests: join, CASE WHEN, GROUP BY
	12: {
		Name: "Shipping Modes and Order Priority",
		SQL: `SELECT
			l_shipmode,
			SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count,
			SUM(CASE WHEN o_orderpriority != '1-URGENT' AND o_orderpriority != '2-HIGH' THEN 1 ELSE 0 END) as low_line_count
		FROM orders
		JOIN lineitem ON o_orderkey = l_orderkey
		WHERE l_shipmode IN ('MAIL', 'SHIP')
			AND l_commitdate < l_receiptdate
			AND l_shipdate < l_commitdate
			AND l_receiptdate >= '1994-01-01'
			AND l_receiptdate < '1995-01-01'
		GROUP BY l_shipmode
		ORDER BY l_shipmode`,
	},

	// Q13: Customer Distribution
	// Tests: LEFT JOIN, subquery in FROM (simplified to LEFT JOIN + GROUP BY)
	13: {
		Name: "Customer Distribution",
		SQL: `SELECT
			c_custkey,
			COUNT(o_orderkey) as c_count
		FROM customer
		LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
		GROUP BY c_custkey
		ORDER BY c_count DESC, c_custkey
		LIMIT 100`,
	},

	// Q14: Promotion Effect
	// Tests: join, CASE WHEN in aggregate, arithmetic
	14: {
		Name: "Promotion Effect",
		SQL: `SELECT
			SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) as promo_revenue,
			SUM(l_extendedprice * (1 - l_discount)) as total_revenue
		FROM lineitem
		JOIN part ON l_partkey = p_partkey
		WHERE l_shipdate >= '1995-09-01'
			AND l_shipdate < '1995-10-01'`,
	},

	// Q15: Top Supplier
	// Tests: CTE (WITH), aggregate, join, subquery
	15: {
		Name: "Top Supplier",
		SQL: `WITH revenue AS (
			SELECT
				l_suppkey as supplier_no,
				SUM(l_extendedprice * (1 - l_discount)) as total_revenue
			FROM lineitem
			WHERE l_shipdate >= '1996-01-01'
				AND l_shipdate < '1996-04-01'
			GROUP BY l_suppkey
		)
		SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
		FROM supplier
		JOIN revenue ON s_suppkey = supplier_no
		WHERE total_revenue = (
			SELECT MAX(total_revenue) FROM revenue
		)
		ORDER BY s_suppkey`,
	},

	// Q16: Parts/Supplier Relationship
	// Tests: multi-join, NOT IN, GROUP BY, COUNT DISTINCT, ORDER BY
	16: {
		Name: "Parts/Supplier Relationship",
		SQL: `SELECT
			p_brand, p_type, p_size,
			COUNT(DISTINCT ps_suppkey) as supplier_cnt
		FROM partsupp
		JOIN part ON p_partkey = ps_partkey
		WHERE p_brand != 'Brand#45'
			AND p_type NOT LIKE 'MEDIUM POLISHED%'
			AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
		GROUP BY p_brand, p_type, p_size
		ORDER BY supplier_cnt DESC, p_brand, p_type, p_size`,
	},

	// Q17: Small-Quantity-Order Revenue
	// Tests: correlated subquery, join, aggregate
	17: {
		Name: "Small-Quantity-Order Revenue",
		SQL: `SELECT
			SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem
		JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23'
			AND p_container = 'MED BOX'
			AND l_quantity < (
				SELECT 0.2 * AVG(l_quantity)
				FROM lineitem
				WHERE l_partkey = p_partkey
			)`,
	},

	// Q18: Large Volume Customer
	// Tests: IN subquery with HAVING, multi-join, GROUP BY, ORDER BY
	18: {
		Name: "Large Volume Customer",
		SQL: `SELECT
			c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
			SUM(l_quantity) as total_qty
		FROM customer
		JOIN orders ON c_custkey = o_custkey
		JOIN lineitem ON o_orderkey = l_orderkey
		WHERE o_orderkey IN (
			SELECT l_orderkey
			FROM lineitem
			GROUP BY l_orderkey
			HAVING SUM(l_quantity) > 300
		)
		GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
		ORDER BY o_totalprice DESC, o_orderdate
		LIMIT 100`,
	},

	// Q19: Discounted Revenue
	// Tests: join, complex OR conditions, BETWEEN
	19: {
		Name: "Discounted Revenue",
		SQL: `SELECT
			SUM(l_extendedprice * (1 - l_discount)) as revenue
		FROM lineitem
		JOIN part ON p_partkey = l_partkey
		WHERE (
			p_brand = 'Brand#12'
			AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
			AND l_quantity >= 1 AND l_quantity <= 11
			AND p_size >= 1 AND p_size <= 5
			AND l_shipmode IN ('AIR', 'REG AIR')
			AND l_shipinstruct = 'DELIVER IN PERSON'
		) OR (
			p_brand = 'Brand#23'
			AND p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
			AND l_quantity >= 10 AND l_quantity <= 20
			AND p_size >= 1 AND p_size <= 10
			AND l_shipmode IN ('AIR', 'REG AIR')
			AND l_shipinstruct = 'DELIVER IN PERSON'
		) OR (
			p_brand = 'Brand#34'
			AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
			AND l_quantity >= 20 AND l_quantity <= 30
			AND p_size >= 1 AND p_size <= 15
			AND l_shipmode IN ('AIR', 'REG AIR')
			AND l_shipinstruct = 'DELIVER IN PERSON'
		)`,
	},

	// Q20: Potential Part Promotion
	// Tests: EXISTS, IN subquery, multi-join
	20: {
		Name: "Potential Part Promotion",
		SQL: `SELECT s_name, s_address
		FROM supplier
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'CANADA'
			AND s_suppkey IN (
				SELECT ps_suppkey
				FROM partsupp
				WHERE ps_partkey IN (
					SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
				)
				AND ps_availqty > (
					SELECT 0.5 * SUM(l_quantity)
					FROM lineitem
					WHERE l_partkey = ps_partkey
						AND l_suppkey = ps_suppkey
						AND l_shipdate >= '1994-01-01'
						AND l_shipdate < '1995-01-01'
				)
			)
		ORDER BY s_name`,
	},

	// Q21: Suppliers Who Kept Orders Waiting
	// Tests: EXISTS, NOT EXISTS, multi-join
	21: {
		Name: "Suppliers Who Kept Orders Waiting",
		SQL: `SELECT s_name, COUNT(*) as numwait
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND l1.l_receiptdate > l1.l_commitdate
			AND n_name = 'SAUDI ARABIA'
			AND EXISTS (
				SELECT 1 FROM lineitem l2
				WHERE l2.l_orderkey = l1.l_orderkey
					AND l2.l_suppkey != l1.l_suppkey
			)
			AND NOT EXISTS (
				SELECT 1 FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey
					AND l3.l_suppkey != l1.l_suppkey
					AND l3.l_receiptdate > l3.l_commitdate
			)
		GROUP BY s_name
		ORDER BY numwait DESC, s_name
		LIMIT 100`,
	},

	// Q22: Global Sales Opportunity
	// Tests: SUBSTR, correlated subquery, NOT EXISTS, IN
	22: {
		Name: "Global Sales Opportunity",
		SQL: `SELECT
			SUBSTR(c_phone, 1, 2) as cntrycode,
			COUNT(*) as numcust,
			SUM(c_acctbal) as totacctbal
		FROM customer
		WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			AND c_acctbal > (
				SELECT AVG(c_acctbal)
				FROM customer
				WHERE c_acctbal > 0.00
					AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
			)
			AND NOT EXISTS (
				SELECT 1 FROM orders WHERE o_custkey = c_custkey
			)
		GROUP BY SUBSTR(c_phone, 1, 2)
		ORDER BY cntrycode`,
	},
}

// QueryDef holds a TPC-H query definition.
type QueryDef struct {
	Name string
	SQL  string
}
