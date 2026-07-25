SELECT
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
			AND o_orderdate >= DATE '1994-01-01'
			AND o_orderdate < DATE '1995-01-01'
		GROUP BY n_name
		ORDER BY revenue DESC
;
