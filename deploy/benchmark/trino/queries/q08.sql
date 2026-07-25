SELECT
			CAST(year(o_orderdate) AS varchar) as o_year,
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
			AND o_orderdate >= DATE '1995-01-01'
			AND o_orderdate <= DATE '1996-12-31'
			AND p_type = 'ECONOMY ANODIZED STEEL'
		GROUP BY CAST(year(o_orderdate) AS varchar)
		ORDER BY o_year
;
