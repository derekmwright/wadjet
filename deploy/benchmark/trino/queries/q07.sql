SELECT
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			CAST(year(l_shipdate) AS varchar) as l_year,
			SUM(l_extendedprice * (1 - l_discount)) as revenue
		FROM supplier
		JOIN lineitem ON s_suppkey = l_suppkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN customer ON c_custkey = o_custkey
		JOIN nation n1 ON s_nationkey = n1.n_nationkey
		JOIN nation n2 ON c_nationkey = n2.n_nationkey
		WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
			OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
			AND l_shipdate >= DATE '1995-01-01'
			AND l_shipdate <= DATE '1996-12-31'
		GROUP BY n1.n_name, n2.n_name, CAST(year(l_shipdate) AS varchar)
		ORDER BY supp_nation, cust_nation, l_year
;
