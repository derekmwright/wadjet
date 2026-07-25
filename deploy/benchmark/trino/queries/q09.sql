SELECT
			n_name as nation,
			CAST(year(o_orderdate) AS varchar) as o_year,
			SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
		FROM part
		JOIN lineitem ON p_partkey = l_partkey
		JOIN supplier ON s_suppkey = l_suppkey
		JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE p_name LIKE '%green%'
		GROUP BY n_name, CAST(year(o_orderdate) AS varchar)
		ORDER BY nation, o_year DESC
