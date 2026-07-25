SELECT
			c_custkey,
			COUNT(o_orderkey) as c_count
		FROM customer
		LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
		GROUP BY c_custkey
		ORDER BY c_count DESC, c_custkey
		LIMIT 100
