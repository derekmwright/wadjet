SELECT
			SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem
		JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23'
			AND p_container = 'MED BOX'
			AND l_quantity < (
				SELECT 0.2 * AVG(l_quantity)
				FROM lineitem
				WHERE l_partkey = p_partkey
			)
;
