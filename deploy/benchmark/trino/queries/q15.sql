WITH revenue AS (
			SELECT
				l_suppkey as supplier_no,
				SUM(l_extendedprice * (1 - l_discount)) as total_revenue
			FROM lineitem
			WHERE l_shipdate >= DATE '1996-01-01'
				AND l_shipdate < DATE '1996-04-01'
			GROUP BY l_suppkey
		)
		SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
		FROM supplier
		JOIN revenue ON s_suppkey = supplier_no
		WHERE total_revenue = (
			SELECT MAX(total_revenue) FROM revenue
		)
		ORDER BY s_suppkey
;
