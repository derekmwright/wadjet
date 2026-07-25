SELECT
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
		ORDER BY value DESC
;
