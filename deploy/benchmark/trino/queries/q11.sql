-- Q11 FRACTION is scale-dependent per the TPC-H spec: 0.0001/SF.
-- These assets target SF100 (like the DDL's hardcoded bucket) -> 0.000001.
-- Mirrors tpch-bench GetQuery() and run-duckdb-comparison.sh Q11_FRACTION.
SELECT
			ps_partkey,
			SUM(ps_supplycost * ps_availqty) as value
		FROM partsupp
		JOIN supplier ON ps_suppkey = s_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'GERMANY'
		GROUP BY ps_partkey
		HAVING SUM(ps_supplycost * ps_availqty) > (
			SELECT SUM(ps_supplycost * ps_availqty) * 0.000001
			FROM partsupp
			JOIN supplier ON ps_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			WHERE n_name = 'GERMANY'
		)
		ORDER BY value DESC
;
