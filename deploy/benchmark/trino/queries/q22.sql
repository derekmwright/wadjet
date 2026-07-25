SELECT
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
		ORDER BY cntrycode
;
