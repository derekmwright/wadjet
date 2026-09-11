// Package tcpflagcases holds the empty-input AST coverage census shared by both planning paths and the wire gate.
package tcpflagcases

type Case struct{ Name, SQL string }

var Cases = []Case{
	{"cte_shadowed_body", `WITH r AS (SELECT id FROM users WHERE id<0) SELECT id FROM (WITH r AS (SELECT tcp_flag_mask('BOGUS') AS id FROM users WHERE id<0) SELECT id FROM users WHERE id<0) s`},
	{"set_order_by", `SELECT id FROM users WHERE id<0 UNION ALL SELECT id FROM users WHERE id<0 ORDER BY tcp_flag_mask('BOGUS')`},
	{"table_function_named", `SELECT * FROM read_json(path=tcp_flag_mask('BOGUS')) WHERE FALSE`},
	{"merge_source", `MERGE INTO users t USING (SELECT tcp_flag_mask('BOGUS') AS id FROM users WHERE id<0) s ON t.id=s.id WHEN MATCHED THEN DELETE`},
	{"column", "SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0"},
	{"aggregate_argument", "SELECT SUM(tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"binary", "SELECT 1+tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0"},
	{"unary", "SELECT -tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0"},
	{"comparison", "SELECT tcp_flag_mask('BOGUS')=1 AS v FROM users WHERE id<0"},
	{"and", "SELECT id>0 AND tcp_flag_mask('BOGUS')=1 AS v FROM users WHERE id<0"},
	{"or", "SELECT id>0 OR tcp_flag_mask('BOGUS')=1 AS v FROM users WHERE id<0"},
	{"not", "SELECT NOT (tcp_flag_mask('BOGUS')=1) AS v FROM users WHERE id<0"},
	{"parentheses", "SELECT (tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"function_argument", "SELECT COALESCE(tcp_flag_mask('BOGUS'),1) AS v FROM users WHERE id<0"},
	{"cast", "SELECT CAST(tcp_flag_mask('BOGUS') AS BIGINT) AS v FROM users WHERE id<0"},
	{"in_list", "SELECT id IN (1,tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"between_low", "SELECT id BETWEEN tcp_flag_mask('BOGUS') AND 9 AS v FROM users WHERE id<0"},
	{"between_high", "SELECT id BETWEEN 1 AND tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0"},
	{"like", "SELECT name LIKE CAST(tcp_flag_mask('BOGUS') AS VARCHAR) AS v FROM users WHERE id<0"},
	{"is", "SELECT tcp_flag_mask('BOGUS') IS NULL AS v FROM users WHERE id<0"},
	{"case_subject", "SELECT CASE tcp_flag_mask('BOGUS') WHEN 1 THEN 0 ELSE 1 END AS v FROM users WHERE id<0"},
	{"case_when", "SELECT CASE WHEN tcp_flag_mask('BOGUS')=1 THEN 0 ELSE 1 END AS v FROM users WHERE id<0"},
	{"case_then", "SELECT CASE WHEN id>0 THEN tcp_flag_mask('BOGUS') ELSE 1 END AS v FROM users WHERE id<0"},
	{"case_else", "SELECT CASE WHEN id>0 THEN 0 ELSE tcp_flag_mask('BOGUS') END AS v FROM users WHERE id<0"},
	{"array", "SELECT ARRAY[1,tcp_flag_mask('BOGUS')] AS v FROM users WHERE id<0"},
	{"tuple", "SELECT (tcp_flag_mask('BOGUS'),1) AS v FROM users WHERE id<0"},
	{"any", "SELECT id=ANY(ARRAY[tcp_flag_mask('BOGUS')]) AS v FROM users WHERE id<0"},
	{"all", "SELECT id=ALL(ARRAY[tcp_flag_mask('BOGUS')]) AS v FROM users WHERE id<0"},
	{"where", "SELECT id FROM users WHERE id<0 AND tcp_flag_mask('BOGUS')=1"},
	{"having", "SELECT id FROM users WHERE id<0 GROUP BY id HAVING tcp_flag_mask('BOGUS')=1"},
	{"group_by", "SELECT COUNT(*) FROM users WHERE id<0 GROUP BY tcp_flag_mask('BOGUS')"},
	{"grouping_sets", "SELECT COUNT(*) FROM users WHERE id<0 GROUP BY GROUPING SETS ((tcp_flag_mask('BOGUS')),())"},
	{"order_by", "SELECT id FROM users WHERE id<0 ORDER BY tcp_flag_mask('BOGUS')"},
	{"qualify", "SELECT ROW_NUMBER() OVER () AS v FROM users WHERE id<0 QUALIFY tcp_flag_mask('BOGUS')=1"},
	{"window_argument", "SELECT SUM(tcp_flag_mask('BOGUS')) OVER () AS v FROM users WHERE id<0"},
	{"window_partition", "SELECT SUM(id) OVER (PARTITION BY tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"window_order", "SELECT SUM(id) OVER (ORDER BY tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"window_frame_start", "SELECT SUM(id) OVER (ORDER BY id ROWS BETWEEN tcp_flag_mask('BOGUS') PRECEDING AND CURRENT ROW) AS v FROM users WHERE id<0"},
	{"window_frame_end", "SELECT SUM(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND tcp_flag_mask('BOGUS') FOLLOWING) AS v FROM users WHERE id<0"},
	{"named_window", "SELECT SUM(id) OVER w AS v FROM users WHERE id<0 WINDOW w AS (ORDER BY tcp_flag_mask('BOGUS'))"},
	{"set_left", "SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0 UNION ALL SELECT id FROM users WHERE id<0"},
	{"set_right", "SELECT id AS v FROM users WHERE id<0 UNION ALL SELECT tcp_flag_mask('BOGUS') FROM users WHERE id<0"},
	{"cte", "WITH r AS (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0) SELECT v FROM r"},
	{"cte_unused", "WITH r AS (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0) SELECT id FROM users WHERE id<0"},
	{"derived", "SELECT v FROM (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0) r"},
	{"lateral", "SELECT r.v FROM users u,LATERAL (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0) r WHERE u.id<0"},
	{"join_on", "SELECT u.id FROM users u JOIN users v ON u.id=v.id AND tcp_flag_mask('BOGUS')=1 WHERE u.id<0"},
	{"scalar_subquery", "SELECT (SELECT tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"exists", "SELECT id FROM users WHERE id<0 AND EXISTS (SELECT tcp_flag_mask('BOGUS'))"},
	{"in_subquery", "SELECT id FROM users WHERE id<0 AND id IN (SELECT tcp_flag_mask('BOGUS'))"},
	{"limit", "SELECT id FROM users WHERE id<0 LIMIT (SELECT tcp_flag_mask('BOGUS'))"},
	{"offset", "SELECT id FROM users WHERE id<0 OFFSET (SELECT tcp_flag_mask('BOGUS'))"},
	{"table_function", "SELECT * FROM unnest(ARRAY[tcp_flag_mask('BOGUS')]) t WHERE FALSE"},
	{"table_sample", "SELECT id FROM users TABLESAMPLE BERNOULLI (tcp_flag_mask('BOGUS')) WHERE id<0"},
	{"update_set", "UPDATE users SET visits=tcp_flag_mask('BOGUS') WHERE id<0"},
	{"update_where", "UPDATE users SET visits=1 WHERE id<0 AND tcp_flag_mask('BOGUS')=1"},
	{"delete_where", "DELETE FROM users WHERE id<0 AND tcp_flag_mask('BOGUS')=1"},
	{"insert_values", "INSERT INTO users(id) VALUES (tcp_flag_mask('BOGUS'))"},
	{"insert_select", "INSERT INTO users(id) SELECT tcp_flag_mask('BOGUS') FROM users WHERE id<0"},
	{"merge_on", "MERGE INTO users t USING users s ON t.id=s.id AND tcp_flag_mask('BOGUS')=1 WHEN MATCHED THEN DELETE"},
	{"merge_condition", "MERGE INTO users t USING users s ON t.id=s.id WHEN MATCHED AND tcp_flag_mask('BOGUS')=1 THEN DELETE"},
	{"merge_set", "MERGE INTO users t USING (SELECT id FROM users WHERE id<0) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET visits=tcp_flag_mask('BOGUS')"},
	{"merge_values", "MERGE INTO users t USING (SELECT id FROM users WHERE id<0) s ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (tcp_flag_mask('BOGUS'))"},
	{"function_argument_subquery", "SELECT COALESCE((SELECT tcp_flag_mask('BOGUS')),1) AS v FROM users WHERE id<0"},
	{"between_high_subquery", "SELECT id BETWEEN 1 AND (SELECT tcp_flag_mask('BOGUS')) AS v FROM users WHERE id<0"},
	{"like_subquery", "SELECT name LIKE CAST((SELECT tcp_flag_mask('BOGUS')) AS VARCHAR) AS v FROM users WHERE id<0"},
	{"case_then_subquery", "SELECT CASE WHEN id>0 THEN (SELECT tcp_flag_mask('BOGUS')) ELSE 1 END AS v FROM users WHERE id<0"},
	{"array_subquery", "SELECT ARRAY[1,(SELECT tcp_flag_mask('BOGUS'))] AS v FROM users WHERE id<0"},
	{"tuple_subquery", "SELECT ((SELECT tcp_flag_mask('BOGUS')),1) AS v FROM users WHERE id<0"},
	{"where_subquery", "SELECT id FROM users WHERE id<0 AND (SELECT tcp_flag_mask('BOGUS'))=1"},
	{"having_subquery", "SELECT id FROM users WHERE id<0 GROUP BY id HAVING (SELECT tcp_flag_mask('BOGUS'))=1"},
	{"group_by_subquery", "SELECT COUNT(*) FROM users WHERE id<0 GROUP BY (SELECT tcp_flag_mask('BOGUS'))"},
	{"order_by_subquery", "SELECT id FROM users WHERE id<0 ORDER BY (SELECT tcp_flag_mask('BOGUS'))"},
	{"window_argument_subquery", "SELECT SUM((SELECT tcp_flag_mask('BOGUS'))) OVER () AS v FROM users WHERE id<0"},
	{"window_partition_subquery", "SELECT SUM(id) OVER (PARTITION BY (SELECT tcp_flag_mask('BOGUS'))) AS v FROM users WHERE id<0"},
	{"window_order_subquery", "SELECT SUM(id) OVER (ORDER BY (SELECT tcp_flag_mask('BOGUS'))) AS v FROM users WHERE id<0"},
	{"join_on_subquery", "SELECT u.id FROM users u JOIN users v ON u.id=v.id AND (SELECT tcp_flag_mask('BOGUS'))=1 WHERE u.id<0"},
	{"position_recursive_cte_seed_empty", "WITH RECURSIVE r AS (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0 UNION ALL SELECT v FROM r WHERE v<0) SELECT v FROM r"},
	{"position_recursive_cte_seed_reached", "WITH RECURSIVE r AS (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<=3 UNION ALL SELECT v FROM r WHERE v<0) SELECT v FROM r"},
	{"position_recursive_cte_term_empty", "WITH RECURSIVE r AS (SELECT id AS v FROM users WHERE id<0 UNION ALL SELECT tcp_flag_mask('BOGUS') FROM r WHERE v<0) SELECT v FROM r"},
	{"position_recursive_cte_term_reached", "WITH RECURSIVE r AS (SELECT id AS v FROM users WHERE id<=3 UNION ALL SELECT tcp_flag_mask('BOGUS') FROM r WHERE v<0) SELECT v FROM r"},
	{"position_recursive_cte_unused_body_empty", "WITH RECURSIVE r AS (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<0 UNION ALL SELECT v FROM r WHERE v<0) SELECT id FROM users WHERE id<0"},
	{"position_recursive_cte_unused_body_reached", "WITH RECURSIVE r AS (SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id<=3 UNION ALL SELECT v FROM r WHERE v<0) SELECT id FROM users WHERE id<0"},
}

// ResidualState names positions outside the SELECT binder's reachable syntax.
// An empty state means MERGE 0: expression compilation is lazy in those clauses
// (ADR-0031). Pins must be changed when support changes, never silently exempted.
var ResidualState = map[string]string{
	"window_frame_start": "42601", "window_frame_end": "42601",
	"named_window": "42601", "limit": "42601", "offset": "42601",
	"table_function": "42601", "table_function_named": "42601", "table_sample": "42601",
	"insert_values": "42601", "insert_select": "42601",
	"merge_on": "0A000", "merge_set": "", "merge_values": "",
}

func State(name, door string) string {
	// registerCTE's additive name map skips a shadowing body's validation.
	// The unused inner body never needs to compile. A UNION-level ORDER BY
	// is also skipped by the binder before its expression loop.
	if name == "set_order_by" || name == "cte_shadowed_body" {
		return ""
	}
	if door == "dag" {
		switch name {
		case "update_set", "update_where", "delete_where", "merge_on", "merge_condition", "merge_source", "merge_set", "merge_values":
			return "0A000"
		}
	}
	if state, ok := ResidualState[name]; ok {
		return state
	}
	return "22023"
}

// RecursiveControls attempt the same recursive-body boundary with a valid name.
// Keeping the body unused isolates validation from recursive execution support.
var RecursiveControls = []Case{
	{"recursive_seed_valid", `WITH RECURSIVE r AS (SELECT tcp_flag_mask('SYN') AS v FROM users WHERE id<0 UNION ALL SELECT v FROM r WHERE v<0) SELECT id FROM users WHERE id<0`},
	{"recursive_term_valid", `WITH RECURSIVE r AS (SELECT id AS v FROM users WHERE id<0 UNION ALL SELECT tcp_flag_mask('SYN') FROM r WHERE v<0) SELECT id FROM users WHERE id<0`},
}
