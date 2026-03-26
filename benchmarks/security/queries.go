package security

// SecurityQueries defines analytics queries representative of real SOC and SIEM workloads.
// Unlike TPC-H (complex multi-way joins), these focus on:
//   - Single-table scan throughput with selective filters
//   - Time-range partition pruning
//   - Network function performance (CIDR, port classification)
//   - Top-N aggregation over large tables
//   - Threat hunting patterns

type QueryDef struct {
	Name string
	SQL  string
}

var SecurityQueries = map[int]QueryDef{

	// --- Netflow analytics (the big table) ---

	// Q01: Top talkers by bytes — classic SOC dashboard widget.
	// Tests: full-table scan, GROUP BY on IPv4, ORDER BY aggregate, LIMIT.
	1: {
		Name: "Top Talkers by Bytes",
		SQL: `SELECT src_ip, SUM(bytes_sent) as total_bytes, COUNT(*) as flow_count
			FROM netflow
			GROUP BY src_ip
			ORDER BY total_bytes DESC
			LIMIT 25`,
	},

	// Q02: Traffic by protocol — tests Protocol type + GROUP BY.
	2: {
		Name: "Traffic by Protocol",
		SQL: `SELECT protocol, COUNT(*) as flows,
				SUM(packets) as total_packets, SUM(bytes_sent + bytes_recv) as total_bytes
			FROM netflow
			GROUP BY protocol
			ORDER BY total_bytes DESC`,
	},

	// Q03: Outbound traffic to non-standard ports — threat hunting pattern.
	// Tests: multi-predicate filter on typed columns, port range check.
	3: {
		Name: "Outbound Non-Standard Ports",
		SQL: `SELECT dst_ip, dst_port, COUNT(*) as flows, SUM(bytes_sent) as bytes_out
			FROM netflow
			WHERE direction = 'outbound'
				AND dst_port NOT IN (80, 443, 53, 22, 25, 993, 587)
				AND protocol = 6
			GROUP BY dst_ip, dst_port
			ORDER BY flows DESC
			LIMIT 50`,
	},

	// Q04: Traffic volume by hour — time-series bucketing.
	// Tests: timestamp extraction, GROUP BY time bucket.
	4: {
		Name: "Hourly Traffic Volume",
		SQL: `SELECT EXTRACT(HOUR FROM ts) as hour,
				COUNT(*) as flows,
				SUM(bytes_sent + bytes_recv) as total_bytes
			FROM netflow
			GROUP BY EXTRACT(HOUR FROM ts)
			ORDER BY hour`,
	},

	// Q05: Long-duration flows — potential exfiltration or tunneling.
	// Tests: Duration type filter, selective scan.
	5: {
		Name: "Long Duration Flows",
		SQL: `SELECT src_ip, dst_ip, dst_port, protocol, duration_ns, bytes_sent
			FROM netflow
			WHERE duration_ns > 60000000000
			ORDER BY duration_ns DESC
			LIMIT 100`,
	},

	// --- Firewall analytics ---

	// Q06: Denied traffic summary — most blocked sources.
	// Tests: string equality filter, GROUP BY IPv4.
	6: {
		Name: "Top Denied Sources",
		SQL: `SELECT src_ip, COUNT(*) as deny_count, SUM(bytes_sent) as bytes_attempted
			FROM firewall
			WHERE action IN ('deny', 'drop')
			GROUP BY src_ip
			ORDER BY deny_count DESC
			LIMIT 25`,
	},

	// Q07: Cross-zone traffic matrix — what's talking to what.
	// Tests: two-column GROUP BY on strings, COUNT.
	7: {
		Name: "Cross-Zone Traffic Matrix",
		SQL: `SELECT zone_src, zone_dst, action, COUNT(*) as events
			FROM firewall
			GROUP BY zone_src, zone_dst, action
			ORDER BY events DESC`,
	},

	// Q08: Firewall rule hit distribution — which rules fire most.
	// Tests: GROUP BY with HAVING filter.
	8: {
		Name: "Hot Firewall Rules",
		SQL: `SELECT rule_id, action, COUNT(*) as hits
			FROM firewall
			GROUP BY rule_id, action
			HAVING COUNT(*) > 10
			ORDER BY hits DESC
			LIMIT 50`,
	},

	// --- DNS analytics ---

	// Q09: NXDOMAIN spike detection — DGA or misconfiguration.
	// Tests: string filter + GROUP BY.
	9: {
		Name: "NXDOMAIN by Client",
		SQL: `SELECT client_ip, COUNT(*) as nxdomain_count
			FROM dns
			WHERE response_code = 'NXDOMAIN'
			GROUP BY client_ip
			ORDER BY nxdomain_count DESC
			LIMIT 25`,
	},

	// Q10: Suspicious domain queries — looks for known-bad domains.
	// Tests: LIKE pattern matching on strings.
	10: {
		Name: "Suspicious DNS Queries",
		SQL: `SELECT client_ip, query_name, query_type, COUNT(*) as query_count
			FROM dns
			WHERE query_name LIKE '%.evil.%'
				OR query_name LIKE '%.badactor.%'
				OR query_name LIKE '%phish%'
				OR query_name LIKE '%malware%'
				OR query_name LIKE '%crypto-mine%'
			GROUP BY client_ip, query_name, query_type
			ORDER BY query_count DESC`,
	},

	// Q11: DNS latency percentiles — operational health.
	// Tests: AVG + MAX aggregate, selective filter.
	11: {
		Name: "DNS Latency by Server",
		SQL: `SELECT server_ip, COUNT(*) as queries,
				AVG(latency_ns) as avg_latency,
				MAX(latency_ns) as max_latency
			FROM dns
			GROUP BY server_ip
			ORDER BY avg_latency DESC`,
	},

	// --- Auth analytics ---

	// Q12: Brute force detection — failed logins per user.
	// Tests: string filter + GROUP BY + HAVING.
	12: {
		Name: "Brute Force Candidates",
		SQL: `SELECT username, service, COUNT(*) as failures, COUNT(DISTINCT src_ip) as unique_sources
			FROM auth
			WHERE action = 'login_failure'
			GROUP BY username, service
			ORDER BY failures DESC
			LIMIT 25`,
	},

	// Q13: Privilege escalation events — high-value security events.
	// Tests: very selective filter on large-ish table.
	13: {
		Name: "Privilege Escalation Events",
		SQL: `SELECT ts, src_ip, username, method, service, device, risk_score
			FROM auth
			WHERE action = 'privilege_escalation'
			ORDER BY risk_score DESC, ts DESC`,
	},

	// Q14: High risk auth events — risk score threshold scan.
	// Tests: integer comparison filter.
	14: {
		Name: "High Risk Auth Events",
		SQL: `SELECT src_ip, username, action, service, risk_score
			FROM auth
			WHERE risk_score > 70
			ORDER BY risk_score DESC
			LIMIT 100`,
	},

	// --- IDS/Alert analytics ---

	// Q15: Alert severity distribution — SOC dashboard.
	// Tests: GROUP BY string, COUNT.
	15: {
		Name: "Alert Severity Distribution",
		SQL: `SELECT severity, category, COUNT(*) as alert_count
			FROM alerts
			GROUP BY severity, category
			ORDER BY alert_count DESC`,
	},

	// Q16: Critical alerts timeline — incident response.
	// Tests: selective string filter, ORDER BY timestamp.
	16: {
		Name: "Critical Alerts Timeline",
		SQL: `SELECT ts, src_ip, dst_ip, dst_port, message, action
			FROM alerts
			WHERE severity = 'critical'
			ORDER BY ts DESC`,
	},

	// Q17: Most targeted internal hosts — which servers are under attack.
	// Tests: GROUP BY IPv4, ORDER BY aggregate.
	17: {
		Name: "Most Targeted Hosts",
		SQL: `SELECT dst_ip, COUNT(*) as alert_count,
				COUNT(DISTINCT signature_id) as unique_sigs
			FROM alerts
			WHERE severity IN ('critical', 'high')
			GROUP BY dst_ip
			ORDER BY alert_count DESC
			LIMIT 25`,
	},

	// --- Cross-table correlation (lightweight joins) ---

	// Q18: Denied firewall sources that also triggered alerts.
	// Tests: JOIN between two medium tables on IPv4 key.
	18: {
		Name: "Denied Sources with Alerts",
		SQL: `SELECT f.src_ip, COUNT(DISTINCT f.dst_port) as ports_tried,
				COUNT(DISTINCT a.signature_id) as alert_types
			FROM firewall f
			JOIN alerts a ON f.src_ip = a.src_ip
			WHERE f.action IN ('deny', 'drop')
			GROUP BY f.src_ip
			ORDER BY alert_types DESC
			LIMIT 25`,
	},

	// Q19: Failed auth → suspicious DNS correlation.
	// Tests: subquery-based correlation, realistic SOC workflow.
	19: {
		Name: "Failed Auth to Suspicious DNS",
		SQL: `SELECT d.client_ip, d.query_name, COUNT(*) as queries
			FROM dns d
			WHERE d.client_ip IN (
				SELECT DISTINCT src_ip FROM auth WHERE action = 'login_failure'
			)
			AND (d.query_name LIKE '%.evil.%' OR d.query_name LIKE '%badactor%'
				OR d.query_name LIKE '%phish%' OR d.query_name LIKE '%malware%')
			GROUP BY d.client_ip, d.query_name
			ORDER BY queries DESC
			LIMIT 50`,
	},

	// Q20: Known-bad IP activity across firewall — targeted correlation.
	// Tests: join on specific IPs (realistic: SOC starts from alert, pivots to firewall).
	20: {
		Name: "Alert Source Firewall Activity",
		SQL: `SELECT f.src_ip, f.action, f.dst_port, COUNT(*) as events
			FROM firewall f
			WHERE f.src_ip IN (
				SELECT DISTINCT src_ip FROM alerts WHERE severity IN ('critical', 'high')
			)
			GROUP BY f.src_ip, f.action, f.dst_port
			ORDER BY events DESC
			LIMIT 50`,
	},
}
