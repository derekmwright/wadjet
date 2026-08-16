package security

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// Security event schemas using Wadjet's network-native types.
// Five tables model a realistic SIEM data pipeline: network flows,
// firewall logs, DNS queries, authentication events, and IDS alerts.

var NetflowSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "src_ip", Type: parquet.TypeIPv4},
		{Name: "dst_ip", Type: parquet.TypeIPv4},
		{Name: "src_port", Type: parquet.TypePort},
		{Name: "dst_port", Type: parquet.TypePort},
		{Name: "protocol", Type: parquet.TypeProtocol},
		{Name: "packets", Type: parquet.TypeInt64},
		{Name: "bytes_sent", Type: parquet.TypeInt64},
		{Name: "bytes_recv", Type: parquet.TypeInt64},
		{Name: "duration_ns", Type: parquet.TypeDuration},
		{Name: "tcp_flags", Type: parquet.TypeString},
		{Name: "direction", Type: parquet.TypeString}, // inbound, outbound, internal
	},
}

var FirewallSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "src_ip", Type: parquet.TypeIPv4},
		{Name: "dst_ip", Type: parquet.TypeIPv4},
		{Name: "src_port", Type: parquet.TypePort},
		{Name: "dst_port", Type: parquet.TypePort},
		{Name: "protocol", Type: parquet.TypeProtocol},
		{Name: "action", Type: parquet.TypeString},  // allow, deny, drop
		{Name: "rule_id", Type: parquet.TypeString},
		{Name: "zone_src", Type: parquet.TypeString}, // internal, dmz, external
		{Name: "zone_dst", Type: parquet.TypeString},
		{Name: "bytes_sent", Type: parquet.TypeInt64},
		{Name: "device", Type: parquet.TypeString},
	},
}

var DNSSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "client_ip", Type: parquet.TypeIPv4},
		{Name: "server_ip", Type: parquet.TypeIPv4},
		{Name: "query_name", Type: parquet.TypeString},
		{Name: "query_type", Type: parquet.TypeString}, // A, AAAA, MX, TXT, CNAME, PTR
		{Name: "response_code", Type: parquet.TypeString}, // NOERROR, NXDOMAIN, SERVFAIL, REFUSED
		{Name: "answer_ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "latency_ns", Type: parquet.TypeDuration},
		{Name: "is_recursive", Type: parquet.TypeBool},
	},
}

var AuthSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "src_ip", Type: parquet.TypeIPv4},
		{Name: "username", Type: parquet.TypeString},
		{Name: "action", Type: parquet.TypeString}, // login_success, login_failure, logout, privilege_escalation
		{Name: "method", Type: parquet.TypeString}, // password, mfa, certificate, sso
		{Name: "service", Type: parquet.TypeString}, // ssh, rdp, vpn, web, ldap
		{Name: "device", Type: parquet.TypeString},
		{Name: "user_agent", Type: parquet.TypeString, Nullable: true},
		{Name: "risk_score", Type: parquet.TypeInt32},
	},
}

var AlertSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "src_ip", Type: parquet.TypeIPv4},
		{Name: "dst_ip", Type: parquet.TypeIPv4},
		{Name: "src_port", Type: parquet.TypePort},
		{Name: "dst_port", Type: parquet.TypePort},
		{Name: "protocol", Type: parquet.TypeProtocol},
		{Name: "signature_id", Type: parquet.TypeInt32},
		{Name: "severity", Type: parquet.TypeString}, // critical, high, medium, low, info
		{Name: "category", Type: parquet.TypeString}, // intrusion, malware, policy, recon, exfiltration
		{Name: "message", Type: parquet.TypeString},
		{Name: "action", Type: parquet.TypeString}, // alert, drop, block
	},
}

// AllTables maps table names to their schemas.
var AllTables = map[string]parquet.Schema{
	"netflow":  NetflowSchema,
	"firewall": FirewallSchema,
	"dns":      DNSSchema,
	"auth":     AuthSchema,
	"alerts":   AlertSchema,
}
