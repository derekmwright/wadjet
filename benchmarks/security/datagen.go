package security

import (
	"fmt"
	"math/rand"
	"time"
)

// ScaleFactor controls data volume. SF=1 generates ~6M rows total.
// Netflow dominates (~60%), similar to how lineitem dominates TPC-H.
type ScaleFactor float64

const (
	SF001 ScaleFactor = 0.01
	SF01  ScaleFactor = 0.1
	SF1   ScaleFactor = 1.0
	SF10  ScaleFactor = 10.0
	SF100 ScaleFactor = 100.0
)

// RowCounts returns expected row counts per table at the given scale.
func (sf ScaleFactor) RowCounts() TableCounts {
	f := float64(sf)
	return TableCounts{
		Netflow:  max(500, int(4_000_000*f)),
		Firewall: max(200, int(1_000_000*f)),
		DNS:      max(200, int(800_000*f)),
		Auth:     max(100, int(200_000*f)),
		Alerts:   max(50, int(50_000*f)),
	}
}

type TableCounts struct {
	Netflow, Firewall, DNS, Auth, Alerts int
}

func (tc TableCounts) Total() int {
	return tc.Netflow + tc.Firewall + tc.DNS + tc.Auth + tc.Alerts
}

// Time window: data spans 24h at SF0.01, 7 days at SF1+.
func (sf ScaleFactor) TimeWindow() (time.Time, time.Time) {
	end := time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC)
	hours := 24.0
	if sf >= 1 {
		hours = 168 // 7 days
	} else if sf >= 0.1 {
		hours = 72 // 3 days
	}
	start := end.Add(-time.Duration(hours) * time.Hour)
	return start, end
}

// Generate produces all security data in-memory. Suitable for small SFs.
func Generate(sf ScaleFactor) map[string][]map[string]any {
	counts := sf.RowCounts()
	start, end := sf.TimeWindow()
	rng := rand.New(rand.NewSource(42))

	data := make(map[string][]map[string]any)
	data["netflow"] = genNetflow(rng, counts.Netflow, start, end)
	data["firewall"] = genFirewall(rng, counts.Firewall, start, end)
	data["dns"] = genDNS(rng, counts.DNS, start, end)
	data["auth"] = genAuth(rng, counts.Auth, start, end)
	data["alerts"] = genAlerts(rng, counts.Alerts, start, end)
	return data
}

// GenerateChunked streams data in chunks for memory-bounded loading.
func GenerateChunked(sf ScaleFactor, chunkSize int, emit func(table string, rows []map[string]any) error) error {
	counts := sf.RowCounts()
	start, end := sf.TimeWindow()
	rng := rand.New(rand.NewSource(42))

	type tableGen struct {
		name string
		gen  func(*rand.Rand, int, time.Time, time.Time) []map[string]any
		n    int
	}
	tables := []tableGen{
		{"netflow", genNetflow, counts.Netflow},
		{"firewall", genFirewall, counts.Firewall},
		{"dns", genDNS, counts.DNS},
		{"auth", genAuth, counts.Auth},
		{"alerts", genAlerts, counts.Alerts},
	}

	for _, tg := range tables {
		remaining := tg.n
		for remaining > 0 {
			batch := min(chunkSize, remaining)
			rows := tg.gen(rng, batch, start, end)
			if err := emit(tg.name, rows); err != nil {
				return fmt.Errorf("emitting %s: %w", tg.name, err)
			}
			remaining -= batch
		}
	}
	return nil
}

// --- IP address distributions ---
// Internal subnets: 10.0.0.0/8 (corp), 10.1.0.0/16 (servers), 172.16.0.0/12 (dmz)
// External: random public IPs with some "known bad" IPs recurring

func randInternalIP(rng *rand.Rand) string {
	switch rng.Intn(10) {
	case 0, 1, 2, 3, 4: // 50% corporate workstations
		return fmt.Sprintf("10.0.%d.%d", rng.Intn(256), 1+rng.Intn(254))
	case 5, 6, 7: // 30% servers
		return fmt.Sprintf("10.1.%d.%d", rng.Intn(16), 1+rng.Intn(254))
	default: // 20% DMZ
		return fmt.Sprintf("172.16.%d.%d", rng.Intn(4), 1+rng.Intn(254))
	}
}

func randExternalIP(rng *rand.Rand) string {
	// 5% "known bad" IPs (recurring threat actors)
	if rng.Intn(20) == 0 {
		return knownBadIPs[rng.Intn(len(knownBadIPs))]
	}
	// Avoid private ranges
	first := 1 + rng.Intn(223)
	for first == 10 || first == 127 || first == 172 || first == 192 {
		first = 1 + rng.Intn(223)
	}
	return fmt.Sprintf("%d.%d.%d.%d", first, rng.Intn(256), rng.Intn(256), 1+rng.Intn(254))
}

var knownBadIPs = []string{
	"185.220.101.42", "45.155.205.233", "94.232.46.202",
	"193.42.33.17", "91.240.118.50", "5.188.206.14",
	"80.82.77.139", "162.247.74.27", "198.96.155.3",
	"23.129.64.130",
}

// randTS returns a random timestamp uniformly distributed in [start, end).
func randTS(rng *rand.Rand, start, end time.Time) time.Time {
	d := end.Sub(start)
	return start.Add(time.Duration(rng.Int63n(int64(d))))
}

// --- Port distributions ---
// Weighted toward common services

var commonDstPorts = []struct {
	port   int32
	weight int
}{
	{443, 40}, {80, 20}, {53, 10}, {22, 5}, {3389, 3},
	{8443, 3}, {8080, 3}, {25, 2}, {993, 2}, {587, 2},
	{3306, 2}, {5432, 2}, {6379, 1}, {27017, 1}, {9200, 1},
	{445, 1}, {139, 1}, {1433, 1},
}

func randDstPort(rng *rand.Rand) int32 {
	total := 0
	for _, p := range commonDstPorts {
		total += p.weight
	}
	r := rng.Intn(total)
	for _, p := range commonDstPorts {
		r -= p.weight
		if r < 0 {
			return p.port
		}
	}
	return 443
}

func randEphemeralPort(rng *rand.Rand) int32 {
	return 49152 + rng.Int31n(16384)
}

// --- Netflow ---

var tcpFlags = []string{"S", "SA", "A", "PA", "FA", "RA", "S", "SA", "PA", "FA"}
var directions = []string{"inbound", "outbound", "internal"}

func genNetflow(rng *rand.Rand, n int, start, end time.Time) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		ts := randTS(rng, start, end)
		isInternal := rng.Intn(10) < 3 // 30% internal-to-internal
		var srcIP, dstIP, dir string
		if isInternal {
			srcIP = randInternalIP(rng)
			dstIP = randInternalIP(rng)
			dir = "internal"
		} else if rng.Intn(2) == 0 {
			srcIP = randInternalIP(rng)
			dstIP = randExternalIP(rng)
			dir = "outbound"
		} else {
			srcIP = randExternalIP(rng)
			dstIP = randInternalIP(rng)
			dir = "inbound"
		}

		proto := int32(6) // TCP
		if rng.Intn(5) == 0 {
			proto = 17 // UDP
		} else if rng.Intn(50) == 0 {
			proto = 1 // ICMP
		}

		maxDur := 30 * time.Second
		if rng.Intn(50) == 0 { // 2% long-lived flows (tunnels, file transfers)
			maxDur = 10 * time.Minute
		}
		dur := time.Duration(rng.Int63n(int64(maxDur)))
		packets := int64(1 + rng.Intn(200))

		rows[i] = map[string]any{
			"ts":          ts,
			"src_ip":      srcIP,
			"dst_ip":      dstIP,
			"src_port":    randEphemeralPort(rng),
			"dst_port":    randDstPort(rng),
			"protocol":    proto,
			"packets":     packets,
			"bytes_sent":  packets * int64(64+rng.Intn(1400)),
			"bytes_recv":  packets * int64(64+rng.Intn(1400)),
			"duration_ns": dur.Nanoseconds(),
			"tcp_flags":   tcpFlags[rng.Intn(len(tcpFlags))],
			"direction":   dir,
		}
	}
	return rows
}

// --- Firewall ---

var fwActions = []struct {
	action string
	weight int
}{
	{"allow", 85}, {"deny", 10}, {"drop", 5},
}
var fwZones = []string{"internal", "dmz", "external"}
var fwDevices = []string{"fw-east-01", "fw-east-02", "fw-west-01", "fw-west-02", "fw-dc-01"}

func randFWAction(rng *rand.Rand) string {
	r := rng.Intn(100)
	for _, a := range fwActions {
		r -= a.weight
		if r < 0 {
			return a.action
		}
	}
	return "allow"
}

func genFirewall(rng *rand.Rand, n int, start, end time.Time) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		action := randFWAction(rng)
		srcIP := randInternalIP(rng)
		dstIP := randExternalIP(rng)
		zoneSrc := "internal"
		zoneDst := "external"

		if rng.Intn(5) == 0 { // 20% external→internal
			srcIP, dstIP = dstIP, srcIP
			zoneSrc, zoneDst = "external", "internal"
		} else if rng.Intn(10) == 0 { // 10% dmz traffic
			zoneSrc = "dmz"
		}

		rows[i] = map[string]any{
			"ts":         randTS(rng, start, end),
			"src_ip":     srcIP,
			"dst_ip":     dstIP,
			"src_port":   randEphemeralPort(rng),
			"dst_port":   randDstPort(rng),
			"protocol":   int32(6),
			"action":     action,
			"rule_id":    fmt.Sprintf("R%04d", 1+rng.Intn(200)),
			"zone_src":   zoneSrc,
			"zone_dst":   zoneDst,
			"bytes_sent": int64(64 + rng.Intn(65000)),
			"device":     fwDevices[rng.Intn(len(fwDevices))],
		}
	}
	return rows
}

// --- DNS ---

var dnsQueryTypes = []string{"A", "A", "A", "A", "AAAA", "MX", "TXT", "CNAME", "PTR"}
var dnsResponseCodes = []struct {
	code   string
	weight int
}{
	{"NOERROR", 85}, {"NXDOMAIN", 10}, {"SERVFAIL", 3}, {"REFUSED", 2},
}
var dnsServers = []string{"10.1.0.53", "10.1.0.54", "10.1.1.53"}

var normalDomains = []string{
	"google.com", "microsoft.com", "amazonaws.com", "cloudflare.com",
	"github.com", "slack.com", "zoom.us", "office365.com",
	"okta.com", "salesforce.com", "akamaihd.net", "fastly.net",
	"apple.com", "mozilla.org", "ubuntu.com", "docker.io",
}
var suspiciousDomains = []string{
	"c2-server.evil.xyz", "exfil.badactor.ru", "phish-login.co",
	"malware-drop.cn", "crypto-mine.cc",
}

func randDomain(rng *rand.Rand) string {
	// 3% suspicious, 7% random subdomains of normal, 90% normal
	r := rng.Intn(100)
	if r < 3 {
		return suspiciousDomains[rng.Intn(len(suspiciousDomains))]
	}
	base := normalDomains[rng.Intn(len(normalDomains))]
	if r < 10 {
		return fmt.Sprintf("%s.%s", randSubdomain(rng), base)
	}
	return base
}

func randSubdomain(rng *rand.Rand) string {
	parts := []string{"api", "cdn", "static", "app", "www", "mail", "vpn", "sso", "auth", "login"}
	return parts[rng.Intn(len(parts))]
}

func randResponseCode(rng *rand.Rand) string {
	r := rng.Intn(100)
	for _, rc := range dnsResponseCodes {
		r -= rc.weight
		if r < 0 {
			return rc.code
		}
	}
	return "NOERROR"
}

func genDNS(rng *rand.Rand, n int, start, end time.Time) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rcode := randResponseCode(rng)
		var answerIP any
		if rcode == "NOERROR" {
			answerIP = randExternalIP(rng)
		}
		rows[i] = map[string]any{
			"ts":            randTS(rng, start, end),
			"client_ip":     randInternalIP(rng),
			"server_ip":     dnsServers[rng.Intn(len(dnsServers))],
			"query_name":    randDomain(rng),
			"query_type":    dnsQueryTypes[rng.Intn(len(dnsQueryTypes))],
			"response_code": rcode,
			"answer_ip":     answerIP,
			"latency_ns":    int64(500_000 + rng.Intn(50_000_000)), // 0.5ms - 50ms
			"is_recursive":  rng.Intn(10) < 8,                      // 80% recursive
		}
	}
	return rows
}

// --- Auth ---

var authActions = []struct {
	action string
	weight int
}{
	{"login_success", 70}, {"login_failure", 20}, {"logout", 8}, {"privilege_escalation", 2},
}
var authMethods = []string{"password", "password", "mfa", "mfa", "certificate", "sso"}
var authServices = []string{"ssh", "rdp", "vpn", "web", "ldap", "web", "web", "sso"}
var authDevices = []string{"dc-01", "dc-02", "vpn-gw-01", "vpn-gw-02", "web-01", "web-02", "jump-01"}

func randUsername(rng *rand.Rand) string {
	firsts := []string{"jsmith", "agarcia", "mjohnson", "lwilliams", "rbrown",
		"klee", "ydavis", "schen", "dwright", "tmartin",
		"pjones", "nwhite", "cthomas", "bharris", "mclark",
		"admin", "svc_backup", "svc_monitor", "root", "sa"}
	return firsts[rng.Intn(len(firsts))]
}

func randAuthAction(rng *rand.Rand) string {
	r := rng.Intn(100)
	for _, a := range authActions {
		r -= a.weight
		if r < 0 {
			return a.action
		}
	}
	return "login_success"
}

func genAuth(rng *rand.Rand, n int, start, end time.Time) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		action := randAuthAction(rng)
		srcIP := randInternalIP(rng)
		if rng.Intn(5) == 0 { // 20% external
			srcIP = randExternalIP(rng)
		}

		risk := int32(rng.Intn(30))
		if action == "login_failure" {
			risk += int32(20 + rng.Intn(40))
		}
		if action == "privilege_escalation" {
			risk += int32(50 + rng.Intn(50))
		}

		var ua any
		if rng.Intn(3) == 0 {
			uas := []string{
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
				"OpenSSH_8.9p1",
				"curl/7.88.1",
			}
			ua = uas[rng.Intn(len(uas))]
		}

		rows[i] = map[string]any{
			"ts":         randTS(rng, start, end),
			"src_ip":     srcIP,
			"username":   randUsername(rng),
			"action":     action,
			"method":     authMethods[rng.Intn(len(authMethods))],
			"service":    authServices[rng.Intn(len(authServices))],
			"device":     authDevices[rng.Intn(len(authDevices))],
			"user_agent": ua,
			"risk_score": risk,
		}
	}
	return rows
}

// --- Alerts ---

type sigDef struct {
	id       int32
	severity string
	category string
	message  string
}

var signatures = []sigDef{
	{1001, "critical", "intrusion", "ET EXPLOIT Apache Log4j RCE Attempt"},
	{1002, "critical", "malware", "MALWARE-CNC Win.Trojan.Cobalt Strike beacon"},
	{1003, "high", "intrusion", "ET SCAN Nmap SYN Scan"},
	{1004, "high", "exfiltration", "ET DNS Large TXT Record Response"},
	{1005, "high", "intrusion", "ET WEB_SERVER SQL Injection Attempt"},
	{1006, "medium", "recon", "ET SCAN SSH Brute Force Attempt"},
	{1007, "medium", "policy", "ET POLICY Outbound SSH Connection"},
	{1008, "medium", "malware", "INDICATOR-COMPROMISE Suspicious .exe Download"},
	{1009, "low", "policy", "ET POLICY DNS Query to Dynamic DNS Provider"},
	{1010, "low", "recon", "ET SCAN Potential Port Scan"},
	{1011, "info", "policy", "ET POLICY TLS Connection to Non-Standard Port"},
	{1012, "critical", "intrusion", "ET EXPLOIT SMB Remote Code Execution"},
	{1013, "high", "exfiltration", "ET DATA_EXFIL ICMP Tunnel Detected"},
	{1014, "medium", "malware", "INDICATOR-SHELLCODE x86 NOOP Sled Detected"},
	{1015, "low", "recon", "ET SCAN ARP Sweep Detected"},
}

var alertActions = []string{"alert", "alert", "alert", "drop", "block"}

func genAlerts(rng *rand.Rand, n int, start, end time.Time) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		sig := signatures[rng.Intn(len(signatures))]
		rows[i] = map[string]any{
			"ts":           randTS(rng, start, end),
			"src_ip":       randExternalIP(rng),
			"dst_ip":       randInternalIP(rng),
			"src_port":     randEphemeralPort(rng),
			"dst_port":     randDstPort(rng),
			"protocol":     int32(6),
			"signature_id": sig.id,
			"severity":     sig.severity,
			"category":     sig.category,
			"message":      sig.message,
			"action":       alertActions[rng.Intn(len(alertActions))],
		}
	}
	return rows
}
