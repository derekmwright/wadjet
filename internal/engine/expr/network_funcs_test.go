package expr

import (
	"testing"
)

// ── CIDR / Subnet Operations ────────────────────────────────────────────────

func TestNetworkAddress(t *testing.T) {
	fn := DefaultRegistry.Lookup("network_address")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.100/24"}, "192.168.1.0"},
		{[]any{"10.0.0.0/8"}, "10.0.0.0"},
		{[]any{"172.16.5.130/16"}, "172.16.0.0"},
		{[]any{"192.168.1.0/32"}, "192.168.1.0"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("network_address(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkBroadcastAddress(t *testing.T) {
	fn := DefaultRegistry.Lookup("broadcast_address")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.0/24"}, "192.168.1.255"},
		{[]any{"10.0.0.0/8"}, "10.255.255.255"},
		{[]any{"172.16.0.0/16"}, "172.16.255.255"},
		{[]any{"192.168.1.0/32"}, "192.168.1.0"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("broadcast_address(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkPrefixLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("prefix_length")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"10.0.0.0/8"}, int64(8)},
		{[]any{"172.16.0.0/16"}, int64(16)},
		{[]any{"192.168.1.0/24"}, int64(24)},
		{[]any{"192.168.1.0/32"}, int64(32)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("prefix_length(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkCIDRToRange(t *testing.T) {
	fn := DefaultRegistry.Lookup("cidr_to_range")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.0/30"}, "192.168.1.0-192.168.1.3"},
		{[]any{"10.0.0.0/24"}, "10.0.0.0-10.0.0.255"},
		{[]any{"192.168.1.0/32"}, "192.168.1.0-192.168.1.0"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("cidr_to_range(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkHostsInCIDR(t *testing.T) {
	fn := DefaultRegistry.Lookup("hosts_in_cidr")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"10.0.0.0/24"}, int64(254)},
		{[]any{"192.168.1.0/32"}, int64(1)},
		{[]any{"192.168.1.0/31"}, int64(2)},
		{[]any{"10.0.0.0/8"}, int64(16777214)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("hosts_in_cidr(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkCIDROverlap(t *testing.T) {
	fn := DefaultRegistry.Lookup("cidr_overlap")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"10.0.0.0/8", "10.1.0.0/16"}, true},
		{[]any{"192.168.1.0/24", "10.0.0.0/8"}, false},
		{[]any{"172.16.0.0/12", "172.20.0.0/24"}, true},
		{[]any{nil, "10.0.0.0/8"}, nil},
		{[]any{"10.0.0.0/8", nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("cidr_overlap(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIPInRange(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_in_range")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.50", "192.168.1.0", "192.168.1.255"}, true},
		{[]any{"192.168.1.0", "192.168.1.0", "192.168.1.255"}, true},
		{[]any{"192.168.1.255", "192.168.1.0", "192.168.1.255"}, true},
		{[]any{"10.0.0.1", "192.168.1.0", "192.168.1.255"}, false},
		{[]any{nil, "192.168.1.0", "192.168.1.255"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_in_range(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkSameSubnet(t *testing.T) {
	fn := DefaultRegistry.Lookup("same_subnet")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.10", "192.168.1.20", int64(24)}, true},
		{[]any{"192.168.1.10", "192.168.2.20", int64(24)}, false},
		{[]any{"10.0.0.1", "10.0.0.254", int64(8)}, true},
		{[]any{"10.0.0.1", "11.0.0.1", int64(8)}, false},
		{[]any{nil, "192.168.1.20", int64(24)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("same_subnet(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── IP Manipulation ─────────────────────────────────────────────────────────

func TestNetworkIPAdd(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_add")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.1", int64(10)}, "192.168.1.11"},
		{[]any{"192.168.1.1", int64(0)}, "192.168.1.1"},
		{[]any{"192.168.1.254", int64(1)}, "192.168.1.255"},
		{[]any{nil, int64(10)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_add(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIPSubtract(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_subtract")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.10", int64(5)}, "192.168.1.5"},
		{[]any{"192.168.1.10", int64(0)}, "192.168.1.10"},
		{[]any{"10.0.0.1", int64(1)}, "10.0.0.0"},
		{[]any{nil, int64(5)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_subtract(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIPDiff(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_diff")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.10", "192.168.1.1"}, int64(9)},
		{[]any{"192.168.1.1", "192.168.1.1"}, int64(0)},
		{[]any{"192.168.1.1", "192.168.1.10"}, int64(-9)},
		{[]any{nil, "192.168.1.1"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_diff(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIPBetween(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_between")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.50", "192.168.1.0", "192.168.1.255"}, true},
		{[]any{"10.0.0.1", "192.168.1.0", "192.168.1.255"}, false},
		{[]any{"192.168.1.0", "192.168.1.0", "192.168.1.0"}, true},
		{[]any{nil, "192.168.1.0", "192.168.1.255"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_between(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkReverseDNS(t *testing.T) {
	fn := DefaultRegistry.Lookup("reverse_dns")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.1"}, "1.1.168.192.in-addr.arpa"},
		{[]any{"10.0.0.1"}, "1.0.0.10.in-addr.arpa"},
		{[]any{"8.8.8.8"}, "8.8.8.8.in-addr.arpa"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("reverse_dns(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIsMulticastIP(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_multicast_ip")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"224.0.0.1"}, true},
		{[]any{"239.255.255.255"}, true},
		{[]any{"192.168.1.1"}, false},
		{[]any{"10.0.0.1"}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_multicast_ip(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIsLinkLocalIP(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_link_local_ip")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"169.254.1.1"}, true},
		{[]any{"169.254.255.255"}, true},
		{[]any{"192.168.1.1"}, false},
		{[]any{"10.0.0.1"}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_link_local_ip(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIsReservedIP(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_reserved_ip")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"127.0.0.1"}, true},         // loopback
		{[]any{"192.168.1.1"}, true},        // private
		{[]any{"224.0.0.1"}, true},          // multicast
		{[]any{"169.254.1.1"}, true},        // link-local
		{[]any{"8.8.8.8"}, false},           // public
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_reserved_ip(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIPToHex(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_to_hex")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.1"}, "c0a80101"},
		{[]any{"10.0.0.1"}, "0a000001"},
		{[]any{"255.255.255.255"}, "ffffffff"},
		{[]any{"0.0.0.0"}, "00000000"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_to_hex(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── MAC Operations ──────────────────────────────────────────────────────────

func TestNetworkMACVendorOUI(t *testing.T) {
	fn := DefaultRegistry.Lookup("mac_vendor_oui")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"aa:bb:cc:dd:ee:ff"}, "AA:BB:CC"},
		{[]any{"00:11:22:33:44:55"}, "00:11:22"},
		{[]any{"ff:ff:ff:ff:ff:ff"}, "FF:FF:FF"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("mac_vendor_oui(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkMACIsUnicast(t *testing.T) {
	fn := DefaultRegistry.Lookup("mac_is_unicast")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"00:11:22:33:44:55"}, true},    // bit 0 of first byte = 0 → unicast
		{[]any{"01:11:22:33:44:55"}, false},   // bit 0 of first byte = 1 → multicast
		{[]any{"02:11:22:33:44:55"}, true},    // locally administered but still unicast
		{[]any{"ff:ff:ff:ff:ff:ff"}, false},   // broadcast (multicast bit set)
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("mac_is_unicast(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkMACIsLocal(t *testing.T) {
	fn := DefaultRegistry.Lookup("mac_is_local")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"02:11:22:33:44:55"}, true},    // bit 1 of first byte = 1 → locally administered
		{[]any{"00:11:22:33:44:55"}, false},   // bit 1 of first byte = 0 → globally unique
		{[]any{"06:11:22:33:44:55"}, true},    // 0x06 & 0x02 = 0x02 → local
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("mac_is_local(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkMACFormat(t *testing.T) {
	fn := DefaultRegistry.Lookup("mac_format")
	tests := []struct {
		args []any
		want any
	}{
		// Default separator is ':'
		{[]any{"aa:bb:cc:dd:ee:ff"}, "aa:bb:cc:dd:ee:ff"},
		// Dash separator
		{[]any{"aa:bb:cc:dd:ee:ff", "-"}, "aa-bb-cc-dd-ee-ff"},
		// Dot separator
		{[]any{"aa:bb:cc:dd:ee:ff", "."}, "aa.bb.cc.dd.ee.ff"},
		// From dash to colon
		{[]any{"aa-bb-cc-dd-ee-ff", ":"}, "aa:bb:cc:dd:ee:ff"},
		// Uppercase input normalized to lowercase
		{[]any{"AA:BB:CC:DD:EE:FF"}, "aa:bb:cc:dd:ee:ff"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("mac_format(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── Port Classification ─────────────────────────────────────────────────────

func TestNetworkPortName(t *testing.T) {
	fn := DefaultRegistry.Lookup("port_name")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(443)}, "https"},
		{[]any{int64(22)}, "ssh"},
		{[]any{int64(80)}, "http"},
		{[]any{int64(53)}, "dns"},
		{[]any{int64(99999)}, nil},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("port_name(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIsWellKnownPort(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_well_known_port")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(443)}, true},
		{[]any{int64(0)}, true},
		{[]any{int64(1023)}, true},
		{[]any{int64(1024)}, false},
		{[]any{int64(3306)}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_well_known_port(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIsRegisteredPort(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_registered_port")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(3306)}, true},
		{[]any{int64(1024)}, true},
		{[]any{int64(49151)}, true},
		{[]any{int64(443)}, false},
		{[]any{int64(49152)}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_registered_port(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkIsEphemeralPort(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_ephemeral_port")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(50000)}, true},
		{[]any{int64(49152)}, true},
		{[]any{int64(65535)}, true},
		{[]any{int64(49151)}, false},
		{[]any{int64(443)}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_ephemeral_port(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkPortClass(t *testing.T) {
	fn := DefaultRegistry.Lookup("port_class")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(443)}, "well-known"},
		{[]any{int64(3306)}, "registered"},
		{[]any{int64(50000)}, "ephemeral"},
		{[]any{int64(0)}, "well-known"},
		{[]any{int64(70000)}, nil},  // out of range
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("port_class(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── Protocol ────────────────────────────────────────────────────────────────

func TestNetworkProtocolName(t *testing.T) {
	fn := DefaultRegistry.Lookup("protocol_name")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(6)}, "tcp"},
		{[]any{int64(17)}, "udp"},
		{[]any{int64(1)}, "icmp"},
		{[]any{int64(132)}, "sctp"},
		{[]any{int64(999)}, nil},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("protocol_name(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetworkProtocolNumber(t *testing.T) {
	fn := DefaultRegistry.Lookup("protocol_number")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"tcp"}, int64(6)},
		{[]any{"udp"}, int64(17)},
		{[]any{"icmp"}, int64(1)},
		{[]any{"TCP"}, int64(6)},  // case-insensitive
		{[]any{"sctp"}, int64(132)},
		{[]any{"unknown"}, nil},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("protocol_number(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── NULL Propagation ────────────────────────────────────────────────────────

func TestNetworkNullPropagation(t *testing.T) {
	singleArgFuncs := []string{
		"network_address", "broadcast_address", "prefix_length",
		"cidr_to_range", "hosts_in_cidr",
		"reverse_dns", "is_multicast_ip", "is_link_local_ip",
		"is_reserved_ip", "ip_to_hex",
		"mac_vendor_oui", "mac_is_unicast", "mac_is_local", "mac_format",
		"port_name", "is_well_known_port", "is_registered_port",
		"is_ephemeral_port", "port_class",
		"protocol_name", "protocol_number",
	}
	for _, name := range singleArgFuncs {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found in registry", name)
			continue
		}
		got := fn([]any{nil})
		if got != nil {
			t.Errorf("%s(nil) = %v, want nil", name, got)
		}
	}
}

// ── Registration Check ──────────────────────────────────────────────────────

func TestNetworkFunctionsRegistered(t *testing.T) {
	expected := []string{
		// CIDR / Subnet (8)
		"network_address", "broadcast_address", "prefix_length",
		"cidr_to_range", "hosts_in_cidr", "cidr_overlap",
		"ip_in_range", "same_subnet",
		// IP Manipulation (9)
		"ip_add", "ip_subtract", "ip_diff", "ip_between",
		"reverse_dns", "is_multicast_ip", "is_link_local_ip",
		"is_reserved_ip", "ip_to_hex",
		// MAC Operations (4)
		"mac_vendor_oui", "mac_is_unicast", "mac_is_local", "mac_format",
		// Port Classification (5)
		"port_name", "is_well_known_port", "is_registered_port",
		"is_ephemeral_port", "port_class",
		// Protocol (2)
		"protocol_name", "protocol_number",
	}
	if len(expected) != 28 {
		t.Fatalf("expected 28 function names, got %d", len(expected))
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered in DefaultRegistry", name)
		}
	}
}
