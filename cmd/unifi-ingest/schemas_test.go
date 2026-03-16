package main

import (
	"testing"
	"time"
)

func TestTransformClients(t *testing.T) {
	now := time.Date(2026, 3, 16, 14, 30, 0, 0, time.UTC)
	signal := int32(-55)
	channel := int32(36)

	clients := []UnifiClientStat{
		{
			Hostname: "desktop-01",
			IP:       "192.168.1.100",
			MAC:      "aa:bb:cc:dd:ee:ff",
			RxBytes:  1048576,
			TxBytes:  524288,
			Signal:   &signal,
			Channel:  &channel,
			APMAC:    "11:22:33:44:55:66",
			ESSID:    "HomeNetwork",
			IsWired:  false,
		},
		{
			Name:    "nas",
			IP:      "192.168.1.50",
			MAC:     "ff:ee:dd:cc:bb:aa",
			RxBytes: 999999999,
			TxBytes: 888888888,
			IsWired: true,
		},
		{
			// Client with no hostname, no IP
			MAC:     "00:11:22:33:44:55",
			RxBytes: 0,
			TxBytes: 0,
			IsWired: true,
		},
	}

	apNames := map[string]string{
		"11:22:33:44:55:66": "Office AP",
	}

	rows := TransformClients(now, clients, apNames)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Wireless client
	r0 := rows[0]
	if r0["date"] != "2026-03-16" {
		t.Errorf("expected date '2026-03-16', got %v", r0["date"])
	}
	if r0["hostname"] != "desktop-01" {
		t.Errorf("expected hostname 'desktop-01', got %v", r0["hostname"])
	}
	if r0["ip"] != "192.168.1.100" {
		t.Errorf("expected ip '192.168.1.100', got %v", r0["ip"])
	}
	if r0["signal"] != int32(-55) {
		t.Errorf("expected signal -55, got %v", r0["signal"])
	}
	if r0["ap_name"] != "Office AP" {
		t.Errorf("expected ap_name 'Office AP', got %v", r0["ap_name"])
	}
	if r0["network"] != "HomeNetwork" {
		t.Errorf("expected network 'HomeNetwork', got %v", r0["network"])
	}
	if r0["is_wired"] != false {
		t.Errorf("expected is_wired false")
	}

	// Wired client with Name field
	r1 := rows[1]
	if r1["hostname"] != "nas" {
		t.Errorf("expected hostname 'nas', got %v", r1["hostname"])
	}
	if _, ok := r1["signal"]; ok {
		t.Error("expected no signal for wired client")
	}
	if _, ok := r1["ap_name"]; ok {
		t.Error("expected no ap_name for wired client")
	}

	// Client with only MAC
	r2 := rows[2]
	if _, ok := r2["hostname"]; ok {
		t.Error("expected no hostname when only MAC available")
	}
}

func TestTransformSwitchPorts(t *testing.T) {
	now := time.Date(2026, 3, 16, 14, 30, 0, 0, time.UTC)

	devices := []UnifiDevice{
		{
			Name: "Home Switch",
			Type: "usw",
			PortTable: []UnifiPort{
				{PortIdx: 1, Name: "Port 1", RxBytes: 1000, TxBytes: 2000, Speed: 1000, IsUplink: false},
				{PortIdx: 2, Name: "Uplink", RxBytes: 5000, TxBytes: 6000, Speed: 1000, IsUplink: true},
				{PortIdx: 3, Name: "PoE Device", RxBytes: 100, TxBytes: 200, Speed: 100, PortPoe: true, PoePower: 12.5},
			},
		},
		{
			Name: "Office AP",
			Type: "uap", // Not a switch — should be skipped
		},
	}

	rows := TransformSwitchPorts(now, devices)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (AP skipped), got %d", len(rows))
	}

	// Regular port
	if rows[0]["switch_name"] != "Home Switch" {
		t.Errorf("expected 'Home Switch', got %v", rows[0]["switch_name"])
	}
	if rows[0]["port_idx"] != int32(1) {
		t.Errorf("expected port_idx 1, got %v", rows[0]["port_idx"])
	}
	if rows[0]["is_uplink"] != false {
		t.Error("expected is_uplink false for port 1")
	}

	// Uplink port
	if rows[1]["is_uplink"] != true {
		t.Error("expected is_uplink true for port 2")
	}

	// PoE port
	if rows[2]["poe_power"] != 12.5 {
		t.Errorf("expected poe_power 12.5, got %v", rows[2]["poe_power"])
	}
}

func TestTransformDeviceHealth(t *testing.T) {
	now := time.Date(2026, 3, 16, 14, 30, 0, 0, time.UTC)

	devices := []UnifiDevice{
		{
			Name:        "Home Switch",
			Type:        "usw",
			Model:       "US16P150",
			Uptime:      86400,
			SystemStats: []byte(`{"cpu":"15.2","mem":"42.1"}`),
		},
		{
			Name:        "UDM Pro",
			Type:        "udm",
			Model:       "UDMPRO",
			Uptime:      604800,
			SystemStats: []byte(`{"cpu":"8.5","mem":"55.3"}`),
			WAN1:        &UnifiWAN{RxBytes: 999999, TxBytes: 888888, IP: "203.0.113.1"},
		},
		{
			// Device with no system stats
			Name:   "Old AP",
			Type:   "uap",
			Model:  "U7PG2",
			Uptime: 3600,
		},
	}

	rows := TransformDeviceHealth(now, devices)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Switch with system stats
	r0 := rows[0]
	if r0["device_name"] != "Home Switch" {
		t.Errorf("expected 'Home Switch', got %v", r0["device_name"])
	}
	if r0["cpu_pct"] != 15.2 {
		t.Errorf("expected cpu_pct 15.2, got %v", r0["cpu_pct"])
	}
	if r0["mem_pct"] != 42.1 {
		t.Errorf("expected mem_pct 42.1, got %v", r0["mem_pct"])
	}
	if r0["uptime_seconds"] != int64(86400) {
		t.Errorf("expected uptime 86400, got %v", r0["uptime_seconds"])
	}
	if _, ok := r0["wan_rx_bytes"]; ok {
		t.Error("expected no wan_rx_bytes for switch")
	}

	// UDM Pro with WAN stats
	r1 := rows[1]
	if r1["wan_rx_bytes"] != int64(999999) {
		t.Errorf("expected wan_rx_bytes 999999, got %v", r1["wan_rx_bytes"])
	}
	if r1["wan_tx_bytes"] != int64(888888) {
		t.Errorf("expected wan_tx_bytes 888888, got %v", r1["wan_tx_bytes"])
	}

	// AP with no system stats
	r2 := rows[2]
	if _, ok := r2["cpu_pct"]; ok {
		t.Error("expected no cpu_pct for device without system stats")
	}
}

func TestBuildAPNameMap(t *testing.T) {
	devices := []UnifiDevice{
		{Name: "Office AP", Type: "uap", MAC: "aa:bb:cc:dd:ee:ff"},
		{Name: "Home Switch", Type: "usw", MAC: "11:22:33:44:55:66"},
		{Name: "UDM Pro", Type: "udm", MAC: "ff:ff:ff:ff:ff:ff"},
	}

	m := BuildAPNameMap(devices)

	if len(m) != 2 {
		t.Fatalf("expected 2 entries (uap + udm), got %d", len(m))
	}
	if m["aa:bb:cc:dd:ee:ff"] != "Office AP" {
		t.Errorf("expected 'Office AP', got %q", m["aa:bb:cc:dd:ee:ff"])
	}
	if m["ff:ff:ff:ff:ff:ff"] != "UDM Pro" {
		t.Errorf("expected 'UDM Pro', got %q", m["ff:ff:ff:ff:ff:ff"])
	}
	// Switch should not be in the map
	if _, ok := m["11:22:33:44:55:66"]; ok {
		t.Error("switch should not be in AP name map")
	}
}
