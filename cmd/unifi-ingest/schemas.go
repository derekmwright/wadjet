package main

import (
	"time"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ClientTrafficSchema defines the schema for the client_traffic table.
var ClientTrafficSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "timestamp", Type: parquet.TypeTimestamp},
		{Name: "date", Type: parquet.TypeString},
		{Name: "hostname", Type: parquet.TypeString, Nullable: true},
		{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "mac", Type: parquet.TypeMAC},
		{Name: "rx_bytes", Type: parquet.TypeInt64},
		{Name: "tx_bytes", Type: parquet.TypeInt64},
		{Name: "signal", Type: parquet.TypeInt32, Nullable: true},
		{Name: "channel", Type: parquet.TypeInt32, Nullable: true},
		{Name: "ap_name", Type: parquet.TypeString, Nullable: true},
		{Name: "network", Type: parquet.TypeString, Nullable: true},
		{Name: "is_wired", Type: parquet.TypeBool},
	},
}

// SwitchPortsSchema defines the schema for the switch_ports table.
var SwitchPortsSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "timestamp", Type: parquet.TypeTimestamp},
		{Name: "date", Type: parquet.TypeString},
		{Name: "switch_name", Type: parquet.TypeString},
		{Name: "port_idx", Type: parquet.TypeInt32},
		{Name: "port_name", Type: parquet.TypeString, Nullable: true},
		{Name: "rx_bytes", Type: parquet.TypeInt64},
		{Name: "tx_bytes", Type: parquet.TypeInt64},
		{Name: "rx_errors", Type: parquet.TypeInt64, Nullable: true},
		{Name: "tx_errors", Type: parquet.TypeInt64, Nullable: true},
		{Name: "speed", Type: parquet.TypeInt32, Nullable: true},
		{Name: "poe_power", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "is_uplink", Type: parquet.TypeBool},
	},
}

// DeviceHealthSchema defines the schema for the device_health table.
var DeviceHealthSchema = parquet.Schema{
	Columns: []parquet.Column{
		{Name: "timestamp", Type: parquet.TypeTimestamp},
		{Name: "date", Type: parquet.TypeString},
		{Name: "device_name", Type: parquet.TypeString},
		{Name: "device_type", Type: parquet.TypeString},
		{Name: "model", Type: parquet.TypeString},
		{Name: "cpu_pct", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "mem_pct", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "uptime_seconds", Type: parquet.TypeInt64},
		{Name: "wan_rx_bytes", Type: parquet.TypeInt64, Nullable: true},
		{Name: "wan_tx_bytes", Type: parquet.TypeInt64, Nullable: true},
	},
}

// TransformClients converts UniFi client stats into rows for the client_traffic table.
func TransformClients(now time.Time, clients []UnifiClientStat, apNames map[string]string) []map[string]any {
	date := now.Format("2006-01-02")
	rows := make([]map[string]any, 0, len(clients))

	for _, c := range clients {
		row := map[string]any{
			"timestamp": now,
			"date":      date,
			"mac":       c.MAC,
			"rx_bytes":  c.RxBytes,
			"tx_bytes":  c.TxBytes,
			"is_wired":  c.IsWired,
		}

		if name := c.DisplayName(); name != c.MAC {
			row["hostname"] = name
		}
		if c.IP != "" {
			row["ip"] = c.IP
		}
		if c.Signal != nil {
			row["signal"] = *c.Signal
		}
		if c.Channel != nil {
			row["channel"] = *c.Channel
		}
		if c.APMAC != "" {
			if apName, ok := apNames[c.APMAC]; ok {
				row["ap_name"] = apName
			}
		}
		if c.ESSID != "" {
			row["network"] = c.ESSID
		} else if c.Network != "" {
			row["network"] = c.Network
		}

		rows = append(rows, row)
	}

	return rows
}

// TransformSwitchPorts converts UniFi device stats into rows for the switch_ports table.
func TransformSwitchPorts(now time.Time, devices []UnifiDevice) []map[string]any {
	date := now.Format("2006-01-02")
	var rows []map[string]any

	for _, d := range devices {
		if d.Type != "usw" {
			continue
		}

		switchName := d.DisplayName()

		for _, p := range d.PortTable {
			row := map[string]any{
				"timestamp":   now,
				"date":        date,
				"switch_name": switchName,
				"port_idx":    p.PortIdx,
				"rx_bytes":    p.RxBytes,
				"tx_bytes":    p.TxBytes,
				"is_uplink":   p.IsUplink,
			}

			if p.Name != "" {
				row["port_name"] = p.Name
			}
			if p.RxErrors > 0 {
				row["rx_errors"] = p.RxErrors
			}
			if p.TxErrors > 0 {
				row["tx_errors"] = p.TxErrors
			}
			if p.Speed > 0 {
				row["speed"] = p.Speed
			}
			if p.PortPoe && p.PoePower > 0 {
				row["poe_power"] = p.PoePower
			}

			rows = append(rows, row)
		}
	}

	return rows
}

// TransformDeviceHealth converts UniFi device stats into rows for the device_health table.
func TransformDeviceHealth(now time.Time, devices []UnifiDevice) []map[string]any {
	date := now.Format("2006-01-02")
	rows := make([]map[string]any, 0, len(devices))

	for _, d := range devices {
		row := map[string]any{
			"timestamp":      now,
			"date":           date,
			"device_name":    d.DisplayName(),
			"device_type":    d.Type,
			"model":          d.Model,
			"uptime_seconds": d.Uptime,
		}

		if cpu, mem, ok := d.ParseSystemStats(); ok {
			row["cpu_pct"] = cpu
			row["mem_pct"] = mem
		}

		if d.WAN1 != nil {
			row["wan_rx_bytes"] = d.WAN1.RxBytes
			row["wan_tx_bytes"] = d.WAN1.TxBytes
		}

		rows = append(rows, row)
	}

	return rows
}
