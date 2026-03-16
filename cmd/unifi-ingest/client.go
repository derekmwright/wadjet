package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"sync"
	"time"
)

// UnifiClient handles authentication and polling against the UniFi Controller API.
type UnifiClient struct {
	baseURL  string
	user     string
	pass     string
	site     string
	client   *http.Client
	logger   *slog.Logger
	mu       sync.Mutex
	loggedIn bool
}

// NewUnifiClient creates a client for the UniFi Controller API.
// The host should be an IP or hostname (e.g., "192.168.1.1").
func NewUnifiClient(host, user, pass, site string, logger *slog.Logger) *UnifiClient {
	jar, _ := cookiejar.New(nil)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return &UnifiClient{
		baseURL: fmt.Sprintf("https://%s", host),
		user:    user,
		pass:    pass,
		site:    site,
		logger:  logger,
		client: &http.Client{
			Jar:       jar,
			Transport: transport,
			Timeout:   15 * time.Second,
		},
	}
}

// Login authenticates with the controller and stores the session cookie.
func (c *UnifiClient) Login(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.login(ctx)
}

func (c *UnifiClient) login(ctx context.Context) error {
	payload := fmt.Sprintf(`{"username":%q,"password":%q}`, c.user, c.pass)
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/auth/login",
		strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("building login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("login request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("login failed: HTTP %d", resp.StatusCode)
	}

	c.loggedIn = true
	c.logger.Info("authenticated with UniFi controller", "host", c.baseURL)
	return nil
}

// Logout ends the session.
func (c *UnifiClient) Logout(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.loggedIn {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/auth/logout", nil)
	if err != nil {
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	c.loggedIn = false
	c.logger.Info("logged out from UniFi controller")
}

// get performs an authenticated GET, re-authenticating on 401.
func (c *UnifiClient) get(ctx context.Context, path string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	body, status, err := c.doGet(ctx, path)
	if err != nil {
		return nil, err
	}

	// Re-authenticate on 401
	if status == http.StatusUnauthorized {
		c.logger.Info("session expired, re-authenticating")
		if err := c.login(ctx); err != nil {
			return nil, fmt.Errorf("re-authentication failed: %w", err)
		}
		body, status, err = c.doGet(ctx, path)
		if err != nil {
			return nil, err
		}
	}

	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", path, status)
	}

	return body, nil
}

func (c *UnifiClient) doGet(ctx context.Context, path string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+path, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	return body, resp.StatusCode, nil
}

// UnifiClientStat represents a client from the stat/sta endpoint.
type UnifiClientStat struct {
	Hostname string  `json:"hostname"`
	Name     string  `json:"name"`
	IP       string  `json:"ip"`
	MAC      string  `json:"mac"`
	RxBytes  int64   `json:"rx_bytes"`
	TxBytes  int64   `json:"tx_bytes"`
	Signal   *int32  `json:"signal"`
	Channel  *int32  `json:"channel"`
	APMAC    string  `json:"ap_mac"`
	Network  string  `json:"network"`
	IsWired  bool    `json:"is_wired"`
	ESSID    string  `json:"essid"`
	SwMAC    string  `json:"sw_mac"`
	SwPort   *int32  `json:"sw_port"`
}

// DisplayName returns the best available name for the client.
func (s *UnifiClientStat) DisplayName() string {
	if s.Name != "" {
		return s.Name
	}
	if s.Hostname != "" {
		return s.Hostname
	}
	return s.MAC
}

// UnifiDevice represents a device from the stat/device endpoint.
type UnifiDevice struct {
	Name        string          `json:"name"`
	Hostname    string          `json:"hostname"`
	Type        string          `json:"type"` // usw, uap, udm, ugw
	Model       string          `json:"model"`
	MAC         string          `json:"mac"`
	Uptime      int64           `json:"uptime"`
	SystemStats json.RawMessage `json:"system-stats"`
	PortTable   []UnifiPort     `json:"port_table"`
	WAN1        *UnifiWAN       `json:"wan1"`
}

// DisplayName returns the best available name for the device.
func (d *UnifiDevice) DisplayName() string {
	if d.Name != "" {
		return d.Name
	}
	if d.Hostname != "" {
		return d.Hostname
	}
	return d.MAC
}

// ParseSystemStats extracts CPU and memory percentage from the system-stats field.
func (d *UnifiDevice) ParseSystemStats() (cpuPct, memPct float64, ok bool) {
	if len(d.SystemStats) == 0 {
		return 0, 0, false
	}

	var stats struct {
		CPU string `json:"cpu"`
		Mem string `json:"mem"`
	}
	if err := json.Unmarshal(d.SystemStats, &stats); err != nil {
		return 0, 0, false
	}

	var cpu, mem float64
	_, _ = fmt.Sscanf(stats.CPU, "%f", &cpu)
	_, _ = fmt.Sscanf(stats.Mem, "%f", &mem)
	return cpu, mem, true
}

// UnifiPort represents a switch port entry.
type UnifiPort struct {
	PortIdx   int32   `json:"port_idx"`
	Name      string  `json:"name"`
	RxBytes   int64   `json:"rx_bytes"`
	TxBytes   int64   `json:"tx_bytes"`
	RxErrors  int64   `json:"rx_errors"`
	TxErrors  int64   `json:"tx_errors"`
	Speed     int32   `json:"speed"`
	IsUplink  bool    `json:"is_uplink"`
	PoePower  float64 `json:"poe_power,string"`
	PortPoe   bool    `json:"port_poe"`
}

// UnifiWAN holds WAN interface statistics.
type UnifiWAN struct {
	RxBytes int64  `json:"rx_bytes"`
	TxBytes int64  `json:"tx_bytes"`
	IP      string `json:"ip"`
}

type unifiResponse struct {
	Data json.RawMessage `json:"data"`
}

// FetchClients returns all client statistics.
func (c *UnifiClient) FetchClients(ctx context.Context) ([]UnifiClientStat, error) {
	body, err := c.get(ctx, fmt.Sprintf("/proxy/network/api/s/%s/stat/sta", c.site))
	if err != nil {
		return nil, fmt.Errorf("fetching clients: %w", err)
	}

	var resp unifiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing client response: %w", err)
	}

	var clients []UnifiClientStat
	if err := json.Unmarshal(resp.Data, &clients); err != nil {
		return nil, fmt.Errorf("parsing client data: %w", err)
	}

	return clients, nil
}

// FetchDevices returns all device statistics.
func (c *UnifiClient) FetchDevices(ctx context.Context) ([]UnifiDevice, error) {
	body, err := c.get(ctx, fmt.Sprintf("/proxy/network/api/s/%s/stat/device", c.site))
	if err != nil {
		return nil, fmt.Errorf("fetching devices: %w", err)
	}

	var resp unifiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing device response: %w", err)
	}

	var devices []UnifiDevice
	if err := json.Unmarshal(resp.Data, &devices); err != nil {
		return nil, fmt.Errorf("parsing device data: %w", err)
	}

	return devices, nil
}

// BuildAPNameMap builds a map of AP MAC address to AP name from the device list.
func BuildAPNameMap(devices []UnifiDevice) map[string]string {
	m := make(map[string]string)
	for _, d := range devices {
		if d.Type == "uap" || d.Type == "udm" {
			m[d.MAC] = d.DisplayName()
		}
	}
	return m
}
