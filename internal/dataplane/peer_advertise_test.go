package dataplane

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// TestAdvertiseAddrResolvesOnce: deriving the advertise address costs a
// netlink RIB dump (net.InterfaceAddrs), and the heartbeat loop calls
// AdvertiseAddr every tick. Netlink route dumps serialize on the kernel's
// global rtnl_lock, so a per-tick dump let a wedged rtnl_lock silence
// heartbeats + peer advertisement while the process stayed alive (the
// dispatch-stall arc's 2026-08-13 frozen-spin capture). The address is
// immutable once the listener binds; it must be resolved exactly once no
// matter how many goroutines ask.
func TestAdvertiseAddrResolvesOnce(t *testing.T) {
	var calls atomic.Int64
	restore := firstUnicastIPv4
	firstUnicastIPv4 = func() net.IP {
		calls.Add(1)
		return net.IPv4(192, 0, 2, 7)
	}
	t.Cleanup(func() { firstUnicastIPv4 = restore })

	// ":0" binds the unspecified address, forcing the derive path.
	srv := NewPeerServer(PeerServerConfig{Addr: ":0"}, &fakeResolver{}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)

	var wg sync.WaitGroup
	addrs := make([]string, 8)
	for i := range addrs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addrs[i] = srv.AdvertiseAddr()
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("firstUnicastIPv4 called %d times; want exactly 1", got)
	}
	for i, a := range addrs {
		if a != addrs[0] {
			t.Fatalf("AdvertiseAddr unstable across calls: [0]=%q [%d]=%q", addrs[0], i, a)
		}
	}
	host, _, err := net.SplitHostPort(addrs[0])
	if err != nil {
		t.Fatalf("AdvertiseAddr %q not host:port: %v", addrs[0], err)
	}
	if host != "192.0.2.7" {
		t.Fatalf("AdvertiseAddr host = %q; want stubbed 192.0.2.7", host)
	}
}

// TestAdvertiseAddrExplicitConfigWins: an explicitly configured address is
// returned verbatim and never triggers interface resolution.
func TestAdvertiseAddrExplicitConfigWins(t *testing.T) {
	var calls atomic.Int64
	restore := firstUnicastIPv4
	firstUnicastIPv4 = func() net.IP {
		calls.Add(1)
		return net.IPv4(192, 0, 2, 7)
	}
	t.Cleanup(func() { firstUnicastIPv4 = restore })

	srv := NewPeerServer(PeerServerConfig{Addr: ":0", AdvertiseAddr: "10.0.0.5:9095"}, &fakeResolver{}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)

	if got := srv.AdvertiseAddr(); got != "10.0.0.5:9095" {
		t.Fatalf("AdvertiseAddr = %q; want configured 10.0.0.5:9095", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("firstUnicastIPv4 called %d times for explicit config; want 0", got)
	}
}
