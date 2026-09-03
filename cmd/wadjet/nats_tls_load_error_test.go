package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A config file that will not PARSE is a startup error, on every mode
// (#827, and #802's doctrine).
//
// Dropping config.Load's error is how the tier's stated invariant gets
// falsified. An unparseable file that NAMES tls_cert, tls_key and tls_ca
// yields a nil config; resolveNATSTLSPaths then sees three empty strings,
// which is the legitimate "no TLS configured" shape, and returns no error —
// so the process connects to NATS in PLAINTEXT, with the operator's
// certificate sitting in a file it never read. Worker mode has no other
// error-checked load at all, so it ran to completion; coordinator mode
// refused only after the embedded server had already started and connected.
//
// #802 settled this exact shape for the auth block two days before this
// arc: "an unreadable config file silently started a server with NO
// authentication at all — that now stops the process with the reason."
func TestAnUnparseableConfigIsAStartupErrorNotPlaintextNATS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet.yaml")
	// A file that names all three TLS paths and then fails to parse: YAML
	// forbids a tab in indentation, which is the single most common way a
	// hand-edited config becomes unreadable.
	body := "nats:\n  tls_cert: /etc/wadjet/cert.pem\n  tls_key: /etc/wadjet/key.pem\n\ttls_ca: /etc/wadjet/ca.pem\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	withNATSTLSEnv(t, "", "", "", "", "", "")
	prev := configFile
	configFile = path
	t.Cleanup(func() { configFile = prev })

	cfg, err := loadConfigForNATSTLS()
	if err == nil {
		cert, key, ca, rerr := resolveNATSTLSPaths(cfg)
		t.Fatalf("an unparseable config file NAMING all three TLS paths was accepted: "+
			"loadConfigForNATSTLS returned nil error, and the tier then resolved to "+
			"cert=%q key=%q ca=%q err=%v — the process would connect to NATS in "+
			"PLAINTEXT with no error and no warning (#827, #802).",
			cert, key, ca, rerr)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("the error does not name the file it could not read: %v", err)
	}
}

// TestAReadableConfigStillLoads is the mirror: the refusal above must not be
// satisfiable by refusing every config file.
func TestAReadableConfigStillLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet.yaml")
	body := "nats:\n  tls_cert: /etc/wadjet/cert.pem\n  tls_key: /etc/wadjet/key.pem\n  tls_ca: /etc/wadjet/ca.pem\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	withNATSTLSEnv(t, "", "", "", "", "", "")
	prev := configFile
	configFile = path
	t.Cleanup(func() { configFile = prev })

	cfg, err := loadConfigForNATSTLS()
	if err != nil {
		t.Fatalf("a well-formed config file was refused: %v", err)
	}
	cert, key, ca, err := resolveNATSTLSPaths(cfg)
	if err != nil {
		t.Fatalf("resolveNATSTLSPaths: %v", err)
	}
	if cert != "/etc/wadjet/cert.pem" || key != "/etc/wadjet/key.pem" || ca != "/etc/wadjet/ca.pem" {
		t.Fatalf("the file tier did not reach the connection: (%q, %q, %q)", cert, key, ca)
	}
}

// TestNoConfigFileIsNotAnError keeps the ordinary flag/env deployment out of
// the refusal's way.
func TestNoConfigFileIsNotAnError(t *testing.T) {
	withNATSTLSEnv(t, "", "", "", "", "", "")
	prev := configFile
	configFile = ""
	t.Cleanup(func() { configFile = prev })

	cfg, err := loadConfigForNATSTLS()
	if err != nil || cfg != nil {
		t.Fatalf("no --config should resolve to (nil, nil); got (%v, %v)", cfg, err)
	}
}
