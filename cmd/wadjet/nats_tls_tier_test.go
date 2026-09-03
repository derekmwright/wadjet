package main

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/config"
)

// resolveNATSTLSPaths' config-file tier exists, and material that is named
// but unusable is a startup error (#827).
//
// The function's own doc comment promised "CLI flags take priority, then
// env vars, then config file values" and the body read the flags and
// os.Getenv and nothing else — cfg.NATS.TLSCert/TLSKey/TLSCA were parsed,
// validated, reported by GET /v1/admin/config, and never consulted. A
// deployment that put its mTLS material in the YAML, exactly as
// docs/configuration.md describes, got a server that connected to NATS
// WITHOUT TLS and logged nothing.
//
// The precedence assertions are here as well as the tier, because the tier
// is only correct if it sits UNDER the two that already worked: a flag must
// still beat an env var, and an env var must still beat the file.
func TestNATSTLSResolutionTiers(t *testing.T) {
	fileCfg := &config.Config{}
	fileCfg.NATS.TLSCert = "/file/cert.pem"
	fileCfg.NATS.TLSKey = "/file/key.pem"
	fileCfg.NATS.TLSCA = "/file/ca.pem"

	for _, tc := range []struct {
		name                      string
		flagCert, flagKey, flagCA string
		envCert, envKey, envCA    string
		cfg                       *config.Config
		wantCert, wantKey, wantCA string
	}{
		{
			name:     "config file alone reaches the connection",
			cfg:      fileCfg,
			wantCert: "/file/cert.pem", wantKey: "/file/key.pem", wantCA: "/file/ca.pem",
		},
		{
			name:    "env beats file",
			envCert: "/env/cert.pem", envKey: "/env/key.pem", envCA: "/env/ca.pem",
			cfg:      fileCfg,
			wantCert: "/env/cert.pem", wantKey: "/env/key.pem", wantCA: "/env/ca.pem",
		},
		{
			name:     "flag beats env",
			flagCert: "/flag/cert.pem", flagKey: "/flag/key.pem", flagCA: "/flag/ca.pem",
			envCert: "/env/cert.pem", envKey: "/env/key.pem", envCA: "/env/ca.pem",
			cfg:      fileCfg,
			wantCert: "/flag/cert.pem", wantKey: "/flag/key.pem", wantCA: "/flag/ca.pem",
		},
		{
			name: "nothing configured stays plaintext",
			cfg:  &config.Config{},
		},
		{
			name: "nil config is not a crash",
		},
		{
			// N3: the tiers are resolved PER FIELD, so a deployment may take
			// the certificate from a flag, the key from the environment and
			// the CA from the file. The property is what position 8 names;
			// nothing pinned it.
			name:     "mixed tiers resolve per field",
			flagCert: "/flag/cert.pem",
			envKey:   "/env/key.pem",
			cfg:      fileCfg,
			wantCert: "/flag/cert.pem", wantKey: "/env/key.pem", wantCA: "/file/ca.pem",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withNATSTLSEnv(t, tc.flagCert, tc.flagKey, tc.flagCA, tc.envCert, tc.envKey, tc.envCA)
			cert, key, ca, err := resolveNATSTLSPaths(tc.cfg)
			if err != nil {
				t.Fatalf("resolveNATSTLSPaths: %v", err)
			}
			if cert != tc.wantCert || key != tc.wantKey || ca != tc.wantCA {
				t.Fatalf("got (%q, %q, %q), want (%q, %q, %q)",
					cert, key, ca, tc.wantCert, tc.wantKey, tc.wantCA)
			}
		})
	}
}

// TestPartialNATSTLSMaterialIsAStartupError: the connection is secured only
// when all three paths are present, so naming one or two of them used to
// disable TLS silently. An operator who wrote a cert and a key and forgot
// the CA got plaintext.
func TestPartialNATSTLSMaterialIsAStartupError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cert, key, ca string
		wantInMessage string
	}{
		{"cert and key, no CA", "/c.pem", "/k.pem", "", "CA"},
		{"cert and CA, no key", "/c.pem", "", "/ca.pem", "private key"},
		{"CA alone", "", "", "/ca.pem", "certificate and private key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withNATSTLSEnv(t, "", "", "", "", "", "")
			cfg := &config.Config{}
			cfg.NATS.TLSCert, cfg.NATS.TLSKey, cfg.NATS.TLSCA = tc.cert, tc.key, tc.ca
			_, _, _, err := resolveNATSTLSPaths(cfg)
			if err == nil {
				t.Fatal("partially configured NATS TLS material was accepted; the process " +
					"would connect to NATS WITHOUT TLS and say nothing (#827)")
			}
			if !strings.Contains(err.Error(), tc.wantInMessage) {
				t.Fatalf("the error does not name what is missing (%q): %v", tc.wantInMessage, err)
			}
		})
	}
}

// withNATSTLSEnv sets the flag variables and the environment for one case
// and restores them afterwards.
func withNATSTLSEnv(t *testing.T, fCert, fKey, fCA, eCert, eKey, eCA string) {
	t.Helper()
	oc, ok_, oa := natsTLSCert, natsTLSKey, natsTLSCA
	natsTLSCert, natsTLSKey, natsTLSCA = fCert, fKey, fCA
	t.Cleanup(func() { natsTLSCert, natsTLSKey, natsTLSCA = oc, ok_, oa })
	t.Setenv("WADJET_NATS_TLS_CERT", eCert)
	t.Setenv("WADJET_NATS_TLS_KEY", eKey)
	t.Setenv("WADJET_NATS_TLS_CA", eCA)
}
