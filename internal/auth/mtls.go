package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

// MTLSConfig holds mTLS authentication configuration.
type MTLSConfig struct {
	Enabled    bool              `yaml:"enabled"`
	CAFile     string            `yaml:"ca_file"`     // CA certificate PEM file for verifying client certs
	RoleMap    map[string]string `yaml:"role_map"`     // CN or SAN -> role mapping
	DefaultRole string           `yaml:"default_role"` // role for valid certs not in role_map
}

// MTLSVerifier authenticates clients via TLS client certificates.
type MTLSVerifier struct {
	roleMap     map[string]string // CN/SAN -> role name
	defaultRole string
	roles       map[string]*RoleDef
}

// NewMTLSVerifier creates an mTLS verifier from configuration.
func NewMTLSVerifier(cfg MTLSConfig, roles map[string]*RoleDef) *MTLSVerifier {
	return &MTLSVerifier{
		roleMap:     cfg.RoleMap,
		defaultRole: cfg.DefaultRole,
		roles:       roles,
	}
}

// Verify extracts identity from a verified client certificate.
// The TLS handshake (certificate chain validation) is handled by Go's TLS stack
// via tls.Config.ClientAuth + ClientCAs. This method maps the verified cert to a role.
func (v *MTLSVerifier) Verify(cert *x509.Certificate) (*Identity, error) {
	// Try matching Common Name first
	roleName := v.matchRole(cert.Subject.CommonName)

	// Try DNS SANs
	if roleName == "" {
		for _, san := range cert.DNSNames {
			roleName = v.matchRole(san)
			if roleName != "" {
				break
			}
		}
	}

	// Try email SANs
	if roleName == "" {
		for _, email := range cert.EmailAddresses {
			roleName = v.matchRole(email)
			if roleName != "" {
				break
			}
		}
	}

	// Fall back to default role
	if roleName == "" {
		roleName = v.defaultRole
	}

	if roleName == "" {
		return nil, fmt.Errorf("no role mapping for certificate CN=%q", cert.Subject.CommonName)
	}

	role := v.roles[roleName]
	if role == nil {
		return nil, fmt.Errorf("unknown role %q for certificate CN=%q", roleName, cert.Subject.CommonName)
	}

	name := cert.Subject.CommonName
	if name == "" && len(cert.DNSNames) > 0 {
		name = cert.DNSNames[0]
	}

	// Build attributes from cert fields for ABAC evaluation
	attrs := Attributes{
		"cn": cert.Subject.CommonName,
	}
	if len(cert.Subject.Organization) > 0 {
		attrs["org"] = cert.Subject.Organization[0]
	}
	if len(cert.Subject.OrganizationalUnit) > 0 {
		attrs["ou"] = cert.Subject.OrganizationalUnit[0]
	}
	if len(cert.DNSNames) > 0 {
		attrs["san_dns"] = cert.DNSNames
	}
	if len(cert.EmailAddresses) > 0 {
		attrs["san_email"] = cert.EmailAddresses
	}
	if cert.Issuer.CommonName != "" {
		attrs["issuer"] = cert.Issuer.CommonName
	}

	return &Identity{
		Name:       name,
		Role:       roleName,
		Method:     "mtls",
		Tables:     role.Tables,
		Perms:      role.Perms,
		Attributes: attrs,
	}, nil
}

func (v *MTLSVerifier) matchRole(identifier string) string {
	if identifier == "" {
		return ""
	}
	// Exact match
	if role, ok := v.roleMap[identifier]; ok {
		return role
	}
	// Case-insensitive match
	lower := strings.ToLower(identifier)
	for k, role := range v.roleMap {
		if strings.ToLower(k) == lower {
			return role
		}
	}
	return ""
}

// LoadClientCA loads a CA certificate pool for verifying client certificates.
// Used when building the tls.Config for the HTTP server.
func LoadClientCA(caFile string) (*x509.CertPool, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate from %s", caFile)
	}

	return pool, nil
}

// NewTLSConfig creates a tls.Config for the server with mTLS client verification.
// serverCert and serverKey are paths to the server's TLS certificate and key.
// clientCA is the CA pool for verifying client certificates.
func NewTLSConfig(serverCertFile, serverKeyFile string, clientCA *x509.CertPool) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
