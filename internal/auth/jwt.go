package auth

import (
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// JWTConfig holds JWT verification configuration.
type JWTConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Secret        string `yaml:"secret"`         // HMAC-SHA256 secret
	PublicKeyFile string `yaml:"public_key_file"` // PEM file path for RSA verification
	RoleClaim     string `yaml:"role_claim"`      // JWT claim containing role (default: "role")
	Issuer        string `yaml:"issuer"`          // expected issuer (optional)
}

// JWTVerifier validates JWT tokens and extracts identities.
type JWTVerifier struct {
	hmacSecret []byte
	rsaKey     *rsa.PublicKey
	roleClaim  string
	issuer     string
	roles      map[string]*RoleDef
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

type jwtClaims struct {
	Sub    string `json:"sub"`
	Iss    string `json:"iss"`
	Exp    int64  `json:"exp"`
	Nbf    int64  `json:"nbf"`
	Iat    int64  `json:"iat"`
	Name   string `json:"name"`
	Extras map[string]any
}

// NewJWTVerifier creates a JWT verifier from configuration.
func NewJWTVerifier(cfg JWTConfig, roles map[string]*RoleDef) (*JWTVerifier, error) {
	v := &JWTVerifier{
		roleClaim: cfg.RoleClaim,
		issuer:    cfg.Issuer,
		roles:     roles,
	}
	if v.roleClaim == "" {
		v.roleClaim = "role"
	}

	if cfg.Secret != "" {
		v.hmacSecret = []byte(cfg.Secret)
	}

	if cfg.PublicKeyFile != "" {
		key, err := loadRSAPublicKey(cfg.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading RSA public key: %w", err)
		}
		v.rsaKey = key
	}

	if v.hmacSecret == nil && v.rsaKey == nil {
		return nil, errors.New("JWT enabled but no secret or public_key_file configured")
	}

	return v, nil
}

// Verify validates a JWT token string and returns the identity.
func (v *JWTVerifier) Verify(tokenStr string) (*Identity, error) {
	parts := strings.SplitN(tokenStr, ".", 3)
	if len(parts) != 3 {
		return nil, errors.New("invalid JWT format")
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT header: %w", err)
	}

	var header jwtHeader
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, fmt.Errorf("parsing JWT header: %w", err)
	}

	// Verify signature
	signingInput := parts[0] + "." + parts[1]
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT signature: %w", err)
	}

	switch header.Alg {
	case "HS256":
		if v.hmacSecret == nil {
			return nil, errors.New("HS256 token but no HMAC secret configured")
		}
		if !verifyHMAC256([]byte(signingInput), signature, v.hmacSecret) {
			return nil, errors.New("invalid JWT signature")
		}
	case "RS256":
		if v.rsaKey == nil {
			return nil, errors.New("RS256 token but no public key configured")
		}
		if err := verifyRSA256([]byte(signingInput), signature, v.rsaKey); err != nil {
			return nil, fmt.Errorf("invalid RSA signature: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported JWT algorithm: %s", header.Alg)
	}

	// Decode claims
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT claims: %w", err)
	}

	// Parse into structured fields and raw map
	var claims jwtClaims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, fmt.Errorf("parsing JWT claims: %w", err)
	}
	// Also parse into generic map for custom role claim
	var rawClaims map[string]any
	json.Unmarshal(claimsJSON, &rawClaims)
	claims.Extras = rawClaims

	// Validate time claims
	now := time.Now().Unix()
	if claims.Exp > 0 && now > claims.Exp {
		return nil, errors.New("JWT token expired")
	}
	if claims.Nbf > 0 && now < claims.Nbf {
		return nil, errors.New("JWT token not yet valid")
	}

	// Validate issuer
	if v.issuer != "" && claims.Iss != v.issuer {
		return nil, fmt.Errorf("JWT issuer mismatch: got %q, want %q", claims.Iss, v.issuer)
	}

	// Extract role from configured claim
	roleName, _ := claims.Extras[v.roleClaim].(string)
	if roleName == "" {
		return nil, fmt.Errorf("JWT missing role claim %q", v.roleClaim)
	}

	role := v.roles[roleName]
	if role == nil {
		return nil, fmt.Errorf("unknown role %q from JWT", roleName)
	}

	name := claims.Name
	if name == "" {
		name = claims.Sub
	}

	return &Identity{
		Name:   name,
		Role:   roleName,
		Method: "jwt",
		Tables: role.Tables,
		Perms:  role.Perms,
	}, nil
}

func verifyHMAC256(message, signature, secret []byte) bool {
	mac := hmac.New(sha256.New, secret)
	mac.Write(message)
	expected := mac.Sum(nil)
	return hmac.Equal(signature, expected)
}

func verifyRSA256(message, signature []byte, key *rsa.PublicKey) error {
	hash := sha256.Sum256(message)
	return rsa.VerifyPKCS1v15(key, crypto.SHA256, hash[:], signature)
}

func loadRSAPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found in public key file")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	rsaKey, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is %T, not RSA", pub)
	}

	return rsaKey, nil
}
