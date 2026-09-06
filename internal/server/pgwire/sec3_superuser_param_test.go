package pgwire

import (
	"encoding/binary"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
)

// `is_superuser` is what the AUTHORIZER says, never what the role is CALLED
// (#938, ADR-0034: no admin by inference).
//
// At 672bb5e1 the parameter was `c.identity.Role != "admin"`. A role NAMED
// `admin` that holds only `read` reported `is_superuser=on`, and a role named
// `ops` that HOLDS `admin` reported `off` — a psql prompt and every client
// that branches on this parameter read a privilege nobody granted.

// startupCollectingParams authenticates and returns every ParameterStatus the
// server sent before ReadyForQuery.
func (c *pgClient) startupCollectingParams(user, database, password string) map[string]string {
	c.t.Helper()
	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, 196608)
	for _, kv := range [][2]string{{"user", user}, {"database", database}} {
		payload = append(payload, kv[0]...)
		payload = append(payload, 0)
		payload = append(payload, kv[1]...)
		payload = append(payload, 0)
	}
	payload = append(payload, 0)
	msg := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+4))
	msg = append(msg, payload...)
	if _, err := c.conn.Write(msg); err != nil {
		c.t.Fatalf("writing startup: %v", err)
	}

	params := map[string]string{}
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading startup response: %v", err)
		}
		switch typ {
		case 'R':
			if binary.BigEndian.Uint32(data[:4]) == 3 {
				c.writeMsg('p', append([]byte(password), 0))
			}
		case 'S':
			k := readCString(data)
			params[k] = readCString(data[len(k)+1:])
		case 'E':
			c.t.Fatalf("startup error: %s", c.parseError(data))
		case 'Z':
			return params
		}
	}
}

// `is_superuser` is what the authorizer says, not what the role is called.
func TestPGWireIsSuperuserFollowsThePermission(t *testing.T) {
	for _, tc := range []struct {
		name, role, want string
		allow            []string
	}{
		{"a role NAMED admin holding only read", "admin", "off", []string{"read"}},
		{"a role named ops HOLDING admin", "ops", "on", []string{"read", "write", "admin"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sec3CopyDB(t)
			authn, authz := auth.New(auth.Config{
				Enabled: true,
				APIKeys: []auth.APIKeyDef{{Key: "k", Name: "u", Role: tc.role}},
				Roles:   []auth.RoleConfig{{Name: tc.role, Tables: []string{"*"}, Allow: tc.allow}},
			})
			provider := auth.NewProvider(authn, authz, nil, nil)
			db.SetAuthProvider(provider)
			srv := startTestServerWithAuth(t, db, provider)

			client := newPGClient(t, srv.Addr())
			defer client.terminate()
			params := client.startupCollectingParams("u", "testdb", "k")
			if got := params["is_superuser"]; got != tc.want {
				t.Errorf("is_superuser=%q; want %q", got, tc.want)
			}
		})
	}
}
