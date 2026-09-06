package server_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A query has an OWNER, and its status, its SQL, its results and its
// cancellation belong to that principal and to an administrator (#936,
// ADR-0034).
//
// At 672bb5e1 the tracker recorded no principal at all and none of the four
// handlers read `IdentityFromContext`, so any authenticated identity could
// read another's SQL by ID, list every query with its full text, fetch a
// completed result the owner's policies had produced, and cancel the query.
// `handleListQueries` published the IDs, so they did not even have to be
// guessed.

const victimSQL = "SELECT secret_value FROM secret"

// registerVictim puts one running query on the tracker owned by admin-user.
func registerVictim(t *testing.T, coord *coordinator.Coordinator, id string) {
	t.Helper()
	coord.Tracker().Register(id, victimSQL,
		auth.IdentitySnapshot{Name: "admin-user", Role: "admin", Method: "apikey"},
		map[string]*coordinator.StageInfo{}, nil)
	coord.Tracker().Start(id)
}

func TestQueryStatusIsOwnerOrAdmin(t *testing.T) {
	ts, coord, _ := censusServer(t)
	registerVictim(t, coord, "victim-query")
	// A second query owned by the reader, so "the caller sees its own" is
	// asserted beside "the caller does not see another's".
	coord.Tracker().Register("reader-query", "SELECT 1 FROM mine",
		auth.IdentitySnapshot{Name: "reader-user", Role: "reader", Method: "apikey"},
		map[string]*coordinator.StageInfo{}, nil)

	for _, tc := range []struct {
		name, key, id string
		want          int
	}{
		{"the owner reads its own status", "admin-key", "victim-query", 200},
		{"another principal is refused", "reader-key", "victim-query", 403},
		{"and so is a third", "writer-key", "victim-query", 403},
		{"the reader reads its own", "reader-key", "reader-query", 200},
		{"an administrator reads anyone's", "admin-key", "reader-query", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := censusDo(t, ts, http.MethodGet, "/v1/queries/"+tc.id, tc.key, "")
			if code != tc.want {
				t.Fatalf("status %d; want %d (body %s)", code, tc.want, body)
			}
			if tc.want == http.StatusForbidden && strings.Contains(body, "secret_value") {
				t.Errorf("the refusal published the SQL it refused: %s", body)
			}
		})
	}
}

func TestQueryResultsAreOwnerOrAdmin(t *testing.T) {
	ts, coord, _ := censusServer(t)
	registerVictim(t, coord, "victim-query")

	for _, tc := range []struct {
		name, key string
		want      int
	}{
		{"the owner", "admin-key", 200},
		{"another principal", "reader-key", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := censusDo(t, ts, http.MethodGet,
				"/v1/queries/victim-query/results", tc.key, "")
			if code != tc.want {
				t.Fatalf("status %d; want %d (body %s)", code, tc.want, body)
			}
		})
	}
}

// A refused cancel does not cancel: the assertion is the query's STATE after
// the refusal, not the status code.
func TestRefusedCancelLeavesTheQueryRunning(t *testing.T) {
	ts, coord, _ := censusServer(t)
	registerVictim(t, coord, "victim-query")

	code, body := censusDo(t, ts, http.MethodDelete, "/v1/queries/victim-query", "reader-key", "")
	if code != http.StatusForbidden {
		t.Fatalf("status %d; want 403 (body %s)", code, body)
	}
	info := coord.Tracker().Get("victim-query")
	if info == nil {
		t.Fatal("the refused cancel deleted the query")
	}
	if info.State != coordinator.QueryStateRunning {
		t.Fatalf("the refused cancel changed the query to %s; want running", info.State)
	}

	// The owner's cancel still works.
	if code, body := censusDo(t, ts, http.MethodDelete,
		"/v1/queries/victim-query", "admin-key", ""); code != http.StatusOK {
		t.Fatalf("the owner's cancel: status %d (body %s); want 200", code, body)
	}
	if info := coord.Tracker().Get("victim-query"); info == nil ||
		info.State != coordinator.QueryStateCancelled {
		t.Fatalf("the owner's cancel did not cancel: %+v", info)
	}
}

// The listing publishes IDs and full SQL text, so it is filtered by the same
// rule that governs reading one of them.
func TestQueryListingIsFilteredByOwner(t *testing.T) {
	ts, coord, _ := censusServer(t)
	registerVictim(t, coord, "victim-query")
	coord.Tracker().Register("reader-query", "SELECT 1 FROM mine",
		auth.IdentitySnapshot{Name: "reader-user", Role: "reader", Method: "apikey"},
		map[string]*coordinator.StageInfo{}, nil)
	// An internal stage entry: never listed, for anyone.
	coord.Tracker().RegisterInternal("st-scan-0-abcd", "",
		map[string]*coordinator.StageInfo{}, nil)

	for _, tc := range []struct {
		name, key string
		wantIDs   []string
		absentIDs []string
	}{
		{"the reader sees only its own", "reader-key",
			[]string{"reader-query"}, []string{"victim-query", "st-scan-0-abcd"}},
		{"the writer owns none", "writer-key",
			nil, []string{"victim-query", "reader-query", "st-scan-0-abcd"}},
		{"an administrator sees every user query, and no internal one", "admin-key",
			[]string{"victim-query", "reader-query"}, []string{"st-scan-0-abcd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := censusDo(t, ts, http.MethodGet, "/v1/queries", tc.key, "")
			if code != http.StatusOK {
				t.Fatalf("status %d; want 200 (%s)", code, body)
			}
			var listing struct {
				Queries []struct {
					QueryID string `json:"query_id"`
				} `json:"queries"`
			}
			if err := json.Unmarshal([]byte(body), &listing); err != nil {
				t.Fatalf("decoding the listing: %v (%s)", err, body)
			}
			listed := map[string]bool{}
			for _, q := range listing.Queries {
				listed[q.QueryID] = true
			}
			for _, want := range tc.wantIDs {
				if !listed[want] {
					t.Errorf("%q is missing from the listing %v", want, keys(listed))
				}
			}
			for _, absent := range tc.absentIDs {
				if listed[absent] {
					t.Errorf("%q is listed to an identity that does not own it: %v",
						absent, keys(listed))
				}
			}
		})
	}
}

// One query's result FILES follow the same rule its status does: the owner
// may drop them, another principal may not.
func TestResultFileDeleteIsOwnerOrAdmin(t *testing.T) {
	ts, coord, _ := censusServer(t)
	coord.Tracker().Register("reader-query", "SELECT 1 FROM mine",
		auth.IdentitySnapshot{Name: "reader-user", Role: "reader", Method: "apikey"},
		map[string]*coordinator.StageInfo{}, nil)

	// The writer owns nothing here.
	if code, body := censusDo(t, ts, http.MethodDelete,
		"/v1/results/reader-query", "writer-key", ""); code != http.StatusForbidden {
		t.Errorf("a non-owner deleted another principal's results: %d (%s)", code, clip(body))
	}
	// The owner gets past the decision (and then meets this fixture's
	// storeless cleaner, which is a 503 — an availability answer, not a
	// refusal).
	if code, body := censusDo(t, ts, http.MethodDelete,
		"/v1/results/reader-query", "reader-key", ""); code == http.StatusForbidden {
		t.Errorf("the owner was refused its own results: %d (%s)", code, clip(body))
	}
}

// The refusal reads the SAME on every door: the HTTP 403 body carries the
// text the gRPC PermissionDenied message carries (ADR-0034).
func TestQueryRefusalTextIsTheSameOnEveryDoor(t *testing.T) {
	ts, coord, _ := censusServer(t)
	registerVictim(t, coord, "victim-query")

	want := `permission denied: query "victim-query" belongs to another principal`
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/v1/queries/victim-query"},
		{http.MethodGet, "/v1/queries/victim-query/results"},
		{http.MethodDelete, "/v1/queries/victim-query"},
	} {
		code, body := censusDo(t, ts, r.method, r.path, "reader-key", "")
		if code != http.StatusForbidden {
			t.Errorf("%s %s: status %d; want 403", r.method, r.path, code)
			continue
		}
		var payload struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			t.Errorf("%s %s: body %s is not JSON: %v", r.method, r.path, clip(body), err)
			continue
		}
		if payload.Error != want {
			t.Errorf("%s %s: refusal text %q; want %q", r.method, r.path, payload.Error, want)
		}
	}
}

// With auth enabled, an entry nobody owns is an administrator's alone: nobody
// can claim what nobody owns.
func TestUnownedQueryIsAdministratorOnly(t *testing.T) {
	ts, coord, _ := censusServer(t)
	coord.Tracker().Register("legacy-query", victimSQL,
		auth.IdentitySnapshot{}, map[string]*coordinator.StageInfo{}, nil)

	if code, body := censusDo(t, ts, http.MethodGet,
		"/v1/queries/legacy-query", "reader-key", ""); code != http.StatusForbidden {
		t.Errorf("an unowned query answered a reader with %d (%s); want 403", code, body)
	}
	if code, body := censusDo(t, ts, http.MethodGet,
		"/v1/queries/legacy-query", "admin-key", ""); code != http.StatusOK {
		t.Errorf("an unowned query refused an administrator with %d (%s); want 200", code, body)
	}
}

// The owner is captured from the SUBMITTING context, through the real door.
func TestSubmittedQueryRecordsItsSubmitter(t *testing.T) {
	ts, coord, cat := censusServer(t)
	if err := cat.CreateTable(t.Context(), "owned_t",
		parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}},
		nil); err != nil {
		t.Fatalf("creating the table: %v", err)
	}

	code, body := censusDo(t, ts, http.MethodPost, "/v1/queries/async", "reader-key",
		`{"sql":"SELECT id FROM owned_t"}`)
	if code != http.StatusAccepted {
		t.Fatalf("submitting: status %d (%s); want 202", code, body)
	}
	var submitted struct {
		QueryID string `json:"query_id"`
	}
	if err := json.Unmarshal([]byte(body), &submitted); err != nil {
		t.Fatalf("decoding the submission: %v (%s)", err, body)
	}
	if submitted.QueryID == "" {
		t.Fatalf("no query_id in %s", body)
	}
	// The full uuid, not eight characters of one: 32 bits is guessable by a
	// caller who may run queries but may not read this one's.
	if len(submitted.QueryID) != 36 {
		t.Errorf("query_id %q is %d characters; want a full 36-character uuid",
			submitted.QueryID, len(submitted.QueryID))
	}

	info := coord.Tracker().Get(submitted.QueryID)
	if info == nil {
		t.Fatalf("the submitted query %q is not tracked", submitted.QueryID)
	}
	if info.Owner.Name != "reader-user" {
		t.Errorf("owner %+v; want the submitting identity reader-user", info.Owner)
	}

	// And the door enforces it: the submitter reads it, another principal does not.
	if code, body := censusDo(t, ts, http.MethodGet,
		"/v1/queries/"+submitted.QueryID, "reader-key", ""); code != http.StatusOK {
		t.Errorf("the submitter was refused its own query: %d (%s)", code, body)
	}
	if code, body := censusDo(t, ts, http.MethodGet,
		"/v1/queries/"+submitted.QueryID, "writer-key", ""); code != http.StatusForbidden {
		t.Errorf("another principal read the submitter's query: %d (%s)", code, body)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
