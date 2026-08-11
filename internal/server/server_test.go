package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-shan/mysql-pitr/internal/server/pitr"
	"github.com/a-shan/mysql-pitr/internal/server/store"
	"github.com/a-shan/mysql-pitr/internal/ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCommander is a controllable pitr.AgentCommander used to smoke-test the
// HTTP flow without a live WebSocket agent: every agent is connected and
// every command is accepted. The production path (New) uses the real hub.
type fakeCommander struct{}

func (fakeCommander) IsConnected(agentID string) bool { return true }

func (fakeCommander) SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error) {
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusOK}, nil
}

// TestNewBootstrap exercises server.New() end to end against a temporary
// AGENT_DATA_DIR: the shared SQLite database and CA file are created, both
// handlers and the hub are wired, the embed placeholder serves "/", and the
// v3 PITR routes are mounted behind auth (401, not 404, when unauthenticated).
func TestNewBootstrap(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("AGENT_DATA_DIR", dataDir)

	srv, err := New()
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	require.NotNil(t, srv.Web, "web handler")
	require.NotNil(t, srv.Agent, "agent handler")
	require.NotNil(t, srv.Hub, "hub")
	require.NotNil(t, srv.TLSConfig, "tls config")

	// Platform state persisted under the data dir: SQLite db + CA file.
	for _, f := range []string{"app.db", "ca.json"} {
		_, statErr := os.Stat(filepath.Join(dataDir, f))
		assert.NoErrorf(t, statErr, "expected %s in data dir", f)
	}

	// GET / serves the embed placeholder page.
	w := httptest.NewRecorder()
	srv.Web.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "PITR 平台")

	// Static stub files resolve through the embed filesystem.
	w = httptest.NewRecorder()
	srv.Web.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/app.css", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/css")

	// Unknown non-API routes fall back to the SPA index page.
	w = httptest.NewRecorder()
	srv.Web.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/some/spa/route", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "PITR 平台")

	// The v3 SSE route exists: unauthenticated requests hit the auth
	// middleware (401) instead of chi's 404 — proving the route is mounted.
	w = httptest.NewRecorder()
	srv.Web.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/pitr/1/events", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestNewBootstrapError asserts New() surfaces a store/setup failure instead
// of panicking when the data directory is unusable.
func TestNewBootstrapError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("file"), 0o600))
	t.Setenv("AGENT_DATA_DIR", filepath.Join(blocker, "sub"))

	_, err := New()
	require.Error(t, err)
}

// TestNewServer_ReconcilesStaleOps asserts the startup reconcile wiring: an
// in-flight operation persisted by a previous server run (left mid-scan) is
// blocked when the new server boots, because every agent is offline at
// startup.
func TestNewServer_ReconcilesStaleOps(t *testing.T) {
	dataDir := t.TempDir()

	// Simulate a previous server run: an operation left mid-scan.
	db, err := store.Open(filepath.Join(dataDir, "app.db"))
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	prevStore := pitr.NewSQLiteOperationStore(db)
	op := &pitr.Operation{ID: "op_stale", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql", Status: pitr.StateScanning}
	require.NoError(t, prevStore.Create(op))
	require.NoError(t, db.Close())

	srv, err := newServer(dataDir, fakeCommander{})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	db2, err := store.Open(filepath.Join(dataDir, "app.db"))
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	got, err := pitr.NewSQLiteOperationStore(db2).Get("op_stale")
	require.NoError(t, err)
	assert.Equal(t, pitr.StateBlocked, got.Status, "stale in-flight op must be blocked at startup")
}

// TestPITRFlowSmoke drives the full HTTP workflow against newServer with an
// injected fake commander: register → login → create org → register agent →
// approve → start pitr → status/transactions. Every step lands in the shared
// SQLite database via the real handlers and router.
func TestPITRFlowSmoke(t *testing.T) {
	dataDir := t.TempDir()
	srv, err := newServer(dataDir, fakeCommander{})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	ts := httptest.NewServer(srv.Web)
	defer ts.Close()

	post := func(t *testing.T, path string, body interface{}, token string) (*http.Response, map[string]interface{}) {
		t.Helper()
		var buf bytes.Buffer
		if body != nil {
			require.NoError(t, json.NewEncoder(&buf).Encode(body))
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
		require.NoError(t, err)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var out map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp, out
	}

	get := func(t *testing.T, path, token string) []byte {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		require.NoError(t, err)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET "+path)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return body
	}

	// Register the admin user.
	resp, _ := post(t, "/api/auth/register",
		map[string]interface{}{"email": "alice@example.com", "password": "secret123"}, "")
	require.Equal(t, http.StatusCreated, resp.StatusCode, "register")

	// Login.
	resp, out := post(t, "/api/auth/login",
		map[string]interface{}{"email": "alice@example.com", "password": "secret123"}, "")
	require.Equal(t, http.StatusOK, resp.StatusCode, "login")
	token, ok := out["token"].(string)
	require.True(t, ok, "login response carries a token")

	// Create the organisation (the user becomes admin).
	resp, out = post(t, "/api/orgs", map[string]interface{}{"name": "Acme"}, token)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create org")
	org := out["organization"].(map[string]interface{})
	orgID, _ := org["id"].(string)
	require.NotEmpty(t, orgID, "org id")

	// Register an agent in the org (pending approval).
	resp, out = post(t, "/api/agents/register",
		map[string]interface{}{"orgId": orgID, "hostname": "db-1"}, token)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "register agent")
	agt := out["agent"].(map[string]interface{})
	agentID, _ := agt["id"].(string)
	require.NotEmpty(t, agentID, "agent id")

	// Approve the agent.
	resp, _ = post(t, "/api/agents/"+agentID+"/approve", nil, token)
	require.Equal(t, http.StatusOK, resp.StatusCode, "approve agent")

	// Start a PITR operation: the fake commander accepts the scan command,
	// so the operation lands in `scanning`.
	resp, out = post(t, "/api/pitr/start", map[string]interface{}{
		"agentId": agentID,
		"type":    "pitr",
		"mode":    "meta",
		"filter":  map[string]interface{}{},
	}, token)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "start pitr: %v", out)
	opID, _ := out["operationId"].(string)
	require.NotEmpty(t, opID, "operation id")
	assert.Equal(t, "scanning", out["status"])

	// Status through the SQLite-backed authorize path.
	statusBody := get(t, "/api/pitr/"+opID+"/status", token)
	var status map[string]interface{}
	require.NoError(t, json.Unmarshal(statusBody, &status))
	assert.Equal(t, "scanning", status["status"])

	// Transactions endpoint reachable (empty preview for a fresh scan).
	_ = get(t, "/api/pitr/"+opID+"/transactions", token)

	// The persisted database now holds the operation.
	dbPath := filepath.Join(dataDir, "app.db")
	info, err := os.Stat(dbPath)
	require.NoError(t, err, "app.db exists")
	assert.Greater(t, info.Size(), int64(0))
}
