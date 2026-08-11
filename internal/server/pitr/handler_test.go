package pitr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/a-shan/mysql-pitr/internal/server/agent"
	"github.com/a-shan/mysql-pitr/internal/server/audit"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/server/store"
	"github.com/a-shan/mysql-pitr/internal/ws"
	"github.com/a-shan/mysql-pitr/internal/ws/hub"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE (Task 4 placeholder): the previous 832-line handler test exercised the
// v2 flow (preflight -> confirmed -> parsing -> previewed -> executing against
// the InMemory store). That model was replaced by the v3 model in this task,
// so those tests are gone; the full v3 handler suite lands with the handler
// rewrite in Task 6. This file keeps placeholder coverage of the endpoints
// that survive: Start (record creation + guard checks), List, Status, Cancel
// (v3 transition gate + audit), and the not-yet-implemented 501 stubs.

// fakeCommander implements AgentCommander with a canned response; the
// placeholder handler only calls IsConnected.
type fakeCommander struct {
	connected bool
}

func (f *fakeCommander) IsConnected(agentID string) bool { return f.connected }

func (f *fakeCommander) SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error) {
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusOK}, nil
}

func (f *fakeCommander) SetProgressHandler(fn hub.ProgressHandler) {}

// testFixture wires a Handler against the SQLite store and the InMemory
// domain stores.
type testFixture struct {
	handler    *Handler
	opStore    OperationStore
	agentStore *agent.InMemoryAgentStore
	orgStore   *org.InMemoryOrgStore
	auditStore *audit.InMemoryAuditStore
	userStore  *auth.InMemoryUserStore
	commander  *fakeCommander
	secret     []byte
}

func setupTest(t *testing.T) *testFixture {
	t.Helper()
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	t.Cleanup(func() { _ = db.Close() })
	opStore := NewSQLiteOperationStore(db)
	agentStore := agent.NewInMemoryAgentStore()
	orgStore := org.NewInMemoryOrgStore()
	auditStore := audit.NewInMemoryAuditStore()
	userStore := auth.NewInMemoryUserStore()
	secret := []byte("pitr-test-secret")
	commander := &fakeCommander{connected: true}
	handler := NewHandler(opStore, agentStore, orgStore, auditStore, secret, commander)
	return &testFixture{
		handler:    handler,
		opStore:    opStore,
		agentStore: agentStore,
		orgStore:   orgStore,
		auditStore: auditStore,
		userStore:  userStore,
		commander:  commander,
		secret:     secret,
	}
}

// userSeq makes fixture user emails unique even when two users are created
// within the same clock tick.
var userSeq int64

func (f *testFixture) createUser(t *testing.T) string {
	t.Helper()
	userSeq++
	user := &auth.User{
		Email:          fmt.Sprintf("%s-%d-%d@example.com", t.Name(), time.Now().UnixNano(), userSeq),
		HashedPassword: "hash",
	}
	err := f.userStore.Create(user)
	require.NoError(t, err)
	return user.ID
}

func (f *testFixture) createOrg(t *testing.T, userID string) string {
	t.Helper()
	org := &org.Organization{Name: "Test Org"}
	err := f.orgStore.Create(org)
	require.NoError(t, err)
	err = f.orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)
	return org.ID
}

func (f *testFixture) createAgent(t *testing.T, orgID string) string {
	t.Helper()
	agt := &agent.AgentRecord{
		OrgID:    orgID,
		Hostname: "db-01.example.com",
		Approved: true,
	}
	err := f.agentStore.Create(agt)
	require.NoError(t, err)
	return agt.ID
}

func (f *testFixture) authenticatedRequest(t *testing.T, method, target string, body interface{}, userID string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, target, &buf)
	claims := &auth.Claims{UserID: userID, Email: "test@example.com"}
	req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
	return req
}

// authenticatedRouteRequest builds an authenticated request with a chi route
// context so chi.URLParam("id") resolves to id.
func (f *testFixture) authenticatedRouteRequest(t *testing.T, method, target, id string, body interface{}, userID string) *http.Request {
	t.Helper()
	req := f.authenticatedRequest(t, method, target, body, userID)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", id)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func startBody(agentID string) map[string]string {
	return map[string]string{
		"agent_id":      agentID,
		"target_table":  "orders",
		"recovery_time": "2026-07-08T14:00:00Z",
		"mode":          "preview",
	}
}

// ---------- Start ----------

func TestStart_Success_CreatesOperation(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	var resp startResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.NotEmpty(t, resp.OperationID)
	assert.Equal(t, StateCreated, resp.Status)

	op, err := f.opStore.Get(resp.OperationID)
	require.NoError(t, err)
	assert.Equal(t, orgID, op.OrgID)
	assert.Equal(t, agentID, op.AgentID)
	assert.Equal(t, "pitr", op.Type)
	assert.Equal(t, StateCreated, op.Status)
	assert.Equal(t, userID, op.CreatedBy)
	assert.False(t, op.CreatedAt.IsZero())
}

func TestStart_NoAuth(t *testing.T) {
	f := setupTest(t)
	req := httptest.NewRequest(http.MethodPost, "/api/pitr/start",
		bytes.NewReader([]byte(`{"agent_id":"x","target_table":"y","recovery_time":"2026-01-01T00:00:00Z","mode":"preview"}`)))
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestStart_MissingFields(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	tests := []struct {
		name string
		body map[string]string
	}{
		{"empty body", map[string]string{}},
		{"missing agent_id", map[string]string{"target_table": "x", "recovery_time": "2026-01-01T00:00:00Z", "mode": "preview"}},
		{"missing target_table", map[string]string{"agent_id": agentID, "recovery_time": "2026-01-01T00:00:00Z", "mode": "preview"}},
		{"missing recovery_time", map[string]string{"agent_id": agentID, "target_table": "x", "mode": "preview"}},
		{"missing mode", map[string]string{"agent_id": agentID, "target_table": "x", "recovery_time": "2026-01-01T00:00:00Z"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", tc.body, userID)
			w := httptest.NewRecorder()
			f.handler.Start(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestStart_InvalidMode(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	body := startBody(agentID)
	body["mode"] = "invalid"
	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", body, userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStart_AgentNotFound(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	_ = f.createOrg(t, userID)

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody("nonexistent"), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestStart_NotOrgMember(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	otherUser := f.createUser(t)
	orgID := f.createOrg(t, otherUser) // other user owns the org
	agentID := f.createAgent(t, orgID)

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestStart_AgentOffline(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.commander.connected = false

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

// ---------- List ----------

func TestList_FiltersByOrg(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	require.NoError(t, f.opStore.Create(&Operation{ID: "op_1", OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateCreated, CreatedBy: userID}))
	require.NoError(t, f.opStore.Create(&Operation{ID: "op_2", OrgID: "other_org", AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateCreated, CreatedBy: userID}))

	req := f.authenticatedRequest(t, http.MethodGet, "/api/pitr?org_id="+orgID, nil, userID)
	w := httptest.NewRecorder()
	f.handler.List(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Operations []operationView `json:"operations"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp.Operations, 1)
	assert.Equal(t, "op_1", resp.Operations[0].ID)
	assert.True(t, resp.Operations[0].AgentConnected)
}

func TestList_NoAuth(t *testing.T) {
	f := setupTest(t)
	req := httptest.NewRequest(http.MethodGet, "/api/pitr?org_id=x", nil)
	w := httptest.NewRecorder()
	f.handler.List(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// ---------- Status ----------

func TestStatus_Success(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	op := &Operation{ID: "op_status", OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateScanning, CreatedBy: userID}
	require.NoError(t, f.opStore.Create(op))

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_status/status", "op_status", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Status(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp statusResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "op_status", resp.ID)
	assert.Equal(t, StateScanning, resp.Status)
	assert.Equal(t, "pitr", resp.Type)
}

func TestStatus_NotFound(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	f.createOrg(t, userID)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/nonexistent/status", "nonexistent", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Status(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestStatus_NotMember(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	otherUser := f.createUser(t)
	orgID := f.createOrg(t, otherUser)
	agentID := f.createAgent(t, orgID)

	op := &Operation{ID: "op_member", OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateCreated}
	require.NoError(t, f.opStore.Create(op))

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_member/status", "op_member", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Status(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ---------- Cancel ----------

func TestCancel_Success(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	op := &Operation{ID: "op_cancel", OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateReady}
	require.NoError(t, f.opStore.Create(op))

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_cancel/cancel", "op_cancel", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Cancel(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp cancelResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "op_cancel", resp.OperationID)
	assert.Equal(t, StateCancelled, resp.Status)

	got, err := f.opStore.Get("op_cancel")
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status)

	// Verify audit entry was created.
	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "cancelled", entries[0].Status)
}

func TestCancel_InvalidState(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	op := &Operation{ID: "op_terminal", OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateDone}
	require.NoError(t, f.opStore.Create(op))

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_terminal/cancel", "op_terminal", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Cancel(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)

	// Terminal state untouched.
	got, err := f.opStore.Get("op_terminal")
	require.NoError(t, err)
	assert.Equal(t, StateDone, got.Status)
}

func TestCancel_NoAuth(t *testing.T) {
	f := setupTest(t)
	req := httptest.NewRequest(http.MethodPost, "/api/pitr/xxx/cancel", nil)
	w := httptest.NewRecorder()
	f.handler.Cancel(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// ---------- Task 6 stubs ----------

func TestV3FlowEndpoints_NotImplemented(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := &Operation{ID: "op_stub", OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: StateReady}
	require.NoError(t, f.opStore.Create(op))

	tests := []struct {
		name string
		fn   func(w http.ResponseWriter, r *http.Request)
	}{
		{"preview", f.handler.Preview},
		{"progress", f.handler.Progress},
		{"execute", f.handler.Execute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_stub/"+tc.name, "op_stub", nil, userID)
			w := httptest.NewRecorder()
			tc.fn(w, req)
			assert.Equal(t, http.StatusNotImplemented, w.Code)
		})
	}
}
