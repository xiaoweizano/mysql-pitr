package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withChiURLParam sets a chi URL parameter on the request context.
func withChiURLParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// test helpers

// fakeCommander implements the archive endpoint's AgentCommander: it records
// every dispatched command and lets tests control connectivity, transport
// errors, and the agent's response payload.
type fakeCommander struct {
	mu        sync.Mutex
	connected bool
	sendErr   error
	agentErr  string
	result    interface{}
	sent      []ws.Command
}

func (f *fakeCommander) IsConnected(agentID string) bool { return f.connected }

func (f *fakeCommander) SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, cmd)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	if f.agentErr != "" {
		return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusError, Error: f.agentErr}, nil
	}
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusOK, Result: f.result}, nil
}

func (f *fakeCommander) commands() []ws.Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ws.Command(nil), f.sent...)
}

func setupAgentTest(t *testing.T) (*Handler, *InMemoryAgentStore, *org.InMemoryOrgStore, *auth.InMemoryUserStore, []byte) {
	t.Helper()
	return setupAgentTestWithCommander(t, &fakeCommander{connected: true})
}

// setupAgentTestWithCommander wires a Handler with an explicit commander for
// endpoints that talk to the agent hub (e.g. archive status).
func setupAgentTestWithCommander(t *testing.T, commander AgentCommander) (*Handler, *InMemoryAgentStore, *org.InMemoryOrgStore, *auth.InMemoryUserStore, []byte) {
	t.Helper()
	agentStore := NewInMemoryAgentStore()
	orgStore := org.NewInMemoryOrgStore()
	userStore := auth.NewInMemoryUserStore()
	secret := []byte("agent-test-secret")
	handler := NewHandler(agentStore, orgStore, secret, commander)
	return handler, agentStore, orgStore, userStore, secret
}

var testUserCounter int64

func createTestUser(t *testing.T, store *auth.InMemoryUserStore) string {
	t.Helper()
	testUserCounter++
	user := &auth.User{
		Email:          fmt.Sprintf("%s-%d-%d@example.com", t.Name(), testUserCounter, time.Now().UnixNano()),
		HashedPassword: "hash",
	}
	err := store.Create(user)
	require.NoError(t, err)
	return user.ID
}

func createTestOrg(t *testing.T, store *org.InMemoryOrgStore, adminID string) *org.Organization {
	t.Helper()
	o := &org.Organization{Name: "Test Org"}
	err := store.Create(o)
	require.NoError(t, err)
	err = store.AddMember(o.ID, adminID, "admin")
	require.NoError(t, err)
	return o
}

func authenticatedRequest(t *testing.T, method, target string, body interface{}, userID string, _ []byte) *http.Request {
	t.Helper()
	req := newRequest(method, target, body)
	claims := &auth.Claims{UserID: userID, Email: "admin@example.com"}
	req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
	return req
}

func newRequest(method, target string, body interface{}) *http.Request {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	return httptest.NewRequest(method, target, &buf)
}

// ---------- Register ----------

func TestRegisterAgent_Success(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	req := authenticatedRequest(t, http.MethodPost, "/api/agents/register",
		map[string]interface{}{
			"orgId":    o.ID,
			"hostname": "db-server-1.example.com",
		}, adminID, secret)
	w := httptest.NewRecorder()
	h.Register(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp registerAgentResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "db-server-1.example.com", resp.Agent.Hostname)
	assert.Equal(t, o.ID, resp.Agent.OrgID)
	assert.Equal(t, "offline", resp.Agent.Status)
	assert.False(t, resp.Agent.Approved)
	assert.NotEmpty(t, resp.RegistrationToken)
}

func TestRegisterAgent_MissingHostname(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	req := authenticatedRequest(t, http.MethodPost, "/api/agents/register",
		map[string]interface{}{"orgId": o.ID}, adminID, secret)
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRegisterAgent_MissingOrgID(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	createTestOrg(t, orgStore, adminID)

	req := authenticatedRequest(t, http.MethodPost, "/api/agents/register",
		map[string]interface{}{"hostname": "db-1"}, adminID, secret)
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRegisterAgent_NonMember(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	nonMemberID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	req := authenticatedRequest(t, http.MethodPost, "/api/agents/register",
		map[string]interface{}{
			"orgId": o.ID, "hostname": "db-1",
		}, nonMemberID, secret)
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ---------- List ----------

func TestListAgents_Success(t *testing.T) {
	h, agentStore, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	// Register two agents directly.
	a1 := &AgentRecord{OrgID: o.ID, Hostname: "db-1", Status: "online"}
	a2 := &AgentRecord{OrgID: o.ID, Hostname: "db-2", Status: "offline"}
	require.NoError(t, agentStore.Create(a1))
	require.NoError(t, agentStore.Create(a2))

	req := authenticatedRequest(t, http.MethodGet, "/api/agents?orgId="+o.ID,
		nil, adminID, secret)
	w := httptest.NewRecorder()
	h.List(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp listAgentsResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Agents, 2)
}

func TestListAgents_NonMember(t *testing.T) {
	h, agentStore, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	otherID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	a1 := &AgentRecord{OrgID: o.ID, Hostname: "db-1"}
	require.NoError(t, agentStore.Create(a1))

	req := authenticatedRequest(t, http.MethodGet, "/api/agents?orgId="+o.ID,
		nil, otherID, secret)
	w := httptest.NewRecorder()
	h.List(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ---------- Approve ----------

func TestApproveAgent_Success(t *testing.T) {
	h, agentStore, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	agent := &AgentRecord{OrgID: o.ID, Hostname: "db-1", Approved: false}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodPost,
		"/api/agents/"+agent.ID+"/approve", nil, adminID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.Approve(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp approveAgentResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Agent.Approved)
	assert.NotEmpty(t, resp.Agent.CertSerial)

	// Verify the store is updated.
	updated, err := agentStore.Get(agent.ID)
	require.NoError(t, err)
	assert.True(t, updated.Approved)
}

func TestApproveAgent_NonAdmin(t *testing.T) {
	h, agentStore, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	memberID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	// Add memberID as a regular member.
	require.NoError(t, orgStore.AddMember(o.ID, memberID, "member"))

	agent := &AgentRecord{OrgID: o.ID, Hostname: "db-1", Approved: false}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodPost,
		"/api/agents/"+agent.ID+"/approve", nil, memberID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.Approve(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestApproveAgent_NotFound(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	createTestOrg(t, orgStore, adminID)

	req := authenticatedRequest(t, http.MethodPost,
		"/api/agents/nonexistent/approve", nil, adminID, secret)
	req = withChiURLParam(req, "id", "nonexistent")
	w := httptest.NewRecorder()
	h.Approve(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// ---------- Get ----------

func TestGetAgent_Success(t *testing.T) {
	h, agentStore, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	agent := &AgentRecord{
		OrgID: o.ID, Hostname: "db-1", Status: "online",
		MySQLVersion: "8.0.35",
	}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodGet,
		"/api/agents/"+agent.ID, nil, adminID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.Get(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp AgentRecord
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, agent.ID, resp.ID)
	assert.Equal(t, "db-1", resp.Hostname)
	assert.Equal(t, "8.0.35", resp.MySQLVersion)
}

func TestGetAgent_NonMember(t *testing.T) {
	h, agentStore, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	otherID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)

	agent := &AgentRecord{OrgID: o.ID, Hostname: "db-1"}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodGet,
		"/api/agents/"+agent.ID, nil, otherID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestGetAgent_NotFound(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAgentTest(t)
	adminID := createTestUser(t, userStore)
	createTestOrg(t, orgStore, adminID)

	req := authenticatedRequest(t, http.MethodGet,
		"/api/agents/nonexistent", nil, adminID, secret)
	req = withChiURLParam(req, "id", "nonexistent")
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// ---------- ArchiveStatus ----------

func TestArchiveStatus_Online(t *testing.T) {
	// The agent's archive loop answers with a collector.State (delivered by the
	// hub as a decoded map with the agent's snake_case keys); the endpoint maps
	// it to the camelCase archive JSON contract.
	fake := &fakeCommander{connected: true, result: map[string]interface{}{
		"last_file":  "mysql-bin.000008",
		"last_pos":   float64(1024),
		"last_gtid":  "c84e7f4a-3d1c-11ee-ba6b-00155d000001",
		"updated_at": "2026-07-08T12:00:00Z",
	}}
	h, agentStore, orgStore, userStore, secret := setupAgentTestWithCommander(t, fake)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)
	agent := &AgentRecord{OrgID: o.ID, Hostname: "db-1"}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodGet,
		"/api/agents/"+agent.ID+"/archive", nil, adminID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.ArchiveStatus(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cmds := fake.commands()
	require.Len(t, cmds, 1)
	assert.Equal(t, ws.CmdArchiveStatus, cmds[0].Type)
	assert.Equal(t, agent.ID, cmds[0].Cmd, "archive status is correlated by agent id")

	var resp struct {
		LastFile  string    `json:"lastFile"`
		LastPos   uint32    `json:"lastPos"`
		LastGTID  string    `json:"lastGtid"`
		UpdatedAt time.Time `json:"updatedAt"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "mysql-bin.000008", resp.LastFile)
	assert.Equal(t, uint32(1024), resp.LastPos)
	assert.Equal(t, "c84e7f4a-3d1c-11ee-ba6b-00155d000001", resp.LastGTID)
	assert.Equal(t, "2026-07-08T12:00:00Z", resp.UpdatedAt.Format(time.RFC3339))
}

func TestArchiveStatus_Offline(t *testing.T) {
	// An offline agent cannot answer archive status: 503, no command sent.
	fake := &fakeCommander{connected: false}
	h, agentStore, orgStore, userStore, secret := setupAgentTestWithCommander(t, fake)
	adminID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)
	agent := &AgentRecord{OrgID: o.ID, Hostname: "db-1"}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodGet,
		"/api/agents/"+agent.ID+"/archive", nil, adminID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.ArchiveStatus(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Empty(t, fake.commands(), "no archive command to an offline agent")
}

func TestArchiveStatus_NotMember(t *testing.T) {
	fake := &fakeCommander{connected: true}
	h, agentStore, orgStore, userStore, secret := setupAgentTestWithCommander(t, fake)
	adminID := createTestUser(t, userStore)
	otherID := createTestUser(t, userStore)
	o := createTestOrg(t, orgStore, adminID)
	agent := &AgentRecord{OrgID: o.ID, Hostname: "db-1"}
	require.NoError(t, agentStore.Create(agent))

	req := authenticatedRequest(t, http.MethodGet,
		"/api/agents/"+agent.ID+"/archive", nil, otherID, secret)
	req = withChiURLParam(req, "id", agent.ID)
	w := httptest.NewRecorder()
	h.ArchiveStatus(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, fake.commands(), "no archive command for a non-member")
}
