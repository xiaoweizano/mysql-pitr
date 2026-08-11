package pitr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-shan/mysql-pitr/internal/server/agent"
	"github.com/a-shan/mysql-pitr/internal/server/audit"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/server/store"
	"github.com/a-shan/mysql-pitr/internal/ws"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCommander implements AgentCommander for the v3 flow: it records every
// command issued via SendToAgent so tests can assert the wire contract
// (command type, command ID = operation ID, params), and lets tests force
// connected/offline and transport errors.
type fakeCommander struct {
	mu        sync.Mutex
	connected bool
	sendErr   error
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
	return &ws.Response{
		Cmd:    cmd.Cmd,
		Status: ws.StatusOK,
		Result: map[string]interface{}{"accepted": true, "operationId": cmd.Cmd},
	}, nil
}

func (f *fakeCommander) commands() []ws.Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ws.Command(nil), f.sent...)
}

func (f *fakeCommander) lastCommand() ws.Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sent) == 0 {
		return ws.Command{}
	}
	return f.sent[len(f.sent)-1]
}

func (f *fakeCommander) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = nil
}

func (f *fakeCommander) findCommand(typ string) (ws.Command, bool) {
	for _, c := range f.commands() {
		if c.Type == typ {
			return c, true
		}
	}
	return ws.Command{}, false
}

// testFixture wires a Handler against the SQLite store, the InMemory domain
// stores, the event bus, and a controllable fake commander.
type testFixture struct {
	handler    *Handler
	opStore    OperationStore
	agentStore *agent.InMemoryAgentStore
	orgStore   *org.InMemoryOrgStore
	auditStore *audit.InMemoryAuditStore
	userStore  *auth.InMemoryUserStore
	commander  *fakeCommander
	bus        *eventBus
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
	bus := NewEventBus()
	commander := &fakeCommander{connected: true}
	handler := NewHandler(opStore, agentStore, orgStore, auditStore, bus, commander, secret)
	return &testFixture{
		handler:    handler,
		opStore:    opStore,
		agentStore: agentStore,
		orgStore:   orgStore,
		auditStore: auditStore,
		userStore:  userStore,
		commander:  commander,
		bus:        bus,
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
	require.NoError(t, f.userStore.Create(user))
	return user.ID
}

func (f *testFixture) createOrg(t *testing.T, userID string) string {
	t.Helper()
	org := &org.Organization{Name: "Test Org"}
	require.NoError(t, f.orgStore.Create(org))
	require.NoError(t, f.orgStore.AddMember(org.ID, userID, "admin"))
	return org.ID
}

func (f *testFixture) createAgent(t *testing.T, orgID string) string {
	t.Helper()
	agt := &agent.AgentRecord{OrgID: orgID, Hostname: "db-01.example.com", Approved: true}
	require.NoError(t, f.agentStore.Create(agt))
	return agt.ID
}

func (f *testFixture) authenticatedRequest(t *testing.T, method, target string, body interface{}, userID string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(method, target, &buf)
	claims := &auth.Claims{UserID: userID, Email: "test@example.com"}
	return req.WithContext(auth.ContextWithClaims(req.Context(), claims))
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

// createOp persists an operation directly in the given state (no agent scan).
func (f *testFixture) createOp(t *testing.T, id, orgID, agentID string, status OperationState) *Operation {
	t.Helper()
	op := &Operation{ID: id, OrgID: orgID, AgentID: agentID, Type: "pitr", Mode: "sql", Status: status}
	require.NoError(t, f.opStore.Create(op))
	return op
}

// startBody builds a valid v3 start request. `type` is omitted so tests cover
// the default.
func startBody(agentID string) map[string]interface{} {
	return map[string]interface{}{
		"agentId": agentID,
		"mode":    "sql",
		"filter": map[string]interface{}{
			"tables":       []map[string]string{{"schema": "shop", "table": "orders"}},
			"timeStart":    "2026-07-08T00:00:00Z",
			"timeEnd":      "2026-07-08T23:59:59Z",
			"maxRowsPerTx": 500,
		},
		"maxPreview": 50,
	}
}

// decodeParams round-trips a recorded command's params into a wire struct,
// mirroring the agent's decodeParams.
func decodeParams(t *testing.T, cmd ws.Command, v interface{}) {
	t.Helper()
	data, err := json.Marshal(cmd.Params)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, v))
}

func txMetaData(txID, commitTime string, rowCount int) map[string]interface{} {
	return map[string]interface{}{
		"txId":       txID,
		"gtid":       txID,
		"commitTime": commitTime,
		"schema":     "shop",
		"tables":     []map[string]string{{"schema": "shop", "table": "orders"}},
		"rowCount":   rowCount,
	}
}

// injectStreamEvent feeds an agent→server stream event through the handler the
// same way the hub read pump would: the command is JSON-marshalled and decoded
// back, so params["data"] arrives as a decoded map/slice rather than raw JSON.
func (f *testFixture) injectStreamEvent(t *testing.T, agentID, opID, kind string, data interface{}) {
	t.Helper()
	wire := ws.Command{
		Cmd:  "ev-" + opID,
		Type: ws.CmdStreamEvent,
		Params: map[string]interface{}{
			"id":   opID,
			"kind": kind,
			"data": data,
		},
	}
	raw, err := json.Marshal(wire)
	require.NoError(t, err)
	var decoded ws.Command
	require.NoError(t, json.Unmarshal(raw, &decoded))
	f.handler.HandleStreamEvent(agentID, decoded)
}

func startResponseBody(t *testing.T, w *httptest.ResponseRecorder) startResponse {
	t.Helper()
	var resp startResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	return resp
}

// ---------- Start ----------

func TestStart_Success_SendsScanCommand(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	resp := startResponseBody(t, w)
	require.NotEmpty(t, resp.OperationID)
	assert.Equal(t, StateScanning, resp.Status)

	// The operation record is persisted with the mapped filter.
	op, err := f.opStore.Get(resp.OperationID)
	require.NoError(t, err)
	assert.Equal(t, orgID, op.OrgID)
	assert.Equal(t, agentID, op.AgentID)
	assert.Equal(t, "pitr", op.Type)
	assert.Equal(t, "sql", op.Mode)
	assert.Equal(t, userID, op.CreatedBy)
	require.Len(t, op.Filter.Tables, 1)
	assert.Equal(t, "shop", op.Filter.Tables[0].Schema)
	assert.Equal(t, "orders", op.Filter.Tables[0].Table)
	require.NotNil(t, op.Filter.TimeRange)
	assert.Equal(t, 500, op.Filter.MaxRowsPerTx)

	// Exactly one CmdScan issued; command ID == operation ID (the cancel
	// contract: the agent keys its op registry by the command ID).
	cmds := f.commander.commands()
	require.Len(t, cmds, 1)
	scan := cmds[0]
	assert.Equal(t, ws.CmdScan, scan.Type)
	assert.Equal(t, resp.OperationID, scan.Cmd)
	var scanReq ws.ScanRequest
	decodeParams(t, scan, &scanReq)
	assert.Equal(t, "sql", scanReq.Mode)
	assert.Equal(t, 50, scanReq.MaxPreview)
	require.Len(t, scanReq.Filter.Tables, 1)
	assert.Equal(t, "shop", scanReq.Filter.Tables[0].Schema)
	assert.Equal(t, "orders", scanReq.Filter.Tables[0].Table)
	assert.Equal(t, "2026-07-08T00:00:00Z", scanReq.Filter.TimeStart)
	assert.Equal(t, "2026-07-08T23:59:59Z", scanReq.Filter.TimeEnd)
	assert.Equal(t, 500, scanReq.Filter.MaxRowsPerTx)
}

func TestStart_ScanDoneTransitionsToReady(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	resp := startResponseBody(t, w)

	f.injectStreamEvent(t, agentID, resp.OperationID, ws.EvScanDone, map[string]interface{}{})

	op, err := f.opStore.Get(resp.OperationID)
	require.NoError(t, err)
	assert.Equal(t, StateReady, op.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "ready", entries[0].Status)
}

func TestStart_MissingAgentID(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)

	body := startBody("")
	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", body, userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStart_InvalidMode(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	for _, mode := range []string{"preview", "execute", "invalid"} {
		body := startBody(agentID)
		body["mode"] = mode
		req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", body, userID)
		w := httptest.NewRecorder()
		f.handler.Start(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code, "mode %q should be rejected", mode)
	}
}

func TestStart_InvalidFilterTime(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	body := startBody(agentID)
	filter := body["filter"].(map[string]interface{})
	filter["timeStart"] = "not-a-time"
	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", body, userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStart_NoAuth(t *testing.T) {
	f := setupTest(t)
	req := httptest.NewRequest(http.MethodPost, "/api/pitr/start",
		strings.NewReader(`{"agentId":"x","mode":"sql"}`))
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
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
	orgID := f.createOrg(t, otherUser)
	agentID := f.createAgent(t, orgID)

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestStart_AgentNotApproved(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agt := &agent.AgentRecord{OrgID: orgID, Hostname: "db-01.example.com", Approved: false}
	require.NoError(t, f.agentStore.Create(agt))

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agt.ID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestStart_AgentOffline_Blocked(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.commander.connected = false

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
	require.Empty(t, f.commander.commands(), "no scan command should be sent to an offline agent")

	// The operation record exists but is blocked.
	ops, err := f.opStore.ListByOrg(orgID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, StateBlocked, ops[0].Status)
}

func TestStart_SendToAgentError_Failed(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.commander.sendErr = fmt.Errorf("hub: agent disconnected while awaiting response")

	req := f.authenticatedRequest(t, http.MethodPost, "/api/pitr/start", startBody(agentID), userID)
	w := httptest.NewRecorder()
	f.handler.Start(w, req)
	assert.Equal(t, http.StatusBadGateway, w.Code)

	ops, err := f.opStore.ListByOrg(orgID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, StateFailed, ops[0].Status)
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
	f.createOp(t, "op_status", orgID, agentID, StateScanning)

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
	f.createOp(t, "op_member", orgID, agentID, StateCreated)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_member/status", "op_member", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Status(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ---------- Transactions ----------

func TestTransactions_ReturnsPreview(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_tx", orgID, agentID, StateScanning)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:1", "2026-07-08T12:00:00Z", 2))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:2", "2026-07-08T12:01:00Z", 1))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvSQL, []ws.StatementWire{
		{SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "aaa:1", TxOrder: 0},
	})

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_tx/transactions", "op_tx", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Transactions(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		OperationID  string       `json:"operationId"`
		Status       OperationState `json:"status"`
		Transactions []txPreview  `json:"transactions"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, op.ID, resp.OperationID)
	assert.Equal(t, StateScanning, resp.Status)
	require.Len(t, resp.Transactions, 2)
	assert.Equal(t, "aaa:1", resp.Transactions[0].TxID)
	assert.Equal(t, 2, resp.Transactions[0].RowCount)
	assert.Equal(t, "aaa:2", resp.Transactions[1].TxID)
	require.Len(t, resp.Transactions[0].SQL, 1)
	assert.Equal(t, "DELETE FROM shop.orders WHERE id=1", resp.Transactions[0].SQL[0].SQL)
	assert.Empty(t, resp.Transactions[1].SQL)

	// The tx_meta wire uses lowercase table keys (TableRef json tags): the
	// preview must carry the tables field, not drop it.
	require.Len(t, resp.Transactions[0].Tables, 1)
	assert.Equal(t, "shop", resp.Transactions[0].Tables[0].Schema)
	assert.Equal(t, "orders", resp.Transactions[0].Tables[0].Table)
}

func TestTransactions_EmptyPreview(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_empty", orgID, agentID, StateScanning)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_empty/transactions", "op_empty", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Transactions(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Transactions []txPreview `json:"transactions"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Empty(t, resp.Transactions)
}

func TestTransactions_NotFound(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	f.createOrg(t, userID)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/nonexistent/transactions", "nonexistent", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Transactions(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestTransactions_NotMember(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	otherUser := f.createUser(t)
	orgID := f.createOrg(t, otherUser)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_member", orgID, agentID, StateScanning)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_member/transactions", "op_member", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Transactions(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ---------- stream event handling ----------

func TestStreamEvent_WrongAgentIgnored(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_scan", orgID, agentID, StateScanning)

	f.injectStreamEvent(t, "some-other-agent", op.ID, ws.EvScanDone, map[string]interface{}{})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateScanning, got.Status, "event from a different agent must be ignored")
}

func TestStreamEvent_UnknownOperationIgnored(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	f.injectStreamEvent(t, agentID, "op_ghost", ws.EvScanDone, map[string]interface{}{})

	_, err := f.opStore.Get("op_ghost")
	require.Error(t, err)
}

func TestStreamEvent_ScanDone_Ready(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_scan", orgID, agentID, StateScanning)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvScanDone, map[string]interface{}{})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateReady, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "ready", entries[0].Status)
}

func TestStreamEvent_Progress_Published(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_prog", orgID, agentID, StateExecuting)

	ch := f.bus.Subscribe(op.ID)
	defer func() {
		f.bus.Unsubscribe(op.ID, ch)
		close(ch)
	}()

	progress := map[string]interface{}{"done": 1, "total": 2}
	f.injectStreamEvent(t, agentID, op.ID, ws.EvProgress, progress)

	select {
	case ev := <-ch:
		assert.Equal(t, ws.EvProgress, ev.Kind)
		assert.Equal(t, op.ID, ev.ID)
	case <-time.After(time.Second):
		t.Fatal("progress event was not published to the bus")
	}

	// Progress does not change the operation state; the checkpoint double-write
	// is tested in TestStreamEvent_Progress_PersistsCheckpoint.
	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status)
}

func TestStreamEvent_Progress_PersistsCheckpoint(t *testing.T) {
	// Server-side checkpoint double-write: progress events upsert the
	// checkpoints table so an operation can be resumed even after the agent
	// (or the server) restarts.
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_cp", orgID, agentID, StateExecuting)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvProgress, map[string]interface{}{
		"done": 20, "total": 100,
		"errors": []map[string]interface{}{
			{"statement": 3, "sql": "DELETE FROM shop.orders WHERE id=3", "err": "duplicate key"},
		},
	})

	sqlStore := f.opStore.(*SQLiteOperationStore)
	var lastStmt, total int
	var errs string
	require.NoError(t, sqlStore.db.QueryRow(
		"SELECT last_statement, total, errors FROM checkpoints WHERE op_id = ?", op.ID).
		Scan(&lastStmt, &total, &errs))
	assert.Equal(t, 20, lastStmt)
	assert.Equal(t, 100, total)
	assert.Contains(t, errs, "duplicate key")
	assert.Contains(t, errs, `"statement":3`)

	// A later progress event overwrites the previous checkpoint row.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvProgress, map[string]interface{}{"done": 40, "total": 100})
	require.NoError(t, sqlStore.db.QueryRow(
		"SELECT last_statement, errors FROM checkpoints WHERE op_id = ?", op.ID).
		Scan(&lastStmt, &errs))
	assert.Equal(t, 40, lastStmt)
	var n int
	require.NoError(t, sqlStore.db.QueryRow("SELECT count(*) FROM checkpoints WHERE op_id = ?", op.ID).Scan(&n))
	assert.Equal(t, 1, n, "checkpoint rows are upserted, never duplicated")
}

func TestStreamEvent_OpDone_WithErrors_AuditDetailAndSSE(t *testing.T) {
	// op_done with a non-empty errors list: the done audit records the error
	// summary, and the SSE op_done payload passes the errors through so the
	// front-end can surface per-statement failures.
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_errs", orgID, agentID, StateExecuting)

	ch := f.bus.Subscribe(op.ID)
	defer func() {
		f.bus.Unsubscribe(op.ID, ch)
		close(ch)
	}()

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{
		"done": 2, "total": 2, "paused": false,
		"errors": []map[string]interface{}{
			{"statement": 1, "sql": "DELETE FROM shop.orders WHERE id=1", "err": "duplicate key"},
		},
	})

	// Operation completes (done) despite statement errors.
	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDone, got.Status)

	// Audit records the error summary.
	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "done", entries[0].Status)
	assert.Contains(t, entries[0].ErrorDetails, "duplicate key")

	// The SSE op_done payload carries the errors verbatim for the front-end.
	select {
	case ev := <-ch:
		assert.Equal(t, ws.EvOpDone, ev.Kind)
		var payload opDonePayload
		require.NoError(t, json.Unmarshal(ev.Data, &payload))
		require.Len(t, payload.Errors, 1)
		assert.Equal(t, 1, payload.Errors[0].Statement)
		assert.Equal(t, "duplicate key", payload.Errors[0].Err)
	case <-time.After(time.Second):
		t.Fatal("op_done was not published to the bus")
	}
}

func TestStreamEvent_OpDone_Done(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_done", orgID, agentID, StateExecuting)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{"done": 2, "total": 2, "paused": false})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDone, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "done", entries[0].Status)
}

func TestStreamEvent_OpError_Failed(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_err", orgID, agentID, StateExecuting)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpError, "mysql connection lost")

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateFailed, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "failed", entries[0].Status)
	assert.Equal(t, "mysql connection lost", entries[0].ErrorDetails)
}

func TestOpDone_ArrivesBeforeExecutePersisted(t *testing.T) {
	// Phase 3 race root cause: the execute handler sends CmdExecute and the
	// agent finishes before the ready -> executing transition is persisted.
	// op_done must treat the ready op as executing (idempotent fallback: the
	// data was restored, so the op must land in done), exactly one audit.
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_race", orgID, agentID, StateReady)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone,
		map[string]interface{}{"done": 2, "total": 2, "paused": false})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDone, got.Status, "op_done to a ready op must fall back to executing then done")

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one done audit")
	assert.Equal(t, "done", entries[0].Status)
}

func TestOpDone_OnCancelled_Unchanged(t *testing.T) {
	// A late op_done for an already-cancelled op leaves it cancelled with no
	// audit — the transition is rejected under CAS just as before (T6 case).
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_cancelled", orgID, agentID, StateCancelled)

	f.injectStreamEvent(t, agentID, "op_cancelled", ws.EvOpDone,
		map[string]interface{}{"done": 1, "total": 1, "paused": false})

	got, err := f.opStore.Get("op_cancelled")
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status, "a terminal operation must not move on a late op_done")

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no audit entry for a rejected late op_done")
}

func TestStreamEvent_OpDone_InvalidTransitionIgnored(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_term", orgID, agentID, StateCancelled)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status, "a terminal operation must not move on a late event")

	// A rejected transition must not write an audit either.
	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no audit entry for a rejected late op_done")
}

func TestStreamEvent_OpDone_Paused_Confirmation(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_paused", orgID, agentID, StatePaused)

	// The agent's executor answers the pause cancel with a FinalReport carrying
	// paused=true: this is the pause confirmation, not a terminal completion.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{"done": 1, "total": 2, "paused": true})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, got.Status, "a paused op must stay paused (resumable), not done")

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "paused", entries[0].Status, "audit records the pause confirmation")
	assert.Equal(t, "agent", entries[0].Operator)
}

func TestStreamEvent_OpDone_Paused_WhileExecuting_TransitionsToPaused(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_exec", orgID, agentID, StateExecuting)

	// Pause acknowledgement racing the HTTP pause handler: the valid
	// executing -> paused transition is applied instead of a bogus done.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{"done": 3, "total": 5, "paused": true})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "paused", entries[0].Status)
}

func TestStreamEvent_OpDone_Paused_OnCancelled_Ignored(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_term", orgID, agentID, StateCancelled)

	// A stale pause acknowledgement for an already-cancelled op is ignored.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{"done": 1, "total": 2, "paused": true})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no audit entry for a stale pause confirmation")
}

func TestStreamEvent_OpError_AfterCancel_NoAudit(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_cancelled", orgID, agentID, StateCancelled)

	// High-frequency path: the operator cancels a scanning operation and the
	// agent's scan goroutine exits with context.Canceled pushing op_error.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpError, "context canceled")

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status, "a terminal operation must not move to failed")

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no failed audit for a rejected late op_error")
}

func TestStreamEvent_ScanDone_AfterCancel_NoAudit(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_cancelled", orgID, agentID, StateCancelled)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvScanDone, map[string]interface{}{})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no ready audit for a rejected late scan_done")
}

// ---------- Select ----------

func TestSelect_SavesStatements(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sel", orgID, agentID, StateReady)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:1", "2026-07-08T12:00:00Z", 2))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:2", "2026-07-08T12:01:00Z", 1))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvSQL, []ws.StatementWire{
		{SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "aaa:1", TxOrder: 0},
		{SQL: "DELETE FROM shop.orders WHERE id=2", TxID: "aaa:1", TxOrder: 1},
		{SQL: "DELETE FROM shop.orders WHERE id=3", TxID: "aaa:2", TxOrder: 0},
	})

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_sel/select", "op_sel",
		map[string]interface{}{"txIds": []string{"aaa:1"}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		SelectedTxIDs []string `json:"selectedTxIds"`
		StatementCount int     `json:"statementCount"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, []string{"aaa:1"}, resp.SelectedTxIDs)
	assert.Equal(t, 2, resp.StatementCount)

	// Statements are persisted and reloadable.
	stmts, err := f.opStore.LoadStatements(op.ID)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.Equal(t, "DELETE FROM shop.orders WHERE id=1", stmts[0].SQL)
	assert.Equal(t, "aaa:1", stmts[0].TxID)
	assert.Equal(t, 0, stmts[0].TxIndex)
	assert.Equal(t, 0, stmts[0].StmtIndex)
	assert.Equal(t, "pending", stmts[0].Status)
	assert.Equal(t, 1, stmts[1].StmtIndex)

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"aaa:1"}, got.SelectedTxIDs)
}

func TestSelect_MultiTx_OrderedByRequest(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sel2", orgID, agentID, StateReady)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:1", "2026-07-08T12:00:00Z", 2))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:2", "2026-07-08T12:01:00Z", 1))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvSQL, []ws.StatementWire{
		{SQL: "SQL-1", TxID: "aaa:1", TxOrder: 0},
		{SQL: "SQL-2", TxID: "aaa:2", TxOrder: 0},
	})

	// Request order differs from scan order — statements follow the request.
	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_sel2/select", "op_sel2",
		map[string]interface{}{"txIds": []string{"aaa:2", "aaa:1"}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	stmts, err := f.opStore.LoadStatements(op.ID)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.Equal(t, "SQL-2", stmts[0].SQL)
	assert.Equal(t, 1, stmts[0].TxIndex)
	assert.Equal(t, "SQL-1", stmts[1].SQL)
	assert.Equal(t, 0, stmts[1].TxIndex)
}

func TestSelect_NotReady(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_scan", orgID, agentID, StateScanning)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_scan/select", "op_scan",
		map[string]interface{}{"txIds": []string{"aaa:1"}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestSelect_EmptyTxIDs(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_sel", orgID, agentID, StateReady)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_sel/select", "op_sel",
		map[string]interface{}{"txIds": []string{}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestSelect_UnknownTxID(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sel", orgID, agentID, StateReady)
	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:1", "2026-07-08T12:00:00Z", 2))

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_sel/select", "op_sel",
		map[string]interface{}{"txIds": []string{"aaa:9"}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestSelect_NoSQLPreview(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_meta", orgID, agentID, StateReady)
	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:1", "2026-07-08T12:00:00Z", 2))

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_meta/select", "op_meta",
		map[string]interface{}{"txIds": []string{"aaa:1"}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestSelect_NotMember(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	otherUser := f.createUser(t)
	orgID := f.createOrg(t, otherUser)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_sel", orgID, agentID, StateReady)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_sel/select", "op_sel",
		map[string]interface{}{"txIds": []string{"aaa:1"}}, userID)
	w := httptest.NewRecorder()
	f.handler.Select(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ---------- Execute ----------

func TestExecute_SendsCmdExecute(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_exec", orgID, agentID, StateReady)
	require.NoError(t, f.opStore.SaveStatements(op.ID, []Statement{
		{TxIndex: 0, StmtIndex: 0, SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "aaa:1", TxOrder: 0, Status: "pending"},
		{TxIndex: 0, StmtIndex: 1, SQL: "DELETE FROM shop.orders WHERE id=2", TxID: "aaa:1", TxOrder: 1, Status: "pending"},
	}))

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_exec/execute", "op_exec",
		map[string]interface{}{"batchSize": 10}, userID)
	w := httptest.NewRecorder()
	f.handler.Execute(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// CmdExecute with command ID == operation ID and the persisted statements.
	cmds := f.commander.commands()
	require.Len(t, cmds, 1)
	exec := cmds[0]
	assert.Equal(t, ws.CmdExecute, exec.Type)
	assert.Equal(t, op.ID, exec.Cmd)
	var execReq ws.ExecuteRequest
	decodeParams(t, exec, &execReq)
	assert.Equal(t, op.ID, execReq.OperationID)
	assert.Equal(t, 10, execReq.BatchSize)
	require.Len(t, execReq.Statements, 2)
	assert.Equal(t, "DELETE FROM shop.orders WHERE id=1", execReq.Statements[0].SQL)
	assert.Equal(t, "aaa:1", execReq.Statements[0].TxID)

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status)
}

func TestExecute_NotReady(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_scan", orgID, agentID, StateScanning)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_scan/execute", "op_scan", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Execute(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestExecute_NoStatements(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_exec", orgID, agentID, StateReady)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_exec/execute", "op_exec", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Execute(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
	require.Empty(t, f.commander.commands())
}

func TestExecute_SendError_StaysReady(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_exec", orgID, agentID, StateReady)
	require.NoError(t, f.opStore.SaveStatements(op.ID, []Statement{
		{TxIndex: 0, StmtIndex: 0, SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "aaa:1", TxOrder: 0, Status: "pending"},
	}))
	f.commander.sendErr = fmt.Errorf("hub: agent not connected")

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_exec/execute", "op_exec", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Execute(w, req)
	assert.Equal(t, http.StatusBadGateway, w.Code)

	// The state machine has no ready -> failed transition: the operation stays
	// ready and retryable after a transport failure.
	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateReady, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no audit entry for a rejected (unapplied) failure transition")
}

// ---------- Pause / Resume ----------

func TestPause_SendsCancel(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_pause", orgID, agentID, StateExecuting)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_pause/pause", "op_pause", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Pause(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	cmd, ok := f.commander.findCommand(ws.CmdCancel)
	require.True(t, ok, "pause must dispatch CmdCancel to the agent")
	assert.Equal(t, op.ID, cmd.Cmd)
	assert.Equal(t, op.ID, cmd.Params["operationId"], "cancel contract: operationId == command ID")

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, got.Status)
}

func TestPause_NotExecuting(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_ready", orgID, agentID, StateReady)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_ready/pause", "op_ready", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Pause(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestPause_SendError_StaysExecuting(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_pause", orgID, agentID, StateExecuting)
	f.commander.sendErr = fmt.Errorf("hub: agent disconnected while awaiting response")

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_pause/pause", "op_pause", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Pause(w, req)
	assert.Equal(t, http.StatusBadGateway, w.Code)

	// A pause transport failure must not fail the operation: CmdCancel never
	// reached the agent, which is still executing, so the op stays executing
	// (retryable) — the agent's later op_done must still be accepted.
	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	assert.Empty(t, entries, "no audit entry for a pause transport failure")

	// The failure is retryable: once the transport recovers, the same pause
	// request pauses the operation normally.
	f.commander.sendErr = nil
	req = f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_pause/pause", "op_pause", nil, userID)
	w = httptest.NewRecorder()
	f.handler.Pause(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	got, err = f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, got.Status)
}

func TestResume_ResendsExecute(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_resume", orgID, agentID, StatePaused)
	require.NoError(t, f.opStore.SaveStatements(op.ID, []Statement{
		{TxIndex: 0, StmtIndex: 0, SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "aaa:1", TxOrder: 0, Status: "pending"},
	}))

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_resume/resume", "op_resume", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Resume(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	exec, ok := f.commander.findCommand(ws.CmdExecute)
	require.True(t, ok, "resume must re-issue CmdExecute")
	assert.Equal(t, op.ID, exec.Cmd)
	var execReq ws.ExecuteRequest
	decodeParams(t, exec, &execReq)
	assert.Equal(t, op.ID, execReq.OperationID)
	require.Len(t, execReq.Statements, 1)

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status)
}

func TestResume_NotPaused(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_ready", orgID, agentID, StateReady)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_ready/resume", "op_ready", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Resume(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

// ---------- Cancel ----------

func TestCancel_SendsCancel(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_cancel", orgID, agentID, StateReady)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_cancel/cancel", "op_cancel", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Cancel(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	cmd, ok := f.commander.findCommand(ws.CmdCancel)
	require.True(t, ok, "cancel must dispatch CmdCancel to the agent")
	assert.Equal(t, op.ID, cmd.Cmd)
	assert.Equal(t, op.ID, cmd.Params["operationId"], "cancel contract: operationId == command ID")

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "cancelled", entries[0].Status)
}

func TestCancel_CancelsPaused(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_paused", orgID, agentID, StatePaused)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_paused/cancel", "op_paused", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Cancel(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	got, err := f.opStore.Get("op_paused")
	require.NoError(t, err)
	assert.Equal(t, StateCancelled, got.Status)
}

func TestCancel_InvalidState(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_terminal", orgID, agentID, StateDone)

	req := f.authenticatedRouteRequest(t, http.MethodPost, "/api/pitr/op_terminal/cancel", "op_terminal", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Cancel(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
	require.Empty(t, f.commander.commands())

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

// ---------- SSE ----------

// sseRecorder is a thread-safe http.ResponseWriter for SSE handler tests: the
// handler writes frames from its own goroutine while the test reads the body.
type sseRecorder struct {
	mu        sync.Mutex
	header    http.Header
	status    int
	body      bytes.Buffer
	flushCh   chan struct{}
	flushOnce sync.Once
}

func newSSERecorder() *sseRecorder {
	return &sseRecorder{header: make(http.Header), status: http.StatusOK, flushCh: make(chan struct{})}
}

func (s *sseRecorder) Header() http.Header { return s.header }

func (s *sseRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.Write(p)
}

func (s *sseRecorder) WriteHeader(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
}

func (s *sseRecorder) Flush() { s.flushOnce.Do(func() { close(s.flushCh) }) }

func (s *sseRecorder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.String()
}

// waitFlush blocks until the handler flushes its headers after subscribing, so
// tests can inject events without racing the subscription.
func (s *sseRecorder) waitFlush(t *testing.T) {
	t.Helper()
	select {
	case <-s.flushCh:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not flush after subscribing")
	}
}

// sseFrames splits an SSE body into its frames keyed by field name.
func sseFrames(body string) []map[string]string {
	var frames []map[string]string
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		fr := map[string]string{}
		for _, line := range strings.Split(block, "\n") {
			if i := strings.Index(line, ":"); i >= 0 {
				fr[strings.TrimSpace(line[:i])] = strings.TrimSpace(line[i+1:])
			}
		}
		frames = append(frames, fr)
	}
	return frames
}

func (f *testFixture) runEvents(t *testing.T, opID, userID string, cancellable bool) (*sseRecorder, <-chan struct{}, context.CancelFunc) {
	t.Helper()
	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/"+opID+"/events", opID, nil, userID)
	var cancel context.CancelFunc
	if cancellable {
		var ctx context.Context
		ctx, cancel = context.WithCancel(req.Context())
		req = req.WithContext(ctx)
	} else {
		cancel = func() {}
	}
	rec := newSSERecorder()
	done := make(chan struct{})
	go func() {
		f.handler.Events(rec, req)
		close(done)
	}()
	rec.waitFlush(t)
	return rec, done, cancel
}

func TestEvents_StreamsFrames(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sse", orgID, agentID, StateScanning)

	rec, _, cancel := f.runEvents(t, op.ID, userID, false)
	defer cancel()

	f.injectStreamEvent(t, agentID, op.ID, ws.EvTxMeta, txMetaData("aaa:1", "2026-07-08T12:00:00Z", 2))
	f.injectStreamEvent(t, agentID, op.ID, ws.EvScanDone, map[string]interface{}{})

	require.Eventually(t, func() bool { return strings.Count(rec.String(), "event:") == 2 }, 2*time.Second, 10*time.Millisecond)

	frames := sseFrames(rec.String())
	require.Len(t, frames, 2)
	assert.Equal(t, ws.EvTxMeta, frames[0]["event"])
	var meta txMetaWire
	require.NoError(t, json.Unmarshal([]byte(frames[0]["data"]), &meta))
	assert.Equal(t, "aaa:1", meta.TxID)
	assert.Equal(t, 2, meta.RowCount)
	assert.Equal(t, ws.EvScanDone, frames[1]["event"])
}

func TestEvents_ClosesOnTerminal(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sse", orgID, agentID, StateExecuting)

	rec, done, cancel := f.runEvents(t, op.ID, userID, false)
	defer cancel()

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{"done": 2, "total": 2})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not terminate after a terminal event")
	}
	frames := sseFrames(rec.String())
	require.Len(t, frames, 1)
	assert.Equal(t, ws.EvOpDone, frames[0]["event"])

	// The subscription was closed: no further events are delivered after the
	// terminal event even if the stream handler publishes.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvProgress, map[string]interface{}{"done": 2, "total": 2})
	require.Never(t, func() bool {
		return strings.Count(rec.String(), "event:") > 1
	}, 200*time.Millisecond, 20*time.Millisecond)
}

func TestEvents_StaysOpenOnPausedOpDone(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sse", orgID, agentID, StatePaused)

	rec, done, cancel := f.runEvents(t, op.ID, userID, true)
	defer cancel()

	// A pause confirmation (op_done with paused=true) is not a terminal event:
	// no op_done frame is emitted and the stream stays open.
	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone, map[string]interface{}{"done": 1, "total": 2, "paused": true})

	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 400*time.Millisecond, 25*time.Millisecond)
	assert.Empty(t, rec.String(), "no terminal frame for a pause confirmation")

	// The operation stays paused (resumable), audited as a pause confirmation.
	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, got.Status)
	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "paused", entries[0].Status)
}

func TestEvents_TerminalAtSubscribe(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_done", orgID, agentID, StateDone)

	rec, done, cancel := f.runEvents(t, op.ID, userID, false)
	defer cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not return for an already-terminal operation")
	}
	frames := sseFrames(rec.String())
	require.Len(t, frames, 1)
	assert.Equal(t, ws.EvOpDone, frames[0]["event"])
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(frames[0]["data"]), &payload))
	assert.Equal(t, "done", payload["status"])
}

func TestEvents_ClosesOnClientDisconnect(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_sse", orgID, agentID, StateScanning)

	rec, done, cancel := f.runEvents(t, op.ID, userID, true)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not terminate when the client disconnected")
	}
	_ = rec
}

func TestEvents_NotMember(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	otherUser := f.createUser(t)
	orgID := f.createOrg(t, otherUser)
	agentID := f.createAgent(t, orgID)
	f.createOp(t, "op_sse", orgID, agentID, StateScanning)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/op_sse/events", "op_sse", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Events(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestEvents_NotFound(t *testing.T) {
	f := setupTest(t)
	userID := f.createUser(t)
	f.createOrg(t, userID)

	req := f.authenticatedRouteRequest(t, http.MethodGet, "/api/pitr/nonexistent/events", "nonexistent", nil, userID)
	w := httptest.NewRecorder()
	f.handler.Events(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}
