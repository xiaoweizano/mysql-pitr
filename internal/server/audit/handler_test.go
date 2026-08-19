package audit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test helpers

func setupAuditTest(t *testing.T) (*Handler, *InMemoryAuditStore, *org.InMemoryOrgStore, *auth.InMemoryUserStore, []byte) {
	t.Helper()
	auditStore := NewInMemoryAuditStore()
	orgStore := org.NewInMemoryOrgStore()
	userStore := auth.NewInMemoryUserStore()
	secret := []byte("audit-test-secret")
	handler := NewHandler(auditStore, orgStore, secret)
	return handler, auditStore, orgStore, userStore, secret
}

// auditUserSeq makes fixture user emails unique even when two users are
// created within the same clock tick.
var auditUserSeq int64

func createTestUser(t *testing.T, store *auth.InMemoryUserStore) string {
	t.Helper()
	auditUserSeq++
	user := &auth.User{
		Email:          fmt.Sprintf("%s-%d-%d@example.com", t.Name(), time.Now().UnixNano(), auditUserSeq),
		HashedPassword: "hash",
	}
	err := store.Create(user)
	require.NoError(t, err)
	return user.ID
}

func authenticatedRequest(t *testing.T, target string, userID string, _ []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	claims := &auth.Claims{UserID: userID, Email: "test@example.com"}
	req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
	return req
}

func seedAuditEntries(t *testing.T, store *InMemoryAuditStore, orgID string) {
	t.Helper()
	now := time.Now()
	entries := []AuditEntry{
		{
			OperationID:  "op_001",
			Operator:     "admin@example.com",
			Timestamp:    now.Add(-2 * time.Hour),
			OrgID:        orgID,
			AgentID:      "agent_01",
			TargetTable:  "orders",
			RecoveryTime: now.Add(-24 * time.Hour),
			RowsAffected: 1250,
			Status:       "completed",
		},
		{
			OperationID:  "op_002",
			Operator:     "admin@example.com",
			Timestamp:    now.Add(-1 * time.Hour),
			OrgID:        orgID,
			AgentID:      "agent_01",
			TargetTable:  "users",
			RecoveryTime: now.Add(-12 * time.Hour),
			RowsAffected: 500,
			Status:       "completed",
		},
		{
			OperationID:  "op_003",
			Operator:     "dev@example.com",
			Timestamp:    now.Add(-30 * time.Minute),
			OrgID:        orgID,
			AgentID:      "agent_02",
			TargetTable:  "payments",
			RecoveryTime: now.Add(-6 * time.Hour),
			RowsAffected: 0,
			Status:       "failed",
			ErrorDetails: "binlog gap detected",
		},
		{
			OperationID:  "op_004",
			Operator:     "dev@example.com",
			Timestamp:    now.Add(-10 * time.Minute),
			OrgID:        orgID,
			AgentID:      "agent_02",
			TargetTable:  "refunds",
			RecoveryTime: now.Add(-3 * time.Hour),
			RowsAffected: 75,
			Status:       "cancelled",
		},
	}

	for _, e := range entries {
		err := store.Append(&e)
		require.NoError(t, err)
	}
}

// ---------- Query ----------

func TestQuery_Success(t *testing.T) {
	h, auditStore, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)

	seedAuditEntries(t, auditStore, org.ID)

	req := authenticatedRequest(t, "/api/audit?org_id="+org.ID, userID, secret)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var entries []AuditEntry
	err = json.NewDecoder(w.Body).Decode(&entries)
	require.NoError(t, err)
	assert.Len(t, entries, 4)
}

func TestQuery_FilterByStatus(t *testing.T) {
	h, auditStore, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)

	seedAuditEntries(t, auditStore, org.ID)

	req := authenticatedRequest(t, "/api/audit?org_id="+org.ID+"&status=failed", userID, secret)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var entries []AuditEntry
	err = json.NewDecoder(w.Body).Decode(&entries)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "failed", entries[0].Status)
}

func TestQuery_FilterByAgent(t *testing.T) {
	h, auditStore, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)

	seedAuditEntries(t, auditStore, org.ID)

	req := authenticatedRequest(t, "/api/audit?org_id="+org.ID+"&agent_id=agent_01", userID, secret)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var entries []AuditEntry
	err = json.NewDecoder(w.Body).Decode(&entries)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
	for _, e := range entries {
		assert.Equal(t, "agent_01", e.AgentID)
	}
}

func TestQuery_FilterByTimeRange(t *testing.T) {
	h, auditStore, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)

	seedAuditEntries(t, auditStore, org.ID)

	now := time.Now()
	from := url.QueryEscape(now.Add(-90 * time.Minute).Format(time.RFC3339))
	to := url.QueryEscape(now.Format(time.RFC3339))
	req := authenticatedRequest(t, "/api/audit?org_id="+org.ID+"&from="+from+"&to="+to, userID, secret)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var entries []AuditEntry
	err = json.NewDecoder(w.Body).Decode(&entries)
	require.NoError(t, err)
	assert.Len(t, entries, 3) // op_002 (-1h), op_003 (-30m), op_004 (-10m) fall inside [-90m, now]
	for _, e := range entries {
		assert.NotEqual(t, "op_001", e.OperationID) // -2h is outside the range
	}
}

func TestQuery_MissingOrgID(t *testing.T) {
	h, _, _, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	req := authenticatedRequest(t, "/api/audit", userID, secret)
	w := httptest.NewRecorder()
	h.Query(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestQuery_NotMember(t *testing.T) {
	h, _, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)
	otherUser := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, otherUser, "admin")
	require.NoError(t, err)

	req := authenticatedRequest(t, "/api/audit?org_id="+org.ID, userID, secret)
	w := httptest.NewRecorder()
	h.Query(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestQuery_NoAuth(t *testing.T) {
	h, _, _, _, _ := setupAuditTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/audit?org_id=test", nil)
	w := httptest.NewRecorder()
	h.Query(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// ---------- Export (CSV) ----------

func TestExport_Success(t *testing.T) {
	h, auditStore, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)

	seedAuditEntries(t, auditStore, org.ID)

	req := authenticatedRequest(t, "/api/audit/export?org_id="+org.ID, userID, secret)
	w := httptest.NewRecorder()
	h.Export(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/csv", w.Header().Get("Content-Type"))
	assert.Contains(t, w.Header().Get("Content-Disposition"), "attachment; filename=")
	assert.Contains(t, string(w.Body.Bytes()), "操作ID,操作人,时间")
	assert.Contains(t, string(w.Body.Bytes()), "op_001")
	assert.Contains(t, string(w.Body.Bytes()), "op_004")
}

func TestExport_NoAuth(t *testing.T) {
	h, _, _, _, _ := setupAuditTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/audit/export?org_id=test", nil)
	w := httptest.NewRecorder()
	h.Export(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestExport_MissingOrgID(t *testing.T) {
	h, _, _, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	req := authenticatedRequest(t, "/api/audit/export", userID, secret)
	w := httptest.NewRecorder()
	h.Export(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ---------- List (delegates to Query) ----------

func TestList_DelegatesToQuery(t *testing.T) {
	h, auditStore, orgStore, userStore, secret := setupAuditTest(t)
	userID := createTestUser(t, userStore)

	org := &org.Organization{Name: "Test Org"}
	err := orgStore.Create(org)
	require.NoError(t, err)
	err = orgStore.AddMember(org.ID, userID, "admin")
	require.NoError(t, err)

	seedAuditEntries(t, auditStore, org.ID)

	req := authenticatedRequest(t, "/api/audit?org_id="+org.ID, userID, secret)
	w := httptest.NewRecorder()
	h.List(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var entries []AuditEntry
	err = json.NewDecoder(w.Body).Decode(&entries)
	require.NoError(t, err)
	assert.Len(t, entries, 4)
}
