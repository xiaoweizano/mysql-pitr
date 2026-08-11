package agent

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/server/store"
)

func newTestAgentStore(t *testing.T) (*SQLiteAgentStore, *sql.DB) {
	t.Helper()
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLiteAgentStore(db), db
}

func newTestAgent() *AgentRecord {
	return &AgentRecord{
		OrgID:             "org_1",
		Hostname:          "host-1",
		MySQLVersion:      "8.0.36",
		Status:            "offline",
		CertSerial:        "",
		RegistrationToken: "tok-1",
		Approved:          false,
	}
}

// TestSQLiteAgentStore_CreateAndGet covers Create defaults (ID, CreatedAt,
// Status="offline"), full field round-trip and the empty-LastSeen case.
func TestSQLiteAgentStore_CreateAndGet(t *testing.T) {
	s, _ := newTestAgentStore(t)

	agent := &AgentRecord{OrgID: "org_1", Hostname: "host-1"}
	require.NoError(t, s.Create(agent))
	require.NotEmpty(t, agent.ID)
	require.False(t, agent.CreatedAt.IsZero())
	require.Equal(t, "offline", agent.Status)

	got, err := s.Get(agent.ID)
	require.NoError(t, err)
	require.Equal(t, agent.ID, got.ID)
	require.Equal(t, "org_1", got.OrgID)
	require.Equal(t, "host-1", got.Hostname)
	require.Equal(t, "offline", got.Status)
	require.True(t, got.LastSeen.IsZero())
	require.Equal(t, agent.CreatedAt.UTC().Format(time.RFC3339Nano),
		got.CreatedAt.UTC().Format(time.RFC3339Nano))
}

// TestSQLiteAgentStore_Create_PreservesAllFields round-trips every AgentRecord
// field, including optional ones and the approved flag.
func TestSQLiteAgentStore_Create_PreservesAllFields(t *testing.T) {
	s, _ := newTestAgentStore(t)

	createdAt := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	lastSeen := time.Date(2026, 8, 1, 9, 30, 0, 500000000, time.UTC)
	agent := &AgentRecord{
		ID:                "agent_fixed",
		OrgID:             "org_1",
		Hostname:          "db-prod-01",
		MySQLVersion:      "5.7.44",
		Status:            "online",
		LastSeen:          lastSeen,
		CreatedAt:         createdAt,
		CertSerial:        "serial_abc",
		RegistrationToken: "tok-xyz",
		Approved:          true,
	}
	require.NoError(t, s.Create(agent))

	got, err := s.Get("agent_fixed")
	require.NoError(t, err)
	require.Equal(t, "db-prod-01", got.Hostname)
	require.Equal(t, "5.7.44", got.MySQLVersion)
	require.Equal(t, "online", got.Status)
	require.Equal(t, "serial_abc", got.CertSerial)
	require.Equal(t, "tok-xyz", got.RegistrationToken)
	require.True(t, got.Approved)
	require.Equal(t, "2026-08-01T09:30:00.5Z",
		got.LastSeen.UTC().Format(time.RFC3339Nano))
	require.Equal(t, "2026-07-01T08:00:00Z",
		got.CreatedAt.UTC().Format(time.RFC3339Nano))
}

func TestSQLiteAgentStore_Get_NotFound(t *testing.T) {
	s, _ := newTestAgentStore(t)

	_, err := s.Get("agent_absent")
	require.EqualError(t, err, "agent not found: agent_absent")
}

// TestSQLiteAgentStore_ListByOrg verifies org filtering and the empty-slice
// contract.
func TestSQLiteAgentStore_ListByOrg(t *testing.T) {
	s, _ := newTestAgentStore(t)

	empty, err := s.ListByOrg("org_1")
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)

	require.NoError(t, s.Create(&AgentRecord{OrgID: "org_1", Hostname: "a"}))
	require.NoError(t, s.Create(&AgentRecord{OrgID: "org_1", Hostname: "b"}))
	require.NoError(t, s.Create(&AgentRecord{OrgID: "org_2", Hostname: "c"}))

	org1, err := s.ListByOrg("org_1")
	require.NoError(t, err)
	require.Len(t, org1, 2)
	org2, err := s.ListByOrg("org_2")
	require.NoError(t, err)
	require.Len(t, org2, 1)
	require.Equal(t, "c", org2[0].Hostname)
}

// TestSQLiteAgentStore_Update verifies replacement semantics and the
// not-found error.
func TestSQLiteAgentStore_Update(t *testing.T) {
	s, _ := newTestAgentStore(t)

	agent := newTestAgent()
	require.NoError(t, s.Create(agent))

	agent.Approved = true
	agent.CertSerial = "serial_123"
	agent.Status = "online"
	require.NoError(t, s.Update(agent))

	got, err := s.Get(agent.ID)
	require.NoError(t, err)
	require.True(t, got.Approved)
	require.Equal(t, "serial_123", got.CertSerial)
	require.Equal(t, "online", got.Status)
	require.Equal(t, "tok-1", got.RegistrationToken)

	missing := &AgentRecord{ID: "agent_absent"}
	err = s.Update(missing)
	require.EqualError(t, err, "agent not found: agent_absent")
}

// TestSQLiteAgentStore_Delete verifies delete and its not-found error.
func TestSQLiteAgentStore_Delete(t *testing.T) {
	s, _ := newTestAgentStore(t)

	agent := newTestAgent()
	require.NoError(t, s.Create(agent))
	require.NoError(t, s.Delete(agent.ID))

	_, err := s.Get(agent.ID)
	require.EqualError(t, err, "agent not found: "+agent.ID)

	err = s.Delete("agent_absent")
	require.EqualError(t, err, "agent not found: agent_absent")
}
