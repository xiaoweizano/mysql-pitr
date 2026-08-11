package audit

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/server/store"
)

func newTestAuditStore(t *testing.T) (*SQLiteAuditStore, *sql.DB) {
	t.Helper()
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLiteAuditStore(db), db
}

// seedSQLiteAuditEntries appends three fixed entries across two orgs.
func seedSQLiteAuditEntries(t *testing.T, s *SQLiteAuditStore) {
	t.Helper()
	entries := []*AuditEntry{
		{
			OperationID:  "op_001",
			Operator:     "admin@example.com",
			Timestamp:    time.Date(2026, 8, 1, 8, 0, 0, 0, time.FixedZone("CST", 8*3600)), // = 2026-08-01T00:00:00Z
			OrgID:        "org_1",
			AgentID:      "agent_01",
			TargetTable:  "orders",
			RecoveryTime: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC),
			RowsAffected: 1250,
			Status:       "completed",
			ErrorDetails: "",
		},
		{
			OperationID:  "op_002",
			Operator:     "admin@example.com",
			Timestamp:    time.Date(2026, 8, 2, 0, 0, 0, 500000000, time.UTC),
			OrgID:        "org_1",
			AgentID:      "agent_01",
			TargetTable:  "users",
			RecoveryTime: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			RowsAffected: 500,
			Status:       "completed",
		},
		{
			OperationID:  "op_003",
			Operator:     "dev@example.com",
			Timestamp:    time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC),
			OrgID:        "org_2",
			AgentID:      "agent_02",
			TargetTable:  "payments",
			RecoveryTime: time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC),
			RowsAffected: 0,
			Status:       "failed",
			ErrorDetails: "binlog gap detected",
		},
	}
	for _, e := range entries {
		require.NoError(t, s.Append(e))
	}
}

// TestSQLiteAuditStore_AppendAndListAll verifies append order, field
// round-trip (including RecoveryTime/RowsAffected/ErrorDetails) and the
// empty-slice contract.
func TestSQLiteAuditStore_AppendAndListAll(t *testing.T) {
	s, _ := newTestAuditStore(t)

	empty, err := s.ListAll()
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)

	seedSQLiteAuditEntries(t, s)

	all, err := s.ListAll()
	require.NoError(t, err)
	require.Len(t, all, 3)
	require.Equal(t, "op_001", all[0].OperationID)
	require.Equal(t, "op_002", all[1].OperationID)
	require.Equal(t, "op_003", all[2].OperationID)

	e := all[0]
	require.Equal(t, "admin@example.com", e.Operator)
	require.Equal(t, "org_1", e.OrgID)
	require.Equal(t, "agent_01", e.AgentID)
	require.Equal(t, "orders", e.TargetTable)
	require.Equal(t, int64(1250), e.RowsAffected)
	require.Equal(t, "completed", e.Status)
	require.Equal(t, "2026-08-01T00:00:00Z",
		e.Timestamp.UTC().Format(time.RFC3339Nano))
	require.Equal(t, "2026-07-31T00:00:00Z",
		e.RecoveryTime.UTC().Format(time.RFC3339Nano))

	require.Equal(t, "binlog gap detected", all[2].ErrorDetails)
}

// TestSQLiteAuditStore_Append_DefaultTimestamp verifies a zero Timestamp is
// populated before persistence.
func TestSQLiteAuditStore_Append_DefaultTimestamp(t *testing.T) {
	s, _ := newTestAuditStore(t)

	entry := &AuditEntry{OrgID: "org_1", OperationID: "op_x"}
	require.NoError(t, s.Append(entry))
	require.False(t, entry.Timestamp.IsZero())

	all, err := s.ListAll()
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.False(t, all[0].Timestamp.IsZero())
}

// TestSQLiteAuditStore_Query_OrgRequired verifies the mandatory-org filter.
func TestSQLiteAuditStore_Query_OrgRequired(t *testing.T) {
	s, _ := newTestAuditStore(t)

	_, err := s.Query(AuditFilter{})
	require.EqualError(t, err, "orgId filter is required")
}

// TestSQLiteAuditStore_Query_Filter verifies each optional filter dimension
// (status, agent, operation, time range) with InMemory-equivalent boundary
// semantics: From/To are inclusive.
func TestSQLiteAuditStore_Query_Filter(t *testing.T) {
	s, _ := newTestAuditStore(t)
	seedSQLiteAuditEntries(t, s)

	// Status filter.
	completed, err := s.Query(AuditFilter{OrgID: "org_1", Status: "completed"})
	require.NoError(t, err)
	require.Len(t, completed, 2)

	// Agent filter.
	byAgent, err := s.Query(AuditFilter{OrgID: "org_1", AgentID: "agent_01"})
	require.NoError(t, err)
	require.Len(t, byAgent, 2)

	// Operation filter.
	byOp, err := s.Query(AuditFilter{OrgID: "org_2", OperationID: "op_003"})
	require.NoError(t, err)
	require.Len(t, byOp, 1)
	require.Equal(t, "failed", byOp[0].Status)

	// Time range: From/To inclusive, across timezone-normalised timestamps.
	// op_002 sits exactly at 2026-08-02T00:00:00.5Z, so a closed window on
	// that instant must match it (and op_001 at 2026-08-01T00:00:00Z is
	// excluded by From).
	boundary := time.Date(2026, 8, 2, 0, 0, 0, 500000000, time.UTC)
	window, err := s.Query(AuditFilter{OrgID: "org_1", From: boundary, To: boundary})
	require.NoError(t, err)
	require.Len(t, window, 1)
	require.Equal(t, "op_002", window[0].OperationID)

	// From-only exactness: a From of .5Z includes op_002, .500000001Z
	// excludes it.
	exact, err := s.Query(AuditFilter{OrgID: "org_1", From: boundary})
	require.NoError(t, err)
	require.Len(t, exact, 1)
	require.Equal(t, "op_002", exact[0].OperationID)

	after, err := s.Query(AuditFilter{
		OrgID: "org_1",
		From:  time.Date(2026, 8, 2, 0, 0, 0, 500000001, time.UTC),
	})
	require.NoError(t, err)
	require.Empty(t, after)
}

// TestSQLiteAuditStore_Query_NoMatches verifies the empty-slice contract for a
// valid org with no matching entries.
func TestSQLiteAuditStore_Query_NoMatches(t *testing.T) {
	s, _ := newTestAuditStore(t)
	seedSQLiteAuditEntries(t, s)

	result, err := s.Query(AuditFilter{OrgID: "org_1", Status: "failed"})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result)
}
