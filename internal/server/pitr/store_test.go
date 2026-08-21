package pitr

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/server/store"
)

// newTestStore opens an in-memory SQLite database, migrates the platform
// schema, and returns a pitr OperationStore backed by it.
func newTestStore(t *testing.T) OperationStore {
	t.Helper()
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLiteOperationStore(db)
}

func sampleFilter(t *testing.T) binlog.Filter {
	t.Helper()
	gtid, err := mysql.ParseGTIDSet("mysql", "2d2a45e8-0e8a-11ef-9e9a-0242ac110002:1-5:8")
	require.NoError(t, err)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	return binlog.Filter{
		BinlogDir: "/var/lib/mysql",
		Tables:    []binlog.TableRef{{Schema: "app", Table: "orders"}, {Schema: "app", Table: "customers"}},
		TimeRange: &binlog.TimeRange{Start: now.Add(-time.Hour), End: now},
		GTIDSet:   gtid,
		StartPos:  mysql.Position{Name: "mysql-bin.000001", Pos: 4},
		EndPos:    mysql.Position{Name: "mysql-bin.000001", Pos: 99999},
		MaxRowsPerTx: 100,
	}
}

func TestCreateGet_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	op := &Operation{
		ID:            "op_roundtrip",
		OrgID:         "org_a",
		AgentID:       "agent_1",
		Type:          "flashback",
		Mode:          "sql",
		Filter:        sampleFilter(t),
		Status:        StateCreated,
		CreatedBy:     "user_1",
		CreatedAt:     now,
		UpdatedAt:     now,
		SelectedTxIDs: []string{"tx-a", "tx-b"},
	}
	require.NoError(t, s.Create(op))

	got, err := s.Get("op_roundtrip")
	require.NoError(t, err)
	assert.Equal(t, op.ID, got.ID)
	assert.Equal(t, op.OrgID, got.OrgID)
	assert.Equal(t, op.AgentID, got.AgentID)
	assert.Equal(t, op.Type, got.Type)
	assert.Equal(t, op.Mode, got.Mode)
	assert.Equal(t, op.Status, got.Status)
	assert.Equal(t, op.CreatedBy, got.CreatedBy)
	assert.Equal(t, op.SelectedTxIDs, got.SelectedTxIDs)
	assert.True(t, op.CreatedAt.Equal(got.CreatedAt), "created_at round trip")
	assert.True(t, op.UpdatedAt.Equal(got.UpdatedAt), "updated_at round trip")

	// Filter round trip: every exported field survives JSON storage.
	assert.Equal(t, op.Filter.BinlogDir, got.Filter.BinlogDir)
	assert.Equal(t, op.Filter.Tables, got.Filter.Tables)
	require.NotNil(t, got.Filter.TimeRange)
	assert.True(t, op.Filter.TimeRange.Start.Equal(got.Filter.TimeRange.Start))
	assert.True(t, op.Filter.TimeRange.End.Equal(got.Filter.TimeRange.End))
	require.NotNil(t, got.Filter.GTIDSet)
	assert.Equal(t, op.Filter.GTIDSet.String(), got.Filter.GTIDSet.String())
	assert.Equal(t, op.Filter.StartPos, got.Filter.StartPos)
	assert.Equal(t, op.Filter.EndPos, got.Filter.EndPos)
	assert.Equal(t, op.Filter.MaxRowsPerTx, got.Filter.MaxRowsPerTx)
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("op_nope")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operation not found")
}

func TestUpdate_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	op := &Operation{ID: "op_upd", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "meta", Status: StateCreated, CreatedBy: "user_1", CreatedAt: now, UpdatedAt: now}
	require.NoError(t, s.Create(op))

	op.Status = StateScanning
	op.SelectedTxIDs = []string{"tx-1"}
	op.Mode = "selected"
	op.UpdatedAt = now.Add(time.Minute)
	require.NoError(t, s.Update(op))

	got, err := s.Get("op_upd")
	require.NoError(t, err)
	assert.Equal(t, StateScanning, got.Status)
	assert.Equal(t, []string{"tx-1"}, got.SelectedTxIDs)
	assert.Equal(t, "selected", got.Mode)
	assert.True(t, got.UpdatedAt.Equal(now.Add(time.Minute)))
}

func TestUpdate_NotFound(t *testing.T) {
	s := newTestStore(t)
	err := s.Update(&Operation{ID: "op_nope"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operation not found")
}

func TestCreate_Defaults(t *testing.T) {
	s := newTestStore(t)
	op := &Operation{OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql"}
	require.NoError(t, s.Create(op))

	assert.NotEmpty(t, op.ID, "ID should be generated")
	assert.Equal(t, StateCreated, op.Status, "status should default to created")
	assert.False(t, op.CreatedAt.IsZero(), "created_at should be set")
	assert.False(t, op.UpdatedAt.IsZero(), "updated_at should be set")

	got, err := s.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, op.ID, got.ID)
	assert.Equal(t, StateCreated, got.Status)
}

func TestListByOrg_Filters(t *testing.T) {
	s := newTestStore(t)
	for _, op := range []*Operation{
		{ID: "op_a1", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql"},
		{ID: "op_a2", OrgID: "org_a", AgentID: "agent_2", Type: "pitr", Mode: "meta"},
		{ID: "op_b1", OrgID: "org_b", AgentID: "agent_1", Type: "pitr", Mode: "sql"},
	} {
		require.NoError(t, s.Create(op))
	}

	got, err := s.ListByOrg("org_a")
	require.NoError(t, err)
	require.Len(t, got, 2)
	ids := map[string]bool{}
	for _, op := range got {
		ids[op.ID] = true
	}
	assert.True(t, ids["op_a1"], "op_a1 listed")
	assert.True(t, ids["op_a2"], "op_a2 listed")

	// Unknown org yields an empty (non-nil) slice.
	empty, err := s.ListByOrg("org_nope")
	require.NoError(t, err)
	assert.NotNil(t, empty)
	assert.Len(t, empty, 0)
}

func TestListByAgent_Filters(t *testing.T) {
	s := newTestStore(t)
	for _, op := range []*Operation{
		{ID: "op_1", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql"},
		{ID: "op_2", OrgID: "org_a", AgentID: "agent_2", Type: "pitr", Mode: "meta"},
		{ID: "op_3", OrgID: "org_b", AgentID: "agent_1", Type: "pitr", Mode: "sql"},
	} {
		require.NoError(t, s.Create(op))
	}

	got, err := s.ListByAgent("agent_1")
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, op := range got {
		assert.Equal(t, "agent_1", op.AgentID)
	}

	empty, err := s.ListByAgent("agent_nope")
	require.NoError(t, err)
	assert.NotNil(t, empty)
	assert.Len(t, empty, 0)
}

func TestListByStatus_Filters(t *testing.T) {
	s := newTestStore(t)
	for _, op := range []*Operation{
		{ID: "op_scan", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql", Status: StateScanning},
		{ID: "op_exec1", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql", Status: StateExecuting},
		{ID: "op_exec2", OrgID: "org_b", AgentID: "agent_2", Type: "pitr", Mode: "meta", Status: StateExecuting},
		{ID: "op_done", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql", Status: StateDone},
	} {
		require.NoError(t, s.Create(op))
	}

	scanning, err := s.ListByStatus(StateScanning)
	require.NoError(t, err)
	require.Len(t, scanning, 1)
	assert.Equal(t, "op_scan", scanning[0].ID)

	executing, err := s.ListByStatus(StateExecuting)
	require.NoError(t, err)
	require.Len(t, executing, 2)
	ids := map[string]bool{}
	for _, op := range executing {
		ids[op.ID] = true
		assert.Equal(t, StateExecuting, op.Status)
	}
	assert.True(t, ids["op_exec1"], "op_exec1 listed")
	assert.True(t, ids["op_exec2"], "op_exec2 listed")

	// A status with no operations yields an empty (non-nil) slice.
	none, err := s.ListByStatus(StatePaused)
	require.NoError(t, err)
	assert.NotNil(t, none)
	assert.Len(t, none, 0)
}

func TestSaveLoadStatements_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	op := &Operation{ID: "op_stmts", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "selected", Status: StateReady}
	require.NoError(t, s.Create(op))

	stmts := []Statement{
		{TxIndex: 0, StmtIndex: 0, SQL: "DELETE FROM orders WHERE id = 1;", TxID: "tx-1", TxOrder: 1, Warnings: []string{"low_cardinality"}, Status: "approved"},
		{TxIndex: 0, StmtIndex: 1, SQL: "DELETE FROM orders WHERE id = 2;", TxID: "tx-1", TxOrder: 1, Status: "approved"},
		{TxIndex: 1, StmtIndex: 2, SQL: "DELETE FROM orders WHERE id = 3;", TxID: "tx-2", TxOrder: 2, Status: "pending"},
	}
	require.NoError(t, s.SaveStatements("op_stmts", stmts))

	got, err := s.LoadStatements("op_stmts")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, stmts, got, "statements round trip")

	// LoadStatements on an op with no saved statements yields empty, not error.
	none, err := s.LoadStatements("op_none")
	require.NoError(t, err)
	assert.Len(t, none, 0)

	// Re-saving replaces the previous set (reselect semantics).
	replacement := []Statement{
		{TxIndex: 2, StmtIndex: 0, SQL: "DELETE FROM orders WHERE id = 9;", TxID: "tx-3", TxOrder: 3, Status: "approved"},
	}
	require.NoError(t, s.SaveStatements("op_stmts", replacement))
	got2, err := s.LoadStatements("op_stmts")
	require.NoError(t, err)
	assert.Equal(t, replacement, got2, "re-save replaces previous statements")
}

func TestSaveStatementsAndSelect_Atomic(t *testing.T) {
	// The sql-mode Select path persists statements and the selection in one
	// transaction: either both land, or neither. A failure of the second step
	// (the selection update) must roll back the statement writes.
	s := newTestStore(t)
	op := &Operation{ID: "op_atomic", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql", Status: StateReady}
	require.NoError(t, s.Create(op))

	stmts := []Statement{
		{TxIndex: 0, StmtIndex: 0, SQL: "DELETE FROM orders WHERE id = 1;", TxID: "tx-1", TxOrder: 1, Status: "pending"},
		{TxIndex: 0, StmtIndex: 1, SQL: "DELETE FROM orders WHERE id = 2;", TxID: "tx-1", TxOrder: 2, Status: "pending"},
	}

	// Success: statements and the selection land in the same commit.
	tables := []binlog.TableRef{{Schema: "shop", Table: "orders"}}
	require.NoError(t, s.SaveStatementsAndSelect("op_atomic", stmts, []string{"tx-1"}, tables))
	got, err := s.Get("op_atomic")
	require.NoError(t, err)
	saved, err := s.LoadStatements("op_atomic")
	require.NoError(t, err)
	require.Len(t, saved, 2)
	assert.Equal(t, []string{"tx-1"}, got.SelectedTxIDs)
	assert.Equal(t, tables, got.TargetTables)

	// Failure of the second step (the operations row is missing, so the
	// selection update fails) rolls back the statement writes: nothing is
	// persisted, and a previous selection stays intact.
	err = s.SaveStatementsAndSelect("op_missing", stmts, []string{"tx-1"}, tables)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operation not found")
	none, err := s.LoadStatements("op_missing")
	require.NoError(t, err)
	assert.Len(t, none, 0, "statement writes must roll back with the failed selection update")
}

func TestGet_DoesNotLoadStatements(t *testing.T) {
	s := newTestStore(t)
	op := &Operation{ID: "op_full", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "selected", Status: StateReady}
	require.NoError(t, s.Create(op))
	require.NoError(t, s.SaveStatements("op_full", []Statement{
		{TxIndex: 0, StmtIndex: 0, SQL: "DELETE FROM t WHERE id = 1;", TxID: "tx-1", TxOrder: 1, Status: "pending"},
	}))

	// Get is on the authorizeOp hot path (every /status poll) and must not
	// drag the statement set along; callers load it explicitly.
	got, err := s.Get("op_full")
	require.NoError(t, err)
	assert.Empty(t, got.Statements)

	stmts, err := s.LoadStatements("op_full")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "DELETE FROM t WHERE id = 1;", stmts[0].SQL)
	assert.Equal(t, "tx-1", stmts[0].TxID)
}

func TestFilter_EmptyFilterRoundTrip(t *testing.T) {
	s := newTestStore(t)
	op := &Operation{ID: "op_nofilter", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "meta"}
	require.NoError(t, s.Create(op))

	got, err := s.Get("op_nofilter")
	require.NoError(t, err)
	assert.Nil(t, got.Filter.GTIDSet)
	assert.Nil(t, got.Filter.TimeRange)
	assert.Len(t, got.Filter.Tables, 0)
	assert.Equal(t, mysql.Position{}, got.Filter.StartPos)
}

func TestUpdateIfStatus_CAS(t *testing.T) {
	s := newTestStore(t)
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	op := &Operation{ID: "op_cas", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql", Status: StateReady, CreatedBy: "user_1", CreatedAt: now, UpdatedAt: now}
	require.NoError(t, s.Create(op))

	// First CAS wins: ready -> executing.
	op.Status = StateExecuting
	op.UpdatedAt = now.Add(time.Minute)
	applied, err := s.UpdateIfStatus(op, StateReady)
	require.NoError(t, err)
	assert.True(t, applied, "CAS from the current status must apply")

	got, err := s.Get("op_cas")
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status)

	// A second CAS with a stale `from` (still ready) must fail: RowsAffected
	// 0, no error, and the target status must not overwrite the persisted one.
	op.Status = StateDone
	applied, err = s.UpdateIfStatus(op, StateReady)
	require.NoError(t, err)
	assert.False(t, applied, "CAS from a stale status must not apply")

	got, err = s.Get("op_cas")
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status, "a lost CAS must not overwrite the persisted status")

	// A CAS from the actual current status still applies afterwards.
	applied, err = s.UpdateIfStatus(op, StateExecuting)
	require.NoError(t, err)
	assert.True(t, applied, "CAS from the updated status must apply")
	got, err = s.Get("op_cas")
	require.NoError(t, err)
	assert.Equal(t, StateDone, got.Status)
}

func TestUpdate_StatusMigrationPersists(t *testing.T) {
	s := newTestStore(t)
	op := &Operation{ID: "op_migrate", OrgID: "org_a", AgentID: "agent_1", Type: "pitr", Mode: "sql"}
	require.NoError(t, s.Create(op))

	op.Status = StateScanning
	require.NoError(t, s.Update(op))
	got, err := s.Get("op_migrate")
	require.NoError(t, err)
	assert.Equal(t, StateScanning, got.Status)

	op.Status = StateReady
	require.NoError(t, s.Update(op))
	got, err = s.Get("op_migrate")
	require.NoError(t, err)
	assert.Equal(t, StateReady, got.Status)

	op.Status = StateExecuting
	require.NoError(t, s.Update(op))
	got, err = s.Get("op_migrate")
	require.NoError(t, err)
	assert.Equal(t, StateExecuting, got.Status)

	op.Status = StateDone
	require.NoError(t, s.Update(op))
	got, err = s.Get("op_migrate")
	require.NoError(t, err)
	assert.Equal(t, StateDone, got.Status)
}
