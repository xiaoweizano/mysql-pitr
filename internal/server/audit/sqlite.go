package audit

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// SQLiteAuditStore is a SQLite-backed implementation of AuditStore. Behaviour
// is equivalent to InMemoryAuditStore: same error text, same filtering
// semantics (org required, From/To inclusive), empty results come back as
// non-nil empty slices, timestamps persisted as RFC3339Nano strings (UTC).
type SQLiteAuditStore struct {
	db *sql.DB
}

// NewSQLiteAuditStore creates an AuditStore backed by the given SQLite
// database. The caller is responsible for calling store.Migrate first.
func NewSQLiteAuditStore(db *sql.DB) *SQLiteAuditStore {
	return &SQLiteAuditStore{db: db}
}

var _ AuditStore = (*SQLiteAuditStore)(nil)

// storeTimeLayout is the canonical timestamp layout for storage: RFC3339 with
// a fixed 9-digit UTC fraction, so lexicographic order of stored values
// matches chronological order. (time.RFC3339Nano trims trailing zeros, which
// breaks string comparison — e.g. ".5Z" sorts after ".500000001Z".)
const storeTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

const entryColumns = "op_id, operator, ts, org_id, agent_id, target_table, recovery_time, rows_affected, status, detail"

// Append adds an audit entry to the store.
func (s *SQLiteAuditStore) Append(entry *AuditEntry) error {
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}
	_, err := s.db.Exec(
		"INSERT INTO audit_logs (op_id, operator, ts, org_id, agent_id, action, target_table, recovery_time, rows_affected, status, detail) "+
			"VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)",
		entry.OperationID, entry.Operator,
		entry.Timestamp.UTC().Format(storeTimeLayout),
		entry.OrgID, entry.AgentID,
		entry.TargetTable,
		entry.RecoveryTime.UTC().Format(storeTimeLayout),
		entry.RowsAffected, entry.Status, entry.ErrorDetails,
	)
	return err
}

// Query filters audit entries by the supplied filter. Only OrgID is required;
// all other fields are optional. Timestamps are compared against the stored
// RFC3339Nano (UTC) form, so filter bounds are normalised the same way before
// the string comparison — exact InMemory boundary semantics (From/To
// inclusive) down to nanoseconds.
func (s *SQLiteAuditStore) Query(filter AuditFilter) ([]AuditEntry, error) {
	if filter.OrgID == "" {
		return nil, fmt.Errorf("orgId filter is required")
	}

	where := []string{"org_id = ?"}
	args := []any{filter.OrgID}
	if !filter.From.IsZero() {
		where = append(where, "ts >= ?")
		args = append(args, formatStoreTime(filter.From))
	}
	if !filter.To.IsZero() {
		where = append(where, "ts <= ?")
		args = append(args, formatStoreTime(filter.To))
	}
	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}
	if filter.AgentID != "" {
		where = append(where, "agent_id = ?")
		args = append(args, filter.AgentID)
	}
	if filter.OperationID != "" {
		where = append(where, "op_id = ?")
		args = append(args, filter.OperationID)
	}

	// id is AUTOINCREMENT: descending id is newest-first — the audit page
	// shows the latest state transitions at the top.
	rows, err := s.db.Query(
		"SELECT "+entryColumns+" FROM audit_logs WHERE "+strings.Join(where, " AND ")+" ORDER BY id DESC",
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err := scanEntries(rows)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []AuditEntry{}, nil
	}
	return result, nil
}

// ListAll returns all audit entries in insertion order.
func (s *SQLiteAuditStore) ListAll() ([]AuditEntry, error) {
	rows, err := s.db.Query("SELECT " + entryColumns + " FROM audit_logs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err := scanEntries(rows)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []AuditEntry{}, nil
	}
	return result, nil
}

func scanEntries(rows *sql.Rows) ([]AuditEntry, error) {
	var result []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var ts, recoveryTime string
		if err := rows.Scan(&e.OperationID, &e.Operator, &ts, &e.OrgID,
			&e.AgentID, &e.TargetTable, &recoveryTime,
			&e.RowsAffected, &e.Status, &e.ErrorDetails); err != nil {
			return nil, err
		}
		parsed, perr := parseStoreTime(ts)
		if perr != nil {
			return nil, fmt.Errorf("audit entry: parse ts: %w", perr)
		}
		e.Timestamp = parsed
		parsed, perr = parseStoreTime(recoveryTime)
		if perr != nil {
			return nil, fmt.Errorf("audit entry: parse recovery_time: %w", perr)
		}
		e.RecoveryTime = parsed
		result = append(result, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// formatStoreTime renders a timestamp for storage; the zero time becomes the
// zero instant (never an empty string).
func formatStoreTime(t time.Time) string {
	return t.UTC().Format(storeTimeLayout)
}

// parseStoreTime decodes an RFC3339Nano timestamp; empty strings (schema
// defaults) decode to the zero time.
func parseStoreTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
