package agent

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SQLiteAgentStore is a SQLite-backed implementation of AgentStore. Behaviour
// is equivalent to InMemoryAgentStore: same error texts, same defaulting
// rules, timestamps persisted as RFC3339Nano strings (UTC).
type SQLiteAgentStore struct {
	db *sql.DB
}

// NewSQLiteAgentStore creates an AgentStore backed by the given SQLite
// database. The caller is responsible for calling store.Migrate first.
func NewSQLiteAgentStore(db *sql.DB) *SQLiteAgentStore {
	return &SQLiteAgentStore{db: db}
}

var _ AgentStore = (*SQLiteAgentStore)(nil)

// storeTimeLayout is the canonical timestamp layout for storage: RFC3339 with
// a fixed 9-digit UTC fraction, so lexicographic order of stored values
// matches chronological order. (time.RFC3339Nano trims trailing zeros, which
// breaks string comparison — e.g. ".5Z" sorts after ".500000001Z".)
const storeTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

const agentColumns = "id, org_id, name, host, mysql_version, status, cert_serial, last_seen, created_at, registration_token, approved"

// Create stores a new agent record.
func (s *SQLiteAgentStore) Create(agent *AgentRecord) error {
	if agent.ID == "" {
		agent.ID = generateAgentID()
	}
	if agent.CreatedAt.IsZero() {
		agent.CreatedAt = time.Now()
	}
	if agent.Status == "" {
		agent.Status = "offline"
	}
	_, err := s.db.Exec(
		"INSERT INTO agents ("+agentColumns+") VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)",
		agent.ID, agent.OrgID, agent.Hostname, agent.MySQLVersion, agent.Status,
		agent.CertSerial, formatStoreTime(agent.LastSeen),
		agent.CreatedAt.UTC().Format(storeTimeLayout),
		agent.RegistrationToken, boolToInt(agent.Approved),
	)
	return err
}

// Get retrieves an agent by its ID.
func (s *SQLiteAgentStore) Get(id string) (*AgentRecord, error) {
	agent, err := scanAgent(s.db.QueryRow("SELECT "+agentColumns+" FROM agents WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	return agent, err
}

// ListByOrg returns all agents belonging to the given organisation.
func (s *SQLiteAgentStore) ListByOrg(orgID string) ([]*AgentRecord, error) {
	rows, err := s.db.Query(
		"SELECT "+agentColumns+" FROM agents WHERE org_id = ? ORDER BY rowid", orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*AgentRecord
	for rows.Next() {
		agent, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, agent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		return []*AgentRecord{}, nil
	}
	return result, nil
}

// Update replaces an existing agent record.
func (s *SQLiteAgentStore) Update(agent *AgentRecord) error {
	res, err := s.db.Exec(
		"UPDATE agents SET org_id = ?, name = '', host = ?, mysql_version = ?, "+
			"status = ?, cert_serial = ?, last_seen = ?, created_at = ?, "+
			"registration_token = ?, approved = ? WHERE id = ?",
		agent.OrgID, agent.Hostname, agent.MySQLVersion, agent.Status,
		agent.CertSerial, formatStoreTime(agent.LastSeen),
		agent.CreatedAt.UTC().Format(storeTimeLayout),
		agent.RegistrationToken, boolToInt(agent.Approved), agent.ID,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("agent not found: %s", agent.ID)
	}
	return nil
}

// Delete removes an agent record by ID.
func (s *SQLiteAgentStore) Delete(id string) error {
	res, err := s.db.Exec("DELETE FROM agents WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("agent not found: %s", id)
	}
	return nil
}

// scanAgent reads one agent row (in agentColumns order) from a scanner.
func scanAgent(row interface{ Scan(...any) error }) (*AgentRecord, error) {
	var agent AgentRecord
	var name, lastSeen, createdAt string
	var approved int
	err := row.Scan(&agent.ID, &agent.OrgID, &name, &agent.Hostname,
		&agent.MySQLVersion, &agent.Status, &agent.CertSerial,
		&lastSeen, &createdAt, &agent.RegistrationToken, &approved)
	if err != nil {
		return nil, err
	}
	agent.LastSeen, err = parseStoreTime(lastSeen)
	if err != nil {
		return nil, fmt.Errorf("agent %s: parse last_seen: %w", agent.ID, err)
	}
	agent.CreatedAt, err = parseStoreTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("agent %s: parse created_at: %w", agent.ID, err)
	}
	agent.Approved = approved != 0
	return &agent, nil
}

// formatStoreTime renders a timestamp for storage; the zero time becomes the
// zero instant (never an empty string, which the schema default would
// otherwise produce for a zero LastSeen).
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
