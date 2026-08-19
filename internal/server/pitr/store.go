package pitr

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/server/store"
)

// SQLiteOperationStore is a SQLite-backed implementation of OperationStore.
// It requires the platform schema (internal/server/store.Migrate) to have been
// applied: the `operations` table maps the Operation record (filter and
// selected_tx_ids stored as JSON TEXT), and the `statements` table is keyed by
// (op_id, stmt_index).
type SQLiteOperationStore struct {
	db *sql.DB
}

// NewSQLiteOperationStore creates an OperationStore backed by the given SQLite
// database. `:memory:` databases are supported. The caller is responsible for
// applying the platform schema first (store.Migrate).
func NewSQLiteOperationStore(db *sql.DB) OperationStore {
	return &SQLiteOperationStore{db: db}
}

var _ OperationStore = (*SQLiteOperationStore)(nil)

// storeTimeLayout is the canonical timestamp layout for storage: RFC3339 with
// a fixed 9-digit UTC fraction, so lexicographic order of stored values
// matches chronological order. (time.RFC3339Nano trims trailing zeros, which
// breaks string comparison — e.g. ".5Z" sorts after ".500000001Z".)
// Same convention as the other domain stores (agent/auth/org/audit sqlite.go).
const storeTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// formatStoreTime renders a timestamp for storage in UTC with a fixed-width
// 9-digit fraction.
func formatStoreTime(t time.Time) string {
	return t.UTC().Format(storeTimeLayout)
}

// parseStoreTime decodes a stored timestamp; empty strings (schema defaults)
// decode to the zero time.
func parseStoreTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

const opColumns = "id, org_id, agent_id, type, mode, filter, selected_tx_ids, status, created_by, created_at, updated_at"

// Create stores a new operation, generating an ID and defaulting Status /
// timestamps when not set. Statements are not persisted here — use
// SaveStatements after selection.
func (s *SQLiteOperationStore) Create(op *Operation) error {
	if op.ID == "" {
		op.ID = generateOpID()
	}
	if op.CreatedAt.IsZero() {
		op.CreatedAt = time.Now()
	}
	if op.UpdatedAt.IsZero() {
		op.UpdatedAt = op.CreatedAt
	}
	if op.Status == "" {
		op.Status = StateCreated
	}
	filterJSON, err := marshalFilter(op.Filter)
	if err != nil {
		return fmt.Errorf("pitr: marshal filter: %w", err)
	}
	sel, err := json.Marshal(op.SelectedTxIDs)
	if err != nil {
		return fmt.Errorf("pitr: marshal selected_tx_ids: %w", err)
	}
	_, err = s.db.Exec(
		"INSERT INTO operations ("+opColumns+") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		op.ID, op.OrgID, op.AgentID, op.Type, op.Mode, filterJSON, string(sel),
		string(op.Status), op.CreatedBy,
		formatStoreTime(op.CreatedAt), formatStoreTime(op.UpdatedAt))
	return err
}

// Get retrieves an operation by ID. Statement rows are NOT included — Get is
// on the hot path (authorizeOp runs it for every /status poll and stream
// event), and loading an operation's full statement set on each call would
// drag megabytes through SQLite for large selections. Call LoadStatements
// explicitly where statements are needed (e.g. sendExecute).
func (s *SQLiteOperationStore) Get(id string) (*Operation, error) {
	op, err := scanOperation(s.db.QueryRow("SELECT "+opColumns+" FROM operations WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("operation not found: %s", id)
	}
	if err != nil {
		return nil, err
	}
	return op, nil
}

// Update persists the whole operation record. Statements are untouched — use
// SaveStatements for those. A zero UpdatedAt is refreshed to now so callers
// can omit it on status transitions.
func (s *SQLiteOperationStore) Update(op *Operation) error {
	if op.UpdatedAt.IsZero() {
		op.UpdatedAt = time.Now()
	}
	filterJSON, err := marshalFilter(op.Filter)
	if err != nil {
		return fmt.Errorf("pitr: marshal filter: %w", err)
	}
	sel, err := json.Marshal(op.SelectedTxIDs)
	if err != nil {
		return fmt.Errorf("pitr: marshal selected_tx_ids: %w", err)
	}
	res, err := s.db.Exec(
		"UPDATE operations SET org_id = ?, agent_id = ?, type = ?, mode = ?, "+
			"filter = ?, selected_tx_ids = ?, status = ?, created_by = ?, "+
			"created_at = ?, updated_at = ? WHERE id = ?",
		op.OrgID, op.AgentID, op.Type, op.Mode, filterJSON, string(sel),
		string(op.Status), op.CreatedBy,
		formatStoreTime(op.CreatedAt), formatStoreTime(op.UpdatedAt), op.ID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("operation not found: %s", op.ID)
	}
	return nil
}

// UpdateIfStatus is the compare-and-swap form of a state transition: it
// updates the operation's status (and updated_at) only while the persisted
// status still equals `from`, closing the read-modify-write window of
// Get-then-Update. It reports whether the update applied (RowsAffected==1);
// a false result with no error means another actor moved the operation first
// and the caller must treat the transition as lost. Unlike Update it touches
// only status and updated_at, so a transition cannot clobber concurrent field
// writes.
func (s *SQLiteOperationStore) UpdateIfStatus(op *Operation, from OperationState) (bool, error) {
	if op.UpdatedAt.IsZero() {
		op.UpdatedAt = time.Now()
	}
	res, err := s.db.Exec(
		"UPDATE operations SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
		string(op.Status), formatStoreTime(op.UpdatedAt), op.ID, string(from))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ListByOrg returns all operations of an organisation (statement rows not
// included; use Get for the full record).
func (s *SQLiteOperationStore) ListByOrg(orgID string) ([]*Operation, error) {
	return s.list("WHERE org_id = ? ORDER BY rowid", orgID)
}

// ListByAgent returns all operations of an agent (statement rows not
// included; use Get for the full record).
func (s *SQLiteOperationStore) ListByAgent(agentID string) ([]*Operation, error) {
	return s.list("WHERE agent_id = ? ORDER BY rowid", agentID)
}

// ListByStatus returns all operations currently in the given state (statement
// rows not included; use Get for the full record). It is the basis for the
// disconnect handler and startup reconcile, which enumerate in-flight
// (scanning/executing) operations.
func (s *SQLiteOperationStore) ListByStatus(status OperationState) ([]*Operation, error) {
	return s.list("WHERE status = ? ORDER BY rowid", string(status))
}

func (s *SQLiteOperationStore) list(where string, arg string) ([]*Operation, error) {
	rows, err := s.db.Query("SELECT "+opColumns+" FROM operations "+where, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*Operation
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		return []*Operation{}, nil
	}
	return result, nil
}

// SaveStatements replaces all persisted statements of an operation with the
// given set, atomically (delete-then-insert in one transaction).
func (s *SQLiteOperationStore) SaveStatements(opID string, stmts []Statement) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("DELETE FROM statements WHERE op_id = ?", opID); err != nil {
		return err
	}
	for _, st := range stmts {
		warns, err := json.Marshal(st.Warnings)
		if err != nil {
			return fmt.Errorf("pitr: marshal warnings: %w", err)
		}
		if _, err := tx.Exec(
			"INSERT INTO statements (op_id, tx_index, stmt_index, sql, tx_id, tx_order, warnings, status) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			opID, st.TxIndex, st.StmtIndex, st.SQL, st.TxID, st.TxOrder,
			string(warns), st.Status); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SaveStatementsAndSelect replaces all persisted statements of an operation
// and records its SelectedTxIDs in a single transaction. The sql-mode Select
// path used to call SaveStatements followed by Update, leaving a window where
// the statements were persisted but the selection was not; both writes now
// commit (or roll back) together, so a failed selection update can never leave
// orphaned statements behind.
func (s *SQLiteOperationStore) SaveStatementsAndSelect(opID string, stmts []Statement, txIDs []string) error {
	sel, err := json.Marshal(txIDs)
	if err != nil {
		return fmt.Errorf("pitr: marshal selected_tx_ids: %w", err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("DELETE FROM statements WHERE op_id = ?", opID); err != nil {
		return err
	}
	for _, st := range stmts {
		warns, err := json.Marshal(st.Warnings)
		if err != nil {
			return fmt.Errorf("pitr: marshal warnings: %w", err)
		}
		if _, err := tx.Exec(
			"INSERT INTO statements (op_id, tx_index, stmt_index, sql, tx_id, tx_order, warnings, status) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			opID, st.TxIndex, st.StmtIndex, st.SQL, st.TxID, st.TxOrder,
			string(warns), st.Status); err != nil {
			return err
		}
	}
	res, err := tx.Exec(
		"UPDATE operations SET selected_tx_ids = ?, updated_at = ? WHERE id = ?",
		string(sel), formatStoreTime(time.Now()), opID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("operation not found: %s", opID)
	}
	return tx.Commit()
}

// LoadStatements returns the persisted statements of an operation ordered by
// statement index.
func (s *SQLiteOperationStore) LoadStatements(opID string) ([]Statement, error) {	rows, err := s.db.Query(
		"SELECT tx_index, stmt_index, sql, tx_id, tx_order, warnings, status "+
			"FROM statements WHERE op_id = ? ORDER BY stmt_index", opID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Statement
	for rows.Next() {
		var st Statement
		var warns, status string
		if err := rows.Scan(&st.TxIndex, &st.StmtIndex, &st.SQL, &st.TxID,
			&st.TxOrder, &warns, &status); err != nil {
			return nil, err
		}
		st.Status = status
		if warns != "" {
			if err := json.Unmarshal([]byte(warns), &st.Warnings); err != nil {
				return nil, fmt.Errorf("pitr: op %s: parse warnings: %w", opID, err)
			}
		}
		result = append(result, st)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		return []Statement{}, nil
	}
	return result, nil
}

// SaveCheckpoint upserts the operation's execution checkpoint into the
// checkpoints table (platform schema, see internal/server/store). Delegates to
// the platform store so the upsert SQL lives next to the table definition.
func (s *SQLiteOperationStore) SaveCheckpoint(opID string, lastStmt, total int, errorsJSON string) error {
	return store.SaveCheckpoint(s.db, opID, lastStmt, total, errorsJSON)
}

// LoadCheckpoint reads the execution checkpoint for an operation. Returns
// found=false when no checkpoint exists.
func (s *SQLiteOperationStore) LoadCheckpoint(opID string) (lastStmt, total int, errorsJSON string, found bool, err error) {
	return store.LoadCheckpoint(s.db, opID)
}

// scanOperation reads one operations row (in opColumns order) from a scanner.
func scanOperation(row interface{ Scan(...any) error }) (*Operation, error) {
	var op Operation
	var filterJSON, sel, status, createdAt, updatedAt string
	err := row.Scan(&op.ID, &op.OrgID, &op.AgentID, &op.Type, &op.Mode,
		&filterJSON, &sel, &status, &op.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	op.Status = OperationState(status)
	op.Filter, err = unmarshalFilter(filterJSON)
	if err != nil {
		return nil, fmt.Errorf("pitr: op %s: parse filter: %w", op.ID, err)
	}
	if err := json.Unmarshal([]byte(sel), &op.SelectedTxIDs); err != nil {
		return nil, fmt.Errorf("pitr: op %s: parse selected_tx_ids: %w", op.ID, err)
	}
	op.CreatedAt, err = parseStoreTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("pitr: op %s: parse created_at: %w", op.ID, err)
	}
	op.UpdatedAt, err = parseStoreTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("pitr: op %s: parse updated_at: %w", op.ID, err)
	}
	return &op, nil
}

// filterJSON is the JSON-storable projection of binlog.Filter. GTIDSet is an
// interface (mysql.GTIDSet); json.Marshal of the interface would lose the
// concrete type on unmarshal (decoding into an interface yields a map), so it
// is stored as its String() form and re-parsed on load. All other fields are
// exported structs/scalars and round-trip directly.
type filterJSON struct {
	BinlogDir     string
	Tables        []binlog.TableRef
	TimeRange     *binlog.TimeRange
	GTIDSet       string
	StartPos      mysql.Position
	EndPos        mysql.Position
	MaxRowsPerTx  int
	SelectedTxIDs []string
}

// marshalFilter encodes a binlog.Filter as JSON TEXT for storage.
func marshalFilter(f binlog.Filter) (string, error) {
	var gtid string
	if f.GTIDSet != nil {
		gtid = f.GTIDSet.String()
	}
	dto := filterJSON{
		BinlogDir:     f.BinlogDir,
		Tables:        f.Tables,
		TimeRange:     f.TimeRange,
		GTIDSet:       gtid,
		StartPos:      f.StartPos,
		EndPos:        f.EndPos,
		MaxRowsPerTx:  f.MaxRowsPerTx,
		SelectedTxIDs: f.SelectedTxIDs,
	}
	b, err := json.Marshal(dto)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// unmarshalFilter decodes a stored filter JSON TEXT back into a binlog.Filter.
func unmarshalFilter(s string) (binlog.Filter, error) {
	var dto filterJSON
	if err := json.Unmarshal([]byte(s), &dto); err != nil {
		return binlog.Filter{}, err
	}
	f := binlog.Filter{
		BinlogDir:     dto.BinlogDir,
		Tables:        dto.Tables,
		TimeRange:     dto.TimeRange,
		StartPos:      dto.StartPos,
		EndPos:        dto.EndPos,
		MaxRowsPerTx:  dto.MaxRowsPerTx,
		SelectedTxIDs: dto.SelectedTxIDs,
	}
	if dto.GTIDSet != "" {
		g, err := mysql.ParseGTIDSet("mysql", dto.GTIDSet)
		if err != nil {
			// MariaDB GTID sets have a different syntax; fall back before
			// giving up on the stored value.
			g, err = mysql.ParseGTIDSet("mariadb", dto.GTIDSet)
		}
		if err != nil {
			return binlog.Filter{}, fmt.Errorf("parse gtid set %q: %w", dto.GTIDSet, err)
		}
		f.GTIDSet = g
	}
	return f, nil
}

// generateOpID creates a unique operation identifier.
func generateOpID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "op_" + hex.EncodeToString(b)
}
