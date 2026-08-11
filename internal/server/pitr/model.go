package pitr

import (
	"time"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

// Operation is the v3 PITR operation model. An operation is created for a
// target agent with a binlog Filter, advanced through the state machine in
// state.go, and persisted in SQLite via OperationStore.
type Operation struct {
	ID      string `json:"id"`
	OrgID   string `json:"orgId"`
	AgentID string `json:"agentId"`
	// Type is the operation kind: "flashback" | "update_rollback" | "pitr" |
	// "tx_rollback".
	Type string `json:"type"`
	// Mode selects the execution granularity: "meta" | "sql" | "selected".
	Mode   string        `json:"mode"`
	Filter binlog.Filter `json:"filter"`
	// Status is the current state machine state (created/scanning/ready/...).
	Status OperationState `json:"status"`
	// CreatedBy is the user ID that started the operation.
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	// SelectedTxIDs are the transactions chosen during the ready phase. The
	// preview transaction list itself is not persisted — it can be regenerated
	// by a re-scan; only the selected transactions (and their statements) are
	// stored.
	SelectedTxIDs []string `json:"selectedTxIds,omitempty"`
	// Statements are the persisted per-transaction statements (filled after
	// selection, and loaded by Get).
	Statements []Statement `json:"statements,omitempty"`
}

// Statement is a single reverse-SQL statement of a selected transaction.
type Statement struct {
	TxIndex   int    `json:"txIndex"`
	StmtIndex int    `json:"stmtIndex"`
	SQL       string `json:"sql"`
	TxID      string `json:"txId,omitempty"`
	TxOrder   int    `json:"txOrder"`
	Warnings  []string `json:"warnings,omitempty"`
	// Status is the statement execution status: "pending" | "approved" |
	// "executed" | "error".
	Status string `json:"status"`
}

// OperationStore is the v3 persistence contract for PITR operations.
type OperationStore interface {
	Create(op *Operation) error
	Get(id string) (*Operation, error)
	// Update persists the whole operation record (state/field update).
	Update(op *Operation) error
	// UpdateIfStatus is a compare-and-swap state transition: it applies
	// `op.Status` only while the persisted status still equals `from`, so a
	// concurrent transition can never be silently overwritten. It reports
	// whether the update was applied (RowsAffected==1); false means another
	// actor already moved the operation.
	UpdateIfStatus(op *Operation, from OperationState) (bool, error)
	ListByOrg(orgID string) ([]*Operation, error)
	ListByAgent(agentID string) ([]*Operation, error)
	// ListByStatus returns all operations currently in the given state
	// (statement rows not included; use Get for the full record). Used by the
	// disconnect handler and startup reconcile to find in-flight operations.
	ListByStatus(status OperationState) ([]*Operation, error)
	// SaveStatements replaces all stored statements of an operation with the
	// given set (selection re-runs overwrite previous selections).
	SaveStatements(opID string, stmts []Statement) error
	LoadStatements(opID string) ([]Statement, error)
	// SaveCheckpoint upserts the operation's execution checkpoint (server-side
	// double-write of the agent's per-batch progress, keyed by op_id in the
	// checkpoints table). errorsJSON is the JSON-encoded executor error list.
	SaveCheckpoint(opID string, lastStmt, total int, errorsJSON string) error
}
