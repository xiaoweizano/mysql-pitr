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
	ListByOrg(orgID string) ([]*Operation, error)
	ListByAgent(agentID string) ([]*Operation, error)
	// SaveStatements replaces all stored statements of an operation with the
	// given set (selection re-runs overwrite previous selections).
	SaveStatements(opID string, stmts []Statement) error
	LoadStatements(opID string) ([]Statement, error)
}
