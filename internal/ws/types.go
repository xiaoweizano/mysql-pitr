package ws

import "encoding/json"

// Command type constants. Each represents a distinct operation that the
// platform can request from an agent, or that an agent can send to the
// platform.
const (
	CmdPreflight    = "preflight"
	CmdPITRParse    = "pitr_parse"
	CmdPITRExecute  = "pitr_execute"
	CmdStatus       = "status"
	CmdShutdown     = "shutdown"
	CmdCertRenewal  = "cert_renewal"
	CmdPITRProgress = "pitr_progress"
	CmdPITRCancel   = "pitr_cancel"
)

// Response status constants.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Command is a request sent over the WebSocket connection. Cmd holds a unique
// identifier (typically a UUID) used to correlate requests with responses.
// Type selects the handler on the receiving side. Params carries
// operation-specific payload.
type Command struct {
	Cmd    string                 `json:"cmd"`
	Type   string                 `json:"type"`
	Params map[string]interface{} `json:"params"`
}

// Response carries the result of a command execution. Cmd echoes the
// identifier from the corresponding Command so the caller can correlate them.
// Status is either "ok" or "error".
type Response struct {
	Cmd    string      `json:"cmd"`
	Status string      `json:"status"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// Extended command types for scan / execute / resume / cancel and archive
// status operations.
const (
	CmdScan          = "scan"
	CmdExecute       = "execute"
	CmdResume        = "resume"
	CmdCancel        = "cancel"
	CmdArchiveStatus = "archive_status"
)

// ScanRequest is the scan command params (wire form, mirrors binlog.Filter).
type ScanRequest struct {
	Filter     ScanFilter `json:"filter"`
	Mode       string     `json:"mode"` // "meta" | "sql" | "selected"
	MaxPreview int        `json:"maxPreview"`
}

type ScanFilter struct {
	Tables        []TableRefJSON `json:"tables,omitempty"`
	TimeStart     string         `json:"timeStart,omitempty"` // RFC3339
	TimeEnd       string         `json:"timeEnd,omitempty"`
	GTIDSet       string         `json:"gtidSet,omitempty"`
	StartFile     string         `json:"startFile,omitempty"`
	StartPos      uint32         `json:"startPos,omitempty"`
	EndFile       string         `json:"endFile,omitempty"`
	EndPos        uint32         `json:"endPos,omitempty"`
	MaxRowsPerTx  int            `json:"maxRowsPerTx,omitempty"`
	SelectedTxIDs []string       `json:"selectedTxIds,omitempty"`
}

// TableRefJSON identifies a table by schema and table name.
type TableRefJSON struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// ExecuteRequest is the execute command params.
type ExecuteRequest struct {
	OperationID string          `json:"operationId"`
	Statements  []StatementWire `json:"statements"`
	BatchSize   int             `json:"batchSize,omitempty"`
}

type StatementWire struct {
	SQL      string   `json:"sql"`
	TxID     string   `json:"txId"`
	TxOrder  int      `json:"txOrder"`
	Warnings []string `json:"warnings,omitempty"`
}

// StreamEvent is an agent→server streaming message (scan tx/SQL, execution
// progress, operation completion).
type StreamEvent struct {
	ID   string          `json:"id"`   // corresponds to the command ID
	Kind string          `json:"kind"` // "tx_meta" | "sql" | "scan_done" | "progress" | "op_done" | "op_error"
	Data json.RawMessage `json:"data"`
}

const (
	EvTxMeta   = "tx_meta"
	EvSQL      = "sql"
	EvScanDone = "scan_done"
	EvProgress = "progress"
	EvOpDone   = "op_done"
	EvOpError  = "op_error"
)
