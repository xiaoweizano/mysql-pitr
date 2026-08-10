package connector

import (
	"fmt"
	"time"
)

// ConnConfig holds database connection configuration for the MySQL PITR system.
type ConnConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	// Params holds additional DSN parameters (e.g., TLS config, timeouts).
	Params map[string]string
}

// DSN returns the MySQL Data Source Name string derived from the config.
func (c ConnConfig) DSN() string {
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 3306
	}
	dsn := c.User + ":" + c.Password + "@tcp(" + host + ":" + itoa(port) + ")/" + c.Database

	// Build query parameters.
	var params string
	if c.Params != nil && len(c.Params) > 0 {
		first := true
		for k, v := range c.Params {
			if first {
				params += "?" + k + "=" + v
				first = false
			} else {
				params += "&" + k + "=" + v
			}
		}
	}
	return dsn + params
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// BinlogFile represents a single MySQL binary log file on the server.
type BinlogFile struct {
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	Earliest time.Time `json:"earliest,omitempty"` // May be zero if unavailable from SHOW BINARY LOGS
}

// ParseRequest specifies which binlog segments to parse and what to look for.
type ParseRequest struct {
	BinlogFiles []string  `json:"binlogFiles"`
	TargetTable string    `json:"targetTable,omitempty"` // Schema-qualified, e.g. "mydb.orders"
	StartTime   time.Time `json:"startTime,omitempty"`
	EndTime     time.Time `json:"endTime,omitempty"`
	StartPos    uint32    `json:"startPos,omitempty"` // Starting position in the first binlog
	StopPos     uint32    `json:"stopPos,omitempty"`  // Stopping position in the last binlog
}

// ParseResult holds all row events recovered from binlog parsing.
type ParseResult struct {
	Events    []RowEvent `json:"events"`
	TotalRows int64      `json:"totalRows"`
	Errors    []string   `json:"errors,omitempty"`
}

// RowEvent describes a single row mutation recorded in the binary log.
type RowEvent struct {
	Type       EventType              `json:"type"`
	Database   string                 `json:"database"`
	Table      string                 `json:"table"`
	Timestamp  time.Time              `json:"timestamp"`
	Before     map[string]interface{} `json:"before,omitempty"`     // Pre-image (UPDATE, DELETE)
	After      map[string]interface{} `json:"after,omitempty"`      // Post-image (INSERT, UPDATE)
	PrimaryKey map[string]interface{} `json:"primaryKey,omitempty"` // Convenience copy of PK columns
}

// EventType classifies the kind of row change.
type EventType string

const (
	InsertEvent EventType = "INSERT"
	UpdateEvent EventType = "UPDATE"
	DeleteEvent EventType = "DELETE"
)

// ExecOptions controls how rollback SQL is applied.
type ExecOptions struct {
	BatchSize int  `json:"batchSize"` // Number of SQL statements per batch
	DryRun    bool `json:"dryRun"`    // When true, log SQL without executing
}

// ExecResult summarises the outcome of a rollback execution.
type ExecResult struct {
	RowsAffected     int64    `json:"rowsAffected"`
	BatchesCompleted int      `json:"batchesCompleted"`
	Errors           []string `json:"errors,omitempty"`
}

// PreflightResult captures the outcome of pre-flight readiness checks.
type PreflightResult struct {
	Status  PreflightStatus  `json:"status"`
	Version string           `json:"version"`
	Checks  []PreflightCheck `json:"checks"`
}

// EnsureOK returns an error when the preflight result is FAIL, so callers can
// abort a flashback operation before touching any data. WARN does not block.
func (r PreflightResult) EnsureOK() error {
	if r.Status == PreflightFail {
		return fmt.Errorf("preflight FAILED")
	}
	return nil
}

// PreflightStatus indicates the overall preflight health.
type PreflightStatus string

const (
	PreflightPass PreflightStatus = "PASS"
	PreflightWarn PreflightStatus = "WARN"
	PreflightFail PreflightStatus = "FAIL"
)

// PreflightCheck represents a single readiness check.
type PreflightCheck struct {
	Name    string          `json:"name"`
	Status  PreflightStatus `json:"status"`
	Message string          `json:"message,omitempty"`
}
