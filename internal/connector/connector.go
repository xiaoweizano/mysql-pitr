package connector

import (
	"context"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/executor"
)

// Connector defines the database connectivity interface for the MySQL PITR
// (Point-In-Time Recovery) flashback system.
//
// Implementations are responsible for connecting to a MySQL server, listing
// binary logs, delegating binlog parsing, executing rollback statements, and
// running pre-flight readiness checks.
type Connector interface {
	// Connect establishes a connection to the database using the provided config.
	Connect(cfg ConnConfig) error

	// GetBinlogFiles lists the binary log files currently available on the server.
	GetBinlogFiles(ctx context.Context) ([]BinlogFile, error)

	// ParseBinlog parses the given binlog files and returns row events that match
	// the supplied request (table filter, time range, etc.).
	//
	// The actual parsing work is delegated to the parser package; this method
	// orchestrates reading the raw bytes from the server and handing them off.
	ParseBinlog(ctx context.Context, req ParseRequest) (*ParseResult, error)

	// ExecuteRollback applies the provided SQL statements as a rollback. The
	// behaviour is governed by opts (batch size, dry-run flag).
	ExecuteRollback(ctx context.Context, sqls []string, opts ExecOptions) (*ExecResult, error)

	// GetBinlogDir returns the filesystem directory containing binary log files
	// by querying the MySQL server variable `log_bin_basename`.
	GetBinlogDir(ctx context.Context) (string, error)

	// Preflight runs a suite of readiness checks against the database and returns
	// a consolidated result. Checks include MySQL version, binlog format, user
	// privileges, disk space, column metadata, and foreign-key dependencies.
	Preflight(ctx context.Context) (*PreflightResult, error)

	// FetchSchema returns the column metadata for the given table, used by the
	// binlog scanner to attach column names to row events when the binlog itself
	// doesn't carry them.
	FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error)

	// AsDB exposes the connector's live database connection as an executor.DB
	// so it can drive batched flashback SQL execution (Task 15).
	AsDB() executor.DB

	// Close closes the underlying database connection and releases resources.
	Close() error
}
