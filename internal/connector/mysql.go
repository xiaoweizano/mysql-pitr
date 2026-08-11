package connector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/executor"
)

// compile-time checks that MySQLConnector satisfies Connector, executor.DB and
// collector.MySQLInfo (归档循环的 MySQL 交互抽象).
var _ Connector = (*MySQLConnector)(nil)
var _ executor.DB = (*MySQLConnector)(nil)
var _ collector.MySQLInfo = (*MySQLConnector)(nil)

// MySQLConnector implements the Connector interface for MySQL 8.0+ databases.
type MySQLConnector struct {
	db     *sql.DB
	config ConnConfig
}

// NewMySQLConnector returns a new MySQLConnector with no active connection.
// Call Connect before using the connector.
func NewMySQLConnector() *MySQLConnector {
	return &MySQLConnector{}
}

// NewMySQLConnectorWithDB returns a MySQLConnector backed by an existing
// *sql.DB. This is primarily useful for testing with sqlmock.
func NewMySQLConnectorWithDB(db *sql.DB) *MySQLConnector {
	return &MySQLConnector{db: db}
}

// ---------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------

func (m *MySQLConnector) Connect(cfg ConnConfig) error {
	host := cfg.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := cfg.Port
	if port == 0 {
		port = 3306
	}

	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = cfg.User
	mysqlCfg.Passwd = cfg.Password
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = fmt.Sprintf("%s:%d", host, port)
	mysqlCfg.DBName = cfg.Database
	mysqlCfg.ParseTime = true
	mysqlCfg.Loc = time.Local
	mysqlCfg.Collation = "utf8mb4_general_ci"

	// Apply any extra user-supplied parameters.
	if cfg.Params != nil {
		for k, v := range cfg.Params {
			switch k {
			case "tls":
				mysqlCfg.TLSConfig = v
			case "timeout":
				d, err := time.ParseDuration(v)
				if err == nil {
					mysqlCfg.Timeout = d
				}
			case "readTimeout":
				d, err := time.ParseDuration(v)
				if err == nil {
					mysqlCfg.ReadTimeout = d
				}
			case "writeTimeout":
				d, err := time.ParseDuration(v)
				if err == nil {
					mysqlCfg.WriteTimeout = d
				}
			case "charset":
				mysqlCfg.Collation = v
			}
		}
	}

	db, err := sql.Open("mysql", mysqlCfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("connector: sql.Open: %w", err)
	}

	// Verify the connection is alive.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("connector: ping failed: %w", err)
	}

	m.db = db
	m.config = cfg
	return nil
}

// ---------------------------------------------------------------------------
// GetBinlogFiles
// ---------------------------------------------------------------------------

func (m *MySQLConnector) GetBinlogFiles(ctx context.Context) ([]BinlogFile, error) {
	if m.db == nil {
		return nil, fmt.Errorf("connector: not connected")
	}

	rows, err := m.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("connector: SHOW BINARY LOGS: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("connector: columns: %w", err)
	}

	// Determine which columns are available (MySQL 8.0.14+ adds Encrypted).
	hasEncrypted := false
	for _, c := range columns {
		if strings.EqualFold(c, "Encrypted") {
			hasEncrypted = true
			break
		}
	}

	var files []BinlogFile
	for rows.Next() {
		var name string
		var size int64
		var encrypted string

		if hasEncrypted {
			if err := rows.Scan(&name, &size, &encrypted); err != nil {
				return nil, fmt.Errorf("connector: scan: %w", err)
			}
		} else {
			if err := rows.Scan(&name, &size); err != nil {
				return nil, fmt.Errorf("connector: scan: %w", err)
			}
		}

		files = append(files, BinlogFile{
			Name: name,
			Size: size,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("connector: rows iteration: %w", err)
	}

	return files, nil
}

// ---------------------------------------------------------------------------
// collector.MySQLInfo adapter
// ---------------------------------------------------------------------------

// ListBinlogs 适配 GetBinlogFiles 到 collector.MySQLInfo 的 ListBinlogs 契约
// （SHOW BINARY LOGS → archive.ManifestFile 列表）。
func (m *MySQLConnector) ListBinlogs(ctx context.Context) ([]archive.ManifestFile, error) {
	files, err := m.GetBinlogFiles(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]archive.ManifestFile, 0, len(files))
	for _, f := range files {
		out = append(out, archive.ManifestFile{Name: f.Name, Size: f.Size})
	}
	return out, nil
}

// MasterPosition 查询 SHOW MASTER STATUS 返回当前 binlog 文件名与位置
// （归档循环的续拉起点）。binlog 未启用时 SHOW MASTER STATUS 返回空结果集
// （sql.ErrNoRows），转成可读错误。
func (m *MySQLConnector) MasterPosition(ctx context.Context) (gomysql.Position, error) {
	if m.db == nil {
		return gomysql.Position{}, fmt.Errorf("connector: not connected")
	}
	var name string
	var pos uint32
	err := m.db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&name, &pos)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return gomysql.Position{}, fmt.Errorf("connector: SHOW MASTER STATUS returned no rows (binary logging may be disabled)")
		}
		return gomysql.Position{}, fmt.Errorf("connector: SHOW MASTER STATUS: %w", err)
	}
	if name == "" {
		return gomysql.Position{}, fmt.Errorf("connector: SHOW MASTER STATUS returned empty binlog file name")
	}
	return gomysql.Position{Name: name, Pos: pos}, nil
}

// ---------------------------------------------------------------------------
// GetBinlogDir
// ---------------------------------------------------------------------------

func (m *MySQLConnector) GetBinlogDir(ctx context.Context) (string, error) {
	if m.db == nil {
		return "", fmt.Errorf("connector: not connected")
	}

	var name, value string
	if err := m.db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'log_bin_basename'").Scan(&name, &value); err != nil {
		return "", fmt.Errorf("connector: query log_bin_basename: %w", err)
	}
	if value == "" {
		return "", fmt.Errorf("connector: log_bin_basename is empty")
	}

	dir := filepath.Dir(value)
	if dir != "." {
		return dir + string(filepath.Separator), nil
	}
	return "", nil
}

// ---------------------------------------------------------------------------
// FetchSchema
// ---------------------------------------------------------------------------

// FetchSchema queries information_schema.COLUMNS for the table's column
// metadata (name, type, nullability, auto-increment flag) and
// information_schema.KEY_COLUMN_USAGE for the primary key column order. The
// binlog scanner uses it to attach column names to RowChange events when the
// binlog itself doesn't carry them.
func (m *MySQLConnector) FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error) {
	if m.db == nil {
		return binlog.TableSchema{}, fmt.Errorf("connector: not connected")
	}

	rows, err := m.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES', EXTRA LIKE '%auto_increment%'
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, schema, table)
	if err != nil {
		return binlog.TableSchema{}, fmt.Errorf("connector: query schema: %w", err)
	}
	defer rows.Close()

	var cols []binlog.ColumnDef
	for rows.Next() {
		var col binlog.ColumnDef
		// IS_NULLABLE = 'YES' and EXTRA LIKE '%auto_increment%' are integer
		// expressions; go-sql-driver/mysql's prepared-statement binary protocol
		// returns them as int64 (0/1), so scan into int64 rather than bool.
		var nullable, autoInc int64
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &autoInc); err != nil {
			return binlog.TableSchema{}, fmt.Errorf("connector: scan column: %w", err)
		}
		col.Nullable = nullable != 0
		col.IsAutoInc = autoInc != 0
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return binlog.TableSchema{}, fmt.Errorf("connector: rows iteration: %w", err)
	}
	if len(cols) == 0 {
		return binlog.TableSchema{}, fmt.Errorf("connector: table %s.%s not found", schema, table)
	}

	schemaOut := binlog.TableSchema{Schema: schema, Table: table, Columns: cols}
	schemaOut.PrimaryKey, err = m.fetchPrimaryKey(ctx, schema, table)
	if err != nil {
		return binlog.TableSchema{}, err
	}
	return schemaOut, nil
}

// fetchPrimaryKey queries information_schema.KEY_COLUMN_USAGE for the table's
// primary key column names in key-definition order. A table without a primary
// key yields an empty (nil) slice.
func (m *MySQLConnector) fetchPrimaryKey(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("connector: query primary key: %w", err)
	}
	defer rows.Close()

	var pk []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("connector: scan primary key: %w", err)
		}
		pk = append(pk, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("connector: rows iteration: %w", err)
	}
	return pk, nil
}

// ---------------------------------------------------------------------------
// GetBasedir
// ---------------------------------------------------------------------------

func (m *MySQLConnector) GetBasedir(ctx context.Context) (string, error) {
	if m.db == nil {
		return "", fmt.Errorf("connector: not connected")
	}
	var name, basedir string
	if err := m.db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'basedir'").Scan(&name, &basedir); err != nil {
		return "", fmt.Errorf("connector: query basedir: %w", err)
	}
	if basedir == "" {
		return "", fmt.Errorf("connector: basedir is empty")
	}
	return basedir, nil
}

// ---------------------------------------------------------------------------
// ParseBinlog verifies that the requested binlog files exist and returns any
// parse errors. Actual event-level parsing is delegated to the parser package
// (Task 2); this method acts as a thin orchestration layer.
func (m *MySQLConnector) ParseBinlog(ctx context.Context, req ParseRequest) (*ParseResult, error) {
	if m.db == nil {
		return nil, fmt.Errorf("connector: not connected")
	}
	if len(req.BinlogFiles) == 0 {
		return nil, fmt.Errorf("connector: no binlog files specified")
	}

	// Verify requested files exist on the server.
	available, err := m.GetBinlogFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("connector: cannot list binlog files: %w", err)
	}

	availableSet := make(map[string]bool, len(available))
	for _, f := range available {
		availableSet[f.Name] = true
	}

	var missing []string
	for _, f := range req.BinlogFiles {
		if !availableSet[f] {
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("connector: binlog files not found: %s", strings.Join(missing, ", "))
	}

	// The actual parsing is handled by the parser package.
	// This connector method will be updated in Task 2 to invoke the parser.
	return &ParseResult{
		Events:    []RowEvent{},
		TotalRows: 0,
	}, nil
}

// ---------------------------------------------------------------------------
// ExecuteRollback
// ---------------------------------------------------------------------------

func (m *MySQLConnector) ExecuteRollback(ctx context.Context, sqls []string, opts ExecOptions) (*ExecResult, error) {
	if m.db == nil {
		return nil, fmt.Errorf("connector: not connected")
	}
	if len(sqls) == 0 {
		return &ExecResult{}, nil
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 100 // default batch size
	}

	result := &ExecResult{}

	if opts.DryRun {
		// Dry-run mode: count but do not execute.
		result.RowsAffected = 0
		result.BatchesCompleted = 0
		return result, nil
	}

	// Execute in batches.
	for i := 0; i < len(sqls); i += batchSize {
		end := i + batchSize
		if end > len(sqls) {
			end = len(sqls)
		}

		batch := sqls[i:end]
		if err := m.executeBatch(ctx, batch); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("batch %d: %v", result.BatchesCompleted, err))
			// Continue with remaining batches despite error.
			continue
		}
		result.BatchesCompleted++
	}

	return result, nil
}

func (m *MySQLConnector) executeBatch(ctx context.Context, batch []string) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback() // no-op if already committed
	}()

	for _, stmt := range batch {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", truncate(stmt, 80), err)
		}
	}

	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Preflight
// ---------------------------------------------------------------------------

func (m *MySQLConnector) Preflight(ctx context.Context) (*PreflightResult, error) {
	if m.db == nil {
		return nil, fmt.Errorf("connector: not connected")
	}

	res := &PreflightResult{
		Status: PreflightPass,
		Checks: make([]PreflightCheck, 0, 7),
	}

	// 1. MySQL version.
	verCheck := m.checkVersion(ctx)
	res.Checks = append(res.Checks, verCheck)
	if verCheck.Status == PreflightFail {
		res.Status = PreflightFail
	}

	// 2. Binary log format must be ROW.
	bfCheck := m.checkBinlogFormat(ctx)
	res.Checks = append(res.Checks, bfCheck)
	if bfCheck.Status == PreflightFail {
		res.Status = PreflightFail
	}

	// 3. Binary logging must be enabled.
	lbCheck := m.checkLogBin(ctx)
	res.Checks = append(res.Checks, lbCheck)
	if lbCheck.Status == PreflightFail {
		res.Status = PreflightFail
	}

	// 4. User privileges.
	privCheck := m.checkPrivileges(ctx)
	res.Checks = append(res.Checks, privCheck)
	if privCheck.Status == PreflightFail {
		res.Status = PreflightFail
	}

	// 5. Database size estimate.
	sizeCheck := m.checkDatabaseSize(ctx)
	res.Checks = append(res.Checks, sizeCheck)

	// 6. Column metadata for configured database.
	colCheck := m.checkColumnMetadata(ctx)
	res.Checks = append(res.Checks, colCheck)

	// 7. Foreign-key relationships.
	fkCheck := m.checkForeignKeys(ctx)
	res.Checks = append(res.Checks, fkCheck)

	// Promote version string from check message to top-level field.
	if verCheck.Status == PreflightPass {
		res.Version = verCheck.Message
		res.Checks[0] = PreflightCheck{
			Name:    verCheck.Name,
			Status:  verCheck.Status,
			Message: "",
		}
	} else {
		res.Version = ""
	}

	return res, nil
}

func (m *MySQLConnector) checkVersion(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "MySQL Version"}
	var version string
	if err := m.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("cannot query version: %v", err)
		return c
	}

	// Require 8.0 or later.
	major, _, _ := parseVersion(version)
	if major < 8 {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("MySQL %s detected; MySQL 8.0+ required", version)
		return c
	}

	c.Status = PreflightPass
	c.Message = version // Store raw version string for the caller
	return c
}

func (m *MySQLConnector) checkBinlogFormat(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "Binlog Format"}
	var variableName, value string
	err := m.db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'binlog_format'").Scan(&variableName, &value)
	if err != nil {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("cannot query binlog_format: %v", err)
		return c
	}
	if strings.ToUpper(value) != "ROW" {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("binlog_format is %s; ROW required for PITR", value)
		return c
	}
	c.Status = PreflightPass
	c.Message = "binlog_format is ROW"
	return c
}

func (m *MySQLConnector) checkLogBin(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "Binary Logging Enabled"}
	var variableName, value string
	err := m.db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'log_bin'").Scan(&variableName, &value)
	if err != nil {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("cannot query log_bin: %v", err)
		return c
	}
	if strings.ToUpper(value) != "ON" && value != "1" {
		c.Status = PreflightFail
		c.Message = "binary logging is not enabled (log_bin is OFF)"
		return c
	}
	c.Status = PreflightPass
	c.Message = "binary logging is ON"
	return c
}

func (m *MySQLConnector) checkPrivileges(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "User Privileges"}
	rows, err := m.db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("cannot query grants: %v", err)
		return c
	}
	defer rows.Close()

	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			c.Status = PreflightFail
			c.Message = fmt.Sprintf("cannot scan grant: %v", err)
			return c
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("rows iteration: %v", err)
		return c
	}

	missing := m.checkRequiredPrivileges(grants)
	if len(missing) > 0 {
		c.Status = PreflightWarn
		c.Message = fmt.Sprintf("missing recommended privileges: %s", strings.Join(missing, ", "))
	} else {
		c.Status = PreflightPass
		c.Message = "required privileges granted"
	}
	return c
}

func (m *MySQLConnector) checkRequiredPrivileges(grants []string) []string {
	var missing []string
	required := []string{"SELECT", "REPLICATION SLAVE", "REPLICATION CLIENT"}
	grantText := strings.Join(grants, " ")
	grantUpper := strings.ToUpper(grantText)

	for _, priv := range required {
		if !strings.Contains(grantUpper, priv) {
			// ALL PRIVILEGES covers everything.
			if !strings.Contains(grantUpper, "ALL PRIVILEGES") &&
				!strings.Contains(grantUpper, "ALL") {
				missing = append(missing, priv)
			}
		}
	}
	return missing
}

func (m *MySQLConnector) checkDatabaseSize(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "Database Size"}
	dbName := m.config.Database
	if dbName == "" {
		c.Status = PreflightWarn
		c.Message = "no database configured; skipping size check"
		return c
	}

	var size sql.NullInt64
	err := m.db.QueryRowContext(ctx,
		`SELECT SUM(data_length + index_length)
		   FROM information_schema.tables
		  WHERE table_schema = ?`, dbName).Scan(&size)
	if err != nil {
		c.Status = PreflightWarn
		c.Message = fmt.Sprintf("cannot estimate database size: %v", err)
		return c
	}

	if size.Valid {
		c.Status = PreflightPass
		c.Message = fmt.Sprintf("estimated database size: %d bytes (%s)", size.Int64, formatBytes(size.Int64))
	} else {
		c.Status = PreflightWarn
		c.Message = "database appears empty or has no tables"
	}
	return c
}

func (m *MySQLConnector) checkColumnMetadata(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "Column Metadata"}
	dbName := m.config.Database
	if dbName == "" {
		c.Status = PreflightWarn
		c.Message = "no database configured; skipping column metadata check"
		return c
	}

	rows, err := m.db.QueryContext(ctx,
		`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
		   FROM information_schema.COLUMNS
		  WHERE table_schema = ?
		  ORDER BY TABLE_NAME, ORDINAL_POSITION`, dbName)
	if err != nil {
		c.Status = PreflightFail
		c.Message = fmt.Sprintf("cannot query column metadata: %v", err)
		return c
	}
	defer rows.Close()

	tableCount := 0
	columnCount := 0
	lastTable := ""
	for rows.Next() {
		var tableName, colName, dataType, isNullable, colKey string
		if err := rows.Scan(&tableName, &colName, &dataType, &isNullable, &colKey); err != nil {
			continue
		}
		if tableName != lastTable {
			tableCount++
			lastTable = tableName
		}
		columnCount++
	}

	c.Status = PreflightPass
	c.Message = fmt.Sprintf("found %d tables with %d columns in database %q", tableCount, columnCount, dbName)
	return c
}

func (m *MySQLConnector) checkForeignKeys(ctx context.Context) PreflightCheck {
	c := PreflightCheck{Name: "Foreign Key Dependencies"}
	dbName := m.config.Database
	if dbName == "" {
		c.Status = PreflightWarn
		c.Message = "no database configured; skipping FK check"
		return c
	}

	rows, err := m.db.QueryContext(ctx,
		`SELECT rc.CONSTRAINT_NAME, rc.TABLE_NAME, rc.REFERENCED_TABLE_NAME,
		        kcu.COLUMN_NAME, kcu.REFERENCED_COLUMN_NAME
		   FROM information_schema.REFERENTIAL_CONSTRAINTS rc
		   JOIN information_schema.KEY_COLUMN_USAGE kcu
		     ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		    AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
		  WHERE rc.CONSTRAINT_SCHEMA = ?
		  ORDER BY rc.TABLE_NAME`, dbName)
	if err != nil {
		c.Status = PreflightWarn
		c.Message = fmt.Sprintf("cannot query FK constraints: %v", err)
		return c
	}
	defer rows.Close()

	var fkCount int
	for rows.Next() {
		fkCount++
	}

	if fkCount > 0 {
		c.Status = PreflightPass
		c.Message = fmt.Sprintf("found %d foreign key constraint(s) in database %q", fkCount, dbName)
	} else {
		c.Status = PreflightPass
		c.Message = fmt.Sprintf("no foreign key constraints in database %q", dbName)
	}
	return c
}

// ---------------------------------------------------------------------------
// executor.DB adapter
// ---------------------------------------------------------------------------

// AsDB exposes the connector's underlying *sql.DB as an executor.DB so the
// flashback CLI can drive batched SQL execution over the same live connection
// used for schema fetching and binlog listing. MySQLConnector implements
// executor.DB directly via Exec/Begin plus its existing Close.
func (m *MySQLConnector) AsDB() executor.DB { return m }

// Exec runs a single SQL statement and wraps the resulting sql.Result for the
// executor package.
func (m *MySQLConnector) Exec(query string, args ...interface{}) (executor.Result, error) {
	if m.db == nil {
		return nil, fmt.Errorf("connector: not connected")
	}
	res, err := m.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return &sqlResultAdapter{res: res}, nil
}

// Begin starts a transaction and wraps it for the executor package.
func (m *MySQLConnector) Begin() (executor.Tx, error) {
	if m.db == nil {
		return nil, fmt.Errorf("connector: not connected")
	}
	tx, err := m.db.Begin()
	if err != nil {
		return nil, err
	}
	return &sqlTxAdapter{tx: tx}, nil
}

// sqlResultAdapter adapts sql.Result to executor.Result.
type sqlResultAdapter struct{ res sql.Result }

func (a *sqlResultAdapter) LastInsertId() (int64, error) { return a.res.LastInsertId() }
func (a *sqlResultAdapter) RowsAffected() (int64, error) { return a.res.RowsAffected() }

// sqlTxAdapter adapts sql.Tx to executor.Tx.
type sqlTxAdapter struct{ tx *sql.Tx }

func (a *sqlTxAdapter) Exec(query string, args ...interface{}) (executor.Result, error) {
	res, err := a.tx.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return &sqlResultAdapter{res: res}, nil
}

func (a *sqlTxAdapter) Commit() error   { return a.tx.Commit() }
func (a *sqlTxAdapter) Rollback() error { return a.tx.Rollback() }

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func (m *MySQLConnector) Close() error {
	if m.db != nil {
		err := m.db.Close()
		m.db = nil
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// parseVersion extracts major, minor from a MySQL version string such as
// "8.0.32" or "5.7.38-log".
func parseVersion(v string) (int, int, int) {
	// Strip suffix after first non-digit, non-dot character.
	idx := strings.IndexFunc(v, func(r rune) bool {
		return r != '.' && (r < '0' || r > '9')
	})
	if idx > 0 {
		v = v[:idx]
	}

	parts := strings.SplitN(v, ".", 4)
	var maj, min, patch int
	if len(parts) > 0 {
		_, _ = fmt.Sscanf(parts[0], "%d", &maj)
	}
	if len(parts) > 1 {
		_, _ = fmt.Sscanf(parts[1], "%d", &min)
	}
	if len(parts) > 2 {
		_, _ = fmt.Sscanf(parts[2], "%d", &patch)
	}
	return maj, min, patch
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
