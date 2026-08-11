package connector

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newMockConnector(t *testing.T) (*MySQLConnector, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewMySQLConnectorWithDB(db)
	t.Cleanup(func() { _ = c.Close() })
	return c, mock
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func TestNewMySQLConnector(t *testing.T) {
	c := NewMySQLConnector()
	assert.NotNil(t, c)
	assert.Nil(t, c.db)
}

func TestNewMySQLConnectorWithDB(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	c := NewMySQLConnectorWithDB(db)
	assert.NotNil(t, c)
	assert.Equal(t, db, c.db)
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestClose_NilDB(t *testing.T) {
	c := NewMySQLConnector()
	err := c.Close()
	assert.NoError(t, err)
}

func TestClose_WithDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()

	c := NewMySQLConnectorWithDB(db)
	err = c.Close()
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// GetBinlogFiles
// ---------------------------------------------------------------------------

func TestGetBinlogFiles_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.GetBinlogFiles(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestGetBinlogFiles_Success(t *testing.T) {
	c, mock := newMockConnector(t)

	rows := sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("mysql-bin.000001", int64(1024)).
		AddRow("mysql-bin.000002", int64(2048))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW BINARY LOGS")).WillReturnRows(rows)

	files, err := c.GetBinlogFiles(context.Background())
	require.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, "mysql-bin.000001", files[0].Name)
	assert.Equal(t, int64(1024), files[0].Size)
	assert.Equal(t, "mysql-bin.000002", files[1].Name)
	assert.Equal(t, int64(2048), files[1].Size)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetBinlogFiles_WithEncryptedColumn(t *testing.T) {
	c, mock := newMockConnector(t)

	rows := sqlmock.NewRows([]string{"Log_name", "File_size", "Encrypted"}).
		AddRow("mysql-bin.000001", int64(512), "NO")
	mock.ExpectQuery(regexp.QuoteMeta("SHOW BINARY LOGS")).WillReturnRows(rows)

	files, err := c.GetBinlogFiles(context.Background())
	require.NoError(t, err)
	assert.Len(t, files, 1)
	assert.Equal(t, "mysql-bin.000001", files[0].Name)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetBinlogFiles_QueryError(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SHOW BINARY LOGS")).
		WillReturnError(errors.New("access denied"))

	_, err := c.GetBinlogFiles(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SHOW BINARY LOGS")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// ParseBinlog
// ---------------------------------------------------------------------------

func TestParseBinlog_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.ParseBinlog(context.Background(), ParseRequest{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestParseBinlog_NoFiles(t *testing.T) {
	c, _ := newMockConnector(t)
	_, err := c.ParseBinlog(context.Background(), ParseRequest{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no binlog files")
}

func TestParseBinlog_FilesMissing(t *testing.T) {
	c, mock := newMockConnector(t)

	rows := sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("mysql-bin.000001", int64(1024))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW BINARY LOGS")).WillReturnRows(rows)

	_, err := c.ParseBinlog(context.Background(), ParseRequest{
		BinlogFiles: []string{"mysql-bin.999999"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog files not found")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParseBinlog_Success(t *testing.T) {
	c, mock := newMockConnector(t)

	rows := sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("mysql-bin.000001", int64(1024))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW BINARY LOGS")).WillReturnRows(rows)

	result, err := c.ParseBinlog(context.Background(), ParseRequest{
		BinlogFiles: []string{"mysql-bin.000001"},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(0), result.TotalRows)
	assert.Empty(t, result.Events)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// FetchSchema
// ---------------------------------------------------------------------------

func TestFetchSchema_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.FetchSchema(context.Background(), "mydb", "orders")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestFetchSchema_Success(t *testing.T) {
	c, mock := newMockConnector(t)

	// int64 values mirror the real MySQL binary protocol: with placeholders,
	// the IS_NULLABLE = 'YES' / EXTRA LIKE '%auto_increment%' expressions come
	// back as 1/0, never as bool (see TestFetchSchema_Success_Int64ExpressionValues).
	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE = 'YES'", "EXTRA LIKE '%auto_increment%'"}).
		AddRow("id", "int", int64(1), int64(1)).
		AddRow("amount", "decimal", int64(1), int64(0)).
		AddRow("user_id", "bigint", int64(0), int64(0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES', EXTRA LIKE '%auto_increment%' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnRows(rows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("id").
			AddRow("user_id"))

	schema, err := c.FetchSchema(context.Background(), "mydb", "orders")
	require.NoError(t, err)
	assert.Equal(t, "mydb", schema.Schema)
	assert.Equal(t, "orders", schema.Table)
	require.Len(t, schema.Columns, 3)
	assert.Equal(t, "id", schema.Columns[0].Name)
	assert.Equal(t, "int", schema.Columns[0].Type)
	assert.True(t, schema.Columns[0].Nullable)
	assert.True(t, schema.Columns[0].IsAutoInc)
	assert.Equal(t, "amount", schema.Columns[1].Name)
	assert.False(t, schema.Columns[1].IsAutoInc)
	assert.Equal(t, "user_id", schema.Columns[2].Name)
	assert.False(t, schema.Columns[2].Nullable)
	assert.Equal(t, []string{"id", "user_id"}, schema.PrimaryKey)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFetchSchema_Success_Int64ExpressionValues is a regression test for the
// go-sql-driver/mysql binary protocol: with placeholders, information_schema
// expressions such as IS_NULLABLE = 'YES' and EXTRA LIKE '%auto_increment%'
// come back as int64 (1/0), never as bool. Scanning them explicitly into
// int64 (instead of relying on database/sql to convert driver int64 values
// into *bool) keeps FetchSchema working on every Go version and protocol
// (go-sql-driver/mysql issue #440 covers the *bool scan failure mode).
func TestFetchSchema_Success_Int64ExpressionValues(t *testing.T) {
	c, mock := newMockConnector(t)

	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE = 'YES'", "EXTRA LIKE '%auto_increment%'"}).
		AddRow("id", "int", int64(1), int64(1)).
		AddRow("amount", "decimal", int64(1), int64(0)).
		AddRow("user_id", "bigint", int64(0), int64(0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES', EXTRA LIKE '%auto_increment%' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnRows(rows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

	schema, err := c.FetchSchema(context.Background(), "mydb", "orders")
	require.NoError(t, err)
	assert.Equal(t, "mydb", schema.Schema)
	assert.Equal(t, "orders", schema.Table)
	require.Len(t, schema.Columns, 3)
	assert.Equal(t, "id", schema.Columns[0].Name)
	assert.True(t, schema.Columns[0].Nullable)
	assert.True(t, schema.Columns[0].IsAutoInc)
	assert.Equal(t, "amount", schema.Columns[1].Name)
	assert.True(t, schema.Columns[1].Nullable)
	assert.False(t, schema.Columns[1].IsAutoInc)
	assert.Equal(t, "user_id", schema.Columns[2].Name)
	assert.False(t, schema.Columns[2].Nullable)
	assert.False(t, schema.Columns[2].IsAutoInc)
	assert.Equal(t, []string{"id"}, schema.PrimaryKey)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchSchema_TableNotFound(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES', EXTRA LIKE '%auto_increment%' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "ghost").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE = 'YES'", "EXTRA LIKE '%auto_increment%'"}))

	_, err := c.FetchSchema(context.Background(), "mydb", "ghost")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchSchema_QueryError(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES', EXTRA LIKE '%auto_increment%' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnError(errors.New("access denied"))

	_, err := c.FetchSchema(context.Background(), "mydb", "orders")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query schema")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFetchSchema_PopulatesPrimaryKey verifies that FetchSchema fills
// TableSchema.PrimaryKey from information_schema.KEY_COLUMN_USAGE, in
// ORDINAL_POSITION order, after the column query.
func TestFetchSchema_PopulatesPrimaryKey(t *testing.T) {
	c, mock := newMockConnector(t)

	// Column query first: a table with two columns.
	cols := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE = 'YES'", "EXTRA LIKE '%auto_increment%'"}).
		AddRow("id", "int", int64(0), int64(1)).
		AddRow("amount", "decimal", int64(1), int64(0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE = 'YES', EXTRA LIKE '%auto_increment%' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnRows(cols)

	// Primary key query second: a single-column PK.
	pk := sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION")).
		WithArgs("mydb", "orders").
		WillReturnRows(pk)

	schema, err := c.FetchSchema(context.Background(), "mydb", "orders")
	require.NoError(t, err)
	require.Len(t, schema.Columns, 2)
	assert.Equal(t, []string{"id"}, schema.PrimaryKey)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// executor.DB adapter (AsDB / Exec / Begin)
// ---------------------------------------------------------------------------

func TestMySQLConnector_AsDB(t *testing.T) {
	c, _ := newMockConnector(t)
	db := c.AsDB()
	require.NotNil(t, db)
	// AsDB returns the connector itself, which implements executor.DB.
	assert.Same(t, c, db)
}

func TestMySQLConnector_AsDB_Exec(t *testing.T) {
	c, mock := newMockConnector(t)
	db := c.AsDB()

	mock.ExpectExec(regexp.QuoteMeta("UPDATE orders SET amount = ? WHERE id = ?")).
		WithArgs(10.0, 1).
		WillReturnResult(sqlmock.NewResult(7, 1))

	res, err := db.Exec("UPDATE orders SET amount = ? WHERE id = ?", 10.0, 1)
	require.NoError(t, err)

	lastID, err := res.LastInsertId()
	require.NoError(t, err)
	assert.Equal(t, int64(7), lastID)

	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLConnector_AsDB_Begin(t *testing.T) {
	c, mock := newMockConnector(t)
	db := c.AsDB()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("DELETE FROM orders").
		WillReturnResult(sqlmock.NewResult(0, 3))
	res, err := tx.Exec("DELETE FROM orders")
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(3), affected)

	mock.ExpectCommit()
	assert.NoError(t, tx.Commit())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLConnector_AsDB_BeginRollback(t *testing.T) {
	c, mock := newMockConnector(t)
	db := c.AsDB()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectRollback()
	assert.NoError(t, tx.Rollback())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLConnector_AsDB_NotConnected(t *testing.T) {
	c := NewMySQLConnector()

	_, err := c.Exec("SELECT 1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")

	_, err = c.Begin()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

// ---------------------------------------------------------------------------
// ExecuteRollback
// ---------------------------------------------------------------------------

func TestExecuteRollback_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.ExecuteRollback(context.Background(), nil, ExecOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestExecuteRollback_EmptySQL(t *testing.T) {
	c, _ := newMockConnector(t)
	result, err := c.ExecuteRollback(context.Background(), []string{}, ExecOptions{})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(0), result.RowsAffected)
	assert.Equal(t, 0, result.BatchesCompleted)
}

func TestExecuteRollback_DryRun(t *testing.T) {
	c, mock := newMockConnector(t)

	result, err := c.ExecuteRollback(context.Background(),
		[]string{"DELETE FROM t1", "DELETE FROM t2"},
		ExecOptions{DryRun: true})
	require.NoError(t, err)
	assert.Equal(t, 0, result.BatchesCompleted)
	assert.Empty(t, result.Errors)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteRollback_Success(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM t1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM t2").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	result, err := c.ExecuteRollback(context.Background(),
		[]string{"DELETE FROM t1", "DELETE FROM t2"},
		ExecOptions{BatchSize: 10})
	require.NoError(t, err)
	assert.Equal(t, 1, result.BatchesCompleted)
	assert.Empty(t, result.Errors)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteRollback_MultipleBatches(t *testing.T) {
	c, mock := newMockConnector(t)

	// First batch of 2.
	mock.ExpectBegin()
	mock.ExpectExec("stmt1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("stmt2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Second batch of 1.
	mock.ExpectBegin()
	mock.ExpectExec("stmt3").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := c.ExecuteRollback(context.Background(),
		[]string{"stmt1", "stmt2", "stmt3"},
		ExecOptions{BatchSize: 2})
	require.NoError(t, err)
	assert.Equal(t, 2, result.BatchesCompleted)
	assert.Empty(t, result.Errors)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteRollback_BatchError(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectBegin()
	mock.ExpectExec("stmt1").WillReturnError(errors.New("constraint violation"))
	// Rollback on error.
	mock.ExpectRollback()

	// Second batch continues despite first error.
	mock.ExpectBegin()
	mock.ExpectExec("stmt2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := c.ExecuteRollback(context.Background(),
		[]string{"stmt1", "stmt2"},
		ExecOptions{BatchSize: 1})
	require.NoError(t, err)
	assert.Equal(t, 1, result.BatchesCompleted)
	assert.Len(t, result.Errors, 1)
	assert.Contains(t, result.Errors[0], "batch 0")
	assert.Contains(t, result.Errors[0], "constraint violation")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// Preflight
// ---------------------------------------------------------------------------

func TestPreflight_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.Preflight(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestPreflight_AllChecksPass(t *testing.T) {
	c, mock := newMockConnector(t)

	// 1. Version.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))

	// 2. binlog_format.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'binlog_format'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "ROW"))

	// 3. log_bin.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'log_bin'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("log_bin", "ON"))

	// 4. SHOW GRANTS.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).
		WillReturnRows(sqlmock.NewRows([]string{"Grants for user"}).AddRow("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'"))

	// 5. Database size (no database configured = skip).
	// ConnConfig.Database is "", so checkDatabaseSize returns early.
	// However, our connector was created with NewMySQLConnectorWithDB which has
	// an empty config. So dbName is "" and the check returns WARN early.

	// 6. Column metadata — skipped due to empty database.
	// 7. Foreign keys — skipped due to empty database.

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightPass, result.Status)
	assert.Equal(t, "8.0.32", result.Version)
	assert.Len(t, result.Checks, 7)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPreflight_BinlogFormatFail(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'binlog_format'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "STATEMENT"))

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightFail, result.Status)
	assert.Equal(t, PreflightFail, result.Checks[1].Status)
	assert.Contains(t, result.Checks[1].Message, "STATEMENT")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPreflight_LogBinOff(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'binlog_format'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "ROW"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'log_bin'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("log_bin", "OFF"))

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightFail, result.Status)
	assert.Equal(t, PreflightFail, result.Checks[2].Status)
	assert.Contains(t, result.Checks[2].Message, "OFF")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPreflight_OldVersion(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.42-log"))

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightFail, result.Status)
	assert.Equal(t, PreflightFail, result.Checks[0].Status)
	assert.Contains(t, result.Checks[0].Message, "5.7")
	assert.Contains(t, result.Checks[0].Message, "8.0+")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// Preflight with database configured
// ---------------------------------------------------------------------------

func TestPreflight_WithDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	c := NewMySQLConnectorWithDB(db)
	c.config.Database = "testdb"
	t.Cleanup(func() { _ = c.Close() })

	// 1. Version.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))
	// 2. binlog_format.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'binlog_format'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "ROW"))
	// 3. log_bin.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'log_bin'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("log_bin", "ON"))
	// 4. SHOW GRANTS.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).
		WillReturnRows(sqlmock.NewRows([]string{"Grants for user"}).AddRow("GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'"))
	// 5. Database size.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = ?")).
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"SUM(data_length + index_length)"}).AddRow(int64(1048576)))
	// 6. Column metadata.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS WHERE table_schema = ? ORDER BY TABLE_NAME, ORDINAL_POSITION")).
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY"}).
			AddRow("orders", "id", "int", "NO", "PRI").
			AddRow("orders", "amount", "decimal", "YES", ""))
	// 7. FK check.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT rc.CONSTRAINT_NAME, rc.TABLE_NAME, rc.REFERENCED_TABLE_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_COLUMN_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS rc JOIN information_schema.KEY_COLUMN_USAGE kcu ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA WHERE rc.CONSTRAINT_SCHEMA = ? ORDER BY rc.TABLE_NAME")).
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"CONSTRAINT_NAME", "TABLE_NAME", "REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME"}).
			AddRow("fk_order_user", "orders", "users", "user_id", "id"))

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightPass, result.Status)
	assert.Equal(t, "8.0.32", result.Version)

	// Check individual check results.
	assert.Equal(t, PreflightPass, result.Checks[0].Status) // version
	assert.Equal(t, PreflightPass, result.Checks[1].Status) // binlog format
	assert.Equal(t, PreflightPass, result.Checks[2].Status) // log_bin
	assert.Equal(t, PreflightPass, result.Checks[3].Status) // grants
	assert.Equal(t, PreflightPass, result.Checks[4].Status) // db size
	assert.Equal(t, PreflightPass, result.Checks[5].Status) // column metadata
	assert.Equal(t, PreflightPass, result.Checks[6].Status) // FK
	assert.Contains(t, result.Checks[6].Message, "1 foreign key")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// Privilege checking
// ---------------------------------------------------------------------------

func TestCheckRequiredPrivileges_AllPresent(t *testing.T) {
	c := NewMySQLConnector()
	grants := []string{
		"GRANT SELECT, INSERT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'u'@'%'",
	}
	missing := c.checkRequiredPrivileges(grants)
	assert.Empty(t, missing)
}

func TestCheckRequiredPrivileges_Missing(t *testing.T) {
	c := NewMySQLConnector()
	grants := []string{
		"GRANT INSERT ON *.* TO 'u'@'%'",
	}
	missing := c.checkRequiredPrivileges(grants)
	assert.Contains(t, missing, "SELECT")
	assert.Contains(t, missing, "REPLICATION SLAVE")
	assert.Contains(t, missing, "REPLICATION CLIENT")
}

func TestCheckRequiredPrivileges_AllPrivileges(t *testing.T) {
	c := NewMySQLConnector()
	grants := []string{
		"GRANT ALL PRIVILEGES ON *.* TO 'u'@'%'",
	}
	missing := c.checkRequiredPrivileges(grants)
	assert.Empty(t, missing)
}

// ---------------------------------------------------------------------------
// Version parsing
// ---------------------------------------------------------------------------

func TestParseVersion(t *testing.T) {
	tests := []struct {
		input string
		maj   int
		min   int
		patch int
	}{
		{"8.0.32", 8, 0, 32},
		{"8.0.32-log", 8, 0, 32},
		{"5.7.42", 5, 7, 42},
		{"8.4.0", 8, 4, 0},
		{"8.0.32-enterprise", 8, 0, 32},
		{"invalid", 0, 0, 0},
	}
	for _, tt := range tests {
		maj, min, patch := parseVersion(tt.input)
		assert.Equal(t, tt.maj, maj, "major for %q", tt.input)
		assert.Equal(t, tt.min, min, "minor for %q", tt.input)
		assert.Equal(t, tt.patch, patch, "patch for %q", tt.input)
	}
}

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

func TestTruncate(t *testing.T) {
	assert.Equal(t, "hello", truncate("hello", 10))
	assert.Equal(t, "hello...", truncate("hello world this is long", 5))
}

// ---------------------------------------------------------------------------
// Format bytes
// ---------------------------------------------------------------------------

func TestFormatBytes(t *testing.T) {
	assert.Equal(t, "500 B", formatBytes(500))
	assert.Equal(t, "1.0 KB", formatBytes(1024))
	assert.Equal(t, "1.0 MB", formatBytes(1024*1024))
	assert.Equal(t, "1.0 GB", formatBytes(1024*1024*1024))
}

// ---------------------------------------------------------------------------
// ConnConfig DSN
// ---------------------------------------------------------------------------

func TestConnConfig_DSN(t *testing.T) {
	cfg := ConnConfig{
		Host:     "db.example.com",
		Port:     3307,
		User:     "root",
		Password: "secret",
		Database: "mydb",
	}
	dsn := cfg.DSN()
	assert.Contains(t, dsn, "root:secret@tcp(db.example.com:3307)/mydb")
}

func TestConnConfig_DSNDefaults(t *testing.T) {
	cfg := ConnConfig{
		User:     "root",
		Password: "",
		Database: "test",
	}
	dsn := cfg.DSN()
	assert.Contains(t, dsn, "127.0.0.1:3306")
}

func TestConnConfig_DSNWithParams(t *testing.T) {
	cfg := ConnConfig{
		User:     "u",
		Password: "p",
		Database: "db",
		Params: map[string]string{
			"charset": "utf8",
			"timeout": "5s",
		},
	}
	dsn := cfg.DSN()
	assert.Contains(t, dsn, "charset=utf8")
	assert.Contains(t, dsn, "timeout=5s")
}

// ---------------------------------------------------------------------------
// Interface satisfaction (compile-time check, also executed)
// ---------------------------------------------------------------------------

func TestInterfaceCompileTime(t *testing.T) {
	var _ Connector = (*MySQLConnector)(nil)
}

// ---------------------------------------------------------------------------
// Integration-style: not connected errors every method
// ---------------------------------------------------------------------------

func TestMySQLConnector_AllMethodsFailWhenNotConnected(t *testing.T) {
	ctx := context.Background()
	c := NewMySQLConnector()

	_, err := c.GetBinlogFiles(ctx)
	assert.Error(t, err)

	_, err = c.ParseBinlog(ctx, ParseRequest{BinlogFiles: []string{"x"}})
	assert.Error(t, err)

	_, err = c.ExecuteRollback(ctx, []string{"x"}, ExecOptions{})
	assert.Error(t, err)

	_, err = c.FetchSchema(ctx, "mydb", "orders")
	assert.Error(t, err)

	_, err = c.Preflight(ctx)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// SQL null handling for database size check
// ---------------------------------------------------------------------------

func TestPreflight_DatabaseSizeNull(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	c := NewMySQLConnectorWithDB(db)
	c.config.Database = "emptydb"

	// Provide enough expectations to get past early checks.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'binlog_format'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "ROW"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'log_bin'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("log_bin", "ON"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).
		WillReturnRows(sqlmock.NewRows([]string{"Grants for user"}).AddRow("GRANT ALL ON *.* TO 'u'@'%'"))

	// Database size returns NULL (no tables).
	mock.ExpectQuery(regexp.QuoteMeta("SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = ?")).
		WithArgs("emptydb").
		WillReturnRows(sqlmock.NewRows([]string{"SUM(data_length + index_length)"}).AddRow(nil))

	// Column metadata returns no rows (empty db).
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS WHERE table_schema = ? ORDER BY TABLE_NAME, ORDINAL_POSITION")).
		WithArgs("emptydb").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY"}))

	// FK check returns no rows.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT rc.CONSTRAINT_NAME, rc.TABLE_NAME, rc.REFERENCED_TABLE_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_COLUMN_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS rc JOIN information_schema.KEY_COLUMN_USAGE kcu ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA WHERE rc.CONSTRAINT_SCHEMA = ? ORDER BY rc.TABLE_NAME")).
		WithArgs("emptydb").
		WillReturnRows(sqlmock.NewRows([]string{"CONSTRAINT_NAME", "TABLE_NAME", "REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME"}))

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightPass, result.Status)

	// Size check should be WARN for empty db.
	assert.Equal(t, PreflightWarn, result.Checks[4].Status)
	assert.Contains(t, result.Checks[4].Message, "empty")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// Empty grants handling
// ---------------------------------------------------------------------------

func TestPreflight_PrivilegesNoGrants(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	c := NewMySQLConnectorWithDB(db)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT VERSION()")).
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.32"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'binlog_format'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "ROW"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW VARIABLES LIKE 'log_bin'")).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("log_bin", "ON"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).
		WillReturnRows(sqlmock.NewRows([]string{"Grants for user"}).AddRow("GRANT USAGE ON *.* TO 'u'@'%'"))

	result, err := c.Preflight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, PreflightWarn, result.Checks[3].Status)
	assert.Contains(t, result.Checks[3].Message, "missing")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// MasterPosition (collector.MySQLInfo)
// ---------------------------------------------------------------------------

func TestMasterPosition_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.MasterPosition(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestMasterPosition_Success(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SHOW MASTER STATUS")).
		WillReturnRows(sqlmock.NewRows([]string{"File", "Position"}).
			AddRow("mysql-bin.000003", 154))

	pos, err := c.MasterPosition(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "mysql-bin.000003", pos.Name)
	assert.Equal(t, uint32(154), pos.Pos)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMasterPosition_NoRows(t *testing.T) {
	// 未启用 binlog 时 SHOW MASTER STATUS 返回空结果集 → sql.ErrNoRows。
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SHOW MASTER STATUS")).
		WillReturnRows(sqlmock.NewRows([]string{"File", "Position"}))

	_, err := c.MasterPosition(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no rows")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMasterPosition_QueryError(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SHOW MASTER STATUS")).
		WillReturnError(errors.New("syntax error"))

	_, err := c.MasterPosition(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SHOW MASTER STATUS")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// ListBinlogs (collector.MySQLInfo adapter over GetBinlogFiles)
// ---------------------------------------------------------------------------

func TestListBinlogs_NotConnected(t *testing.T) {
	c := NewMySQLConnector()
	_, err := c.ListBinlogs(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestListBinlogs_ConvertsToManifestFiles(t *testing.T) {
	c, mock := newMockConnector(t)

	mock.ExpectQuery(regexp.QuoteMeta("SHOW BINARY LOGS")).
		WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 512).
			AddRow("mysql-bin.000002", 1024))

	files, err := c.ListBinlogs(context.Background())
	require.NoError(t, err)
	require.Len(t, files, 2)
	assert.Equal(t, "mysql-bin.000001", files[0].Name)
	assert.Equal(t, int64(512), files[0].Size)
	assert.Equal(t, "mysql-bin.000002", files[1].Name)
	assert.Equal(t, int64(1024), files[1].Size)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// Preflight regression: archive.MasterPosition compile-time interface check
// ---------------------------------------------------------------------------

var _ = gomysql.Position{Name: "mysql-bin.000001", Pos: 4}
