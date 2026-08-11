package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/config"
	"github.com/a-shan/mysql-pitr/internal/connector"
	"github.com/a-shan/mysql-pitr/internal/executor"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// mockConnector is a Connector that returns canned results. AsDB returns a
// recordingDB so the execute path can be verified without a live MySQL server.
type mockConnector struct {
	connectErr      error
	preflightResult *connector.PreflightResult
	preflightErr    error
	binlogDir       string
	binlogDirErr    error
	schemas         map[string]binlog.TableSchema // "schema.table" -> schema
	db              *recordingDB
	closeCalled     bool
}

func (m *mockConnector) Connect(cfg connector.ConnConfig) error {
	return m.connectErr
}

func (m *mockConnector) GetBinlogFiles(ctx context.Context) ([]connector.BinlogFile, error) {
	return nil, nil
}

func (m *mockConnector) GetBinlogDir(ctx context.Context) (string, error) {
	if m.binlogDirErr != nil {
		return "", m.binlogDirErr
	}
	if m.binlogDir != "" {
		return m.binlogDir, nil
	}
	return "/var/lib/mysql/", nil
}

func (m *mockConnector) ParseBinlog(ctx context.Context, req connector.ParseRequest) (*connector.ParseResult, error) {
	return nil, nil
}

func (m *mockConnector) ExecuteRollback(ctx context.Context, sqls []string, opts connector.ExecOptions) (*connector.ExecResult, error) {
	return nil, nil
}

func (m *mockConnector) Preflight(ctx context.Context) (*connector.PreflightResult, error) {
	return m.preflightResult, m.preflightErr
}

func (m *mockConnector) FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error) {
	if m.schemas != nil {
		if sch, ok := m.schemas[schema+"."+table]; ok {
			return sch, nil
		}
	}
	return binlog.TableSchema{}, fmt.Errorf("mock: no schema for %s.%s", schema, table)
}

func (m *mockConnector) AsDB() executor.DB {
	if m.db == nil {
		m.db = &recordingDB{}
	}
	return m.db
}

func (m *mockConnector) Close() error {
	m.closeCalled = true
	return nil
}

// recordingDB records the SQL executed through the executor path.
type recordingDB struct {
	executed []string
	commits  int
}

type recordingResult struct{}

func (recordingResult) LastInsertId() (int64, error) { return 0, nil }
func (recordingResult) RowsAffected() (int64, error) { return 1, nil }

type recordingTx struct{ db *recordingDB }

func (t *recordingTx) Exec(query string, args ...interface{}) (executor.Result, error) {
	t.db.executed = append(t.db.executed, query)
	return recordingResult{}, nil
}
func (t *recordingTx) Commit() error {
	t.db.commits++
	return nil
}
func (t *recordingTx) Rollback() error { return nil }

func (d *recordingDB) Exec(query string, args ...interface{}) (executor.Result, error) {
	d.executed = append(d.executed, query)
	return recordingResult{}, nil
}
func (d *recordingDB) Begin() (executor.Tx, error) { return &recordingTx{db: d}, nil }
func (d *recordingDB) Close() error                { return nil }

// fakeScanner is a binlog.Scanner that yields canned transactions.
type fakeScanner struct {
	txs        []*binlog.Transaction
	scanErr    error
	nextErr    error
	filter     binlog.Filter
	scanCalled bool
	closed     bool
}

func (f *fakeScanner) Scan(ctx context.Context, filter binlog.Filter) error {
	f.scanCalled = true
	f.filter = filter
	return f.scanErr
}

func (f *fakeScanner) Next() (*binlog.Transaction, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	if len(f.txs) == 0 {
		return nil, io.EOF
	}
	tx := f.txs[0]
	f.txs = f.txs[1:]
	return tx, nil
}

func (f *fakeScanner) Close() error {
	f.closed = true
	return nil
}

// withFakeScanner substitutes the binlog scanner seam for the duration of a
// test so RunFlashback can be exercised without a live binlog directory.
func withFakeScanner(t *testing.T, fs *fakeScanner) {
	t.Helper()
	orig := newBinlogScanner
	newBinlogScanner = func(sf binlog.SchemaFetcher, opts ...binlog.Option) binlog.Scanner {
		return fs
	}
	t.Cleanup(func() { newBinlogScanner = orig })
}

// defaultMock creates a mock connector configured for a successful flashback.
func defaultMock() *mockConnector {
	return &mockConnector{
		preflightResult: &connector.PreflightResult{
			Status:  connector.PreflightPass,
			Version: "8.0.32",
			Checks: []connector.PreflightCheck{
				{Name: "MySQL Version", Status: connector.PreflightPass, Message: "8.0.32"},
				{Name: "Binlog Format", Status: connector.PreflightPass},
				{Name: "Binary Logging Enabled", Status: connector.PreflightPass},
				{Name: "User Privileges", Status: connector.PreflightPass},
			},
		},
		schemas: ordersSchema(),
	}
}

// ordersSchema is the mydb.orders column metadata used by fixtures.
func ordersSchema() map[string]binlog.TableSchema {
	return map[string]binlog.TableSchema{
		"mydb.orders": {
			Schema: "mydb",
			Table:  "orders",
			Columns: []binlog.ColumnDef{
				{Name: "id"},
				{Name: "name"},
			},
		},
	}
}

// flashbackFixture returns one transaction containing an INSERT then an UPDATE
// on mydb.orders. reverse.Generate emits LIFO: the UPDATE reversal first, then
// the INSERT reversal.
func flashbackFixture() []*binlog.Transaction {
	tx, err := binlog.NewTransaction(
		"11111111-1111-1111-1111-111111111111:1", 0,
		time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), "mydb")
	if err != nil {
		panic(err)
	}
	tx.Statements = []binlog.RowChange{
		{
			Schema: "mydb", Table: "orders", Action: binlog.ActionInsert,
			After: []interface{}{int64(1), "widget"},
		},
		{
			Schema: "mydb", Table: "orders", Action: binlog.ActionUpdate,
			Before: []interface{}{int64(1), "widget"},
			After:  []interface{}{int64(1), "gadget"},
		},
	}
	return []*binlog.Transaction{&tx}
}

// expected fixture SQL, in LIFO generation order (UPDATE reversal first).
var (
	fixtureUpdateSQL = "UPDATE `mydb`.`orders` SET `id` = 1, `name` = 'widget' WHERE `id` = 1 AND `name` = 'gadget'"
	fixtureDeleteSQL = "DELETE FROM `mydb`.`orders` WHERE `id` = 1 AND `name` = 'widget'"
)

// captureStdout redirects os.Stdout for the duration of the test and returns a
// function that restores it and returns everything printed.
func captureStdout(t *testing.T) func() string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	out := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		out <- string(b)
	}()

	return func() string {
		_ = w.Close()
		os.Stdout = old
		return <-out
	}
}

// ---------------------------------------------------------------------------
// Test: NewFlashbackCommand flag registration
// ---------------------------------------------------------------------------

func TestFlashbackCommand_Flags(t *testing.T) {
	cmd := NewFlashbackCommand()
	// Execute with --help to verify flags are registered
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetArgs([]string{"--help"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "--mysql-dsn")
	assert.Contains(t, output, "--target-table")
	assert.Contains(t, output, "--recovery-time")
	assert.Contains(t, output, "--output")
	assert.Contains(t, output, "--dry-run")
	assert.Contains(t, output, "--batch-size")
	assert.Contains(t, output, "--config")
	assert.Contains(t, output, "--passphrase")
}

func TestFlashbackCommand_ValidatesRequiredFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no flags", []string{}},
		{"missing target-table", []string{"--mysql-dsn=root:pass@tcp(127.0.0.1:3306)/db", "--recovery-time=2024-01-15T10:00:00Z"}},
		{"missing recovery-time", []string{"--mysql-dsn=root:pass@tcp(127.0.0.1:3306)/db", "--target-table=mydb.orders"}},
		{"config without passphrase", []string{"--config=/path/to/config", "--target-table=mydb.orders", "--recovery-time=2024-01-15T10:00:00Z"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewFlashbackCommand()
			buf := new(bytes.Buffer)
			cmd.SetErr(buf)
			cmd.SetArgs(tt.args)

			// Execute should return an error
			err := cmd.Execute()
			assert.Error(t, err, "expected error for args: %v", tt.args)
		})
	}
}

// ---------------------------------------------------------------------------
// Test: RunFlashback with mock connector + fake scanner
// ---------------------------------------------------------------------------

func TestRunFlashback_DryRun(t *testing.T) {
	mock := defaultMock()
	fs := &fakeScanner{txs: flashbackFixture()}
	withFakeScanner(t, fs)
	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
		DryRun:       true,
		BatchSize:    1000,
	}

	gotStdout := captureStdout(t)
	err := RunFlashback(context.Background(), opts)
	require.NoError(t, err)
	output := gotStdout()

	assert.True(t, mock.closeCalled, "connector should be closed")
	assert.True(t, fs.scanCalled)
	assert.True(t, fs.closed)
	assert.Contains(t, output, fixtureUpdateSQL+";")
	assert.Contains(t, output, fixtureDeleteSQL+";")
}

func TestRunFlashback_OutputFile(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "rollback.sql")

	mock := defaultMock()
	fs := &fakeScanner{txs: flashbackFixture()}
	withFakeScanner(t, fs)
	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
		OutputFile:   outputPath,
		BatchSize:    1000,
	}

	err := RunFlashback(context.Background(), opts)
	require.NoError(t, err)

	// Verify file contents
	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	content := string(data)
	assert.Contains(t, content, fixtureUpdateSQL+";")
	assert.Contains(t, content, fixtureDeleteSQL+";")
}

func TestRunFlashback_Execute(t *testing.T) {
	mock := defaultMock()
	fs := &fakeScanner{txs: flashbackFixture()}
	withFakeScanner(t, fs)
	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
		BatchSize:    1000,
	}

	err := RunFlashback(context.Background(), opts)
	require.NoError(t, err)

	// Both statements executed in LIFO order via a single committed batch.
	require.NotNil(t, mock.db)
	assert.Equal(t, []string{fixtureUpdateSQL, fixtureDeleteSQL}, mock.db.executed)
	assert.Equal(t, 1, mock.db.commits)
}

func TestRunFlashback_BuildsCorrectFilter(t *testing.T) {
	mock := defaultMock()
	fs := &fakeScanner{txs: flashbackFixture()}
	withFakeScanner(t, fs)

	recoveryTime := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: recoveryTime.Format(time.RFC3339),
		DryRun:       true,
	}

	err := RunFlashback(context.Background(), opts)
	require.NoError(t, err)

	require.True(t, fs.scanCalled)
	assert.Equal(t, "/var/lib/mysql/", fs.filter.BinlogDir)
	assert.Equal(t, []binlog.TableRef{{Schema: "mydb", Table: "orders"}}, fs.filter.Tables)
	require.NotNil(t, fs.filter.TimeRange)
	assert.Equal(t, recoveryTime, fs.filter.TimeRange.End)
}

func TestRunFlashback_PreflightFail(t *testing.T) {
	mock := defaultMock()
	mock.preflightResult = &connector.PreflightResult{
		Status:  connector.PreflightFail,
		Version: "5.7.42",
		Checks: []connector.PreflightCheck{
			{Name: "MySQL Version", Status: connector.PreflightFail, Message: "MySQL 5.7.42 detected; 8.0+ required"},
		},
	}

	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
	}

	err := RunFlashback(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "preflight FAILED")
}

func TestRunFlashback_NoTransactions(t *testing.T) {
	mock := defaultMock()
	withFakeScanner(t, &fakeScanner{txs: nil})

	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
	}

	err := RunFlashback(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no transactions found")
}

func TestRunFlashback_ScanError(t *testing.T) {
	mock := defaultMock()
	withFakeScanner(t, &fakeScanner{scanErr: errors.New("binlog file corrupted")})

	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
		DryRun:       true,
	}

	err := RunFlashback(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scan")
}

func TestRunFlashback_NextError(t *testing.T) {
	mock := defaultMock()
	withFakeScanner(t, &fakeScanner{nextErr: errors.New("parse failure")})

	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
		DryRun:       true,
	}

	err := RunFlashback(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scan next")
}

func TestRunFlashback_ConnectorProvided(t *testing.T) {
	// When using a pre-injected connector, DSN is not required.
	mock := defaultMock()
	withFakeScanner(t, &fakeScanner{txs: flashbackFixture()})
	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "2024-01-15T11:00:00Z",
		DryRun:       true,
	}

	err := RunFlashback(context.Background(), opts)
	require.NoError(t, err)
}

func TestRunFlashback_InvalidRecoveryTime(t *testing.T) {
	mock := defaultMock()
	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "mydb.orders",
		RecoveryTime: "not-a-timestamp",
		DryRun:       true,
	}

	err := RunFlashback(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse recovery-time")
}

func TestRunFlashback_InvalidTargetTable(t *testing.T) {
	mock := defaultMock()
	withFakeScanner(t, &fakeScanner{})

	opts := FlashbackOptions{
		Connector:    mock,
		TargetTable:  "orders", // missing schema part
		RecoveryTime: "2024-01-15T11:00:00Z",
		DryRun:       true,
	}

	err := RunFlashback(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be schema.table")
}

// ---------------------------------------------------------------------------
// Test: splitTableRef
// ---------------------------------------------------------------------------

func TestSplitTableRef(t *testing.T) {
	schema, table, err := splitTableRef("mydb.orders")
	require.NoError(t, err)
	assert.Equal(t, "mydb", schema)
	assert.Equal(t, "orders", table)

	for _, bad := range []string{"orders", ".orders", "mydb.", ""} {
		_, _, err := splitTableRef(bad)
		assert.Error(t, err, "expected error for %q", bad)
		assert.Contains(t, err.Error(), "must be schema.table")
	}
}

// ---------------------------------------------------------------------------
// Test: resolveConnConfig
// ---------------------------------------------------------------------------

func TestResolveConnConfig_FromDSN(t *testing.T) {
	opts := FlashbackOptions{
		DSN: "user:pass@tcp(host.example.com:3307)/mydb?tls=skip-verify",
	}
	cc, err := resolveConnConfig(opts)
	require.NoError(t, err)
	assert.Equal(t, "host.example.com", cc.Host)
	assert.Equal(t, 3307, cc.Port)
	assert.Equal(t, "user", cc.User)
	assert.Equal(t, "pass", cc.Password)
	assert.Equal(t, "mydb", cc.Database)
}

func TestResolveConnConfig_FromConfigFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.conf")

	// We need to create a valid encrypted config file.
	// Use the internal config package's SaveConfig.
	cfg := &config.Config{
		MySQL: config.MySQLConfig{
			Host:     "192.168.1.50",
			Port:     3306,
			User:     "replicator",
			Password: "s3cret!",
			Database: "prod",
		},
	}
	err := config.SaveConfig(cfgPath, "test-passphrase", cfg)
	require.NoError(t, err)

	opts := FlashbackOptions{
		ConfigFile: cfgPath,
		Passphrase: "test-passphrase",
	}
	cc, err := resolveConnConfig(opts)
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.50", cc.Host)
	assert.Equal(t, 3306, cc.Port)
	assert.Equal(t, "replicator", cc.User)
	assert.Equal(t, "s3cret!", cc.Password)
	assert.Equal(t, "prod", cc.Database)
}

func TestResolveConnConfig_Neither(t *testing.T) {
	opts := FlashbackOptions{}
	_, err := resolveConnConfig(opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "either --mysql-dsn or --config")
}

// ---------------------------------------------------------------------------
// Test: writeSQLToFile
// ---------------------------------------------------------------------------

func TestWriteSQLToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.sql")

	sqls := []string{
		"DELETE FROM `orders` WHERE `id` = 1 LIMIT 1;",
		"INSERT INTO `orders` (`id`, `name`) VALUES (2, 'widget');",
	}

	err := writeSQLToFile(path, sqls)
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	content := strings.TrimSpace(string(data))
	lines := strings.Split(content, "\n")
	assert.Len(t, lines, 2)
	assert.Equal(t, sqls[0], lines[0])
	assert.Equal(t, sqls[1], lines[1])
}

func TestWriteSQLToFile_InvalidPath(t *testing.T) {
	err := writeSQLToFile("/nonexistent/dir/output.sql", []string{"SELECT 1"})
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Test: Root command wiring
// ---------------------------------------------------------------------------

func TestNewRootCommand_HasFlashbackSubcommand(t *testing.T) {
	root := NewRootCommand()
	assert.NotNil(t, root)

	// Find the flashback subcommand
	var flashbackCmd *cobra.Command
	for _, sub := range root.Commands() {
		if sub.Use == "flashback" {
			flashbackCmd = sub
			break
		}
	}
	require.NotNil(t, flashbackCmd, "root command should have flashback subcommand")

	// Verify flashback flags are accessible through the subcommand
	assert.NotNil(t, flashbackCmd.Flags().Lookup("mysql-dsn"))
	assert.NotNil(t, flashbackCmd.Flags().Lookup("target-table"))
	assert.NotNil(t, flashbackCmd.Flags().Lookup("recovery-time"))
}
