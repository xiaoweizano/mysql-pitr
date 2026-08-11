package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/config"
	"github.com/a-shan/mysql-pitr/internal/connector"
	"github.com/a-shan/mysql-pitr/internal/daemon"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

func testServeDaemon(t *testing.T) *serveDaemon {
	t.Helper()
	cfg := &config.Config{
		MySQL: config.MySQLConfig{
			Host: "127.0.0.1", Port: 3306, User: "u", Password: "p", Database: "d",
		},
		DataDir:         t.TempDir(),
		MySQLBinlogPath: "/opt/mysql/bin/mysqlbinlog",
	}
	return newServeDaemon(cfg, "agent-1")
}

// withFailingExec 把 daemon 换成「exec 工厂永远失败」的版本，让 execute/resume
// 的接受测试不触碰真 MySQL（handler 返回 accepted 时 goroutine 才异步失败）。
func (d *serveDaemon) withFailingExec(t *testing.T) *serveDaemon {
	t.Helper()
	d.daemon = daemon.NewDaemon(
		daemon.ScanDeps{ArchiveDir: t.TempDir(), Logger: d.logger()},
		executor.NewExecutor(func(plan executor.Plan) (executor.DB, error) {
			return nil, errors.New("test: db factory disabled")
		}, executor.NewFileCheckpointStore(t.TempDir())),
		d.loopState, d,
	)
	return d
}

// ---------------------------------------------------------------------------
// Constructor wiring
// ---------------------------------------------------------------------------

func TestNewServeDaemon_WiresDaemon(t *testing.T) {
	d := testServeDaemon(t)
	require.NotNil(t, d.daemon, "daemon command layer must be wired")
	require.Nil(t, d.loop, "loop stays nil until startArchiveLoop")
}

// ---------------------------------------------------------------------------
// Param helpers
// ---------------------------------------------------------------------------

func TestParamString(t *testing.T) {
	assert.Equal(t, "abc", paramString(map[string]interface{}{"k": " abc "}, "k"))
	assert.Equal(t, "", paramString(map[string]interface{}{"k": 42}, "k"))
	assert.Equal(t, "", paramString(map[string]interface{}{}, "k"))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func TestHandleShutdown(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleShutdown(context.Background(), ws.Command{Cmd: "s", Type: ws.CmdShutdown})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusOK, resp.Status)

	select {
	case <-d.stopCh:
	case <-time.After(2 * time.Second):
		t.Fatal("stopCh was not closed by shutdown")
	}
}

func TestHandleStatus_ReportsMySQLConnectivity(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleStatus(context.Background(), ws.Command{Cmd: "s", Type: ws.CmdStatus})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusOK, resp.Status)

	result, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "agent-1", result["agentId"])

	mysql, ok := result["mysql"].(map[string]interface{})
	require.True(t, ok)
	// No MySQL is reachable in the test environment.
	assert.Equal(t, false, mysql["connected"])
}

func TestHandleScan_Accepted(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleScan(context.Background(), ws.Command{
		Cmd: "scan-1", Type: ws.CmdScan,
		Params: map[string]interface{}{"mode": "meta", "filter": map[string]interface{}{}},
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusOK, resp.Status)

	result, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, result["accepted"])
	assert.Equal(t, "scan-1", result["operationId"])
}

func TestHandleScan_UnknownMode(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleScan(context.Background(), ws.Command{
		Cmd: "scan-2", Type: ws.CmdScan,
		Params: map[string]interface{}{"mode": "bogus"},
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusError, resp.Status)
	assert.Contains(t, resp.Error, "scan")
}

func TestHandleExecute_Accepted(t *testing.T) {
	d := testServeDaemon(t).withFailingExec(t)
	resp := d.handleExecute(context.Background(), ws.Command{
		Cmd: "exec-1", Type: ws.CmdExecute,
		Params: map[string]interface{}{
			"operationId": "op-1",
			"statements": []interface{}{
				map[string]interface{}{
					"sql": "UPDATE shop.orders SET status='ok' WHERE id=1",
					"txId": "tx-1", "txOrder": 0,
				},
			},
		},
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusOK, resp.Status)
	assert.Empty(t, resp.Error, "accepted response carries no error text")

	result, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "op-1", result["operationId"])
}

func TestHandleExecute_MissingOperationID(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleExecute(context.Background(), ws.Command{
		Cmd: "exec-2", Type: ws.CmdExecute,
		Params: map[string]interface{}{"statements": []interface{}{}},
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusError, resp.Status)
	assert.Contains(t, resp.Error, "operationID")
}

func TestHandleResume_Accepted(t *testing.T) {
	d := testServeDaemon(t).withFailingExec(t)
	resp := d.handleResume(context.Background(), ws.Command{
		Cmd: "resume-1", Type: ws.CmdResume,
		Params: map[string]interface{}{
			"operationId": "op-2",
			"statements": []interface{}{
				map[string]interface{}{
					"sql": "UPDATE shop.orders SET status='ok' WHERE id=2",
					"txId": "tx-2", "txOrder": 0,
				},
			},
		},
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusOK, resp.Status)
}

func TestHandleCancel_NoSuchOp(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleCancel(context.Background(), ws.Command{
		Cmd: "cancel-1", Type: ws.CmdCancel,
		Params: map[string]interface{}{"operationId": "missing"},
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusError, resp.Status)
	assert.Contains(t, resp.Error, "no running op")
}

func TestHandleCancel_MissingOperationID(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleCancel(context.Background(), ws.Command{
		Cmd: "cancel-2", Type: ws.CmdCancel,
	})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusError, resp.Status)
	assert.Contains(t, resp.Error, "operationId")
}

func TestHandleArchiveStatus_NoLoopZeroState(t *testing.T) {
	d := testServeDaemon(t)
	resp := d.handleArchiveStatus(context.Background(), ws.Command{Cmd: "as", Type: ws.CmdArchiveStatus})
	require.NotNil(t, resp)
	assert.Equal(t, ws.StatusOK, resp.Status)

	st, ok := resp.Result.(collector.State)
	require.True(t, ok)
	assert.Empty(t, st.LastFile, "loop not started => zero state")
}

// ---------------------------------------------------------------------------
// EventSink envelope convention
// ---------------------------------------------------------------------------

func TestStreamEventCommand_Envelope(t *testing.T) {
	ev := ws.StreamEvent{ID: "scan-1", Kind: ws.EvTxMeta, Data: json.RawMessage(`{"x":1}`)}
	cmd := streamEventCommand(ev)

	assert.Equal(t, "ev-scan-1", cmd.Cmd)
	assert.Equal(t, streamEventType, cmd.Type)
	assert.Equal(t, "scan-1", cmd.Params["id"])
	assert.Equal(t, ws.EvTxMeta, cmd.Params["kind"])
	// data 保持原始 JSON（json.RawMessage 序列化时展开，不转义成字符串）。
	dataJSON, err := json.Marshal(cmd.Params["data"])
	require.NoError(t, err)
	assert.JSONEq(t, `{"x":1}`, string(dataJSON))
}

// ---------------------------------------------------------------------------
// startArchiveLoop validation (fails before any MySQL connection)
// ---------------------------------------------------------------------------

func TestStartArchiveLoop_MissingArchiveDir(t *testing.T) {
	d := testServeDaemon(t) // cfg.Archive 为 nil
	err := d.startArchiveLoop(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "archive.dir")
}

func TestStartArchiveLoop_MissingServerID(t *testing.T) {
	d := testServeDaemon(t)
	d.cfg.Archive = &config.ArchiveConfig{Dir: t.TempDir()}
	err := d.startArchiveLoop(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "archive.server_id")
}

// ---------------------------------------------------------------------------
// friendlyConnError — MySQL 1045 translation
// ---------------------------------------------------------------------------

func TestFriendlyConnError_AccessDenied(t *testing.T) {
	cfg := connector.ConnConfig{User: "pitr", Database: "zlmy"}
	raw := &mysql.MySQLError{
		Number:  1045,
		Message: "Access denied for user 'pitr'@'172.19.0.3' (using password: YES)",
	}
	err := fmt.Errorf("connector: ping failed: %w", raw)

	got := friendlyConnError(cfg, err)
	require.Error(t, got)
	assert.Contains(t, got.Error(), "pitr")
	assert.Contains(t, got.Error(), "zlmy")
	assert.Contains(t, got.Error(), "172.19.0.3")
	assert.Contains(t, got.Error(), "CREATE USER")
	assert.Contains(t, got.Error(), "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT")
	assert.Contains(t, got.Error(), "FLUSH PRIVILEGES")
}

func TestFriendlyConnError_OtherErrorsPassthrough(t *testing.T) {
	cfg := connector.ConnConfig{User: "pitr", Database: "zlmy"}

	refused := errors.New("dial tcp 127.0.0.1:3306: connect: connection refused")
	assert.Same(t, refused, friendlyConnError(cfg, refused))

	// A non-1045 MySQL error (e.g. unknown database) is also left untouched.
	unknownDB := &mysql.MySQLError{Number: 1049, Message: "Unknown database 'zlmy'"}
	assert.Same(t, unknownDB, friendlyConnError(cfg, unknownDB))
}
