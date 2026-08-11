package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	"github.com/a-shan/mysql-pitr/internal/reverse"
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
// 返回的 *int32 在工厂被调用时 +1（工厂调用发生在检查点文件落盘之后、goroutine
// 结束之前）——接受测试用它等待异步 goroutine 收尾，避免测试返回时后台 goroutine
// 仍在向 t.TempDir 写检查点文件（Windows 上会与 TempDir 清理竞争）。
func (d *serveDaemon) withFailingExec(t *testing.T) (*serveDaemon, *int32) {
	t.Helper()
	var calls int32
	d.daemon = daemon.NewDaemon(
		daemon.ScanDeps{ArchiveDir: t.TempDir(), Logger: d.logger()},
		executor.NewExecutor(func(plan executor.Plan) (executor.DB, error) {
			atomic.AddInt32(&calls, 1)
			return nil, errors.New("test: db factory disabled")
		}, executor.NewFileCheckpointStore(t.TempDir())),
		d.loopState, d,
	)
	return d, &calls
}

// ---------------------------------------------------------------------------
// Constructor wiring
// ---------------------------------------------------------------------------

func TestNewServeDaemon_WiresDaemon(t *testing.T) {
	d := testServeDaemon(t)
	require.NotNil(t, d.daemon, "daemon command layer must be wired")
	require.Nil(t, d.loop, "loop stays nil until startArchiveLoop")
}

// TestNewServeDaemon_ExecutorWired 验证 executor 构造接线：archive.dir 配置时
// executor 非 nil（检查点目录派生自归档目录的兄弟目录）；未配置 archive 时
// executor 返回 nil（Execute/Resume 报 "executor not configured"，且不会把
// checkpoints 落到进程 cwd）。
func TestNewServeDaemon_ExecutorWired(t *testing.T) {
	cfg := &config.Config{
		MySQL: config.MySQLConfig{
			Host: "127.0.0.1", Port: 3306, User: "u", Password: "p", Database: "d",
		},
		Archive: &config.ArchiveConfig{Dir: filepath.Join(t.TempDir(), "archive"), ServerID: 1},
	}
	d := newServeDaemon(cfg, "agent-1")
	require.NotNil(t, d.daemon, "daemon command layer must be wired")
	require.NotNil(t, d.newExecutor(), "archive.dir 配置时 executor 必须构造")

	// testServeDaemon 的 cfg 未配置 archive：executor 应禁用（不启用执行）。
	d2 := testServeDaemon(t)
	require.Nil(t, d2.newExecutor(), "未配置 archive 时不启用执行")
}

// TestNewServeDaemon_ExecutorFactoryBindsLocalMySQL 验证 executor 工厂绑定 agent
// 本地 MySQL 配置而非 Plan.DSN：cfg 指向一个不监听的高端口（3399），Run 时工厂
// 必须按本地配置发起连接——错误须来自 connector.Connect 的主动 ping 且含该端口。
// 单测环境无真实 MySQL，以「按本地 cfg 尝试连接并失败」反证工厂绑定正确。
func TestNewServeDaemon_ExecutorFactoryBindsLocalMySQL(t *testing.T) {
	const bindPort = 3399
	cfg := &config.Config{
		MySQL: config.MySQLConfig{
			Host: "127.0.0.1", Port: bindPort, User: "u", Password: "p", Database: "d",
		},
		Archive: &config.ArchiveConfig{Dir: filepath.Join(t.TempDir(), "archive"), ServerID: 1},
	}
	d := newServeDaemon(cfg, "agent-1")
	exec := d.newExecutor()
	require.NotNil(t, exec)

	// Plan.DSN 为空；工厂必须忽略它并按本地 MySQL 配置（127.0.0.1:3399）连接。
	_, err := exec.Run(context.Background(), executor.Plan{
		OperationID: "op-factory-bind",
		Statements:  []reverse.Statement{{SQL: "SELECT 1"}},
	}, nil)
	require.Error(t, err, "本环境无 MySQL 监听 3399，Run 必须失败")
	assert.True(t, strings.Contains(err.Error(), "3399"),
		"工厂应按 agent 本地 MySQL 配置（端口 %d）连接而非 Plan.DSN；err=%v", bindPort, err)
	assert.Contains(t, err.Error(), "connector: ping failed",
		"连接错误应来自 connector.Connect 的主动 ping（工厂时点即失败）")
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
	d, factoryCalls := testServeDaemon(t).withFailingExec(t)
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
	// cancel 契约：accepted 响应的 operationId 回显命令 ID（cmd.Cmd）——daemon
	// op 注册表以命令 ID 为键，server 须把该值原样发给 cancel。请求里的
	// operationId 是检查点文件名（另一命名空间），不得混用。
	assert.Equal(t, "exec-1", result["operationId"])

	// 等异步执行 goroutine 到达工厂（检查点已落盘、不再写临时目录）再返回，
	// 避免测试 teardown 与后台写入竞争（Windows TempDir 清理竞态）。
	require.Eventually(t, func() bool { return atomic.LoadInt32(factoryCalls) > 0 },
		2*time.Second, 5*time.Millisecond, "execute goroutine should reach the db factory")
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
	d, factoryCalls := testServeDaemon(t).withFailingExec(t)
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

	result, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	// 与 execute 同一契约：operationId 回显命令 ID（cmd.Cmd）。
	assert.Equal(t, "resume-1", result["operationId"])

	// 同 TestHandleExecute_Accepted：等异步 goroutine 到达工厂再返回。
	require.Eventually(t, func() bool { return atomic.LoadInt32(factoryCalls) > 0 },
		2*time.Second, 5*time.Millisecond, "resume goroutine should reach the db factory")
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
