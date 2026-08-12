//go:build integration

// Package server —— server↔agent↔MySQL 黄金路径 e2e（integration build tag）。
//
// 前置条件（环境变量，缺则 SKIP）：E2E_MYSQL_DSN、E2E_BINLOG_DIR；实例必须
// GTID+ROW（instanceOK，不满足则 SKIP）。本机无 docker 时由用户在
// docker-capable 主机/容器内执行，见 scripts/e2e/README.md（与
// internal/binlog/e2e_test.go 同约定）。
//
// 组装方式（Task 8 决策，详见 task-8-report.md）：
//   - 平台：newServer(dataDir, nil)（commander=nil → 真实 *hub.Hub），
//     SQLite 落在临时 dataDir，CA 落在 dataDir/ca.json。
//   - 真实 mTLS WebSocket：httptest.NewUnstartedServer(srv.Agent) +
//     srv.TLSConfig 起真实监听；agent 用 internal/ws/agent 的 Client 连接
//     （ServerName=127.0.0.1 → AGENT_CERT_HOSTS 注入 IP SAN）。
//   - agent 侧组件在测试进程内直接组装（不 spawn `agent serve` 子进程）：
//     daemon.NewDaemon + executor（本地 MySQL 工厂）+ ws/agent client +
//     dispatcher（scan/execute/resume/cancel 处理器，语义镜像 cmd/agent
//     serve.go）+ 测试 EventSink（复刻 serve.go 的 stream_event 信封约定）。
//   - 不做 collector 归档循环：扫描直接读 E2E_BINLOG_DIR（scanDeps.
//     ArchiveDir），与 collector 镜像进归档的内容等价；collector 循环本身有
//     独立 e2e（internal/collector TestE2E_ArchiveLoop），避免第二个复制连接
//     （固定 server-id）带来的慢与漂移。
//
// 过滤策略：与 binlog e2e 一致——action 前后紧邻采集 @@global.gtid_executed
// 差集作为 filter.gtidSet，精确命中 action 产生的事务。
package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	mysqldrv "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/connector"
	"github.com/a-shan/mysql-pitr/internal/daemon"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/server/pitr"
	"github.com/a-shan/mysql-pitr/internal/ws"
	wsagent "github.com/a-shan/mysql-pitr/internal/ws/agent"
	"github.com/a-shan/mysql-pitr/internal/ws/ca"
)

// e2eDB 是本组测试使用的数据库名（各测试自行 DROP+CREATE 保证干净）。
const e2eDB = "e2e_pitr"

// e2eHTTPClient 给所有 HTTP 调用一个上界，避免 hub 挂起时测试无限阻塞。
var e2eHTTPClient = &http.Client{Timeout: 60 * time.Second}

// ---------------------------------------------------------------------------
// 黄金路径：DELETE 回滚
// ---------------------------------------------------------------------------

// TestE2E_ServerAgentMySQL_GoldenPath 走完整 v3 流程：
// 注册/登录/建 org/注册 agent/approve（HTTP）→ agent 以 mTLS 连 hub →
// start（mode=sql，GTID 差集过滤）→ 等 ready → 预览 1 事务 → select →
// execute → 等 done → 断言数据库回滚正确。
//
// action 用 50 行 DELETE + batchSize=1：单条逆向语句的执行在 ~2-3ms 内完成，
// 存在「agent 的 op_done 先于 server 的 executing 落库到达 → ready→done 非法
// 转移被拒 → 操作卡 executing」的竞态；50 个批提交把窗口拉到几十毫秒，彻底
// 规避。执行器每次 Run 都新建 MySQL 连接（握手 ≥1ms），该竞态本已罕见，
// 这里做到可忽略。
func TestE2E_ServerAgentMySQL_GoldenPath(t *testing.T) {
	h, token, agentID := newE2EHarness(t)

	h.execSQL("DROP DATABASE IF EXISTS " + e2eDB + ";")
	h.execSQL("CREATE DATABASE " + e2eDB + ";")
	h.execSQL("CREATE TABLE " + e2eDB + ".t (id INT PRIMARY KEY, v INT);")
	h.execSQL("INSERT INTO " + e2eDB + ".t (id, v) VALUES " + multiValuePairs(100) + ";")

	// action：DELETE id<=50 → 逆向 50 条 INSERT → 回滚后 100 行，id=50 恢复
	// 为 v=50。
	gtidBefore := h.captureGTID()
	h.execSQL("DELETE FROM " + e2eDB + ".t WHERE id <= 50;")
	gtidAfter := h.captureGTID()
	added := subtractGTIDSets(gtidAfter, gtidBefore)
	require.False(t, added.IsEmpty(), "action produced no GTIDs")

	opID := h.startScan(token, agentID, "sql", added.String())
	h.waitForStatus(token, opID, pitr.StateReady)

	txs := h.transactions(token, opID)
	require.Len(t, txs, 1, "scan preview transactions")
	txID, _ := txs[0]["txId"].(string)
	require.NotEmpty(t, txID, "preview tx id")
	sqlStmts, ok := txs[0]["sql"].([]interface{})
	require.True(t, ok && len(sqlStmts) >= 50, "preview carries reverse SQL (mode=sql, got %d)", len(sqlStmts))

	h.selectTx(token, opID, txID)
	h.execute(token, opID, 1)
	h.waitForStatus(token, opID, pitr.StateDone)

	require.Equal(t, "100", h.queryValue("SELECT COUNT(*) FROM "+e2eDB+".t"), "rows deleted by action restored")
	require.Equal(t, "50", h.queryValue("SELECT v FROM "+e2eDB+".t WHERE id=50"), "restored row carries original value")
}

// ---------------------------------------------------------------------------
// cancel-during-scan（T6 修复轮交接覆盖）
// ---------------------------------------------------------------------------

// TestE2E_ServerAgentMySQL_CancelDuringScan 在扫描进行中取消操作：
// 大事务（5 万行 DELETE）让扫描耗时，start 返回后立即 cancel。cancel 是本地
// 权威转移，无论 agent 侧扫描是否已结束，终态都必须是 cancelled；迟到的
// scan_done（ready 转移）或 op_error（failed 转移）都被状态机拒绝。回滚从未
// 执行 → 数据库保持 action 后状态（全部被删）。
func TestE2E_ServerAgentMySQL_CancelDuringScan(t *testing.T) {
	h, token, agentID := newE2EHarness(t)

	h.execSQL("DROP DATABASE IF EXISTS " + e2eDB + ";")
	h.execSQL("CREATE DATABASE " + e2eDB + ";")
	h.execSQL("CREATE TABLE " + e2eDB + ".t (id INT PRIMARY KEY);")
	h.execSQL("INSERT INTO " + e2eDB + ".t VALUES " + multiValues(50000) + ";")

	gtidBefore := h.captureGTID()
	h.execSQL("DELETE FROM " + e2eDB + ".t;") // 大事务 → 扫描耗时
	gtidAfter := h.captureGTID()
	added := subtractGTIDSets(gtidAfter, gtidBefore)
	require.False(t, added.IsEmpty(), "action produced no GTIDs")

	opID := h.startScan(token, agentID, "meta", added.String())

	// start 返回时 agent 侧扫描仍在异步执行，立即 cancel。
	resp, out := e2ePost(t, h.webURL, "/api/pitr/"+opID+"/cancel", nil, token)
	require.Equal(t, http.StatusOK, resp.StatusCode, "cancel: %v", out)

	st := h.waitForStatus(token, opID, pitr.StateCancelled)
	require.Equal(t, pitr.StateCancelled, st)

	// 短暂观察：不得被迟到的 scan_done 推回 ready、或被 op_error 推成 failed。
	time.Sleep(1000 * time.Millisecond)
	require.Equal(t, "cancelled", h.status(token, opID)["status"], "cancelled must be terminal")

	// 回滚从未执行 → 数据保持 action 后状态（全部被删）。
	require.Equal(t, "0", h.queryValue("SELECT COUNT(*) FROM "+e2eDB+".t"))
}

// ---------------------------------------------------------------------------
// pause→resume（T6 修复轮交接覆盖）
// ---------------------------------------------------------------------------

// TestE2E_ServerAgentMySQL_PauseResume 在执行中暂停并恢复：
// 1000 行 DELETE → mode=sql 得到 1000 条逆向 INSERT → execute batchSize=1 →
// 立即 pause（1000 个批提交给了 cancel 充足的时间窗口）→ 等 paused →
// 让旧的 op_done(paused) 确认落地（防 stale ack 在 resume 后把 executing
// 打回 paused）→ resume → 等 done → 全部行恢复。
func TestE2E_ServerAgentMySQL_PauseResume(t *testing.T) {
	h, token, agentID := newE2EHarness(t)

	h.execSQL("DROP DATABASE IF EXISTS " + e2eDB + ";")
	h.execSQL("CREATE DATABASE " + e2eDB + ";")
	h.execSQL("CREATE TABLE " + e2eDB + ".t (id INT PRIMARY KEY);")
	h.execSQL("INSERT INTO " + e2eDB + ".t VALUES " + multiValues(1000) + ";")

	gtidBefore := h.captureGTID()
	h.execSQL("DELETE FROM " + e2eDB + ".t WHERE id <= 1000;")
	gtidAfter := h.captureGTID()
	added := subtractGTIDSets(gtidAfter, gtidBefore)
	require.False(t, added.IsEmpty(), "action produced no GTIDs")

	opID := h.startScan(token, agentID, "sql", added.String())
	h.waitForStatus(token, opID, pitr.StateReady)

	txs := h.transactions(token, opID)
	require.Len(t, txs, 1, "scan preview transactions")
	txID, _ := txs[0]["txId"].(string)
	require.NotEmpty(t, txID)
	stmts, _ := txs[0]["sql"].([]interface{})
	require.True(t, len(stmts) >= 1000, "preview carries one reverse statement per row (got %d)", len(stmts))

	h.selectTx(token, opID, txID)
	h.execute(token, opID, 1)

	resp, out := e2ePost(t, h.webURL, "/api/pitr/"+opID+"/pause", nil, token)
	require.Equal(t, http.StatusOK, resp.StatusCode, "pause: %v", out)

	h.waitForStatus(token, opID, pitr.StatePaused)

	// 让 agent 侧被取消的 executor 的 op_done(paused) 确认落地，避免它迟到到
	// resume 之后把 executing 打回 paused（生产路径同样存在该竞态，这里用有界
	// 等待规避）。等待后确认状态仍是 paused。
	time.Sleep(1500 * time.Millisecond)
	require.Equal(t, "paused", h.status(token, opID)["status"], "op must stay paused while settling")

	resp, out = e2ePost(t, h.webURL, "/api/pitr/"+opID+"/resume", nil, token)
	require.Equal(t, http.StatusOK, resp.StatusCode, "resume: %v", out)

	h.waitForStatus(token, opID, pitr.StateDone)
	require.Equal(t, "1000", h.queryValue("SELECT COUNT(*) FROM "+e2eDB+".t"), "all rows restored after resume")

	// 判别性断言（最终评审 C1）：resume 必须从 agent 检查点续跑（CmdResume →
	// executor.Resume），而不是清检查点从 0 重跑（CmdExecute → executor.Run
	// 会把暂停前已提交的批次再执行一遍、产生主键冲突错误）。因此 done 审计的
	// errorDetails 必须为空——出现任何 statement failed 即说明 resume 走了
	// 重跑路径。
	require.Eventually(t, func() bool {
		for _, e := range h.auditForOp(opID) {
			if e["status"] == "done" {
				details, _ := e["errorDetails"].(string)
				return details == ""
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond,
		"done audit must carry no statement errors — resume must continue from the checkpoint, not re-run")
}

// ---------------------------------------------------------------------------
// 装配（server + HTTP 引导 + agent 组件）
// ---------------------------------------------------------------------------

// e2eHarness 持有一组测试共享的最小状态（web 端点 + 数据连接 + 引导身份）。
type e2eHarness struct {
	t      *testing.T
	webURL string
	token  string
	orgID  string
	db     *sql.DB
}

// newE2EHarness 完成全部装配并返回 (harness, token, agentID)。不满足前置条件
// 时直接 t.Skip。清理（server/client/httptest/db）注册到 t.Cleanup。
func newE2EHarness(t *testing.T) (*e2eHarness, string, string) {
	t.Helper()
	dsn := os.Getenv("E2E_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set E2E_MYSQL_DSN to run integration tests")
	}
	binlogDir := os.Getenv("E2E_BINLOG_DIR")
	if binlogDir == "" {
		t.Skip("set E2E_BINLOG_DIR to run integration tests")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	if reason, ok := instanceOK(t, db); !ok {
		t.Skipf("E2E instance not suitable: %s", reason)
	}
	connCfg, err := dsnToConnConfig(dsn)
	if err != nil {
		t.Skipf("cannot parse E2E_MYSQL_DSN: %v", err)
	}

	// ---- 平台（真实 hub + SQLite + CA + 两个监听）----
	dataDir := t.TempDir()
	t.Setenv("AGENT_CERT_HOSTS", "127.0.0.1,localhost")
	srv, err := newServer(dataDir, nil) // commander=nil → 真实 hub
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	webSrv := httptest.NewServer(srv.Web)
	t.Cleanup(webSrv.Close)

	agentSrv := httptest.NewUnstartedServer(srv.Agent)
	agentSrv.TLS = srv.TLSConfig
	agentSrv.StartTLS()
	t.Cleanup(agentSrv.Close)
	agentURL := "wss://" + strings.TrimPrefix(agentSrv.URL, "https://") + "/ws/agent"

	// ---- HTTP 引导：注册/登录/建 org/注册 agent/approve ----
	token, agentID, orgID := bootstrapPlatform(t, webSrv.URL)

	// ---- 组装并连接 agent（测试进程内）----
	client := assembleAgent(t, agentURL, agentID, dataDir, connCfg, binlogDir, t.TempDir())
	t.Cleanup(func() { _ = client.Close() })

	h := &e2eHarness{t: t, webURL: webSrv.URL, token: token, orgID: orgID, db: db}
	require.Eventually(t, func() bool { return srv.Hub.IsConnected(agentID) },
		30*time.Second, 200*time.Millisecond, "agent should connect to hub")
	return h, token, agentID
}

// bootstrapPlatform 走真实 HTTP API：注册用户 → 登录 → 建 org → 注册 agent
// （pending）→ approve。返回 (token, agentID, orgID)。
func bootstrapPlatform(t *testing.T, webURL string) (string, string, string) {
	t.Helper()

	resp, _ := e2ePost(t, webURL, "/api/auth/register",
		map[string]interface{}{"email": "e2e@example.com", "password": "secret123"}, "")
	require.Equal(t, http.StatusCreated, resp.StatusCode, "register")

	resp, out := e2ePost(t, webURL, "/api/auth/login",
		map[string]interface{}{"email": "e2e@example.com", "password": "secret123"}, "")
	require.Equal(t, http.StatusOK, resp.StatusCode, "login")
	token, _ := out["token"].(string)
	require.NotEmpty(t, token, "login token")

	resp, out = e2ePost(t, webURL, "/api/orgs", map[string]interface{}{"name": "E2E Org"}, token)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create org")
	org := out["organization"].(map[string]interface{})
	orgID, _ := org["id"].(string)
	require.NotEmpty(t, orgID, "org id")

	resp, out = e2ePost(t, webURL, "/api/agents/register",
		map[string]interface{}{"orgId": orgID, "hostname": "e2e-agent"}, token)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "register agent")
	agt := out["agent"].(map[string]interface{})
	agentID, _ := agt["id"].(string)
	require.NotEmpty(t, agentID, "agent id")

	resp, out = e2ePost(t, webURL, "/api/agents/"+agentID+"/approve", nil, token)
	require.Equal(t, http.StatusOK, resp.StatusCode, "approve agent")

	return token, agentID, orgID
}

// assembleAgent 在测试进程内组装 agent 侧组件：daemon（scan/execute/resume/
// cancel 处理层 + 本地 MySQL executor）+ ws/agent client（mTLS 连 hub）+
// dispatcher + stream_event 信封 sink。证书用 server 的内部 CA 现场签发
// （CN=agentID），实现与 `agent serve` 相同的信任链。
func assembleAgent(t *testing.T, agentURL, agentID, dataDir string, connCfg connector.ConnConfig, binlogDir, cpDir string) *wsagent.Client {
	t.Helper()
	certFile, keyFile, caFile := writeAgentCredentials(t, agentID, dataDir)

	sink := &e2eSink{}
	d := daemon.NewDaemon(
		daemon.ScanDeps{
			ArchiveDir:    binlogDir,
			SchemaFetcher: &e2eSchemaFetcher{connCfg: connCfg},
		},
		executor.NewExecutor(
			func(executor.Plan) (executor.DB, error) {
				conn := connector.NewMySQLConnector()
				if err := conn.Connect(connCfg); err != nil {
					return nil, err
				}
				return conn.AsDB(), nil
			},
			executor.NewFileCheckpointStore(cpDir),
		),
		nil, // archive stateFn —— 本测试不启动 collector 循环
		sink,
	)

	client := wsagent.NewClient(wsagent.ClientConfig{
		ServerURL: agentURL,
		CertFile:  certFile,
		KeyFile:   keyFile,
		CAPath:    caFile,
		AgentID:   agentID,
	})
	sink.client = client

	agent := &e2eAgent{daemon: d}
	dispatcher := wsagent.NewDispatcher()
	dispatcher.RegisterHandler(ws.CmdScan, agent.handleScan)
	dispatcher.RegisterHandler(ws.CmdExecute, agent.handleExecute)
	dispatcher.RegisterHandler(ws.CmdResume, agent.handleResume)
	dispatcher.RegisterHandler(ws.CmdCancel, agent.handleCancel)
	client.SetDispatcher(dispatcher)

	require.NoError(t, client.Connect(context.Background()), "agent ws connect")
	return client
}

// writeAgentCredentials 用 server 的 CA（dataDir/ca.json）为 agent 签发一张
// CN=agentID 的客户端证书，并把证书/私钥/根证书写到临时文件（ws client 走
// 文件路径）。CA.GenerateRoot 在根已存在时是加载既有 root 的无操作，因此与
// server 共享同一根。
func writeAgentCredentials(t *testing.T, agentID, dataDir string) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()

	storage := ca.NewFileStorage(filepath.Join(dataDir, "ca.json"))
	testCA := ca.NewCA(storage)
	_, err := testCA.GenerateRoot()
	require.NoError(t, err, "load server root CA")

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:            pkix.Name{CommonName: agentID},
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}, key)
	require.NoError(t, err)
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	certPEM, err := testCA.SignCSR(csrPEM, agentID)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	rootPEM, _, err := storage.LoadRootCA()
	require.NoError(t, err, "read root CA PEM")

	certFile = filepath.Join(dir, "client.pem")
	keyFile = filepath.Join(dir, "client-key.pem")
	caFile = filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(caFile, rootPEM, 0o600))
	return certFile, keyFile, caFile
}

// ---------------------------------------------------------------------------
// agent 侧：命令处理器 + 事件 sink + schema fetcher（镜像 cmd/agent serve.go）
// ---------------------------------------------------------------------------

// e2eAgent 持有一个 daemon，命令处理器语义与 cmd/agent serve.go 的
// handleScan/handleExecute/handleResume/handleCancel 一一对应（Phase 3 交接的
// wire 契约：command ID = opID = daemon op 注册表键）。
type e2eAgent struct {
	daemon *daemon.Daemon
}

func (a *e2eAgent) handleScan(ctx context.Context, cmd ws.Command) *ws.Response {
	var req ws.ScanRequest
	if err := decodeE2EParams(cmd, &req); err != nil {
		return e2eErrResp(cmd, "scan: %v", err)
	}
	if err := a.daemon.Scan(ctx, cmd.Cmd, req); err != nil {
		return e2eErrResp(cmd, "scan: %v", err)
	}
	return e2eOKResp(cmd, map[string]interface{}{"accepted": true, "operationId": cmd.Cmd})
}

func (a *e2eAgent) handleExecute(ctx context.Context, cmd ws.Command) *ws.Response {
	var req ws.ExecuteRequest
	if err := decodeE2EParams(cmd, &req); err != nil {
		return e2eErrResp(cmd, "execute: %v", err)
	}
	if err := a.daemon.Execute(ctx, cmd.Cmd, req); err != nil {
		return e2eErrResp(cmd, "execute: %v", err)
	}
	return e2eOKResp(cmd, map[string]interface{}{"accepted": true, "operationId": cmd.Cmd})
}

func (a *e2eAgent) handleResume(ctx context.Context, cmd ws.Command) *ws.Response {
	var req ws.ExecuteRequest
	if err := decodeE2EParams(cmd, &req); err != nil {
		return e2eErrResp(cmd, "resume: %v", err)
	}
	if err := a.daemon.Resume(ctx, cmd.Cmd, req); err != nil {
		return e2eErrResp(cmd, "resume: %v", err)
	}
	return e2eOKResp(cmd, map[string]interface{}{"accepted": true, "operationId": cmd.Cmd})
}

func (a *e2eAgent) handleCancel(ctx context.Context, cmd ws.Command) *ws.Response {
	opID, _ := cmd.Params["operationId"].(string)
	if opID == "" {
		return e2eErrResp(cmd, "cancel: missing required param 'operationId'")
	}
	if err := a.daemon.CancelOp(opID); err != nil {
		return e2eErrResp(cmd, "cancel: %v", err)
	}
	return e2eOKResp(cmd, map[string]interface{}{"cancelled": true, "operationId": opID})
}

// e2eSink 实现 daemon.EventSink：把流事件包装成 stream_event 信封经 client
// 单向推给 server hub。信封与 cmd/agent serve.go 的 streamEventCommand 完全
// 一致（{cmd:"ev-<opId>", type:"stream_event", params:{id,kind,data}}）。
type e2eSink struct {
	client *wsagent.Client
}

func (s *e2eSink) Send(ev ws.StreamEvent) error {
	if s.client == nil {
		return nil
	}
	return s.client.Send(ws.Command{
		Cmd:  "ev-" + ev.ID,
		Type: ws.CmdStreamEvent,
		Params: map[string]interface{}{
			"id":   ev.ID,
			"kind": ev.Kind,
			"data": json.RawMessage(ev.Data),
		},
	})
}

// e2eSchemaFetcher 复用 connector.FetchSchema（惰性建立一条 MySQL 连接），与
// cmd/agent serve.go 的 mysqlSchemaFetcher 行为一致。
type e2eSchemaFetcher struct {
	connCfg connector.ConnConfig
	mu      sync.Mutex
	conn    *connector.MySQLConnector
}

func (f *e2eSchemaFetcher) FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		conn := connector.NewMySQLConnector()
		if err := conn.Connect(f.connCfg); err != nil {
			return binlog.TableSchema{}, err
		}
		f.conn = conn
	}
	return f.conn.FetchSchema(ctx, schema, table)
}

func decodeE2EParams(cmd ws.Command, v interface{}) error {
	if len(cmd.Params) == 0 {
		return nil
	}
	data, err := json.Marshal(cmd.Params)
	if err != nil {
		return fmt.Errorf("marshal params: %w", err)
	}
	return json.Unmarshal(data, v)
}

func e2eOKResp(cmd ws.Command, result interface{}) *ws.Response {
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusOK, Result: result}
}

func e2eErrResp(cmd ws.Command, format string, args ...interface{}) *ws.Response {
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusError, Error: fmt.Sprintf(format, args...)}
}

// ---------------------------------------------------------------------------
// HTTP 助手
// ---------------------------------------------------------------------------

func e2ePost(t *testing.T, base, path string, body interface{}, token string) (*http.Response, map[string]interface{}) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(http.MethodPost, base+path, &buf)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e2eHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func e2eGetJSON(t *testing.T, base, path, token string) map[string]interface{} {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e2eHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", path)
	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// ---------------------------------------------------------------------------
// PITR 流程助手
// ---------------------------------------------------------------------------

func (h *e2eHarness) startScan(token, agentID, mode, gtidSet string) string {
	h.t.Helper()
	resp, out := e2ePost(h.t, h.webURL, "/api/pitr/start", map[string]interface{}{
		"agentId": agentID,
		"type":    "pitr",
		"mode":    mode,
		"filter": map[string]interface{}{
			"tables":  []map[string]string{{"schema": e2eDB, "table": "t"}},
			"gtidSet": gtidSet,
		},
	}, token)
	require.Equal(h.t, http.StatusCreated, resp.StatusCode, "start scan: %v", out)
	opID, _ := out["operationId"].(string)
	require.NotEmpty(h.t, opID, "operation id")
	return opID
}

func (h *e2eHarness) status(token, opID string) map[string]interface{} {
	h.t.Helper()
	return e2eGetJSON(h.t, h.webURL, "/api/pitr/"+opID+"/status", token)
}

func (h *e2eHarness) transactions(token, opID string) []map[string]interface{} {
	h.t.Helper()
	out := e2eGetJSON(h.t, h.webURL, "/api/pitr/"+opID+"/transactions", token)
	raw, _ := out["transactions"].([]interface{})
	txs := make([]map[string]interface{}, 0, len(raw))
	for _, r := range raw {
		txs = append(txs, r.(map[string]interface{}))
	}
	return txs
}

func (h *e2eHarness) selectTx(token, opID, txID string) {
	h.t.Helper()
	resp, out := e2ePost(h.t, h.webURL, "/api/pitr/"+opID+"/select",
		map[string]interface{}{"txIds": []string{txID}}, token)
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "select: %v", out)
}

func (h *e2eHarness) execute(token, opID string, batchSize int) {
	h.t.Helper()
	resp, out := e2ePost(h.t, h.webURL, "/api/pitr/"+opID+"/execute",
		map[string]interface{}{"batchSize": batchSize}, token)
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "execute: %v", out)
}

// auditForOp 返回某操作的全部审计条目（GET /api/audit?org_id&operation_id，
// 响应为裸 JSON 数组）。
func (h *e2eHarness) auditForOp(opID string) []map[string]interface{} {
	h.t.Helper()
	body := e2eRawBody(h.t, h.webURL, "/api/audit?org_id="+h.orgID+"&operation_id="+opID, h.token)
	var entries []map[string]interface{}
	require.NoError(h.t, json.Unmarshal([]byte(body), &entries))
	return entries
}

// e2eRawBody 返回 GET 响应的原始 body（e2eGetJSON 只解 map 对象，审计 Query
// 返回裸数组）。
func e2eRawBody(t *testing.T, base, path, token string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e2eHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", path)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

// waitForStatus 轮询 /status 直到命中 want 之一；提前到达其他终态立即失败。
func (h *e2eHarness) waitForStatus(token, opID string, want ...pitr.OperationState) pitr.OperationState {
	h.t.Helper()
	wantSet := map[pitr.OperationState]bool{}
	for _, w := range want {
		wantSet[w] = true
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := h.status(token, opID)["status"].(string)
		cur := pitr.OperationState(st)
		if wantSet[cur] {
			return cur
		}
		if pitr.IsTerminal(cur) {
			h.t.Fatalf("op %s reached terminal state %q before wanted %v", opID, cur, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	h.t.Fatalf("op %s did not reach %v within 90s", opID, want)
	return ""
}

// ---------------------------------------------------------------------------
// MySQL / GTID 助手（与 internal/binlog/e2e_test.go 同模式）
// ---------------------------------------------------------------------------

func (h *e2eHarness) execSQL(q string) {
	h.t.Helper()
	_, err := h.db.Exec(q)
	require.NoError(h.t, err, "exec: %s", q)
}

func (h *e2eHarness) queryValue(q string) string {
	h.t.Helper()
	var raw interface{}
	require.NoError(h.t, h.db.QueryRow(q).Scan(&raw))
	switch v := raw.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (h *e2eHarness) captureGTID() *gomysql.MysqlGTIDSet {
	h.t.Helper()
	var raw string
	require.NoError(h.t, h.db.QueryRow("SELECT @@global.gtid_executed").Scan(&raw))
	if strings.TrimSpace(raw) == "" {
		s := gomysql.NewMysqlGTIDSet()
		return &s
	}
	s, err := binlog.ParseGTIDSet("mysql", raw)
	require.NoError(h.t, err, "parse gtid_executed %q", raw)
	return s.(*gomysql.MysqlGTIDSet)
}

func instanceOK(t *testing.T, db *sql.DB) (string, bool) {
	t.Helper()
	var gtidMode, binlogFormat string
	if err := db.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		return "cannot query @@gtid_mode: " + err.Error(), false
	}
	if err := db.QueryRow("SELECT @@binlog_format").Scan(&binlogFormat); err != nil {
		return "cannot query @@binlog_format: " + err.Error(), false
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return "gtid_mode is " + gtidMode + " (want ON)", false
	}
	if !strings.EqualFold(binlogFormat, "ROW") {
		return "binlog_format is " + binlogFormat + " (want ROW)", false
	}
	return "", true
}

// dsnToConnConfig 把 go-sql-driver DSN 转成 connector.ConnConfig（agent 的
// 本地 MySQL 连接配置，等价于 cmd/agent config 的 mysql 段）。
func dsnToConnConfig(dsn string) (connector.ConnConfig, error) {
	cfg, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return connector.ConnConfig{}, err
	}
	host, port := cfg.Addr, 3306
	if h, p, err := net.SplitHostPort(cfg.Addr); err == nil {
		host = h
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	return connector.ConnConfig{
		Host:     host,
		Port:     port,
		User:     cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
	}, nil
}

// subtractGTIDSets 返回 after 中不在 before 里的 GTID（action 新增的 GTID）。
// 与 binlog e2e 一致，按 map[uuid]map[tag]IntervalSlice 结构在区间粒度上做
// 差集（Interval 为 [Start, Stop) 半开区间，AddGTIDWithTag 逐 gno 补回）。
func subtractGTIDSets(after, before *gomysql.MysqlGTIDSet) *gomysql.MysqlGTIDSet {
	diff := gomysql.NewMysqlGTIDSet()
	for sid, tags := range *after {
		for tag, ivs := range tags {
			beforeIvs := (*before)[sid][tag]
			for _, iv := range ivs {
				for _, rem := range subtractIntervals(iv, beforeIvs) {
					for gno := rem.Start; gno < rem.Stop; gno++ {
						diff.AddGTIDWithTag(sid, tag, gno)
					}
				}
			}
		}
	}
	return &diff
}

func subtractIntervals(iv gomysql.Interval, before []gomysql.Interval) []gomysql.Interval {
	var out []gomysql.Interval
	cur := iv.Start
	for _, b := range before {
		if b.Stop <= cur {
			continue
		}
		if b.Start >= iv.Stop {
			break
		}
		if b.Start > cur {
			out = append(out, gomysql.Interval{Start: cur, Stop: b.Start})
		}
		if b.Stop >= iv.Stop {
			cur = iv.Stop
			break
		}
		cur = b.Stop
	}
	if cur < iv.Stop {
		out = append(out, gomysql.Interval{Start: cur, Stop: iv.Stop})
	}
	return out
}

// multiValues 生成 "(1),(2),...,(n)" — 单列批量 INSERT 用。
func multiValues(n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("(%d)", i+1)
	}
	return strings.Join(parts, ",")
}

// multiValuePairs 生成 "(1,1),(2,2),...,(n,n)" — 双列批量 INSERT 用。
func multiValuePairs(n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("(%d,%d)", i+1, i+1)
	}
	return strings.Join(parts, ",")
}
