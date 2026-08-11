package main

import (
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/config"
	"github.com/a-shan/mysql-pitr/internal/connector"
	"github.com/a-shan/mysql-pitr/internal/daemon"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/stream"
	"github.com/a-shan/mysql-pitr/internal/ws"
	wsagent "github.com/a-shan/mysql-pitr/internal/ws/agent"
)

// ServeOptions holds parameters for the `serve` daemon subcommand.
type ServeOptions struct {
	ConfigFile string
	Passphrase string
	AgentID    string
}

// serveDaemon 持有 agent daemon 的共享状态：归档循环、命令处理层与 WS 客户端。
type serveDaemon struct {
	cfg     *config.Config
	agentID string
	connCfg connector.ConnConfig

	client  *wsagent.Client
	started time.Time

	loopMu sync.Mutex       // 保护 loop 字段（startArchiveLoop 写 / loopState 读）
	loop   *collector.Loop  // 归档循环（startArchiveLoop 启动）
	daemon *daemon.Daemon   // scan/execute/resume/cancel/archive_status 命令处理层

	rootCtx    context.Context
	rootCancel context.CancelFunc
	stopOnce   sync.Once
	stopCh     chan struct{}
}

func newServeDaemon(cfg *config.Config, agentID string) *serveDaemon {
	ctx, cancel := context.WithCancel(context.Background())
	d := &serveDaemon{
		cfg:        cfg,
		agentID:    agentID,
		connCfg:    cfg.MySQL.BuildConnConfig(),
		started:    time.Now(),
		rootCtx:    ctx,
		rootCancel: cancel,
		stopCh:     make(chan struct{}),
	}
	d.daemon = daemon.NewDaemon(
		daemon.ScanDeps{
			ArchiveDir:    d.archiveDir(),
			SchemaFetcher: d.newSchemaFetcher(),
			Logger:        d.logger(),
		},
		d.newExecutor(),
		d.loopState,
		d, // serveDaemon 实现 daemon.EventSink（Send 方法）
	)
	return d
}

// archiveDir 返回归档目录（cfg.Archive 未配置时为 ""）。
func (d *serveDaemon) archiveDir() string {
	if d.cfg.Archive == nil {
		return ""
	}
	return d.cfg.Archive.Dir
}

// loopState 是 daemon.ArchiveStatus 的状态源：循环未启动时返回零值状态。
func (d *serveDaemon) loopState() collector.State {
	d.loopMu.Lock()
	l := d.loop
	d.loopMu.Unlock()
	if l == nil {
		return collector.State{}
	}
	return l.State()
}

// newExecutor 构造 Phase 2 的执行器。DBConnFactory 用 agent 自身的 MySQL 连接
// 配置打开连接（executor.Plan.DSN 在 Phase 2 为空；Phase 3 由 server 层注入
// DSN 后替换）。data_dir 未配置时不启用执行（Execute/Resume 返回错误）。
func (d *serveDaemon) newExecutor() executor.Executor {
	if d.cfg.DataDir == "" {
		return nil
	}
	cpDir := filepath.Join(d.cfg.DataDir, "checkpoints")
	_ = os.MkdirAll(cpDir, 0o755) // FileCheckpointStore 不自动建目录
	return executor.NewExecutor(
		func(plan executor.Plan) (executor.DB, error) {
			db, err := sql.Open("mysql", d.cfg.MySQL.BuildDSN())
			if err != nil {
				return nil, fmt.Errorf("open mysql: %w", err)
			}
			return connector.NewMySQLConnectorWithDB(db), nil
		},
		executor.NewFileCheckpointStore(cpDir),
	)
}

// logger 返回归档循环与命令层共用的结构化日志器（stderr 文本格式）。
func (d *serveDaemon) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

// streamEventType 是 daemon 流事件推送的 wire 命令类型。
const streamEventType = "stream_event"

// streamEventCommand 把 StreamEvent 包装成单向推送的 ws.Command。信封约定
// （Phase 3 server 按此解包）：
//
//	{ "cmd": "ev-<opId>", "type": "stream_event",
//	  "params": { "id": "<opId>", "kind": "<event kind>", "data": <原始 JSON> } }
//
// kind 取值见 ws.EvTxMeta/EvSQL/EvScanDone/EvProgress/EvOpDone/EvOpError；
// data 是 StreamEvent.Data 的原始 JSON（json.RawMessage，序列化时保持原样）。
func streamEventCommand(ev ws.StreamEvent) ws.Command {
	return ws.Command{
		Cmd:  "ev-" + ev.ID,
		Type: streamEventType,
		Params: map[string]interface{}{
			"id":   ev.ID,
			"kind": ev.Kind,
			"data": json.RawMessage(ev.Data),
		},
	}
}

// Send 实现 daemon.EventSink：把 StreamEvent 经 client 推给 server（单向推送，
// 不等响应）。客户端未连接时静默丢弃——daemon 调用方不处理 Send 错误。
func (d *serveDaemon) Send(ev ws.StreamEvent) error {
	if d.client == nil {
		return nil
	}
	return d.client.Send(streamEventCommand(ev))
}

// mysqlSchemaFetcher 实现 binlog.SchemaFetcher：惰性建立并复用 MySQL 连接，
// 委托 connector.FetchSchema。一次扫描内的 schema 缓存由 scan.Stream 负责
// （每个 (schema,table) 只拉一次）。
type mysqlSchemaFetcher struct {
	connCfg connector.ConnConfig

	mu   sync.Mutex
	conn *connector.MySQLConnector
}

func (d *serveDaemon) newSchemaFetcher() binlog.SchemaFetcher {
	return &mysqlSchemaFetcher{connCfg: d.connCfg}
}

func (f *mysqlSchemaFetcher) FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		conn := connector.NewMySQLConnector()
		if err := conn.Connect(f.connCfg); err != nil {
			return binlog.TableSchema{}, friendlyConnError(f.connCfg, err)
		}
		f.conn = conn
	}
	return f.conn.FetchSchema(ctx, schema, table)
}

// ---------------------------------------------------------------------------
// commandResponse helpers
// ---------------------------------------------------------------------------

func okResp(cmd ws.Command, result interface{}) *ws.Response {
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusOK, Result: result}
}

func errResp(cmd ws.Command, format string, args ...interface{}) *ws.Response {
	return &ws.Response{Cmd: cmd.Cmd, Status: ws.StatusError, Error: fmt.Sprintf(format, args...)}
}

// paramString 从命令的 params map 读字符串值（trim 后返回）。
func paramString(params map[string]interface{}, key string) string {
	s, _ := params[key].(string)
	return strings.TrimSpace(s)
}

// decodeParams 把命令 params（map[string]interface{}）JSON 往返解码到目标
// 结构（ws.ScanRequest / ws.ExecuteRequest）。空 params 视为零值目标。
func decodeParams(cmd ws.Command, v interface{}) error {
	if len(cmd.Params) == 0 {
		return nil
	}
	data, err := json.Marshal(cmd.Params)
	if err != nil {
		return fmt.Errorf("marshal params: %w", err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("parse params: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// friendlyConnError rewrites MySQL 1045 (access denied) into an actionable
// message. The agent connects from a container/bridge network, so a MySQL user
// created for 'localhost' only is rejected even with the correct password.
func friendlyConnError(cfg connector.ConnConfig, err error) error {
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1045 {
		return fmt.Errorf(
			"MySQL access denied (user %q, database %q): %s. "+
				"The MySQL account must be allowed from this agent's host. "+
				"On the MySQL server run: CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '<password>'; "+
				"GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@'%%'; "+
				"GRANT SELECT ON `%s`.* TO '%s'@'%%'; FLUSH PRIVILEGES;",
			cfg.User, cfg.Database, me.Message, cfg.User, cfg.User, cfg.Database, cfg.User)
	}
	return err
}

// handleStatus answers the `status` command with daemon and MySQL connectivity.
func (d *serveDaemon) handleStatus(ctx context.Context, cmd ws.Command) *ws.Response {
	return okResp(cmd, map[string]interface{}{
		"agentId":   d.agentID,
		"uptime":    time.Since(d.started).Round(time.Second).String(),
		"startedAt": d.started.Format(time.RFC3339),
		"mysql":     d.checkMySQL(ctx),
	})
}

func (d *serveDaemon) checkMySQL(ctx context.Context) map[string]interface{} {
	conn := connector.NewMySQLConnector()
	defer conn.Close()
	if err := conn.Connect(d.connCfg); err != nil {
		return map[string]interface{}{"connected": false, "error": friendlyConnError(d.connCfg, err).Error()}
	}
	return map[string]interface{}{
		"connected": true,
		"host":      d.connCfg.Host,
		"port":      d.connCfg.Port,
		"database":  d.connCfg.Database,
	}
}

// handleShutdown stops the daemon gracefully. The response is flushed before
// the connection is closed. 归档循环随 rootCtx 取消而停止。
func (d *serveDaemon) handleShutdown(ctx context.Context, cmd ws.Command) *ws.Response {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		d.rootCancel()
	})
	return okResp(cmd, "shutting down")
}

// handlePreflight runs the preflight checks on the agent's local MySQL and
// returns per-check results plus the available binlog files.
func (d *serveDaemon) handlePreflight(ctx context.Context, cmd ws.Command) *ws.Response {
	conn := connector.NewMySQLConnector()
	if err := conn.Connect(d.connCfg); err != nil {
		return errResp(cmd, "connect to MySQL: %v", friendlyConnError(d.connCfg, err))
	}
	defer conn.Close()

	pCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pfRes, err := conn.Preflight(pCtx)
	if err != nil {
		return errResp(cmd, "preflight: %v", err)
	}

	binlogs, err := conn.GetBinlogFiles(pCtx)
	if err != nil {
		return errResp(cmd, "list binlog files: %v", err)
	}
	names := make([]string, 0, len(binlogs))
	var totalSize int64
	for _, bf := range binlogs {
		names = append(names, bf.Name)
		totalSize += bf.Size
	}

	binlogDir := d.cfg.BinlogDir
	if binlogDir == "" {
		if dir, err := resolveDataDir(d.connCfg); err == nil {
			binlogDir = dir
		}
	}

	return okResp(cmd, map[string]interface{}{
		"preflight":   pfRes,
		"binlogFiles": names,
		"totalSize":   totalSize,
		"binlogDir":   binlogDir,
	})
}

// handleScan 解析 scan 请求并交给 daemon 异步执行；接受后立即响应。
// 流式结果（tx_meta/sql/scan_done/op_error）经 EventSink 推送。
func (d *serveDaemon) handleScan(ctx context.Context, cmd ws.Command) *ws.Response {
	var req ws.ScanRequest
	if err := decodeParams(cmd, &req); err != nil {
		return errResp(cmd, "scan: %v", err)
	}
	if err := d.daemon.Scan(ctx, cmd.Cmd, req); err != nil {
		return errResp(cmd, "scan: %v", err)
	}
	return okResp(cmd, map[string]interface{}{"accepted": true, "operationId": cmd.Cmd})
}

// handleExecute 解析执行请求并交给 daemon 异步执行（进度/完成事件流推送）。
func (d *serveDaemon) handleExecute(ctx context.Context, cmd ws.Command) *ws.Response {
	var req ws.ExecuteRequest
	if err := decodeParams(cmd, &req); err != nil {
		return errResp(cmd, "execute: %v", err)
	}
	if err := d.daemon.Execute(ctx, cmd.Cmd, req); err != nil {
		return errResp(cmd, "execute: %v", err)
	}
	// operationId 回显命令 ID（cmd.Cmd）：daemon op 注册表以它为键，
	// server 侧拿到 accepted 响应后须用同一值发 cancel（与 handleScan 一致）。
	return okResp(cmd, map[string]interface{}{"accepted": true, "operationId": cmd.Cmd})
}

// handleResume 与 handleExecute 同一路径（Phase 2 语义：从零重跑；Phase 3 由
// server 重发持久化 Plan 续跑）。
func (d *serveDaemon) handleResume(ctx context.Context, cmd ws.Command) *ws.Response {
	var req ws.ExecuteRequest
	if err := decodeParams(cmd, &req); err != nil {
		return errResp(cmd, "resume: %v", err)
	}
	if err := d.daemon.Resume(ctx, cmd.Cmd, req); err != nil {
		return errResp(cmd, "resume: %v", err)
	}
	// operationId 回显命令 ID（cmd.Cmd），与 handleScan/handleExecute 统一。
	return okResp(cmd, map[string]interface{}{"accepted": true, "operationId": cmd.Cmd})
}

// handleCancel 取消一次运行中的 scan/execute。operationId 参数是启动命令的
// 命令 ID（daemon op 注册表的键）。
func (d *serveDaemon) handleCancel(ctx context.Context, cmd ws.Command) *ws.Response {
	opID := paramString(cmd.Params, "operationId")
	if opID == "" {
		return errResp(cmd, "cancel: missing required param 'operationId'")
	}
	if err := d.daemon.CancelOp(opID); err != nil {
		return errResp(cmd, "cancel: %v", err)
	}
	return okResp(cmd, map[string]interface{}{"cancelled": true, "operationId": opID})
}

// handleArchiveStatus 返回归档循环的当前状态（collector.State；循环未启动时
// 为零值状态）。
func (d *serveDaemon) handleArchiveStatus(ctx context.Context, cmd ws.Command) *ws.Response {
	return okResp(cmd, d.daemon.ArchiveStatus())
}

// ---------------------------------------------------------------------------
// Archive loop
// ---------------------------------------------------------------------------

// startArchiveLoop 校验归档配置、连接 MySQL 并启动 collector 归档循环。
// 循环在后台 goroutine 运行；fatal 退出只记日志（archive_status 反映循环
// 状态，平台可据此诊断）。启动失败（配置缺失/连不上 MySQL/无法解析 binlog
// 目录）同步返回错误。
func (d *serveDaemon) startArchiveLoop(ctx context.Context) error {
	if d.cfg.Archive == nil || d.cfg.Archive.Dir == "" {
		return fmt.Errorf("serve: config field archive.dir is required")
	}
	if d.cfg.Archive.ServerID == 0 {
		return fmt.Errorf("serve: config field archive.server_id is required")
	}
	if err := os.MkdirAll(d.cfg.Archive.Dir, 0o755); err != nil {
		return fmt.Errorf("serve: create archive dir %s: %w", d.cfg.Archive.Dir, err)
	}
	binlogDir, err := d.binlogDir()
	if err != nil {
		return err
	}
	conn := connector.NewMySQLConnector()
	if err := conn.Connect(d.connCfg); err != nil {
		return friendlyConnError(d.connCfg, err)
	}
	loop := collector.NewLoop(collector.Config{
		MySQL:         conn, // connector 实现 collector.MySQLInfo
		BinlogDir:     binlogDir,
		ArchiveDir:    d.cfg.Archive.Dir,
		ServerID:      d.cfg.Archive.ServerID,
		RetentionDays: d.cfg.Archive.RetentionDays,
		Logger:        d.logger(),
		// SourceFactory 必须显式传完整连接配置：nil 回退是 localhost:3306
		// 空凭据（T5 carry-in）。
		SourceFactory: collector.DefaultSourceFactory(stream.Config{
			Host:     d.connCfg.Host,
			Port:     d.connCfg.Port,
			User:     d.connCfg.User,
			Password: d.connCfg.Password,
			ServerID: d.cfg.Archive.ServerID,
		}),
	}, archive.NewWriter(d.cfg.Archive.Dir))
	d.loopMu.Lock()
	d.loop = loop
	d.loopMu.Unlock()
	go func() {
		if err := loop.Run(ctx); err != nil && ctx.Err() == nil {
			d.logger().Error("archive loop exited", "err", err)
		}
	}()
	return nil
}

// binlogDir 返回 MySQL 侧 binlog 目录：优先 cfg.BinlogDir，否则查
// log_bin_basename（reconcile 回填的复制源目录）。
func (d *serveDaemon) binlogDir() (string, error) {
	if d.cfg.BinlogDir != "" {
		return d.cfg.BinlogDir, nil
	}
	dir, err := resolveDataDir(d.connCfg)
	if err != nil {
		return "", fmt.Errorf("serve: resolve binlog directory (set config binlog_dir to override): %w", err)
	}
	return dir, nil
}

// ---------------------------------------------------------------------------
// Serve command
// ---------------------------------------------------------------------------

// certCNFromFile extracts the CommonName from the first certificate in a PEM
// file. Used to derive the agent ID from the client certificate.
func certCNFromFile(certFile string) (string, error) {
	data, err := os.ReadFile(certFile)
	if err != nil {
		return "", fmt.Errorf("read client certificate: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return "", fmt.Errorf("no PEM block in %s", certFile)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse client certificate: %w", err)
	}
	if cert.Subject.CommonName == "" {
		return "", fmt.Errorf("client certificate has empty CommonName")
	}
	return cert.Subject.CommonName, nil
}

// NewServeCommand creates the `agent serve` cobra command that runs the agent
// as a persistent daemon connected to the platform hub.
func NewServeCommand() *cobra.Command {
	opts := ServeOptions{}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the agent as a persistent daemon connected to the platform",
		Long: `Run the agent as a persistent daemon that maintains a long-lived mTLS
WebSocket connection to the mysql-pitr-server and serves binlog archive,
scan, execute, resume, cancel, preflight and status commands for the platform.

The daemon runs a binlog archive loop (config section "archive": dir,
server_id, retention_days) that mirrors the MySQL binlog stream into the
archive directory, then serves scan/execute against the archived binlogs.

The agent ID is taken from the --agent-id flag, or derived from the
CommonName of the mTLS client certificate when the flag is omitted.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.ConfigFile == "" {
				return fmt.Errorf("--config is required")
			}
			if opts.Passphrase == "" {
				return fmt.Errorf("--passphrase is required")
			}

			cfg, err := config.LoadConfig(opts.ConfigFile, opts.Passphrase)
			if err != nil {
				return fmt.Errorf("serve: load config: %w", err)
			}
			if cfg.Server.URL == "" {
				return fmt.Errorf("serve: config field server.url is required")
			}
			if cfg.Server.CertFile == "" || cfg.Server.KeyFile == "" || cfg.Server.CAFile == "" {
				return fmt.Errorf("serve: config fields server.cert_file, server.key_file, and server.ca_file are required")
			}

			agentID := opts.AgentID
			if agentID == "" {
				cn, err := certCNFromFile(cfg.Server.CertFile)
				if err != nil {
					return fmt.Errorf("serve: derive agent id from client certificate: %w (pass --agent-id explicitly)", err)
				}
				agentID = cn
			}

			d := newServeDaemon(cfg, agentID)

			client := wsagent.NewClient(wsagent.ClientConfig{
				ServerURL: cfg.Server.URL,
				CertFile:  cfg.Server.CertFile,
				KeyFile:   cfg.Server.KeyFile,
				CAPath:    cfg.Server.CAFile,
				AgentID:   agentID,
			})
			d.client = client

			dispatcher := wsagent.NewDispatcher()
			dispatcher.RegisterHandler(ws.CmdStatus, d.handleStatus)
			dispatcher.RegisterHandler(ws.CmdShutdown, d.handleShutdown)
			dispatcher.RegisterHandler(ws.CmdPreflight, d.handlePreflight)
			dispatcher.RegisterHandler(ws.CmdScan, d.handleScan)
			dispatcher.RegisterHandler(ws.CmdExecute, d.handleExecute)
			dispatcher.RegisterHandler(ws.CmdResume, d.handleResume)
			dispatcher.RegisterHandler(ws.CmdCancel, d.handleCancel)
			dispatcher.RegisterHandler(ws.CmdArchiveStatus, d.handleArchiveStatus)
			client.SetDispatcher(dispatcher)

			log.Printf("agent %s starting (platform %s, mysql %s:%d)", agentID, cfg.Server.URL, d.connCfg.Host, d.connCfg.Port)
			if err := client.Connect(cmd.Context()); err != nil {
				return fmt.Errorf("serve: connect to platform: %w", err)
			}

			// 归档循环启动失败即 fatal；运行中退出只记日志。
			if err := d.startArchiveLoop(d.rootCtx); err != nil {
				_ = client.Close()
				return err
			}

			// Certificates are renewed automatically when nearing expiry.
			if tlsCfg := client.TLSConfig(); tlsCfg != nil {
				wsagent.StartAutoRenew(d.rootCtx, client, tlsCfg, agentID)
			}

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

			select {
			case <-sigCh:
				log.Printf("agent %s received interrupt, shutting down", agentID)
			case <-d.stopCh:
				log.Printf("agent %s shutdown requested by platform", agentID)
			}

			// Give the shutdown response time to flush before closing.
			time.Sleep(500 * time.Millisecond)
			_ = client.Close()
			return nil
		},
		SilenceUsage: true,
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.ConfigFile, "config", "", "Encrypted config file path (required)")
	flags.StringVar(&opts.Passphrase, "passphrase", "", "Passphrase for config decryption (required)")
	flags.StringVar(&opts.AgentID, "agent-id", "", "Agent identifier (default: from client certificate CommonName)")

	return cmd
}

// resolveDataDir queries MySQL for the data directory path.
// (Moved here from the legacy flashback.go, which was replaced by the new
// engine-based implementation in the flashback CLI rewire.)
func resolveDataDir(cfg connector.ConnConfig) (string, error) {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 3306
	}

	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = cfg.User
	mysqlCfg.Passwd = cfg.Password
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	mysqlCfg.ParseTime = true

	db, err := sql.Open("mysql", mysqlCfg.FormatDSN())
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var name, value string
	if err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'log_bin_basename'").Scan(&name, &value); err != nil {
		return "", fmt.Errorf("query log_bin_basename: %w", err)
	}
	if value == "" {
		return "", fmt.Errorf("log_bin_basename is empty — binary logging may not be enabled")
	}
	// Extract directory from the full path (e.g. "/var/log/mysql/mysql-bin" -> "/var/log/mysql/").
	dir := filepath.Dir(value)
	if dir != "." {
		value = dir + string(filepath.Separator)
	} else {
		value = ""
	}
	return value, nil
}
