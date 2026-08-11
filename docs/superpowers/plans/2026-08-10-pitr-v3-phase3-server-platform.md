# PITR v3 Phase 3：server 平台（SQLite 持久化 + 操作状态机 + SSE + 新协议接线）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 server 从「内存元数据 + 旧 pitr 协议」重写为「SQLite 持久化 + v3 操作状态机 + 新 agent 协议（scan/execute 流事件）+ SSE 进度」。

**Architecture:** 单一 SQLite 库（modernc.org/sqlite，纯 Go）承载全部平台元数据；现有 5 个域 store 接口（auth/org/agent/audit/pitr）保持不变、生产实现换 SQLite（InMemory 实现保留作 handler 测试替身）；pitr 操作状态机重写为 v3 流程（created→scanning→ready→executing⇄paused→done/failed/blocked），hub 新增 `stream_event` 路由（agent 推送 → pitr handler → SSE 广播）；REST 增补事务选择/执行/pause/resume 端点；`go:embed` 静态占位（Phase 4 填 SvelteKit 产物）。

**Tech Stack:** Go 1.25、modernc.org/sqlite v1.56.0（经 goproxy.cn）、go-chi、gorilla/websocket（hub）、现有 internal/ws + internal/daemon + internal/collector + internal/executor + internal/scan + internal/binlog。

## 本计划范围（阶段拆分）

- Phase 1（完成）：采集引擎；Phase 2（完成）：agent daemon
- **Phase 3（本计划）**：server 平台——SQLite 持久化、操作状态机、SSE、REST、hub 协议接线、agent 侧两处补线（wire 归一化 + executor DSN）
- Phase 4：SvelteKit Web（embed 占位先立，Phase 4 填产物）

## Global Constraints

- Go 工具链 1.26.5；go.mod `go 1.25.0`；GOPROXY=goproxy.cn；go-mysql v1.16.0 锁版不动
- **不得破坏现有构建**：`web/` 保持原样（Phase 4 动）；每任务结束 `go build ./...` 必须通过
- 域 store **接口不变**：`auth.UserStore`、`org.OrgStore`、`agent.AgentStore`、`audit.AuditStore` 保持现有签名（handler 代码因此基本不动）；SQLite 实现在各域包内新增（`sqlite.go`），InMemory 实现**保留**作 handler 测试替身（测试零churn）
- pitr 的 OperationStore 与 Operation 模型随 v3 流程**重写**（旧 173 行 store + 旧状态机替换；旧 handler 832 行测试随 handler 重写更新）
- flavor：仅 MySQL；新包只依赖标准库 + go-mysql + 仓内包，禁止新增对 agent 侧包的直接依赖（server 通过 ws 协议与 agent 通信）
- TDD：先写失败测试 → 跑通失败 → 实现 → 跑通 → 提交；每任务独立 commit

## Phase 2 交接（本计划必须落实的硬要求）

1. **SQL 事件 wire 归一化**（T7 交接）：agent daemon 的 SQL 事件当前 marshal 原始 `reverse.Statement`（大写键 + SourceRow 内部镜像上 wire）——改为 `ws.StatementWire`（Task 3）
2. **Plan.DSN 注入**（T7 交接）：agent serve 的 executor 需用**本地 MySQL 配置**构造 DBConnFactory（server 不传 DSN；agent 用自己的连接执行）——Task 3
3. **cancel 契约**（T8 交接）：server 发 cancel 时 `params.operationId` = 被取消命令的命令 ID（与 daemon op 注册表键一致）——Task 6
4. **stream_event 信封解包**（T8 交接）：agent 推送 `{type:"stream_event", params:{id, kind, data}}`；kind ∈ EvTxMeta/EvSQL/EvScanDone/EvProgress/EvOpDone/EvOpError——Task 5 的 hub 路由按此解包
5. 空文件回填缺陷已随 Phase 2 修复波闭环；其余 Phase 2 residual（M2/M6/M7）不在本计划范围

---

### Task 1: SQLite 基础设施（internal/server/store）

**Files:**
- Create: `internal/server/store/store.go`、`internal/server/store/migrate.go`、`internal/server/store/store_test.go`

**Interfaces:**
```go
package store

// Open 打开（或创建）SQLite 库并启用 WAL + foreign_keys。path=":memory:" 支持测试。
func Open(path string) (*sql.DB, error)
// Migrate 幂等地建全部表（CREATE TABLE IF NOT EXISTS）。
func Migrate(db *sql.DB) error
// AppDB 是共享句柄的持有者（server.New 创建后注入各域 store）。
```

- [ ] **Step 1: 拉依赖 + 写失败测试**

```bash
go get modernc.org/sqlite@latest && go mod tidy   # 期望 v1.56.x
```

`internal/server/store/store_test.go`：

```go
func TestOpenAndMigrate_IsIdempotent(t *testing.T) {
    db, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
    require.NoError(t, err)
    require.NoError(t, store.Migrate(db))
    require.NoError(t, store.Migrate(db)) // 二次迁移无错
    // 每张表可查询
    for _, tbl := range []string{"users", "orgs", "members", "agents", "operations",
        "operation_txs", "statements", "checkpoints", "archive_state", "audit_logs"} {
        var n int
        require.NoError(t, db.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n))
    }
    require.NoError(t, db.Close())
}

func TestOpen_Memory(t *testing.T) {
    db, err := store.Open(":memory:")
    require.NoError(t, err)
    require.NoError(t, store.Migrate(db))
    require.NoError(t, db.Close())
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/server/store/ -v`
Expected: 编译失败（包不存在）

- [ ] **Step 3: 实现 store.go + migrate.go**

`store.go`：

```go
package store

import (
    "database/sql"
    "fmt"

    _ "modernc.org/sqlite" // 纯 Go 驱动
)

func Open(path string) (*sql.DB, error) {
    db, err := sql.Open("sqlite", path)
    if err != nil {
        return nil, fmt.Errorf("store: open %s: %w", path, err)
    }
    // WAL + 外键：单写者多读者、重启安全
    for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
        if _, err := db.Exec(pragma); err != nil {
            db.Close()
            return nil, fmt.Errorf("store: pragma %q: %w", pragma, err)
        }
    }
    return db, nil
}
```

`migrate.go`：`Migrate(db)` 依次执行下述 DDL（`db.Exec`，含多语句拆分——modernc 驱动一次 Exec 支持多语句，直接拼一个脚本即可；若驱动限制则按语句数组逐个执行）：

```sql
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE,
  hashed_password TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orgs (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS members (
  org_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL,
  PRIMARY KEY (org_id, user_id)
);
CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY, org_id TEXT NOT NULL, name TEXT NOT NULL,
  host TEXT NOT NULL DEFAULT '', mysql_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending', cert_serial TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY, org_id TEXT NOT NULL, agent_id TEXT NOT NULL,
  type TEXT NOT NULL, mode TEXT NOT NULL, filter TEXT NOT NULL,
  status TEXT NOT NULL, created_by TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operation_txs (
  op_id TEXT NOT NULL, tx_index INTEGER NOT NULL,
  tx_id TEXT NOT NULL, gtid TEXT NOT NULL DEFAULT '', xid INTEGER NOT NULL DEFAULT 0,
  commit_time TEXT NOT NULL, schema_name TEXT NOT NULL DEFAULT '',
  tables TEXT NOT NULL DEFAULT '', row_count INTEGER NOT NULL DEFAULT 0,
  truncated INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'previewed',
  PRIMARY KEY (op_id, tx_index)
);
CREATE TABLE IF NOT EXISTS statements (
  op_id TEXT NOT NULL, tx_index INTEGER NOT NULL, stmt_index INTEGER NOT NULL,
  sql TEXT NOT NULL, tx_id TEXT NOT NULL, tx_order INTEGER NOT NULL DEFAULT 0,
  warnings TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending',
  PRIMARY KEY (op_id, stmt_index)
);
CREATE TABLE IF NOT EXISTS checkpoints (
  op_id TEXT PRIMARY KEY, last_statement INTEGER NOT NULL DEFAULT 0,
  total INTEGER NOT NULL DEFAULT 0, errors TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS archive_state (
  agent_id TEXT PRIMARY KEY, last_file TEXT NOT NULL DEFAULT '',
  last_pos INTEGER NOT NULL DEFAULT 0, last_gtid TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, op_id TEXT NOT NULL DEFAULT '',
  operator TEXT NOT NULL DEFAULT '', ts TEXT NOT NULL,
  org_id TEXT NOT NULL, agent_id TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT ''
);
```

（时间统一存 RFC3339Nano 字符串；JSON 字段存 TEXT。迁移脚本用版本化 `schema_version` 表 + 每版本一个脚本的骨架——本计划只含 v1，结构留出 `PRAGMA user_version` 或 `schema_version` 表位。）

- [ ] **Step 4: 跑测试 + 提交**

Commit: `feat(store): sqlite open/migrate with full platform schema`

---

### Task 2: 域仓储 SQLite 实现（auth/org/agent/audit）

在现有 4 个域包内新增 SQLite 实现，**接口签名不动**（InMemory 保留作测试替身）。

**Files:**
- Create: `internal/server/auth/sqlite.go`、`internal/server/org/sqlite.go`、`internal/server/agent/sqlite.go`、`internal/server/audit/sqlite.go` + 各自 `_test.go`
- Modify: 无（接口不变）

**Interfaces:**
- Consumes: `store.Open`/`store.Migrate`（Task 1）、各域现有接口（`auth.UserStore{Create,GetByID,GetByEmail}` 等）
- Produces: `auth.NewSQLiteUserStore(db)`、`org.NewSQLiteOrgStore(db)`、`agent.NewSQLiteAgentStore(db)`、`audit.NewSQLiteAuditStore(db)` —— 签名对齐现有 New 模式

- [ ] **Step 1: 先看接口再写失败测试**

```bash
sed -n '1,60p' internal/server/org/store.go   # OrgStore 接口
sed -n '1,60p' internal/server/agent/store.go # AgentStore 接口
```

（org/agent 接口含 org 校验、成员邀请等——照现有接口实现。）

每个域写 CRUD 失败测试（SQLite 行为断言，如 email 唯一冲突返回错误、Query 过滤正确）：

```go
// auth/sqlite_test.go 示例
func TestSQLiteUserStore_UniqueEmail(t *testing.T) {
    db, _ := store.Open(":memory:")
    store.Migrate(db)
    s := auth.NewSQLiteUserStore(db)
    require.NoError(t, s.Create(&auth.User{ID: "u1", Email: "a@x.com"}))
    err := s.Create(&auth.User{ID: "u2", Email: "a@x.com"})
    require.Error(t, err) // UNIQUE 约束映射错误
    u, err := s.GetByEmail("a@x.com")
    require.NoError(t, err)
    require.Equal(t, "u1", u.ID)
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/server/auth/ ./internal/server/org/ ./internal/server/agent/ ./internal/server/audit/ -run TestSQLite -v`
Expected: 编译失败

- [ ] **Step 3: 实现各 sqlite.go**

通用模式：`INSERT`/`SELECT` 映射；错误映射（唯一冲突 → 域错误文本与 InMemory 一致）；时间为 RFC3339Nano 字符串；`audit.Query` 动态 WHERE（org_id 必填，其余可选 AND）。**行为必须与 InMemory 版本等价**——对照现有 InMemory 实现逐方法对齐错误文案与返回形状（含「空列表返回 `[]T{}` 而非 nil」等细节）。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go test ./internal/server/auth/ ./internal/server/org/ ./internal/server/agent/ ./internal/server/audit/ -count=1`
Commit: `feat(server): sqlite-backed domain stores (auth/org/agent/audit)`

---

### Task 3: agent 侧补线——SQL 事件 wire 归一化 + executor DSN

**Files:**
- Modify: `internal/daemon/scan.go`（SQL 事件改 marshal `ws.StatementWire`）、`internal/ws/types.go`（增 `CmdStreamEvent` 常量）、`cmd/agent/serve.go`（用 ws.CmdStreamEvent、executor 真工厂 + FileCheckpointStore 接线）、`internal/daemon/scan_test.go`、`cmd/agent/serve_test.go`
- Test: 追加断言

**Interfaces:**
- Consumes: `ws.StatementWire{SQL, TxID, TxOrder, Warnings}`（Phase 2 T6 已定义）、`executor.NewExecutor(factory, store)`、`executor.NewFileCheckpointStore(dir)`、`connector.AsDB()`
- Produces: daemon SQL 事件 `Data` = `[]ws.StatementWire` JSON；serve 的 executor 用本地 MySQL 连接执行

- [ ] **Step 1: 写失败测试（wire 归一化 + 常量）**

`internal/daemon/scan_test.go` 追加：`TestScan_SelectedSQLModeEmitsStatementWire`——断言 `EvSQL` 事件的 `Data` 反序列化为 `[]ws.StatementWire` 且**不含 SourceRow 字段**（unmarshal 到 map 断言无 "SourceRow" 键、字段为小写驼峰 `"sql"/"txId"/"txOrder"/"warnings"`）。

`internal/ws/types_test.go` 追加：`ws.CmdStreamEvent == "stream_event"` 常量断言。

`cmd/agent/serve_test.go` 追加：`NewServeCommand` 构造后 daemon 的 exec 非 nil 且执行路径能用（用 recordingDB 类似替身断言 `Execute` 调用到 executor）——若 serve 构造方式不便注入，退而断言 `startArchiveLoop`/daemon 构造参数完整（报告说明取舍）。

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/daemon/ ./internal/ws/ -run "TestScan_SelectedSQLModeEmitsStatementWire|TestCmdStreamEvent" -v`
Expected: FAIL（当前 Data 是原始 reverse.Statement 大写键）

- [ ] **Step 3: 实现**

`internal/ws/types.go` 增：

```go
const CmdStreamEvent = "stream_event"
```

`internal/daemon/scan.go` 的 scan goroutine 中，SQL 事件推送改为：

```go
wire := make([]ws.StatementWire, 0, len(r.SQL))
for _, s := range r.SQL {
    wire = append(wire, ws.StatementWire{
        SQL: s.SQL, TxID: s.TxID, TxOrder: s.TxOrder, Warnings: s.Warnings,
    })
}
data, _ := json.Marshal(wire)
d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvSQL, Data: data})
```

`cmd/agent/serve.go`：
- `streamEventType` 常量改为引用 `ws.CmdStreamEvent`
- `newServeDaemon`/`NewServeCommand` 内构造 executor：

```go
cpDir := filepath.Join(filepath.Dir(d.cfg.Archive.Dir), "checkpoints") // 或 config 显式字段
exec := executor.NewExecutor(
    func(executor.Plan) (executor.DB, error) {
        conn := connector.NewMySQLConnector()
        if err := conn.Connect(d.connCfg); err != nil {
            return nil, friendlyConnError(d.connCfg, err)
        }
        return conn.AsDB(), nil
    },
    executor.NewFileCheckpointStore(cpDir),
)
```

（工厂忽略 Plan.DSN——agent 用自己的本地 MySQL 配置执行；连接生命周期由 executor 的 DB 接口管理。`friendlyConnError` 已在 serve.go。若 `connector.AsDB()` 连接语义与 executor 的 Begin/Exec 生命周期冲突，报告说明并最小调整。）

- [ ] **Step 4: 跑测试 + 提交**

Run: `go build ./... && go test ./internal/daemon/ ./internal/ws/ ./cmd/agent/ -count=1`
Commit: `feat(daemon,agent): statement wire normalization; executor bound to local MySQL config`

---

### Task 4: pitr 操作模型 + v3 状态机 + SQLite 仓储

**Files:**
- Rewrite: `internal/server/pitr/state.go`（v3 状态机）、`internal/server/pitr/store.go`（新模型 + SQLite 实现）、`internal/server/pitr/state_test.go`
- Create: `internal/server/pitr/model.go`、`internal/server/pitr/store_test.go`

**Interfaces:**
```go
package pitr

type OperationState string
const (
    StateCreated   OperationState = "created"
    StateScanning  OperationState = "scanning"
    StateReady     OperationState = "ready"
    StateExecuting OperationState = "executing"
    StatePaused    OperationState = "paused"
    StateDone      OperationState = "done"
    StateFailed    OperationState = "failed"
    StateBlocked   OperationState = "blocked" // agent 离线等外部条件
)

// validTransitions：created→{scanning, blocked, failed}；
// scanning→{ready, failed, blocked, cancelled?}；ready→{executing, cancelled}；
// executing⇄paused；executing→{done, failed}；paused→{executing, cancelled}
// cancelled 是否保留：v3 用 failed/blocked 覆盖取消语义——决策：保留显式 StateCancelled，
// ready/scanning/paused 可到 cancelled。终态：done/failed/cancelled/blocked。

type Operation struct {
    ID, OrgID, AgentID string
    Type   string // "flashback" | "update_rollback" | "pitr" | "tx_rollback"
    Mode   string // "meta" | "sql" | "selected"
    Filter binlog.Filter // 序列化 JSON
    Status OperationState
    CreatedBy string
    CreatedAt, UpdatedAt time.Time
    SelectedTxIDs []string // 持久化
    Statements    []Statement // 持久化（选中后）
}

type Statement struct {
    TxIndex, StmtIndex int
    SQL, TxID string
    TxOrder int
    Warnings []string
    Status string // pending/approved/executed/error
}

// OperationStore（新）：v3 模型 CRUD
type OperationStore interface {
    Create(op *Operation) error
    Get(id string) (*Operation, error)
    Update(op *Operation) error            // 状态/字段整体更新
    ListByOrg(orgID string) ([]*Operation, error)
    ListByAgent(agentID string) ([]*Operation, error)
    SaveStatements(opID string, stmts []Statement) error
    LoadStatements(opID string) ([]Statement, error)
}

func NewSQLiteOperationStore(db *sql.DB) OperationStore
```

（预览事务列表不持久化——设计文档「预览列表可重新扫描生成，不落库」；只有选中的事务与其语句落库。Filter 的 JSON 序列化：binlog.Filter 字段是导出的，直接 json.Marshal/Unmarshal。）

- [ ] **Step 1: 写失败测试（状态机 v3 + store CRUD）**

`state_test.go` 重写：断言 v3 转移表（如 `TransitionValid(StateScanning, StateReady)` 为真、`TransitionValid(StateReady, StateScanning)` 为假、终态判定）。

`store_test.go`（`:memory:`）：Create/Get/Update 往返、ListByOrg 过滤、SaveStatements/LoadStatements 往返、Update 状态迁移持久化。

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/server/pitr/ -run TestState -v`
Expected: FAIL（旧状态机无 v3 状态）

- [ ] **Step 3: 实现 model.go + 重写 state.go/store.go**

按上述接口实现。SQLite 存储：operations 表映射（filter 列 JSON、selected_tx_ids 以 JSON TEXT 列存——**schema 增列**：operations 表加 `selected_tx_ids TEXT NOT NULL DEFAULT '[]'`，Task 1 的 DDL 需要同步更新——在 Task 1 的 migrate.go 里**预先包含**该列，避免本任务改 schema；若 Task 1 已完成，本任务在 migrate 里补 `ALTER TABLE ... ADD COLUMN` 幂等段并报告）。statements 表按 (op_id, stmt_index) 主键。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go test ./internal/server/pitr/ -count=1`
Commit: `feat(pitr): v3 operation model with sqlite store and state machine`

---

### Task 5: hub stream_event 路由 + SSE 发布订阅

**Files:**
- Modify: `internal/ws/hub/hub.go`（readPump 路由 stream_event → 注册的 handler）
- Create: `internal/server/pitr/events.go`（op 事件总线 + SSE 辅助）、`internal/server/pitr/events_test.go`

**Interfaces:**
```go
// hub 新增（仿 SetProgressHandler 模式）
func (h *Hub) SetStreamEventHandler(fn func(agentID string, cmd ws.Command))
// readPump 内：cmd.Type == ws.CmdStreamEvent → h.streamHandler(agentID, cmd)（goroutine）

// pitr 事件总线（按 op 订阅/发布；发布非阻塞，无订阅者则丢弃）
type eventBus struct{ mu sync.Mutex; subs map[string][]chan ws.StreamEvent }
func (b *eventBus) Subscribe(opID string) <-chan ws.StreamEvent  // 返回新 channel（带缓冲 16）
func (b *eventBus) Publish(opID string, ev ws.StreamEvent)
func (b *eventBus) Unsubscribe(opID string, ch <-chan ws.StreamEvent)
// 当 op 进入终态时 handler 关闭订阅（SSE 端检测 close 后清理）
```

- [ ] **Step 1: 写失败测试**

`internal/ws/hub/hub_test.go` 追加：构造 fake conn 推送 `stream_event` 信封消息 → 断言 handler 收到 `(agentID, cmd)` 且 params.id/kind/data 原样。

`internal/server/pitr/events_test.go`：Subscribe/Publish/Unsubscribe 行为（有订阅者收到、无订阅者不阻塞、Unsubscribe 后不再收到）。

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/ws/hub/ ./internal/server/pitr/ -run "TestStreamEvent|TestEventBus" -v`
Expected: FAIL

- [ ] **Step 3: 实现**

hub.go：`SetStreamEventHandler` + readPump 分支（仿 CmdPITRProgress 分支，goroutine 调 handler）。pitr/events.go：按接口实现 eventBus。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go test ./internal/ws/hub/ ./internal/server/pitr/ -count=1`
Commit: `feat(hub,pitr): stream_event routing and per-operation event bus`

---

### Task 6: PITR handler 新流程（创建/预览/选择/执行/pause/resume/cancel + SSE）

**Files:**
- Rewrite: `internal/server/pitr/handler.go`、`internal/server/pitr/handler_test.go`
- Modify: `internal/server/router.go`（新路由）、`internal/server/server.go`（hub stream handler 接线，或留 Task 7）

**API 面（v3）：**
```
POST   /api/pitr/start          {agentId, type, mode, filter{...}, maxPreview}
GET    /api/pitr/               列表（org 过滤 + agentConnected）
GET    /api/pitr/{id}/status
GET    /api/pitr/{id}/transactions   预览事务列表（扫描期间/ready 后）
POST   /api/pitr/{id}/select    {txIds: []string} → 持久化选中事务语句
POST   /api/pitr/{id}/execute   {batchSize?} → 下发 CmdExecute
POST   /api/pitr/{id}/pause
POST   /api/pitr/{id}/resume
POST   /api/pitr/{id}/cancel
GET    /api/pitr/{id}/events    SSE 流（tx_meta/sql/progress/op_done/op_error）
```

**核心流程：**
- `Start`：校验 agent 存在且在线（hub.IsConnected）→ 创建 op（created）→ `hub.SendToAgent(ctx, agentID, ws.Command{Cmd: opID, Type: ws.CmdScan, Params: scanRequest})` → op=scanning；失败 → op=blocked/failed
- **stream handler**（hub → handler）：`{id, kind, data}`：
  - `tx_meta`：op 预览列表 append（内存）+ eventBus.Publish（SSE 透传）
  - `sql`：暂存 op 内存（select 时落库）——**决策**：SQL 事件只用于 UI 预览，落库以 select 为准
  - `scan_done`：op=ready + Publish
  - `progress`：更新 checkpoint（可选） + Publish
  - `op_done`：op=done + audit + Publish + 关闭订阅
  - `op_error`：op=failed + audit + Publish
- `Select`：校验 op=ready → 按 txIds 把语句落库（从内存 SQL 预览取）→ 标记 selected
- `Execute`：校验 op=ready → 构造 `ws.ExecuteRequest{OperationID: opID, Statements: 选中语句, BatchSize}` → `SendToAgent(Cmd: opID, Type: ws.CmdExecute, Params: executeRequest)` → op=executing
- `Pause/Resume/Cancel`：`SendToAgent` 下发 `CmdCancel`/`CmdResume`（pause 用 CmdCancel? **决策**：v3 用 CmdCancel 统一取消，pause 语义留 Phase 4 UI；本计划提供 cancel（终态）与 resume（ready→executing 重发）。pause 端点先返回 501 或与 cancel 区分——**实现选择**：提供 pause→发 CmdCancel+op=paused 暂态，resume→重发 execute；评审把关）
- `Events`（SSE）：Subscribe(opID) → 逐帧写 `event: <kind>\ndata: <json>\n\n` → op 终态或客户端断开时清理

**测试**（fake hub）：fake hub 实现捕获 SendToAgent 命令 + 可控注入 stream 事件。场景：
- Start → 断言 CmdScan 下发、op=scanning、事件注入 scan_done → op=ready
- tx_meta 注入 → transactions 端点返回预览
- Select → statements 落库
- Execute → CmdExecute 下发（含选中语句、OperationID=opID 满足 cancel 契约）、op=executing
- op_done 注入 → op=done、audit 写入
- cancel → CmdCancel 下发（params.operationId=opID）、op=cancelled
- 离线 agent start → op=blocked

- [ ] **Step 1: 写失败测试（fake hub）**

`internal/server/pitr/handler_test.go` 重写为上述场景（fake hub + 内存 store——InMemory 的 OperationStore 已被 v3 重写替换，测试用 `NewSQLiteOperationStore(:memory:)` 或提供 InMemory v3 实现——**决策**：本任务起 pitr 测试用 SQLite :memory:，不再维护 pitr 的 InMemory store）。

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/server/pitr/ -v`
Expected: FAIL（旧 handler 无新端点）

- [ ] **Step 3: 实现 handler.go**

按上述流程实现（复用现有 writeJSON/writeError/userIDFromRequest 等 helper）。handler 构造参数变化：需要 `hub`（现有）、`eventBus`、`agentStore`、`auditStore`——**构造签名调整**：`NewHandler(opStore, agentStore, orgStore, auditStore, bus, hub, jwtSecret)`。router.go/server.go 同步调整（Task 7 统一接线）。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go test ./internal/server/pitr/ -count=1`
Commit: `feat(pitr): v3 operation flow (scan/select/execute/pause/resume/cancel) with SSE`

---

### Task 7: 平台接线（server.New 换 SQLite + router + embed 占位）

**Files:**
- Modify: `internal/server/server.go`（SQLite 初始化、各 store 换 SQLite、hub stream handler 接线、embed）、`internal/server/router.go`（新 pitr 路由 + SSE）、`internal/server/embed.go`（新，`//go:embed` 占位）
- Create: `internal/server/embed_stub/`（占位静态资源）、`internal/server/server_test.go`（引导测试）

**Interfaces:**
- Consumes: `store.Open/Migrate`、各域 `NewSQLite*Store`、`pitr.NewSQLiteOperationStore`、hub `SetStreamEventHandler`
- Produces: `server.New()` 全 SQLite 引导；`GET /` 返回 embed 占位；`GET /api/pitr/{id}/events` SSE 路由

- [ ] **Step 1: 写失败测试（server 引导 + 路由）**

`internal/server/server_test.go`：`New()` 用临时 AGENT_DATA_DIR → 断言：`web` handler 非 nil、`/` 返回 200（占位页）、`/api/pitr/1/events` 路由存在（未登录 → 401 而非 404）、注册→登录→建 org→注册 agent→approve→start pitr 的 HTTP 冒烟（fake hub 注入——**决策**：hub 在 server.New 内创建，测试用真实 hub + httptest agent 连接或注入 fake——以现有 server 测试手法为准，若现有无引导测试则用 httptest + 真实 hub + 直连 agent 模拟）。

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/server/ -v`
Expected: FAIL

- [ ] **Step 3: 实现**

`server.New()` 改造：

```go
db, err := store.Open(filepath.Join(dataDir, "app.db"))
if err != nil { return nil, err }
if err := store.Migrate(db); err != nil { return nil, err }

userStore := auth.NewSQLiteUserStore(db)
orgStore := org.NewSQLiteOrgStore(db)
agentStore := agent.NewSQLiteAgentStore(db)
pitrStore := pitr.NewSQLiteOperationStore(db)
auditStore := audit.NewSQLiteAuditStore(db)
bus := pitr.NewEventBus()
pitrHandler := pitr.NewHandler(pitrStore, agentStore, orgStore, auditStore, bus, agentHub, jwtSecret())
agentHub.SetStreamEventHandler(func(agentID string, cmd ws.Command) { pitrHandler.HandleStreamEvent(agentID, cmd) })
```

`router.go`：新增 v3 路由（transactions/select/execute/pause/resume/events）；`/` 与 `/static/*` 用 embed 占位（`internal/server/embed_stub/index.html` 一个「PITR 平台（Phase 4 前端）」页；embed.go：`//go:embed embed_stub/*` → http.FileServer + 兜底 index）。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go build ./... && go test ./internal/server/... -count=1`
Commit: `feat(server): wire sqlite stores, stream-event routing and embed placeholder`

---

### Task 8: 集成 e2e 文件（server↔agent↔MySQL，docker 缺失 → 文件就绪）

**Files:**
- Create: `internal/server/e2e_test.go`（`//go:build integration`）、`scripts/e2e/README.md` 追加

**内容**：一条黄金路径：docker MySQL 8.0（GTID+ROW）→ 起真实 agent（serve，连 server hub）→ 起 server（SQLite 临时目录）→ 注册/审批 agent → start pitr 操作 → 等 scan_done → select → execute → 等 done → 断言数据库状态回滚正确。环境变量沿用 `E2E_MYSQL_DSN`/`E2E_BINLOG_DIR`，缺失则 SKIP；编译检查 `go vet -tags integration ./internal/server/`；README 追加 server 侧运行说明。实际运行由用户在服务器上执行（与 Phase 2 T10 同约定）。

- [ ] **Step 1: 写 e2e 文件（integration tag）**
- [ ] **Step 2: 编译检查**

Run: `go vet -tags integration ./internal/server/`（无错误）+ `go build ./...` 全绿
- [ ] **Step 3: 提交**

Commit: `test(e2e): server-agent-mysql golden path (run on docker-capable host)`

---

## Self-Review 结论（编写时已对照 Phase 2 ledger 交接）

- **交接覆盖**：wire 归一化 → Task 3；Plan.DSN → Task 3（agent 本地 MySQL 工厂）；cancel 契约 → Task 6（OperationID=opID=命令 ID）；stream_event 信封 → Task 5；M2/M6/M7 → 不在本计划（Phase 3 交接清单已注明）
- **类型一致性**：`ws.StatementWire`（T6 定义）→ daemon（Task 3）+ pitr（Task 6）双端使用；`ws.CmdStreamEvent` 常量（Task 3）→ hub 路由（Task 5）；`pitr.OperationStore` v3（Task 4）→ handler（Task 6）+ server.New（Task 7）；`eventBus`（Task 5）→ handler SSE（Task 6）
- **决策记录**：预览事务不落库（仅 select 落库）；pause 端点先实现为 CmdCancel+op=paused 暂态（评审把关）；pitr 测试改用 SQLite :memory:（不再维护 pitr InMemory store）；operations 表含 selected_tx_ids 列（Task 1 DDL 预先包含）
- **风险**：handler_test 832 行重写量大（Task 6）；modernc sqlite 驱动多语句 Exec 行为需 Task 1 验证；hub readPump 的 stream_event 分支与旧 CmdPITRProgress 分支并存（旧常量保留未注册，Phase 4 清理）

## 执行交接

计划保存于 `docs/superpowers/plans/2026-08-10-pitr-v3-phase3-server-platform.md`。执行方式同前：Subagent-Driven 或 Inline。完成后接续 Phase 4（SvelteKit Web）。
