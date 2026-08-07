# PITR 平台 v2 设计文档

**日期**：2026-08-07
**状态**：草案，待评审
**作者**：a-shan 团队

## 背景

当前 `a-shan/mysql-pitr` 是一个基于 MySQL binlog 的 PITR 工具，agent + server 架构，使用 `mysqlbinlog` 命令行解析 binlog 文件，前端 React 19 + Ant Design。

主要痛点：
- binlog 解析依赖外部 `mysqlbinlog` 进程，部署需额外安装 mysql-client，且进程间开销大
- 自实现的 ROW 事件解析代码（`internal/parser/rows_event.go` 26KB）维护负担重
- GTID 定位、事务级粒度回滚、大文件流式扫描等能力补丁式扩展困难
- 平台元数据全部 in-memory（`internal/server/server.go:59-63`），重启即丢
- 前端 React 栈较重，希望引入更轻量栈

## 目标

基于 [go-mysql](https://github.com/go-mysql-org/go-mysql) 重写 binlog 引擎，使用 SvelteKit 重写前端，扩展支持：
- 误删恢复（已有）
- UPDATE 回滚（已有）
- 指定时间恢复（已有）
- **指定事务恢复**（新）
- **GTID 定位**（新）
- **大 binlog 文件高性能一次扫描**（新）

## 关键决策

| 维度 | 选择 | 理由 |
|---|---|---|
| 改造范围 | 全量重写（agent + server + web） | 与新功能集匹配，旧抽象撑不住 GTID/事务粒度 |
| 部署拓扑 | 保留 agent + 读本地 binlog 文件 | 与现有部署模型一致，离线可用 |
| go-mysql 用法 | `replication.BinlogParser` 离线文件解析 | agent 在 MySQL 主机，文件可达，不走网络复制协议 |
| 大 binlog 含义 | 大文件高性能一次扫描 | YAGNI，不引入持续同步 |
| PITR 过滤维度 | 表 + 时间区间 + GTID 集 + 事务选择 | 覆盖全部用户场景 |
| 前端部署 | SvelteKit adapter-static 嵌入 Go 二进制 | 单二进制交付，部署最简 |
| UI 栈 | shadcn-svelte + Tailwind | 现代轻量，可控 |
| 平台持久化 | 嵌入 SQLite（modernc.org/sqlite，纯 Go 无 CGO） | 重启零丢失，与单二进制契合 |
| 多租户认证 | 保留多组织 + JWT + agent 审批 | 企业场景必需 |
| 执行模型 | 预览-勾选-执行-检查点（检查点入 SQLite） | 与现有体验一致 + 可恢复 |
| 反向 SQL 生成位置 | server 端 | 纯逻辑，server 跑更利于预览 |
| 事务选择 | 两阶段（先扫元数据，再生成 SQL） | UI 流畅，按需生成 |
| 检查点存储 | 双写（agent 本地 + server SQLite） | agent 重启 + server 重启都可恢复 |
| 中断语义 | 当前批次回滚 | 数据完整性优先 |
| Scanner 接口风格 | `Next() (*Transaction, error)` | Go 1.22 兼容 |
| 进度推送 | SSE | 单向推送，简单 |

## 不在本期范围

- 持续 binlog 同步到本地存储（canal 模式）
- 跨 MySQL 实例的 PITR（仅支持单实例）
- 自动化定时备份
- 触发器自动禁用（仅 UI 文档提示）
- 多机房灾备

---

## 架构

### 进程拓扑

```
┌──────────────────────────────────────────────────────────────────────┐
│ 浏览器                                                                │
│  SvelteKit SPA (shadcn-svelte + Tailwind)                            │
└──────────────────────────────────────────────────────────────────────┘
                  │ HTTPS (REST + SSE)
                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│ server.exe  (单二进制，Go 1.22+)                                      │
│   /api/*                       REST handlers                         │
│   /api/operations/{id}/events  SSE 实时进度                          │
│   /                embed.FS 服务 SvelteKit 静态资源                  │
│   :9443 mTLS WebSocket /ws/agent                                     │
│   SQLite ./data/app.db (users/orgs/members/agents/operations/        │
│            audit_logs/checkpoints/ca_state)                          │
└──────────────────────────────────────────────────────────────────────┘
                  │ mTLS WebSocket
                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│ agent.exe  (部署在 MySQL 主机)                                        │
│   ws/client         mTLS 连接、自动重连                              │
│   binlog/engine     基于 go-mysql replication.BinlogParser           │
│   reverse           （server 端运行，agent 不持有）                  │
│   executor          检查点化批量执行                                 │
│   connector         本地 MySQL 连接                                  │
│   读取本地文件: /var/lib/mysql/mysql-bin.*                           │
│   执行本地连接: 127.0.0.1:3306                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### 关键架构选择

- **静态前端嵌入 server 二进制**：SvelteKit adapter-static → `web/build/` → Go `//go:embed`
- **进度推送 SSE 不用 WS**：浏览器侧单向，自动走 HTTP/2 多路复用
- **Agent 仍走 mTLS WebSocket**：双向消息（命令下发 + 流式回传）确实需要 WS
- **平台数据全部入 SQLite**：含 CA 状态；重启零丢失

### 目录结构

```
cmd/
  agent/main.go        serve + flashback CLI
  server/main.go       web + mTLS
internal/
  binlog/              新：go-mysql 包装，文件扫描 + 事务聚合
    engine.go
    transaction.go
    gtid.go
    reader.go
  reverse/             新：纯逻辑，逆向 SQL 生成
    generator.go
    order.go
  executor/            新：检查点化批量执行
    executor.go
    checkpoint.go
  connector/           简化：MySQL DSN、preflight、FK 处理、SchemaFetcher
  config/              保留：加密配置
  ws/                  保留：mTLS、hub、CA
    hub/
    client/
    ca/
    proto/             新：消息定义
  server/
    router.go
    auth/              JWT + 用户
    org/               组织、成员、邀请
    agent/             agent 元数据、审批
    pitr/              重写：操作状态机
    audit/
    store/             新：SQLite 仓储层
    embed.go           //go:embed web/build
web/                   SvelteKit 全新实现
```

---

## 模块边界

### `internal/binlog` — binlog 文件扫描与事务聚合

**职责**：枚举本地 binlog 文件，调用 go-mysql 的 `replication.BinlogParser` 解析事件，按 XID/GTID/COMMIT 边界聚合成 `Transaction` 流。

**核心接口**：

```go
type Filter struct {
    Tables    []TableRef
    TimeRange *TimeRange
    GTIDSet   mysql.GTIDSet
    StartPos  mysql.Position
    EndPos    mysql.Position
}

type Transaction struct {
    GTID       string
    XID        uint64
    CommitTime time.Time
    Schema     string
    Statements []RowChange
    Truncated  bool   // 超过 max_rows_per_tx 时为 true
}

type RowChange struct {
    Schema, Table string
    Action        RowAction   // Insert / Update / Delete
    Before, After []byte
    ColumnNames   []string
}

type Scanner interface {
    Scan(ctx context.Context, f Filter) error    // 启动一次扫描
    Next() (*Transaction, error)                  // 返回 io.EOF 表示扫完
    Close() error
}

func NewScanner(conn SchemaFetcher, opts ...Option) Scanner
```

**依赖**：`go-mysql-org/go-mysql/replication`、`go-mysql-org/go-mysql/mysql`、`internal/connector`（仅 `SchemaFetcher` 接口）

**不做的事**：生成逆向 SQL、写本地存储、与 server/ws 通信、执行 SQL

**关键实现点**：
- `BinlogParser` 同步，goroutine 包成可 `ctx.Done()` 中断的迭代器
- TableMap event 按 table-id 缓存在 scanner 内
- 大事务（默认 100 万行，可配置 `max_rows_per_tx`）截断 + 标记 `Truncated`

### `internal/reverse` — 逆向 SQL 生成（纯逻辑）

**职责**：给定 `Transaction` + 表结构，产出逆向 SQL 列表（LIFO）。无 IO，无副作用。

**核心接口**：

```go
type Options struct {
    IgnoreAutoIncrement bool
    MaxStatementSize    int
}

type Statement struct {
    SQL       string
    TxID      string       // 必填
    TxOrder   int
    SourceRow binlog.RowChange
    Warnings  []string
}

// Generate 把一个事务翻成 0..N 条 SQL
// 入参 tx 必须有 TxID 或 GTID 至少其一非空（构造时校验）
func Generate(tx *binlog.Transaction, schema map[string]TableSchema, opts Options) ([]Statement, error)
```

**依赖**：`internal/binlog` 类型；标准库。**无 IO**。

**行为**：
- `DELETE` → `INSERT`（Before image）
- `INSERT` → `DELETE`（After image 拼 WHERE）
- `UPDATE` → `UPDATE`（After image 拼 WHERE，Before image 作 SET）
- DDL → 跳过，warnings 标"不可逆"
- Schema 缺列 → warning
- SQL 超 `MaxStatementSize` → 跳过 + warning
- 同事务内严格 LIFO

### `internal/executor` — 检查点化批量执行

**职责**：把已批准的 `Statement` 在 MySQL 上执行；每条成功推进检查点；可中断可恢复。

**核心接口**：

```go
type Plan struct {
    OperationID string
    Statements  []reverse.Statement
    DSN         string
    BatchSize   int   // 默认 50
}

type Progress struct {
    Done, Total int
    LastTxID    string
    LastSQL     string
    Errors      []ExecError
}

type Executor interface {
    Run(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error)
    Resume(ctx context.Context, operationID string, cb ProgressCallback) (FinalReport, error)
}

type CheckpointStore interface {
    Load(operationID string) (*Checkpoint, error)
    Save(c Checkpoint) error
    Clear(operationID string) error
}
```

**依赖**：`internal/connector`、`internal/reverse`、`CheckpointStore` 接口

**关键实现点**：
- 单连接、按顺序、显式事务包裹批次
- 检查点格式：`(operation_id, last_completed_statement_id, total, errors)`
- 中断语义（`ctx.Done()`）：**当前批次回滚**，检查点回到上一已完成批次
- 单条 SQL 失败 → 记入 `Errors`，**继续下一条**（不中止批次）
- 批次 COMMIT 失败 → 整批回滚，op = failed

### 简化/保留模块

| 模块 | 改动 | 职责 |
|---|---|---|
| `internal/connector` | 简化 | MySQL 连接池、SchemaFetcher、preflight、FK 处理 |
| `internal/config` | 保留 | 加密配置、passphrase |
| `internal/ws/hub` | 保留 | server 侧 agent hub、生命周期钩子 |
| `internal/ws/client` | 保留 | agent 侧 mTLS client、自动重连 |
| `internal/ws/ca` | 保留 | 内部 CA、root/leaf 证书签发 |
| `internal/ws/proto` | 新 | 消息定义（JSON）：Command / Response / StreamEvent |
| `internal/server/store` | 新 | SQLite 仓储层 |
| `internal/server/pitr` | 重写 | 操作状态机 |
| `internal/server/{auth,org,agent,audit}` | 接线 | 业务保留，store 换 SQLite |

### 设计原则

1. **binlog → reverse → executor 单向依赖**：低层不依赖高层
2. **`reverse` 纯函数**：无 IO，给定输入永远得相同输出
3. **`CheckpointStore` 是接口**：默认 SQLite，单测可注入内存实现
4. **`Scanner` 是接口**：默认本地文件，单测可注入 mock

---

## 数据流

### 操作状态机

```
                ┌─────────┐
   POST /api/   │         │
   operations   │ created │
   ────────────►│         │
                └────┬────┘
                     │ agentHub.Scan(filter) dispatched
                     ▼
                ┌─────────┐
                │scanning │◄─── agent 流式回传 transactions
                └────┬────┘
                     │ scan 完成或达到 max-preview-transactions
                     ▼
                ┌─────────┐
                │  ready  │◄─── 用户看事务/反向 SQL，勾选
                └────┬────┘
                     │ POST /execute {selected_ids}
                     ▼
                ┌─────────┐
        ┌───────│executing│
        │       └────┬────┘
        │ SSE        │
   pause/cancel     │
        ▼           ▼
   ┌─────────┐ ┌─────────┐
   │ paused  │ │  done   │
   └────┬────┘ └─────────┘
        │ resume
        ▼
   回到 executing (或 failed)
   ┌─────────┐
   │ failed  │
   └─────────┘
```

### 端到端时序

```
浏览器            server                agent                MySQL
  │ POST /api/ops │                       │                    │
  │──────────────►│                       │                    │
  │               │ op=created, 派发      │                    │
  │               │──────Scan(filter)────►│                    │
  │               │                       │ SHOW BINARY LOGS   │
  │               │                       │───────────────────►│
  │               │                       │◄───file list───────│
  │               │                       │ BinlogParser 扫描  │
  │               │                       │ SchemaFetcher      │
  │               │                       │──SHOW COLUMNS─────►│
  │               │                       │◄──columns──────────│
  │               │ 流式回传              │                    │
  │               │◄──Transaction #1──────│                    │
  │               │◄──Transaction #2──────│                    │
  │ SSE 推送      │                       │                    │
  │◄──tx #1,#2────│                       │                    │
  │               │   ...                 │                    │
  │               │◄──ScanDone(n=247)─────│                    │
  │               │ op=ready              │                    │
  │◄──op=ready────│                       │                    │
  │ 用户勾选 12 条事务，POST /execute     │                    │
  │──────────────►│                       │                    │
  │               │ server 计算 reverse SQL（server 端）       │
  │               │ op=executing,         │                    │
  │               │ 派发 Execute(plan)    │                    │
  │               │──────Execute(plan)───►│                    │
  │               │                       │ 加载检查点         │
  │               │                       │──BEGIN────────────►│
  │               │                       │──SQL 1..50────────►│
  │               │                       │──COMMIT───────────►│
  │               │                       │ 写检查点           │
  │               │ Progress(50/240)      │                    │
  │               │◄────Progress(50/240)──│                    │
  │◄──50/240──────│                       │                    │
  │               │   ...                 │                    │
  │               │◄──Done(report)────────│                    │
  │               │ op=done, audit 写入   │                    │
  │◄──op=done─────│                       │                    │
```

### Filter 四维映射

```
用户 UI 输入            Filter 字段               Scanner 行为
─────────────           ────────────              ──────────
schema.table 多选  ──►  Filter.Tables       ──►   row event 按表过滤
时间区间           ──►  Filter.TimeRange    ──►   按 CommitTime 过滤
GTID 集            ──►  Filter.GTIDSet      ──►   按 GTID 事件过滤
"事务选择"模式     ──►  (UI 阶段，不是 Filter)
```

事务选择两阶段：
1. Scan 用前三维扫候选事务（回传元数据：GTID/XID/时间/影响行数）
2. 用户勾选 N 个事务
3. server 对选中事务调 `reverse.Generate` 出 SQL

非"事务选择"模式：Scan 直接对全部候选事务生成 SQL，UI 显示完整列表，用户勾选 SQL 行（与现有体验一致）。

### Agent ↔ Server 消息（`ws/proto` 包）

```go
// Command (server → agent)
type Command struct {
    ID   string          // 消息 id
    Op   string          // "scan" | "execute" | "resume" | "preflight" | "cancel"
    Body json.RawMessage
}

// StreamEvent (agent → server, 流式)
type StreamEvent struct {
    CmdID   string
    Type    string       // "transaction" | "progress" | "log" | "done" | "error"
    Payload json.RawMessage
}

// Response (agent → server, 终结)
type Response struct {
    CmdID string
    OK    bool
    Error string
    Body  json.RawMessage
}
```

一个 Command 触发 0..N 条 StreamEvent + 1 条 Response。

### 检查点双写

- agent 本地：`data/<operation_id>.ckpt.json`（agent 重启恢复用）
- server SQLite：`checkpoints` 表（UI / 审计 / resume 用）
- Resume 时 server 把 SQLite 检查点下发给 agent；agent 用它继续

---

## 错误处理

### 故障域 A：网络层（agent ↔ server WebSocket）

| 场景 | 检测 | 恢复 | 用户可见 |
|---|---|---|---|
| agent 短暂断连（<30s） | hub 心跳超时 | op 转 `paused`，等待重连 | "agent 重连中..." |
| agent 长时间断连（>5min） | 超时阈值 | op = `failed`，audit 记录 | UI 失败原因 |
| 重连后状态对齐 | agent 重连时 server 下发 op_state | agent 从检查点续或重扫 | 透明 |
| server 重启 | agent WS 断开 | server 从 SQLite 恢复 op；in-flight op 转 `paused` | "已暂停，请 resume" |

scanning 阶段无检查点（binlog 文件本身是源），断连后默认重扫。

### 故障域 B：binlog 解析层（agent 端）

| 场景 | 检测 | 恢复 | 用户可见 |
|---|---|---|---|
| 文件不存在 / 无读权限 | `os.Open` 失败 | 立即失败 | 路径 + 权限提示 |
| binlog 文件损坏 | parser 返回 EOF 或格式错误 | **立即 fail**（不静默丢数据） | "文件 X 在偏移 Y 处损坏" |
| GTID 集无效 | `mysql.ParseGTIDSet` 错误 | 创建 op 时 HTTP 400 | 表单错误 |
| MySQL 未开 GTID 但 Filter 含 GTIDSet | preflight 检测 | 创建 op 时 HTTP 400 | "目标 MySQL 未启用 GTID" |
| TableMap 缺失 | parser 内部状态缺失 | 跳过该事务，记 `skipped_transactions` | UI 警告列表 |
| DDL 事件 | QueryEvent 含 DDL | 事务标 `un_reversible`，跳过 reverse | UI "事务 #N 含 DDL，不可逆" |
| 大事务（> `max_rows_per_tx`） | scanner 计数 | 截断 + 标记 `Truncated`，不 fail | UI "事务过大已截断" |

扫描期间错误**累积不立即终止**，扫完给完整列表，用户决定。

### 故障域 C：reverse SQL 生成层（server 端）

| 场景 | 检测 | 恢复 | 用户可见 |
|---|---|---|---|
| Schema 不匹配 | `SchemaFetcher` 查询结果 | 跳过该 SQL，warning | UI 标红 + warning |
| 行 image 解码失败 | go-mysql 解码 err | 整事务标 `un_generatable`，跳过 | UI 显示 |
| 生成 SQL 超 `MaxStatementSize` | 字节数检查 | 跳过 + warning | UI warning |
| 事务无 TxID/GTID | 构造函数校验 | 不会发生 | n/a |

### 故障域 D：executor 执行层（agent 端）

| 场景 | 检测 | 恢复 | 用户可见 |
|---|---|---|---|
| 单条 SQL 失败（FK 等） | `db.Exec` err | 记 `errors`，**继续下一条** | UI 错误列表 |
| 批次 COMMIT 失败 | `COMMIT` err | 整批回滚，op = `failed` | 失败原因 |
| MySQL 连接断开 | 连接错误 | 重连 + 重试当前批次 1 次，仍失败 → op = failed | "MySQL 连接失败" |
| 用户取消 | `ctx.Done()` | **当前批次回滚**，检查点回到上一已提交批次，op = `paused` | "已暂停" |
| agent 进程崩溃 | 重启 server 检测重连 | 从 server SQLite 检查点 resume | 透明 |
| 检查点写失败 | SQLite 写错误 | op = failed（数据完整性优先） | 失败原因 |

FK 处理：默认 `SET FOREIGN_KEY_CHECKS=0;` 包裹批次，UI 高级选项可保留 FK 检查。

触发器：默认不禁用（避免改语义），UI 文档明确告知。

### 故障域 E：平台层（server 自身）

| 场景 | 检测 | 恢复 |
|---|---|---|
| SQLite 锁竞争 | `SQLITE_BUSY` | 默认 5s 超时 + 3 次重试 |
| SQLite 文件损坏 | 启动 PRAGMA 检查 | 启动失败，要求从备份恢复 |
| JWT secret 丢失 | 启动检查 env | 拒绝启动 |
| CA 私钥丢失/损坏 | 启动载入失败 | 拒绝启动；已有 agent 失联 |
| 磁盘满 | 写操作失败 | op = failed，明确错误 |

### 故障域 F：用户输入

| 场景 | 处理 |
|---|---|
| 时间区间反（start > end） | HTTP 400 |
| 表名缺 schema | 表单校验 |
| 区间内无 binlog | 进入 ready，0 个事务 |
| GTID 集超范围 | 0 个事务（不报错） |
| 勾选 0 条 SQL 执行 | HTTP 400 |

### 审计完整性

**审计先于副作用**：任何改变数据库的操作（执行 SQL、暂停、取消、失败）先写 `audit_logs` 表，再执行。审计写失败则操作中止。

---

## 测试策略

### 测试金字塔

```
        ┌──────────────────┐
        │  e2e (浏览器+真  │   1-3 个 happy path，仅 main 分支
        │  MySQL+agent)    │   ~10s
        ├──────────────────┤
        │  integration     │   agent↔server↔测试 MySQL
        │  (Go test, 容器) │   每次提交，~60s
        ├──────────────────┤
        │  unit (Go test)  │   单文件/单函数，秒级
        └──────────────────┘
```

### 各模块测试重点

**`internal/binlog`**：
- 单测用 fixture binlog 文件（`testdata/*.bin`，<100KB 每个，含各种事件类型）
- 不依赖 MySQL 连接，SchemaFetcher 用 stub
- 容器集成测：MySQL 8.0 容器跑 DML → flush logs → 让 scanner 读
- Fixture 维护：`testdata/Makefile` 用 docker 重新生成

**`internal/reverse`**：
- 表驱动单测爆炸性枚举：`(action, columns, before, after) → expected SQL`
- 每种类型 + 列类型组合至少一例（int / varchar / text / json / datetime / nullable / NULL / 默认值 / AI 列）
- LIFO、跨事务不混合、DDL warning、Schema 缺列 warning、大 SQL 截断

**`internal/executor`**：
- 单测用 `DATA-DOG/go-sqlmock`（项目已用）
- 检查点写读、批次边界、ctx 取消回滚、单条失败继续、COMMIT 失败回滚
- 容器集成测：真 MySQL 预置数据，执行 plan，断言行数恢复

**`internal/server/*`**：
- 每个 handler 用 `httptest` + 内存 SQLite
- 状态机迁移全枚举
- 权限边界（跨组织 → 403）
- 审计完整性

**`internal/ws/*`**：
- mTLS 握手、CSR 签发、证书续期、hub 路由、消息编解码
- 集成测：起 server + agent，验证 reconnect、双向消息、大规模 message（10MB stream）

**`web/`（SvelteKit）**：
- 单元测：`@testing-library/svelte` + `vitest`，组件 ≤ 50 个，每个有基础渲染测
- e2e：Playwright，一个 happy path 烟雾测

### CI 流水线（GitHub Actions）

```
on: push / pull_request
jobs:
  go-test:    go test -race -coverprofile=coverage.out ./...
  go-lint:    golangci-lint run
  web-test:   pnpm install && pnpm test && pnpm build
  e2e:        docker-compose up + pnpm playwright test
              （仅 main 分支或带 e2e 标签的 PR）
```

### 覆盖率目标

| 模块 | 目标 |
|---|---|
| `internal/reverse` | ≥ 95% |
| `internal/binlog` | ≥ 90% |
| `internal/executor` | ≥ 90% |
| `internal/server/pitr` | ≥ 90% |
| `internal/server/{auth,org,agent,audit,store}` | ≥ 90% |
| `internal/ws/*` | ≥ 90% |
| `internal/connector` | ≥ 90% |
| `internal/config` | ≥ 90% |
| `web/` | ≥ 60% |

### PITR 端到端场景（集成测试矩阵）

| # | 场景 | 期望 |
|---|---|---|
| 1 | 简单 DELETE 回滚 | 行恢复 |
| 2 | 简单 UPDATE 回滚 | 行恢复旧值 |
| 3 | 简单 INSERT 回滚 | 行被删除 |
| 4 | 大事务（10 万行） | 流式回滚成功 |
| 5 | 混合 DDL+DML 事务 | DDL 标 warning，DML 回滚 |
| 6 | 跨 binlog 文件的事务 | file 切换正确 |
| 7 | 用户中途取消 | 检查点保存，部分回滚 |
| 8 | GTID 集定位 | 仅回滚指定 GTID 的事务 |

### 测试数据敏感性

`testdata/*.bin` 不含敏感数据（dummy 值如 `test_user_1`）。CI 阶段 grep 常见 secret 模式。

---

## 依赖

### Go

新增：
- `github.com/go-mysql-org/go-mysql` — binlog 解析（核心）
- `modernc.org/sqlite` — 嵌入 SQLite（纯 Go 无 CGO）

保留：
- `github.com/go-chi/chi/v5` — HTTP router
- `github.com/golang-jwt/jwt/v5` — JWT
- `github.com/gorilla/websocket` — WS
- `github.com/spf13/cobra` — CLI
- `golang.org/x/crypto` — 加密
- `github.com/DATA-DOG/go-sqlmock` — 测试
- `github.com/stretchr/testify` — 测试

移除：
- `github.com/go-sql-driver/mysql` — 改用 go-mysql 的 `client` 包建立 MySQL 连接（与 binlog 解析共用一个库，少一个依赖）

### 前端

新增：
- SvelteKit + adapter-static
- Tailwind CSS
- shadcn-svelte（含 Melt UI、Tailwind 依赖）
- `@tanstack/svelte-query` — 数据获取（与现有 react-query 等价）
- Playwright + `@testing-library/svelte` + vitest

移除：
- React 19 + react-dom
- Ant Design + @ant-design/icons
- react-router-dom
- @tanstack/react-query
- @vitejs/plugin-react + vite（保留 vite 给 SvelteKit）

---

## 风险

| 风险 | 缓解 |
|---|---|
| go-mysql 对最新 MySQL 8.x binlog 格式兼容性滞后 | CI 矩阵覆盖 MySQL 8.0/8.4；fixture 持续更新 |
| modernc.org/sqlite 性能不如 CGO 版本 | 平台元数据量小（< 100 万行），性能不是瓶颈 |
| shadcn-svelte 与 Antd 视觉/交互差异引起用户不适 | UI 设计稿先行评审；保留交互模型（向导步骤、勾选）一致 |
| 重写期间线上仍跑老版本 | 老版本进入维护模式，仅修严重 bug；新版本稳定后切换 |
| 大事务（千万级行）扫描 OOM | 默认 `max_rows_per_tx=1_000_000`，UI 可调；超出截断 |
| GTID 解析对 MariaDB 格式不同 | `mysql.MySQLFlavor` / `mysql.MariaDBFlavor` 区分；preflight 探测 |

---

## 后续工作（不在本期）

- 持续 binlog 同步到 SQLite/BoltDB（canal 模式）— 真实时报存
- 多 MySQL 实例支持
- Web UI 表结构差异可视化（binlog 时的列 vs 当前 schema）
- 审计日志的 SIEM 集成（webhook 出口）
- 国际化（i18n）— 中文 / 英文
