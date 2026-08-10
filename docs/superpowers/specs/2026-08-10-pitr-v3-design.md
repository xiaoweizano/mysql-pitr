# PITR 平台 v3 设计文档（go-mysql 采集引擎 + SvelteKit）

**日期**：2026-08-10
**状态**：已评审通过
**作者**：a-shan 团队

## 背景

当前 `a-shan/mysql-pitr` 是 agent + server 架构的 PITR 工具，binlog 解析依赖外部 `mysqlbinlog` 命令行进程，前端为 React 19 + Ant Design。主要痛点：

- binlog 解析依赖外部 `mysqlbinlog` 进程，部署需额外安装 mysql-client，进程间开销大
- 自实现的 ROW 事件解析代码（`internal/parser/rows_event.go`）维护负担重
- GTID 定位、事务级粒度回滚、大文件流式扫描等能力补丁式扩展困难
- 平台元数据 in-memory，重启即丢
- React 栈较重，希望引入更轻量栈

## 目标

基于 [go-mysql](https://github.com/go-mysql-org/go-mysql)（master，要求 Go 1.25）重写采集引擎，使用 SvelteKit 重写前端，全新设计实现：

- 误删恢复（DELETE → INSERT）
- UPDATE 回滚
- 指定时间恢复
- 指定事务恢复（两阶段：扫元数据 → 勾选 → 按需生成 SQL）
- GTID 定位
- 大 binlog 增量采集 + 本地归档（保窗口）

## 关键决策

| 维度 | 选择 | 理由 |
|---|---|---|
| 改造方式 | 现有仓库内重写 agent 采集引擎 + 重写 web | 「改造这个项目」，保留 server 的 auth/org/agent 管理 |
| 依赖版本 | Go 1.25 + go-mysql master | 充分利用 go-mysql 能力，本地参考副本 `D:\a-shan-referce\go-mysql-master` |
| 读取方式 | 混合：本地文件解析（历史扫描）+ binlogsyncer（增量流式） | 历史大文件扫描最快 + 增量走复制协议 |
| 进程拓扑 | agent + server 双进程，mTLS WebSocket | 现有架构，多实例管理 |
| 增量用途 | 本地原始 binlog 文件归档，保留窗口不受 MySQL 清理影响 | PITR 恢复窗口任意长 |
| 归档地位 | 归档目录为唯一事实源，PITR 扫描不回源 MySQL | 同步滞后（秒级）可接受 |
| DB 支持 | MySQL 8.0/5.7 为主，flavor 抽象预留 MariaDB | 本期只做 MySQL |
| 实例范围 | 多实例：一个 server 管多个 agent | 已确认 |
| 平台持久化 | SQLite（modernc.org/sqlite，纯 Go 无 CGO） | 单二进制、重启零丢失 |
| 前端 | SvelteKit + adapter-static 内嵌 Go 二进制 | 单二进制交付 |
| UI 组件 | shadcn-svelte + Tailwind | 现代轻量，中文界面 |
| 执行位置 | agent 端执行，server 收进度 | 已确认 |
| 检查点 | 双写（agent 本地 + server SQLite） | 两边重启不丢进度 |
| 中断语义 | 当前批次回滚 | 数据完整性优先 |

## 不在本期范围

- MariaDB flavor 完整支持（仅架构预留）
- 自动化定时备份（mysqldump 全量）
- 触发器自动禁用（仅 UI 文档提示）
- 多机房灾备
- 跨 MySQL 实例的 PITR

---

## 架构

### 进程拓扑

```
┌──────────────────────────────────────────────────────────────────────┐
│ 浏览器                                                                │
│  SvelteKit SPA (adapter-static，shadcn-svelte + Tailwind，中文)        │
└──────────────────────────────────────────────────────────────────────┘
                  │ HTTPS：REST + SSE（操作进度）
                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│ server.exe  (Go 1.25，单二进制)                                       │
│  /api/*                        REST handlers                         │
│  /api/ops/{id}/events          SSE 进度推送                           │
│  /                             //go:embed 内嵌 SvelteKit 静态资源     │
│  :9443 mTLS WebSocket /ws/agent    agent 通道（双向命令+流式回传）     │
│  SQLite (modernc.org/sqlite，纯 Go 无 CGO)                           │
└──────────────────────────────────────────────────────────────────────┘
                  │ mTLS WebSocket（自动重连，JSON 消息）
                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│ agent.exe  (部署在 MySQL 主机)                                        │
│  internal/collector   统一采集引擎（核心）                             │
│    FileSource ─ParseFile──► 事务聚合器 ──► ArchiveWriter ─►归档目录    │
│    StreamSource─binlogsyncer─►(同上管道)   └► ScanStream（PITR 扫描）  │
│  internal/reverse     共享纯逻辑包，agent 端就地生成 SQL（行不出 agent）│
│  internal/executor     检查点化批量执行                                │
│  internal/connector    本地 MySQL 连接、preflight、FK 处理             │
│  归档目录: /opt/pitr-archive/mysql-bin.*   （唯一事实源）              │
└──────────────────────────────────────────────────────────────────────┘
```

### 模块分层原则

1. `collector → reverse → executor` 单向依赖（agent 侧链路），低层不依赖高层
2. `reverse` 纯函数：无 IO，给定输入永远得相同输出；agent/server 同仓库共享该包
3. `Source` / `CheckpointStore` / `ArchiveStore` 均为接口，单测可注入 mock
4. agent 只做采集 + 执行，不做 UI 逻辑

---

## 采集引擎（internal/collector）

两条数据源汇入一条管道，产出两类消费者：**归档写入** 和 **PITR 扫描流**。

```
        SHOW BINARY LOGS
  ┌─────────────┐
  │ FileSource   │── BinlogParser.ParseFile ──►┐
  └─────────────┘                              │
                                               ▼
  ┌─────────────┐  raw mode          ┌────────────────┐     ┌──────────────┐
  │ StreamSource │── binlogsyncer ──►│ 事务聚合器       │──┬─►│ ArchiveWriter │─► 归档目录
  └─────────────┘                    │ (Transaction 流) │  │ └──────────────┘
                                     └────────────────┘  │ ┌──────────────┐
                                                         └►│ ScanStream    │─► server（WS 流式）
                                                            └──────────────┘
```

### 数据源接口

```go
type Source interface {
    Next(ctx context.Context) (*replication.BinlogEvent, error) // io.EOF 表示扫完
    Close() error
}
```

- **FileSource**：封装 `BinlogParser.ParseFile`，按归档目录文件顺序逐个扫描。PITR 扫描用。
- **StreamSource**：封装 `binlogsyncer`（raw 模式），从最后同步点（GTID/Position）续拉增量。归档用。

两边都产出 `*replication.BinlogEvent`，事务聚合器只看事件不看来源。

### 事务聚合器

```go
type Transaction struct {
    GTID       string
    XID        uint64
    CommitTime time.Time
    Schema     string
    Statements []RowChange
    Truncated  bool   // 超过 max_rows_per_tx 截断标记
}

type RowChange struct {
    Schema, Table string
    Action        RowAction        // Insert / Update / Delete
    Before, After []byte           // 行镜像，JSON 序列化
    ColumnNames   []string
    HasPK         bool
}
```

- 边界：GTIDEvent / XIDEvent / COMMIT QueryEvent
- TableMapEvent 由 go-mysql parser 内部按 table-id 缓存，RowsEvent 自动带出 Schema/Table
- 大事务（`max_rows_per_tx`，默认 100 万行，**用户可自定义**）截断 + 标记 `Truncated`，防止 OOM
- `Truncated` 事务不可选用于恢复（UI 禁用 + 警告「事务过大已截断，无法完整回滚」）
- MariaDB 预留：flavor 检测切换 GTID 解析（`SetFlavor`），本期只做 MySQL

**扫描模式**（决定流式回传内容）：

| 模式 | 回传内容 | 用途 |
|---|---|---|
| `META_ONLY` | 事务元数据（GTID/XID/时间/表/行数），轻量 | 模式 B 第一阶段、归档预览 |
| `WITH_SQL` | 元数据 + 已生成的逆向 SQL（agent 端就地生成） | 模式 A 直接预览 SQL |
| `SELECTED_SQL` | 对选中的 GTID/XID 集合做定向二次扫描，仅回传这些事务的 SQL | 模式 B 第二阶段 |

内存边界：任一模式都只保留「当前事务」的行镜像（受 `max_rows_per_tx` 约束），不整库驻留；回传的 SQL 受 `max_preview_transactions`（默认 500）约束。

### 归档写入（ArchiveWriter）——保真 + 自愈

归档目录是唯一事实源，写入策略：

1. **初始回填**：agent 启动时对 `SHOW BINARY LOGS` 里缺失的文件直接本地文件拷贝（快、精确）
2. **增量流式**：syncer raw 模式把当前打开文件的尾部事件还原成 `.partial` 临时文件
3. **轮转时封口**：收到 RotateEvent → 用 `ParseFile` + `SetVerifyChecksum(true)` 验证 `.partial` 文件能否完整解析
   - 通过 → seal 成正式归档文件
   - 失败 → 回退为本地整文件拷贝（此时 MySQL 文件已封口，可安全复制），保证零缺口
4. **断线自愈**：agent 重启/重连后，比对归档目录与 `SHOW BINARY LOGS`，缺口文件补齐，然后从上次 GTID/Position 续拉

### PITR 扫描（ScanStream）——过滤维度

```go
type Filter struct {
    Tables    []TableRef     // schema.table 多选
    TimeRange *TimeRange     // 起止时间
    GTIDSet   mysql.GTIDSet  // GTID 定位
    StartPos  mysql.Position // 起始 file+offset（可选）
    EndPos    mysql.Position // 结束位置（可选）
}
```

- 表过滤：RowsEvent 层，命中才进入事务
- 时间过滤：按 `CommitTime`（XID/GTID 事件时间戳）
- GTID 定位：GTID 事件命中集合才聚合进事务——「指定事务恢复」的定位手段
- 大 binlog 一次性流式扫描，边扫边经 WS 回传 server → SSE 推给浏览器，不落内存
- 先回传事务**元数据**（GTID/XID/时间/表/行数），达到 `max_preview_transactions`（默认 500，可配置）即停；用户勾选后再按需生成 SQL

---

## 恢复/回滚链路

### 逆向 SQL 生成（internal/reverse，共享纯函数库，**agent 端执行**）

行镜像只在 agent 本地，reverse 作为**共享纯逻辑包**由 agent 调用就地生成 SQL，只有生成的 SQL 文本经 WS 回传 server 用于预览/勾选/持久化——行镜像永不出 agent，大文件扫描无传输失控风险。server 不生成 SQL，只做展示与决策。

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

func Generate(tx *binlog.Transaction, schema map[string]TableSchema, opts Options) ([]Statement, error)
```

| 原始操作 | 逆向 SQL | 数据来源 |
|---|---|---|
| DELETE | `INSERT`（还原被删行） | Before image |
| INSERT | `DELETE`（WHERE 定位） | After image |
| UPDATE | `UPDATE`（SET=旧值，WHERE=当前值） | Before→SET，After→WHERE |

- 同事务内严格 LIFO（后执行的先回滚，避免依赖冲突）
- WHERE 构造：有主键用主键；无主键退化全行匹配 + 警告
- 不可逆项（DDL、缺列、超 `MaxStatementSize`）跳过并记 `Warnings`，UI 明确展示
- 入参校验：`tx` 必须带 GTID 或 XID 至少其一

### 两类恢复模式

**模式 A：行级 SQL 勾选（误删恢复 / UPDATE 回滚）**
```
选实例+表+时间区间 ──► scan(WITH_SQL) agent 边扫边生成 SQL
    ──► UI 按事务分组展示已生成 SQL，逐行勾选 ──► 执行
```

**模式 B：指定事务恢复（GTID/事务级）**
```
选实例 + 过滤(GTID集/时间/表) ──► 两阶段：
    阶段1 scan(META_ONLY) 回传候选事务元数据 ──► 用户勾选 N 个事务
    阶段2 agent 定向二次扫描（SELECTED_SQL），仅生成选中事务的 SQL
    ──► 预览 ──► 执行
```

### 执行引擎（internal/executor，agent 端）

```
执行计划: 已批准 SQL 列表 + batchSize(默认 50)
  ──► preflight（连接、权限、行数估算、FK 检查——复用 connector/fk.go）
  ──► 逐批次: BEGIN → SQL 1..50 → COMMIT
  ──► 每批提交后写检查点 (operation_id, last_statement, done/total, errors)
  ──► WS 回传进度 → server SSE 推送
```

**执行语义：**
- 中断（用户取消 / 断连 / ctx 取消）→ **当前批次回滚**，检查点停在上一已提交批次 → 可恢复
- 单条 SQL 失败 → 记入 `Errors`，**继续下一条**（不中断整批）
- 批次 COMMIT 失败 → 整批回滚，op = failed
- **检查点双写**：agent 本地 + server SQLite
- pause / resume / cancel 均由 server 发 WS 命令到 agent

### 操作状态机

```
created → scanning → ready（用户勾选）
        → executing ⇄ paused（可恢复）
        → done / failed
每步状态变更落 SQLite + audit_log
```

---

## 平台侧

### SQLite 数据模型

只持久化「有长期价值」的数据，预览列表可重新扫描生成，不落库：

```
users / orgs / members         认证与多组织（沿用现有逻辑）
agents                         实例元数据 + 审批 + 归档状态
archive_state                  agent_id, file, size, status(partial/sealed),
                               first/last_event_ts —— 归档健康度/缺口检测
operations                     id, org_id, agent_id, type, filter(JSON),
                               status, created_by/at
operation_txs                  op_id, gtid, xid, commit_time, tables, rows,
                               status(previewed/selected/executed)
statements                     op_id, tx_id, sql, warnings —— 用户勾选后的 SQL
checkpoints                    op_id, last_stmt, done/total, errors —— 双写之一
audit_logs                     操作全链路审计
```

### Web 页面（SvelteKit + shadcn-svelte + Tailwind，中文）

```
/login                      登录
/instances                  实例列表：状态、归档覆盖、同步滞后、审批待办
/instances/{id}/archive     归档监控：文件清单、时间线覆盖、缺口告警
/instances/{id}/pitr        恢复向导（核心）：
                            步骤1 选类型：误删恢复 / UPDATE回滚 / 指定时间 / 指定事务 / GTID定位
                            步骤2 过滤：表多选 + 时间区间 + GTID 集输入
                            步骤3 事务列表（扫描进度条，流式刷新）
                            步骤4 SQL 预览：按事务分组、warnings 标记、逐行勾选
                            步骤5 执行：SSE 进度、pause/resume/cancel、失败详情
/operations                 操作历史 + 审计
/org                        组织与成员管理
```

### 错误处理与可靠性

| 场景 | 处理 |
|---|---|
| 归档缺口（agent 离线期间轮转） | 重连后 reconcile 补齐；归档时间线 UI 显示缺口 |
| WS 断连 | 指数退避自动重连，扫描/执行状态持久化可恢复 |
| syncer 原始模式还原失败 | 封口验证失败 → 整文件本地拷贝兜底 |
| 执行中断 | 当前批次回滚 + 检查点恢复 |
| agent 离线执行操作 | op 置为 blocked，UI 明确提示 |
| 敏感配置 | 沿用 config/crypto.go 加密 |

### 测试策略

| 层 | 方法 |
|---|---|
| 单元 | 事务聚合用 go-mysql 自带 testdata binlog 文件；reverse 纯函数穷举；filter 组合；检查点恢复 |
| 集成 | Docker MySQL 8.0 跑已知 DML → 扫描断言精确 Transaction → 逆向执行 → 断言库状态；轮转+purge 验证归档完整性；大 binlog（压测脚本生成）验证流式不 OOM |
| E2E | gstack /browse 在 SvelteKit UI 跑误删恢复黄金路径（生成数据→误删→向导→恢复→验证） |
