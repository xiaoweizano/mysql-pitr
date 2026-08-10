# PITR v2 Phase 2 — Agent Daemon + Server 层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 Phase 1 的引擎（`internal/binlog` + `internal/reverse` + `internal/executor`）重写 agent daemon（`cmd/agent/serve.go`），通过流式 ws 协议（新增 `ws.StreamEvent`）驱动 `scan` / `execute` / `resume` / `cancel`；server 端全量 SQLite 化（`internal/server/store` 仓储层），pitr 操作状态机重写为 spec 的 `created → scanning → ready → executing → paused/done/failed/cancelled`（支持事务选择两阶段），SSE 推送实时进度；检查点双写（agent 本地 `ckpt.json` + server SQLite），断连/重启可恢复。

**Architecture:** 六阶段，依赖自底向上：

```
阶段 A  ws/proto 流式协议（StreamEvent 消息 + client/hub 流式收发）
阶段 B  executor 持久化 CheckpointStore（FileCheckpointStore）+ Resume 真实现
阶段 C  agent daemon 重写（serve.go 拆 scan/execute/resume/cancel 长任务）
阶段 D  server SQLite 仓储层（modernc.org/sqlite + internal/server/store）
阶段 E  server pitr 状态机重写（两阶段事务选择）+ SSE 进度端点
阶段 F  检查点双写 + resume 全链路 + 集成测试
```

依赖方向：Phase 1 的 `binlog → reverse → executor` 保持不变，**任何 task 不得改动这三个包已提交的导出签名**（Phase 1 计划的 Global Constraints 约定继续有效）；`ws/proto` 存放双方共享的消息 payload 类型（可依赖 `binlog`/`executor` 类型做 JSON 互转，但 `binlog`/`reverse`/`executor` 不得反向依赖 ws）；`internal/server/store` 只依赖标准库 + modernc.org/sqlite。

**Tech Stack:** Go 1.23、`modernc.org/sqlite`（新增，纯 Go 无 CGO）、`go-mysql-org/go-mysql` v1.13.0（binlog 解析 + MySQL client）、`go-chi/chi/v5`（HTTP/SSE）、`gorilla/websocket`（WS）、`golang-jwt/jwt/v5`、`DATA-DOG/go-sqlmock` + `stretchr/testify`（测试）。

## Global Constraints

下列约束**所有 task 都 implicitly 包含**，无需在每个 task 重复：

- **Go 版本**：1.23（go.mod 已固定，不要升级）
- **包路径**：`github.com/a-shan/mysql-pitr/internal/...`；新增包 `internal/ws/proto`、`internal/server/store`
- **禁止改动 Phase 1 已提交的导出签名**：`binlog.Scanner/Filter/Transaction/NewScanner/SchemaFetcher`、`reverse.Generate/Options/Statement`、`executor.Executor/Plan/Progress/CheckpointStore/DBConnFactory` 等（执行中若确需改签名，先在 plan 的对应 task 里注明并更新本段）
- **WS 消息信封（不破坏现有协议）**：
  - server→agent 命令仍是 `ws.Command{Cmd, Type, Params}`；agent→server 终结响应仍是 `ws.Response{Cmd, Status, Result, Error}`
  - 新增 agent→server 流式事件 `ws.StreamEvent{Cmd, Type, Result}`：`Cmd` 必须回显触发它的 Command 的 `Cmd`（UUID），`Result` 承载 payload（见 Task 1 定义的事件类型）
  - 短命令（`status`/`preflight`/`shutdown`/`cert_renewal`/`cancel`）保持同步往返；**长命令（`scan`/`execute`/`resume`）用"启动即返回 + 事件流终结"模式**：handler 注册 opRun 后启动 goroutine 立即返回 `okResp`，goroutine 全程用 `client.SendStreamEvent` 发事件，最后发 `type="done"` 或 `type="error"` 终结事件
- **SQLite 约定**：只用 `modernc.org/sqlite`；连接 DSN `file:<path>?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)`；迁移用 `PRAGMA user_version` 递增；所有写操作在显式事务里
- **长任务可取消**：每个 opRun 持有一个 `context.WithCancel` 派生 ctx；`cancel` 命令只能取消对应 operationId 的 opRun；取消语义沿用 Phase 1：executor 当前批次回滚、检查点保留
- **gofmt**：所有新建/修改的 `.go` 文件在提交前必须运行 `gofmt -w <file>`（plan 中的代码块用 4 空格缩进，Go 要求 tab；golangci-lint 的 gofmt linter 会检查）
- **提交粒度**：每个 task 一个或多个 commit，commit message 用 `feat:`/`refactor:`/`test:`/`chore:`/`docs:` 前缀；HEREDOC 写多行 message
- **不要**添加 README/CHANGELOG 注释；**不要**为"理论上不可能"的场景添加错误处理

---

## 阶段 A：ws/proto 流式协议

### Task 1: ws/proto 包 — 流式事件类型与请求 payload

**Files:**
- Create: `internal/ws/proto/events.go`、`internal/ws/proto/events_test.go`
- Create: `internal/ws/proto/requests.go`、`internal/ws/proto/requests_test.go`

**Interfaces:**

```go
// internal/ws/proto/events.go
package proto

// 命令 Op 常量（server → agent，挂在 ws.Command.Type 上）
const (
    CmdScan    = "scan"
    CmdExecute = "execute"
    CmdResume  = "resume"
    CmdCancel  = "cancel"
)

// 流式事件 Type 常量（agent → server，挂在 ws.StreamEvent.Type 上）
const (
    EventTransaction = "transaction"
    EventSchema      = "schema"
    EventProgress    = "progress"
    EventDone        = "done"
    EventError       = "error"
)

// RowChangeJSON 是 binlog.RowChange 的 JSON 载体。
type RowChangeJSON struct {
    Schema      string        `json:"schema"`
    Table       string        `json:"table"`
    Action      string        `json:"action"` // "insert" | "update" | "delete"
    Before      []interface{} `json:"before,omitempty"`
    After       []interface{} `json:"after,omitempty"`
    ColumnNames []string      `json:"column_names"`
}

// TransactionEvent 是 scan 命令的逐事务事件（元数据 + 行数据，server 端
// 用 ToTransaction 还原为 binlog.Transaction 供 reverse.Generate 使用）。
type TransactionEvent struct {
    TxID       string          `json:"tx_id"`
    GTID       string          `json:"gtid,omitempty"`
    XID        uint64          `json:"xid,omitempty"`
    CommitTime time.Time       `json:"commit_time"`
    Schema     string          `json:"schema"`
    Truncated  bool            `json:"truncated"`
    Rows       []RowChangeJSON `json:"rows"`
}

func (e TransactionEvent) ToTransaction() *binlog.Transaction

// SchemaEvent 是扫描中首次遇到某表时发送的表结构事件（server 端 Generate
// 需要；同一 schema.table 只发一次）。
type SchemaEvent struct {
    Schema  string            `json:"schema"`
    Table   string            `json:"table"`
    Columns []binlog.ColumnDef `json:"columns"`
}

// ProgressEvent 是 execute/resume 的进度事件。
type ProgressEvent struct {
    Done     int          `json:"done"`
    Total    int          `json:"total"`
    LastTxID string       `json:"last_tx_id,omitempty"`
    LastSQL  string       `json:"last_sql,omitempty"`
    Errors   []ErrorEntry `json:"errors,omitempty"`
}

// ErrorEntry 是单条 SQL 失败的记录（server 端转成 executor.ExecError）。
type ErrorEntry struct {
    Statement int    `json:"statement"`
    SQL       string `json:"sql"`
    Err       string `json:"err"`
}

// DoneEvent 是长命令的终结事件。
type DoneEvent struct {
    Kind   string      `json:"kind"` // "scan" | "execute" | "resume"
    Total  int         `json:"total"`
    Errors []ErrorEntry `json:"errors,omitempty"`
    Paused bool        `json:"paused,omitempty"`
}

// ErrorEvent 是长命令的失败终结事件。
type ErrorEvent struct {
    Message string `json:"message"`
}
```

```go
// internal/ws/proto/requests.go
package proto

// ScanRequest 是 scan 命令的 Params 载荷。字段名与现有前端/旧 handler
// 保持一致（targetTable、startTime、endTime、startPos、stopPos）。
type ScanRequest struct {
    OperationID  string            `json:"operationId"`
    BinlogDir    string            `json:"binlogDir,omitempty"` // 空 = agent 自动解析 dataDir
    Flavor       string            `json:"flavor,omitempty"`     // "mysql" | "mariadb"，空默认 "mysql"（ParseGTIDSet 用）
    Tables       []binlog.TableRef `json:"tables,omitempty"`   // 空 = 全部表
    StartTime    *time.Time        `json:"startTime,omitempty"`
    EndTime      *time.Time        `json:"endTime,omitempty"`
    GTIDSet      string            `json:"gtidSet,omitempty"` // 空 = 不限；非空则 preflight 确认 MySQL 开了 GTID
    StartPos     uint32            `json:"startPos,omitempty"`
    EndPos       uint32            `json:"stopPos,omitempty"`
    MaxRowsPerTx int               `json:"maxRowsPerTx,omitempty"` // 0 = 默认 1_000_000
}

// 把一个 ScanRequest 组装成 binlog.Filter（GTIDSet 非空时用 ParseGTIDSet
// 解析，flavor 空默认 "mysql"；解析失败返回 error）。
func (r ScanRequest) ToFilter() (binlog.Filter, error)

// StatementJSON 是 execute 命令的 SQL 条目（server 端由 reverse.Generate
// 产出；agent 端重建为 reverse.Statement，SourceRow/Warnings 留空）。
type StatementJSON struct {
    SQL     string `json:"sql"`
    TxID    string `json:"tx_id"`
    TxOrder int    `json:"tx_order"`
}

// ExecuteRequest 是 execute 命令的 Params 载荷。
type ExecuteRequest struct {
    OperationID string          `json:"operationId"`
    Statements  []StatementJSON `json:"statements"`
    BatchSize   int             `json:"batchSize,omitempty"` // 0 = 默认 50
}

// ResumeRequest 是 resume 命令的 Params 载荷（检查点已由 agent 本地
// FileCheckpointStore 持有，只传 operationId 即可）。
type ResumeRequest struct {
    OperationID string `json:"operationId"`
}

// CancelRequest 是 cancel 命令的 Params 载荷。
type CancelRequest struct {
    OperationID string `json:"operationId"`
}
```

- [ ] **Step 1: 写失败测试（事件 JSON 往返）**

Create `internal/ws/proto/events_test.go`:

```go
package proto

import (
    "encoding/json"
    "testing"
    "time"

    "github.com/a-shan/mysql-pitr/internal/binlog"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestTransactionEventRoundTrip(t *testing.T) {
    tx, err := binlog.NewTransaction("uuid:1-5", 42, time.Unix(1700000000, 0), "e2e")
    require.NoError(t, err)
    tx.AppendRow(binlog.RowChange{
        Schema: "e2e", Table: "t",
        Action: binlog.ActionDelete,
        Before: []interface{}{1, "a"},
        After:  nil,
        ColumnNames: []string{"id", "name"},
    })
    ev := TransactionEvent{
        TxID: tx.TxID, GTID: "uuid:1-5", XID: 42,
        CommitTime: tx.CommitTime, Schema: "e2e", Truncated: false,
        Rows: []RowChangeJSON{{
            Schema: "e2e", Table: "t", Action: "delete",
            Before: []interface{}{1, "a"}, ColumnNames: []string{"id", "name"},
        }},
    }
    b, err := json.Marshal(ev)
    require.NoError(t, err)
    var got TransactionEvent
    require.NoError(t, json.Unmarshal(b, &got))
    assert.Equal(t, ev, got)

    rt := got.ToTransaction()
    assert.Equal(t, tx.TxID, rt.TxID)
    assert.Equal(t, "uuid:1-5", rt.GTID)
    assert.Equal(t, uint64(42), rt.XID)
    assert.Len(t, rt.Statements, 1)
    assert.Equal(t, binlog.ActionDelete, rt.Statements[0].Action)
    assert.Equal(t, []interface{}{1, "a"}, rt.Statements[0].Before)
    assert.Equal(t, []string{"id", "name"}, rt.Statements[0].ColumnNames)
}
```

Expected: 编译失败（`internal/ws/proto` 不存在）。

- [ ] **Step 2: 实现 events.go**

按上方 Interfaces 实现。`ToTransaction` 中把 `RowChangeJSON.Action` 字符串转回 `binlog.RowAction`（"insert"/"update"/"delete"，未知值返回错误），`Rows` 转回 `[]binlog.RowChange`（nil Before/After 保持 nil）。`TransactionEvent` 缺 TxID 时 `ToTransaction` 报错（构造校验与 Phase 1 一致）。

- [ ] **Step 3: 写 requests 测试 + 实现 requests.go**

```go
func TestScanRequestToFilter(t *testing.T) {
    start := time.Unix(1700000000, 0)
    end := time.Unix(1700003600, 0)
    r := ScanRequest{
        OperationID: "op-1",
        Tables:      []binlog.TableRef{{Schema: "e2e", Table: "t"}},
        StartTime:   &start,
        EndTime:     &end,
        GTIDSet:     "uuid:1-3",
        MaxRowsPerTx: 100,
    }
    f, err := r.ToFilter()
    require.NoError(t, err)
    assert.Equal(t, []binlog.TableRef{{Schema: "e2e", Table: "t"}}, f.Tables)
    assert.Equal(t, start, f.TimeRange.Start)
    assert.Equal(t, end, f.TimeRange.End)
    assert.Equal(t, 100, f.MaxRowsPerTx)

    bad := ScanRequest{GTIDSet: "not-a-gtid-set"}
    _, err = bad.ToFilter()
    assert.Error(t, err)
}
```

`ToFilter` 实现要点：`TimeRange` 只设置非 nil 的时间字段（`StartTime` nil 时 TimeRange 为 nil）；`GTIDSet` 非空时 `binlog.ParseGTIDSet(flavor, r.GTIDSet)`（flavor 空用 "mysql"；该函数已在 Phase 1 Task 3 实现），解析失败返回 error；`MaxRowsPerTx` 0 时保持 0（Scanner 内部默认 1_000_000）。

- [ ] **Step 4: 跑测试**

```bash
cd D:/a-shan && go test ./internal/ws/proto/...
```

Expected: 全部 PASS。

- [ ] **Step 5: 提交**

```bash
cd D:/a-shan && git add internal/ws/proto/ && git commit -m "$(cat <<'EOF'
feat(ws/proto): stream event types and scan/execute request payloads

Adds the JSON contracts shared by agent and server for the Phase 2
protocol: TransactionEvent/SchemaEvent/ProgressEvent/DoneEvent/ErrorEvent
stream events, plus ScanRequest/ExecuteRequest/ResumeRequest/CancelRequest
command payloads. TransactionEvent.ToTransaction restores a
binlog.Transaction for server-side reverse.Generate.
EOF
)"
```

### Task 2: ws 层流式收发 — StreamEvent 消息 + client/hub 流式通道

**Files:**
- Modify: `internal/ws/types.go`
- Modify: `internal/ws/agent/client.go`（加 `SendStreamEvent`）
- Modify: `internal/ws/hub/hub.go`（pending 改 `pendingCall`、readPump 分流 StreamEvent、加 `SendToAgentStream`）
- Modify: `internal/ws/hub/hub_test.go`、`internal/ws/agent/client_test.go`

**Interfaces（新增/修改，不破坏现有签名）:**

```go
// internal/ws/types.go 追加
// StreamEvent 是 agent 主动向 server 推送的流式事件，Cmd 回显触发它的
// Command 的 Cmd（UUID）。Type 取值见 internal/ws/proto 的 Event* 常量。
type StreamEvent struct {
    Cmd    string      `json:"cmd"`
    Type   string      `json:"type"`
    Result interface{} `json:"result,omitempty"`
}

// internal/ws/agent/client.go 追加
// SendStreamEvent 把事件写入连接（单向，不等响应）。线程安全（复用 writeMu）。
func (c *Client) SendStreamEvent(ev ws.StreamEvent) error

// internal/ws/hub/hub.go 内部重构
// pendingCall 取代现有 map[string]chan *ws.Response 的 value 类型。
type pendingCall struct {
    respCh  chan *ws.Response
    onEvent func(ws.StreamEvent) // 可为 nil（纯同步命令）
}

// 追加导出方法：流式版 SendToAgent
// onEvent 在 readPump 协程内同步调用，注意实现不得阻塞（事件量大时
// 调用方应自己做异步转存）。
func (h *Hub) SendToAgentStream(ctx context.Context, agentID string, cmd ws.Command, onEvent func(ws.StreamEvent)) (*ws.Response, error)
```

- [ ] **Step 1: 写失败测试（hub 流式往返）**

在 `hub_test.go` 追加：起 httptest server + 一个连上的假 agent（写一个 goroutine 收命令，回 `ws.Response{ok}` + 若干 `ws.StreamEvent`），`SendToAgentStream` 应收到全部事件再收到 Response；`onEvent` 计数正确。

- [ ] **Step 2: types.go 加 StreamEvent 类型**

- [ ] **Step 3: client.go 加 SendStreamEvent**

内部复用 `writeMu` + `writeJSON`（现有私有方法，确认名字后使用；`Send(cmd)` 已有相同模式，直接对照实现）。

- [ ] **Step 4: hub.go 重构 pending + readPump 分流 + SendToAgentStream**

要点：
- `pending map[string]*pendingCall`；`SendToAgent` 改为内部注册 `pendingCall{respCh, nil}` 后复用公共发送路径（保持对外签名不变）
- `SendToAgentStream` 注册 `pendingCall{respCh, onEvent}`，写 cmd，等 `respCh`（ctx 取消时清理 pending 返回错误）
- readPump 的消息分支顺序：先按 JSON `type` 字段区分——`"stream_event"`（新，见下）→ 找 `pendingCall.onEvent` 调用（Cmd 不匹配则丢弃）；`"response"` → 找 `pendingCall.respCh` 发送；否则按 Command 分发（cert renewal、progress 等现状逻辑）

编码约定：StreamEvent 帧的 JSON 与现有帧一致（`{cmd, type, result}`），agent 侧收帧时用 `ws.StreamEvent` 结构；hub 侧 readPump 用字段判型（`cmd` + `type` 的组合），具体判型方式与现状 handleMessage 保持一致（看现有实现是反序列化为 ws.Command/ws.Response 还是原始字段判断，沿用）。

- [ ] **Step 5: 跑测试**

```bash
cd D:/a-shan && go test ./internal/ws/...
```

Expected: 全部 PASS（含既有 hub/client 测试，无回归）。

- [ ] **Step 6: 提交**

```bash
cd D:/a-shan && git add internal/ws/ && git commit -m "$(cat <<'EOF'
feat(ws): streaming event channel between agent and server

Adds ws.StreamEvent wire type, client.SendStreamEvent for agent-side
event pushes, and hub.SendToAgentStream so the server can subscribe to
per-command event streams. Hub pending map upgraded to pendingCall with
an optional event callback; existing synchronous commands unaffected.
EOF
)"
```

---

## 阶段 B：executor 持久化检查点与 Resume

### Task 3: Checkpoint 携带 Plan + FileCheckpointStore

**Files:**
- Modify: `internal/executor/types.go`（Checkpoint 加 Plan 字段）
- Create: `internal/executor/file_checkpoint.go`、`internal/executor/file_checkpoint_test.go`

**Interfaces:**

```go
// internal/executor/types.go 修改 Checkpoint：
type Checkpoint struct {
    OperationID            string
    LastCompletedStatement int
    Total                  int
    Errors                 []ExecError
    Plan                   Plan   // 新增：Resume 用；JSON 序列化会带上 Statements
}

// internal/executor/file_checkpoint.go
// FileCheckpointStore 把检查点写到 <dir>/<operation_id>.ckpt.json。
// 写用 tmp 文件 + rename 原子替换；目录不存在时创建。
type FileCheckpointStore struct{ dir string }

func NewFileCheckpointStore(dir string) *FileCheckpointStore
func (s *FileCheckpointStore) Load(operationID string) (*Checkpoint, error)
func (s *FileCheckpointStore) Save(c Checkpoint) error
func (s *FileCheckpointStore) Clear(operationID string) error
// Clear 返回 os.IsNotExist 时视为成功（与 InMemory 语义一致：幂等）。
```

- [ ] **Step 1: 写失败测试**

覆盖：Save→Load 往返（含 Plan.Statements 非空、Errors 非空）；Load 不存在的文件返回带 operationID 的 error；Clear 不存在幂等成功；Save 两次后 Load 取到最新；`Plan.DSN` 与 `BatchSize` 保真。

- [ ] **Step 2: types.go 加 Plan 字段 + 实现 file_checkpoint.go**

JSON 序列化用 `encoding/json`（`Plan.Statements` 内 `reverse.Statement.SourceRow` 是 `binlog.RowChange`——含 `interface{}` 列值，可 JSON 化；`Action` 是 int 枚举，可 JSON 化）。原子写：`os.CreateTemp(dir, "*.tmp")` → `json.MarshalIndent` → `Sync` → `Rename`。注意 `Save` 空 OperationID 报错（与 InMemory 一致）。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./internal/executor/...
```

Expected: 全部 PASS（InMemory 测试不受 Plan 字段影响）。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add internal/executor/ && git commit -m "$(cat <<'EOF'
feat(executor): persist plan inside checkpoint and add FileCheckpointStore

Checkpoint now carries the full Plan so Resume can rebuild the execution
context from disk alone. FileCheckpointStore writes <dir>/<id>.ckpt.json
atomically (tmp + rename) as the agent-local side of the dual-write
checkpoint design.
EOF
)"
```

### Task 4: executor.Run/Resume 重构 — 共用 runFrom，Resume 从检查点续跑

**Files:**
- Modify: `internal/executor/executor.go`（重构 Run/Resume 共用 runFrom；Resume 真实现）
- Modify: `internal/executor/executor_test.go`

**现状（Phase 1）：** `Run` 启动时 `Clear` 旧检查点，按 `BatchSize` 分批 Begin/Exec/Commit，每条前查 `ctx.Err()`，写检查点，调 cb；`Resume` 是占位（返回 error，注释说明 server 层责任）。`executor.go:47-57` 附近可确认。

**目标语义：**

```go
// Run：清检查点 → runFrom(plan, 0)
// Resume：store.Load(operationID) → 重建 plan → runFrom(plan, cp.LastCompletedStatement+1)
//   - Load 返回 error（无检查点）→ 返回 "executor: no checkpoint for operation %q"
//   - runFrom 完成且无暂停 → Clear 检查点；Paused → 保留
// runFrom(ctx, plan, startIdx, cb)：从 startIdx 起按批次执行；startIdx == len(Statements)
//   时直接返回完成报告（不 Begin）。
```

- [ ] **Step 1: 写失败测试（Resume 断点续跑）**

用现有 fake `DBConnFactory`（看 `executor_test.go` 里怎么构造）+ `NewFileCheckpointStore(t.TempDir())` 或 InMemory：

```go
func TestResumeFromCheckpoint(t *testing.T) {
    // 构造 12 条语句的 plan，BatchSize 5
    // 先手动 Save 一个 Checkpoint{LastCompletedStatement: 6, Total: 12, Plan: plan}
    // 调用 executor.Resume(ctx, "op-1", cb)
    // 断言：执行了 6 条（statement 7..12），cb 收到 Done 从 7 开始
    // 断言：完成后检查点被 Clear
}

func TestResumeNoCheckpoint(t *testing.T) {
    // Resume 不存在的 operation → error
}
```

- [ ] **Step 2: 重构 executor.go**

把 Run 的执行循环抽成 `runFrom`；Run 调 `Clear` 后进 runFrom(0)；Resume 调 `Load` → 校验 `Plan.Statements` 非空 → runFrom(last+1)。**行为不变式**（Phase 1 已测）：每批 Begin→N 条 Exec→Commit；失败单条记 Errors 继续；ctx 取消回滚当前批、检查点停在上一已完成批、报告 `Paused: true`。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./internal/executor/... && go test ./cmd/agent/...
```

Expected: 全部 PASS（executor 原有 Run 测试无回归；cmd/agent flashback 测试不受影响）。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add internal/executor/ && git commit -m "$(cat <<'EOF'
feat(executor): Resume resumes from persisted checkpoint

Run and Resume share a runFrom(ctx, plan, startIdx, cb) core. Resume
loads the checkpoint via CheckpointStore, rebuilds the plan from the
persisted Plan, and continues at LastCompletedStatement+1. Checkpoint is
cleared on normal completion and kept when paused.
EOF
)"
```

---

## 阶段 C：agent daemon 重写

### Task 5: serve.go 长任务骨架 — ops map、opRun、cancel 注册

**Files:**
- Modify: `cmd/agent/serve.go`、`cmd/agent/serve_test.go`

**背景：** Phase 1 已删除 serve.go 的 legacy PITR handlers 与 mysqlbinlog helpers（提交 6adf5e3），当前只保留 `handleStatus`/`handleShutdown`/`handlePreflight` 和 `wsagent.NewDispatcher()` 路由。本 task 只搭长任务骨架（scan/execute/resume/cancel 注册为占位或最小实现），**不实现扫描/执行逻辑**（Task 6/7）。

**Interfaces（serve.go 内新增）：**

```go
// serveDaemon 新增字段（现状没有 client：NewServeCommand 里是局部变量，
// 本 task 把 client 挂回 daemon）：
//   client *wsagent.Client        // 由 NewServeCommand 创建后赋值给 d.client
//   mu   sync.Mutex
//   ops  map[string]*opRun        // operationId → 运行态
//   ckptDir string                // 检查点目录（Task 7 用）

// opRun 记录一个进行中的长任务。
type opRun struct {
    opID   string
    ctx    context.Context
    cancel context.CancelFunc
    kind   string // "scan" | "execute" | "resume"
    done   chan struct{}
}

// 新增方法：
func (d *serveDaemon) startOp(kind, opID string) (*opRun, error)  // 重复 opID → error
func (d *serveDaemon) getOp(opID string) *opRun
func (d *serveDaemon) endOp(opID string)                          // goroutine 完成时调用
func (d *serveDaemon) handleScan(ctx context.Context, cmd ws.Command) *ws.Response     // Task 6 填实现
func (d *serveDaemon) handleExecute(ctx context.Context, cmd ws.Command) *ws.Response  // Task 7 填实现
func (d *serveDaemon) handleResume(ctx context.Context, cmd ws.Command) *ws.Response   // Task 7 填实现
func (d *serveDaemon) handleCancel(ctx context.Context, cmd ws.Command) *ws.Response   // 本 task 实现
```

- [ ] **Step 1: 确认现状并写骨架**

先读 `cmd/agent/serve.go` 的 `NewServeCommand` 路由区（Phase 1 后用 `wsagent.NewDispatcher()` + `RegisterHandler(ws.CmdStatus/... )`）。把 `client := wsagent.NewClient(...)`（serve.go:234）改为在 `newServeDaemon` 里留空字段、`NewServeCommand` 创建 client 后赋给 `d.client`（**其余连接/重连逻辑不动**）。在 dispatcher 上注册四个新命令：

```go
d.RegisterHandler(ws.CmdStatus, d.handleStatus)
d.RegisterHandler(ws.CmdShutdown, d.handleShutdown)
d.RegisterHandler(ws.CmdPreflight, d.handlePreflight)
d.RegisterHandler(ws.CmdScan, d.handleScan)         // 新增
d.RegisterHandler(ws.CmdExecute, d.handleExecute)   // 新增
d.RegisterHandler(ws.CmdResume, d.handleResume)     // 新增
d.RegisterHandler(ws.CmdCancel, d.handleCancel)     // 新增
```

`handleCancel` 实现（本 task 即可完成）：

```go
func (d *serveDaemon) handleCancel(ctx context.Context, cmd ws.Command) *ws.Response {
    var req proto.CancelRequest
    if err := decodeParams(cmd.Params, &req); err != nil {
        return errResp(cmd, "%v", err)
    }
    op := d.getOp(req.OperationID)
    if op == nil {
        return errResp(cmd, "no running operation %q", req.OperationID)
    }
    op.cancel()
    return okResp(cmd, map[string]interface{}{"operationId": req.OperationID, "cancelled": true})
}
```

`decodeParams` 辅助（新增）：`json.Marshal(cmd.Params)` → `json.Unmarshal(&req)`（沿用 params 的 map 机制，新命令都走它）。

`handleScan`/`handleExecute`/`handleResume` 本 task 先返回 `okResp(cmd, map[string]interface{}{"operationId": ..., "status": "started"})` 占位 + TODO 注释，Task 6/7 填实。

- [ ] **Step 2: 写 opRun 管理测试**

`serve_test.go`（现有 HTTP/WS 级测试基础设施）或新增纯逻辑测试：`startOp` 重复 id 报错；`cancel` 后 ctx.Err() 为 `context.Canceled`；`endOp` 后 `getOp` 返回 nil。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./cmd/agent/... && go build ./...
```

Expected: 全部 PASS。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add cmd/agent/ && git commit -m "$(cat <<'EOF'
feat(agent): long-running operation registry and cancel command

serve.go gains an opRun registry (map[operationId] -> ctx/cancel/kind)
and a cancel handler that aborts the matching in-flight operation.
scan/execute/resume handlers are registered as stubs to be filled in by
follow-up tasks.
EOF
)"
```

### Task 6: handleScan — Scanner 流式回传事务与 schema

**Files:**
- Modify: `cmd/agent/serve.go`、`cmd/agent/serve_test.go`

**实现：**

```go
// handleScan 解析 ScanRequest → binlog.Filter → 启动扫描 goroutine：
//   1. 连接本地 MySQL（connector.NewMySQLConnector + Connect）
//   2. preflight 确认 GTID（GTIDSet 非空时）+ resolveDataDir 兜底 BinlogDir
//   3. NewScanner(conn, WithMaxRowsPerTx(...))
//   4. 逐事务：scanner.Next() → client.SendStreamEvent(EventTransaction)
//      （schema 表缓存：首次遇到某 schema.table 时先发 EventSchema）
//   5. 扫描结束（io.EOF）→ SendStreamEvent(EventDone{Kind:"scan", Total:n})
//      错误 → SendStreamEvent(EventError{Message})
//   6. 每 50 个事务发一次 EventProgress（可选，便于 UI 感知扫描进度）
// goroutine 结束时 endOp。
func (d *serveDaemon) handleScan(ctx context.Context, cmd ws.Command) *ws.Response
```

**关键点：**
- handler 只做：解析 params → `startOp("scan", opID)`（重复 id 返回 error）→ `go d.runScan(op, cmd, req)` → 立即返回 `okResp("started")`
- runScan 里所有事件用 `d.client.SendStreamEvent`（Task 5 已把 client 挂回 serveDaemon）
- `scanner.Next()` 返回 `io.EOF` 是正常结束；其他 error → ErrorEvent + endOp
- 事件序列：可选的 schema 事件紧跟首个引用该表的事务事件**之前**；事务事件按扫描顺序发送
- `ScanRequest.Tables` 空时 `Filter.Tables` 为 nil（不过滤）
- 超时：整个 scan 受 `op.ctx`（cancel 命令可断）控制，不用固定超时

- [ ] **Step 1: 写失败测试（扫描流程）**

用 `cmd/agent/flashback_test.go` 现有的 `newBinlogScanner` 包级 seam（Phase 1 Task 15 建立的）替换 scanner：fake scanner 产出 2 个事务 → 断言：
- 发出 2 个 `EventTransaction`（先后顺序、行数据 JSON 可还原）
- 发出 1 个 `EventSchema`（首表）
- 最后 1 个 `EventDone{Total: 2}`
- opRun 结束后 `getOp` 为 nil

`serve_test.go` 测试模式参考现有 handler 测试（直接构造 daemon + fake client？看现有 serve_test.go 怎么注入——若用真 WS 链路则用 httptest + fake server 抓事件帧）。

- [ ] **Step 2: 实现 handleScan + runScan**

按上述关键点。注意 `serveDaemon` 需要 `client` 字段（确认现状后决定补回）；`ckptDir` 本 task 暂不用。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./cmd/agent/... && go build ./...
```

Expected: 全部 PASS。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add cmd/agent/ && git commit -m "$(cat <<'EOF'
feat(agent): scan command streams transactions over WebSocket

handleScan dispatches a background scanner that sends SchemaEvent +
TransactionEvent stream events per transaction and finishes with
DoneEvent. Cancellation via the op registry context aborts the scan.
EOF
)"
```

### Task 7: handleExecute / handleResume — executor 流式执行与恢复

**Files:**
- Modify: `cmd/agent/serve.go`、`cmd/agent/serve_test.go`

**实现：**

```go
// handleExecute：解析 ExecuteRequest → 重建 []reverse.Statement →
//   executor.NewExecutor(connector.AsDB, NewFileCheckpointStore(d.ckptDir)) →
//   go runExecute(op, cmd, plan) → 立即返回 okResp("started")
// runExecute：ex.Run(op.ctx, plan, cb)；
//   cb 里每 10 条发一次 EventProgress；完成后 EventDone{Kind:"execute",
//   Total, Errors, Paused}（Paused 为 true 时检查点保留，供 resume）
//
// handleResume：解析 ResumeRequest → 同一个 store → ex.Resume(op.ctx, opID, cb)
//   → EventDone{Kind:"resume", ...}
func (d *serveDaemon) handleExecute(ctx context.Context, cmd ws.Command) *ws.Response
func (d *serveDaemon) handleResume(ctx context.Context, cmd ws.Command) *ws.Response
```

**关键点：**
- `ckptDir` 初始化：`cfg.DataDir`（读 config.Config 字段确认名字）非空用它，否则 `os.TempDir()/mysql-pitr-agent/checkpoints`（`os.MkdirAll`）
- `connector.ConnConfig.AsDB` 在 Phase 1 Task 14 已实现（executor.DB 适配）；`NewExecutor(dbFactory, store)` 的工厂签名对照 Phase 1
- `StatementJSON` → `reverse.Statement`：`SourceRow` 置零值、`Warnings` 置空（执行不需要）
- 进度 cb 里 `Progress` → `proto.ProgressEvent`（Errors 转 `[]proto.ErrorEntry`）
- `handleCancel` 已能在执行中取消（op.ctx 传播到 executor.Run，Task 4 的取消语义生效）

- [ ] **Step 1: 写失败测试（execute 事件流 + resume 复用检查点）**

用 fake DBConnFactory（executor 测试的模式）+ 临时目录 `FileCheckpointStore`：execute 发出 progress/done 事件、完成时检查点被 Clear；手动预置检查点后 resume 从断点续跑并发 `EventDone{Kind:"resume"}`。

- [ ] **Step 2: 实现**

按上述结构。**注意 executor 的 DB 连接来源**：`Plan.DSN` 由 server 下发？还是 agent 用自己的 `connCfg`？——设计决策：**agent 忽略 Plan.DSN，用自己的 `d.connCfg` 建连接**（agent 永远连本地 MySQL，DSN 由 server 下发不可信/不必要）。`DBConnFactory` 用 `connector.NewMySQLConnectorWithDB` 或直接 `NewMySQLConnector` + `AsDB()`（对照 Phase 1 flashback.go 怎么给 executor 传连接）。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./cmd/agent/... && go test ./internal/executor/...
```

Expected: 全部 PASS。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add cmd/agent/ && git commit -m "$(cat <<'EOF'
feat(agent): execute and resume commands stream executor progress

handleExecute runs the checkpointed executor against the local MySQL
connection (ignoring any server-provided DSN), streaming ProgressEvent
and finishing with DoneEvent. handleResume resumes from the agent-local
FileCheckpointStore. Cancellation rolls back the current batch per the
Phase 1 semantics.
EOF
)"
```

---

## 阶段 D：server SQLite 仓储层

### Task 8: 引入 modernc.org/sqlite + store 包骨架（Open/Migrate/建表）

**Files:**
- Modify: `go.mod`（go get modernc.org/sqlite）
- Create: `internal/server/store/db.go`、`internal/server/store/db_test.go`
- Create: `internal/server/store/migrations.go`

**DDL（`internal/server/store/migrations.go`，v1）：**

```sql
-- users: id INTEGER PK AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
--        password_hash TEXT NOT NULL, created_at TEXT NOT NULL
-- orgs: id, name UNIQUE NOT NULL, created_by INTEGER, created_at
-- members: org_id, user_id, role TEXT, joined_at, PRIMARY KEY(org_id, user_id)
-- agents: id TEXT PK, org_id INTEGER, name, status, host, binlog_dir,
--         created_at, updated_at
-- operations: id TEXT PK, org_id INTEGER, kind TEXT, status TEXT,
--             filter_json TEXT, preview_json TEXT, selected_ids TEXT,
--             progress_json TEXT, report_json TEXT, created_at, updated_at
-- checkpoints: operation_id TEXT PK, payload_json TEXT, updated_at
-- audit_logs: id INTEGER PK AUTOINCREMENT, org_id, actor_id, action TEXT,
--             detail_json TEXT, created_at
-- ca_state: key TEXT PK, value TEXT NOT NULL  -- CA 证书/私钥 JSON
```

- [ ] **Step 1: 加依赖 + 写迁移测试**

```bash
cd D:/a-shan && go get modernc.org/sqlite@latest
```

`db_test.go`：`Open(t.TempDir()+"/test.db")` → `Migrate(db)` → `PRAGMA user_version` 应为 1；重复 Migrate 幂等；`PRAGMA table_list`（或 `sqlite_master` 查询）断言 8 张表存在。**测试用 t.TempDir()，不留脏文件。**

- [ ] **Step 2: 实现 db.go + migrations.go**

`Open(path)` 返回 `*sql.DB`（DSN 用 Global Constraints 的 pragma 串）；`Migrate(db)` 用 `PRAGMA user_version` 判断，=0 时在事务里执行 v1 DDL 并 `PRAGMA user_version=1`。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./internal/server/store/...
```

Expected: 全部 PASS。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add go.mod go.sum internal/server/store/ && git commit -m "$(cat <<'EOF'
feat(server/store): sqlite open/migrate scaffolding with v1 schema

Adds modernc.org/sqlite, the store package, and a PRAGMA user_version
based migration creating users/orgs/members/agents/operations/
checkpoints/audit_logs/ca_state tables.
EOF
)"
```

### Task 9: SQLite store 实现 — 替换 5 个 in-memory store

**Files:**
- Create: `internal/server/store/users_store.go`、`orgs_store.go`、`agents_store.go`、`operations_store.go`、`audit_store.go`、`ca_store.go` + 各 `_test.go`
- Modify: 现有 `internal/server/{auth,org,agent,pitr,audit}` 的 store 接口定义（如果接口定义在各自包内，则 SQLite 实现放到 store 包并满足接口；**接口本身不改变签名**）

**做法：** 逐个对齐现有 `XStore` 接口的方法清单（读 `internal/server/auth/store.go`、`org/store.go`、`agent/store.go`、`pitr/store.go`、`audit/store.go` 的方法签名），在 store 包内实现 `SQLiteXStore`。每个 store 一个 task step，先写失败测试（真实 SQLite 文件 + 临时目录）再实现：

- [ ] **Step 1: users_store.go**（Create/GetByEmail/GetByID/…按现有接口）
- [ ] **Step 2: orgs_store.go + members**（Create/List/AddMember/…）
- [ ] **Step 3: agents_store.go**（Create/ListByOrg/Get/UpdateStatus/…）
- [ ] **Step 4: operations_store.go**（Create/Get/Update/ListByOrg/…）
- [ ] **Step 5: audit_store.go**（Append/Query/…）
- [ ] **Step 6: ca_store.go**（Load/Save/…，替代 FileStorage 的职责）
- [ ] **Step 7: 提交**

```bash
cd D:/a-shan && go test ./internal/server/... && git add internal/server/store/ && git commit -m "$(cat <<'EOF'
feat(server/store): sqlite-backed stores for all platform entities

SQLite implementations of the auth/org/agent/pitr/audit store interfaces
plus CA state storage, matching each existing interface method-by-method
without changing the interfaces themselves.
EOF
)"
```

### Task 10: server.New 接线 SQLite store

**Files:**
- Modify: `internal/server/server.go`
- Modify: `internal/server/router.go`（若 store 注入方式变化）
- Modify: `cmd/server/main.go`（数据目录参数）

- [ ] **Step 1: 改 server.New 签名**

`New(opts ...Option)` 增加 `WithDataDir(path)` / `WithDB(db *sql.DB)` Option（对照现有 `New()` 的写法）：`WithDataDir` 时 `store.Open(path+"/app.db")` + `Migrate`；默认（测试便捷路径 `NewRouter()`）仍用 in-memory store，**现有测试全部不破坏**。

- [ ] **Step 2: 替换 5 个 store 构造**

`New()` 里 5 个 in-memory store 换成 SQLite 版本（`WithDB` 时）；CA 从 `ca_state` 表恢复（`ca.LoadFromStore`）；`ca.json` FileStorage 保留为向后兼容读取（已有 agent 的证书在文件里）或直接迁移——**决策：优先从 SQLite 读，文件存在且表为空时导入一次**。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./internal/server/... && go build ./...
```

Expected: 全部 PASS（in-memory 路径测试不变；新增 `WithDataDir` 冒烟测试：起 server → 注册用户 → 重启 → 用户仍在）。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add cmd/server/ internal/server/ && git commit -m "$(cat <<'EOF'
feat(server): wire sqlite stores into server startup

Server.New accepts a data dir / db option; platform state now persists
across restarts via SQLite (users/orgs/agents/operations/audit/CA).
The existing in-memory path is kept for tests via NewRouter().
EOF
)"
```

---

## 阶段 E：server pitr 状态机 + SSE

### Task 11: pitr 状态机重写 — 两阶段事务选择

**Files:**
- Modify: `internal/server/pitr/state.go`（状态集迁移到 spec 8 态）
- Modify: `internal/server/pitr/store.go`（Operation 结构扩展）
- Modify: `internal/server/pitr/handler.go`（runOperation 重写为两阶段）
- Modify: `internal/server/pitr/` 各测试

**目标状态集（spec）：**

```go
// state.go
const (
    StateCreated   OperationState = "created"
    StateScanning  OperationState = "scanning"
    StateReady     OperationState = "ready"
    StateExecuting OperationState = "executing"
    StatePaused    OperationState = "paused"
    StateDone      OperationState = "done"
    StateFailed    OperationState = "failed"
    StateCancelled OperationState = "cancelled"
)
// 迁移：created→scanning→ready→executing→(paused↔executing)→done
//       executing→failed/cancelled；scanning→failed/cancelled
```

**Operation 结构扩展（store.go）：**

```go
type Operation struct {
    ID          string
    OrgID       int64
    Kind        string   // "scan" | "execute"
    State       OperationState
    Filter      *proto.ScanRequest
    Transactions []proto.TransactionEvent   // scan 流回传的事务（元数据+行）
    SelectedIDs []string
    Progress    *executor.Progress
    Report      *executor.FinalReport
    Error       string
    CreatedAt, UpdatedAt time.Time
}
```

**两阶段流程（handler.go）：**

```
POST /api/pitr           {filter} → Create op(created) → 发 scan 命令（流式）
  agent 回传 transaction/schema 事件 → SSE 推送 + op.Transactions 累积 → scan done
  → op=ready
POST /api/pitr/{id}/execute {selected_ids}
  → server 端对选中事务调 reverse.Generate（需 schema：scan 时已缓存）
  → op=executing → 发 execute 命令（plan）→ progress 事件写 op.Progress → done
POST /api/pitr/{id}/cancel   → 发 cancel 命令 → op=cancelled/paused
POST /api/pitr/{id}/resume   → 发 resume 命令（server 从 SQLite checkpoints 表取
  checkpoint 下发给 agent 或直接发 resume 命令让 agent 用本地检查点）
```

- [ ] **Step 1: 写失败测试（状态机迁移 + 两阶段流）**

在现有 pitr handler 测试基础上（fake AgentCommander 已有）改造：fake commander 支持流式回调，模拟 agent 回传 2 个事务 + done → 断言 op 状态 scanning→ready、Transactions 2 条；execute 模拟 progress/done → executing→done；cancel → cancelled。

- [ ] **Step 2: 重写 state.go + store.go**

按上述。**现有状态常量（confirmed/parsing/previewed 等）全部迁移**，检查 server/pitr 及依赖处的引用并一并改掉（`TransitionValid` 迁移表同步更新）。

- [ ] **Step 3: 重写 handler.go runOperation**

两阶段；`AgentCommander` 接口扩展：

```go
// internal/server/pitr/handler.go
type AgentCommander interface {
    IsConnected(agentID string) bool
    SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error)
    SendToAgentStream(ctx context.Context, agentID string, cmd ws.Command,
        onEvent func(ws.StreamEvent)) (*ws.Response, error)   // 新增
}
```

`*hub.Hub` 满足新接口（Task 2 已加 SendToAgentStream）。事件处理：`onEvent` 里把事务/进度写入 op（加锁）并推 SSE 通道（Task 12 的通道先预置字段）。

- [ ] **Step 4: 跑测试**

```bash
cd D:/a-shan && go test ./internal/server/... && go build ./...
```

Expected: 全部 PASS。

- [ ] **Step 5: 提交**

```bash
cd D:/a-shan && git add internal/server/pitr/ && git commit -m "$(cat <<'EOF'
refactor(server/pitr): two-phase transaction selection state machine

Operation state set moves to created/scanning/ready/executing/paused/
done/failed/cancelled. Scan streams candidate transactions (metadata +
row data) via ws events into the operation; execute generates reverse
SQL server-side and dispatches the plan to the agent.
EOF
)"
```

### Task 12: SSE 进度端点 /api/pitr/{id}/events

**Files:**
- Modify: `internal/server/pitr/handler.go`（SSE handler）
- Modify: `internal/server/router.go`（注册路由）

**实现：**

```go
// GET /api/pitr/{id}/events（JWT 保护，SSE）
// 1. 校验 op 存在且属于当前 org（403 跨组织）
// 2. 立即发一条 event: state <当前状态>
// 3. 订阅 op 的 event channel（handler 里 op 增加 subs map / chan 列表，
//    SSE handler 结束时注销）
// 4. 每条 agent 流事件（transaction/progress/done/error）转 SSE：
//    event: transaction\ndata: {"tx_id":...}\n\n
// 5. 心跳注释行（每 15s 发 ": ping\n\n"）防代理超时
// 6. 连接断开（http.Flusher + ctx 取消）清理订阅
```

- [ ] **Step 1: 写失败测试**

`httptest` + 已注入事件的 op：读 SSE 流断言事件顺序与格式；跨组织 403；`op` 不存在 404。

- [ ] **Step 2: 实现**

chi 路由 `/api/pitr/{id}/events` 注册在 JWT 保护组；`Operation` 增加 `subs map[chan ws.StreamEvent]struct{}`（加锁）+ `subscribe/unsubscribe` 方法；pitr handler 的 onEvent 写 op 数据的同时广播给所有订阅者。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./internal/server/...
```

Expected: 全部 PASS。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add internal/server/ && git commit -m "$(cat <<'EOF'
feat(server/pitr): SSE progress endpoint for operation events

GET /api/pitr/{id}/events streams transaction/progress/done events as
Server-Sent Events with heartbeat pings. Subscribers are per-operation
channels cleaned up on disconnect.
EOF
)"
```

---

## 阶段 F：检查点双写 + 全链路集成

### Task 13: 检查点双写 — agent ckpt.json + server SQLite + resume 链路

**Files:**
- Modify: `internal/server/pitr/handler.go`（execute 后写 checkpoints 表；resume 读）
- Modify: `internal/server/store/operations_store.go`（checkpoints 表读写方法）
- Modify: `cmd/agent/serve.go`（若 resume 需要 server 下发 checkpoint 对齐）

**设计（spec）：**

```
execute 时：
  agent 本地 FileCheckpointStore 持续写 <data>/<op_id>.ckpt.json（已实现）
  agent 每批完成后发 EventProgress{...} → server 把 executor.Progress 写入
  SQLite checkpoints 表（payload_json 存 Progress + Plan）
resume 时：
  用户 POST /api/pitr/{id}/resume
  server 读 SQLite checkpoints 表 → 有：发 resume 命令（agent 用本地 ckpt 续跑，
    完成后 server 再更新 SQLite 检查点）；无：报 409 "no checkpoint"
agent 重启后：
  新连接建立时 server 把 SQLite 里的 in-flight ops 标记 paused（现状
  OnConnect/OnDisconnect 钩子已有，补 op 状态迁移）
```

- [ ] **Step 1: 写失败测试（双写一致性）**

store 层：Save/Get 检查点 round-trip；pitr handler：execute done 后检查点表有数据；resume 无检查点 409；resume 有检查点 → 发 `resume` 命令断言命令参数。

- [ ] **Step 2: 实现**

operations_store.go 加 `SaveCheckpoint(opID, payloadJSON)` / `GetCheckpoint(opID)`；handler 在 progress 事件里写检查点（节流：每完成一批写一次）；resume handler 检查并下发。

- [ ] **Step 3: 跑测试**

```bash
cd D:/a-shan && go test ./internal/server/... && go test ./cmd/agent/... && go build ./...
```

Expected: 全部 PASS。

- [ ] **Step 4: 提交**

```bash
cd D:/a-shan && git add internal/ cmd/ && git commit -m "$(cat <<'EOF'
feat(pitr): dual-write checkpoints and resume flow

Agent persists checkpoints to local ckpt.json (already), server mirrors
progress into SQLite checkpoints table. Resume serves from either side:
agent-local for in-flight recovery, server-side for UI-driven resume.
EOF
)"
```

### Task 14: 集成测试 — server↔agent↔MySQL 容器 + CI 更新 + 全量验证

**Files:**
- Create: `internal/server/integration_test.go`（`//go:build integration`）
- Modify: `.github/workflows/`（CI 加 integration job，若已有 CI 文件；确认 `.github/workflows` 现状）
- Modify: `cmd/agent/serve_test.go` 或新增 e2e 脚本

**集成场景（真 MySQL 容器，沿用 Phase 1 `internal/binlog/e2e_test.go` 的环境变量模式：`E2E_MYSQL_DSN`）：**

```
1. 起 server（内存或 SQLite 临时目录）+ 真 agent（连接同一 server）+ MySQL 8.0
2. MySQL 里建表插数 → DELETE 一行的操作日志
3. 通过 pitr API 创建 scan op → 断言 SSE 收到 transaction 事件 → op=ready
4. 勾选事务 → execute → 断言 MySQL 行恢复 → op=done
5. cancel 场景：大事务执行中 cancel → op=paused → resume → 完成
```

- [ ] **Step 1: 写集成测试骨架**

对照 `internal/binlog/e2e_test.go` 的 `//go:build integration` + env guard 模式；复用现有 `ws/hub` 的 httptest 起 server，agent 用真 `wsagent.NewClient` 连本地。

- [ ] **Step 2: 本地跑通**

```bash
cd D:/a-shan && go test -tags integration ./internal/server/... -run TestIntegration -v
```

（需要本机 docker MySQL 8.0 + binlog/GTID 开启；对照 Phase 1 的执行方式。）

- [ ] **Step 3: 更新 CI（若存在 workflows）**

加 integration job（docker compose 起 MySQL 后 `go test -tags integration`）。

- [ ] **Step 4: 全量回归**

```bash
cd D:/a-shan && go build ./... && go test -race ./...
```

Expected: 全部 PASS。

- [ ] **Step 5: 提交**

```bash
cd D:/a-shan && git add internal/server/ .github/ && git commit -m "$(cat <<'EOF'
test(pitr): end-to-end integration across server, agent, and MySQL

Build-tagged integration test spins a real server + agent + MySQL 8.0
and walks scan -> ready -> execute -> done, plus cancel/resume recovery.
EOF
)"
```

---

## Self-Review

**1. Spec coverage（redesign spec 对应关系）:**

| Spec 要求 | 对应 Task |
|---|---|
| ws/proto 消息（Command/StreamEvent/Response） | Task 1、2 |
| agent daemon 重写（serve.go） | Task 5、6、7 |
| 检查点化执行 + Resume | Task 3、4、7 |
| 检查点双写（agent JSON + server SQLite） | Task 13 |
| server SQLite 仓储层（8 张表） | Task 8、9、10 |
| pitr 状态机重写（8 态 + 事务选择两阶段） | Task 11 |
| 反向 SQL 生成移到 server | Task 11（server 端调 reverse.Generate） |
| SSE 进度推送 | Task 12 |
| 审计先于副作用 | 现有 audit handler 接线 SQLite（Task 9、10），状态机迁移写入审计保持 |
| 覆盖率 ≥ 90%（server/pitr、store、ws） | 各 task 的失败测试 + 覆盖率检查 |

**2. 未在本计划实现（后续 phase）：**
- 前端 SvelteKit 重写（Phase 3）
- 前端轮询 /progress 迁移到 SSE 消费（Phase 3 与 REST 并存）
- mysqlbinlog 路径退役清理（Phase 1 已删 parser；agent 配置项 MySQLBinlogPath 的移除可在 Phase 3）

**3. 类型一致性：**
- `ws.StreamEvent.Cmd` 恒等于触发命令的 `ws.Command.Cmd`（UUID）
- `proto.TransactionEvent.ToTransaction` 与 `binlog.Transaction` 字段一一对应
- `proto.ErrorEntry` ↔ `executor.ExecError` 双向转换
- `Operation.Transactions` 存 `proto.TransactionEvent`（不做二次 JSON）
- `executor.Checkpoint.Plan` 是唯一持久化执行上下文（agent 端）

**4. 风险与未决：**
- Task 2 中 hub readPump 的帧判型方式需对照现有实现（反序列化结构 vs 原始字段判断），以最小 diff 接入 StreamEvent 分支
- Task 5 中 `serveDaemon.client` 字段：方案已定（Task 5 把 NewServeCommand 的局部 client 挂回 daemon），执行时按现状代码落位
- Task 9 的 5 个现有 store 接口方法清单以实际代码为准，SQLite 实现逐方法对齐；若接口含不可 SQLite 化的语义（如内存排序），在 store 包内以 SQL 语义等价实现并在测试断言
- Task 11 中现有 8 态 state.go 已有 `TransitionValid` 迁移表，重写时同步更新避免漏迁移
- `modernc.org/sqlite` 首次引入需 `go get`，若网络受限执行时处理
- spec 中"agent 忽略 Plan.DSN 用自己的连接"是计划决策；若后续需要跨机执行，改为下发 DSN（Phase 3 决策）
- 新协议命令（scan/execute/resume）与旧前端命令（pitr_parse/pitr_execute）并存：前端未重写前旧命令继续可用；`CmdPITRParse`/`CmdPITRExecute` 常量保留不删（Phase 3 前端迁移后清理）

---

## Plan 完成
