# PITR v3 Phase 2：agent daemon（归档循环 + 执行器 + 命令处理）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Phase 1 的采集引擎闭环为可运行的 agent：归档循环（回填+增量+自愈）、检查点化执行器、WS 命令处理（scan/execute/resume/cancel/archive_status）、flashback CLI 重接，删除旧解析器。

**Architecture:** agent 守护进程在后台跑**归档循环**（`internal/collector`）：启动时 reconcile（SHOW BINARY LOGS 对比归档目录补齐缺口 + 清理陈旧 .partial），用 binlogsyncer 从上次归档位置续拉增量事件，`archive.Writer` 落盘（backfill 过的当前文件用 append 续写，轮转时封口验证）；WS 命令层（`internal/daemon`）把 scan/execute/resume/cancel 映射到 Phase 1 引擎与 `internal/executor`（检查点化批量执行，agent 本地文件检查点）；`cmd/agent` 的 flashback CLI 与 serve daemon 重接新引擎。

**Tech Stack:** Go 1.25、go-mysql v1.16.0、现有 ws/types.go 协议、cobra、testify、sqlmock、worktree 已验证基线（`.claude/worktrees/pitr-v2-phase1/internal/{executor,ws/agent}` 与 `cmd/agent/{flashback,serve}`）。

## 本计划范围（阶段拆分）

- Phase 1（已完成）：采集引擎核心——`internal/{binlog,reverse,scan,archive,stream,binlogtest}`
- **Phase 2（本计划）**：agent daemon——归档循环、executor、WS 命令处理、CLI 重接、删旧包
- Phase 3：server 平台（SQLite 仓储、操作状态机、SSE、REST、WS hub 协议接线）
- Phase 4：SvelteKit Web

## Global Constraints

- Go 工具链 1.26.5；go.mod `go 1.25.0`；GOPROXY=goproxy.cn（GitHub 直连不通）
- go-mysql v1.16.0 已锁（go.mod 现状，不得降级）
- **不得破坏现有构建**：`internal/server`、`web/` 保持原样（Phase 3 才动 server）；每任务结束 `go build ./...` 必须通过
- **删除旧包顺序**：必须先重接 `cmd/agent`（Task 8/9），最后删 `internal/parser` + `internal/rollback`（Task 9），期间旧包测试的既有失败（`internal/parser` 11 个）不是本工作缺陷，Task 9 删除后自然消失
- 复用基线：`.claude/worktrees/pitr-v2-phase1/` 的 `internal/executor`、`internal/ws/agent`（client.go 与 main 相同，dispatcher/renewal/client_test 有增量）、`cmd/agent/flashback.go`（307 行新版）、`internal/binlog/e2e_test.go`、`internal/connector` 的 FetchSchema/AsDB
- flavor：仅 MySQL；新包只依赖标准库 + go-mysql + 仓库内低层包，禁止依赖 `internal/server`
- TDD：先写失败测试 → 跑通失败 → 实现 → 跑通 → 提交；每任务独立 commit

## Phase 1 交接（本计划必须落实的硬要求）

1. **归档增量接缝**（final-review Important #1）：归档循环必须保证文件边界——回填当前文件头（复制到 master position）后，syncer 从该位置 append 续写尾部；writer 拒绝「全量重建覆盖已封口文件」；reconcile 清理陈旧 .partial
2. **stream EOF 语义**：真实 syncer 不返回 io.EOF（`ErrNeedSyncAgain`/`ErrSyncClosed`），归档循环把「关闭后错误」映射为正常停止/重启
3. **零值 SyncPos 校验**：collector 层校验 SyncPos/SyncGTID 至少其一
4. **schema 拉取缓存**（final-review Important #3）：scan.Stream 循环内缓存 schema map
5. **RowCount 2× 修正**（UPDATE 显示）：`binlog.Transaction.RowCount` 改为按语句数

---

### Task 1: connector 增量合并 + FetchSchema PrimaryKey

把 worktree connector 的 Phase 2 增量并入 main，并让 FetchSchema 输出主键（v3 Task 4 给 `TableSchema` 加了 `PrimaryKey` 字段，worktree 版未填）。

**Files:**
- Modify: `internal/connector/mysql.go`（合并 FetchSchema/AsDB 等 worktree 增量）、`internal/connector/connector.go`（Connector 接口增 FetchSchema）、`internal/connector/types.go`（如需）
- Test: `internal/connector/mysql_test.go`（worktree 版 + 新增 PK 断言）

**Interfaces:**
- Produces: `MySQLConnector.FetchSchema(ctx, schema, table) (binlog.TableSchema, error)` —— 填 `Columns` 且填 `PrimaryKey []string`；`MySQLConnector.AsDB() *sql.DB`（executor 用）
- Consumes: `binlog.TableSchema{Columns, PrimaryKey}`（Phase 1 Task 4 产出）

- [ ] **Step 1: 合并 worktree connector 增量**

```bash
W=/d/a-shan/.claude/worktrees/pitr-v2-phase1
git diff --stat -- internal/connector   # 先看差异清单
# 把 FetchSchema 实现 + AsDB + 相关接口方法与测试合并进 main（以 main 为基准手工合并，
# 不整文件覆盖——main 的 connector 有旧代码仍被 cmd/agent 引用）
```

合并清单（worktree 相对 main 的增量）：`FetchSchema` 方法（mysql.go 与 connector.go 接口）、`AsDB` 方法、`mysql_test.go` 的 FetchSchema 测试（TestFetchSchema_NotConnected/Success/Int64ExpressionValues 等）。注意 main 的 `Connector` 接口加 `FetchSchema` 后，所有实现者都要满足——检查 mock/测试实现。

- [ ] **Step 2: 写失败测试（PK 填充）**

`internal/connector/mysql_test.go` 追加（sqlmock）：

```go
func TestFetchSchema_PopulatesPrimaryKey(t *testing.T) {
    // columns 查询返回 2 列；新增 key_column_usage 查询返回 1 个主键列
    // 断言 TableSchema.PrimaryKey == []string{"id"}
}
```

（实现细节：FetchSchema 在列查询之外，再查一次 `information_schema.KEY_COLUMN_USAGE`：`SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION`。sqlmock 需按查询顺序匹配两个查询，注意 `ExpectQuery` 顺序。）

- [ ] **Step 3: 实现 PK 填充 + 跑测试**

实现后跑 `go test ./internal/connector/ -run TestFetchSchema -v`（全过）与 `go test ./internal/connector/` 全量。

- [ ] **Step 4: 提交**

```bash
git add internal/connector
git commit -m "feat(connector): FetchSchema populates primary key; merge AsDB"
```

---

### Task 2: scan.Stream schema 缓存 + RowCount 修正

落实 Phase 1 交接 #4、#5。

**Files:**
- Modify: `internal/scan/scan.go`（schema map 缓存提升到 Stream 循环）、`internal/binlog/transaction.go`（RowCount）、对应测试

**Interfaces:**
- Consumes: `binlog.SchemaFetcher.FetchSchema`、`binlog.Transaction.RowCount()`
- Produces: `scan.Stream` 每次扫描每个表只拉一次 schema；`RowCount()` 返回语句数

- [ ] **Step 1: 写失败测试**

`internal/scan/scan_test.go` 追加：

```go
type countingFetcher struct {
    binlog.StaticSchemaFetcher
    calls map[string]int
}

func (c *countingFetcher) FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error) {
    key := schema + "." + table
    c.calls[key]++
    return c.StaticSchemaFetcher.FetchSchema(ctx, schema, table)
}

func TestStream_SchemaFetchedOncePerTable(t *testing.T) {
    cf := &countingFetcher{StaticSchemaFetcher: fixtureSchema, calls: map[string]int{}}
    _, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{},
        Mode:       scan.ModeWithSQL,
        SchemaFetcher: cf,
    })
    require.NoError(t, err)
    require.Equal(t, 1, cf.calls["shop.orders"], "同一扫描中每个表只拉一次 schema")
}
```

`internal/binlog/transaction_test.go` 追加：

```go
func TestRowCount_CountsStatements(t *testing.T) {
    tx, _ := NewTransaction("uuid:1", 0, time.Now(), "shop")
    tx.AppendRow(RowChange{Schema: "shop", Table: "t", Action: ActionUpdate, Before: []interface{}{1}, After: []interface{}{2}})
    tx.AppendRow(RowChange{Schema: "shop", Table: "t", Action: ActionInsert, After: []interface{}{3}})
    require.Equal(t, 2, tx.RowCount()) // 两条语句，不是 2×2=4
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/scan/ ./internal/binlog/ -run "TestStream_SchemaFetchedOncePerTable|TestRowCount" -v`
Expected: 两个都 FAIL（当前每事务每表都拉 schema；RowCount 返回 4）

- [ ] **Step 3: 实现**

`internal/scan/scan.go`：`Stream` 内维护 `schemaCache := map[string]binlog.TableSchema{}`，`generateSQL` 改为接收缓存并在内部填充/读取；每个 (schema,table) 只在缓存缺失时 `FetchSchema`。

`internal/binlog/transaction.go`：

```go
// RowCount 返回事务包含的行变更条数（语句数）。
func (t *Transaction) RowCount() int {
    return len(t.Statements)
}
```

检查既有 `RowCount` 测试断言（coverage_test/transaction_test）同步调整。

- [ ] **Step 4: 跑测试验证通过 + 提交**

Run: `go test ./internal/scan/ ./internal/binlog/ -count=1`
Commit: `feat(scan,binlog): per-scan schema cache; RowCount counts statements`

---

### Task 3: executor 迁入 + 文件检查点存储

**Files:**
- Copy: 从 worktree 复制 `internal/executor/` 全部 7 个文件
- Create: `internal/executor/file_store.go`、`internal/executor/file_store_test.go`

**Interfaces:**
- Produces: `executor.Executor`（`Run(ctx, Plan, cb)` / `Resume(ctx, operationID, cb)`）、`Plan{OperationID, Statements []reverse.Statement, DSN, BatchSize}`、`Checkpoint`、`CheckpointStore`、`Progress`、`FinalReport`、`DBConnFactory`（全部沿用 worktree 签名）、`NewFileCheckpointStore(dir string) CheckpointStore`
- Consumes: `reverse.Statement`（Phase 1，签名一致）、`connector.AsDB()`（Task 1）

- [ ] **Step 1: 复制 worktree executor 并跑测试**

```bash
W=/d/a-shan/.claude/worktrees/pitr-v2-phase1
# 注意：executor/types.go 已由 Task 1 随 AsDB 合并提前并入 main（控制器已批准 AsDB() executor.DB 签名）。
# 复制其余文件（不含 types.go）：
cp $W/internal/executor/checkpoint.go $W/internal/executor/doc.go \
   $W/internal/executor/executor.go $W/internal/executor/executor_test.go \
   $W/internal/executor/checkpoint_test.go $W/internal/executor/types_test.go internal/executor/
go test ./internal/executor/ -count=1
```

Expected: 全过（sqlmock 已在依赖）。若 v1.16.0 或 main 的 reverse 差异导致编译错，最小修复。

- [ ] **Step 2: 写失败测试（文件检查点存储）**

`internal/executor/file_store_test.go`：

```go
func TestFileCheckpointStore_RoundTrip(t *testing.T) {
    dir := t.TempDir()
    s := executor.NewFileCheckpointStore(dir)
    cp := executor.Checkpoint{OperationID: "op-1", LastCompletedStatement: 42, Total: 100,
        Errors: []executor.ExecError{{Statement: 7, SQL: "UPDATE t SET a=1", Err: "dup key"}}}
    require.NoError(t, s.Save(cp))
    got, err := s.Load("op-1")
    require.NoError(t, err)
    require.Equal(t, cp, got)
    require.NoError(t, s.Clear("op-1"))
    _, err = s.Load("op-1")
    require.Error(t, err) // 已清除
}

func TestFileCheckpointStore_LoadMissing(t *testing.T) {
    s := executor.NewFileCheckpointStore(t.TempDir())
    _, err := s.Load("nope")
    require.Error(t, err)
}

func TestFileCheckpointStore_AtomicWrite(t *testing.T) {
    // 写入后目录里只有 <op>.json，无 .tmp 残留
}
```

- [ ] **Step 3: 实现 file_store.go**

```go
package executor

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
)

// FileCheckpointStore 把检查点存为 <dir>/<operationID>.json，原子写入（临时文件+rename）。
type FileCheckpointStore struct{ dir string }

func NewFileCheckpointStore(dir string) *FileCheckpointStore { return &FileCheckpointStore{dir: dir} }

func (s *FileCheckpointStore) path(id string) string {
    return filepath.Join(s.dir, id+".json")
}

func (s *FileCheckpointStore) Load(operationID string) (*Checkpoint, error) {
    data, err := os.ReadFile(s.path(operationID))
    if err != nil {
        return nil, fmt.Errorf("executor: load checkpoint %s: %w", operationID, err)
    }
    var cp Checkpoint
    if err := json.Unmarshal(data, &cp); err != nil {
        return nil, fmt.Errorf("executor: parse checkpoint %s: %w", operationID, err)
    }
    return &cp, nil
}

func (s *FileCheckpointStore) Save(c Checkpoint) error {
    data, err := json.Marshal(c)
    if err != nil {
        return fmt.Errorf("executor: marshal checkpoint: %w", err)
    }
    tmp := s.path(c.OperationID) + ".tmp"
    if err := os.WriteFile(tmp, data, 0o644); err != nil {
        return fmt.Errorf("executor: write checkpoint: %w", err)
    }
    if err := os.Rename(tmp, s.path(c.OperationID)); err != nil {
        return fmt.Errorf("executor: commit checkpoint: %w", err)
    }
    return nil
}

func (s *FileCheckpointStore) Clear(operationID string) error {
    err := os.Remove(s.path(operationID))
    if err != nil && !os.IsNotExist(err) {
        return fmt.Errorf("executor: clear checkpoint %s: %w", operationID, err)
    }
    return nil
}
```

- [ ] **Step 4: 跑测试 + 提交**

Run: `go test ./internal/executor/ -count=1`
Commit: `feat(executor): file-backed checkpoint store for agent-local resume`

---

### Task 4: archive.Writer append 续写模式 + 封口语义强化

归档循环需要「回填的当前文件 + syncer 续写尾部」。Phase 1 的 Writer 只能全量重建（写 magic+FDE），且 Seal 无条件 rename 会覆盖已封口文件。

**Files:**
- Modify: `internal/archive/archive.go`、`internal/archive/archive_test.go`

**Interfaces:**
- Consumes: `binlog.Source`（Phase 1）
- Produces:
```go
// ConsumeAppend 把事件流续写到已有文件的 .partial（不写 magic）；
// 事件流以 RotateEvent 结束当前文件（Seal 追加到已封口文件）。
func (w *Writer) ConsumeAppend(ctx context.Context, src binlog.Source, fileName string) error

// Seal 语义升级：
//   - .partial 以 binlog magic 开头（全量重建）：验证含 FDE + 校验和；目标文件已存在 → 拒绝（防覆盖）
//   - .partial 无 magic（append 续写尾部）：magic+partial 拼临时文件验证；目标文件必须已存在 → 追加
func (w *Writer) Seal(partialName string) error
```

- [ ] **Step 1: 写失败测试**

`internal/archive/archive_test.go` 追加：

```go
func TestWriter_ConsumeAppend_AppendsToSealed(t *testing.T) {
    dir := t.TempDir()
    w := archive.NewWriter(dir)
    // 先造一个"已回填"的封口文件：全量重建一份 FDE+XID
    evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
    require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
    require.NoError(t, w.Seal("mysql-bin.000001.partial"))

    // append 续写：XID(2) 尾部
    tail := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftXID(2))}
    require.NoError(t, w.ConsumeAppend(context.Background(), &sliceSource{evs: tail}, "mysql-bin.000001"))
    require.NoError(t, w.Seal("mysql-bin.000001.partial"))

    // 最终文件 = 原封口内容 + 尾部（无重复 magic）
    got, _ := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
    want := append(append([]byte{}, binlogtest.CraftFile(evs)...), tail[0].Raw...)
    require.Equal(t, want, got)
}

func TestWriter_SealFullReconstructionOverExistingRefuses(t *testing.T) {
    dir := t.TempDir()
    w := archive.NewWriter(dir)
    evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
    require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
    require.NoError(t, w.Seal("mysql-bin.000001.partial"))
    // 再次全量重建同名文件 → Seal 必须拒绝（防覆盖）
    require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
    require.Error(t, w.Seal("mysql-bin.000001.partial"))
}

func TestWriter_ConsumeAppendVerifyTailCorruption(t *testing.T) {
    // append 尾部被篡改（破坏 CRC）→ Seal 失败，不追加
    // 实现要点：验证用 magic+partial 拼临时文件后 ParseFile+SetVerifyChecksum(true)
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/archive/ -run TestWriter_ConsumeAppend -v`
Expected: FAIL（ConsumeAppend 不存在 / Seal 行为不符）

- [ ] **Step 3: 实现**

`archive.go` 增 `ConsumeAppend`（与 Consume 同构，但不写 magic、文件名由参数给定、Rotate 时不切新文件只结束当前）：内部共用 `consume(ctx, src, fileName string, writeMagic bool)`。

`Seal` 重写：

```go
func (w *Writer) Seal(partialName string) error {
    src := filepath.Join(w.dir, partialName)
    final := strings.TrimSuffix(src, ".partial")

    data, err := os.ReadFile(src)
    if err != nil {
        return fmt.Errorf("archive: read partial %s: %w", partialName, err)
    }
    _, statErr := os.Stat(final)
    finalExists := statErr == nil

    if len(data) >= 4 && string(data[:4]) == string(binlogMagic) {
        // 全量重建：必须含 FDE 且目标不存在
        if finalExists {
            return fmt.Errorf("archive: refuse full reconstruction over sealed file %s", filepath.Base(final))
        }
        if err := w.verifyParseable(data); err != nil {
            return fmt.Errorf("archive: seal verify %s: %w", partialName, err)
        }
        return os.Rename(src, final)
    }

    // append 续写：目标必须已存在；magic+tail 拼临时文件验证后追加
    if !finalExists {
        return fmt.Errorf("archive: append seal %s but final %s missing", partialName, filepath.Base(final))
    }
    if err := w.verifyParseable(append(append([]byte{}, binlogMagic...), data...)); err != nil {
        return fmt.Errorf("archive: append seal verify %s: %w", partialName, err)
    }
    f, err := os.OpenFile(final, os.O_APPEND|os.O_WRONLY, 0o644)
    if err != nil {
        return fmt.Errorf("archive: open final %s: %w", partialName, err)
    }
    if _, err := f.Write(data); err != nil {
        f.Close()
        return fmt.Errorf("archive: append %s: %w", partialName, err)
    }
    if err := f.Close(); err != nil {
        return fmt.Errorf("archive: close final: %w", err)
    }
    return os.Remove(src)
}

// verifyParseable 用 magic+data 拼临时文件做 ParseFile + SetVerifyChecksum(true)。
func (w *Writer) verifyParseable(data []byte) error {
    tmp := filepath.Join(w.dir, ".verify-"+fmt.Sprint(time.Now().UnixNano()))
    if err := os.WriteFile(tmp, append(append([]byte{}, binlogMagic...), data...), 0o644); err != nil {
        return err
    }
    defer os.Remove(tmp)
    parser := replication.NewBinlogParser()
    parser.SetVerifyChecksum(true)
    return parser.ParseFile(tmp, 0, func(*replication.BinlogEvent) error { return nil })
}
```

（`Consume` 保持现有行为——新文件模式写 magic；Rotate 时若当前是 append 模式则只关文件。）

- [ ] **Step 4: 跑测试 + 全量回归 + 提交**

Run: `go test ./internal/archive/ ./internal/binlog/ ./internal/scan/ -count=1`
Commit: `feat(archive): append continuation mode and sealed-file overwrite guard`

---

### Task 5: internal/collector 归档循环

agent 的核心后台服务：reconcile（缺口补齐 + 陈旧 .partial 清理）→ 初始回填 → syncer 续拉 → 轮转封口 → 状态持久化 → 断线自愈。

**Files:**
- Create: `internal/collector/loop.go`、`internal/collector/state.go`、`internal/collector/loop_test.go`

**Interfaces:**
```go
package collector

// MySQLInfo 抽象归档循环需要的 MySQL 交互（生产=connector，测试=stub）。
type MySQLInfo interface {
    ListBinlogs(ctx context.Context) ([]archive.ManifestFile, error) // SHOW BINARY LOGS
    MasterPosition(ctx context.Context) (mysql.Position, error)      // SHOW MASTER STATUS
}

type Config struct {
    MySQL        MySQLInfo
    BinlogDir    string        // MySQL 侧 binlog 目录（回填复制源）
    ArchiveDir   string        // 归档目录（writer 的 dir）
    ServerID     uint32        // syncer server id（必填 >0）
    RetentionDays int          // 0 = 不清理
    Logger       *slog.Logger
    // SourceFactory 生产 binlogsyncer Source；测试注入 fake。默认实现见 NewSource。
    SourceFactory func(ctx context.Context, pos mysql.Position) (binlog.Source, error)
}

type State struct { // archive_state.json 内容
    LastFile string    `json:"last_file"`
    LastPos  uint32    `json:"last_pos"`  // 最近一次轮转封口的位置（轮转事件位置）
    LastGTID string    `json:"last_gtid,omitempty"`
    UpdatedAt time.Time `json:"updated_at"`
}

func LoadState(dir string) (State, error)
func SaveState(dir string, s State) error

type Loop struct { ... }
func NewLoop(cfg Config, w *archive.Writer) *Loop
// Run 阻塞直到 ctx 取消或 fatal 错误；内部自动重连（指数退避）。
func (l *Loop) Run(ctx context.Context) error
// State 返回当前归档状态（供 archive_status 命令）。
func (l *Loop) State() State
```

- [ ] **Step 1: 写失败测试（状态持久化 + 回填 + 续拉）**

`internal/collector/loop_test.go`（stub MySQLInfo + fake source，用 binlogtest 造事件）：

```go
type stubMySQL struct {
    files []archive.ManifestFile
    pos   mysql.Position
    err   error
}
func (s *stubMySQL) ListBinlogs(ctx context.Context) ([]archive.ManifestFile, error) { return s.files, s.err }
func (s *stubMySQL) MasterPosition(ctx context.Context) (mysql.Position, error)     { return s.pos, s.err }

// fakeSource 依次吐出事件（含一个 FDE + 若干 XID + 一个 Rotate）
type fakeSource struct { evs []binlogtest.Event; cur int; err error }
func (f *fakeSource) Next(ctx context.Context) (*replication.BinlogEvent, error) { ... io.EOF ... }
func (f *fakeSource) Close() error { return nil }

func TestLoop_FirstRunBackfillsAndAppends(t *testing.T) {
    // 归档目录空；MySQL 有两个文件（000001 封口、000002 打开）
    // fake source 从 MasterPosition 起产出 000002 尾部事件（XID(2)）然后 Rotate 到 000003
    // 断言：000001 被整文件回填；000002 = 回填前缀 + append 尾部；000003 新文件以 magic 开头；
    //       archive_state.json 的 LastFile/LastPos 正确
}

func TestLoop_ResumeFromState(t *testing.T) {
    // 预先写好 archive_state.json（LastFile=000002, LastPos=X）与已回填的 000002
    // SourceFactory 收到 (000002, X) 位置 → 断言续拉从该位置开始（fake 记录入参）
}

func TestLoop_ReconcileCopiesMissing(t *testing.T) {
    // 归档缺 000002；MySQL 有 → 回填补齐
}

func TestLoop_SyncErrorBackoffRetries(t *testing.T) {
    // fake source 第一次返回 error，第二次成功 → Run 不应 fatal，应重试（用可控制次数的 factory）
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/collector/ -v`
Expected: 编译失败（collector 包不存在）

- [ ] **Step 3: 实现 loop.go + state.go**

`state.go`：`LoadState`/`SaveState`（原子写，同 FileCheckpointStore 手法；文件不存在返回零值 State 而非错误——区分「从未运行」与「损坏」：损坏才报错）。

`loop.go` 核心逻辑（要点，代码骨架）：

```go
func (l *Loop) Run(ctx context.Context) error {
    if l.cfg.ServerID == 0 { return fmt.Errorf("collector: ServerID required") }
    if err := l.reconcile(ctx); err != nil { return err }
    return l.syncLoop(ctx)
}

// reconcile：回填缺失文件（含打开文件前缀到 master position）+ 清理 .partial
func (l *Loop) reconcile(ctx context.Context) error {
    files, err := l.cfg.MySQL.ListBinlogs(ctx)
    ...
    pos, err := l.cfg.MySQL.MasterPosition(ctx)
    ...
    for _, mf := range files {
        final := filepath.Join(l.cfg.ArchiveDir, mf.Name)
        if _, err := os.Stat(final); os.IsNotExist(err) {
            if err := l.backfillFile(ctx, mf.Name, pos); err != nil { return err }
        }
    }
    // 清理陈旧 .partial（reconcile 时全部清掉——syncer 会重造）
    entries, _ := os.ReadDir(l.cfg.ArchiveDir)
    for _, e := range entries {
        if strings.HasSuffix(e.Name(), ".partial") { os.Remove(filepath.Join(l.cfg.ArchiveDir, e.Name())) }
    }
    return nil
}

// backfillFile：整文件复制；若是当前打开文件（名字==master pos 的文件），只复制前缀
func (l *Loop) backfillFile(ctx context.Context, name string, masterPos mysql.Position) error {
    src := filepath.Join(l.cfg.BinlogDir, name)
    dst := filepath.Join(l.cfg.ArchiveDir, name)
    limit := int64(-1) // 全文件
    if name == masterPos.Name {
        limit = int64(masterPos.Pos) // 复制到 master position（前缀稳定）
    }
    return copyFilePrefix(src, dst, limit) // io.CopyN + 原子落盘（先 .partial 再 rename）
}

// syncLoop：读状态 → 续拉 → 轮转封口 → 更新状态；错误退避重试
func (l *Loop) syncLoop(ctx context.Context) error {
    st, err := LoadState(l.cfg.ArchiveDir)
    if err != nil { return err }
    if st.LastFile == "" {
        // 从未运行：从 master position 开始（不回溯历史——历史已被回填覆盖）
        pos, err := l.cfg.MySQL.MasterPosition(ctx)
        ...
        st = State{LastFile: pos.Name, LastPos: pos.Pos, UpdatedAt: time.Now()}
    }
    pos := mysql.Position{Name: st.LastFile, Pos: st.LastPos}

    for {
        if err := l.syncOnce(ctx, pos); err != nil {
            if ctx.Err() != nil { return nil } // 正常停止
            l.logger.Warn("sync interrupted; retrying", "err", err)
            if !sleepBackoff(ctx) { return nil }
            continue
        }
        return nil
    }
}

// syncOnce：一次连续同步，直到轮转封口或错误
func (l *Loop) syncOnce(ctx context.Context, pos mysql.Position) error {
    src, err := l.cfg.SourceFactory(ctx, pos)
    if err != nil { return err }
    defer src.Close()

    // 判断续写模式：目标文件已封口（回填过）→ ConsumeAppend 且跳过流首 FDE
    final := filepath.Join(l.cfg.ArchiveDir, pos.Name)
    appending := fileExists(final)

    if appending {
        // 丢弃 master 重发的首个 FDE（它不属于文件内容）
        first, err := src.Next(ctx)
        if err != nil { return err }
        if first.Header == nil || first.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
            return fmt.Errorf("collector: expected FDE at stream start, got %v", first.Header)
        }
        if err := l.w.ConsumeAppend(ctx, src, pos.Name); err != nil { return err }
    } else {
        if err := l.w.Consume(ctx, src); err != nil { return err }
    }
    return nil
}
```

`syncOnce` 的轮转封口与状态更新：`Consume`/`ConsumeAppend` 在 Rotate 时已自动 Seal（Phase 1 Consume 在 Rotate 时只关文件——**注意**：Phase 1 的 Consume 不 Seal！轮转时只是关掉当前文件，Seal 由调用方做。Phase 2 归档循环在 `Consume*` 返回后统一 `Seal("当前文件.partial")`。为让循环知道「哪个文件刚被轮转」，`Consume*` 需返回最后处理的文件名——**扩展接口**：`Consume*` 返回值 `(lastFileName string, err error)`。Phase 1 的调用（roundtrip 测试）同步适配。）

轮转后：`SaveState(State{LastFile: nextName, LastPos: 0 或轮转位置, ...})`——简化：轮转后 LastFile=新文件、LastPos=0（新文件从头开始）；下一次 syncOnce 从新文件头开始。`syncOnce` 返回 nil 后循环读最新状态继续。状态里 LastPos 对已轮转文件无意义（文件已封口），记录 LastFile 即可。

`sleepBackoff`：1s 起步、60s 封顶、指数退避，ctx 取消即返回 false。

`SourceFactory` 默认实现（`collector.NewSyncerSource(ctx, pos, cfg)` 包装 `stream.NewSource`，并做交接 #3 的 SyncPos/SyncGTID 校验）。

`RetentionDays`：每次 Seal 后扫归档目录，删除 mtime 早于 cutoff 的封口文件（保留 .partial 与 state 文件）。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go test ./internal/collector/ -count=1 -v`
Commit: `feat(collector): archive loop with backfill, resume, reconcile and state persistence`

---

### Task 6: ws 协议扩展（scan/execute/resume/cancel/archive_status + 流事件）

**Files:**
- Modify: `internal/ws/types.go`
- Test: `internal/ws/types_test.go`

**Interfaces:**
```go
// 新增命令类型
const (
    CmdScan          = "scan"
    CmdExecute       = "execute"
    CmdResume        = "resume"
    CmdCancel        = "cancel"
    CmdArchiveStatus = "archive_status"
)

// ScanRequest 是 scan 命令参数（wire 形态，与 binlog.Filter 一一对应）。
type ScanRequest struct {
    Filter     ScanFilter `json:"filter"`
    Mode       string     `json:"mode"` // "meta" | "sql" | "selected"
    MaxPreview int        `json:"maxPreview"`
}

type ScanFilter struct {
    Tables       []TableRefJSON `json:"tables,omitempty"`
    TimeStart    string         `json:"timeStart,omitempty"`    // RFC3339
    TimeEnd      string         `json:"timeEnd,omitempty"`
    GTIDSet      string         `json:"gtidSet,omitempty"`
    StartFile    string         `json:"startFile,omitempty"`
    StartPos     uint32         `json:"startPos,omitempty"`
    EndFile      string         `json:"endFile,omitempty"`
    EndPos       uint32         `json:"endPos,omitempty"`
    MaxRowsPerTx int            `json:"maxRowsPerTx,omitempty"`
    SelectedTxIDs []string      `json:"selectedTxIds,omitempty"`
}

// ExecuteRequest 是 execute 命令参数。
type ExecuteRequest struct {
    OperationID string            `json:"operationId"`
    Statements  []StatementWire   `json:"statements"`
    BatchSize   int               `json:"batchSize,omitempty"`
}

type StatementWire struct {
    SQL      string   `json:"sql"`
    TxID     string   `json:"txId"`
    TxOrder  int      `json:"txOrder"`
    Warnings []string `json:"warnings,omitempty"`
}

// StreamEvent 是 agent→server 的流式消息（scan 事务/SQL、执行进度、操作结束）。
type StreamEvent struct {
    ID   string          `json:"id"`   // 对应命令 ID
    Kind string          `json:"kind"` // "tx_meta" | "sql" | "scan_done" | "progress" | "op_done" | "op_error"
    Data json.RawMessage `json:"data"`
}

const (
    EvTxMeta   = "tx_meta"
    EvSQL      = "sql"
    EvScanDone = "scan_done"
    EvProgress = "progress"
    EvOpDone   = "op_done"
    EvOpError  = "op_error"
)
```

- [ ] **Step 1: 写失败测试（marshal roundtrip + 常量）**

`internal/ws/types_test.go`：构造 ScanRequest/ExecuteRequest/StreamEvent，marshal→unmarshal 断言字段相等（含 `SelectedTxIDs`、空切片零值、时间字符串原样）。

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/ws/ -v`（先 `go build ./internal/ws/` 确认编译失败）

- [ ] **Step 3: 实现 types.go 扩展**

按上述接口实现（`TableRefJSON{Schema, Table string}` 带 json tag）。

- [ ] **Step 4: 跑测试 + 提交**

Commit: `feat(ws): scan/execute/resume/cancel protocol with stream events`

---

### Task 7: internal/daemon 命令处理层（scan/execute/resume/cancel/archive_status）

命令 handler 的**逻辑层**（不依赖 WS 连接，通过 `EventSink` 接口推送），可单测。

**Files:**
- Create: `internal/daemon/scan.go`、`internal/daemon/execute.go`、`internal/daemon/daemon.go`、`internal/daemon/scan_test.go`、`internal/daemon/execute_test.go`

**Interfaces:**
```go
package daemon

// EventSink 抽象向 server 推送流事件（生产=ws client.Send，测试=fake）。
type EventSink interface {
    Send(ev ws.StreamEvent) error
}

// ScanDeps 是 scan 依赖（可注入）。
type ScanDeps struct {
    ArchiveDir    string
    SchemaFetcher binlog.SchemaFetcher
    MaxRowsPerTx  int
    Logger        *slog.Logger
}

// Scan 启动一次异步扫描；事件经 sink 推送；返回 opID（=命令 ID）供 cancel。
func (d *Daemon) Scan(ctx context.Context, id string, req ws.ScanRequest) error
func (d *Daemon) CancelScan(id string) error
func (d *Daemon) Execute(ctx context.Context, id string, req ws.ExecuteRequest) error
func (d *Daemon) Resume(ctx context.Context, id string, req ws.ExecuteRequest) error
func (d *Daemon) ArchiveStatus() collector.State
```

`Daemon` 持有：`scanDeps`、`executor.Executor`、`collector.State()` 函数、运行中的 op 注册表（`map[string]context.CancelFunc` + mutex）。

- [ ] **Step 1: 写失败测试（scan 流式 + cancel）**

`internal/daemon/scan_test.go`：

```go
type fakeSink struct{ mu sync.Mutex; events []ws.StreamEvent }
func (f *fakeSink) Send(ev ws.StreamEvent) error { f.mu.Lock(); f.events = append(f.events, ev); f.mu.Unlock(); return nil }

func TestScan_StreamsTxMetaAndScanDone(t *testing.T) {
    // fixture 目录（fixtureDir 手法：拷贝 testdata 到临时目录）
    d := NewDaemon(ScanDeps{ArchiveDir: dir, SchemaFetcher: fixtureSchema}, nil, nil)
    sink := &fakeSink{}
    err := d.Scan(ctx, "scan-1", ws.ScanRequest{
        Filter: ws.ScanFilter{}, Mode: "meta", MaxPreview: 0,
    })
    require.NoError(t, err)
    // 等待 scan_done
    // 断言：tx_meta 事件数 == fixture 事务数；最后一个事件 kind == scan_done；ID 均为 "scan-1"
}

func TestScan_CancelStopsStreaming(t *testing.T) {
    // MaxPreview 设大 + 大 fixture（或小 MaxPreview 配合慢消费）→ CancelScan 后无更多事件
}

func TestScan_SelectedSQLModeGeneratesStatements(t *testing.T) {
    // 先 meta 扫一遍拿 TxID，再 Mode="selected" + SelectedTxIDs → sql 事件出现
}
```

（等待 scan_done：sink 轮询或 channel——测试里给 fakeSink 加 `done chan struct{}`，Scan 完成时由 daemon 发 scan_done 后 close。）

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/daemon/ -v`
Expected: 编译失败

- [ ] **Step 3: 实现**

`daemon.go`：`Daemon` 结构 + op 注册表 + `NewDaemon(scanDeps, exec, stateFn)`。

`scan.go`：

```go
func (d *Daemon) Scan(ctx context.Context, id string, req ws.ScanRequest) error {
    filter, err := wsFilterToBinlog(req.Filter) // 转换：时间解析 RFC3339、GTIDSet 解析（binlog.ParseGTIDSet）、位置
    if err != nil { return err }
    mode := scan.ModeMetaOnly
    switch req.Mode {
    case "sql": mode = scan.ModeWithSQL
    case "selected": mode = scan.ModeSelectedSQL
    case "", "meta": mode = scan.ModeMetaOnly
    default: return fmt.Errorf("daemon: unknown scan mode %q", req.Mode)
    }

    ctx, cancel := context.WithCancel(ctx)
    d.registerOp(id, cancel)

    go func() {
        defer d.unregisterOp(id)
        defer cancel()
        cfg := scan.Config{
            ArchiveDir: d.scanDeps.ArchiveDir,
            Filter: filter, Mode: mode,
            MaxPreview: req.MaxPreview,
            SchemaFetcher: d.scanDeps.SchemaFetcher,
            MaxRowsPerTx: d.scanDeps.MaxRowsPerTx,
            Logger: d.scanDeps.Logger,
        }
        ch, errCh := scan.Stream(ctx, cfg)
        for r := range ch {
            if data, err := json.Marshal(r.Meta); err == nil {
                d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvTxMeta, Data: data})
            }
            if len(r.SQL) > 0 {
                data, _ := json.Marshal(r.SQL)
                d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvSQL, Data: data})
            }
        }
        if err := <-errCh; err != nil {
            data, _ := json.Marshal(err.Error())
            d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvOpError, Data: data})
            return
        }
        d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvScanDone, Data: json.RawMessage("{}")})
    }()
    return nil
}
```

（`r.Meta` 是 `scan.TxMeta`——需要可 JSON 序列化：给 `scan.TxMeta` 加 json tag（Phase 1 未加）。**接口调整**：`internal/scan.TxMeta` 加 json tags。）

`execute.go`：`Execute` 把 `ws.ExecuteRequest` 转 `executor.Plan`（Statements 转换），用 `executor.Executor.Run` 跑在 goroutine，Progress callback 发 `EvProgress`，结束发 `EvOpDone`/`EvOpError`；`CancelScan` 复用 cancel 注册表（execute 也注册 op，cancel 发 `EvOpDone{paused:true}`）；`Resume` 重新发 Plan 后 `Run`（检查点由 FileCheckpointStore 续起——executor.Run 的语义是清检查点重跑，**注意**：executor 的 `Run` 启动时 Clear 检查点；Resume 的正确路径是 Phase 3 server 重发 Plan 后 agent 再 Run——Phase 2 的 `Resume` 先实现为「验证 op 存在 + 重发 Plan 再 Run」，文档注明 Phase 3 完善）。

**安全要求（T3 评审 carry-in，控制器裁决）**：operationID 从 WS 输入（网络侧不可信）——`Execute`/`Resume` 入口必须校验：`operationID != ""` 且 `filepath.Base(operationID) == operationID` 且不含 `..`（拒绝路径注入；防 FileCheckpointStore 写/删出目录）。补测试：`Execute` 收到 `"../evil"` operationID 返回错误。

`wsFilterToBinlog` 转换函数 + 测试。

- [ ] **Step 4: 跑测试 + 提交**

Commit: `feat(daemon): scan/execute/resume/cancel command handlers with stream events`

---

### Task 8: serve daemon 接线 + ws/agent 增量合并 + config 扩展

**Files:**
- Modify: `internal/ws/agent/`（合并 worktree 增量：dispatcher_test/renewal/client_test 差异）、`cmd/agent/serve.go`（重写 handler 注册 + 归档循环启动 + EventSink 接线）、`internal/config/config.go`（归档配置字段）、`cmd/agent/config.go`（如有）

**Interfaces:**
- Consumes: `internal/daemon.Daemon`、`internal/collector`、`wsagent.Client.Send`（push 通道）
- Produces: 可运行的 `agent serve`——启动归档循环 + 注册全部 handler

- [ ] **Step 1: 合并 ws/agent worktree 增量**

```bash
W=/d/a-shan/.claude/worktrees/pitr-v2-phase1
git diff --stat -- internal/ws/agent
# client.go 相同不动；dispatcher.go/renewal.go/client_test.go/dispatcher_test.go 差异手工合并
go test ./internal/ws/agent/ -count=1
```

- [ ] **Step 2: config 扩展**

`internal/config/config.go` 的 `Config` 增：

```go
// Archive 归档采集配置。
type ArchiveConfig struct {
    Dir           string `json:"dir"`            // 归档目录（必填）
    ServerID      uint32 `json:"server_id"`      // syncer server id（必填）
    RetentionDays int    `json:"retention_days,omitempty"` // 0 = 不清理
}
// Config 内：
Archive ArchiveConfig `json:"archive,omitempty"`
```

`cmd/agent/config.go` 的 config 命令生成示例配置时带上 archive 段；`config_test.go` 补序列化断言。

- [ ] **Step 3: 重写 serve.go**

```go
// 新 serveDaemon 持有：
//   collector loop（goroutine 启动）、daemon.Daemon、wsagent.Client

func (d *serveDaemon) startArchiveLoop(ctx context.Context) error {
    if d.cfg.Archive.Dir == "" { return fmt.Errorf("serve: config field archive.dir is required") }
    if d.cfg.Archive.ServerID == 0 { return fmt.Errorf("serve: config field archive.server_id is required") }
    conn := connector.NewMySQLConnector()
    if err := conn.Connect(d.connCfg); err != nil { return friendlyConnError(d.connCfg, err) }
    loop := collector.NewLoop(collector.Config{
        MySQL:      conn,  // connector 实现 collector.MySQLInfo（ListBinlogs/MasterPosition 已有？确认：GetBinlogFiles 已有，MasterPosition 需新增或适配）
        BinlogDir:  d.binlogDir(),   // resolveDataDir / cfg 现有逻辑
        ArchiveDir: d.cfg.Archive.Dir,
        ServerID:   d.cfg.Archive.ServerID,
        RetentionDays: d.cfg.Archive.RetentionDays,
        Logger:     d.logger(),
        SourceFactory: collector.DefaultSourceFactory, // 包装 stream.NewSource
    }, archive.NewWriter(d.cfg.Archive.Dir))
    d.loop = loop
    go func() {
        if err := loop.Run(ctx); err != nil && ctx.Err() == nil {
            d.logger().Error("archive loop exited", "err", err)
        }
    }()
    return nil
}
```

handler 注册（`NewServeCommand` 内）：

```go
dispatcher.RegisterHandler(ws.CmdStatus, d.handleStatus)
dispatcher.RegisterHandler(ws.CmdShutdown, d.handleShutdown)
dispatcher.RegisterHandler(ws.CmdPreflight, d.handlePreflight)   // 沿用 worktree 版
dispatcher.RegisterHandler(ws.CmdScan, d.handleScan)             // daemon.Scan + accepted 响应
dispatcher.RegisterHandler(ws.CmdExecute, d.handleExecute)
dispatcher.RegisterHandler(ws.CmdResume, d.handleResume)
dispatcher.RegisterHandler(ws.CmdCancel, d.handleCancel)
dispatcher.RegisterHandler(ws.CmdArchiveStatus, d.handleArchiveStatus) // collector.State()
```

`handleScan`：解析 params（json 到 ws.ScanRequest）→ `d.daemon.Scan(ctx, cmd.Cmd, req)` → `okResp(cmd, map{"accepted": true})`。EventSink 实现：`d.client.Send(ws.StreamEvent)`（client.Send 发的是 `ws.Command`——**适配**：EventSink 包装把 StreamEvent 包进 `ws.Command{Cmd: "<event>", Type: <op>, Params: ...}` 或扩展 client 的发送方法——实现选择：给 client 加 `SendStream(ev ws.StreamEvent) error`，或 EventSink 用现有 Send 包 Command；选后者避免动 client.go，Phase 3 server 侧解析时按约定解包）。

`connector` 实现 `collector.MySQLInfo`：`ListBinlogs` 已有（GetBinlogFiles 适配返回类型）；`MasterPosition` **需在 connector 新增**（`SHOW MASTER STATUS` → Position）——归入 Task 1 或本任务（本任务做，附 sqlmock 测试）。

- [ ] **Step 4: 跑测试 + 手工冒烟**

Run: `go build ./... && go test ./cmd/agent/ ./internal/ws/... ./internal/config/ -count=1`
（冒烟：`go run ./cmd/agent serve --help` 能出；归档循环的端到端验证归 Task 10）
Commit: `feat(agent): wire archive loop and scan/execute handlers into serve daemon`

---

### Task 9: flashback CLI 重接 + 删除旧解析器

**Files:**
- Replace: `cmd/agent/flashback.go`（worktree 新版 307 行）、`cmd/agent/flashback_test.go`（worktree 版）
- Delete: `internal/parser/`、`internal/rollback/` 全部文件
- Modify: 视需要清理残留引用

- [ ] **Step 1: 重接 flashback**

```bash
W=/d/a-shan/.claude/worktrees/pitr-v2-phase1
cp $W/cmd/agent/flashback.go $W/cmd/agent/flashback_test.go cmd/agent/
go test ./cmd/agent/ -run TestFlashback -count=1
```

（worktree 版用 `conn.AsDB()`——Task 1 已并入；`config.ParseDSNToConnConfig` main 已有。若编译错最小修复。）

- [ ] **Step 2: 确认旧包引用清零**

```bash
grep -rln "internal/parser\|internal/rollback" cmd internal --include="*.go" | grep -v _test || echo "no references"
```

Expected: 仅剩测试文件引用（如旧包的测试自身）→ 删除后自然消失。

- [ ] **Step 3: 删除旧包**

```bash
rm -rf internal/parser internal/rollback
go build ./... && go vet ./...
go test ./internal/... ./cmd/... -count=1
```

Expected: 全绿——**internal/parser 的 11 个既有失败随删除消失**；这是 Phase 1 期间已知的基线问题，删除即修复。

- [ ] **Step 4: 提交**

Commit: `refactor(agent): rewire flashback to new engine; remove legacy parser and rollback packages`

---

### Task 10: docker e2e 集成测试（8 场景 + 归档完整性）

**Files:**
- Copy: worktree `internal/binlog/e2e_test.go`（integration tag）+ `internal/binlog/schema_fetcher_mysql_test.go`（Task 1 若未并入则在此并入）
- Create: `internal/collector/e2e_test.go`（归档 e2e：真实 syncer + 归档完整性）、`scripts/e2e/` 辅助脚本或复用现有 Makefile

- [ ] **Step 1: 迁入 binlog e2e 并跑通**

```bash
W=/d/a-shan/.claude/worktrees/pitr-v2-phase1
cp $W/internal/binlog/e2e_test.go internal/binlog/
go vet ./internal/binlog/   # integration tag 下编译检查
# 依赖 docker MySQL 8.0（gtid_mode=ON, binlog_format=ROW）：
# 参照 worktree testdata/Makefile 起容器；E2E_MYSQL_DSN / E2E_BINLOG_DIR 环境变量
go test -tags integration ./internal/binlog/ -run TestE2E -count=1
```

8 场景（沿用 worktree）：delete/update/insert rollback、large-tx、mixed-ddl-dml、cross-binlog、cancel、gtid。

- [ ] **Step 2: 写归档 e2e（失败测试先行）**

`internal/collector/e2e_test.go`（integration tag）：docker MySQL 跑 DML → 启动真实归档循环（短超时）→ `FLUSH LOGS` → 停循环 → 断言归档目录含全部文件、`archive_state.json` 正确、再用 `binlog.Scanner` 扫归档目录能还原事务（与 MySQL 侧 `SHOW BINARY LOGS` 对比）。

- [ ] **Step 3: 实现 + 跑通 + 提交**

Commit: `test(e2e): binlog rollback scenarios and archive-loop integration against MySQL 8.0`

---

## Self-Review 结论（编写时已对照 Phase 1 ledger 交接）

- **交接覆盖**：#1 归档增量接缝 → Task 4（append 模式 + 拒覆盖）+ Task 5（回填前缀 + 续拉）；#2 stream EOF 语义 → Task 5 syncLoop 错误重试 + ctx 停止；#3 零值 SyncPos 校验 → Task 5 SourceFactory；#4 schema 缓存 → Task 2；#5 RowCount → Task 2
- **Phase 1 final-review 其余项**：#3（schema 缓存）Task 2 落实；rotating 校验已完成；`.partial` sweep → Task 5 reconcile；stale 清理 → Task 5；`archive.Consume` 返回文件名扩展 → Task 5（Phase 1 roundtrip 测试同步适配）
- **类型一致性**：`ws.StreamEvent`（Task 6）→ daemon.EventSink（Task 7）→ serve 接线（Task 8）；`executor.Plan`（Task 3）→ daemon.Execute（Task 7）；`collector.State`（Task 5）→ `daemon.ArchiveStatus`（Task 7）→ `handleArchiveStatus`（Task 8）；`scan.TxMeta` 加 json tag（Task 7 标注的接口调整）；`connector.MasterPosition` 新增（Task 8）
- **风险**：serve.go 重写面较大（Task 8），handler 逻辑已抽到 daemon（Task 7）降低风险；executor.Resume 的完整语义依赖 Phase 3 server 重发 Plan，Phase 2 先实现「重发 Plan 再 Run」路径并注明；`consume` 返回文件名是 Phase 1 API 的小破坏，涉及 roundtrip 测试同步改

## 执行交接

计划保存于 `docs/superpowers/plans/2026-08-10-pitr-v3-phase2-agent-daemon.md`。执行方式同 Phase 1：Subagent-Driven（每任务独立 subagent + 两阶段 review）或 Inline。完成后接续 Phase 3（server 平台）计划。
