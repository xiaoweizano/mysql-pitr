# PITR v3 Phase 1：采集引擎核心 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 go-mysql 重写采集引擎核心（统一 Source 事件流 + 事务聚合 + 扫描模式 + 归档写入 + 逆向 SQL 库），全部无网络依赖、可单测。

**Architecture:** 两条数据源（FileSource 本地文件 / StreamSource binlogsyncer）实现同一个 `binlog.Source` 事件流接口；事务聚合器消费事件流产出 Transaction；`internal/scan` 在其上叠加三种扫描模式（META_ONLY / WITH_SQL / SELECTED_SQL）与预览上限；`internal/archive` 把事件流 RawData 还原成原始 binlog 文件（封口验证 + 缺口检测）；`internal/reverse` 是共享纯函数库（agent 端生成 SQL，行镜像不出 agent）。

**Tech Stack:** Go 1.25、github.com/go-mysql-org/go-mysql（v1.16.0，经 goproxy.cn）、现有仓库模块 github.com/a-shan/mysql-pitr、testify。

## 本计划范围（阶段拆分）

v3 spec 覆盖四个子系统，拆成四份计划，每份独立产出可测试软件：

- **Phase 1（本计划）**：采集引擎核心 —— `internal/binlog`（Source/FileSource/聚合器/reader/gtid/schema_fetcher）、`internal/reverse`、`internal/scan`、`internal/archive`、`internal/stream` + binlog 测试 fixture。纯引擎层，不碰 daemon/网络/server。
- Phase 2：agent daemon（WS 客户端接线、归档循环、断线自愈 reconcile、executor 检查点执行）+ 重接 flashback CLI、删除旧 `internal/parser` / `internal/rollback`
- Phase 3：server 平台（SQLite 仓储、操作状态机、SSE、REST）
- Phase 4：SvelteKit Web

## Global Constraints

- Go 工具链 1.26.5（已装）；go.mod 升到 `go 1.25.0`。GOPROXY=`https://goproxy.cn,https://goproxy.io,direct`（已配置，GitHub 直连不通，必须走代理）
- go-mysql 版本：`go get github.com/go-mysql-org/go-mysql@latest` 解析为 v1.16.0；若 v1.16.0 相对 v1.13.0 的 API 破坏编译，回退锁定 v1.13.0（worktree 基线验证过的版本）并在提交信息注明原因
- **不得破坏现有构建**：旧 `internal/parser`、`internal/rollback`、`cmd/agent`、`internal/server`、`web/` 保持原样（Phase 2 才删除旧解析器）；每任务结束 `go build ./...` 必须通过
- 复用基线：`.claude/worktrees/pitr-v2-phase1/internal/{binlog,reverse}` 是已验证实现（提交 2538f04 前），Task 1 整体迁入后逐任务改造为 v3 形态
- flavor：仅 MySQL（`mysql.MySQLFlavor`），MariaDB 只在代码里留注释位
- TDD：先写失败测试 → 跑通失败 → 实现 → 跑通 → 提交。每个任务独立 commit
- 新包只依赖标准库 + go-mysql + 仓库内低层包，禁止依赖 daemon/server/网络包

---

### Task 1: 基线迁移（go.mod 升级 + 迁入 v2 引擎与 fixture）

把已验证的 v2 Phase 1 引擎整体迁入主树作为改造基线，并升级依赖。

**Files:**
- Modify: `go.mod`（go 1.25.0 + go-mysql）
- Copy: 从 `.claude/worktrees/pitr-v2-phase1/` 复制 `internal/binlog/`（除 `e2e_test.go`——它依赖 internal/executor，属 Phase 2）与 `internal/reverse/` 全部文件、`internal/binlog/testdata/`（fixture + Makefile + setup.sql + README）
- Test: `internal/binlog/*_test.go`、`internal/reverse/*_test.go`（随复制进入）

**Interfaces:**
- Produces: 包 `internal/binlog`（`Scanner` 接口、`Transaction`、`RowChange`、`Filter`、`TableSchema`、`SchemaFetcher`、`EnumerateBinlogFiles`、`ParseGTIDSet`、`MatchGTID`）、包 `internal/reverse`（`Generate`、`Statement`、`Options`）—— 全部沿用 worktree 现有签名，后续任务在其上演进

- [ ] **Step 1: 升级 go.mod 并拉依赖**

```bash
cd /d/a-shan
# 编辑 go.mod：go 1.22 → go 1.25.0
go get github.com/go-mysql-org/go-mysql@latest
go mod tidy
go list -m github.com/go-mysql-org/go-mysql   # 期望 v1.16.0
```

- [ ] **Step 2: 复制基线包**

```bash
SRC=/d/a-shan/.claude/worktrees/pitr-v2-phase1
mkdir -p internal/binlog internal/reverse
cp $SRC/internal/binlog/doc.go $SRC/internal/binlog/engine.go $SRC/internal/binlog/engine_test.go \
   $SRC/internal/binlog/gtid.go $SRC/internal/binlog/gtid_test.go \
   $SRC/internal/binlog/reader.go $SRC/internal/binlog/reader_test.go \
   $SRC/internal/binlog/schema_fetcher.go $SRC/internal/binlog/schema_fetcher_test.go \
   $SRC/internal/binlog/transaction.go $SRC/internal/binlog/transaction_test.go \
   $SRC/internal/binlog/coverage_test.go internal/binlog/
cp $SRC/internal/reverse/*.go internal/reverse/
cp -r $SRC/internal/binlog/testdata internal/binlog/testdata
```

（不复制 `schema_fetcher_mysql_test.go` 与 `e2e_test.go`：前者连真 MySQL，后者依赖 executor，均归 Phase 2。）

- [ ] **Step 3: 构建并跑基线测试**

```bash
go build ./...          # 必须全绿（旧代码不受影响）
go test ./internal/binlog/... ./internal/reverse/...
```

Expected: 全部 PASS。若 go-mysql v1.16.0 与 v1.13.0 API 差异导致编译错误，按报错最小化修复（签名以 v1.16.0 为准）；若差异过大，`go get github.com/go-mysql-org/go-mysql@v1.13.0` 回退并注明。

- [ ] **Step 4: 提交**

```bash
git add go.mod go.sum internal/binlog internal/reverse
git commit -m "feat(binlog): migrate verified v2 collector engine + reverse lib as v3 baseline"
```

---

### Task 2: `binlog.Source` 接口 + FileSource（事件流迭代器）

v3 核心抽象：文件/网络两种来源统一为事件流迭代器。

**Files:**
- Create: `internal/binlog/source.go`、`internal/binlog/source_test.go`

**Interfaces:**
- Produces:
```go
// Source 是一次事件流迭代器；io.EOF 表示流结束。
type Source interface {
    Next(ctx context.Context) (*replication.BinlogEvent, error)
    Close() error
}

// OpenFileSource 打开单个 binlog 文件（校验 magic），从 offset 处开始解析。
// offset <= 4 表示从文件头开始。goroutine 后台跑 ParseReader，Next 拉取。
func OpenFileSource(ctx context.Context, path string, offset int64, parser *replication.BinlogParser) (*FileSource, error)
func (s *FileSource) Next(ctx context.Context) (*replication.BinlogEvent, error)
func (s *FileSource) Close() error
```

- [ ] **Step 1: 写失败测试**

`internal/binlog/source_test.go`：

```go
package binlog_test

import (
    "context"
    "io"
    "path/filepath"
    "testing"

    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/stretchr/testify/require"

    "github.com/a-shan/mysql-pitr/internal/binlog"
)

func TestFileSource_ReadsAllEvents(t *testing.T) {
    path := filepath.Join("testdata", "mysql-8.0-row-full.bin")
    ctx := context.Background()
    src, err := binlog.OpenFileSource(ctx, path, 0, replication.NewBinlogParser())
    require.NoError(t, err)
    defer src.Close()

    var n int
    firstType := replication.EventType(-1)
    for {
        ev, err := src.Next(ctx)
        if err == io.EOF {
            break
        }
        require.NoError(t, err)
        if firstType == replication.EventType(-1) {
            firstType = ev.Header.EventType
        }
        n++
    }
    require.Greater(t, n, 3, "fixture 至少含 FDE + TableMap + Rows 等事件")
    require.Equal(t, replication.FORMAT_DESCRIPTION_EVENT, firstType, "第一个事件必须是 FDE")
}

func TestFileSource_StartMidFile(t *testing.T) {
    path := filepath.Join("testdata", "mysql-8.0-row-full.bin")
    ctx := context.Background()
    // 从 offset 4 开始：FDE 被重新解析后事件流应仍可消费
    src, err := binlog.OpenFileSource(ctx, path, 4, replication.NewBinlogParser())
    require.NoError(t, err)
    defer src.Close()
    ev, err := src.Next(ctx)
    require.NoError(t, err)
    require.NotNil(t, ev)
}

func TestFileSource_ContextCancel(t *testing.T) {
    path := filepath.Join("testdata", "mysql-8.0-row-full.bin")
    ctx, cancel := context.WithCancel(context.Background())
    src, err := binlog.OpenFileSource(ctx, path, 0, replication.NewBinlogParser())
    require.NoError(t, err)
    defer src.Close()
    cancel()
    _, err = src.Next(ctx)
    require.Error(t, err)
}

func TestFileSource_BadMagic(t *testing.T) {
    dir := t.TempDir()
    bad := filepath.Join(dir, "mysql-bin.000001")
    require.NoError(t, os.WriteFile(bad, []byte("not a binlog"), 0o644))
    _, err := binlog.OpenFileSource(context.Background(), bad, 0, replication.NewBinlogParser())
    require.Error(t, err)
}
```

（需 `"os"` import。）

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/binlog/ -run TestFileSource -v`
Expected: 编译失败 `undefined: binlog.OpenFileSource`

- [ ] **Step 3: 实现 source.go**

```go
package binlog

import (
    "context"
    "fmt"
    "io"
    "os"
    "sync"

    "github.com/go-mysql-org/go-mysql/replication"
)

// Source 是一次 binlog 事件流迭代器；io.EOF 表示流结束。
// 实现：FileSource（本地文件）、internal/stream（binlogsyncer）。
type Source interface {
    Next(ctx context.Context) (*replication.BinlogEvent, error)
    Close() error
}

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e} // "\xfe\x62\x69\x6e"

// FileSource 顺序解析单个 binlog 文件。goroutine 后台跑 ParseReader（内部
// 已处理 missing-table-map 跳过与校验和），Next 从 channel 拉取。
type FileSource struct {
    ctx    context.Context
    cancel context.CancelFunc
    evs    chan *replication.BinlogEvent
    errs   chan error
    done   chan struct{}
    once   sync.Once
}

// OpenFileSource 打开文件、校验 magic，并从 offset 处开始后台解析。
// offset <= 4 表示从文件头开始；offset > 4 时 FDE 会被重新解析（ParseFile 语义）。
func OpenFileSource(ctx context.Context, path string, offset int64, parser *replication.BinlogParser) (*FileSource, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("binlog: open %s: %w", path, err)
    }
    magic := make([]byte, 4)
    if _, err := io.ReadFull(f, magic); err != nil {
        f.Close()
        return nil, fmt.Errorf("binlog: read magic %s: %w", path, err)
    }
    if string(magic) != string(binlogMagic) {
        f.Close()
        return nil, fmt.Errorf("binlog: bad magic in %s", path)
    }
    if offset < 4 {
        offset = 4
    }
    if _, err := f.Seek(offset, io.SeekStart); err != nil {
        f.Close()
        return nil, fmt.Errorf("binlog: seek %s to %d: %w", path, offset, err)
    }

    ctx, cancel := context.WithCancel(ctx)
    s := &FileSource{
        ctx:    ctx,
        cancel: cancel,
        evs:    make(chan *replication.BinlogEvent, 16),
        errs:   make(chan error, 1),
        done:   make(chan struct{}),
    }
    go s.run(ctx, f, parser)
    return s, nil
}

func (s *FileSource) run(ctx context.Context, f *os.File, parser *replication.BinlogParser) {
    defer close(s.evs)
    defer close(s.errs)
    defer f.Close()

    err := parser.ParseReader(f, func(ev *replication.BinlogEvent) error {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case s.evs <- ev:
            return nil
        }
    })
    if err != nil && err != context.Canceled {
        s.errs <- err
    }
}

func (s *FileSource) Next(ctx context.Context) (*replication.BinlogEvent, error) {
    select {
    case ev, ok := <-s.evs:
        if !ok {
            if err, ok := <-s.errs; ok {
                return nil, err
            }
            return nil, io.EOF
        }
        return ev, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

func (s *FileSource) Close() error {
    s.once.Do(func() { s.cancel() })
    return nil
}
```

注意：`FileSource.Next` 的 ctx 与 `OpenFileSource` 传入的 ctx 同时生效（Next 阻塞期间取两者任一取消）。错误路径用 LIFO defer（close errs 先于 close evs），保证 evs 关闭后 errs 必已关闭——与 Scanner.Next 同样的手法。

- [ ] **Step 4: 跑测试验证通过**

Run: `go test ./internal/binlog/ -run TestFileSource -v`
Expected: 4 个测试全 PASS

- [ ] **Step 5: 提交**

```bash
git add internal/binlog/source.go internal/binlog/source_test.go
git commit -m "feat(binlog): add Source interface and FileSource event iterator"
```

---

### Task 3: 聚合器重构——scanner 消费 FileSource

把 `engine.go` 里「自己打开文件 + 读 magic + ParseReader」改为「用 FileSource」，API 不变，全部现有测试保持绿。同时给 `Filter` 增加 `SelectedTxIDs` 字段（SELECTED_SQL 模式的匹配基础）。

**Files:**
- Modify: `internal/binlog/engine.go`（parseFile 改用 FileSource；matchesFilter 增加 SelectedTxIDs 分支）、`internal/binlog/transaction.go`（Filter 加字段）
- Test: `internal/binlog/engine_test.go`、`internal/binlog/coverage_test.go`（现有）

**Interfaces:**
- Consumes: `OpenFileSource(ctx, path, offset, parser)`（Task 2）
- Produces: `Filter` 增加字段 `SelectedTxIDs []string`——`matchesFilter` 中命中集合才算通过

- [ ] **Step 1: 先加 SelectedTxIDs 过滤测试（红）**

在 `internal/binlog/engine_test.go` 追加：

```go
func TestScanner_FilterSelectedTxIDs(t *testing.T) {
    // 用 fixture：先把全部事务扫出来，取前 1 个 TxID 作为过滤集
    ctx := context.Background()
    all := scanAll(t, binlog.Filter{BinlogDir: "testdata"})
    require.NotEmpty(t, all)

    want := all[0].TxID
    got := scanAll(t, binlog.Filter{BinlogDir: "testdata", SelectedTxIDs: []string{want}})
    require.Len(t, got, 1)
    require.Equal(t, want, got[0].TxID)
}

func scanAll(t *testing.T, f binlog.Filter) []*binlog.Transaction {
    t.Helper()
    s := binlog.NewScanner(nil, binlog.WithMaxRowsPerTx(0))
    ctx := context.Background()
    require.NoError(t, s.Scan(ctx, f))
    var out []*binlog.Transaction
    for {
        tx, err := s.Next()
        if err == io.EOF {
            break
        }
        require.NoError(t, err)
        out = append(out, tx)
    }
    return out
}
```

（`scanAll` 放测试文件顶部；imports 增 `io`。fixture 文件与 `testdata` 目录同名——`EnumerateBinlogFiles("testdata")` 直接枚举。注意现有 engine_test 中若已有同名 helper 则复用。）

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/binlog/ -run TestScanner_FilterSelectedTxIDs`
Expected: FAIL（SelectedTxIDs 被忽略，返回全部事务）

- [ ] **Step 3: 实现 Filter 字段 + matchesFilter 分支**

`internal/binlog/transaction.go` 的 Filter 增加：

```go
    SelectedTxIDs []string // SELECTED_SQL 定向二次扫描：仅保留 TxID 命中的事务
```

`internal/binlog/engine.go` 的 matchesFilter 增加（放在 GTIDSet 分支后）：

```go
    if len(f.SelectedTxIDs) > 0 {
        found := false
        for _, id := range f.SelectedTxIDs {
            if id == tx.TxID {
                found = true
                break
            }
        }
        if !found {
            return false
        }
    }
```

- [ ] **Step 4: 重构 parseFile 使用 FileSource**

`engine.go` 的 `parseFile` 开头改为：

```go
func (s *scanner) parseFile(ctx context.Context, path string, f Filter) error {
    src, err := OpenFileSource(ctx, path, int64(f.StartPos.Pos), s.parser)
    if err != nil {
        return err
    }
    defer src.Close()

    var pending *pendingTx
    tableMaps := map[uint64]*replication.TableMapEvent{}

    for {
        ev, err := src.Next(ctx)
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }
        if err := s.handleEvent(ev, &pending, tableMaps, f); err != nil {
            return err
        }
    }
    return nil
}
```

把原 onEvent 函数体提取为 `handleEvent(ev *replication.BinlogEvent, pending **pendingTx, tableMaps map[uint64]*replication.TableMapEvent, f Filter) error`（逻辑原样，删掉 ctx.Done 检查——FileSource 已处理取消）。删除原 magic 读取代码（FileSource 负责）。

- [ ] **Step 5: 跑全量测试**

Run: `go build ./... && go test ./internal/binlog/...`
Expected: 全部 PASS（engine_test / coverage_test / source_test 等）

- [ ] **Step 6: 提交**

```bash
git add internal/binlog/engine.go internal/binlog/transaction.go internal/binlog/engine_test.go
git commit -m "refactor(binlog): scanner consumes FileSource; Filter gains SelectedTxIDs"
```

---

### Task 4: `internal/reverse` PK 优先 WHERE

v3 spec：WHERE 构造「有主键用主键，无主键退化全行匹配」。

**Files:**
- Modify: `internal/binlog/schema_fetcher.go`（`TableSchema` 增 `PrimaryKey []string`；`StaticSchemaFetcher` 测试数据带 PK）、`internal/reverse/generator.go`（buildWhere 用 PK）、`internal/reverse/generator_test.go`
- Test: `internal/reverse/generator_test.go`（现有 + 新增 PK 用例）

**Interfaces:**
- Produces: `binlog.TableSchema` 增加字段 `PrimaryKey []string`（列名序列，空 = 无主键）
- Consumes: `binlog.TableSchema`、`binlog.RowChange`

- [ ] **Step 1: 写失败测试（PK 优先）**

`internal/reverse/generator_test.go` 追加：

```go
func TestGenerate_UpdateUsesPrimaryKeyInWhere(t *testing.T) {
    tx := &binlog.Transaction{
        TxID:   "test-1",
        GTID:   "uuid:1",
        CommitTime: time.Now(),
        Statements: []binlog.RowChange{{
            Schema: "shop", Table: "orders",
            Action: binlog.ActionUpdate,
            Before: []interface{}{1, "old"},
            After:  []interface{}{1, "new"},
        }},
    }
    schema := map[string]binlog.TableSchema{
        "shop.orders": {
            Schema: "shop", Table: "orders",
            Columns: []binlog.ColumnDef{{Name: "id"}, {Name: "status"}},
            PrimaryKey: []string{"id"},
        },
    }
    stmts, err := reverse.Generate(tx, schema, reverse.Options{})
    require.NoError(t, err)
    require.Len(t, stmts, 1)
    // WHERE 只用主键 id，不匹配 status
    require.Contains(t, stmts[0].SQL, "WHERE `id` = 1")
    require.NotContains(t, stmts[0].SQL, "`status` IS NULL")
}
```

（需 imports：`time`、`binlog`、`reverse`——按现有测试文件风格。）

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/reverse/ -run TestGenerate_UpdateUsesPrimaryKeyInWhere`
Expected: FAIL（当前 buildWhere 用全部列）

- [ ] **Step 3: 实现**

`internal/binlog/schema_fetcher.go`：

```go
type TableSchema struct {
    Schema     string
    Table      string
    Columns    []ColumnDef
    PrimaryKey []string // 主键列名序列；空表示无主键
}
```

`internal/reverse/generator.go`：`buildWhere` 改签名，优先 PK：

```go
func buildWhere(rc binlog.RowChange, cols []string, pk []string) string {
    if len(pk) > 0 {
        // 仅用主键列做定位；主键值从 After image（行镜像按列序对齐）取值
        names := make([]string, 0, len(pk))
        for _, p := range pk {
            if i := indexOf(cols, p); i >= 0 {
                names = append(names, i)
            }
        }
        ...
    }
    ...
}
```

具体实现建议（保持现有风格）：把 `buildWhere(cols, values)` 泛化为 `buildWhereCols(colNames, values)`，PK 存在时传 `pkCols := resolvePKIndices(cols, pk)` 映射到 image 下标；无 PK 时传全部列。`indexOf` 工具函数放 generator.go。DELETE/UPDATE 都走同一 `buildWhere` 入口（两处调用点改为传 PK）。无主键时行为与现状一致（全列匹配）。

- [ ] **Step 4: 跑测试验证通过**

Run: `go test ./internal/reverse/...`
Expected: 全部 PASS（现有用例不受影响——无 PK 时行为不变）

- [ ] **Step 5: 提交**

```bash
git add internal/binlog/schema_fetcher.go internal/reverse/generator.go internal/reverse/generator_test.go
git commit -m "feat(reverse): WHERE prefers primary key columns, falls back to full-row match"
```

---

### Task 5: `internal/reverse` 值格式化增强（时间/十进制/二进制）

go-mysql 解析出的行镜像类型取决于 parser 选项。聚合器启用 `SetParseTime(true)` + `SetUseDecimal(true)` 后，`formatValue` 必须正确处理 `time.Time` 与 `decimal.Decimal`。

**Files:**
- Modify: `internal/binlog/engine.go`（NewScanner 处启用 parser 选项）、`internal/reverse/generator.go`（formatValue 增强）、`internal/reverse/generator_test.go`
- Test: `internal/reverse/generator_test.go`

**Interfaces:**
- Consumes: go-mysql `SetParseTime(true)`（TIMESTAMP/DATETIME → `time.Time`）、`SetUseDecimal(true)`（DECIMAL → `decimal.Decimal`）
- Produces: `formatValue(v interface{}) string` 支持：nil、整型、浮点、`time.Time`、`decimal.Decimal`、`[]byte`（hex）、bool、string

- [ ] **Step 1: 写失败测试**

```go
func TestFormatValue_TimeAndDecimal(t *testing.T) {
    loc := time.UTC
    ts := time.Date(2026, 8, 10, 12, 30, 45, 0, loc)
    // 通过 Generate 验证 time.Time / decimal.Decimal 被正确渲染
    tx := &binlog.Transaction{
        TxID: "test-2", CommitTime: ts,
        Statements: []binlog.RowChange{{
            Schema: "shop", Table: "orders",
            Action: binlog.ActionInsert,
            After: []interface{}{
                uint64(1), decimal.NewFromFloat(19.99), ts, []byte{0x01, 0xff},
            },
        }},
    }
    schema := map[string]binlog.TableSchema{
        "shop.orders": {
            Schema: "shop", Table: "orders",
            Columns: []binlog.ColumnDef{
                {Name: "id"}, {Name: "amount"}, {Name: "created_at"}, {Name: "blob_col"},
            },
        },
    }
    stmts, err := reverse.Generate(tx, schema, reverse.Options{})
    require.NoError(t, err)
    require.Len(t, stmts, 1)
    require.Contains(t, stmts[0].SQL, "'2026-08-10 12:30:45'")
    require.Contains(t, stmts[0].SQL, "19.99")
    require.Contains(t, stmts[0].SQL, "X'01ff'")
}
```

（imports：`time`、`github.com/shopspring/decimal`——go-mysql 已间接依赖，需 `go get github.com/shopspring/decimal` 提为直接依赖。）

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/reverse/ -run TestFormatValue_TimeAndDecimal`
Expected: FAIL（`%v` 渲染 `time.Time` 为 `'2026-08-10 12:30:45 +0000 UTC'`，不含期望子串）

- [ ] **Step 3: 实现 formatValue 增强**

`internal/reverse/generator.go`：

```go
import (
    "time"
    "github.com/shopspring/decimal"
)

func formatValue(v interface{}) string {
    switch x := v.(type) {
    case nil:
        return "NULL"
    case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
        return fmt.Sprintf("%d", x)
    case float32:
        return strconv.FormatFloat(float64(x), 'f', -1, 32)
    case float64:
        return strconv.FormatFloat(x, 'f', -1, 64)
    case decimal.Decimal:
        return x.String()
    case time.Time:
        return "'" + x.UTC().Format("2006-01-02 15:04:05") + "'"
    case []byte:
        // 用 X'hex' 而非 _binary 'hex'：后者是字符字面量，会把 "6162" 当字符串，
        // 无法还原 0x61 0x62 原始字节（评审发现，baseline 带入）
        return fmt.Sprintf("X'%x'", x)
    case string:
        // 反斜杠也要转义：MySQL 默认 NO_BACKSLASH_ESCAPES=off，\ 是转义符
        escaped := strings.ReplaceAll(x, "\\", "\\\\")
        escaped = strings.ReplaceAll(escaped, "'", "''")
        return "'" + escaped + "'"
    case bool:
        if x {
            return "1"
        }
        return "0"
    default:
        return fmt.Sprintf("'%v'", x)
    }
}
```

（float 用 `strconv.FormatFloat` 避免 `%v` 的指数/精度噪音；需 import `strconv`。时间统一 UTC 输出。）

- [ ] **Step 4: 启用 parser 选项**

`internal/binlog/engine.go` 的 `Scan` 中，`s.parser = replication.NewBinlogParser()` 后追加：

```go
    s.parser.SetVerifyChecksum(true)
    s.parser.SetParseTime(true)   // TIMESTAMP/DATETIME → time.Time
    s.parser.SetUseDecimal(true)  // DECIMAL → decimal.Decimal
```

跑 `go test ./internal/binlog/...` 确认现有测试仍绿（fixture 断言若依赖字符串渲染需同步调整——coverage_test 手工构造的事件不涉及时间/十进制断言，预期无影响）。

- [ ] **Step 5: 跑测试验证通过**

Run: `go build ./... && go test ./internal/binlog/... ./internal/reverse/...`
Expected: 全部 PASS

- [ ] **Step 6: 提交**

```bash
git add go.mod go.sum internal/binlog/engine.go internal/reverse/generator.go internal/reverse/generator_test.go
git commit -m "feat(reverse): format time.Time / decimal.Decimal / binary values faithfully"
```

---

### Task 6: `internal/scan`——扫描模式（META_ONLY / WITH_SQL / SELECTED_SQL）

v3 核心消费层：在 binlog.Scanner 之上叠加模式、预览上限与 SQL 生成。

**Files:**
- Create: `internal/scan/scan.go`、`internal/scan/scan_test.go`

**Interfaces:**
- Consumes: `binlog.Scanner`、`binlog.Filter`（含 `SelectedTxIDs`）、`binlog.SchemaFetcher`、`binlog.TableSchema`、`reverse.Generate`、`reverse.Statement`
- Produces:
```go
package scan

type Mode int

const (
    ModeMetaOnly Mode = iota // 仅事务元数据（轻量）
    ModeWithSQL              // 元数据 + 逆向 SQL（边扫边生成）
    ModeSelectedSQL          // 定向二次扫描：Filter.SelectedTxIDs 命中才生成 SQL
)

type TxMeta struct {
    TxID       string
    GTID       string
    XID        uint64
    CommitTime time.Time
    Schema     string
    Tables     []binlog.TableRef
    RowCount   int
    Truncated  bool
}

type Result struct {
    Meta TxMeta
    SQL  []reverse.Statement // 非 META_ONLY 时填充；SQL=="" 的行表示 warning-only
}

type Config struct {
    ArchiveDir    string
    Filter        binlog.Filter
    Mode          Mode
    MaxPreview    int // 达到即停；默认 500
    SchemaFetcher binlog.SchemaFetcher
    MaxRowsPerTx  int // 0 = 默认 1_000_000
    Logger        *slog.Logger
}

// Stream 跑一次扫描，产出 Result 流与终止错误。调用方必须消费到 channel 关闭。
func Stream(ctx context.Context, cfg Config) (<-chan Result, <-chan error)
```

- [ ] **Step 1: 写失败测试**

`internal/scan/scan_test.go`（fixture 驱动，StaticSchemaFetcher 提供 shop.orders 结构）：

```go
package scan_test

import (
    "context"
    "io"
    "testing"
    "time"

    "github.com/stretchr/testify/require"

    "github.com/a-shan/mysql-pitr/internal/binlog"
    "github.com/a-shan/mysql-pitr/internal/scan"
)

var fixtureSchema = binlog.StaticSchemaFetcher{
    "shop.orders": {
        Schema: "shop", Table: "orders",
        Columns: []binlog.ColumnDef{
            {Name: "id", IsAutoInc: true},
            {Name: "user_id"},
            {Name: "amount"},
            {Name: "status"},
            {Name: "created_at"},
        },
        PrimaryKey: []string{"id"},
    },
}

// fixtureDir 把 testdata 的 fixture 拷贝到临时目录并重命名为 mysql-bin.000001。
// 必须重命名：EnumerateBinlogFiles 只认数字后缀（isBinlogFile 规则），
// 而提交的 fixture 文件名为 mysql-8.0-row-full.bin。
func fixtureDir(t *testing.T) string {
    t.Helper()
    src, err := os.ReadFile(filepath.Join("..", "binlog", "testdata", "mysql-8.0-row-full.bin"))
    require.NoError(t, err)
    dir := t.TempDir()
    require.NoError(t, os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), src, 0o644))
    return dir
}

func collect(t *testing.T, cfg scan.Config) ([]scan.Result, error) {
    t.Helper()
    ctx := context.Background()
    ch, errCh := scan.Stream(ctx, cfg)
    var out []scan.Result
    for r := range ch {
        out = append(out, r)
    }
    return out, <-errCh
}

func TestStream_ModeMetaOnly(t *testing.T) {
    out, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{},
        Mode:       scan.ModeMetaOnly,
        SchemaFetcher: fixtureSchema,
    })
    require.NoError(t, err)
    require.NotEmpty(t, out)
    for _, r := range out {
        require.Empty(t, r.SQL, "META_ONLY 不回传 SQL")
        require.NotEmpty(t, r.Meta.TxID)
        require.Greater(t, r.Meta.RowCount, 0)
    }
}

func TestStream_ModeWithSQL_ProducesReverseStatements(t *testing.T) {
    out, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{},
        Mode:       scan.ModeWithSQL,
        SchemaFetcher: fixtureSchema,
    })
    require.NoError(t, err)
    require.NotEmpty(t, out)
    // fixture 的 setup.sql：INSERT 2 行、UPDATE 1 行、DELETE 1 行（分属若干事务）
    total := 0
    for _, r := range out {
        for _, s := range r.SQL {
            if s.SQL != "" {
                total++
            }
        }
    }
    require.GreaterOrEqual(t, total, 4)
}

func TestStream_ModeSelectedSQL_OnlySelected(t *testing.T) {
    all, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{},
        Mode:       scan.ModeMetaOnly,
        SchemaFetcher: fixtureSchema,
    })
    require.NoError(t, err)
    require.NotEmpty(t, all)

    sel := all[0].Meta.TxID
    out, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{SelectedTxIDs: []string{sel}},
        Mode:       scan.ModeSelectedSQL,
        SchemaFetcher: fixtureSchema,
    })
    require.NoError(t, err)
    require.Len(t, out, 1)
    require.Equal(t, sel, out[0].Meta.TxID)
    require.NotEmpty(t, out[0].SQL)
}

func TestStream_MaxPreviewCap(t *testing.T) {
    out, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{},
        Mode:       scan.ModeMetaOnly,
        SchemaFetcher: fixtureSchema,
        MaxPreview: 1,
    })
    require.NoError(t, err)
    require.Len(t, out, 1, "达到 MaxPreview 即停")
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/scan/ -v`
Expected: 编译失败 `undefined: scan.Stream`

- [ ] **Step 3: 实现 scan.go**

```go
package scan

import (
    "context"
    "io"
    "log/slog"

    "github.com/a-shan/mysql-pitr/internal/binlog"
    "github.com/a-shan/mysql-pitr/internal/reverse"
)

type Mode int

const (
    ModeMetaOnly Mode = iota
    ModeWithSQL
    ModeSelectedSQL
)

type TxMeta struct {
    TxID       string
    GTID       string
    XID        uint64
    CommitTime time.Time
    Schema     string
    Tables     []binlog.TableRef
    RowCount   int
    Truncated  bool
}

type Result struct {
    Meta TxMeta
    SQL  []reverse.Statement
}

type Config struct {
    ArchiveDir    string
    Filter        binlog.Filter
    Mode          Mode
    MaxPreview    int
    SchemaFetcher binlog.SchemaFetcher
    MaxRowsPerTx  int
    Logger        *slog.Logger
}

func Stream(ctx context.Context, cfg Config) (<-chan Result, <-chan error) {
    out := make(chan Result, 16)
    errCh := make(chan error, 1)
    go func() {
        defer close(out)
        defer close(errCh)
        if cfg.MaxPreview <= 0 {
            cfg.MaxPreview = 500
        }
        if cfg.Logger == nil {
            cfg.Logger = slog.Default()
        }
        f := cfg.Filter
        f.BinlogDir = cfg.ArchiveDir
        if cfg.MaxRowsPerTx > 0 {
            f.MaxRowsPerTx = cfg.MaxRowsPerTx
        }

        s := binlog.NewScanner(cfg.SchemaFetcher, binlog.WithLogger(cfg.Logger))
        if err := s.Scan(ctx, f); err != nil {
            errCh <- err
            return
        }
        defer s.Close()

        sent := 0 // 已发送结果数（不能看 len(out)：channel 有缓冲，消费者可能滞后）
        for {
            tx, err := s.Next()
            if err == io.EOF {
                return
            }
            if err != nil {
                errCh <- err
                return
            }
            meta := TxMeta{
                TxID:       tx.TxID,
                GTID:       tx.GTID,
                XID:        tx.XID,
                CommitTime: tx.CommitTime,
                Schema:     tx.Schema,
                RowCount:   tx.RowCount(),
                Truncated:  tx.Truncated,
            }
            for _, rc := range tx.Statements {
                ref := binlog.TableRef{Schema: rc.Schema, Table: rc.Table}
                if !containsRef(meta.Tables, ref) {
                    meta.Tables = append(meta.Tables, ref)
                }
            }
            r := Result{Meta: meta}
            if cfg.Mode != ModeMetaOnly {
                r.SQL = generateSQL(cfg.SchemaFetcher, tx)
            }
            select {
            case out <- r:
                sent++
            case <-ctx.Done():
                errCh <- ctx.Err()
                return
            }
            if sent >= cfg.MaxPreview {
                return
            }
        }
    }()
    return out, errCh
}
```

（注：`MaxPreview` 判定不能用 `len(out)`——channel 有缓冲。用 goroutine 内计数变量 `sent int`，每发一个 `sent++`，`sent >= cfg.MaxPreview` 即 return。上述代码需修正为计数方式。`generateSQL` 对 `Truncated` 事务返回空 SQL + warning 行；`containsRef` 为小工具。）

`generateSQL`：

```go
func generateSQL(sf binlog.SchemaFetcher, tx *binlog.Transaction) []reverse.Statement {
    if tx.Truncated {
        return []reverse.Statement{{
            SQL: "", TxID: tx.TxID,
            Warnings: []string{"transaction truncated, cannot generate full reverse SQL"},
        }}
    }
    ctx := context.Background()
    schema := map[string]binlog.TableSchema{}
    for _, rc := range tx.Statements {
        key := rc.Schema + "." + rc.Table
        if _, ok := schema[key]; ok {
            continue
        }
        sch, err := sf.FetchSchema(ctx, rc.Schema, rc.Table)
        if err != nil {
            continue // reverse.Generate 对缺表输出 warning
        }
        schema[key] = sch
    }
    stmts, err := reverse.Generate(tx, schema, reverse.Options{})
    if err != nil {
        return []reverse.Statement{{
            SQL: "", TxID: tx.TxID,
            Warnings: []string{"reverse generate failed: " + err.Error()},
        }}
    }
    return stmts
}
```

- [ ] **Step 4: 跑测试验证通过**

Run: `go build ./... && go test ./internal/scan/ -v`
Expected: 4 个测试全 PASS

- [ ] **Step 5: 提交**

```bash
git add internal/scan/scan.go internal/scan/scan_test.go
git commit -m "feat(scan): scan modes (META_ONLY / WITH_SQL / SELECTED_SQL) with preview cap"
```

---

### Task 7: `internal/binlogtest` 共享测试工具 + `internal/archive` 归档写入器

把 coverage_test 的手工造事件 helpers 提取为共享包，归档写入器在它之上做「还原 binlog 文件」的 TDD。

**Files:**
- Create: `internal/binlogtest/craft.go`、`internal/binlog/archive.go`、`internal/binlog/archive_test.go`、`internal/archive/archive.go`、`internal/archive/archive_test.go`
- Modify: `internal/binlog/coverage_test.go`（改用 binlogtest 的 craft helpers，删本地副本）

**Interfaces:**
- Consumes: `binlog.Source`（Task 2）、`replication.BinlogParser.ParseFile`（封口验证）
- Produces:
```go
package archive

type ManifestFile struct{ Name string; Size int64 }
type Manifest interface { List(ctx context.Context) ([]ManifestFile, error) }

type Writer struct{ /* dir, verify parser */ }
func NewWriter(dir string) *Writer
// Consume 把事件流写入 dir 下当前 binlog 文件（.partial）；RotateEvent 时封口。
func (w *Writer) Consume(ctx context.Context, src binlog.Source) error
// Seal 验证当前 .partial 可完整解析（ParseFile + SetVerifyChecksum）后改名为正式文件。
func (w *Writer) Seal(partialName string) error
// Gaps 对比 manifest 找出归档缺失的文件。
func (w *Writer) Gaps(ctx context.Context, m Manifest) ([]string, error)
```

`internal/binlogtest` 导出：`CraftFDE`, `CraftTableMap`, `CraftWriteRows`, `CraftUpdateRows`, `CraftDeleteRows`, `CraftXID`, `CraftGTID`, `CraftRotate`, `CraftQuery`, `CraftFile(events ...[]byte) []byte`（拼接 magic + 各事件）。签名沿用 coverage_test 现有 helpers（从 `internal/binlog/coverage_test.go` 原样迁移，导出化）。

- [ ] **Step 1: 迁移 craft helpers 到 binlogtest**

把 `internal/binlog/coverage_test.go` 的 `craft*` 函数（`craftEvent`、FDE/TableMap/Rows/XID/GTID/Rotate/Query 构造）复制到 `internal/binlogtest/craft.go`，首字母大写导出，包内注释注明「从 binlog 包测试迁出，供 binlog/archive/stream 测试共用」。coverage_test.go 删除本地副本改为 `binlogtest.CraftXXX` 调用。跑 `go test ./internal/binlog/` 确认仍全绿。

（此步骤无新测试——是纯重构；验证方式为现有测试全绿。）

- [ ] **Step 2: 写失败测试（archive 还原 + 封口验证）**

`internal/archive/archive_test.go`：

```go
package archive_test

import (
    "context"
    "io"
    "os"
    "path/filepath"
    "testing"

    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/stretchr/testify/require"

    "github.com/a-shan/mysql-pitr/internal/archive"
    "github.com/a-shan/mysql-pitr/internal/binlog"
    "github.com/a-shan/mysql-pitr/internal/binlogtest"
)

// sliceSource 从 binlogtest.Event 切片构造一个 binlog.Source。
type sliceSource struct {
    evs []binlogtest.Event
    cur int
}

func (s *sliceSource) Next(ctx context.Context) (*replication.BinlogEvent, error) {
    if s.cur >= len(s.evs) {
        return nil, io.EOF
    }
    e := s.evs[s.cur]
    s.cur++
    return &replication.BinlogEvent{RawData: e.Raw, Header: &replication.EventHeader{EventType: e.Type, Timestamp: 1754294400}}, nil
}
func (s *sliceSource) Close() error { return nil }

type stubManifest []archive.ManifestFile

func (m stubManifest) List(ctx context.Context) ([]archive.ManifestFile, error) { return m, nil }
```

测试主体：

```go
func TestWriter_ConsumeRoundTrip(t *testing.T) {
    dir := t.TempDir()
    w := archive.NewWriter(dir)

    evs := []binlogtest.Event{
        binlogtest.MustCraft(binlogtest.CraftFDE()),
        binlogtest.MustCraft(binlogtest.CraftGTID("uuid", 1)),
        binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN")),
        binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
        binlogtest.MustCraft(binlogtest.CraftWriteRows(1, 2)),
        binlogtest.MustCraft(binlogtest.CraftXID(100)),
    }
    src := &sliceSource{evs: evs}
    require.NoError(t, w.Consume(context.Background(), src))
    require.NoError(t, w.Seal("mysql-bin.000001.partial"))

    // 还原出的文件字节 == craft 拼接
    got, err := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
    require.NoError(t, err)
    want := binlogtest.CraftFile(evs)
    require.Equal(t, want, got)
}

func TestWriter_SealCorruptedFails(t *testing.T) {
    dir := t.TempDir()
    w := archive.NewWriter(dir)
    evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
    require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
    // 篡改一个字节破坏校验和
    p := filepath.Join(dir, "mysql-bin.000001.partial")
    b, _ := os.ReadFile(p)
    b[20] ^= 0xff
    os.WriteFile(p, b, 0o644)
    require.Error(t, w.Seal("mysql-bin.000001.partial"))
}

func TestWriter_Gaps(t *testing.T) {
    dir := t.TempDir()
    w := archive.NewWriter(dir)
    os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), []byte("x"), 0o644)
    gaps, err := w.Gaps(context.Background(), stubManifest{
        files: []archive.ManifestFile{{Name: "mysql-bin.000001", Size: 1}, {Name: "mysql-bin.000002", Size: 5}},
    })
    require.NoError(t, err)
    require.Equal(t, []string{"mysql-bin.000002"}, gaps)
}

func TestWriter_ConsumeRotateStartsNewFile(t *testing.T) {
    dir := t.TempDir()
    w := archive.NewWriter(dir)

    evs := []binlogtest.Event{
        binlogtest.MustCraft(binlogtest.CraftFDE()),
        binlogtest.MustCraft(binlogtest.CraftXID(1)),
        binlogtest.MustCraft(binlogtest.CraftRotate("mysql-bin.000002")),
        binlogtest.MustCraft(binlogtest.CraftXID(2)),
    }
    require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
    require.NoError(t, w.Seal("mysql-bin.000001.partial"))
    require.NoError(t, w.Seal("mysql-bin.000002.partial"))

    for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002"} {
        b, err := os.ReadFile(filepath.Join(dir, name))
        require.NoError(t, err)
        require.Equal(t, []byte{0xfe, 0x62, 0x69, 0x6e}, b[:4], "每个归档文件必须以 binlog magic 开头")
    }
}

type stubManifest []archive.ManifestFile
func (m stubManifest) List(ctx context.Context) ([]archive.ManifestFile, error) { return m, nil }
```

（imports：`bytes` 不需要——fakeSource 方案已简化；需 `io`。）

- [ ] **Step 3: 跑测试验证失败**

Run: `go test ./internal/archive/ -v`
Expected: 编译失败（archive 包不存在）

- [ ] **Step 4: 实现 archive.go**

```go
package archive

import (
    "context"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "strings"

    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/go-mysql-org/go-mysql/replication"

    "github.com/a-shan/mysql-pitr/internal/binlog"
)

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e}

type ManifestFile struct {
    Name string
    Size int64
}

// Manifest 列出 MySQL 当前持有的 binlog 文件（SHOW BINARY LOGS 的抽象）。
type Manifest interface {
    List(ctx context.Context) ([]ManifestFile, error)
}

type Writer struct {
    dir string
}

func NewWriter(dir string) *Writer { return &Writer{dir: dir} }

// Consume 把事件流写入 dir 下以文件名命名的 .partial 文件。
// 第一个事件前写 magic；RotateEvent 触发文件切换（新文件等下一个事件再开）。
func (w *Writer) Consume(ctx context.Context, src binlog.Source) error {
    var f *os.File
    var current string // 不含 .partial
    defer func() {
        if f != nil {
            f.Close()
        }
    }()
    for {
        ev, err := src.Next(ctx)
        if err != nil {
            if err.Error() == "EOF" { // binlog.Source 用 io.EOF
                return nil
            }
            return err
        }
        if ev.Header == nil {
            continue
        }
        if ev.Header.EventType == replication.ROTATE_EVENT {
            // 轮转：当前文件收尾，后续事件属于新文件
            if f != nil {
                f.Close()
                f = nil
            }
            continue
        }
        if f == nil {
            // 新文件：从事件头拿文件名不现实（archive 由调用方给定目录+文件名序列），
            // 因此由 Consume 的调用方通过命名约定驱动：默认文件名 mysql-bin.000001。
            current = "mysql-bin.000001"
            name := filepath.Join(w.dir, current+".partial")
            f, err = os.Create(name)
            if err != nil {
                return err
            }
            if _, err := f.Write(binlogMagic); err != nil {
                return err
            }
        }
        if _, err := f.Write(ev.RawData); err != nil {
            return err
        }
    }
}
```

**实现说明（最终决策）**：归档文件名由 ROTATE_EVENT 驱动——首个事件前默认 `mysql-bin.000001`；收到 `*replication.RotateEvent` 时取其 `NextLogName` 作为下一个文件名（关闭当前文件、为新文件写 magic）。Phase 2 的归档循环只在 Consume 之外负责「初始回填 + 缺口补齐」，命名规则本任务定死。

`Seal`：

```go
// Seal 用 ParseFile + SetVerifyChecksum(true) 验证 .partial，通过则去掉后缀。
// 验证失败返回错误，调用方回退整文件拷贝。
func (w *Writer) Seal(partialName string) error {
    src := filepath.Join(w.dir, partialName)
    parser := replication.NewBinlogParser()
    parser.SetVerifyChecksum(true)
    if err := parser.ParseFile(src, 0, func(*replication.BinlogEvent) error { return nil }); err != nil {
        return fmt.Errorf("archive: seal verify %s: %w", partialName, err)
    }
    final := strings.TrimSuffix(src, ".partial")
    if err := os.Rename(src, final); err != nil {
        return fmt.Errorf("archive: rename %s: %w", partialName, err)
    }
    return nil
}
```

`Gaps`：

```go
func (w *Writer) Gaps(ctx context.Context, m Manifest) ([]string, error) {
    files, err := m.List(ctx)
    if err != nil {
        return nil, err
    }
    var missing []string
    for _, mf := range files {
        final := filepath.Join(w.dir, mf.Name)
        if _, err := os.Stat(final); os.IsNotExist(err) {
            missing = append(missing, mf.Name)
        }
    }
    sort.Strings(missing)
    return missing, nil
}
```

（`Gaps` 只报完全缺失的文件；`Size` 不匹配属「部分归档」场景，Phase 2 的 reconcile 处理。imports 增加 `sort`、`strings`；`mysql` 若未用则删。）

- [ ] **Step 5: 跑测试验证通过**

Run: `go build ./... && go test ./internal/archive/ ./internal/binlog/`
Expected: 全部 PASS

- [ ] **Step 6: 提交**

```bash
git add internal/binlogtest internal/archive internal/binlog/coverage_test.go
git commit -m "feat(archive): binlog file reconstruction with verify-on-seal and gap detection"
```

---

### Task 8: `internal/stream`——StreamSource（binlogsyncer 封装）

增量侧的 Source 实现：包装 go-mysql binlogsyncer（解析模式，RawData 仍可用），支持从 Position 或 GTID 集启动。

**Files:**
- Create: `internal/stream/source.go`、`internal/stream/source_test.go`

**Interfaces:**
- Consumes: `binlog.Source`（Task 2 接口）、go-mysql `replication.BinlogSyncer` / `BinlogStreamer`
- Produces:
```go
package stream

type Config struct {
    Host     string
    Port     int
    User     string
    Password string
    ServerID uint32
    Flavor   string // 默认 mysql
    SyncPos  mysql.Position // StartSync 起点（SyncGTID 为空时）
    SyncGTID mysql.GTIDSet  // 优先于 SyncPos
}

// NewSource 创建 binlogsyncer 驱动的 Source。
func NewSource(cfg Config) (binlog.Source, error)

// NewSourceWithStreamer 注入 fake streamer（测试用）。
func NewSourceWithStreamer(st streamer) binlog.Source
```

- [ ] **Step 1: 写失败测试（fake streamer）**

`internal/stream/source_test.go`：

```go
package stream_test

import (
    "context"
    "io"
    "testing"

    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/stretchr/testify/require"

    "github.com/a-shan/mysql-pitr/internal/binlogtest"
    "github.com/a-shan/mysql-pitr/internal/stream"
)

type fakeStreamer struct {
    evs []binlogtest.Event
    cur int
    err error
}

func (f *fakeStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
    if f.err != nil {
        return nil, f.err
    }
    if f.cur >= len(f.evs) {
        return nil, io.EOF
    }
    e := f.evs[f.cur]
    f.cur++
    return &replication.BinlogEvent{
        RawData: e.Raw,
        Header:  &replication.EventHeader{EventType: e.Type, Timestamp: 1754294400},
    }, nil
}
func (f *fakeStreamer) Close() error { return nil }

func TestStreamSource_YieldsEvents(t *testing.T) {
    evs := []binlogtest.Event{
        binlogtest.MustCraft(binlogtest.CraftFDE()),
        binlogtest.MustCraft(binlogtest.CraftXID(1)),
    }
    src := stream.NewSourceWithStreamer(&fakeStreamer{evs: evs})
    defer src.Close()

    ev, err := src.Next(context.Background())
    require.NoError(t, err)
    require.Equal(t, replication.FORMAT_DESCRIPTION_EVENT, ev.Header.EventType)
    ev, err = src.Next(context.Background())
    require.NoError(t, err)
    require.Equal(t, replication.XID_EVENT, ev.Header.EventType)
    _, err = src.Next(context.Background())
    require.Equal(t, io.EOF, err)
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/stream/ -v`
Expected: 编译失败（stream 包不存在）

- [ ] **Step 3: 实现 source.go**

```go
package stream

import (
    "context"
    "fmt"
    "io"

    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/go-mysql-org/go-mysql/replication"
)

// streamer 抽象 binlogsyncer 的事件拉取，便于测试注入 fake。
type streamer interface {
    GetEvent(ctx context.Context) (*replication.BinlogEvent, error)
    Close() error
}

type Config struct {
    Host     string
    Port     int
    User     string
    Password string
    ServerID uint32
    Flavor   string
    SyncPos  mysql.Position
    SyncGTID mysql.GTIDSet
}

// binlogStreamer 适配 replication.BinlogStreamer。
type binlogStreamer struct{ s *replication.BinlogStreamer }

func (b *binlogStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
    return b.s.GetEvent(ctx)
}
func (b *binlogStreamer) Close() error { return nil }

type source struct {
    st streamer
}

func (s *source) Next(ctx context.Context) (*replication.BinlogEvent, error) {
    return s.st.GetEvent(ctx)
}
func (s *source) Close() error { return s.st.Close() }

// NewSource 创建真实 binlogsyncer 驱动的事件源。
func NewSource(cfg Config) (binlog.Source, error) {
    flavor := cfg.Flavor
    if flavor == "" {
        flavor = mysql.MySQLFlavor
    }
    sync := replication.BinlogSyncerConfig{
        ServerID:   cfg.ServerID,
        Flavor:     flavor,
        Host:       cfg.Host,
        Port:       uint16(cfg.Port),
        User:       cfg.User,
        Password:   cfg.Password,
        RawModeEnabled: false, // 解析模式；BinlogEvent.RawData 始终可用，归档照常还原
    }
    syncer := replication.NewBinlogSyncer(sync)
    var st *replication.BinlogStreamer
    var err error
    if cfg.SyncGTID != nil && !cfg.SyncGTID.IsEmpty() {
        st, err = syncer.StartSyncGTID(cfg.SyncGTID)
    } else {
        st, err = syncer.StartSync(cfg.SyncPos)
    }
    if err != nil {
        return nil, fmt.Errorf("stream: start sync: %w", err)
    }
    return &source{st: &binlogStreamer{s: st}}, nil
}

func NewSourceWithStreamer(st streamer) binlog.Source {
    return &source{st: st}
}
```

- [ ] **Step 4: 跑测试验证通过**

Run: `go build ./... && go test ./internal/stream/ -v`
Expected: 全 PASS

- [ ] **Step 5: 提交**

```bash
git add internal/stream/source.go internal/stream/source_test.go
git commit -m "feat(stream): binlogsyncer-backed Source (position or GTID start)"
```

---

### Task 9: fixture 黄金路径 + 归档字节级往返验证

把「真实 fixture 上的全链路断言」与「归档还原 = 原文件字节一致」固化下来。

**Files:**
- Create: `internal/scan/golden_test.go`、`internal/archive/roundtrip_test.go`

**Interfaces:**
- Consumes: `scan.Stream`（Task 6）、`archive.Writer` + `binlog.FileSource`（Task 2/7）、fixture `internal/binlog/testdata/mysql-8.0-row-full.bin`

- [ ] **Step 1: 写 fixture 黄金路径测试**

`internal/scan/golden_test.go`：

```go
package scan_test

import (
    "testing"

    "github.com/stretchr/testify/require"

    "github.com/a-shan/mysql-pitr/internal/binlog"
    "github.com/a-shan/mysql-pitr/internal/scan"
)

// fixture 由 testdata/setup.sql 生成：INSERT 2 行 → UPDATE id=1 → DELETE id=2，
// 每次操作一个事务（setup.sql 无显式事务，autocommit 各自成事务）。
// 此处断言 WITH_SQL 模式产出的逆向 SQL 精确覆盖三种操作类型。
func TestGolden_SetupSQLReverseStatements(t *testing.T) {
    out, err := collect(t, scan.Config{
        ArchiveDir: fixtureDir(t),
        Filter:     binlog.Filter{Tables: []binlog.TableRef{{Schema: "shop", Table: "orders"}}},
        Mode:       scan.ModeWithSQL,
        SchemaFetcher: fixtureSchema,
    })
    require.NoError(t, err)
    require.NotEmpty(t, out)

    var deletes, updates, inserts int // 逆向语句类型计数
    for _, r := range out {
        for _, s := range r.SQL {
            switch {
            case s.SQL == "":
                continue // warning-only
            case len(s.SQL) > 12 && s.SQL[:6] == "DELETE":
                deletes++
            case len(s.SQL) > 6 && s.SQL[:6] == "UPDATE":
                updates++
            case len(s.SQL) > 6 && s.SQL[:6] == "INSERT":
                inserts++
            }
        }
    }
    // setup.sql：2 个 INSERT + 1 个 UPDATE + 1 个 DELETE
    require.Equal(t, 1, deletes, "逆向 DELETE（还原被误删行）")
    require.Equal(t, 1, updates, "逆向 UPDATE")
    require.Equal(t, 2, inserts, "逆向 INSERT")
}
```

（表过滤 `Tables` 限定 shop.orders，隔离 fixture 内可能存在的其他库表。）

- [ ] **Step 2: 写归档字节级往返测试**

`internal/archive/roundtrip_test.go`：

```go
package archive_test

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/stretchr/testify/require"

    "github.com/a-shan/mysql-pitr/internal/archive"
    "github.com/a-shan/mysql-pitr/internal/binlog"
)

// 归档写入器消费 FileSource（真实 fixture）→ Seal → 还原文件必须与源字节一致。
func TestRoundtrip_FixtureBytesIdentical(t *testing.T) {
    fixture := filepath.Join("..", "binlog", "testdata", "mysql-8.0-row-full.bin")
    src, err := binlog.OpenFileSource(context.Background(), fixture, 0, replication.NewBinlogParser())
    require.NoError(t, err)
    defer src.Close()

    dir := t.TempDir()
    w := archive.NewWriter(dir)
    require.NoError(t, w.Consume(context.Background(), src))
    require.NoError(t, w.Seal("mysql-bin.000001.partial"))

    got, err := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
    require.NoError(t, err)
    want, err := os.ReadFile(fixture)
    require.NoError(t, err)
    require.Equal(t, want, got, "归档还原必须字节级一致")
}
```

（此测试同时验证 FileSource 的 RawData 完整性与 Seal 的校验和验证在真实文件上成立。）

- [ ] **Step 3: 跑测试验证通过**

Run: `go test ./internal/scan/ ./internal/archive/ -run TestGolden -v && go test ./internal/archive/ -run TestRoundtrip -v`
Expected: 全部 PASS

- [ ] **Step 4: 全仓回归**

Run: `go build ./... && go vet ./internal/... && go test ./internal/...`
Expected: 全部 PASS（旧代码不受影响）

- [ ] **Step 5: 提交**

```bash
git add internal/scan/golden_test.go internal/archive/roundtrip_test.go
git commit -m "test(scan,archive): fixture golden path and byte-identical archive roundtrip"
```

---

## Self-Review 结论（计划编写时已对照 spec 检查）

- **Spec 覆盖**：Source 双源（FileSource Task 2 / StreamSource Task 8）、事务聚合（Task 3 重构保留）、四维过滤（Tables/TimeRange/GTIDSet 已有 + SelectedTxIDs Task 3）、扫描模式与预览上限（Task 6）、归档写入封口验证与缺口检测（Task 7）、PK 优先 WHERE 与值格式化（Task 4/5）、大事务截断（基线已实现 WithMaxRowsPerTx）。不在本计划：SSE/WS/daemon/SQLite/Web（Phase 2-4）。
- **占位符扫描**：无 TBD/TODO；所有步骤含真实代码。
- **类型一致性**：`binlog.Source`（Task 2）→ 被 archive.Consume（Task 7）、stream.source（Task 8）消费；`Filter.SelectedTxIDs`（Task 3）→ scan.ModeSelectedSQL（Task 6）；`TableSchema.PrimaryKey`（Task 4）→ reverse buildWhere；`binlogtest.Event`（Task 7 定义）→ stream/archive 测试共用。均已对齐。
- **已知技术风险**：go-mysql v1.16.0 相对 v1.13.0 的 API 漂移在 Task 1 消化（回退策略见 Global Constraints）；`Consume` 的文件名由 RotateEvent.NextLogName 驱动，首个文件默认名 mysql-bin.000001（Phase 2 归档循环接管命名）。

## 执行交接

计划保存于 `docs/superpowers/plans/2026-08-10-pitr-v3-phase1-collector-engine.md`。两种执行方式：

1. **Subagent-Driven（推荐）**——每个任务派发独立 subagent，任务间两阶段 review
2. **Inline Execution**——本会话内按 executing-plans 批量执行，带检查点

完成后接续 Phase 2（agent daemon + executor + 重接 CLI）计划。
