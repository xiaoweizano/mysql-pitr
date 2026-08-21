# 审计条目字段补齐实现计划（目标表 / 恢复时间 / 影响行数）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让每条审计条目带上目标表与恢复时间，done/paused 条目带上 executor 累计的影响行数。

**Architecture:** 三段改动——executor 层逐语句累计 `sql.Result.RowsAffected()` 并经 `FinalReport`/`Checkpoint` 携带（跨 pause/resume 累计）；server 层 `appendAudit` 从 `op.Filter` 推导目标表/恢复时间、从 op_done 报告取行数写入审计；前端把行数 0 渲染为 `—`。设计文档：`docs/superpowers/specs/2026-08-21-audit-rich-fields-design.md`。

**Tech Stack:** Go 1.25（testing + testify）、SvelteKit 2 / svelte-check。

## Global Constraints

- 不引入新依赖；不改 SQLite schema（`audit_logs.target_table / recovery_time / rows_affected` 列已存在）。
- wire JSON 字段名用 camelCase：`rowsAffected`（与现有 `rowsAffected` 前端 TS 类型一致）。
- 行数只累计**成功提交**的语句：语句执行失败不计；批次回滚（取消/提交失败）整批不计。
- 旧审计记录不回填；`cancelled`/`failed` 终态条目行数传 0（前端渲染 `—`）。
- agent 端 `internal/daemon` 与 `internal/ws` **零改动**：op_done 事件已是整个 `FinalReport` 的 `json.Marshal`（`internal/daemon/execute.go:88`），新字段自动过 wire。
- 旧检查点 JSON 文件缺 `RowsAffected` 字段时反序列化为 0，无需迁移。

---

### Task 1: executor 累计影响行数

**Files:**
- Modify: `internal/executor/types.go:34-51`（FinalReport / Checkpoint 加字段）
- Modify: `internal/executor/executor.go:38-209`（累计、携带、持久化）
- Test: `internal/executor/executor_test.go`（fake 改造 + 4 个新测试）

**Interfaces:**
- Consumes: 现有 `Result.RowsAffected() (int64, error)`（`internal/executor/types.go:84-87`）
- Produces: `FinalReport.RowsAffected int64`（json `rowsAffected`）——Task 2 的 `opDonePayload` 与 done/paused 审计依赖；`Checkpoint.RowsAffected int64`——Resume 跨断点累计依赖。

- [ ] **Step 1: 改造测试 fake（可配置行数 + 行数错误 + 定向下标阻塞）**

在 `internal/executor/executor_test.go` 中：

把 30-33 行的 fakeResult 换成：

```go
type fakeResult struct {
	rows    int64
	errRows bool
}

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) {
	if r.errRows {
		return 0, fmt.Errorf("RowsAffected not supported (injected)")
	}
	return r.rows, nil
}
```

fakeDB 结构体（16-28 行）加三个字段：

```go
	// rowsOn：全局语句下标 → RowsAffected 返回值；缺省 1（保持旧行为）。
	rowsOn map[int]int64
	// rowsErrOn：命中的语句 RowsAffected 返回 error（driver 不支持），行数按 0 计。
	rowsErrOn map[int]bool
	// blockFromIdx：blockExec 非 nil 时从该全局语句下标起阻塞（0 = 全部阻塞，旧行为）。
	blockFromIdx int
```

fakeTx.Exec（41-54 行）整体替换为：

```go
func (t *fakeTx) Exec(query string, args ...interface{}) (Result, error) {
	t.db.mu.Lock()
	idx := len(t.db.executed)
	t.db.executed = append(t.db.executed, query)
	err := t.db.failOn[idx]
	t.db.mu.Unlock()
	if t.db.blockExec != nil && idx >= t.db.blockFromIdx {
		<-t.db.blockExec
	}
	if err != nil {
		return nil, err
	}
	if t.db.rowsErrOn[idx] {
		return fakeResult{errRows: true}, nil
	}
	rows := int64(1)
	if r, ok := t.db.rowsOn[idx]; ok {
		rows = r
	}
	return fakeResult{rows: rows}, nil
}
```

- [ ] **Step 2: 写 4 个失败测试**

追加到 `internal/executor/executor_test.go` 末尾：

```go
func TestExecutor_Run_AccumulatesRowsAffected(t *testing.T) {
	db := &fakeDB{rowsOn: map[int]int64{0: 2, 1: 3, 2: 0}}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-rows",
		Statements: []reverse.Statement{
			makeStmt("S1", 0), makeStmt("S2", 1), makeStmt("S3", 2),
		},
		BatchSize: 2,
	}

	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, report.Done)
	assert.Equal(t, int64(5), report.RowsAffected, "2+3+0 成功语句行数累计")

	cp, err := store.Load("op-rows")
	require.NoError(t, err)
	assert.Equal(t, int64(5), cp.RowsAffected, "检查点携带累计行数")
}

func TestExecutor_Run_RowsAffectedErrorCountsZero(t *testing.T) {
	// driver 不支持 RowsAffected（返回 error）：该语句按 0 计，不影响执行。
	db := &fakeDB{rowsErrOn: map[int]bool{0: true}}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-rows-err",
		Statements: []reverse.Statement{
			makeStmt("S1", 0), makeStmt("S2", 1),
		},
		BatchSize: 10,
	}

	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), report.RowsAffected, "语句 0 行数错误按 0 计，语句 1 计 1")
}

func TestExecutor_Resume_AccumulatesRowsAcrossCheckpoint(t *testing.T) {
	// 检查点携带 RowsAffected=4（断点前 2 条语句）；Resume 续跑 2 条 → 累计 6。
	db := &fakeDB{}
	store := NewInMemoryCheckpointStore()
	require.NoError(t, store.Save(Checkpoint{
		OperationID: "op-resume", LastCompletedStatement: 2, Total: 4, RowsAffected: 4,
	}))
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-resume",
		Statements: []reverse.Statement{
			makeStmt("S1", 0), makeStmt("S2", 1), makeStmt("S3", 2), makeStmt("S4", 3),
		},
		BatchSize: 10,
	}

	report, err := ex.Resume(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, report.Done)
	assert.Equal(t, int64(6), report.RowsAffected, "4（检查点携带）+ 2（续跑语句）")
}

func TestExecutor_Run_PausedReportCarriesRows(t *testing.T) {
	// BatchSize=2、4 条语句；第 3 条（idx 2，第二批内）阻塞。第一批提交后
	// （rows=2）取消 ctx → 当前批次回滚（其行数不计），paused 报告携带 2。
	db := &fakeDB{blockExec: make(chan struct{}), blockFromIdx: 2}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-pause-rows",
		Statements: []reverse.Statement{
			makeStmt("SQL 1", 0), makeStmt("SQL 2", 1), makeStmt("SQL 3", 2), makeStmt("SQL 4", 3),
		},
		BatchSize: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var report FinalReport
	var runErr error
	runDone := make(chan struct{})
	go func() {
		report, runErr = ex.Run(ctx, plan, nil)
		close(runDone)
	}()

	require.Eventually(t, func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		return len(db.executed) == 3
	}, time.Second, time.Millisecond)

	cancel()
	close(db.blockExec)
	<-runDone

	require.NoError(t, runErr)
	assert.True(t, report.Paused)
	assert.Equal(t, 2, report.Done)
	assert.Equal(t, int64(2), report.RowsAffected, "已提交批次的行数；回滚批次不计")
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/executor/ -run 'RowsAffected|PausedReportCarriesRows' -v`
Expected: FAIL——编译错误 `report.RowsAffected undefined` / `cp.RowsAffected undefined`（字段尚不存在）。

- [ ] **Step 4: 实现**

`internal/executor/types.go`——FinalReport（34-40 行）替换为：

```go
// FinalReport 是 Run/Resume 的返回值。
type FinalReport struct {
	Done         int         `json:"done"`
	Total        int         `json:"total"`
	RowsAffected int64       `json:"rowsAffected"` // 成功提交语句的累计影响行数
	Errors       []ExecError `json:"errors,omitempty"`
	Paused       bool        `json:"paused"` // true = ctx 取消导致暂停；false = 正常完成或失败
}
```

Checkpoint（45-51 行）替换为：

```go
// Checkpoint 持久化的执行进度。
type Checkpoint struct {
	OperationID            string
	LastCompletedStatement int
	Total                  int
	RowsAffected           int64
	Errors                 []ExecError
}
```

`internal/executor/executor.go`：

(a) Run 末行（54 行）：`return e.runFromIndex(ctx, plan, 0, nil, 0, cb)`

(b) Resume（65-92 行）替换为：

```go
func (e *executor) Resume(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error) {
	plan, err := e.normalizePlan(plan)
	if err != nil {
		return FinalReport{}, err
	}

	cp, err := e.store.Load(plan.OperationID)
	if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
		return FinalReport{}, fmt.Errorf("executor: load checkpoint: %w", err)
	}

	startIdx := 0
	var carriedErrs []ExecError
	var carriedRows int64
	if cp != nil {
		if cp.LastCompletedStatement > 0 {
			startIdx = cp.LastCompletedStatement
		}
		carriedErrs = cp.Errors
		carriedRows = cp.RowsAffected
	}
	if startIdx > len(plan.Statements) {
		// 检查点推进已超出当前 Plan（Plan 变更过）：没有剩余语句，返回检查点口径。
		return FinalReport{
			Done: len(plan.Statements), Total: len(plan.Statements),
			RowsAffected: carriedRows, Errors: carriedErrs, Paused: false,
		}, nil
	}
	return e.runFromIndex(ctx, plan, startIdx, carriedErrs, carriedRows, cb)
}
```

(c) runFromIndex（96-186 行）替换为：

```go
// runFromIndex 从 plan.Statements[startIdx] 开始执行。initialErrs/initialRows
// 携带断点前的已记录错误与累计行数（Run 传 nil/0；Resume 传检查点值），随最终
// 报告一起返回。行数只累计成功提交的语句：语句失败不计；批次回滚整批不计
// （batchRows 只在 Commit 成功后并入 rows）。
func (e *executor) runFromIndex(ctx context.Context, plan Plan, startIdx int, initialErrs []ExecError, initialRows int64, cb ProgressCallback) (FinalReport, error) {
	db, err := e.factory(plan)
	if err != nil {
		return FinalReport{}, fmt.Errorf("executor: open db: %w", err)
	}
	defer db.Close()

	errs := append([]ExecError(nil), initialErrs...)
	rows := initialRows
	completed := startIdx
	total := len(plan.Statements)

	for completed < total {
		// 检查 ctx 取消
		if err := ctx.Err(); err != nil {
			return e.pausedReport(plan, completed, rows, errs), nil
		}

		batchEnd := completed + plan.BatchSize
		if batchEnd > total {
			batchEnd = total
		}

		// 开启事务
		tx, err := db.Begin()
		if err != nil {
			return FinalReport{
				Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: begin tx: %w", err)
		}

		var batchErrs []ExecError
		var batchRows int64
		aborted := false
		for i := completed; i < batchEnd; i++ {
			// 每条 SQL 前检查取消
			if err := ctx.Err(); err != nil {
				aborted = true
				break
			}
			stmt := plan.Statements[i]
			if stmt.SQL == "" {
				continue
			}
			res, err := tx.Exec(stmt.SQL)
			if err != nil {
				batchErrs = append(batchErrs, ExecError{
					Statement: i, SQL: stmt.SQL, Err: err.Error(),
				})
				continue
			}
			if n, rerr := res.RowsAffected(); rerr == nil {
				batchRows += n
			}
		}

		if aborted {
			// 当前批次回滚，已完成的批次保留（其行数一并丢弃）
			_ = tx.Rollback()
			return e.pausedReport(plan, completed, rows, errs), nil
		}

		// 提交
		if err := tx.Commit(); err != nil {
			// 整批回滚（tx.Commit 失败时 driver 通常已回滚）
			_ = tx.Rollback()
			return FinalReport{
				Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: commit batch [%d,%d): %w", completed, batchEnd, err)
		}

		completed = batchEnd
		errs = append(errs, batchErrs...)
		rows += batchRows

		// 写检查点
		if err := e.store.Save(Checkpoint{
			OperationID:            plan.OperationID,
			LastCompletedStatement: completed,
			Total:                  total,
			RowsAffected:           rows,
			Errors:                 errs,
		}); err != nil {
			return FinalReport{
				Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: save checkpoint: %w", err)
		}

		if cb != nil {
			cb(Progress{
				Done: completed, Total: total,
				LastTxID: lastTxID(plan, completed-1),
				LastSQL:  lastSQL(plan, completed-1),
				Errors:   errs,
			})
		}
	}

	return FinalReport{Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false}, nil
}
```

(d) pausedReport（188-195 行）替换为：

```go
func (e *executor) pausedReport(plan Plan, completed int, rows int64, errs []ExecError) FinalReport {
	return FinalReport{
		Done:         completed,
		Total:        len(plan.Statements),
		RowsAffected: rows,
		Errors:       errs,
		Paused:       true,
	}
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/executor/ -v`
Expected: PASS（新增 4 个 + 现有全部——fake 缺省行为不变）。

- [ ] **Step 6: 提交**

```bash
git add internal/executor/types.go internal/executor/executor.go internal/executor/executor_test.go
git commit -m "feat(executor): accumulate rows affected in FinalReport and Checkpoint"
```

---

### Task 2: server 审计写入补齐

**Files:**
- Modify: `internal/server/pitr/handler.go:414-426`（appendAudit）、`779-860`（opDonePayload / op_done / confirmPause）、`445,466,748,823,841,914,1667`（7 个调用点）
- Test: `internal/server/pitr/handler_test.go`（4 个新测试追加到 op_done 相关测试附近）

**Interfaces:**
- Consumes: Task 1 的 `FinalReport.RowsAffected int64`（json `rowsAffected`）。
- Produces: `AuditEntry.TargetTable`（`"shop.orders, shop.items"` 格式）、`AuditEntry.RecoveryTime`（`Filter.TimeRange.End`）、`AuditEntry.RowsAffected`——审计 API / CSV / 前端已有消费方，无新接口。

- [ ] **Step 1: 写 4 个失败测试**

追加到 `internal/server/pitr/handler_test.go`（放在 `TestStreamEvent_OpDone_Done` 之后）：

```go
func TestStreamEvent_OpDone_AuditRichFields(t *testing.T) {
	// done 审计条目带上操作过滤器的目标表（多表逗号连接）与恢复时间
	// （Filter.TimeRange.End），以及 op_done 报告的累计影响行数。
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	recovery := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	op := &Operation{
		ID: "op_rich", OrgID: orgID, AgentID: agentID,
		Type: "pitr", Mode: "sql", Status: StateExecuting,
		Filter: binlog.Filter{
			Tables: []binlog.TableRef{
				{Schema: "shop", Table: "orders"}, {Schema: "shop", Table: "items"},
			},
			TimeRange: &binlog.TimeRange{Start: recovery.Add(-time.Hour), End: recovery},
		},
	}
	require.NoError(t, f.opStore.Create(op))

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone,
		map[string]interface{}{"done": 2, "total": 2, "rowsAffected": 7, "paused": false})

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "done", entries[0].Status)
	assert.Equal(t, "shop.orders, shop.items", entries[0].TargetTable)
	assert.Equal(t, recovery, entries[0].RecoveryTime)
	assert.Equal(t, int64(7), entries[0].RowsAffected)
}

func TestStreamEvent_OpDone_Paused_AuditCarriesRows(t *testing.T) {
	// paused 确认（op_done paused=true）的审计条目带上暂停时的累计行数。
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	op := &Operation{
		ID: "op_pause_rows", OrgID: orgID, AgentID: agentID,
		Type: "pitr", Mode: "sql", Status: StateExecuting,
		Filter: binlog.Filter{Tables: []binlog.TableRef{{Schema: "shop", Table: "orders"}}},
	}
	require.NoError(t, f.opStore.Create(op))

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone,
		map[string]interface{}{"done": 1, "total": 2, "rowsAffected": 3, "paused": true})

	got, err := f.opStore.Get(op.ID)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, got.Status)

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "paused", entries[0].Status)
	assert.Equal(t, "shop.orders", entries[0].TargetTable)
	assert.Equal(t, int64(3), entries[0].RowsAffected)
}

func TestStreamEvent_ScanDone_AuditTableAndTimeWithoutRows(t *testing.T) {
	// ready 条目（scan_done）：目标表/恢复时间已知，行数尚不可知 → 0。
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)

	recovery := time.Date(2026, 8, 20, 18, 30, 0, 0, time.UTC)
	op := &Operation{
		ID: "op_ready", OrgID: orgID, AgentID: agentID,
		Type: "pitr", Mode: "sql", Status: StateScanning,
		Filter: binlog.Filter{
			Tables:    []binlog.TableRef{{Schema: "shop", Table: "orders"}},
			TimeRange: &binlog.TimeRange{End: recovery},
		},
	}
	require.NoError(t, f.opStore.Create(op))

	f.injectStreamEvent(t, agentID, op.ID, ws.EvScanDone, map[string]interface{}{"txCount": 0})

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "ready", entries[0].Status)
	assert.Equal(t, "shop.orders", entries[0].TargetTable)
	assert.Equal(t, recovery, entries[0].RecoveryTime)
	assert.Equal(t, int64(0), entries[0].RowsAffected)
}

func TestStreamEvent_OpDone_NoFilterFieldsEmpty(t *testing.T) {
	// 无表过滤、无时间区间的操作（如纯 GTID 定位）：目标表空串、恢复时间
	// 零值；op_done 不带 rowsAffected 键 → 行数 0。
	f := setupTest(t)
	userID := f.createUser(t)
	orgID := f.createOrg(t, userID)
	agentID := f.createAgent(t, orgID)
	op := f.createOp(t, "op_nofilter", orgID, agentID, StateExecuting)

	f.injectStreamEvent(t, agentID, op.ID, ws.EvOpDone,
		map[string]interface{}{"done": 1, "total": 1, "paused": false})

	entries, err := f.auditStore.Query(audit.AuditFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "done", entries[0].Status)
	assert.Empty(t, entries[0].TargetTable)
	assert.True(t, entries[0].RecoveryTime.IsZero())
	assert.Equal(t, int64(0), entries[0].RowsAffected)
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/server/pitr/ -run 'AuditRichFields|CarriesRows|TableAndTimeWithoutRows|NoFilterFieldsEmpty' -v`
Expected: FAIL——`TargetTable` 为空 / `RowsAffected` 为 0（写入端未填）。

- [ ] **Step 3: 实现**

`internal/server/pitr/handler.go`（`binlog`、`strings` 已在 import 里）：

(a) `opDonePayload`（855-860 行）替换为：

```go
type opDonePayload struct {
	Done         int                  `json:"done"`
	Total        int                  `json:"total"`
	RowsAffected int64                `json:"rowsAffected"`
	Paused       bool                 `json:"paused"`
	Errors       []executor.ExecError `json:"errors,omitempty"`
}
```

(b) `appendAudit`（414-426 行）替换为：

```go
// appendAudit records an audit entry for an operation state change. Stream
// events record the agent as the operator; HTTP actions record the user.
// TargetTable / RecoveryTime derive from the operation's filter (static
// properties known since creation); rows is the execution result where the
// triggering event carries it (done / paused), 0 otherwise.
func (h *Handler) appendAudit(op *Operation, state OperationState, operator, errorDetails string, rows int64) {
	var recovery time.Time
	if op.Filter.TimeRange != nil {
		recovery = op.Filter.TimeRange.End
	}
	_ = h.auditStore.Append(&audit.AuditEntry{
		OperationID:  op.ID,
		Operator:     operator,
		Timestamp:    time.Now(),
		OrgID:        op.OrgID,
		AgentID:      op.AgentID,
		TargetTable:  joinTableRefs(op.Filter.Tables),
		RecoveryTime: recovery,
		RowsAffected: rows,
		Status:       string(state),
		ErrorDetails: errorDetails,
	})
}

// joinTableRefs renders the filter's table list as "schema.table" entries
// joined with ", " (e.g. "shop.orders, shop.items"); an empty filter yields "".
func joinTableRefs(tables []binlog.TableRef) string {
	if len(tables) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		parts = append(parts, t.Schema+"."+t.Table)
	}
	return strings.Join(parts, ", ")
}
```

(c) 7 个调用点改为传 `rows`：

| 行号（改前） | 旧调用 | 新调用 |
|---|---|---|
| 445 | `h.appendAudit(op, StateFailed, "agent", details)` | `h.appendAudit(op, StateFailed, "agent", details, 0)` |
| 466 | `h.appendAudit(op, StateBlocked, "system", details)` | `h.appendAudit(op, StateBlocked, "system", details, 0)` |
| 748 | `h.appendAudit(op, StateReady, "agent", "")` | `h.appendAudit(op, StateReady, "agent", "", 0)` |
| 823 | `h.appendAudit(op, StateDone, "agent", errorSummary(report.Errors))` | `h.appendAudit(op, StateDone, "agent", errorSummary(report.Errors), report.RowsAffected)` |
| 841 | `h.appendAudit(op, StateFailed, "agent", errorDetailsFromEvent(raw))` | `h.appendAudit(op, StateFailed, "agent", errorDetailsFromEvent(raw), 0)` |
| 914 | `h.appendAudit(op, StatePaused, "agent", "")` | `h.appendAudit(op, StatePaused, "agent", "", report.RowsAffected)` |
| 1667 | `h.appendAudit(op, StateCancelled, emailFromRequest(r), "")` | `h.appendAudit(op, StateCancelled, emailFromRequest(r), "", 0)` |

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/server/pitr/ -v`
Expected: PASS（新增 4 个 + 现有全部——现有断言不涉及新字段）。

- [ ] **Step 5: 提交**

```bash
git add internal/server/pitr/handler.go internal/server/pitr/handler_test.go
git commit -m "feat(pitr): audit entries carry target table, recovery time and rows affected"
```

---

### Task 3: 前端行数 0 渲染为 —

**Files:**
- Modify: `web/src/routes/audit/+page.svelte:368`
- Regenerate: `internal/server/embed_build/`（经 `make build-web`）

**Interfaces:**
- Consumes: `AuditEntry.rowsAffected: number`（`web/src/lib/api/audit.ts` 已有，无改动）。
- Produces: 无。

- [ ] **Step 1: 修改渲染**

`web/src/routes/audit/+page.svelte` 368 行：

```svelte
<TableCell class="text-right tabular-nums">{e.rowsAffected}</TableCell>
```

改为（与 205-209 行 `recoveryLabel` 的零值 `—` 处理对齐）：

```svelte
<TableCell class="text-right tabular-nums">{e.rowsAffected > 0 ? e.rowsAffected : '—'}</TableCell>
```

- [ ] **Step 2: 类型检查**

Run: `cd web && npm run check`
Expected: 0 errors（纯展示表达式改动）。

- [ ] **Step 3: 重建内嵌前端**

Run: `make build-web`（仓库根目录；`cd web && npm ci && npm run build` 后拷入 `internal/server/embed_build/`）
Expected: `web/build` 生成且 `internal/server/embed_build` 内容刷新（`.gitkeep` 保留）。

- [ ] **Step 4: 提交**

```bash
git add web/src/routes/audit/+page.svelte internal/server/embed_build
git commit -m "feat(web): audit rows-affected renders dash for zero"
```

---

### Task 4: 全量回归验证

**Files:** 无新改动（本任务只跑验证；发现问题回上游任务修）。

- [ ] **Step 1: Go 全量构建 + 测试**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: 全部 PASS（README 记录 24 个包）。

- [ ] **Step 2: server 二进制冒烟（前端已内嵌）**

Run: `go build -o bin/mysql-pitr-server ./cmd/server && ./bin/mysql-pitr-server`（默认 `:8080`，Ctrl-C 退出即可）
Expected: 启动无 panic、无 embed 缺失告警。

- [ ] **Step 3: 收尾**

若以上全绿，本计划完成；无需额外提交。若有失败，修复后按所属任务的 commit 风格追加提交。
