# 审计条目字段补齐设计（目标表 / 恢复时间 / 影响行数）

**日期**：2026-08-21
**状态**：已评审（用户确认），待实现
**作者**：a-shan 团队

## 背景

审计页面（`web/src/routes/audit/+page.svelte`）的目标表、影响行数两列恒为空（`—` / `0`），展开详情里的恢复时间也恒为 `—`。

排查结论：整条链路的 schema、SQLite 列（`audit_logs.target_table / recovery_time / rows_affected`）、JSON wire 字段、前端 TS 类型、UI 列、CSV 导出表头**全部已存在**，唯独写入端 `appendAudit`（`internal/server/pitr/handler.go:416`）只填了 OperationID / Operator / Timestamp / OrgID / AgentID / Status / ErrorDetails 七个字段，三个业务字段从未写入。

## 目标

- 每条审计条目带上该操作的**目标表**与**恢复时间**（创建时已知，静态属性）
- done / paused 状态的审计条目带上**影响行数**（执行结果，按已知时点填）
- CSV 导出自动随之有值（已引用字段，无需改动）

## 已确认决策

| 决策点 | 选择 |
|---|---|
| 历史数据 | **不回填**——只修写入端，旧记录保持空（`—`） |
| 行数语义 | **按已知时点填**——目标表/恢复时间在所有条目上填；行数只在 done/paused 条目上填，其余状态显示 `—`（前端把 0 渲染为 `—`） |
| 方案 | 写入端补齐 + executor 累计行数（否决联表方案：audit 包反向耦合 pitr 表结构、行数问题仍需动 executor） |

## 字段数据源

| 字段 | 数据源 | 说明 |
|---|---|---|
| 目标表 | `op.Filter.Tables`（`[]binlog.TableRef`） | 多表逗号连接 `db.t1, db.t2`；无表过滤（如全库扫描）为空串 → 前端 `—` |
| 恢复时间 | `op.Filter.TimeRange.End` | 恢复目标时间点；无时间区间（如纯 GTID 定位）为零值 → 前端已渲染 `—` |
| 影响行数 | executor 逐语句累计 `sql.Result.RowsAffected()` | 经 `FinalReport` → op_done 事件 → server `opDonePayload` |

## 改动设计

### 1. executor 层（`internal/executor/`）

- `FinalReport` 加字段：

  ```go
  RowsAffected int64 `json:"rowsAffected"`
  ```

- `Checkpoint` 加字段 `RowsAffected int64`——resume 时作为初始累计值继续累加，与 `Errors` 的跨断点携带模式完全一致。旧检查点 JSON 文件缺该字段时反序列化为 0，天然向后兼容。
- `runFromIndex`（executor.go）：

  ```go
  res, err := tx.Exec(stmt.SQL)
  // 成功：rows += res.RowsAffected()（driver 不支持时该调用返回 error，按 0 处理，不影响执行）
  ```

  Run 起点（0）、Resume 起点（checkpoint 携带值）、`pausedReport` 都带上累计值；每批 commit 后的 `Checkpoint.Save` 同步写入。

### 2. server 层（`internal/server/pitr/handler.go`）

- `opDonePayload` 加 `RowsAffected int64 \`json:"rowsAffected"\``（agent 端 op_done 本就整结构序列化 `FinalReport`，wire 自动携带，`internal/ws` 无需改动）。
- `appendAudit` 补齐字段：
  - `TargetTable`：所有调用点从手头 `op.Filter.Tables` 推导（joinTableRefs 辅助函数）
  - `RecoveryTime`：`op.Filter.TimeRange` 非空时取 `.End`
  - `RowsAffected`：函数加 `rows int64` 参数——done 调用点传 `report.RowsAffected`，`confirmPause`（paused 条目）传暂停报告行数，其余调用点传 0
- 现有 7 个 `appendAudit` 调用点：ready(748)、done(823)、failed(445/841)、blocked(466)、paused(914)、cancelled(1667)。

### 3. 前端（`web/src/routes/audit/+page.svelte`）

- 行数列渲染：`e.rowsAffected > 0 ? e.rowsAffected : '—'`（与 `recoveryLabel` 的零时间 `—` 处理对齐）

### 4. 不改动

- CSV 导出（`audit/handler.go` Export 已引用三字段）
- `internal/ws` 类型（整结构序列化自动携带）
- SQLite schema / 迁移（列已存在）
- 前端 TS 类型（`web/src/lib/api/audit.ts` 已有字段）

## 测试

- **executor 单测**：多语句行数累计；resume 跨检查点累计（断点前后行数相加）；paused 报告行数；driver 返回 RowsAffected 错误时按 0
- **pitr handler 单测**：done 审计条目带 `report.RowsAffected`；paused 条目带暂停行数；所有条目带目标表/恢复时间；多表 join 格式；无表/无区间为空值
- **前端**：`svelte-check`

## 影响面

| 文件 | 改动 |
|---|---|
| `internal/executor/types.go` | FinalReport / Checkpoint 加字段 |
| `internal/executor/executor.go` | 累计行数、携带与持久化 |
| `internal/server/pitr/handler.go` | opDonePayload、appendAudit 补齐 |
| `web/src/routes/audit/+page.svelte` | 行数 0 → `—` |
| 各自测试文件 | 新增断言 |

## 已知限制

- 旧审计记录不回填（已确认）
- 语句执行失败的行数不计入（`RowsAffected` 仅累计成功语句）
- cancelled / failed 终态条目的行数不可知（agent 无最终报告送达路径），保持 `—`
