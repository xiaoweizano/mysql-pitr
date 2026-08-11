# PITR v3 Phase 4：SvelteKit Web + 后端收尾 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付可用产品面：后端收尾（Resume 真实现、execute 幂等、断线处置、两阶段扫描补全、API 契约修正）+ SvelteKit Web 前端（adapter-static 内嵌 server 二进制）+ 集成验证。

**Architecture:** 两部分——Part A 后端（Go）：executor 检查点续跑、op_done 幂等回退 + CAS、hub 断线→blocked + 启动 reconcile、selected 模式两阶段二次扫描、List filter DTO 投影、archive_status 端点；Part B 前端（SvelteKit 2 + Svelte 5 + adapter-static SPA + Tailwind + shadcn-svelte，中文轻量 i18n）：登录/实例/归档监控/PITR 向导/操作历史/组织六类页面，`web/build` 经构建步骤拷入 `internal/server/embed_build/` 由 `//go:embed` 内嵌。

**Tech Stack:** Go 1.25（后端）、Node 22 + npm（registry.npmjs.org 可达）、SvelteKit 2 + Svelte 5（runes）、adapter-static（`ssr=false` + `fallback:'index.html'`）、Tailwind CSS v4、shadcn-svelte、TypeScript。

## 本计划范围（阶段拆分）

- Phase 1/2/3（完成）：采集引擎 / agent daemon / server 平台
- **Phase 4（本计划）**：Part A 后端收尾（5 任务）+ Part B SvelteKit Web（5 任务）+ Part C 集成（1 任务）

## Global Constraints

- Go 工具链 1.26.5；go.mod `go 1.25.0`；GOPROXY=goproxy.cn；go-mysql v1.16.0 锁版
- **Node 22.14.0 / npm 10.9.2**（已装）；npm registry npmjs/npmmirror 均可达；前端依赖全部走 npm
- **web/ 目录整体替换**：旧 React 应用删除，SvelteKit 全新实现；`web/node_modules` 不提交（.gitignore）
- `//go:embed` 不能跨包目录：`web/build` 产物经构建步骤拷入 `internal/server/embed_build/`（提交 `.gitkeep` 保证目录存在）；embed.go 同时 embed `embed_stub`（无前端产物时的兜底）
- 每任务结束 `go build ./...` 必须通过；前端任务 `npm run check`（svelte-check）+ `npm run build` 必须通过
- 后端新端点/契约变更遵循 Phase 3 既有 wire 约定（小写驼峰、txMetaWire/StatementWire 形状）
- TDD（后端）+ 前端组件以 `svelte-check` 类型检查与 `npm run build` 为门禁；每任务独立 commit

## Phase 3 交接（本计划必须落实）

1. **Resume 真实现**（final-review I 类）：executor 从检查点续跑（Run 不清检查点、从 LastCompletedStatement+1 继续）；FinalReport.Errors surfacing 到 SSE/审计；server 侧 progress → checkpoints 表双写
2. **execute 幂等回退**：op_done 到达时 op 仍 ready → 视作 executing 落库（`UPDATE ... WHERE status=?` CAS）；顺带 Get→Update CAS 化
3. **executing 断线处置 + 启动 reconcile**：hub OnDisconnect → executing op → blocked（+ audit）；server 启动扫描非终态 op 处置
4. **List filter DTO 投影**：operationView 的 filter 用 filterJSON DTO（小写键、GTIDSet 字符串）而非内部 binlog.Filter
5. **selected 模式两阶段补全**（Phase 3 功能缺口）：Select 对选中 TxIDs 触发第二次 `CmdScan(Mode="selected", SelectedTxIDs)` 定向生成 SQL
6. **archive_status 端点**：GET /api/agents/{id}/archive → CmdArchiveStatus → collector.State
7. Minor 批量：type 校验、preview 内存上界、API 404 语义、Select 两步原子化、SSE pause 帧、迁移 user_version 测试

---

## Part A：后端收尾

### Task 1: executor 检查点续跑（Resume 真实现）+ 错误 surfacing

**Files:**
- Modify: `internal/executor/executor.go`（Resume 真实现：载入检查点 → `runFromIndex`）、`internal/executor/types.go`（Resume 签名：`Resume(ctx, plan Plan, cb)`）、`internal/executor/executor_test.go`
- Modify: `internal/daemon/execute.go`（Resume 用 executor.Resume；EvOpDone 载荷含 Errors）、`internal/server/pitr/handler.go`（opDonePayload 解析 Errors → SSE/audit）、`internal/server/pitr/events.go`（progress → server 检查点落库）

**Interfaces:**
```go
// types.go 变化
Executor interface {
    Run(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error)
    Resume(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error) // 原签名 operationID 版废弃
}
// Resume：Load(plan.OperationID) 检查点 → 无检查点 = 从 0 全跑；有 → runFromIndex(LastCompletedStatement+1)
// 与 Run 的唯一区别：不清检查点、起点为检查点推进处
```

- [ ] **Step 1: 写失败测试（executor.Resume 续跑）**

`internal/executor/executor_test.go` 追加：
- `TestResume_ContinuesFromCheckpoint`：Plan 100 条语句；先 Run 到中途取消（ctx cancel 模拟，检查点停在某批次）；再 Resume 同 Plan → 断言仅执行了剩余语句（sqlmock 期望语句数 = 剩余）、Done=Total
- `TestResume_NoCheckpoint_FullRun`：无检查点 → 从 0 全跑
- `TestResume_ErrorsSurfaced`：中途语句失败 → FinalReport.Errors 含该条

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/executor/ -run TestResume -v`
Expected: FAIL（Resume 是 stub 返回错误）

- [ ] **Step 3: 实现 executor.Resume + daemon/server 接线**

`executor.go`：`Resume` 载入检查点（`store.Load`），`startIdx := cp.LastCompletedStatement + 1`，`runFromIndex(ctx, plan, startIdx, cb)`；Run 保持现状（清检查点）。检查点保存时机不变（每批提交后 Save 含 LastCompletedStatement）。

`internal/daemon/execute.go`：`Resume(ctx, id, req)` 改为 `exec.Resume(ctx, plan, cb)`（不再 Run）；`EvOpDone` 的 Data 保持 `json.Marshal(FinalReport)`（含 Errors 字段——types.go 的 FinalReport.Errors 已有 json tag？检查并补）。

`internal/server/pitr/handler.go`：opDonePayload 增 `Errors []executor.ExecError`；op_done 时 Errors 非空 → audit detail 记录 + SSE `op_done` 载荷带 errors（前端展示）。progress 事件 → `checkpoints` 表 upsert（`store` 包增 `SaveCheckpoint(opID, lastStmt, total, errors)`——operations 已有 checkpoints 表，本任务启用）。

- [ ] **Step 4: 跑测试 + 提交**

Run: `go build ./... && go test ./internal/executor/ ./internal/daemon/ ./internal/server/pitr/ ./internal/server/store/ -count=1`
Commit: `feat(executor,pitr): checkpoint-aware resume with error surfacing`

---

### Task 2: execute 幂等回退 + 状态 CAS

**Files:**
- Modify: `internal/server/pitr/handler.go`（op_done 对 ready→executing 幂等落库；Update 改 CAS）、`internal/server/pitr/store.go`（`UpdateIfStatus(op, from, to)` 条件更新）、`internal/server/pitr/store_test.go`、`internal/server/pitr/handler_test.go`

**Interfaces:**
```go
// store.go 增
// UpdateIfStatus 仅当 op 当前状态 == from 时更新为 to（CAS）；RowsAffected==0 返回 (false, nil)。
func (s *SQLiteOperationStore) UpdateIfStatus(op *Operation, from OperationState) (bool, error)
```

- [ ] **Step 1: 写失败测试**

- `store_test.go`：`TestUpdateIfStatus_CAS`——并发读改写模拟（先置 ready，CAS ready→executing 成功；再 CAS ready→executing 失败返回 false）
- `handler_test.go`：`TestOpDone_ArrivesBeforeExecutePersisted`——注入 op_done 到 ready op → 断言 op 被置为 done（幂等回退：视 executing 落库后 done）；注入 op_done 到已 cancelled op → 不变

- [ ] **Step 2: 跑测试验证失败**

Run: `go test ./internal/server/pitr/ -run "TestUpdateIfStatus|TestOpDone_Arrives" -v`

- [ ] **Step 3: 实现**

`store.go`：`UPDATE operations SET status=?, updated_at=? WHERE id=? AND status=?` → RowsAffected 判定。
`handler.go`：
- op_done 分支：op 状态为 ready → 先 `UpdateIfStatus(op, StateExecuting)` 幂等落库（忽略 false——另一并发已推进）→ 再走 executing→done 迁移
- `transitionTo` 内部改用 `UpdateIfStatus`（Get→CAS 合一，消除读改写窗口）：`TransitionOp(from, to, mutate)` helper——读 op → 校验转移 → CAS → 成功才 audit

- [ ] **Step 4: 跑测试 + 提交**

Commit: `fix(pitr): idempotent op_done fallback and CAS state transitions`

---

### Task 3: executing 断线处置 + 启动 reconcile

**Files:**
- Modify: `internal/server/server.go`（hub OnDisconnect → 处置 executing ops）、`internal/server/pitr/store.go`（`ListByStatus`）、`internal/server/pitr/handler.go`（`HandleAgentDisconnect(agentID)`：executing op → blocked + audit；scanning op → blocked）、`internal/server/server_test.go`

**Interfaces:**
```go
// store.go 增
ListByStatus(status OperationState) ([]*Operation, error)
// pitr 包增
func (h *Handler) HandleAgentDisconnect(agentID string) // 幂等：op 非终态且 agentID 匹配 → blocked + audit
func (h *Handler) ReconcileOnStartup() error            // 启动时：非终态 op → blocked + audit（agent 重连后由用户重试）
```

- [ ] **Step 1: 写失败测试**

`handler_test.go`：`TestHandleAgentDisconnect_ExecutingBecomesBlocked`（executing op + 断连 → blocked + audit）；`TestHandleAgentDisconnect_Idempotent`（done op 不受影响）。`server_test.go` 或 pitr 测试内 `TestReconcileOnStartup_NonTerminalBlocked`。

- [ ] **Step 2: 跑测试验证失败**

- [ ] **Step 3: 实现**

`server.go` 的 hub 生命周期钩子：OnDisconnect 里调 `pitrHandler.HandleAgentDisconnect(agentID)`（现有 OnDisconnect 已更新 agentStore.status，追加 op 处置）。`New()` 引导末尾调 `ReconcileOnStartup()`（错误仅 log——启动不应因历史 op 失败）。

- [ ] **Step 4: 跑测试 + 提交**

Commit: `feat(pitr): block executing ops on agent disconnect; startup reconcile`

---

### Task 4: selected 模式两阶段补全 + archive_status 端点

**Files:**
- Modify: `internal/server/pitr/handler.go`（Select 触发二次扫描）、`internal/server/pitr/handler_test.go`、`internal/server/router.go`（archive 路由）、`internal/server/agent/handler.go` 或新 handler（archive_status）

**接口（wire 不变）：**
- `POST /api/pitr/{id}/select {txIds}`：op=ready 且 mode="selected" → 先下发 `CmdScan(Cmd: opID, Type: CmdScan, Params: {filter: 原 filter, mode: "selected", selectedTxIds: txIds})` 二次扫描 → SQL 事件暂存 → op 保持 ready（SQL 就绪后由前端调 transactions 查看/execute）——**状态设计**：二次扫描期间 op 用 `scanning` 态（ready→scanning 需加合法转移，见 Task 4 Step 3），scan_done 回 ready。mode="sql" 的 Select 保持现状（SQL 已在首扫暂存）
- `GET /api/agents/{id}/archive`：SendToAgent(CmdArchiveStatus) → 返回 collector.State JSON（离线 → 503 + agent offline）

- [ ] **Step 1: 写失败测试**

`handler_test.go`：
- `TestSelect_SelectedMode_TriggersRescan`：mode="selected" op（meta 首扫完成）→ Select(txIds) → 断言 fake hub 收到第二个 CmdScan（Mode="selected"、SelectedTxIDs=txIds、Cmd=opID）→ 注入 sql 事件 → scan_done → op 回 ready
- `TestArchiveStatus_Online`：fake hub 返回 collector.State → 断言响应 JSON
- `TestArchiveStatus_Offline`：IsConnected false → 503

- [ ] **Step 2: 跑测试验证失败**

- [ ] **Step 3: 实现**

状态机：`ready→scanning` 加为合法转移（state.go 增列 + state_test 同步）。Select 的 mode 分支：`"sql"` 走现状（内存暂存→SaveStatements）；`"selected"` 发二次扫描命令、op→scanning；scan_done 回 ready 时 SaveStatements（从二次暂存取）。

`router.go`：`/api/agents/{id}/archive` → 新 handler 方法（放 `internal/server/agent` 包或 pitr——**决策**：放 agent 包，依赖 pitr 的 AgentCommander 接口（IsConnected/SendToAgent 已抽象，agent handler 构造时注入 commander）。

- [ ] **Step 4: 跑测试 + 提交**

Commit: `feat(pitr,agent): two-phase selected-mode rescan and archive status endpoint`

---

### Task 5: List filter DTO + minor 批量

**Files:**
- Modify: `internal/server/pitr/handler.go`（operationView filter 投影 + type 校验 + preview 内存上界 + SSE pause 帧 + Select 原子化）、`internal/server/router.go`（API 404）、`internal/server/pitr/events.go`、对应测试

- [ ] **Step 1: 写失败测试**

- `TestList_FilterDTO_Shape`：创建 op（含 filter 的 GTIDSet/时间）→ List → 断言 filter 键小写（tables/timeStart/gtidSet）且 GTIDSet 为字符串
- `TestStart_RejectsUnknownType`：type="bogus" → 400
- `TestPreview_MemoryBound`：MaxPreview 大 + 大 RowCount 模拟 → 断言预览字节/条目上界生效（实现：opPreview 累计条目数 > N 时丢弃后续——**决策**：沿用 MaxPreview 条数上限 + 单事务 RowCount 上限（maxRowsPerTx 服务端钳制 1_000_000）即可，字节上界不做）
- `TestSelect_Atomic`：SaveStatements + Update 同一事务（store.SaveStatementsAndSelect(opID, stmts, txIds)）
- `TestEvents_PauseFrame`：pause 确认时 Publish 一个 `op_paused` 事件（新增 Kind `EvOpPaused = "op_paused"`，ws/types.go 增常量 + 测试）

- [ ] **Step 2: 跑测试验证失败**

- [ ] **Step 3: 实现**

- operationView：filter 用 `marshalFilter(op.Filter)` 产出 DTO（复用现有 marshalFilter）
- Start：type 白名单校验
- store：`SaveStatementsAndSelect` 单事务
- events/SSE：confirmPause 时 Publish EvOpPaused；SSE 客户端据此显示 paused
- router：`/api/*` 未知路径返回 404（保留 SPA fallback 仅对非 /api 前缀）

- [ ] **Step 4: 跑测试 + 提交**

Commit: `feat(pitr): filter DTO projection, type validation, atomic select, pause event`

---

## Part B：SvelteKit Web

### Task 6: 脚手架（SvelteKit + adapter-static + Tailwind + shadcn-svelte + i18n）

**Files:**
- Create: `web/`（全新替换旧 React 应用——先 `rm -rf web/src web/*.ts web/*.json 中旧文件`，保留 node_modules 清理后重装）、`web/package.json`、`web/svelte.config.js`、`web/vite.config.ts`、`web/tsconfig.json`、`web/src/...` 骨架

**关键配置：**
- `svelte.config.js`：`adapter: adapterStatic({ fallback: 'index.html' })`
- `src/hooks.js`：`export const ssr = false`（纯 SPA——adapter-static 的 SPA 模式）
- Tailwind v4：`@tailwindcss/vite` 插件 + `src/app.css` 单文件 CSS 配置（无 tailwind.config.js）
- shadcn-svelte：`npx shadcn-svelte@latest init`（components.json 到 web/）——网络可用，初始化后提交 components.json；**首次可用按钮/卡片/表格/对话框组件**
- i18n：`src/lib/i18n.ts`（`t(key)` + `locales/zh-CN.json`；Svelte 5 runes `$state` 存当前 locale；缺省中文）

- [ ] **Step 1: 脚手架命令**

```bash
cd /d/a-shan/web
rm -rf src index.html vite.config.ts tsconfig*.json package.json package-lock.json public node_modules
npm create svelte@latest . -- --yes --template skeleton --types ts --no-add-ons 2>&1 | tail -5
npm install
npm i -D @sveltejs/adapter-static tailwindcss @tailwindcss/vite
npx shadcn-svelte@latest init -y -d   # 或按交互提示选 base color 等
npm install  # shadcn 追加依赖
```

（若 `npm create svelte` 交互受限，改用手动写 package.json + svelte.config.js + vite.config.ts + src 骨架——以能 `npm run dev` 起为准，报告说明实际方式。）

- [ ] **Step 2: 配置 adapter-static + Tailwind + 最小页面**

`src/routes/+layout.svelte`（中文骨架 + Tailwind 样式）、`src/routes/+page.svelte`（占位首页）。

- [ ] **Step 3: 门禁验证**

Run: `cd web && npm run check && npm run build`
Expected: 通过且产出 `web/build/index.html` + 静态资源

- [ ] **Step 4: 提交**

```bash
git add web/  # 排除 node_modules（确认 .gitignore 已含）
git commit -m "feat(web): scaffold SvelteKit SPA with adapter-static, tailwind and shadcn-svelte"
```

---

### Task 7: Auth（登录/注册 + JWT 客户端 + 路由守卫）

**Files:**
- Create: `web/src/lib/api/client.ts`（fetch 包装：baseURL /api、Authorization Bearer、401 → 跳 /login）、`web/src/lib/api/auth.ts`、`web/src/lib/auth.ts`（$state token/user + localStorage 持久化）、`web/src/routes/login/+page.svelte`、`web/src/routes/register/+page.svelte`、`web/src/routes/+layout.svelte`（守卫：未登录重定向）

**接口（对照 Phase 3 wire）：**
- `POST /api/auth/register {email, password}` → 201
- `POST /api/auth/login {email, password}` → `{token, user}`（Phase 3 auth handler 现有形状）

- [ ] **Step 1: 实现 client/auth + 登录/注册页 + 守卫**

（前端无单测设施约定——门禁为 `npm run check`；行为验证：`npm run dev` 手工冒烟或接 gstack /browse——**决策**：本任务以 check+build 为门禁，浏览器冒烟在 Task 11 集成统一做。）

- [ ] **Step 2: 门禁 + 提交**

Commit: `feat(web): auth pages with JWT client and route guard`

---

### Task 8: 布局 + 实例列表 + 归档监控

**Files:**
- Create: `web/src/routes/+layout.svelte`（侧边导航：实例/操作/审计/组织 + 退出）、`web/src/routes/instances/+page.svelte`（实例列表：状态徽章、归档摘要、审批按钮）、`web/src/routes/instances/[id]/+page.svelte`（实例详情：归档状态卡片）、`web/src/lib/api/agents.ts`

**接口：**
- `GET /api/agents`（列表，含 status/approved/lastSeen）
- `POST /api/agents/{id}/approve` / `reject`
- `GET /api/agents/{id}/archive`（Task 4 产出：归档状态 JSON）

- [ ] **Step 1: 实现三页面 + agents API**
- [ ] **Step 2: 门禁 + 提交**

Commit: `feat(web): instances list, approval and archive monitoring pages`

---

### Task 9: PITR 恢复向导（核心）

**Files:**
- Create: `web/src/lib/api/pitr.ts`（start/transactions/select/execute/pause/resume/cancel/status/list + SSE 客户端）、`web/src/lib/sse.ts`（EventSource 封装：`/api/pitr/{id}/events` → 事件回调）、`web/src/routes/pitr/+page.svelte`（向导容器 + 步骤状态机）、`web/src/routes/pitr/[id]/+page.svelte`（详情：进度/勾选/执行）、`web/src/routes/operations/+page.svelte`（操作历史列表）

**向导五步（设计文档）：**
1. **类型与实例**：误删恢复/UPDATE回滚/指定时间/指定事务/GTID定位 → mode 映射：误删恢复/UPDATE回滚/指定时间 → `"sql"`；指定事务/GTID定位 → `"selected"`；选实例（在线 agent）
2. **过滤**：表多选（文本输入 schema.table 列表）、时间区间（datetime-local → RFC3339）、GTID 集输入（文本）；→ `POST /api/pitr/start`
3. **事务列表**（scanning 态）：SSE 流式 tx_meta → 表格（TxID/时间/表/行数/截断标记）+ 扫描进度；scan_done → ready
4. **SQL 预览勾选**：按事务分组展示（mode=sql 直接用；mode=selected 勾选事务 → POST select → 二次扫描 SQL 事件到齐后展示）→ 勾选 SQL 行（或全选事务）→ `POST /api/pitr/{id}/execute`
5. **执行**：SSE progress（done/total/errors）→ pause/resume/cancel 按钮 → op_done（含 errors 展示）/op_error

**SSE 事件处理：** `tx_meta`/`sql`/`scan_done`/`progress`/`op_paused`（Task 5）/`op_done`/`op_error`；SSE 断开重连（EventSource 自动）+ 终态关闭。

- [ ] **Step 1: sse.ts + pitr.ts + 向导骨架（步骤 1-2）**
- [ ] **Step 2: 步骤 3-5 + 详情页 + 操作历史页**
- [ ] **Step 3: 门禁 + 提交**

Commit: `feat(web): PITR wizard with SSE-driven scan and execution`

---

### Task 10: 审计 + 组织页 + 构建接入 embed

**Files:**
- Create: `web/src/routes/audit/+page.svelte`（审计日志：筛选 + 导出 CSV）、`web/src/routes/org/+page.svelte`（组织/成员/邀请——对照 org API）、`web/src/lib/api/audit.ts`、`web/src/lib/api/org.ts`
- Modify: `internal/server/embed.go`（embed_build 优先、embed_stub 兜底）、`internal/server/embed_build/.gitkeep`（新）、`Makefile`（`build-web` 目标：npm build → 拷贝 web/build → embed_build）、`deploy/README.md`（WEB_DIR 清理——Phase 3 minor）

**接口：**
- `GET /api/audit?orgId=&from=&to=&status=&agentId=`（audit.Query 现有形状）、`GET /api/audit/export`
- `GET/POST /api/orgs`、`POST /api/orgs/{id}/invite`、`POST /api/orgs/{id}/accept`、`GET /api/orgs/{id}/members`

- [ ] **Step 1: 审计页 + 组织页**
- [ ] **Step 2: embed 接线改造**

`internal/server/embed.go`：

```go
//go:embed embed_stub/*
var stubFS embed.FS
//go:embed embed_build/*
var buildFS embed.FS

// resolveWebFS 返回前端产物 FS；build 产物存在（含 index.html）则用它，否则回退占位。
func resolveWebFS() fs.FS {
    if _, err := buildFS.Open("embed_build/index.html"); err == nil {
        sub, _ := fs.Sub(buildFS, "embed_build")
        return sub
    }
    sub, _ := fs.Sub(stubFS, "embed_stub")
    return sub
}
```

router 的 fileServer 改用 `resolveWebFS()`（逻辑不变）。

`Makefile`：

```makefile
build-web:
	cd web && npm ci && npm run build
	rm -rf internal/server/embed_build/*
	cp -r web/build/* internal/server/embed_build/
```

- [ ] **Step 3: 门禁 + 全量验证 + 提交**

Run: `make build-web && go build ./... && go test ./internal/server/ -count=1`
Commit: `feat(web): audit and org pages; embed frontend build into server binary`

---

## Part C：集成

### Task 11: 集成验证 + 文档清理

**Files:**
- Modify: `deploy/README.md`（WEB_DIR 移除、单二进制交付说明）、`README.md`（构建流程：make build-web → go build）
- 验证：`make build-web` → `go build ./...` → `go test ./internal/... ./cmd/... -count=1` 全绿 → 本地冒烟：起 server（临时 data dir）→ curl 首页返回 SvelteKit 产物（非占位页）

- [ ] **Step 1: 构建全链验证 + 首页冒烟**
- [ ] **Step 2: 文档更新**
- [ ] **Step 3: 提交**

Commit: `docs: single-binary build flow; remove WEB_DIR references`

---

## Self-Review 结论（编写时已对照 Phase 3 ledger 交接）

- **交接覆盖**：Resume（T1）、幂等回退+CAS（T2）、断线处置+reconcile（T3）、List filter DTO（T5）、selected 两阶段（T4 新发现缺口）、archive_status（T4）、checkpoints 双写（T1）、WEB_DIR 清理（T10/T11）、SSE pause 帧（T5）
- **Phase 3 其余 minor 分流**：mariadb 不对称、流事件乱序、confirmPause 幂等、旧大写 filter JSON 兼容、preview 内存上界（T5 部分）、operation_txs 空表（Phase 4 后视需要）、e2e 实跑（用户服务器，T11 文档交接）
- **类型一致性**：`executor.Resume(ctx, plan, cb)` 签名变化 → daemon（T1）→ server Resume 端点（T1）三端同步；`EvOpPaused` 常量（T5）→ SSE 客户端（T9）；`SaveStatementsAndSelect`（T5）→ Select handler；`resolveWebFS`（T10）→ router
- **风险**：SvelteKit 脚手架交互式命令在自动化环境可能受限（T6 报告实际方式）；shadcn-svelte init 需网络与交互（备选：手写基础组件，T6 决策）；selected 两阶段的状态机变更（ready→scanning）影响既有测试（T4 同步）；npm ci 首次拉取量大（可接受）

## 执行交接

计划保存于 `docs/superpowers/plans/2026-08-10-pitr-v3-phase4-web-backend.md`。执行方式同前：Subagent-Driven。完成后 PITR v3 全阶段交付（Phase 1-4）。
