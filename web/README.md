# web — PITR 平台前端

MySQL PITR 平台的 Web 控制台：SvelteKit 5 + Tailwind 4 + shadcn-svelte，纯 SPA
（`adapter-static` + `fallback: index.html`，`ssr = false`），运行时由 server
二进制同源托管（REST API 与 SSE 均在同一 origin）。

## 开发

```sh
npm ci
npm run dev        # http://localhost:5173（纯前端开发；API 走同源 /api，联调走下方构建流程）
```

> 开发服务器没有配置 API 代理：`src/lib/api/client.ts` 固定请求相对路径
> `/api`，联调（登录、向导、SSE 进度）时按「检查与构建」的流程构建并由
> server 二进制托管页面即可，无需代理。

## 检查与构建

```sh
npm run check      # svelte-kit sync + svelte-check（类型检查，提交前必须全绿）
npm run build      # 产物输出到 web/build/（index.html + _app/ + favicon.svg + robots.txt）
```

`npm run build` 的产物**不入库**，也不会被直接使用——构建 server 时由
`make build-web`（仓库根目录）复制进 `internal/server/embed_build/` 并
`go:embed` 进二进制，实现单二进制交付：

```sh
# 仓库根目录
make build-web     # cd web && npm ci && npm run build; cp -r web/build/* internal/server/embed_build/
go build ./cmd/server
```

> `internal/server/embed_build/` 的构建产物已被 `.gitignore` 忽略（仅保留
> `.gitkeep`）。若前端有改动，重新执行 `make build-web` 后重建 server
> 二进制，否则服务的是旧产物（或未构建时的占位页）。

## 目录结构

```
src/
  routes/          页面与路由（SPA：所有路由都落到 index.html 客户端渲染）
  lib/
    api/           REST API 客户端（含 JWT 注入、401 跳转）
    sse/           SSE 事件订阅（PITR 扫描/执行进度）
    components/    共享组件（TxTable / SqlPreview / ExecutePanel 等）
e2e/               端到端冒烟脚本
```
