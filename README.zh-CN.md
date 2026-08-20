<div align="center">

<img src="docs/diagrams/logo.svg" width="96" alt="MySQL PITR logo"/>

# MySQL PITR 平台

基于 [go-mysql](https://github.com/go-mysql-org/go-mysql) 的 MySQL 时间点恢复（PITR）平台。它持续归档二进制日志，让你浏览任意时间点的事务，并生成逆向 SQL 撤销误操作——自带 Web 控制台、多 agent 架构与单二进制 server。

[![CI](https://img.shields.io/github/actions/workflow/status/xiaoweizano/mysql-pitr/ci.yml?branch=main&logo=github&logoColor=white&label=CI)](https://github.com/xiaoweizano/mysql-pitr/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com)
[![SvelteKit](https://img.shields.io/badge/SvelteKit-2-FF3E00?logo=svelte&logoColor=white)](https://kit.svelte.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/xiaoweizano/mysql-pitr?include_prereleases&logo=github&logoColor=white)](https://github.com/xiaoweizano/mysql-pitr/releases)
[![Stars](https://img.shields.io/github/stars/xiaoweizano/mysql-pitr?logo=github&logoColor=white)](https://github.com/xiaoweizano/mysql-pitr/stargazers)

[English](README.md) | 简体中文

</div>

## 功能特性

- **误删恢复**——由 DELETE 的行镜像生成逆向 INSERT
- **UPDATE 回滚**——逆向 UPDATE 恢复 Before 镜像值
- **指定时间恢复**——扫描到任意时间点的 binlog
- **指定事务恢复**——按 GTID / XID 精确定位并恢复指定事务
- **GTID 定位**——按 GTID 集过滤候选事务
- **大 binlog 增量归档**——本地归档目录保留完整的 binlog 镜像，不受 MySQL 清理窗口影响，数周后仍可恢复
- **多实例管理**——一个 server 通过 mTLS 管理多个 agent（每个 MySQL 主机一个）
- **检查点化执行**——中断后从最后已提交批次继续回滚

## 系统架构

三层：**SvelteKit SPA** 浏览器控制台、**单二进制 Go server**、以及运行在 MySQL 主机上的 **agent**。

![PITR v3 架构](docs/diagrams/pitr-architecture.png)

| 层 | 职责 |
|---|---|
| 浏览器 | SvelteKit SPA（内嵌于 server 二进制）：实例管理、归档健康度、5 步 PITR 向导、审计日志、组织管理 |
| server | REST API + SSE 进度、JWT 认证、agent mTLS WebSocket hub、SQLite 平台存储、`go:embed` 内嵌前端 |
| agent | go-mysql 采集引擎：`ParseFile` 解析本地 binlog、`binlogsyncer` 增量流式、事务聚合、原始 binlog 还原进归档目录、检查点化逆向 SQL 执行 |

### 操作状态机

```mermaid
stateDiagram-v2
    [*] --> created
    created --> scanning: POST /start
    created --> blocked: agent 离线
    scanning --> ready: scan_done
    scanning --> blocked: agent 断连
    ready --> executing: POST /execute
    ready --> cancelled: cancel
    executing --> paused: pause
    paused --> executing: resume
    executing --> done: op_done
    executing --> failed: op_error
    done --> [*]
    failed --> [*]
    cancelled --> [*]
    blocked --> [*]
```

完整流程：`start` 经 agent 扫描归档并流式回传事务元数据（与逆向 SQL，经 SSE）；你勾选事务或 SQL 行；`execute` 在 agent 上按批次检查点执行已批准语句；进度经 SSE 流式回传。

## 技术栈

| 层 | 选型 |
|---|---|
| binlog 引擎 | [go-mysql](https://github.com/go-mysql-org/go-mysql) v1.16.0（`replication.BinlogParser`、`binlogsyncer`） |
| 服务端语言 | Go 1.25 |
| HTTP / 路由 | [go-chi/chi](https://github.com/go-chi/chi) v5 |
| WebSocket | [gorilla/websocket](https://github.com/gorilla/websocket)（mTLS agent hub） |
| 认证 | [golang-jwt/jwt](https://github.com/golang-jwt/jwt) v5 |
| 平台存储 | [modernc.org/sqlite](https://gitlab.com/cznic/sqlite)（纯 Go，无 CGO） |
| Web 前端 | SvelteKit 2 / Svelte 5（adapter-static，`ssr=false` SPA） |
| UI 组件 | Tailwind CSS v4 + [shadcn-svelte](https://shadcn-svelte.com) |
| 测试 | Go `testing` + testify + sqlmock；前端 `svelte-check` / Playwright |

## 快速开始

### 环境要求

- Go 1.25+（go-mysql v1.16.0 经 goproxy.cn 或直接源获取）
- Node.js 20+（仅构建前端时需要；运行时前端已内嵌进二进制）
- MySQL 8.0（建议 `binlog_format=ROW`；GTID 恢复需要 `gtid_mode=ON`）

### 构建

```bash
make build-web   # 构建 SvelteKit 前端并拷入 embed_build
make build       # 产出 bin/mysql-pitr-server 与 bin/mysql-pitr-agent
```

或 Docker：`docker build --target server .` / `docker build --target agent .`

### Docker Compose 部署

仓库自带的 `docker-compose.yml` 可一键部署全栈，连接的是**宿主机 MySQL**（不在容器内另起 MySQL）。一次性 `provision` 服务完成 agent 注册、mTLS 证书签发与加密配置写入；之后 `agent`（含归档循环）与 `server` 常驻运行。

宿主机 MySQL 前置要求：

- `log-bin=mysql-bin`、`binlog-format=ROW`、`binlog-row-image=FULL`
- 允许容器网段访问的账号（如 `'pitr'@'%'`，授予 `SELECT, REPLICATION SLAVE, REPLICATION CLIENT`）

```bash
cp .env.example .env     # 填写 MYSQL_PASSWORD 与 MYSQL_BINLOG_DIR_HOST
docker compose up -d
```

- `server` → http://localhost:8080（注册账号、创建组织、在界面上审批 agent）
- `agent` → 只读挂载宿主机 binlog 目录（`MYSQL_BINLOG_DIR_HOST`）并运行归档循环；归档状态存于 `agent-data` 卷
- `ARCHIVE_SERVER_ID`（syncer id，每 agent 唯一）与 `ARCHIVE_RETENTION_DAYS`（0 = 永久保留）可在 `.env` 配置
- `PROVISION_EMAIL` / `PROVISION_PASSWORD` / `PROVISION_ORG`——一次性 `provision` 服务注册 agent 所用的账号（默认 `e2e-provision@example.com` / `e2e-pass-123` / `E2E Org`）。**agent 注册在该账号的组织下**——用这组凭据登录控制台才能看到它。在 `.env` 里改成你自己的账号即可挂到你名下，然后重新 provision：`docker compose down && docker volume rm <项目名>_agent-config && docker compose up -d`（旧 agent 记录留在旧组织下，可在控制台 reject 删除）。

前端已由 Dockerfile 多阶段构建在编译期内嵌进 server 二进制（`web/build` 拷入 `go:embed` 树）——无需单独的前端容器。

#### 低内存服务器（2C2G）部署

2 GB 内存**跑运行时**够用，但直接 `docker compose up -d --build` 可能把机器打挂：Go 编译/链接的内存峰值，叠加宿主机 MySQL 与仍在运行的旧容器，超出可用内存——内核假死，只能重启恢复。按以下顺序部署：

**1. 加 swap（一次性）**——没有 swap 时内存耗尽直接假死；有了 swap 构建只是慢一点：

```bash
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab   # 重启后自动挂载
free -h                                           # Swap 应显示 2.0Gi
```

> `fallocate failed: Text file busy` 说明 swapfile 已在启用中，无需重复操作（顺带检查 `/etc/fstab` 是否写了重复行）。

**2. 本机构建前端并上传**——compose 构建默认 `FRONTEND_FROM=prebuilt`，容器内不会跑 `npm ci` / Vite（2 GB 下必 OOM）。标准 `docker build --target server .` 仍是自包含构建（容器内编译前端），但需要 2 GB 以上内存。

```bash
cd web && npm run build                # 本机（Node 22+）
scp -r web/build root@<服务器>:/path/to/project/web/build
```

**3. 分步构建、分步启动**——`up -d --build` 会一边重建一边让旧容器占着内存：

```bash
docker compose down
docker compose build server            # 共享的 builder 阶段先填满缓存
docker compose build agent provision   # 基本全命中缓存
docker compose up -d
```

被 OOM 打断的构建不会把最后的层写入缓存，于是每次重试都在同一步重编——完整跑成功一次后，后续构建就快了。

**Docker 缓存清理**——构建缓存涨得很快（几轮迭代就能到数 GB）：

```bash
docker system df                            # 查看可回收空间
docker builder prune -a -f                  # 清掉全部构建缓存
docker builder prune --keep-storage 1GB -f  # 或只保留 1GB 缓存上限
```

> ⚠️ 栈停着的时候跑 `docker system prune -a -f` 会连已构建的 `mysql-pitr` 镜像一起删掉，下次启动就得全量重编。只清 **builder cache** 即可。

### 启动 server

```bash
export AGENT_DATA_DIR=./data      # SQLite + CA 存放目录（默认 ./data）
export LISTEN_ADDR=:8080          # Web 控制台 + REST
export AGENT_LISTEN_ADDR=:9443    # agent mTLS 端点
./bin/mysql-pitr-server
```

打开 <http://localhost:8080>，注册账号、创建组织，并在 agent 接入后审批它。

### 启动 agent（MySQL 主机上）

```bash
# 1. 生成加密配置（交互输入 MySQL 连接信息与归档目录）
./bin/mysql-pitr-agent config encrypt -o agent.json

# 2. 后台服务：连接 server + 启动归档循环
./bin/mysql-pitr-agent serve --config agent.json --passphrase '...'

# 3. 或命令行直接闪回（不依赖 server）
./bin/mysql-pitr-agent flashback --mysql-dsn 'user:pass@tcp(127.0.0.1:3306)/' \
  --target-table shop.orders --recovery-time '2026-08-01T00:00:00Z' --dry-run
```

agent 配置参考：

```jsonc
{
  "mysql": { "host": "127.0.0.1", "port": 3306, "user": "pitr", "password": "***", "database": "" },
  "server": { "url": "wss://server-host:9443/ws/agent", "cert_file": "client.pem", "key_file": "client-key.pem", "ca_file": "ca.pem" },
  "data_dir": "/var/lib/mysql-pitr",
  "archive": { "dir": "/var/lib/mysql-pitr/archive", "server_id": 424242, "retention_days": 30 }
}
```

MySQL 账号需要 `SELECT`、`REPLICATION SLAVE`、`REPLICATION CLIENT`（以及要恢复库的 `SELECT`）。agent 永远不会把 MySQL 凭据发送给 server。

## Web 控制台

**登录**——默认深色主题，侧边栏可切换浅色：

![登录页](docs/screenshots/login.png)

**实例**——agent 状态、审批流程、归档健康度：

![实例列表](docs/screenshots/instances.png)

**PITR 向导**——经 SSE 实时流式扫描事务：

![PITR 向导](docs/screenshots/pitr-wizard.png)

- **实例**——agent 列表、在线/离线状态、审批流程、每实例归档健康度
- **PITR 向导**——5 步：选恢复类型（误删恢复 / UPDATE 回滚 / 指定时间 / 指定事务 / GTID 定位）→ 设过滤（表、时间区间、GTID 集）→ 经 SSE 实时查看扫描 → 按事务分组审阅并勾选逆向 SQL → 执行并实时查看进度（pause / resume / cancel）
- **操作历史**——历史操作的状态、过滤摘要与审计条目
- **审计 / 组织**——审计日志（含 CSV 导出）；组织、成员、邀请

## 执行语义

- 逆向 SQL 在 **agent 端**生成（行镜像永不出 MySQL 主机）；server 只负责展示与编排。
- 执行按批次检查点推进；中断仅回滚当前批次，`resume` 从最后已提交批次继续（要求 agent 在线）。
- 单条语句失败记入 `errors` 并继续执行。
- 执行中 agent 断连会使操作置为 `blocked`（终态）——需新建操作重做；重复键错误按单条容忍。

## 测试

```bash
make test          # 单元测试（24 个包）
make test-race     # race detector（建议在 amd64 CI 上跑）
make lint          # golangci-lint
```

集成测试（`integration` tag，需真实 MySQL 8.0——见 `scripts/e2e/README.md`）：

```bash
go test -tags integration ./internal/binlog/ -run TestE2E        # 8 个回滚场景
go test -tags integration ./internal/collector/ -run TestE2E     # 归档循环完整性
go test -tags integration ./internal/server/ -run TestE2E        # server-agent-mysql 黄金路径
```

## 项目结构

```
cmd/agent          agent CLI：serve（守护进程）/ flashback / config
cmd/server         server 入口（Web + mTLS agent 端点）
internal/binlog    go-mysql 封装：Source 事件流、事务聚合、GTID
internal/scan      扫描模式（META_ONLY / WITH_SQL / SELECTED_SQL）
internal/archive   归档写入（raw 事件还原、封口验证、缺口检测）
internal/collector 归档循环（回填、增量、reconcile、状态持久化）
internal/reverse   逆向 SQL 纯函数库（agent 端生成）
internal/executor  检查点化批量执行 + 文件检查点存储
internal/stream    binlogsyncer 事件源封装
internal/daemon    agent 命令层（scan/execute/resume/cancel）
internal/ws        WebSocket 协议 + hub + mTLS CA + agent 客户端
internal/server    REST/SSE、SQLite 仓储、操作状态机、embed 前端
internal/connector MySQL 连接、schema 拉取、preflight
web                SvelteKit SPA（adapter-static）
docs/diagrams      架构图源文件（.mmd / .svg / .png / .excalidraw）
```

## 参考资料

- [go-mysql](https://github.com/go-mysql-org/go-mysql) —— binlog 解析与复制协议（Apache-2.0）
- [SvelteKit](https://kit.svelte.dev) —— MIT
- [shadcn-svelte](https://shadcn-svelte.com) —— MIT
- [Tailwind CSS](https://tailwindcss.com) —— MIT
- [modernc.org/sqlite](https://gitlab.com/cznic/sqlite) —— BSD-3-Clause（`github.com/modernc.org/sqlite` 镜像）
- [go-chi/chi](https://github.com/go-chi/chi) —— MIT
- [gorilla/websocket](https://github.com/gorilla/websocket) —— BSD-2-Clause
- [golang-jwt/jwt](https://github.com/golang-jwt/jwt) —— MIT
- [testify](https://github.com/stretchr/testify) —— MIT

## 已知限制

- 仅支持 **DML 逆向**（`DELETE` / `UPDATE`）；DDL 变更不在恢复范围内。
- 要求 MySQL 8.0+ 且 `binlog_format=ROW`、`binlog_row_image=FULL`；GTID 定位额外要求 `gtid_mode=ON`。
- 逆向 SQL 经 agent 在 MySQL 主机上执行；`resume` 要求 agent（及其 MySQL）在线。

## 许可证

[MIT](LICENSE) © 2026 xiaoweizano
