# MySQL PITR — 基于 Binlog 闪回的时间点恢复

[English](./README.md)

**MySQL PITR** 是一个基于 MySQL 二进制日志（binlog）的闪回/时间点恢复工具。它读取 binlog，生成能够**撤销变更的逆向 SQL**，由你检查后**选择性执行**——误删、误改、误插的数据无需恢复整库备份即可回滚。

## 特性

- **从 binlog 生成逆向 SQL** — 解析 ROW 格式 binlog 事件，生成精确的撤销语句（`DELETE` → `INSERT`、`INSERT` → `DELETE`、`UPDATE` → 恢复旧值），并按**新→旧（LIFO）**顺序输出，避免先删后插等依赖事件互相冲突。
- **先预览、再执行** — Web 控制台只负责从 binlog *生成* SQL，**绝不自动执行**。你逐条检查、勾选需要恢复的语句，点击「**执行选中的 SQL**」后数据库才会真正变更。
- **Agent + Server 架构** — `agent` 部署在 MySQL 主机上（读取本地 binlog 文件、用 `mysqlbinlog` 解析、在本地连接上执行回滚）；`server` 提供 Web 控制台与 REST API，本身不接触 MySQL 和 binlog。二者通过长连接的 **mTLS WebSocket** 通信，证书自动续期。
- **组织多租户 + Agent 审批** — 支持组织、成员与邀请；agent 需经组织管理员审批后才能使用。
- **审计日志** — 每次操作（预览/取消/完成）都有记录，支持 CSV 导出。
- **Docker Compose 一键部署** — 一次性 `provision` 服务自动完成 agent 注册、mTLS 证书签发与加密配置写入。

## 架构

```
┌─────────────┐   wss:// (mTLS)   ┌──────────────┐
│  MySQL 主机  │ ◄───────────────► │   server     │
│  + agent    │   命令/响应        │  :8080 Web   │
│  (binlog)   │                   │  :9443 mTLS  │
└─────────────┘                   └──────────────┘
```

| 组件 | 职责 |
|---|---|
| **agent** | 部署在 MySQL 主机。读取本地 binlog 目录，用 `mysqlbinlog` 解析，响应预检/解析/执行命令，在本地 MySQL 连接上执行回滚 SQL。MySQL 凭据不出主机。 |
| **server** | Web 控制台 + REST API + agent 连接中枢。JWT 用户认证，管理组织/agent/操作，通过 WebSocket 中枢驱动 agent。不直接访问 binlog 或 MySQL。 |
| **web** | SvelteKit 前端，通过 `make build-web` 嵌入 server 二进制。 |

## 快速开始（Docker + 宿主机 MySQL）

### 前置条件

- Docker Engine 24+ 与 Docker Compose v2+
- 宿主机上的 MySQL **8.0+**，已开启二进制日志：

  ```ini
  [mysqld]
  log-bin=mysql-bin
  binlog-format=ROW
  binlog-row-image=FULL
  ```

- 供 agent 使用的 MySQL 账号，且允许来自 Docker 网段（只建 `localhost` 的账号会被拒绝）：

  ```sql
  CREATE USER 'pitr'@'%' IDENTIFIED BY '<密码>';
  GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pitr'@'%';
  GRANT SELECT ON `<你的库>`.* TO 'pitr'@'%';
  FLUSH PRIVILEGES;
  ```

### 步骤

```bash
git clone git@github.com:xiaoweizano/database-flashback.git
cd database-flashback

# 1. 配置宿主机 MySQL 连接（参考 .env.example）
cp .env.example .env
#    修改：MYSQL_PASSWORD、MYSQL_BINLOG_DIR_HOST（宿主机 MySQL 数据目录，
#    即包含 mysql-bin.* 文件的目录），可按需改 MYSQL_USER

# 2. 启动全部服务（provision 只跑一次，之后 agent 自动连接）
docker compose up -d

# 3. 打开控制台
#    http://localhost:8080
```

**默认 Web 登录账号** — provision 步骤会注册一个固定账号并创建组织：

```
e2e-provision@example.com / e2e-pass-123
```

（凭据硬编码在 `scripts/e2e-provision.sh`，生产环境请修改）

### 验证

```bash
docker compose logs -f agent   # 等待 agent 上线并保持在线
```

## Web 控制台使用步骤

1. **组织** — 创建组织、邀请成员。
2. **代理** — provision 的 agent 会出现在组织下；若显示待审批，点击审批。也可用「注册代理」按钮添加新 agent。
3. **PITR 恢复向导**：
   - 选择在线且已审批的 agent。
   - 输入目标表（`库名.表名`）与恢复时间——该时间点**之前**的变更将被撤销。
   - 执行预检（binlog 配置、权限检查）。
   - **预览** — 生成的全部逆向 SQL 按执行顺序列出，此时**不会执行**。
   - **勾选需要执行的语句**，点击「**执行选中的 SQL**」——这是唯一会改动数据库的时刻。执行中显示进度与恢复行数。
4. **审计日志** — 每次操作均有记录，可导出 CSV。

## 执行语义与恢复

- **执行中 agent 断线** — 若 agent 在 `scanning`/`executing` 期间掉线，操作会进入 `blocked`（终态），需要新建操作重新扫描并执行仍要恢复的语句。`paused` 操作与 agent 侧检查点可跨 server 重启保留，但 `resume` 仅在 agent 在线时有效。
- **重复执行依赖单条错误容忍** — 带主键的逆向 SQL 可能与已恢复的行冲突，产生 duplicate-key 错误，逐条记录后继续执行。`resume` 不会重跑 pause 前已提交的 batch，而是从 agent 持久化的检查点继续。
- **server 的 `checkpoints` 表为预留数据** — 检查点会双写留存备用；当前没有任何读取方，`resume` 使用 agent 本地的检查点文件。

## CLI（flashback）

agent 二进制还自带独立 CLI，无需 server 即可生成逆向 SQL：

```bash
mysql-pitr-agent flashback \
  --mysql-dsn 'pitr:Pitr123456!@tcp(127.0.0.1:3306)/mydb' \
  --target-table "mydb.orders" \
  --recovery-time "2026-07-25T13:39:00Z" \
  --dry-run            # 只打印逆向 SQL；去掉该参数则直接执行
```

以守护进程方式连接平台：

```bash
mysql-pitr-agent serve \
  --config /etc/agent/config.json \
  --passphrase '<口令>'   # 配置格式见 deploy/README.md
```

## 从源码构建

依赖：Go 1.25+（见 go.mod）、Node.js 20+。

```bash
# Agent 与 server 二进制
go build -o bin/mysql-pitr-agent ./cmd/agent
go build -o bin/mysql-pitr-server ./cmd/server

# 前端 — 内嵌进 server 二进制（单二进制构建）
# `make build-web` 执行 `npm ci` + `npm run build`，然后把
# `web/build/` 拷贝到 `internal/server/embed_build/`，
# 下次 `go build ./cmd/server` 时前端会被编译进二进制。
make build-web

# 或直接构建两个 Docker 镜像——Dockerfile 的多阶段构建会自行构建并内嵌前端，
# 无需先执行 `make build-web`
make docker-build
```

运行测试：

```bash
make test          # 或：go test ./...
```

## 目录结构

```
cmd/agent       agent 守护进程（serve）+ CLI（flashback、config）
cmd/server      server 入口
internal/
  parser        binlog 事件解析与逆向 SQL 生成
  rollback      分批逆向 SQL 执行（检查点、进度）
  connector     MySQL 连接器与预检
  checkpoint    回滚检查点持久化
  server/       REST 处理器：auth、org、agent、pitr、audit
  ws/           mTLS WebSocket 客户端/服务端中枢、内置 CA、证书续期
scripts/        provision 与 e2e 测试脚本
web/            SvelteKit 前端（经 `make build-web` 内嵌进 server 二进制）
deploy/         部署指南（systemd、Windows、故障排查）
```

## 文档

- [部署指南](./deploy/README.md) — systemd / 裸机 / Windows 部署、配置参考、故障排查
- [English README](./README.md)

## 常见问题

| 现象 | 解决办法 |
|---|---|
| `Access denied for user 'xxx'@'172.x.x.x' (using password: YES)` | MySQL 账号未授权 Docker 网段——授权到 `'xxx'@'%'`（见快速开始）。 |
| agent 已连接但一直未审批 | 在控制台「代理」页面点击审批。 |
| `no row events found for table ... before ...` | 恢复时间点之前该表没有 binlog 事件（或其 binlog 已被清理）。把恢复时间选在最后一次变更之后，并用 `SHOW BINARY LOGS` 确认 binlog 仍覆盖该时间段。 |
| `mysqlbinlog not found` | 安装 `mysql-client`/`mariadb-client`，或在 agent 配置中设置 `mysqlbinlog_path`。 |
