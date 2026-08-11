# E2E 集成测试（需真实 MySQL）

本目录下的 `//go:build integration` 测试需要连一台真实的、开启 GTID 与
ROW binlog 的 MySQL 实例，并在本机直接读取其 binlog 文件。它们**不参与**
普通的 `go test`（无 `-tags integration` 时不会被编译/执行）。

> 本机无 docker 时无法自己起实例，请在 docker-capable 主机 / 容器内按本
> README 运行（或复用已有符合前置条件的测试实例）。

## 前置条件

- MySQL 8.0，`gtid_mode=ON`，`binlog_format=ROW`
  （`binlog_row_image=FULL` 可选，但建议开启以便 reverse 生成精确逆向 SQL）
- 测试进程须能直接读取 binlog 文件（即 `E2E_BINLOG_DIR` 指向 mysqld 的
  datadir，进程须对该目录有读权限）
- 环境变量：

  | 变量 | 说明 |
  | --- | --- |
  | `E2E_MYSQL_DSN` | go-sql-driver DSN，**必须 tcp**（如 `root:pass@tcp(127.0.0.1:3306)/`），缺则 SKIP |
  | `E2E_BINLOG_DIR` | mysqld 的 binlog 目录（datadir），缺则 SKIP |

- MySQL 账号需具备 binlog 复制相关权限：

  ```sql
  CREATE USER IF NOT EXISTS 'pitr_e2e'@'%' IDENTIFIED BY 'pitr_e2e';
  GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pitr_e2e'@'%';
  FLUSH PRIVILEGES;
  ```

  DSN 形如：`pitr_e2e:pitr_e2e@tcp(127.0.0.1:3306)/`

## 实例要求

- **建议使用专用/新起的测试实例**：collector 的 e2e 在启动时会把 `SHOW
  BINARY LOGS` 的全部文件回填进临时归档目录，历史 binlog 越多，回填越慢，
  超时（60s）风险越高。
- **server-id 需唯一**：collector e2e 的归档 syncer 使用固定
  `server-id=424242`。若实例上已有别的同步进程占用该 id，请修改测试里的
  `e2eServerID` 常量（`internal/collector/e2e_test.go`）。

## 运行

在仓库根目录：

```sh
# binlog 端到端场景（delete/update/insert 回滚、大事务、DDL+DML、跨文件、
# 中途取消、GTID 精确定位）
go test -tags integration ./internal/binlog/ -run TestE2E -count=1

# schema fetcher 集成测试（只读 information_schema，不需 E2E_BINLOG_DIR）
go test -tags integration ./internal/binlog/ -run TestE2E_FetchSchema -count=1

# collector 归档循环 e2e（回填 + 续拉 + FLUSH LOGS 轮转 + 归档可扫描还原）
go test -tags integration ./internal/collector/ -run TestE2E_ArchiveLoop -count=1
```

- 环境变量缺省时测试直接 `t.Skip`，不会失败。
- `-run TestE2E` 会同时命中 `TestE2E_FetchSchema`（前缀匹配）；如需精确只跑
  场景，用 `-run TestE2E_`（带下划线）。
- 所有测试都在独立数据库（`e2e.*` / `e2e_collector.*` / `e2e_schema.*`）上
  操作并负责清理，可在共享实例上并行跑多个仓库副本。

## 时序说明（collector e2e，无 sleep）

collector 的 `TestE2E_ArchiveLoop` 用文件/状态轮询而非 `sleep` 保证确定性：

1. 先提交断言用的 DML（3 个事务），并拍下 `SHOW BINARY LOGS` 文件清单。
2. `Loop.Run` 的 reconcile 把全部既有文件回填为归档前缀（含 DDL，故当前
   打开文件的前缀含 FDE——规避「空文件在回填时只有 4 字节 magic 前缀」的
   退化情形）。
3. 轮询 `归档目录/<当前文件>.partial` 出现（append 已写内容）→ 确认循环
   已接流续拉。
4. `FLUSH LOGS` 触发轮转 → 轮询 `archive_state.json`（轮转封口 + 状态落盘）。
5. `cancel()` → `Run` 返回 nil，随后断言归档完整性与可扫描还原。
