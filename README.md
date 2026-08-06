# MySQL PITR — Point-in-Time Recovery via Binlog Flashback

[中文文档](./README.zh-CN.md)

**MySQL PITR** is a point-in-time recovery (flashback) tool for MySQL. It reads the server's binary logs (binlog), generates **reverse SQL** that undoes the changes, and lets you review and selectively execute the recovery — so accidental `DELETE` / `UPDATE` / `INSERT` mistakes can be rolled back without restoring a full backup.

## Highlights

- **Reverse SQL from binlog** — parses ROW-format binlog events and generates the exact statements that undo them (`DELETE` → `INSERT`, `INSERT` → `DELETE`, `UPDATE` → restore old values), emitted **newest-first (LIFO)** so dependent changes don't collide.
- **Preview first, execute on demand** — the web console only *generates* SQL from the binlog. You review every statement, check the ones you want, and click **Execute selected SQL** — only then is the database touched.
- **Agent + server architecture** — the `agent` runs on the MySQL host (reads the local binlog files, parses them with `mysqlbinlog`, executes rollbacks on its local connection); the `server` provides the web dashboard and REST API and never touches MySQL or binlog files itself. The two talk over a long-lived **mTLS WebSocket** with automatic certificate renewal.
- **Multi-org with agent approval** — organisations, members and invites; agents must be approved by an org admin before they can be used.
- **Audit log** — every operation (preview / cancel / completed) is recorded, with CSV export.
- **Docker Compose deployment** — one-shot `provision` service registers the agent, issues its mTLS certificate and writes the encrypted config.

## Architecture

```
┌─────────────┐   wss:// (mTLS)   ┌──────────────┐
│  MySQL host │ ◄───────────────► │   server     │
│  + agent    │   commands/       │  :8080 web   │
│  (binlogs)  │   responses       │  :9443 mTLS  │
└─────────────┘                   └──────────────┘
```

| Component | Role |
|---|---|
| **agent** | Deployed on the MySQL host. Reads the local binlog directory, parses binlog files with `mysqlbinlog`, answers preflight/parse/execute commands, executes rollback SQL on its local MySQL connection. Never sends MySQL credentials to the server. |
| **server** | Web dashboard + REST API + agent hub. Authenticates users (JWT), tracks organisations/agents/operations, drives agents over the WebSocket hub. Never accesses binlog files or MySQL directly. |
| **web** | React + Ant Design frontend served by the server. |

## Quick Start (Docker, host MySQL)

### Prerequisites

- Docker Engine 24+ and Docker Compose v2+
- A MySQL **8.0+** on the host with binary logging enabled:

  ```ini
  [mysqld]
  log-bin=mysql-bin
  binlog-format=ROW
  binlog-row-image=FULL
  ```

- A MySQL account the agent can use, allowed from the Docker bridge network (a `localhost`-only account will be rejected):

  ```sql
  CREATE USER 'pitr'@'%' IDENTIFIED BY '<password>';
  GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pitr'@'%';
  GRANT SELECT ON `<your-database>`.* TO 'pitr'@'%';
  FLUSH PRIVILEGES;
  ```

### Steps

```bash
git clone git@github.com:xiaoweizano/database-flashback-two.git
cd database-flashback-two

# 1. Configure the host MySQL connection (see .env.example)
cp .env.example .env
#    edit: MYSQL_PASSWORD, MYSQL_BINLOG_DIR_HOST (= the host MySQL data dir
#    that contains the mysql-bin.* files), optionally MYSQL_USER

# 2. Start all services (provision runs once, then the agent connects)
docker compose up -d

# 3. Open the dashboard
#    http://localhost:8080
```

**Default web login** — the provision step registers a fixed account and creates an organisation for it:

```
e2e-provision@example.com / e2e-pass-123
```

(credentials are hardcoded in `scripts/e2e-provision.sh`; change them for production)

### Verify

```bash
docker compose logs -f agent   # wait for the agent to connect and stay online
```

## Using the Web Console

1. **Organisations** — create your org, invite members.
2. **Agents** — the provisioned agent appears under the org; approve it if it shows as pending. Register additional agents with the **Register agent** button.
3. **PITR recovery wizard**:
   - Select an online, approved agent.
   - Enter the target table (`schema.table`) and the recovery time — changes **before** this moment will be undone.
   - Run the preflight check (binlog config, privileges).
   - **Preview** — the generated reverse SQL is listed in execution order. It is **not executed**.
   - **Check the statements you want** and click **Execute selected SQL** — this is the only moment the database changes. Progress and the restored row count are shown.
4. **Audit log** — every operation is recorded and exportable as CSV.

## CLI (flashback)

The agent binary also ships a standalone CLI that generates reverse SQL without any server:

```bash
mysql-pitr-agent flashback \
  --mysql-dsn 'pitr:Pitr123456!@tcp(127.0.0.1:3306)/mydb' \
  --target-table "mydb.orders" \
  --recovery-time "2026-07-25T13:39:00Z" \
  --dry-run            # print the reverse SQL; remove for direct execution
```

Run the agent as a daemon connected to the platform:

```bash
mysql-pitr-agent serve \
  --config /etc/agent/config.json \
  --passphrase '<passphrase>'   # see deploy/README.md for the config format
```

## Build from Source

Requirements: Go 1.22+, Node.js 20+.

```bash
# Agent + server binaries
go build -o bin/mysql-pitr-agent ./cmd/agent
go build -o bin/mysql-pitr-server ./cmd/server

# Frontend (must be built before the server image is useful)
cd web && npm ci && npm run build && cd ..

# Or build both Docker images
make docker-build
```

Run tests:

```bash
make test          # or: go test ./...
```

## Project Layout

```
cmd/agent       agent daemon (serve) + CLI (flashback, config)
cmd/server      server entrypoint
internal/
  parser        binlog event parsing & reverse-SQL generation
  rollback      batched reverse-SQL execution (checkpoints, progress)
  connector     MySQL connector & preflight checks
  checkpoint    rollback checkpoint persistence
  server/       REST handlers: auth, org, agent, pitr, audit
  ws/           mTLS WebSocket client/server hub, internal CA, cert renewal
scripts/        provisioning & e2e test scripts
web/            React + Ant Design frontend
deploy/         deployment guide (systemd, Windows, troubleshooting)
```

## Documentation

- [Deployment Guide](./deploy/README.md) — systemd / bare-metal / Windows deployment, config reference, troubleshooting
- [中文文档](./README.zh-CN.md)

## Troubleshooting (common)

| Symptom | Fix |
|---|---|
| `Access denied for user 'xxx'@'172.x.x.x' (using password: YES)` | The MySQL account is not allowed from the Docker network — grant it to `'xxx'@'%'` (see Quick Start). |
| Agent connects but stays unapproved | Approve it in the web console (Agents page). |
| `no row events found for table ... before ...` | The table had no binlog events before the recovery time (or its binlogs were purged). Pick a recovery time after the last change, and check `SHOW BINARY LOGS`. |
| `mysqlbinlog not found` | Install `mysql-client`/`mariadb-client`, or set `mysqlbinlog_path` in the agent config. |
