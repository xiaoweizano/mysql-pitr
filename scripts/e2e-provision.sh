#!/bin/sh
# ============================================================================
# Provisioning: register an agent, issue its mTLS certificate from the
# server's internal CA, and write the encrypted agent config.
#
# Runs once inside the `provision` compose service (agent image). Expects:
#   - server:8080 reachable (REST API)
#   - /var/lib/mysql-pitr/ca.json (server CA, created on server startup)
#   - /etc/agent writable (agent-config volume)
#   - MYSQL_HOST / MYSQL_PASSWORD etc. set by compose (host MySQL)
# ============================================================================
set -e

# Provisioning tools are preinstalled in the agent image (debian base). Fall
# back to installing them on the fly when this script runs on other bases.
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 openssl curl jq >/dev/null 2>&1 || true
elif command -v apt-get >/dev/null 2>&1; then
  apt-get update -qq >/dev/null 2>&1 && \
    apt-get install -y -qq --no-install-recommends python3 openssl curl jq >/dev/null 2>&1 || true
fi

SERVER_URL="http://server:8080"
DATA_DIR="/var/lib/mysql-pitr"
CONFIG_DIR="/etc/agent"
PASSPHRASE="${PITR_PASSPHRASE:-pitr-test}"

# 幂等:agent-config 卷在 docker compose down 后仍保留,config.json 存在即说明
# 此前已注册 agent 并签发过证书。每次 up 都无条件注册会在 server 库里留下重复
# agent 记录(旧容器已删除的 agent 显示为 offline)。如需强制重新注册:
#   docker compose down && docker volume rm <项目名>_agent-config
if [ -f "$CONFIG_DIR/config.json" ]; then
  echo "[provision] config.json exists — agent already provisioned, skipping"
  exit 0
fi

# Host MySQL connection (configured via .env, see docker-compose.yml).
MYSQL_HOST="${MYSQL_HOST:-host.docker.internal}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:?set MYSQL_PASSWORD in .env}"
MYSQL_DATABASE="${MYSQL_DATABASE:-mysql}"
MYSQL_BINLOG_DIR="${MYSQL_BINLOG_DIR:-/var/lib/mysql}"

echo "[provision] waiting for server CA..."
for i in $(seq 1 60); do
  [ -f "$DATA_DIR/ca.json" ] && break
  sleep 1
done
if [ ! -f "$DATA_DIR/ca.json" ]; then
  echo "[provision] ERROR: server CA not found after 60s"
  exit 1
fi

echo "[provision] extracting CA material..."
python3 - "$DATA_DIR/ca.json" "$CONFIG_DIR" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
out = sys.argv[2]
with open(out + "/ca.pem", "w") as f:
    f.write(data["caCert"])
with open(out + "/ca-key.pem", "w") as f:
    f.write(data["caKey"])
print("[provision] wrote ca.pem and ca-key.pem")
PY

echo "[provision] registering agent via API..."
# Fixed credentials shared with scripts/e2e-test.sh so the host-side script
# can log in as the same user and see this org and agent.
EMAIL="e2e-provision@example.com"
PASS="e2e-pass-123"

curl -fsS -X POST "$SERVER_URL/api/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" >/dev/null 2>&1 || true

TOKEN=$(curl -fsS -X POST "$SERVER_URL/api/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" | jq -r .token)

ORG_ID=$(curl -fsS -X POST "$SERVER_URL/api/orgs" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"E2E Org"}' | jq -r .organization.id)

AGENT_ID=$(curl -fsS -X POST "$SERVER_URL/api/agents/register" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"orgId\":\"$ORG_ID\",\"hostname\":\"agent-1\"}" | jq -r .agent.id)

echo "[provision] agent id: $AGENT_ID"

# The PITR wizard only lists approved agents, so approve immediately after
# registration (registration already required the platform admin token).
echo "[provision] approving agent..."
curl -fsS -X POST "$SERVER_URL/api/agents/$AGENT_ID/approve" \
  -H "Authorization: Bearer $TOKEN" >/dev/null

echo "[provision] issuing client certificate (CN=$AGENT_ID)...";
openssl ecparam -name prime256v1 -genkey -noout -out "$CONFIG_DIR/client-key.pem"
openssl req -new -key "$CONFIG_DIR/client-key.pem" -subj "/CN=$AGENT_ID" -out /tmp/client.csr
openssl x509 -req -in /tmp/client.csr \
  -CA "$CONFIG_DIR/ca.pem" -CAkey "$CONFIG_DIR/ca-key.pem" -CAcreateserial \
  -out "$CONFIG_DIR/client.pem" -days 90 -sha256 2>/dev/null

echo "[provision] writing encrypted agent config..."
cat > "$CONFIG_DIR/plain.json" <<EOF
{
  "mysql": {
    "host": "$MYSQL_HOST",
    "port": $MYSQL_PORT,
    "user": "$MYSQL_USER",
    "password": "$MYSQL_PASSWORD",
    "database": "$MYSQL_DATABASE"
  },
  "server": {
    "url": "wss://server:9443/ws/agent",
    "cert_file": "$CONFIG_DIR/client.pem",
    "key_file": "$CONFIG_DIR/client-key.pem",
    "ca_file": "$CONFIG_DIR/ca.pem"
  },
  "data_dir": "/var/lib/mysql-pitr",
  "binlog_dir": "$MYSQL_BINLOG_DIR",
  "archive": {
    "dir": "/var/lib/mysql-pitr/archive",
    "server_id": ${ARCHIVE_SERVER_ID:-424242},
    "retention_days": ${ARCHIVE_RETENTION_DAYS:-0}
  }
}
EOF

mysql-pitr-agent config encrypt \
  --input "$CONFIG_DIR/plain.json" \
  --output "$CONFIG_DIR/config.json" \
  --passphrase "$PASSPHRASE"

rm -f "$CONFIG_DIR/plain.json" "$CONFIG_DIR/ca-key.pem" "$CONFIG_DIR/ca-key.pem.srl"

# Verify the MySQL server is reachable from the Docker network. The v3 agent
# no longer ships mysql client tools (go-mysql does the binlog parsing), so
# use a plain python3 TCP probe. A reachable server with wrong credentials
# will surface in the agent logs via friendlyConnError with the grant
# guidance below — this check catches the common container networking failure
# (host unreachable / port blocked) early.
echo "[provision] verifying MySQL reachability as '$MYSQL_USER'..."
if ! python3 - "$MYSQL_HOST" "$MYSQL_PORT" <<'PYEOF'
import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=5).close()
except OSError:
    sys.exit(1)
PYEOF
then
  echo "[provision] ERROR: cannot connect to MySQL at $MYSQL_HOST:$MYSQL_PORT as user '$MYSQL_USER'."
  echo "[provision] The agent needs a MySQL account allowed from the Docker network."
  echo "[provision] On the MySQL host, run as root:"
  echo "  CREATE USER IF NOT EXISTS '$MYSQL_USER'@'%' IDENTIFIED BY '<password>';"
  echo "  ALTER USER '$MYSQL_USER'@'%' IDENTIFIED BY '<password>';   # if the user already exists"
  echo "  GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '$MYSQL_USER'@'%';"
  echo "  GRANT SELECT ON \`$MYSQL_DATABASE\`.* TO '$MYSQL_USER'@'%';"
  echo "  FLUSH PRIVILEGES;"
  echo "[provision] and make sure MYSQL_USER/MYSQL_PASSWORD in .env match."
  exit 1
fi
echo "[provision] MySQL connectivity OK"

echo "[provision] done — agent $AGENT_ID ready to start"
