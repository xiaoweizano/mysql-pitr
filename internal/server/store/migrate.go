package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// schemaVersion is the latest schema version, tracked via PRAGMA user_version.
// Each migration appends one script that upgrades the previous version to the
// next; the versioned skeleton leaves room for future scripts.
const schemaVersion = 2

// schemaV1 is the full platform schema (version 1). Every statement is
// idempotent (CREATE TABLE IF NOT EXISTS), so re-running is a no-op.
//
// Conventions: timestamps are RFC3339Nano strings, JSON payloads are TEXT.
const schemaV1 = `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE,
  hashed_password TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orgs (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS members (
  org_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL,
  PRIMARY KEY (org_id, user_id)
);
CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY, org_id TEXT NOT NULL, name TEXT NOT NULL,
  host TEXT NOT NULL DEFAULT '', mysql_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending', cert_serial TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY, org_id TEXT NOT NULL, agent_id TEXT NOT NULL,
  type TEXT NOT NULL, mode TEXT NOT NULL, filter TEXT NOT NULL,
  selected_tx_ids TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL, created_by TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operation_txs (
  op_id TEXT NOT NULL, tx_index INTEGER NOT NULL,
  tx_id TEXT NOT NULL, gtid TEXT NOT NULL DEFAULT '', xid INTEGER NOT NULL DEFAULT 0,
  commit_time TEXT NOT NULL, schema_name TEXT NOT NULL DEFAULT '',
  tables TEXT NOT NULL DEFAULT '', row_count INTEGER NOT NULL DEFAULT 0,
  truncated INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'previewed',
  PRIMARY KEY (op_id, tx_index)
);
CREATE TABLE IF NOT EXISTS statements (
  op_id TEXT NOT NULL, tx_index INTEGER NOT NULL, stmt_index INTEGER NOT NULL,
  sql TEXT NOT NULL, tx_id TEXT NOT NULL, tx_order INTEGER NOT NULL DEFAULT 0,
  warnings TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending',
  PRIMARY KEY (op_id, stmt_index)
);
CREATE TABLE IF NOT EXISTS checkpoints (
  op_id TEXT PRIMARY KEY, last_statement INTEGER NOT NULL DEFAULT 0,
  total INTEGER NOT NULL DEFAULT 0, errors TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS archive_state (
  agent_id TEXT PRIMARY KEY, last_file TEXT NOT NULL DEFAULT '',
  last_pos INTEGER NOT NULL DEFAULT 0, last_gtid TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, op_id TEXT NOT NULL DEFAULT '',
  operator TEXT NOT NULL DEFAULT '', ts TEXT NOT NULL,
  org_id TEXT NOT NULL, agent_id TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT ''
);
`

// schemaV2 extends v1 with the columns/tables the domain stores (auth/org/
// agent/audit) need: member join time, agent registration/approval state, the
// invites table, and the audit entry detail columns. All ALTERs are additive
// (new columns have defaults), so v1 data is preserved; version tracking
// guarantees each statement runs exactly once per database.
const schemaV2 = `
ALTER TABLE members ADD COLUMN joined_at TEXT NOT NULL DEFAULT '';
ALTER TABLE agents ADD COLUMN registration_token TEXT NOT NULL DEFAULT '';
ALTER TABLE agents ADD COLUMN approved INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS invites (
  code TEXT PRIMARY KEY, org_id TEXT NOT NULL, created_by TEXT NOT NULL,
  created_at TEXT NOT NULL
);
ALTER TABLE audit_logs ADD COLUMN target_table TEXT NOT NULL DEFAULT '';
ALTER TABLE audit_logs ADD COLUMN recovery_time TEXT NOT NULL DEFAULT '';
ALTER TABLE audit_logs ADD COLUMN rows_affected INTEGER NOT NULL DEFAULT 0;
ALTER TABLE audit_logs ADD COLUMN status TEXT NOT NULL DEFAULT '';
`

// migrations maps each schema version to its upgrade script.
var migrations = map[int]string{
	1: schemaV1,
	2: schemaV2,
}

// Migrate idempotently brings db up to the current schema version: applied
// versions are skipped via PRAGMA user_version. Each version's script runs
// inside a single transaction (see applyVersion), so a mid-script failure
// rolls back the whole version — partial DDL and the user_version bump
// included — and the database is left cleanly at the previous version, ready
// for a re-run after the conflict is resolved. Safe to call repeatedly.
func Migrate(db *sql.DB) error {
	var current int
	if err := db.QueryRow("PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}
	for v := current + 1; v <= schemaVersion; v++ {
		ddl, ok := migrations[v]
		if !ok {
			return fmt.Errorf("store: no migration for schema version %d", v)
		}
		if err := applyVersion(db, v, ddl); err != nil {
			return err
		}
	}
	return nil
}

// applyVersion runs one version's upgrade script as a single all-or-nothing
// transaction on a pinned connection:
//
//	BEGIN; <ddl>; PRAGMA user_version = v; COMMIT;
//
// SQLite DDL is transactional, so on failure the driver stops at the failing
// statement and the explicit ROLLBACK reverts every statement of the script
// plus the version bump. Two details matter: the modernc driver does not roll
// back an open transaction by itself, and the ROLLBACK must run on the same
// connection that executed the script, so the connection is pinned via
// db.Conn rather than going through the pool.
func applyVersion(db *sql.DB, v int, ddl string) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: apply schema version %d: %w", v, err)
	}
	defer conn.Close()

	script := "BEGIN;\n" + ddl + "\nPRAGMA user_version = " + strconv.Itoa(v) + ";\nCOMMIT;"
	if _, err := conn.ExecContext(ctx, script); err != nil {
		// The transaction started by BEGIN is still open: the driver stops at
		// the failing statement, so clean it up before the connection returns
		// to the pool.
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return fmt.Errorf("store: apply schema version %d: %w", v, err)
	}
	return nil
}
