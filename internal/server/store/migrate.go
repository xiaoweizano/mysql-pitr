package store

import (
	"database/sql"
	"fmt"
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
// versions are skipped via PRAGMA user_version, and every statement uses
// CREATE TABLE IF NOT EXISTS. Safe to call repeatedly.
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
		if _, err := db.Exec(ddl); err != nil {
			return fmt.Errorf("store: apply schema version %d: %w", v, err)
		}
		if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", v)); err != nil {
			return fmt.Errorf("store: set schema version %d: %w", v, err)
		}
	}
	return nil
}
