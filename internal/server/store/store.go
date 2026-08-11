package store

import (
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// dsnPragmas are the per-connection PRAGMAs passed as DSN query parameters.
// The modernc driver applies `_pragma=` values on every connection it opens
// (see newConn/applyQueryParams), so every pooled connection gets a busy
// timeout and foreign-key enforcement — running the pragmas once with
// db.Exec would only affect the single pooled connection that executed them,
// leaving the others without a busy timeout and prone to SQLITE_BUSY
// ("database is locked") under concurrent writes.
const dsnPragmas = "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"

// Open opens (or creates) the SQLite database at path and enables WAL +
// foreign keys + a busy timeout. path=":memory:" is supported for tests.
//
// Per-connection pragmas (busy_timeout, foreign_keys) travel in the DSN via
// `_pragma=` parameters so they apply to every pooled connection, not just
// the first one. journal_mode=WAL is a persistent property of the database
// file rather than a per-connection setting, so it is set once here.
//
// Note: a ":memory:" database is private to a single connection, so the pool
// is pinned to one connection (MaxOpenConns=1) to keep every Migrate/query on
// the same in-memory database. File databases keep the default pool.
func Open(path string) (*sql.DB, error) {
	dsn := path
	if strings.Contains(path, "?") {
		dsn += "&" + dsnPragmas
	} else {
		dsn += "?" + dsnPragmas
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	// WAL: single-writer/many-readers, restart-safe. A file property, so a
	// single idempotent run suffices (on a :memory: database it is a no-op).
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: pragma %q: %w", "journal_mode=WAL", err)
	}
	return db, nil
}
