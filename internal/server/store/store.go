package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// Open opens (or creates) the SQLite database at path and enables WAL +
// foreign keys + a busy timeout. path=":memory:" is supported for tests.
//
// Note: a ":memory:" database is private to a single connection, so the pool
// is pinned to one connection (MaxOpenConns=1) to keep every Migrate/query on
// the same in-memory database. File databases keep the default pool.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	// WAL + foreign keys: single-writer/many-readers, restart-safe.
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: pragma %q: %w", pragma, err)
		}
	}
	return db, nil
}
