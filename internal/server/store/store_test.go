package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/server/store"
)

// TestSaveCheckpoint_RoundTrip verifies SaveCheckpoint upserts into the
// checkpoints table: insert, then overwrite in place (still exactly one row),
// with the JSON error payload preserved verbatim.
func TestSaveCheckpoint_RoundTrip(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "cp.db"))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, store.Migrate(db))

	require.NoError(t, store.SaveCheckpoint(db, "op-1", 20, 100, `[{"statement":3,"err":"boom"}]`))

	var lastStmt, total int
	var errs string
	require.NoError(t, db.QueryRow(
		"SELECT last_statement, total, errors FROM checkpoints WHERE op_id = ?", "op-1").
		Scan(&lastStmt, &total, &errs))
	assert.Equal(t, 20, lastStmt)
	assert.Equal(t, 100, total)
	assert.Equal(t, `[{"statement":3,"err":"boom"}]`, errs)

	// Upsert semantics: the same op_id overwrites in place — still one row.
	require.NoError(t, store.SaveCheckpoint(db, "op-1", 50, 100, "[]"))
	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM checkpoints WHERE op_id = ?", "op-1").Scan(&n))
	assert.Equal(t, 1, n)
	require.NoError(t, db.QueryRow(
		"SELECT last_statement, total, errors FROM checkpoints WHERE op_id = ?", "op-1").
		Scan(&lastStmt, &total, &errs))
	assert.Equal(t, 50, lastStmt)
	assert.Equal(t, 100, total)
	assert.Equal(t, "[]", errs)

	// Distinct operations keep independent rows.
	require.NoError(t, store.SaveCheckpoint(db, "op-2", 7, 9, ""))
	require.NoError(t, db.QueryRow("SELECT count(*) FROM checkpoints").Scan(&n))
	assert.Equal(t, 2, n)
}

// TestOpenAndMigrate_IsIdempotent verifies Open creates a usable SQLite file,
// Migrate is idempotent, and every platform table is queryable afterwards.
func TestOpenAndMigrate_IsIdempotent(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	require.NoError(t, store.Migrate(db)) // second migration must be a no-op

	for _, tbl := range []string{"users", "orgs", "members", "agents", "invites", "operations",
		"operation_txs", "statements", "checkpoints", "archive_state", "audit_logs"} {
		var n int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+tbl).Scan(&n), "query %s", tbl)
	}

	// After a full migration the version watermark matches the current schema
	// version (schemaVersion in migrate.go), so a re-run is a no-op.
	var version int
	require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 3, version)
	// v3 column is present after the full migration chain.
	var n int
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM pragma_table_info('operations') WHERE name='target_tables'").Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, db.Close())
}

// TestOpen_Memory verifies the :memory: path works for tests.
func TestOpen_Memory(t *testing.T) {
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	require.NoError(t, db.Close())
}

// TestConcurrentWrites_NoBusy verifies the busy_timeout DSN parameter reaches
// every pooled connection, not just the first one the pool hands out. Two
// goroutines each pin their own connection and run interleaved write
// transactions (a write lock held across a small sleep guarantees the writers
// overlap); without a per-connection busy timeout the second writer would
// surface SQLITE_BUSY ("database is locked") instead of waiting.
func TestConcurrentWrites_NoBusy(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "busy.db"))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, store.Migrate(db))

	const (
		writers    = 2
		iterations = 100
	)

	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			// Each goroutine pins its own dedicated connection from the pool.
			conn, err := db.Conn(context.Background())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			<-start
			for n := 0; n < iterations; n++ {
				tx, err := conn.BeginTx(context.Background(), nil)
				if err != nil {
					errs <- err
					return
				}
				_, err = tx.Exec(
					"INSERT INTO users (id, email, hashed_password, created_at) VALUES (?, ?, ?, ?)",
					fmt.Sprintf("u%d-%d", writer, n),
					fmt.Sprintf("w%d-%d@example.com", writer, n),
					"hash",
					time.Now().Format(time.RFC3339Nano),
				)
				if err != nil {
					_ = tx.Rollback()
					errs <- err
					return
				}
				// Hold the write lock briefly so the two writers reliably
				// contend; busy_timeout makes the loser wait, not fail.
				time.Sleep(time.Millisecond)
				if err := tx.Commit(); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	close(start) // release both goroutines at the same instant
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "concurrent writers must not hit SQLITE_BUSY or any other write error")
	}

	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM users").Scan(&n))
	require.Equal(t, writers*iterations, n, "every concurrent write must be committed")
}

// TestMigrate_FailedVersion_RollsBack verifies migrations run transactionally
// per version: when a version script fails mid-way, the partial DDL (both the
// statements before the failing one and the user_version bump) is rolled back,
// leaving the database cleanly at the previous version. The conflict can then
// be resolved and the migration re-run — the pre-fix behaviour (partial v2
// applied, user_version stuck, every re-run failing on the duplicate column)
// would wedge the database permanently.
func TestMigrate_FailedVersion_RollsBack(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "migrate.db"))
	require.NoError(t, err)
	defer db.Close()

	// Pre-create agents with the v2 column registration_token so schema
	// version 2 fails at its second statement — after its first statement
	// (members.joined_at) has already executed inside the same transaction.
	_, err = db.Exec(`CREATE TABLE agents (
	  id TEXT PRIMARY KEY, org_id TEXT NOT NULL, name TEXT NOT NULL,
	  host TEXT NOT NULL DEFAULT '', mysql_version TEXT NOT NULL DEFAULT '',
	  status TEXT NOT NULL DEFAULT 'pending', cert_serial TEXT NOT NULL DEFAULT '',
	  last_seen TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
	  registration_token TEXT NOT NULL DEFAULT ''
	)`)
	require.NoError(t, err)

	err = store.Migrate(db)
	require.Error(t, err, "migration must fail on the conflicting v2 column")

	// The failed version never advanced user_version: v1 committed (== 1),
	// v2 rolled back.
	var version int
	require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 1, version)

	// The entire v2 script rolled back: neither the statement before the
	// failure (members.joined_at) nor the statements after it (agents.approved,
	// invites) are present.
	var n int
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM pragma_table_info('members') WHERE name='joined_at'").Scan(&n))
	require.Zero(t, n, "members.joined_at (v2 statement 1) must be rolled back")
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM pragma_table_info('agents') WHERE name='approved'").Scan(&n))
	require.Zero(t, n, "agents.approved (v2 statement 3) must not exist")
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='invites'").Scan(&n))
	require.Zero(t, n, "invites (v2 statement 4) must not exist")

	// The user_version pragma is transactional too: setting it inside a
	// transaction and rolling back must revert it.
	_, err = db.Exec("BEGIN; PRAGMA user_version = 9; ROLLBACK;")
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 1, version, "user_version set inside a rolled-back transaction must revert")

	// Resolve the conflict and re-run: the migration now completes, proving a
	// failed version no longer wedges the database.
	_, err = db.Exec("ALTER TABLE agents DROP COLUMN registration_token")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db), "re-run after resolving the conflict must succeed")
	require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 3, version)
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM pragma_table_info('members') WHERE name='joined_at'").Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='invites'").Scan(&n))
	require.Equal(t, 1, n)
}
