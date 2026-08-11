package store_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/server/store"
)

// TestOpenAndMigrate_IsIdempotent verifies Open creates a usable SQLite file,
// Migrate is idempotent, and every platform table is queryable afterwards.
func TestOpenAndMigrate_IsIdempotent(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	require.NoError(t, store.Migrate(db)) // second migration must be a no-op

	for _, tbl := range []string{"users", "orgs", "members", "agents", "operations",
		"operation_txs", "statements", "checkpoints", "archive_state", "audit_logs"} {
		var n int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+tbl).Scan(&n), "query %s", tbl)
	}
	require.NoError(t, db.Close())
}

// TestOpen_Memory verifies the :memory: path works for tests.
func TestOpen_Memory(t *testing.T) {
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	require.NoError(t, db.Close())
}
