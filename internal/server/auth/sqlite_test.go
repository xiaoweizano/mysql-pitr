package auth

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/server/store"
)

func newTestUserStore(t *testing.T) (*SQLiteUserStore, *sql.DB) {
	t.Helper()
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLiteUserStore(db), db
}

// TestSQLiteUserStore_CreateAndGet covers Create (with auto-generated ID and
// CreatedAt), GetByID and GetByEmail round-trips.
func TestSQLiteUserStore_CreateAndGet(t *testing.T) {
	s, _ := newTestUserStore(t)

	user := &User{Email: "alice@example.com", HashedPassword: "hash-1"}
	require.NoError(t, s.Create(user))
	require.NotEmpty(t, user.ID)
	require.False(t, user.CreatedAt.IsZero())

	got, err := s.GetByID(user.ID)
	require.NoError(t, err)
	require.Equal(t, user.ID, got.ID)
	require.Equal(t, "alice@example.com", got.Email)
	require.Equal(t, "hash-1", got.HashedPassword)
	require.Equal(t, user.CreatedAt.UTC().Format(time.RFC3339Nano),
		got.CreatedAt.UTC().Format(time.RFC3339Nano))

	byEmail, err := s.GetByEmail("alice@example.com")
	require.NoError(t, err)
	require.Equal(t, user.ID, byEmail.ID)
}

// TestSQLiteUserStore_Create_PreservesExplicitFields verifies explicit ID and
// CreatedAt are persisted unchanged (RFC3339Nano round-trip).
func TestSQLiteUserStore_Create_PreservesExplicitFields(t *testing.T) {
	s, _ := newTestUserStore(t)

	createdAt := time.Date(2026, 8, 1, 12, 30, 45, 123456789, time.UTC)
	user := &User{ID: "fixed-id", Email: "bob@example.com",
		HashedPassword: "hash-2", CreatedAt: createdAt}
	require.NoError(t, s.Create(user))

	got, err := s.GetByID("fixed-id")
	require.NoError(t, err)
	require.Equal(t, "2026-08-01T12:30:45.123456789Z",
		got.CreatedAt.UTC().Format(time.RFC3339Nano))
}

// TestSQLiteUserStore_UniqueEmail verifies a duplicate email maps to the same
// error text as the InMemory implementation and does not clobber the original.
func TestSQLiteUserStore_UniqueEmail(t *testing.T) {
	s, _ := newTestUserStore(t)

	require.NoError(t, s.Create(&User{ID: "u1", Email: "a@x.com"}))
	err := s.Create(&User{ID: "u2", Email: "a@x.com"})
	require.Error(t, err)
	require.EqualError(t, err, "user with email a@x.com already exists")

	u, err := s.GetByEmail("a@x.com")
	require.NoError(t, err)
	require.Equal(t, "u1", u.ID)
}

func TestSQLiteUserStore_GetByID_NotFound(t *testing.T) {
	s, _ := newTestUserStore(t)

	_, err := s.GetByID("nope")
	require.EqualError(t, err, "user not found: nope")
}

func TestSQLiteUserStore_GetByEmail_NotFound(t *testing.T) {
	s, _ := newTestUserStore(t)

	_, err := s.GetByEmail("nobody@example.com")
	require.EqualError(t, err, "user not found: nobody@example.com")
}
