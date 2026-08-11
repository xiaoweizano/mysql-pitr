package auth

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SQLiteUserStore is a SQLite-backed implementation of UserStore. Behaviour is
// equivalent to InMemoryUserStore: same error texts, same defaulting rules,
// timestamps persisted as RFC3339Nano strings (UTC).
type SQLiteUserStore struct {
	db *sql.DB
}

// NewSQLiteUserStore creates a UserStore backed by the given SQLite database.
// The caller is responsible for calling store.Migrate first.
func NewSQLiteUserStore(db *sql.DB) *SQLiteUserStore {
	return &SQLiteUserStore{db: db}
}

var _ UserStore = (*SQLiteUserStore)(nil)

// storeTimeLayout is the canonical timestamp layout for storage: RFC3339 with
// a fixed 9-digit UTC fraction, so lexicographic order of stored values
// matches chronological order. (time.RFC3339Nano trims trailing zeros, which
// breaks string comparison — e.g. ".5Z" sorts after ".500000001Z".)
const storeTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// Create stores a new user. Email must be unique.
func (s *SQLiteUserStore) Create(user *User) error {
	// Mirror the InMemory duplicate check (exact error text, checked before
	// defaulting so a failed Create leaves the input untouched).
	var one int
	err := s.db.QueryRow("SELECT 1 FROM users WHERE email = ?", user.Email).Scan(&one)
	switch {
	case err == nil:
		return fmt.Errorf("user with email %s already exists", user.Email)
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}

	if user.ID == "" {
		user.ID = generateID()
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now()
	}

	if _, err := s.db.Exec(
		"INSERT INTO users (id, email, hashed_password, created_at) VALUES (?, ?, ?, ?)",
		user.ID, user.Email, user.HashedPassword,
		user.CreatedAt.UTC().Format(storeTimeLayout),
	); err != nil {
		// Constraint race (concurrent insert of the same email): map to the
		// same error text as the sequential case.
		var two int
		if qerr := s.db.QueryRow("SELECT 1 FROM users WHERE email = ?", user.Email).Scan(&two); qerr == nil {
			return fmt.Errorf("user with email %s already exists", user.Email)
		}
		return err
	}
	return nil
}

// GetByID retrieves a user by its ID.
func (s *SQLiteUserStore) GetByID(id string) (*User, error) {
	return s.getBy("id", id)
}

// GetByEmail retrieves a user by its email address.
func (s *SQLiteUserStore) GetByEmail(email string) (*User, error) {
	return s.getBy("email", email)
}

func (s *SQLiteUserStore) getBy(column, value string) (*User, error) {
	var user User
	var createdAt string
	err := s.db.QueryRow(
		"SELECT id, email, hashed_password, created_at FROM users WHERE "+column+" = ?",
		value,
	).Scan(&user.ID, &user.Email, &user.HashedPassword, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("user not found: %s", value)
	}
	if err != nil {
		return nil, err
	}
	user.CreatedAt, err = parseStoreTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("user %s: parse created_at: %w", user.ID, err)
	}
	return &user, nil
}

// parseStoreTime decodes an RFC3339Nano timestamp; empty strings (schema
// defaults) decode to the zero time.
func parseStoreTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
