package org

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SQLiteOrgStore is a SQLite-backed implementation of OrgStore. Behaviour is
// equivalent to InMemoryOrgStore: same error texts, same defaulting rules,
// timestamps persisted as RFC3339Nano strings (UTC).
type SQLiteOrgStore struct {
	db *sql.DB
}

// NewSQLiteOrgStore creates an OrgStore backed by the given SQLite database.
// The caller is responsible for calling store.Migrate first.
func NewSQLiteOrgStore(db *sql.DB) *SQLiteOrgStore {
	return &SQLiteOrgStore{db: db}
}

var _ OrgStore = (*SQLiteOrgStore)(nil)

// storeTimeLayout is the canonical timestamp layout for storage: RFC3339 with
// a fixed 9-digit UTC fraction, so lexicographic order of stored values
// matches chronological order. (time.RFC3339Nano trims trailing zeros, which
// breaks string comparison — e.g. ".5Z" sorts after ".500000001Z".)
const storeTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// Create stores a new organisation.
func (s *SQLiteOrgStore) Create(org *Organization) error {
	if org.ID == "" {
		org.ID = generateOrgID()
	}
	if org.CreatedAt.IsZero() {
		org.CreatedAt = time.Now()
	}
	_, err := s.db.Exec(
		"INSERT INTO orgs (id, name, created_at) VALUES (?, ?, ?)",
		org.ID, org.Name, org.CreatedAt.UTC().Format(storeTimeLayout),
	)
	return err
}

// Get retrieves an organisation by ID.
func (s *SQLiteOrgStore) Get(id string) (*Organization, error) {
	var org Organization
	var createdAt string
	err := s.db.QueryRow(
		"SELECT id, name, created_at FROM orgs WHERE id = ?", id,
	).Scan(&org.ID, &org.Name, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("organisation not found: %s", id)
	}
	if err != nil {
		return nil, err
	}
	org.CreatedAt, err = parseStoreTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("org %s: parse created_at: %w", org.ID, err)
	}
	return &org, nil
}

// Delete removes an organisation and all its members and invites.
func (s *SQLiteOrgStore) Delete(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var one int
	err = tx.QueryRow("SELECT 1 FROM orgs WHERE id = ?", id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("organisation not found: %s", id)
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec("DELETE FROM members WHERE org_id = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM invites WHERE org_id = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM orgs WHERE id = ?", id); err != nil {
		return err
	}
	return tx.Commit()
}

// ListAll returns all organisations.
func (s *SQLiteOrgStore) ListAll() ([]*Organization, error) {
	rows, err := s.db.Query("SELECT id, name, created_at FROM orgs ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orgs, err := scanOrgs(rows)
	if err != nil {
		return nil, err
	}
	if orgs == nil {
		return []*Organization{}, nil
	}
	return orgs, nil
}

// ListByUserID returns all organisations the given user is a member of.
func (s *SQLiteOrgStore) ListByUserID(userID string) ([]*Organization, error) {
	rows, err := s.db.Query(
		`SELECT o.id, o.name, o.created_at
		   FROM orgs o
		   JOIN members m ON m.org_id = o.id
		  WHERE m.user_id = ?
		  ORDER BY o.rowid`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orgs, err := scanOrgs(rows)
	if err != nil {
		return nil, err
	}
	if orgs == nil {
		return []*Organization{}, nil
	}
	return orgs, nil
}

// AddMember adds a user to an organisation with the given role.
func (s *SQLiteOrgStore) AddMember(orgID, userID, role string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var one int
	err = tx.QueryRow("SELECT 1 FROM orgs WHERE id = ?", orgID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("organisation not found: %s", orgID)
	}
	if err != nil {
		return err
	}

	var two int
	err = tx.QueryRow(
		"SELECT 1 FROM members WHERE org_id = ? AND user_id = ?", orgID, userID,
	).Scan(&two)
	if err == nil {
		return fmt.Errorf("user %s is already a member of organisation %s",
			userID, orgID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	_, err = tx.Exec(
		"INSERT INTO members (org_id, user_id, role, joined_at) VALUES (?, ?, ?, ?)",
		orgID, userID, role, time.Now().UTC().Format(storeTimeLayout),
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// RemoveMember removes a user from an organisation.
func (s *SQLiteOrgStore) RemoveMember(orgID, userID string) error {
	res, err := s.db.Exec(
		"DELETE FROM members WHERE org_id = ? AND user_id = ?", orgID, userID,
	)
	if err != nil {
		return err
	}
	// Like the InMemory version, a missing organisation surfaces as the
	// "not a member" error (there are simply no rows to delete).
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("user %s is not a member of organisation %s",
			userID, orgID)
	}
	return nil
}

// ListMembers returns all members of an organisation.
func (s *SQLiteOrgStore) ListMembers(orgID string) ([]Member, error) {
	var one int
	err := s.db.QueryRow("SELECT 1 FROM orgs WHERE id = ?", orgID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("organisation not found: %s", orgID)
	}
	if err != nil {
		return nil, err
	}

	rows, err := s.db.Query(
		"SELECT org_id, user_id, role, joined_at FROM members WHERE org_id = ? ORDER BY rowid",
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []Member
	for rows.Next() {
		var m Member
		var joinedAt string
		if err := rows.Scan(&m.OrgID, &m.UserID, &m.Role, &joinedAt); err != nil {
			return nil, err
		}
		parsed, perr := parseStoreTime(joinedAt)
		if perr != nil {
			return nil, fmt.Errorf("org %s: parse joined_at: %w", orgID, perr)
		}
		m.JoinedAt = parsed
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if members == nil {
		return []Member{}, nil
	}
	return members, nil
}

// CreateInvite generates a new invitation code for an organisation.
func (s *SQLiteOrgStore) CreateInvite(orgID, createdBy string) (*Invite, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var one int
	err = tx.QueryRow("SELECT 1 FROM orgs WHERE id = ?", orgID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("organisation not found: %s", orgID)
	}
	if err != nil {
		return nil, err
	}

	invite := &Invite{
		Code:      generateInviteCode(),
		OrgID:     orgID,
		CreatedBy: createdBy,
		CreatedAt: time.Now(),
	}
	_, err = tx.Exec(
		"INSERT INTO invites (code, org_id, created_by, created_at) VALUES (?, ?, ?, ?)",
		invite.Code, invite.OrgID, invite.CreatedBy,
		invite.CreatedAt.UTC().Format(storeTimeLayout),
	)
	if err != nil {
		return nil, err
	}
	return invite, tx.Commit()
}

// GetInviteByCode retrieves an invitation by its code.
func (s *SQLiteOrgStore) GetInviteByCode(code string) (*Invite, error) {
	var invite Invite
	var createdAt string
	err := s.db.QueryRow(
		"SELECT code, org_id, created_by, created_at FROM invites WHERE code = ?",
		code,
	).Scan(&invite.Code, &invite.OrgID, &invite.CreatedBy, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("invite not found: %s", code)
	}
	if err != nil {
		return nil, err
	}
	invite.CreatedAt, err = parseStoreTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("invite %s: parse created_at: %w", code, err)
	}
	return &invite, nil
}

// DeleteInvite removes an invitation by its code. Deleting a missing invite is
// a no-op, matching the InMemory implementation.
func (s *SQLiteOrgStore) DeleteInvite(code string) error {
	_, err := s.db.Exec("DELETE FROM invites WHERE code = ?", code)
	return err
}

func scanOrgs(rows *sql.Rows) ([]*Organization, error) {
	var orgs []*Organization
	for rows.Next() {
		var org Organization
		var createdAt string
		if err := rows.Scan(&org.ID, &org.Name, &createdAt); err != nil {
			return nil, err
		}
		parsed, err := parseStoreTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("org %s: parse created_at: %w", org.ID, err)
		}
		org.CreatedAt = parsed
		orgs = append(orgs, &org)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orgs, nil
}

// parseStoreTime decodes an RFC3339Nano timestamp; empty strings (schema
// defaults) decode to the zero time.
func parseStoreTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
