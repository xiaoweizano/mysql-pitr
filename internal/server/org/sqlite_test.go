package org

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/server/store"
)

func newTestOrgStore(t *testing.T) (*SQLiteOrgStore, *sql.DB) {
	t.Helper()
	db, err := store.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(db))
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLiteOrgStore(db), db
}

// TestSQLiteOrgStore_CreateAndGet covers Create (auto ID + CreatedAt) and Get.
func TestSQLiteOrgStore_CreateAndGet(t *testing.T) {
	s, _ := newTestOrgStore(t)

	org := &Organization{Name: "Acme"}
	require.NoError(t, s.Create(org))
	require.NotEmpty(t, org.ID)
	require.False(t, org.CreatedAt.IsZero())

	got, err := s.Get(org.ID)
	require.NoError(t, err)
	require.Equal(t, org.ID, got.ID)
	require.Equal(t, "Acme", got.Name)
	require.Equal(t, org.CreatedAt.UTC().Format(time.RFC3339Nano),
		got.CreatedAt.UTC().Format(time.RFC3339Nano))
}

func TestSQLiteOrgStore_Get_NotFound(t *testing.T) {
	s, _ := newTestOrgStore(t)

	_, err := s.Get("org_absent")
	require.EqualError(t, err, "organisation not found: org_absent")
}

// TestSQLiteOrgStore_ListAll verifies listing and the empty-slice contract.
func TestSQLiteOrgStore_ListAll(t *testing.T) {
	s, _ := newTestOrgStore(t)

	empty, err := s.ListAll()
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)

	require.NoError(t, s.Create(&Organization{Name: "One"}))
	require.NoError(t, s.Create(&Organization{Name: "Two"}))
	all, err := s.ListAll()
	require.NoError(t, err)
	require.Len(t, all, 2)
}

// TestSQLiteOrgStore_ListByUserID verifies membership-based org listing.
func TestSQLiteOrgStore_ListByUserID(t *testing.T) {
	s, _ := newTestOrgStore(t)

	none, err := s.ListByUserID("u-1")
	require.NoError(t, err)
	require.NotNil(t, none)
	require.Empty(t, none)

	orgA := &Organization{Name: "A"}
	orgB := &Organization{Name: "B"}
	require.NoError(t, s.Create(orgA))
	require.NoError(t, s.Create(orgB))
	require.NoError(t, s.AddMember(orgA.ID, "u-1", "admin"))
	require.NoError(t, s.AddMember(orgB.ID, "u-2", "member"))

	orgs, err := s.ListByUserID("u-1")
	require.NoError(t, err)
	require.Len(t, orgs, 1)
	require.Equal(t, orgA.ID, orgs[0].ID)
}

// TestSQLiteOrgStore_AddMember verifies add, org validation and duplicate
// rejection with InMemory-equivalent error text.
func TestSQLiteOrgStore_AddMember(t *testing.T) {
	s, _ := newTestOrgStore(t)

	org := &Organization{Name: "Acme"}
	require.NoError(t, s.Create(org))

	require.NoError(t, s.AddMember(org.ID, "u-1", "admin"))

	err := s.AddMember("org_missing", "u-1", "admin")
	require.EqualError(t, err, "organisation not found: org_missing")

	err = s.AddMember(org.ID, "u-1", "member")
	require.EqualError(t, err,
		"user u-1 is already a member of organisation "+org.ID)

	members, err := s.ListMembers(org.ID)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "u-1", members[0].UserID)
	require.Equal(t, "admin", members[0].Role)
	require.Equal(t, org.ID, members[0].OrgID)
	require.False(t, members[0].JoinedAt.IsZero())
}

// TestSQLiteOrgStore_RemoveMember verifies remove and its error paths.
func TestSQLiteOrgStore_RemoveMember(t *testing.T) {
	s, _ := newTestOrgStore(t)

	org := &Organization{Name: "Acme"}
	require.NoError(t, s.Create(org))
	require.NoError(t, s.AddMember(org.ID, "u-1", "member"))

	require.NoError(t, s.RemoveMember(org.ID, "u-1"))

	err := s.RemoveMember(org.ID, "u-1")
	require.EqualError(t, err,
		"user u-1 is not a member of organisation "+org.ID)

	err = s.RemoveMember("org_missing", "u-1")
	require.EqualError(t, err,
		"user u-1 is not a member of organisation org_missing")
}

// TestSQLiteOrgStore_ListMembers verifies the org-missing error and the
// empty-slice contract.
func TestSQLiteOrgStore_ListMembers(t *testing.T) {
	s, _ := newTestOrgStore(t)

	_, err := s.ListMembers("org_missing")
	require.EqualError(t, err, "organisation not found: org_missing")

	org := &Organization{Name: "Acme"}
	require.NoError(t, s.Create(org))

	members, err := s.ListMembers(org.ID)
	require.NoError(t, err)
	require.NotNil(t, members)
	require.Empty(t, members)

	require.NoError(t, s.AddMember(org.ID, "u-1", "admin"))
	require.NoError(t, s.AddMember(org.ID, "u-2", "member"))
	members, err = s.ListMembers(org.ID)
	require.NoError(t, err)
	require.Len(t, members, 2)
}

// TestSQLiteOrgStore_InviteFlow verifies CreateInvite/GetInviteByCode/
// DeleteInvite and the org-missing / invite-missing error paths.
func TestSQLiteOrgStore_InviteFlow(t *testing.T) {
	s, _ := newTestOrgStore(t)

	_, err := s.CreateInvite("org_missing", "u-1")
	require.EqualError(t, err, "organisation not found: org_missing")

	org := &Organization{Name: "Acme"}
	require.NoError(t, s.Create(org))

	invite, err := s.CreateInvite(org.ID, "u-1")
	require.NoError(t, err)
	require.NotEmpty(t, invite.Code)
	require.Equal(t, org.ID, invite.OrgID)
	require.Equal(t, "u-1", invite.CreatedBy)
	require.False(t, invite.CreatedAt.IsZero())

	got, err := s.GetInviteByCode(invite.Code)
	require.NoError(t, err)
	require.Equal(t, invite.Code, got.Code)
	require.Equal(t, org.ID, got.OrgID)
	require.Equal(t, "u-1", got.CreatedBy)
	require.Equal(t, invite.CreatedAt.UTC().Format(time.RFC3339Nano),
		got.CreatedAt.UTC().Format(time.RFC3339Nano))

	require.NoError(t, s.DeleteInvite(invite.Code))
	_, err = s.GetInviteByCode(invite.Code)
	require.EqualError(t, err, "invite not found: "+invite.Code)
}
