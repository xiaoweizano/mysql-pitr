package org

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Organization represents a named organisation.
type Organization struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

// Member represents a user's membership within an organisation.
type Member struct {
	UserID   string    `json:"userId"`
	OrgID    string    `json:"orgId"`
	Role     string    `json:"role"` // "admin" or "member"
	JoinedAt time.Time `json:"joinedAt"`
}

// Invite represents a pending invitation to join an organisation.
type Invite struct {
	Code      string    `json:"code"`
	OrgID     string    `json:"orgId"`
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
}

// OrgStore defines the interface for organisation persistence.
type OrgStore interface {
	Create(org *Organization) error
	Get(id string) (*Organization, error)
	Delete(id string) error
	ListAll() ([]*Organization, error)
	ListByUserID(userID string) ([]*Organization, error)
	AddMember(orgID, userID, role string) error
	RemoveMember(orgID, userID string) error
	ListMembers(orgID string) ([]Member, error)
	CreateInvite(orgID, createdBy string) (*Invite, error)
	GetInviteByCode(code string) (*Invite, error)
	DeleteInvite(code string) error
}

// InMemoryOrgStore is a thread-safe in-memory implementation of OrgStore.
type InMemoryOrgStore struct {
	mu       sync.RWMutex
	orgs     map[string]*Organization
	members  map[string][]Member // orgID -> members
	invites  map[string]*Invite  // code -> invite
}

// NewInMemoryOrgStore creates an initialised InMemoryOrgStore.
func NewInMemoryOrgStore() *InMemoryOrgStore {
	return &InMemoryOrgStore{
		orgs:    make(map[string]*Organization),
		members: make(map[string][]Member),
		invites: make(map[string]*Invite),
	}
}

// Create stores a new organisation.
func (s *InMemoryOrgStore) Create(org *Organization) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if org.ID == "" {
		org.ID = generateOrgID()
	}
	if org.CreatedAt.IsZero() {
		org.CreatedAt = time.Now()
	}
	s.orgs[org.ID] = org
	return nil
}

// Get retrieves an organisation by ID.
func (s *InMemoryOrgStore) Get(id string) (*Organization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[id]
	if !ok {
		return nil, fmt.Errorf("organisation not found: %s", id)
	}
	return org, nil
}

// Delete removes an organisation and all its members.
func (s *InMemoryOrgStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.orgs[id]; !ok {
		return fmt.Errorf("organisation not found: %s", id)
	}
	delete(s.orgs, id)
	delete(s.members, id)
	return nil
}

// ListAll returns all organisations.
func (s *InMemoryOrgStore) ListAll() ([]*Organization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var orgs []*Organization
	for _, o := range s.orgs {
		orgs = append(orgs, o)
	}
	if orgs == nil {
		return []*Organization{}, nil
	}
	return orgs, nil
}

// ListByUserID returns all organisations the given user is a member of.
func (s *InMemoryOrgStore) ListByUserID(userID string) ([]*Organization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var orgs []*Organization
	for orgID, members := range s.members {
		for _, m := range members {
			if m.UserID == userID {
				if org, ok := s.orgs[orgID]; ok {
					orgs = append(orgs, org)
				}
				break
			}
		}
	}
	if orgs == nil {
		return []*Organization{}, nil
	}
	return orgs, nil
}

// AddMember adds a user to an organisation with the given role.
func (s *InMemoryOrgStore) AddMember(orgID, userID, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.orgs[orgID]; !ok {
		return fmt.Errorf("organisation not found: %s", orgID)
	}

	// Check for duplicate membership.
	for _, m := range s.members[orgID] {
		if m.UserID == userID {
			return fmt.Errorf("user %s is already a member of organisation %s",
				userID, orgID)
		}
	}

	member := Member{
		UserID:   userID,
		OrgID:    orgID,
		Role:     role,
		JoinedAt: time.Now(),
	}
	s.members[orgID] = append(s.members[orgID], member)
	return nil
}

// RemoveMember removes a user from an organisation.
func (s *InMemoryOrgStore) RemoveMember(orgID, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	members := s.members[orgID]
	found := false
	for i, m := range members {
		if m.UserID == userID {
			s.members[orgID] = append(members[:i], members[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("user %s is not a member of organisation %s",
			userID, orgID)
	}
	return nil
}

// ListMembers returns all members of an organisation.
func (s *InMemoryOrgStore) ListMembers(orgID string) ([]Member, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.orgs[orgID]; !ok {
		return nil, fmt.Errorf("organisation not found: %s", orgID)
	}

	members := s.members[orgID]
	if members == nil {
		return []Member{}, nil
	}
	result := make([]Member, len(members))
	copy(result, members)
	return result, nil
}

// CreateInvite generates a new invitation code for an organisation.
func (s *InMemoryOrgStore) CreateInvite(orgID, createdBy string) (*Invite, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.orgs[orgID]; !ok {
		return nil, fmt.Errorf("organisation not found: %s", orgID)
	}

	code := generateInviteCode()
	invite := &Invite{
		Code:      code,
		OrgID:     orgID,
		CreatedBy: createdBy,
		CreatedAt: time.Now(),
	}
	s.invites[code] = invite
	return invite, nil
}

// GetInviteByCode retrieves an invitation by its code.
func (s *InMemoryOrgStore) GetInviteByCode(code string) (*Invite, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	invite, ok := s.invites[code]
	if !ok {
		return nil, fmt.Errorf("invite not found: %s", code)
	}
	return invite, nil
}

// DeleteInvite removes an invitation by its code.
func (s *InMemoryOrgStore) DeleteInvite(code string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.invites, code)
	return nil
}

func generateOrgID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "org_" + hex.EncodeToString(b)
}

func generateInviteCode() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
