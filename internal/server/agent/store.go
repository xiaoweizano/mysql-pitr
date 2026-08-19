package agent

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// AgentRecord represents a registered agent within an organisation.
type AgentRecord struct {
	ID                string    `json:"id"`
	OrgID             string    `json:"orgId"`
	Hostname          string    `json:"hostname"`
	MySQLVersion      string    `json:"mySQLVersion,omitempty"`
	Status            string    `json:"status"` // online, offline, rejected, error
	LastSeen          time.Time `json:"lastSeen"`
	CreatedAt         time.Time `json:"createdAt"`
	CertSerial        string    `json:"certSerial,omitempty"`
	RegistrationToken string    `json:"registrationToken,omitempty"`
	Approved          bool      `json:"approved"`
}

// AgentStore defines the interface for agent persistence.
type AgentStore interface {
	Create(agent *AgentRecord) error
	Get(id string) (*AgentRecord, error)
	ListByOrg(orgID string) ([]*AgentRecord, error)
	Update(agent *AgentRecord) error
	Delete(id string) error
	// MarkAllOffline marks every agent offline. Called at server startup:
	// agents that reconnect are set online again by the hub lifecycle hook,
	// stale records (whose containers are gone) stay offline instead of
	// showing a stale "online" state.
	MarkAllOffline() error
}

// InMemoryAgentStore is a thread-safe in-memory implementation of AgentStore.
type InMemoryAgentStore struct {
	mu     sync.RWMutex
	agents map[string]*AgentRecord
}

// NewInMemoryAgentStore creates an initialised InMemoryAgentStore.
func NewInMemoryAgentStore() *InMemoryAgentStore {
	return &InMemoryAgentStore{
		agents: make(map[string]*AgentRecord),
	}
}

// Create stores a new agent record.
func (s *InMemoryAgentStore) Create(agent *AgentRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if agent.ID == "" {
		agent.ID = generateAgentID()
	}
	if agent.CreatedAt.IsZero() {
		agent.CreatedAt = time.Now()
	}
	if agent.Status == "" {
		agent.Status = "offline"
	}
	s.agents[agent.ID] = agent
	return nil
}

// Get retrieves an agent by its ID.
func (s *InMemoryAgentStore) Get(id string) (*AgentRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	agent, ok := s.agents[id]
	if !ok {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	return agent, nil
}

// ListByOrg returns all agents belonging to the given organisation.
func (s *InMemoryAgentStore) ListByOrg(orgID string) ([]*AgentRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*AgentRecord
	for _, a := range s.agents {
		if a.OrgID == orgID {
			result = append(result, a)
		}
	}
	if result == nil {
		return []*AgentRecord{}, nil
	}
	return result, nil
}

// Update replaces an existing agent record.
func (s *InMemoryAgentStore) Update(agent *AgentRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.agents[agent.ID]; !ok {
		return fmt.Errorf("agent not found: %s", agent.ID)
	}
	s.agents[agent.ID] = agent
	return nil
}

// Delete soft-deletes an agent record: unapproves it and marks it rejected.
// The record stays in the store so the UI can show it and allow re-approval.
func (s *InMemoryAgentStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	agent, ok := s.agents[id]
	if !ok {
		return fmt.Errorf("agent not found: %s", id)
	}
	agent.Approved = false
	agent.Status = "rejected"
	return nil
}

// MarkAllOffline marks every agent offline (see the interface doc).
func (s *InMemoryAgentStore) MarkAllOffline() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.agents {
		a.Status = "offline"
	}
	return nil
}

func generateAgentID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "agent_" + hex.EncodeToString(b)
}
