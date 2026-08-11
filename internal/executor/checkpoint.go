package executor

import (
	"fmt"
	"sync"
)

// InMemoryCheckpointStore 是 CheckpointStore 的内存实现，仅用于测试。
type InMemoryCheckpointStore struct {
	mu   sync.Mutex
	data map[string]Checkpoint
}

func NewInMemoryCheckpointStore() *InMemoryCheckpointStore {
	return &InMemoryCheckpointStore{data: map[string]Checkpoint{}}
}

func (s *InMemoryCheckpointStore) Load(operationID string) (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.data[operationID]
	if !ok {
		return nil, fmt.Errorf("%w (operation %q)", ErrCheckpointNotFound, operationID)
	}
	cp := c
	return &cp, nil
}

func (s *InMemoryCheckpointStore) Save(c Checkpoint) error {
	if c.OperationID == "" {
		return fmt.Errorf("executor: checkpoint.OperationID required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[c.OperationID] = c
	return nil
}

func (s *InMemoryCheckpointStore) Clear(operationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, operationID)
	return nil
}
