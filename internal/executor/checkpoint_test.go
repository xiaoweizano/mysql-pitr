package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryCheckpointStore_RoundTrip(t *testing.T) {
	s := NewInMemoryCheckpointStore()
	c := Checkpoint{OperationID: "op-1", LastCompletedStatement: 5, Total: 100}
	require.NoError(t, s.Save(c))

	loaded, err := s.Load("op-1")
	require.NoError(t, err)
	assert.Equal(t, 5, loaded.LastCompletedStatement)
	assert.Equal(t, 100, loaded.Total)
}

func TestInMemoryCheckpointStore_NotFound(t *testing.T) {
	s := NewInMemoryCheckpointStore()
	_, err := s.Load("missing")
	require.Error(t, err)
}

func TestInMemoryCheckpointStore_Clear(t *testing.T) {
	s := NewInMemoryCheckpointStore()
	require.NoError(t, s.Save(Checkpoint{OperationID: "op-1", Total: 10}))
	require.NoError(t, s.Clear("op-1"))
	_, err := s.Load("op-1")
	require.Error(t, err)
}

func TestInMemoryCheckpointStore_EmptyID(t *testing.T) {
	s := NewInMemoryCheckpointStore()
	err := s.Save(Checkpoint{OperationID: ""})
	require.Error(t, err)
}
