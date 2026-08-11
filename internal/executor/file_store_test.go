package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileCheckpointStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewFileCheckpointStore(dir)
	cp := Checkpoint{OperationID: "op-1", LastCompletedStatement: 42, Total: 100,
		Errors: []ExecError{{Statement: 7, SQL: "UPDATE t SET a=1", Err: "dup key"}}}
	require.NoError(t, s.Save(cp))
	got, err := s.Load("op-1")
	require.NoError(t, err)
	assert.Equal(t, cp, *got)
	require.NoError(t, s.Clear("op-1"))
	_, err = s.Load("op-1")
	require.Error(t, err) // 已清除
}

func TestFileCheckpointStore_LoadMissing(t *testing.T) {
	s := NewFileCheckpointStore(t.TempDir())
	_, err := s.Load("nope")
	require.Error(t, err)
}

func TestFileCheckpointStore_RejectsEmptyOperationID(t *testing.T) {
	dir := t.TempDir()
	s := NewFileCheckpointStore(dir)
	err := s.Save(Checkpoint{LastCompletedStatement: 1, Total: 10})
	require.Error(t, err)
	require.Contains(t, err.Error(), "OperationID required")
	// 目录内不得写入 `<dir>/.json`（空 ID 的残留文件）
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestFileCheckpointStore_AtomicWrite(t *testing.T) {
	// 写入后目录里只有 <op>.json，无 .tmp 残留
	dir := t.TempDir()
	s := NewFileCheckpointStore(dir)
	cp := Checkpoint{OperationID: "op-atomic", LastCompletedStatement: 3, Total: 10}
	require.NoError(t, s.Save(cp))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "op-atomic.json", entries[0].Name())
	assert.Equal(t, cp.OperationID+".json", filepath.Base(s.path(cp.OperationID)))

	data, err := os.ReadFile(filepath.Join(dir, "op-atomic.json"))
	require.NoError(t, err)
	assert.JSONEq(t, `{"OperationID":"op-atomic","LastCompletedStatement":3,"Total":10,"Errors":null}`, string(data))
}
