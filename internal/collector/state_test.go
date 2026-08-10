package collector

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadState_MissingFileReturnsZero(t *testing.T) {
	st, err := LoadState(t.TempDir())
	require.NoError(t, err, "归档目录无状态文件 = 从未运行，必须返回零值而非错误")
	require.Equal(t, State{}, st)
}

func TestLoadState_CorruptFileErrors(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "archive_state.json"), []byte("{not json"), 0o644))
	_, err := LoadState(dir)
	require.Error(t, err, "状态文件损坏必须报错（区分「从未运行」与「损坏」）")
}

func TestSaveLoadState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := State{LastFile: "mysql-bin.000004", LastPos: 0, UpdatedAt: time.Now()}
	require.NoError(t, SaveState(dir, want))

	got, err := LoadState(dir)
	require.NoError(t, err)
	require.Equal(t, want.LastFile, got.LastFile)
	require.Equal(t, want.LastPos, got.LastPos)
	require.False(t, got.UpdatedAt.IsZero())

	// 原子写：无残留 .tmp
	_, err = os.Stat(filepath.Join(dir, "archive_state.json.tmp"))
	require.True(t, os.IsNotExist(err), "原子写后不得残留 .tmp")
}
