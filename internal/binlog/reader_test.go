package binlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFakeBinlogs(t *testing.T, dir string, names []string) {
	t.Helper()
	for _, n := range names {
		f, err := os.Create(filepath.Join(dir, n))
		require.NoError(t, err)
		// 写 4 字节 magic 让校验通过（reader 不读这里，但 EnumerateBinlogFiles
		// 的 caller 是 scanner，scanner 自己读 magic；这里仅占位）
		_, err = f.Write([]byte{0xfe, 0x62, 0x69, 0x6e})
		require.NoError(t, err)
		f.Close()
	}
}

func TestEnumerateBinlogFiles_NoFilter(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{
		"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003",
	})
	files, err := EnumerateBinlogFiles(dir, mysql.Position{}, mysql.Position{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(dir, "mysql-bin.000001"),
		filepath.Join(dir, "mysql-bin.000002"),
		filepath.Join(dir, "mysql-bin.000003"),
	}, files)
}

func TestEnumerateBinlogFiles_StartFileOnly(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{
		"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003",
	})
	start := mysql.Position{Name: "mysql-bin.000002", Pos: 0}
	files, err := EnumerateBinlogFiles(dir, start, mysql.Position{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(dir, "mysql-bin.000002"),
		filepath.Join(dir, "mysql-bin.000003"),
	}, files)
}

func TestEnumerateBinlogFiles_StartAndEnd(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{
		"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003",
	})
	start := mysql.Position{Name: "mysql-bin.000002", Pos: 0}
	end := mysql.Position{Name: "mysql-bin.000003", Pos: 0}
	files, err := EnumerateBinlogFiles(dir, start, end)
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(dir, "mysql-bin.000002"),
		filepath.Join(dir, "mysql-bin.000003"),
	}, files)
}

func TestEnumerateBinlogFiles_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	_, err := EnumerateBinlogFiles(dir, mysql.Position{}, mysql.Position{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no binlog files")
}

func TestEnumerateBinlogFiles_IgnoresNonBinlog(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{"mysql-bin.000001"})
	// 杂物：不应被纳入
	writeFakeBinlogs(t, dir, []string{"mysql-bin.index", "mysqld.log", "ibdata1"})
	files, err := EnumerateBinlogFiles(dir, mysql.Position{}, mysql.Position{})
	require.NoError(t, err)
	assert.Len(t, files, 1)
}

func TestEnumerateBinlogFiles_StartAfterEnd(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{
		"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003",
	})
	start := mysql.Position{Name: "mysql-bin.000003", Pos: 0}
	end := mysql.Position{Name: "mysql-bin.000001", Pos: 0}
	_, err := EnumerateBinlogFiles(dir, start, end)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start file after end")
}

func TestEnumerateBinlogFiles_NonexistentDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	_, err := EnumerateBinlogFiles(dir, mysql.Position{}, mysql.Position{})
	require.Error(t, err)
}

func TestEnumerateBinlogFiles_SkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{"mysql-bin.000001"})
	// 名字像 binlog 的目录不应被纳入
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "mysql-bin.000002"), 0o755))
	files, err := EnumerateBinlogFiles(dir, mysql.Position{}, mysql.Position{})
	require.NoError(t, err)
	assert.Len(t, files, 1)
}

func TestEnumerateBinlogFiles_StartNotFound(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{"mysql-bin.000001", "mysql-bin.000002"})
	start := mysql.Position{Name: "mysql-bin.000009", Pos: 0}
	_, err := EnumerateBinlogFiles(dir, start, mysql.Position{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start file")
}

func TestEnumerateBinlogFiles_EndNotFound(t *testing.T) {
	dir := t.TempDir()
	writeFakeBinlogs(t, dir, []string{"mysql-bin.000001", "mysql-bin.000002"})
	end := mysql.Position{Name: "mysql-bin.000009", Pos: 0}
	_, err := EnumerateBinlogFiles(dir, mysql.Position{}, end)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "end file")
}
