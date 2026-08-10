package binlog_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

func TestFileSource_ReadsAllEvents(t *testing.T) {
	path := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	ctx := context.Background()
	src, err := binlog.OpenFileSource(ctx, path, 0, replication.NewBinlogParser())
	require.NoError(t, err)
	defer src.Close()

	var n int
	firstType := replication.EventType(255)
	for {
		ev, err := src.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if firstType == replication.EventType(255) {
			firstType = ev.Header.EventType
		}
		n++
	}
	require.Greater(t, n, 3, "fixture 至少含 FDE + TableMap + Rows 等事件")
	require.Equal(t, replication.FORMAT_DESCRIPTION_EVENT, firstType, "第一个事件必须是 FDE")
}

func TestFileSource_StartMidFile(t *testing.T) {
	path := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	ctx := context.Background()
	// 从 offset 4 开始：FDE 被重新解析后事件流应仍可消费
	src, err := binlog.OpenFileSource(ctx, path, 4, replication.NewBinlogParser())
	require.NoError(t, err)
	defer src.Close()
	ev, err := src.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, ev)
}

func TestFileSource_ContextCancel(t *testing.T) {
	path := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	ctx, cancel := context.WithCancel(context.Background())
	src, err := binlog.OpenFileSource(ctx, path, 0, replication.NewBinlogParser())
	require.NoError(t, err)
	defer src.Close()
	cancel()
	_, err = src.Next(ctx)
	require.Error(t, err)
}

func TestFileSource_BadMagic(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "mysql-bin.000001")
	require.NoError(t, os.WriteFile(bad, []byte("not a binlog"), 0o644))
	_, err := binlog.OpenFileSource(context.Background(), bad, 0, replication.NewBinlogParser())
	require.Error(t, err)
}
