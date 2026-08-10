package archive_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
)

// 归档写入器消费 FileSource（真实 fixture）→ Seal → 还原文件必须与源字节一致。
//
// 唯一例外：fixture 末尾的 ROTATE_EVENT。Consume 把 rotate 事件当作命名指令
// （取 RotateEvent.NextLogName 切换文件名），不落盘该事件本身（archive.go 设计
// 决策），因此期望字节 = magic + 除 ROTATE_EVENT 外所有事件的 RawData。
// 其余事件（FDE/GTID/QUERY/TABLE_MAP/ROWS/XID）必须逐字节还原，且 Seal 的
// 校验和验证在真实文件上成立。
func TestRoundtrip_FixtureBytesIdentical(t *testing.T) {
	fixture := filepath.Join("..", "binlog", "testdata", "mysql-8.0-row-full.bin")
	src, err := binlog.OpenFileSource(context.Background(), fixture, 0, replication.NewBinlogParser())
	require.NoError(t, err)
	defer src.Close()

	dir := t.TempDir()
	w := archive.NewWriter(dir)
	require.NoError(t, w.Consume(context.Background(), src))
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	got, err := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
	require.NoError(t, err)
	want := wantWithoutRotate(t, fixture)
	require.Equal(t, want, got, "归档还原必须字节级一致（ROTATE_EVENT 除外）")
}

// wantWithoutRotate 用 go-mysql 重解析 fixture，拼接 magic + 除 ROTATE_EVENT
// 外所有事件的 RawData，得到 Consume 对真实文件的精确期望输出。
func wantWithoutRotate(t *testing.T, path string) []byte {
	t.Helper()
	parser := replication.NewBinlogParser()
	want := []byte{0xfe, 0x62, 0x69, 0x6e} // binlog magic
	require.NoError(t, parser.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
		if ev.Header != nil && ev.Header.EventType != replication.ROTATE_EVENT {
			want = append(want, ev.RawData...)
		}
		return nil
	}))
	return want
}
