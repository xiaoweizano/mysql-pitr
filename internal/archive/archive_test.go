package archive_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/binlogtest"
)

// sliceSource 从 binlogtest.Event 切片构造一个 binlog.Source。
type sliceSource struct {
	evs []binlogtest.Event
	cur int
}

func (s *sliceSource) Next(ctx context.Context) (*replication.BinlogEvent, error) {
	if s.cur >= len(s.evs) {
		return nil, io.EOF
	}
	e := s.evs[s.cur]
	s.cur++
	return &replication.BinlogEvent{RawData: e.Raw, Header: &replication.EventHeader{EventType: e.Type, Timestamp: 1754294400}}, nil
}
func (s *sliceSource) Close() error { return nil }

var _ binlog.Source = (*sliceSource)(nil) // 编译期断言：sliceSource 实现 binlog.Source

type stubManifest []archive.ManifestFile

func (m stubManifest) List(ctx context.Context) ([]archive.ManifestFile, error) { return m, nil }

func TestWriter_ConsumeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)

	evs := []binlogtest.Event{
		binlogtest.MustCraft(binlogtest.CraftFDE()),
		binlogtest.MustCraft(binlogtest.CraftGTID("uuid", 1)),
		binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN")),
		binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
		binlogtest.MustCraft(binlogtest.CraftWriteRows(1, 2)),
		binlogtest.MustCraft(binlogtest.CraftXID(100)),
	}
	src := &sliceSource{evs: evs}
	require.NoError(t, w.Consume(context.Background(), src))
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	// 还原出的文件字节 == craft 拼接
	got, err := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
	require.NoError(t, err)
	want := binlogtest.CraftFile(evs)
	require.Equal(t, want, got)
}

func TestWriter_SealCorruptedFails(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
	// 篡改一个字节破坏校验和
	p := filepath.Join(dir, "mysql-bin.000001.partial")
	b, _ := os.ReadFile(p)
	b[20] ^= 0xff
	os.WriteFile(p, b, 0o644)
	require.Error(t, w.Seal("mysql-bin.000001.partial"))
}

func TestWriter_Gaps(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), []byte("x"), 0o644)
	gaps, err := w.Gaps(context.Background(), stubManifest{
		{Name: "mysql-bin.000001", Size: 1},
		{Name: "mysql-bin.000002", Size: 5},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"mysql-bin.000002"}, gaps)
}

func TestWriter_ConsumeRotateStartsNewFile(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)

	evs := []binlogtest.Event{
		binlogtest.MustCraft(binlogtest.CraftFDE()),
		binlogtest.MustCraft(binlogtest.CraftXID(1)),
		binlogtest.MustCraft(binlogtest.CraftRotate("mysql-bin.000002")),
		binlogtest.MustCraft(binlogtest.CraftXID(2)),
	}
	require.NoError(t, w.Consume(context.Background(), &sliceSource{evs: evs}))
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))
	require.NoError(t, w.Seal("mysql-bin.000002.partial"))

	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, []byte{0xfe, 0x62, 0x69, 0x6e}, b[:4], "每个归档文件必须以 binlog magic 开头")
	}
}

// TestWriter_ConsumeRejectsPathTraversalName 回归 final review Important #4：
// ROTATE_EVENT 携带的恶意/损坏名字（含路径分隔符）不得写出归档目录——
// Consume 必须报错，且目录外不产生任何文件。
// CraftRotate 原样把名字拼进 body（无 hex 解码路径），"../evil" 可直接测试。
func TestWriter_ConsumeRejectsPathTraversalName(t *testing.T) {
	dir := t.TempDir()
	parent := filepath.Dir(dir)
	w := archive.NewWriter(dir)

	evs := []binlogtest.Event{
		binlogtest.MustCraft(binlogtest.CraftRotate("../evil")),
		binlogtest.MustCraft(binlogtest.CraftXID(1)),
	}
	err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.Error(t, err, "路径穿越名必须被拒绝")
	assert.Contains(t, err.Error(), "rotate next log name")

	// 目录内不得残留任何文件（rotate 被拒绝时尚未写任何文件）
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "拒绝后不得在归档目录内留下文件")

	// 目录外（filepath.Join 本可写出的位置）不得出现 evil / evil.partial
	for _, n := range []string{"evil", "evil.partial"} {
		_, err := os.Stat(filepath.Join(parent, n))
		assert.True(t, os.IsNotExist(err), "路径穿越不得在归档目录外创建 %s", n)
	}
}

// TestWriter_ConsumeRejectsRotateNonBinlogName 补充校验：不符合
// "<前缀>.<全数字>" binlog 命名的 rotate 名字（如 "evil.txt"、".."）也拒绝。
func TestWriter_ConsumeRejectsRotateNonBinlogName(t *testing.T) {
	for _, name := range []string{"evil.txt", "..", ".", "mysql-bin.", "mysql-bin.000002.extra"} {
		dir := t.TempDir()
		w := archive.NewWriter(dir)
		evs := []binlogtest.Event{
			binlogtest.MustCraft(binlogtest.CraftRotate(name)),
			binlogtest.MustCraft(binlogtest.CraftXID(1)),
		}
		err := w.Consume(context.Background(), &sliceSource{evs: evs})
		require.Error(t, err, "rotate name %q 必须被拒绝", name)
		assert.Contains(t, err.Error(), "rotate next log name")
	}
}
