package archive_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
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
