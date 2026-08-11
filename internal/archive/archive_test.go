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
	_, err := w.Consume(context.Background(), src)
	require.NoError(t, err)
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
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	// 篡改一个字节破坏校验和
	p := filepath.Join(dir, "mysql-bin.000001.partial")
	b, _ := os.ReadFile(p)
	b[20] ^= 0xff
	os.WriteFile(p, b, 0o644)
	require.Error(t, w.Seal("mysql-bin.000001.partial"))
}

func TestWriter_ConsumeRotateStartsNewFile(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)

	// 段 1：无公告轮转，首个文件用默认名 mysql-bin.000001；真实轮转结束段
	seg1 := []binlogtest.Event{
		binlogtest.MustCraft(binlogtest.CraftFDE()),
		binlogtest.MustCraft(binlogtest.CraftXID(1)),
		binlogtest.MustCraft(binlogtest.CraftRotate("mysql-bin.000002")),
	}
	next, err := w.Consume(context.Background(), &sliceSource{evs: seg1})
	require.NoError(t, err)
	require.Equal(t, "mysql-bin.000002", next, "真实轮转必须返回下一个文件名并结束本段")
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	// 段 2：起始公告轮转命名文件（fake rotate，不结束段），随后真实轮转结束
	seg2 := []binlogtest.Event{
		binlogtest.MustCraft(binlogtest.CraftRotate("mysql-bin.000002")),
		binlogtest.MustCraft(binlogtest.CraftFDE()),
		binlogtest.MustCraft(binlogtest.CraftXID(2)),
		binlogtest.MustCraft(binlogtest.CraftRotate("mysql-bin.000003")),
	}
	next, err = w.Consume(context.Background(), &sliceSource{evs: seg2})
	require.NoError(t, err)
	require.Equal(t, "mysql-bin.000003", next)
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
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
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

// TestWriter_ConsumeAppend_AppendsToSealed 验证 append 续写模式：
// ConsumeAppend 把尾部事件（不写 magic）写到 .partial，Seal 在已封口文件
// 末尾追加（无重复 magic）。
func TestWriter_ConsumeAppend_AppendsToSealed(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	// 先造一个"已回填"的封口文件：全量重建一份 FDE+XID
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	// append 续写：XID(2) 尾部
	tail := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftXID(2))}
	_, err = w.ConsumeAppend(context.Background(), &sliceSource{evs: tail}, "mysql-bin.000001")
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	// 最终文件 = 原封口内容 + 尾部（无重复 magic）
	got, _ := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
	want := append(append([]byte{}, binlogtest.CraftFile(evs)...), tail[0].Raw...)
	require.Equal(t, want, got)
}

// TestWriter_SealFullReconstructionOverExistingRefuses 验证封口守卫：
// 全量重建（magic 开头）的 .partial 在目标文件已存在时 Seal 必须拒绝（防覆盖）。
func TestWriter_SealFullReconstructionOverExistingRefuses(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))
	// 再次全量重建同名文件 → Seal 必须拒绝（防覆盖）
	_, err = w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.Error(t, w.Seal("mysql-bin.000001.partial"))
}

// TestWriter_ConsumeAppendVerifyTailCorruption 验证 append 尾部的完整性检查：
// 尾部被篡改（结构破坏）→ Seal 失败，不追加，partial 保留供调用方回退。
//
// 注：go-mysql 的校验和验证以 FDE 解析为门槛（Phase 1 Seal 注释已记录），
// magic+tail（无 FDE）会跳过 CRC 校验，因此这里篡改事件头的 EventSize
// （31 → 95）做结构性破坏——解析时读超触发 unexpected EOF，保证 ParseFile 失败。
func TestWriter_ConsumeAppendVerifyTailCorruption(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	// append 尾部后篡改 EventSize：XID 事件头 [9:13] 是 event size
	tail := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftXID(2))}
	_, err = w.ConsumeAppend(context.Background(), &sliceSource{evs: tail}, "mysql-bin.000001")
	require.NoError(t, err)
	p := filepath.Join(dir, "mysql-bin.000001.partial")
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	b[9] ^= 0x40 // event size 31 → 95：解析时读超 → 验证失败
	require.NoError(t, os.WriteFile(p, b, 0o644))

	require.Error(t, w.Seal("mysql-bin.000001.partial"))

	// 不追加：最终文件保持原封口内容，partial 仍在
	got, err := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
	require.NoError(t, err)
	require.Equal(t, binlogtest.CraftFile(evs), got)
	_, err = os.Stat(p)
	require.NoError(t, err, "Seal 失败后 partial 必须保留，供调用方回退/重试")
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
		_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
		require.Error(t, err, "rotate name %q 必须被拒绝", name)
		assert.Contains(t, err.Error(), "rotate next log name")
	}
}

// TestWriter_SealAppendVerified_AppendsToSealed 验证 CRC 强化的 append 封口
// 正常路径：最终文件 + 尾部组合验证通过后追加，结果与 Seal 的 append 分支一致。
func TestWriter_SealAppendVerified_AppendsToSealed(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	tail := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftXID(2))}
	_, err = w.ConsumeAppend(context.Background(), &sliceSource{evs: tail}, "mysql-bin.000001")
	require.NoError(t, err)
	require.NoError(t, w.SealAppendVerified("mysql-bin.000001.partial"))

	got, _ := os.ReadFile(filepath.Join(dir, "mysql-bin.000001"))
	want := append(append([]byte{}, binlogtest.CraftFile(evs)...), tail[0].Raw...)
	require.Equal(t, want, got)
}

// TestWriter_SealAppendVerified_RejectsTamperedFinal 验证组合验证启用 CRC：
// 篡改最终文件内一个事件字节（XID body）→ 组合验证失败 → 不追加、partial 保留。
// 这是 Seal 的 append 分支（magic+tail 无 FDE，CRC 被 go-mysql 跳过）捕获不到的
// 场景，T4 评审 carry-in 要求归档循环的 append 封口必须使用本方法。
func TestWriter_SealAppendVerified_RejectsTamperedFinal(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))

	// 篡改最终文件 XID(1) 的事件体一个字节（CRC 之前的 body 区）
	finalPath := filepath.Join(dir, "mysql-bin.000001")
	b, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	b[len(b)-5] ^= 0x01 // XID body 末字节（末 4 字节是 CRC）
	require.NoError(t, os.WriteFile(finalPath, b, 0o644))

	// append 尾部后组合验证必须失败
	tail := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftXID(2))}
	_, err = w.ConsumeAppend(context.Background(), &sliceSource{evs: tail}, "mysql-bin.000001")
	require.NoError(t, err)
	require.Error(t, w.SealAppendVerified("mysql-bin.000001.partial"))

	// 不追加：最终文件保持篡改后原样（无 XID(2) 尾部），partial 仍在
	got, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	require.Equal(t, b, got)
	_, err = os.Stat(filepath.Join(dir, "mysql-bin.000001.partial"))
	require.NoError(t, err, "验证失败后 partial 必须保留，供调用方回退/重试")
}

// TestWriter_SealAppendVerified_AppendIdempotent 验证追加幂等（I1）：
// 最终文件末尾已含与 partial 相同的尾部（此前 Seal 成功但状态持久化失败、
// 崩溃窗口后从旧位置重拉的典型场景）→ SealAppendVerified 必须跳过追加、
// 清理 partial，而不是再追加一遍（否则归档重复事务）。
func TestWriter_SealAppendVerified_AppendIdempotent(t *testing.T) {
	dir := t.TempDir()
	w := archive.NewWriter(dir)
	evs := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftFDE()), binlogtest.MustCraft(binlogtest.CraftXID(1))}
	tail := []binlogtest.Event{binlogtest.MustCraft(binlogtest.CraftXID(2))}
	// 造一个「尾部已被追加过」的最终文件：全量重建 FDE+XID(1)，再手动追加上 XID(2)
	_, err := w.Consume(context.Background(), &sliceSource{evs: evs})
	require.NoError(t, err)
	require.NoError(t, w.Seal("mysql-bin.000001.partial"))
	finalPath := filepath.Join(dir, "mysql-bin.000001")
	f, err := os.OpenFile(finalPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Write(tail[0].Raw)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// 重拉场景：partial 里又是同一段尾部
	_, err = w.ConsumeAppend(context.Background(), &sliceSource{evs: tail}, "mysql-bin.000001")
	require.NoError(t, err)
	require.NoError(t, w.SealAppendVerified("mysql-bin.000001.partial"))

	// 尾部只出现一次：最终文件 == 原封口 + 一份尾部（无重复）
	got, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	want := append(append([]byte{}, binlogtest.CraftFile(evs)...), tail[0].Raw...)
	require.Equal(t, want, got)
	_, err = os.Stat(filepath.Join(dir, "mysql-bin.000001.partial"))
	require.True(t, os.IsNotExist(err), "幂等跳过后 partial 必须被清理")
}
