package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/binlogtest"
)

// ---------------------------------------------------------------------------
// stubs
// ---------------------------------------------------------------------------

type stubMySQL struct {
	files []archive.ManifestFile
	pos   mysql.Position
	err   error
}

func (s *stubMySQL) ListBinlogs(ctx context.Context) ([]archive.ManifestFile, error) {
	return s.files, s.err
}
func (s *stubMySQL) MasterPosition(ctx context.Context) (mysql.Position, error) { return s.pos, s.err }

var _ MySQLInfo = (*stubMySQL)(nil)

// fakeSource 依次吐出事件（含一个 FDE + 若干 XID + 一个 Rotate）。
// err 非 nil 时第一次 Next 返回该错误（模拟断线），之后正常。
type fakeSource struct {
	evs []binlogtest.Event
	cur int
	err error
}

func (f *fakeSource) Next(ctx context.Context) (*replication.BinlogEvent, error) {
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	if f.cur >= len(f.evs) {
		return nil, io.EOF
	}
	e := f.evs[f.cur]
	f.cur++
	return &replication.BinlogEvent{RawData: e.Raw, Header: &replication.EventHeader{EventType: e.Type}}, nil
}

func (f *fakeSource) Close() error { return nil }

var _ binlog.Source = (*fakeSource)(nil)

// positionRecorder 记录 SourceFactory 收到的每个续拉位置。
type positionRecorder struct {
	mu    sync.Mutex
	poses []mysql.Position
}

func (r *positionRecorder) record(p mysql.Position) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.poses = append(r.poses, p)
}

func (r *positionRecorder) all() []mysql.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]mysql.Position(nil), r.poses...)
}

// factoryFor 构造一个按文件名分发事件序列的 SourceFactory，记录请求位置；
// 前 failFirstN 次调用返回「首次 Next 报错」的源（模拟断线）。未知文件名返回空源。
func factoryFor(rec *positionRecorder, byName map[string][]binlogtest.Event, failFirstN int) func(context.Context, mysql.Position) (binlog.Source, error) {
	var mu sync.Mutex
	calls := 0
	return func(ctx context.Context, pos mysql.Position) (binlog.Source, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		rec.record(pos)
		if n <= failFirstN {
			return &fakeSource{err: errors.New("collector: fake source connection lost")}, nil
		}
		return &fakeSource{evs: byName[pos.Name]}, nil
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// mustEvent 造一个 binlogtest.Event（测试内 panic 语义，同 MustCraft）。
func mustEvent(ev []byte, err error) binlogtest.Event {
	if err != nil {
		panic(err)
	}
	return binlogtest.MustCraft(ev, nil)
}

func fde() binlogtest.Event               { return mustEvent(binlogtest.CraftFDE()) }
func xid(n uint64) binlogtest.Event       { return mustEvent(binlogtest.CraftXID(n)) }
func rotate(name string) binlogtest.Event { return mustEvent(binlogtest.CraftRotate(name)) }

func writeBinlog(t *testing.T, dir, name string, events []binlogtest.Event) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), binlogtest.CraftFile(events), 0o644))
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// offsetOf 返回 events 拼进 binlog 文件后的偏移（magic 4B + 各事件长度）。
func offsetOf(events []binlogtest.Event) uint32 {
	n := len(replication.BinLogFileHeader)
	for _, e := range events {
		n += len(e.Raw)
	}
	return uint32(n)
}

func newLoop(mysqlInfo MySQLInfo, binlogDir, archiveDir string, factory func(context.Context, mysql.Position) (binlog.Source, error)) *Loop {
	cfg := Config{
		MySQL:         mysqlInfo,
		BinlogDir:     binlogDir,
		ArchiveDir:    archiveDir,
		ServerID:      1,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		SourceFactory: factory,
		Backoff:       func(ctx context.Context) bool { return true }, // 测试：即时重试
	}
	return NewLoop(cfg, archive.NewWriter(archiveDir))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestLoop_FirstRunBackfillsAndAppends：首次运行——归档目录空，MySQL 有 000001
// （封口）与 000002（打开）。断言：000001 整文件回填；000002 = 回填前缀 + append
// 尾部；000003 新文件以 magic 开头；archive_state.json 的 LastFile/LastPos 正确。
func TestLoop_FirstRunBackfillsAndAppends(t *testing.T) {
	binlogDir := t.TempDir()
	archiveDir := t.TempDir()

	// 源文件：000001 全量；000002 在 master position 之后还有尾部 XID(21)
	events1 := []binlogtest.Event{fde(), xid(10)}
	events2 := []binlogtest.Event{fde(), xid(20), xid(21)}
	writeBinlog(t, binlogDir, "mysql-bin.000001", events1)
	writeBinlog(t, binlogDir, "mysql-bin.000002", events2)

	masterPos := mysql.Position{Name: "mysql-bin.000002", Pos: offsetOf([]binlogtest.Event{fde(), xid(20)})}

	rec := &positionRecorder{}
	factory := factoryFor(rec, map[string][]binlogtest.Event{
		"mysql-bin.000002": {fde(), xid(21), rotate("mysql-bin.000003")},
		"mysql-bin.000003": {rotate("mysql-bin.000003"), fde(), xid(30), rotate("mysql-bin.000004")},
	}, 0)

	l := newLoop(&stubMySQL{
		files: []archive.ManifestFile{{Name: "mysql-bin.000001"}, {Name: "mysql-bin.000002"}},
		pos:   masterPos,
	}, binlogDir, archiveDir, factory)

	require.NoError(t, l.Run(context.Background()))

	// 000001 整文件回填 == 源字节
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000001")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000001")))
	// 000002 = 回填前缀(magic+FDE+XID20) + append 尾部(XID21) == 源字节
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000002")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000002")))
	// 000003 新文件以 magic 开头
	b3 := readFile(t, filepath.Join(archiveDir, "mysql-bin.000003"))
	require.Equal(t, []byte{0xfe, 0x62, 0x69, 0x6e}, b3[:4])

	// 续拉位置：先 (000002, masterPos)，轮转后 (000003, 0)
	poses := rec.all()
	require.Equal(t, mysql.Position{Name: "mysql-bin.000002", Pos: masterPos.Pos}, poses[0])
	require.Equal(t, mysql.Position{Name: "mysql-bin.000003", Pos: 0}, poses[1])

	// 状态：最后轮转到 000004，新文件从头开始
	st, err := LoadState(archiveDir)
	require.NoError(t, err)
	require.Equal(t, "mysql-bin.000004", st.LastFile)
	require.Equal(t, uint32(0), st.LastPos)
	require.False(t, st.UpdatedAt.IsZero())
}

// TestLoop_ResumeFromState：预先写好 archive_state.json 与已回填的 000002 →
// 续拉必须从 (000002, P) 开始（SourceFactory 记录入参）；断线重跑不重复回填。
func TestLoop_ResumeFromState(t *testing.T) {
	binlogDir := t.TempDir()
	archiveDir := t.TempDir()

	// 已回填的 000002 前缀（magic+FDE），状态 LastPos 指向该处
	P := offsetOf([]binlogtest.Event{fde()})
	writeBinlog(t, archiveDir, "mysql-bin.000002", []binlogtest.Event{fde()})
	require.NoError(t, SaveState(archiveDir, State{
		LastFile:  "mysql-bin.000002",
		LastPos:   P,
		UpdatedAt: time.Now(),
	}))

	writeBinlog(t, binlogDir, "mysql-bin.000001", []binlogtest.Event{fde(), xid(10)})
	writeBinlog(t, binlogDir, "mysql-bin.000002", []binlogtest.Event{fde(), xid(20)})

	rec := &positionRecorder{}
	factory := factoryFor(rec, map[string][]binlogtest.Event{
		"mysql-bin.000002": {fde(), xid(20), rotate("mysql-bin.000003")},
	}, 0)

	l := newLoop(&stubMySQL{
		files: []archive.ManifestFile{{Name: "mysql-bin.000001"}, {Name: "mysql-bin.000002"}},
		pos:   mysql.Position{Name: "mysql-bin.000002", Pos: offsetOf([]binlogtest.Event{fde(), xid(20)})},
	}, binlogDir, archiveDir, factory)

	require.NoError(t, l.Run(context.Background()))

	// 续拉从状态位置开始（而非 master position 或文件头）
	poses := rec.all()
	require.Equal(t, mysql.Position{Name: "mysql-bin.000002", Pos: P}, poses[0])

	// 000002 = 已回填前缀 + append 尾部 XID(20)（不重复 XID(20)）
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000002")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000002")))
	// 000001 缺失 → reconcile 回填
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000001")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000001")))

	st, err := LoadState(archiveDir)
	require.NoError(t, err)
	require.Equal(t, "mysql-bin.000003", st.LastFile)
}

// TestLoop_ReconcileCopiesMissing：归档缺文件 → reconcile 补齐（整文件 / 打开文件
// 前缀到 master position），不进入续拉（源为空 → 干净结束）。
func TestLoop_ReconcileCopiesMissing(t *testing.T) {
	binlogDir := t.TempDir()
	archiveDir := t.TempDir()

	events1 := []binlogtest.Event{fde(), xid(10)}
	events2 := []binlogtest.Event{fde(), xid(20)}
	writeBinlog(t, binlogDir, "mysql-bin.000001", events1)
	writeBinlog(t, binlogDir, "mysql-bin.000002", events2)

	rec := &positionRecorder{}
	factory := factoryFor(rec, nil, 0) // 所有位置返回空源

	l := newLoop(&stubMySQL{
		files: []archive.ManifestFile{{Name: "mysql-bin.000001"}, {Name: "mysql-bin.000002"}},
		pos:   mysql.Position{Name: "mysql-bin.000002", Pos: offsetOf(events2)},
	}, binlogDir, archiveDir, factory)

	require.NoError(t, l.Run(context.Background()))

	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000001")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000001")))
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000002")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000002")))
}

// TestLoop_SyncErrorBackoffRetries：syncer 第一次报错 → Run 不 fatal，
// 退避后重试成功；最终 000002 = 回填前缀 + append 尾部，状态推进到 000003。
func TestLoop_SyncErrorBackoffRetries(t *testing.T) {
	binlogDir := t.TempDir()
	archiveDir := t.TempDir()

	// 源文件 000002 = magic+FDE+XID(20)；master position 在 FDE 之后，
	// 续拉尾部 = XID(20)
	events2 := []binlogtest.Event{fde(), xid(20)}
	writeBinlog(t, binlogDir, "mysql-bin.000002", events2)
	P := offsetOf([]binlogtest.Event{fde()})

	rec := &positionRecorder{}
	factory := factoryFor(rec, map[string][]binlogtest.Event{
		"mysql-bin.000002": {fde(), xid(20), rotate("mysql-bin.000003")},
	}, 1) // 第一次创建返回断线源，第二次成功

	l := newLoop(&stubMySQL{
		files: []archive.ManifestFile{{Name: "mysql-bin.000002"}},
		pos:   mysql.Position{Name: "mysql-bin.000002", Pos: P},
	}, binlogDir, archiveDir, factory)

	require.NoError(t, l.Run(context.Background()))

	// 重试发生：factory 被调用 ≥ 2 次
	poses := rec.all()
	require.GreaterOrEqual(t, len(poses), 2, "syncer 断线后必须重试")
	// 第一次失败后重试仍从同一位置开始
	require.Equal(t, mysql.Position{Name: "mysql-bin.000002", Pos: P}, poses[0])
	require.Equal(t, mysql.Position{Name: "mysql-bin.000002", Pos: P}, poses[1])

	// 重试成功后：000002 = 回填前缀 + append 尾部（无重复）
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000002")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000002")))
	st, err := LoadState(archiveDir)
	require.NoError(t, err)
	require.Equal(t, "mysql-bin.000003", st.LastFile)
}

// TestLoop_SealTransientFailureDoesNotDoubleAppend：append 封口瞬时失败
// （IO 错误，如磁盘满/只读——区别于永久 CRC 篡改）→ syncOnce 失败返回前必须
// 清理 .partial；重试从同一位置重新拉取尾部、追加到干净 partial，最终文件尾部
// 只出现一次（无 tail+tail）。
//
// 模拟方式：首次 syncOnce 前把最终文件 chmod 0444（只读）。SealAppendVerified
// 的组合验证只读最终文件（仍通过），但最后一步 O_APPEND 打开写入失败（权限
// 错误）——恰好命中「Seal 验证通过但追加落盘瞬时失败」的路径，partial 被保留。
// 恢复可写后以同一位置重跑第二段（syncLoop 错误重试即同位置重拉），
// SealAppendVerified 透传成功。
//
// 取舍：未用包装 Writer（会要求 Loop.w 改为接口，扩大生产 diff 到一行之外），
// 而是用真实 SealAppendVerified 制造真实的瞬时 IO 失败，完整覆盖其 verify →
// 追加 → 失败路径与 syncOnce 的 cleanup 路径。依赖 OS 权限语义（POSIX root
// 会绕过；本机 Windows 与常规 CI 非 root 均生效）。
func TestLoop_SealTransientFailureDoesNotDoubleAppend(t *testing.T) {
	binlogDir := t.TempDir()
	archiveDir := t.TempDir()

	// 已回填的 000002 前缀（magic+FDE+XID20）；续拉尾部 XID(21) 后轮转到 000003
	events2 := []binlogtest.Event{fde(), xid(20), xid(21)}
	P := offsetOf([]binlogtest.Event{fde(), xid(20)})
	writeBinlog(t, archiveDir, "mysql-bin.000002", []binlogtest.Event{fde(), xid(20)})
	writeBinlog(t, binlogDir, "mysql-bin.000002", events2)

	rec := &positionRecorder{}
	factory := factoryFor(rec, map[string][]binlogtest.Event{
		"mysql-bin.000002": {fde(), xid(21), rotate("mysql-bin.000003")},
	}, 0)

	l := newLoop(&stubMySQL{
		files: []archive.ManifestFile{{Name: "mysql-bin.000002"}},
		pos:   mysql.Position{Name: "mysql-bin.000002", Pos: P},
	}, binlogDir, archiveDir, factory)

	pos := mysql.Position{Name: "mysql-bin.000002", Pos: P}
	final := filepath.Join(archiveDir, "mysql-bin.000002")
	partial := final + ".partial"

	// 第一段：只读最终文件 → SealAppendVerified 组合验证通过但追加落盘失败。
	// 失败点必须落在 "open final"（O_APPEND 打开写入），而非 CRC 验证——证明是
	// 瞬时 IO 错误而非永久篡改。
	require.NoError(t, os.Chmod(final, 0o444))
	err := l.syncOnce(context.Background(), pos)
	require.Error(t, err)
	require.Contains(t, err.Error(), "open final")

	// 修复点：封口失败返回前必须清理 .partial——重试才不至于 O_APPEND 续写残留
	require.NoFileExists(t, partial, "seal 失败后残留 partial 会致重试 tail+tail")

	// 恢复可写，同一位置重跑第二段（与 syncLoop 错误重试语义一致）
	require.NoError(t, os.Chmod(final, 0o644))
	require.NoError(t, l.syncOnce(context.Background(), pos))

	// 尾部只出现一次：最终文件字节 == 源文件字节（无 tail+tail）
	require.Equal(t, readFile(t, filepath.Join(binlogDir, "mysql-bin.000002")),
		readFile(t, filepath.Join(archiveDir, "mysql-bin.000002")))
	// 状态推进到轮转后的新文件
	st, err := LoadState(archiveDir)
	require.NoError(t, err)
	require.Equal(t, "mysql-bin.000003", st.LastFile)
}

// TestLoop_ServerIDRequired：ServerID=0 时 Run 立即报错（不落盘）。
func TestLoop_ServerIDRequired(t *testing.T) {
	l := NewLoop(Config{
		MySQL:      &stubMySQL{},
		ArchiveDir: t.TempDir(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, archive.NewWriter(t.TempDir()))
	err := l.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "ServerID required")
}
