package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/stream"
)

// errStreamEnded 标记一次 syncOnce 以「流干净结束（无轮转）」终止：
// 对应一个空/无轮转的源（测试用），或真实 syncer 的异常关闭前 EOF。
// syncLoop 据此正常返回（区别于需退避重试的错误）。
var errStreamEnded = errors.New("collector: stream ended cleanly without rotation")

// MySQLInfo 抽象归档循环需要的 MySQL 交互（生产=connector，测试=stub）。
type MySQLInfo interface {
	ListBinlogs(ctx context.Context) ([]archive.ManifestFile, error) // SHOW BINARY LOGS
	MasterPosition(ctx context.Context) (mysql.Position, error)      // SHOW MASTER STATUS
}

// Config 配置归档循环。
type Config struct {
	MySQL         MySQLInfo
	BinlogDir     string // MySQL 侧 binlog 目录（回填复制源）
	ArchiveDir    string // 归档目录（writer 的 dir）
	ServerID      uint32 // syncer server id（必填 >0）
	RetentionDays int    // 0 = 不清理
	Logger        *slog.Logger
	// SourceFactory 生产 binlogsyncer Source；测试注入 fake。nil 时用
	// DefaultSourceFactory 包装 stream.NewSource。
	SourceFactory func(ctx context.Context, pos mysql.Position) (binlog.Source, error)
	// Backoff 重试退避：返回 true 继续重试，false 停止（ctx 取消）。nil 时用
	// 指数退避（1s 起步、60s 封顶）。测试注入即时重试以加速。
	Backoff func(ctx context.Context) bool
}

// Loop 是归档循环。Run 阻塞直到 ctx 取消或 fatal 错误；循环内对 syncer
// 错误指数退避重试（断线自愈）。
type Loop struct {
	cfg Config
	w   *archive.Writer

	mu    sync.Mutex
	state State // 最近状态（供 State() 查询，与磁盘 archive_state.json 同步）

	backoffMu    sync.Mutex
	backoffDelay time.Duration
}

// NewLoop 创建归档循环。w 是写 ArchiveDir 的 Writer。
func NewLoop(cfg Config, w *archive.Writer) *Loop {
	return &Loop{cfg: cfg, w: w}
}

// State 返回当前归档状态（供 archive_status 命令）。
func (l *Loop) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

func (l *Loop) setState(s State) {
	l.mu.Lock()
	l.state = s
	l.mu.Unlock()
}

// Run 执行归档循环：reconcile（回填缺失 + 清理 .partial）→ syncLoop（续拉）。
// 阻塞直到 ctx 取消或 fatal 错误（reconcile 失败、状态损坏）。
func (l *Loop) Run(ctx context.Context) error {
	if l.cfg.ServerID == 0 {
		return fmt.Errorf("collector: ServerID required")
	}
	if l.cfg.Logger == nil {
		l.cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if l.cfg.SourceFactory == nil {
		l.cfg.SourceFactory = DefaultSourceFactory(stream.Config{ServerID: l.cfg.ServerID})
	}
	if l.cfg.Backoff == nil {
		l.cfg.Backoff = l.defaultBackoff
	}
	if err := l.reconcile(ctx); err != nil {
		return err
	}
	return l.syncLoop(ctx)
}

// reconcile：回填缺失文件（含打开文件前缀到 master position）+ 清理陈旧 .partial。
// 只在启动时执行一次；.partial 被全部清掉——syncer 会重造。
func (l *Loop) reconcile(ctx context.Context) error {
	files, err := l.cfg.MySQL.ListBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("collector: list binlogs: %w", err)
	}
	pos, err := l.cfg.MySQL.MasterPosition(ctx)
	if err != nil {
		return fmt.Errorf("collector: master position: %w", err)
	}
	for _, mf := range files {
		final := filepath.Join(l.cfg.ArchiveDir, mf.Name)
		if _, err := os.Stat(final); os.IsNotExist(err) {
			if err := l.backfillFile(ctx, mf.Name, pos); err != nil {
				return fmt.Errorf("collector: backfill %s: %w", mf.Name, err)
			}
		}
	}
	// 清理陈旧 .partial（reconcile 时全部清掉——syncer 会重造）
	l.cleanupPartials()
	return nil
}

// backfillFile：整文件复制；若是当前打开文件（名字==master pos 的文件），
// 只复制到 master position 的前缀（前缀稳定，尾部由 syncer 续拉补齐）。
func (l *Loop) backfillFile(ctx context.Context, name string, masterPos mysql.Position) error {
	src := filepath.Join(l.cfg.BinlogDir, name)
	dst := filepath.Join(l.cfg.ArchiveDir, name)
	limit := int64(-1) // 全文件
	if name == masterPos.Name {
		limit = int64(masterPos.Pos) // 复制到 master position
	}
	return copyFilePrefix(src, dst, limit)
}

// copyFilePrefix 复制 src 的前 limit 字节到 dst（limit<0 = 全文件），
// 原子落盘：先写 dst+".partial" 再 rename 到 dst。master position 落在
// 文件头之前时钳制到 magic（4 字节），保证回填出的文件总是以 binlog magic
// 开头。
func copyFilePrefix(src, dst string, limit int64) error {
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("collector: open %s: %w", src, err)
	}
	defer f.Close()

	partial := dst + ".partial"
	out, err := os.Create(partial)
	if err != nil {
		return fmt.Errorf("collector: create %s: %w", partial, err)
	}

	var copyErr error
	if limit >= 0 && limit < 4 {
		limit = 4 // 至少包含 binlog magic
	}
	if limit < 0 {
		_, copyErr = io.Copy(out, f)
	} else {
		_, copyErr = io.CopyN(out, f, limit)
	}
	if cerr := out.Close(); copyErr == nil {
		copyErr = cerr
	}
	if copyErr != nil {
		_ = os.Remove(partial)
		return fmt.Errorf("collector: copy %s: %w", src, copyErr)
	}
	if err := os.Rename(partial, dst); err != nil {
		_ = os.Remove(partial)
		return fmt.Errorf("collector: rename %s: %w", dst, err)
	}
	return nil
}

// cleanupPartials 删除归档目录下所有 .partial 文件。用于 reconcile 的陈旧清理
// 与 syncOnce 失败后的重试前清理（防止 O_APPEND 重试时重复追加尾部）。
func (l *Loop) cleanupPartials() {
	entries, err := os.ReadDir(l.cfg.ArchiveDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".partial") {
			_ = os.Remove(filepath.Join(l.cfg.ArchiveDir, e.Name()))
		}
	}
}

// syncLoop：读状态 → 确定续拉位置 → syncOnce 续拉一个文件段 → 轮转封口 →
// 更新状态；错误指数退避重试，ctx 取消即停。
func (l *Loop) syncLoop(ctx context.Context) error {
	st, err := LoadState(l.cfg.ArchiveDir)
	if err != nil {
		return err
	}
	if st.LastFile == "" {
		// 从未运行：从 master position 开始（不回溯历史——历史已被回填覆盖）
		pos, err := l.cfg.MySQL.MasterPosition(ctx)
		if err != nil {
			return fmt.Errorf("collector: master position: %w", err)
		}
		st = State{LastFile: pos.Name, LastPos: pos.Pos, UpdatedAt: time.Now()}
		l.setState(st)
	}
	pos := mysql.Position{Name: st.LastFile, Pos: st.LastPos}

	for {
		err := l.syncOnce(ctx, pos)
		if err != nil {
			if errors.Is(err, errStreamEnded) {
				return nil // 流干净结束（正常停止）
			}
			if ctx.Err() != nil {
				return nil // 正常停止
			}
			l.cfg.Logger.Warn("collector: sync interrupted; retrying", "err", err)
			if !l.cfg.Backoff(ctx) {
				return nil
			}
			continue
		}
		// syncOnce 成功（轮转封口并 SaveState）：重读最新状态继续
		l.resetBackoff()
		st, err := LoadState(l.cfg.ArchiveDir)
		if err != nil {
			return err
		}
		if st.LastFile == "" || st.LastFile == pos.Name {
			return nil // 状态未推进：段未轮转（空源），停止
		}
		l.setState(st)
		pos = mysql.Position{Name: st.LastFile, Pos: st.LastPos}
	}
}

// syncOnce：一次连续同步，直到轮转封口或错误。
//
// 判定续写模式：目标文件已封口（回填过）→ ConsumeAppend 且丢弃流首
// 公告轮转 + FDE；否则（新文件）→ Consume（全量重建，写 magic）。
//
// Consume/ConsumeAppend 在真实轮转处返回下一个文件名（段边界），调用方：
//  1. Seal 本段写出的 pos.Name 的 .partial——append 场景用 SealAppendVerified
//     （最终文件+尾部组合验证，CRC 生效），全量重建用 Seal；
//  2. SaveState(LastFile=next, LastPos=0)（新文件从头开始）；
//
// 流结束（EOF）但未轮转 → 本段不完整，丢弃 partial，返回 errStreamEnded。
func (l *Loop) syncOnce(ctx context.Context, pos mysql.Position) error {
	src, err := l.cfg.SourceFactory(ctx, pos)
	if err != nil {
		return fmt.Errorf("collector: create source at %s/%d: %w", pos.Name, pos.Pos, err)
	}
	defer src.Close()

	final := filepath.Join(l.cfg.ArchiveDir, pos.Name)
	appending := fileExists(final)

	var next string
	if appending {
		// 丢弃 master 重发的首个公告轮转 + FDE（它们不属于文件内容）
		first, err := skipStreamPreamble(ctx, src)
		if err != nil {
			l.cleanupPartials()
			return err
		}
		if first == nil {
			// 源为空（无可续拉内容）：干净结束
			l.cleanupPartials()
			return errStreamEnded
		}
		next, err = l.w.ConsumeAppend(ctx, &prependSource{src: src, first: first}, pos.Name)
	} else {
		next, err = l.w.Consume(ctx, src)
	}
	if err != nil {
		l.cleanupPartials()
		return err
	}
	if next == "" {
		// 流干净结束（无轮转）：本段不完整 → 丢弃 partial，干净停止
		l.cleanupPartials()
		return errStreamEnded
	}

	// 轮转：封口 pos.Name（本段写的文件），并持久化状态到下一个文件
	partial := filepath.Join(l.cfg.ArchiveDir, pos.Name+".partial")
	if !fileExists(partial) {
		return fmt.Errorf("collector: segment for %s rotated but partial %s missing (source must announce the segment file via an initial rotate)", pos.Name, pos.Name+".partial")
	}
	if appending {
		if err := l.w.SealAppendVerified(pos.Name + ".partial"); err != nil {
			// 封口瞬时失败（磁盘满/IO 抖动，区别于永久 CRC 篡改）也必须清理
			// .partial：否则重试会 O_APPEND 续写残留 partial → tail+tail
			// 重复追加（组合验证仍通过，事件自包含）。与 Consume* 错误路径一致。
			l.cleanupPartials()
			return err
		}
	} else {
		if err := l.w.Seal(pos.Name + ".partial"); err != nil {
			l.cleanupPartials()
			return err
		}
	}
	l.pruneRetention()

	st := State{LastFile: next, LastPos: 0, UpdatedAt: time.Now()}
	if err := SaveState(l.cfg.ArchiveDir, st); err != nil {
		return err
	}
	l.setState(st)
	return nil
}

// skipStreamPreamble 丢弃流起始的公告轮转（fake ROTATE_EVENT）与
// FORMAT_DESCRIPTION_EVENT，返回第一个内容事件；源为空（EOF）返回 (nil, nil)。
// 这些事件是 StartSync 时 master 为初始化 parser 重发的，不属于文件内容——
// append 续写时必须剔除，否则会重复写入 FDE。
func skipStreamPreamble(ctx context.Context, src binlog.Source) (*replication.BinlogEvent, error) {
	for {
		ev, err := src.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, err
		}
		if ev.Header == nil {
			continue
		}
		switch ev.Header.EventType {
		case replication.ROTATE_EVENT, replication.FORMAT_DESCRIPTION_EVENT:
			continue
		default:
			return ev, nil
		}
	}
}

// prependSource 先吐出 skipStreamPreamble 已预读的首个内容事件，再委托原源。
// 让 archive.Writer 的 ConsumeAppend 无感知地吃到「已预读一事件」的流。
type prependSource struct {
	src   binlog.Source
	first *replication.BinlogEvent
	used  bool
}

func (p *prependSource) Next(ctx context.Context) (*replication.BinlogEvent, error) {
	if !p.used {
		p.used = true
		return p.first, nil
	}
	return p.src.Next(ctx)
}

func (p *prependSource) Close() error { return p.src.Close() }

// pruneRetention 删除 mtime 早于 cut off 的封口文件（RetentionDays > 0 时）。
// 保留 .partial、状态文件与校验临时文件。
func (l *Loop) pruneRetention() {
	if l.cfg.RetentionDays <= 0 {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -l.cfg.RetentionDays)
	entries, err := os.ReadDir(l.cfg.ArchiveDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".partial") || strings.HasSuffix(name, ".json") ||
			strings.HasPrefix(name, ".verify-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(l.cfg.ArchiveDir, name))
		}
	}
}

// defaultBackoff：1s 起步、60s 封顶、指数退避；ctx 取消返回 false（停止重试）。
func (l *Loop) defaultBackoff(ctx context.Context) bool {
	l.backoffMu.Lock()
	d := l.backoffDelay
	if d == 0 {
		d = time.Second
	}
	l.backoffDelay = d * 2
	if l.backoffDelay > 60*time.Second {
		l.backoffDelay = 60 * time.Second
	}
	l.backoffMu.Unlock()

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *Loop) resetBackoff() {
	l.backoffMu.Lock()
	l.backoffDelay = 0
	l.backoffMu.Unlock()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DefaultSourceFactory 是 Config.SourceFactory 的默认实现：包装 stream.NewSource
// 并落实交接 #3 的 SyncPos/SyncGTID 校验——两者至少其一非零，否则拒绝启动同步
// （避免零值位置静默开始复制错误的事件流）。position 小于 4（文件 magic 之后）
// 时钳制到 4，与 MySQL COM_BINLOG_DUMP 的语义一致（归档循环对轮转后的新文件
// 存 LastPos=0，表示「从文件头开始」，须钳制后才是合法 dump 位置）。
func DefaultSourceFactory(scfg stream.Config) func(ctx context.Context, pos mysql.Position) (binlog.Source, error) {
	return func(ctx context.Context, pos mysql.Position) (binlog.Source, error) {
		hasGTID := scfg.SyncGTID != nil && !scfg.SyncGTID.IsEmpty()
		if !hasGTID && pos.Name == "" {
			return nil, fmt.Errorf("collector: zero SyncPos (name=%q pos=%d) and no SyncGTID; refusing to start sync", pos.Name, pos.Pos)
		}
		if !hasGTID && pos.Pos < 4 {
			pos.Pos = 4
		}
		scfg.SyncPos = pos
		return stream.NewSource(scfg)
	}
}
