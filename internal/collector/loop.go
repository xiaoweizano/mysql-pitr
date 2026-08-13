package collector

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

// errStateAdvance 标记 syncOnce 已完成封口（Seal 成功、尾部已落盘）但状态
// 持久化失败：syncLoop 必须从内存中的最新状态继续，而不是从旧位置重拉
// （重拉会重复消费同一段尾部；append 幂等检查虽能拦截追加，仍应避免）。
var errStateAdvance = errors.New("collector: state persistence failed after seal; continuing from sealed position")

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

	// saveState 是状态持久化函数（默认 SaveState；测试注入失败以覆盖
	// 「Seal 成功但状态写盘失败」的路径）。
	saveState func(dir string, s State) error
}

// NewLoop 创建归档循环。w 是写 ArchiveDir 的 Writer。
func NewLoop(cfg Config, w *archive.Writer) *Loop {
	return &Loop{cfg: cfg, w: w, saveState: SaveState}
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
//
// 打开文件为空（master position ≤ 4，文件仅含 binlog magic 或更少）时跳过
// 回填：避免造出无 FDE 的 magic-only 文件——否则续拉时 skipStreamPreamble
// 丢弃首个 FDE、封口文件缺 FDE、SealAppendVerified 的组合验证 nil-deref
// （T10 确认的同一缺陷根源）。
func (l *Loop) backfillFile(ctx context.Context, name string, masterPos mysql.Position) error {
	if name == masterPos.Name && masterPos.Pos <= 4 {
		return nil // 打开文件为空：跳过前缀回填
	}
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
	if err := clearInUseFlag(partial); err != nil {
		_ = os.Remove(partial)
		return fmt.Errorf("collector: clear in-use flag %s: %w", partial, err)
	}
	if err := os.Rename(partial, dst); err != nil {
		_ = os.Remove(partial)
		return fmt.Errorf("collector: rename %s: %w", dst, err)
	}
	return nil
}

// clearInUseFlag 修复「活跃 binlog 文件」回填副本的 FDE 校验不一致。
//
// mysqld 创建 binlog 文件时按 flags=0 计算并写入 FDE 校验和,随后直接把
// LOG_EVENT_BINLOG_IN_USE_F(0x0001)标志位写进 FDE 头而不重算 CRC(文件
// 干净关闭时同样只回写标志位)。因此正在写入的 binlog 文件,其 FDE 的严格
// CRC 校验(parser.SetVerifyChecksum(true),扫描引擎与封口验证均使用)必然
// 失败——线上实测 MySQL 8.0.46 的活跃文件即如此。
//
// 归档副本不是"使用中"文件:把标志位清零后 CRC 恰好恢复一致(等价于正常
// 关闭后的文件)。仅当清零使校验确实通过时才落盘修改;本来就一致的文件、
// 或清零也无法修复的文件(真损坏)都保持原样。
func clearInUseFlag(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	head := make([]byte, 23) // magic(4) + FDE header(19)
	if _, err := io.ReadFull(f, head); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil // 不足一个事件头(极短前缀),无可修复
		}
		return err
	}
	if !bytes.Equal(head[:4], []byte{0xfe, 0x62, 0x69, 0x6e}) ||
		head[8] != byte(replication.FORMAT_DESCRIPTION_EVENT) {
		return nil // 非 binlog 或首事件不是 FDE(不应发生),不动
	}
	if head[21] == 0 && head[22] == 0 {
		return nil // 无 in-use 标志
	}

	evSize := binary.LittleEndian.Uint32(head[13:17])
	if evSize <= replication.EventHeaderSize+replication.BinlogChecksumLength || evSize > 1<<16 {
		return nil // 尺度异常(FDE 实际约 120B),不动
	}
	ev := make([]byte, evSize)
	if _, err := f.ReadAt(ev, 4); err != nil {
		return nil // FDE 不完整(截断/损坏的副本),保持原样交给校验失败暴露
	}
	crcLen := replication.BinlogChecksumLength
	stored := binary.LittleEndian.Uint32(ev[len(ev)-crcLen:])
	if crc32.ChecksumIEEE(ev[:len(ev)-crcLen]) == stored {
		return nil // 校验本来就一致(含标志位的 CRC 变体),不动
	}
	patched := append([]byte(nil), ev...)
	patched[17] = 0
	patched[18] = 0
	if crc32.ChecksumIEEE(patched[:len(patched)-crcLen]) != stored {
		return nil // 清零也修不好(真损坏),不动
	}
	_, err = f.WriteAt([]byte{0, 0}, 21)
	return err
}

// saveStateRetry 持久化状态并重试：SaveState 是极小原子写，瞬态失败
// （IO 抖动/目录瞬时忙）用短退避重试几次；仍失败则返回错误。
func (l *Loop) saveStateRetry(ctx context.Context, st State) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if err = l.saveState(l.cfg.ArchiveDir, st); err == nil {
			return nil
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("collector: persist state %s canceled: %w", st.LastFile, ctx.Err())
		}
	}
	return fmt.Errorf("collector: persist state %s after 3 attempts: %w", st.LastFile, err)
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
				if ctx.Err() == nil {
					l.cfg.Logger.Info("collector: archive stream ended cleanly without rotation; stopping")
				}
				return nil // 流干净结束（正常停止）
			}
			if errors.Is(err, errStateAdvance) {
				// Seal 已成功、状态写盘失败：从内存中的最新状态继续，
				// 不重拉旧位置（尾部已落盘，重拉会重复消费）。
				l.cfg.Logger.Warn("collector: state persistence failed; continuing from sealed position", "err", err)
				next := l.State()
				if next.LastFile == "" || next.LastFile == pos.Name {
					return nil
				}
				l.resetBackoff()
				pos = mysql.Position{Name: next.LastFile, Pos: next.LastPos}
				continue
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
			if ctx.Err() == nil {
				l.cfg.Logger.Info("collector: segment complete but no rotation occurred; stopping", "file", pos.Name)
			}
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
// 流首即遇边界轮转（空尾部轮转）→ 当前文件无内容可封口，直接推进状态。
// 封口失败 → 文件已在 MySQL 侧轮转时整文件回填兜底（recoverSealedFile），
// 未轮转则维持退避重试；状态持久化失败 → 内存状态先行（errStateAdvance）。
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
		// 丢弃 master 重发的首个公告轮转 + 当前文件 FDE（它们不属于续拉内容），
		// 但区分公告轮转（NextLogName == pos.Name）与边界轮转（!=）：
		// 边界轮转而本段尚无内容事件 = 空尾部轮转（agent 追平+闲置时
		// FLUSH LOGS 的典型序列 [公告R, FDE, 边界R, ...]）——若把它当
		// preamble 吞掉，下一文件的首个内容事件会被写进当前文件的 .partial，
		// 组合验证通过后错档追加（归档缺档 + 事务重复）。
		first, boundary, err := skipStreamPreamble(ctx, src, pos.Name)
		if err != nil {
			l.cleanupPartials()
			return err
		}
		if boundary != "" {
			// 空尾部轮转：当前文件无内容可封口（无 partial 或空则跳过 Seal），
			// 推进状态到边界文件、正常返回，syncLoop 续下一文件。
			l.cleanupPartials()
			st := State{LastFile: boundary, LastPos: 0, UpdatedAt: time.Now()}
			if err := l.saveStateRetry(ctx, st); err != nil {
				l.setState(st)
				return errStateAdvance
			}
			l.setState(st)
			return nil
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
			// 封口失败也须清理 .partial（否则重试 O_APPEND 续写残留 partial
			// → tail+tail 重复追加）；区分瞬时失败（磁盘满/IO 抖动）与永久
			// 失败（final 被篡改/损坏）由 recoverSealedFile 判定：文件已在
			// MySQL 侧轮转 → 删除坏归档 + 整文件回填兜底；仍打开 → 退避重试。
			l.cleanupPartials()
			if recovered, rerr := l.recoverSealedFile(ctx, pos); recovered {
				return rerr // nil 或 errStateAdvance：不再从旧位置重拉
			} else if rerr != nil {
				return rerr
			}
			return err
		}
	} else {
		if err := l.w.Seal(pos.Name + ".partial"); err != nil {
			l.cleanupPartials()
			if recovered, rerr := l.recoverSealedFile(ctx, pos); recovered {
				return rerr
			} else if rerr != nil {
				return rerr
			}
			return err
		}
	}
	l.pruneRetention()

	st := State{LastFile: next, LastPos: 0, UpdatedAt: time.Now()}
	if err := l.saveStateRetry(ctx, st); err != nil {
		// Seal 已成功、尾部已落盘：内存状态先行，syncLoop 从新位置继续
		// （不从旧位置重拉——重拉会重复消费同一段尾部）。崩溃窗口内磁盘
		// 状态陈旧，重启后重拉由 SealAppendVerified 的追加幂等兜底。
		l.setState(st)
		return errStateAdvance
	}
	l.setState(st)
	return nil
}

// recoverSealedFile 处理封口失败（Seal/SealAppendVerified 永久失败，final
// 被篡改/损坏）时的「整文件拷贝兜底」（设计承诺：封口验证失败回退本地整文件
// 拷贝，保证零缺口）。
//
// 判定文件是否已在 MySQL 侧轮转封口：SHOW MASTER STATUS 的名字已越过它
// （序号文件名按字典序即时间序），或它已从 SHOW BINARY LOGS 消失（被 purge）。
//   - 已轮转：远端已有权威副本 → 删除坏归档文件（final + partial）、整文件
//     回填（backfillFile）、把状态推进到 pos.Name 之后的下一个文件，返回
//     (true, nil)；状态写盘失败则内存状态先行，返回 (true, errStateAdvance)。
//   - 仍打开：数据可能还在增长，不能信任整文件拷贝 → 返回 (false, nil)，
//     调用方维持退避重试。
//
// 查询失败返回 (false, err)。
func (l *Loop) recoverSealedFile(ctx context.Context, pos mysql.Position) (bool, error) {
	files, err := l.cfg.MySQL.ListBinlogs(ctx)
	if err != nil {
		return false, fmt.Errorf("collector: list binlogs during seal recovery: %w", err)
	}
	masterPos, err := l.cfg.MySQL.MasterPosition(ctx)
	if err != nil {
		return false, fmt.Errorf("collector: master position during seal recovery: %w", err)
	}
	rotated := masterPos.Name > pos.Name
	if !rotated {
		found := false
		for _, mf := range files {
			if mf.Name == pos.Name {
				found = true
				break
			}
		}
		rotated = !found // 已从 manifest 消失（被 purge）→ 视同已轮转
	}
	if !rotated {
		return false, nil
	}

	// 已轮转：删除坏归档，整文件回填（先删 final——Windows 上 rename 覆盖
	// 已存在文件会失败；partial 由 backfillFile 的 os.Create 截断重建）
	final := filepath.Join(l.cfg.ArchiveDir, pos.Name)
	_ = os.Remove(final)
	_ = os.Remove(final + ".partial")
	if err := l.backfillFile(ctx, pos.Name, masterPos); err != nil {
		return false, fmt.Errorf("collector: backfill %s after seal failure: %w", pos.Name, err)
	}

	// 状态推进到 pos.Name 之后的下一个文件（manifest 有序；兜底用 master pos）
	next := ""
	for _, mf := range files {
		if mf.Name > pos.Name {
			next = mf.Name
			break
		}
	}
	if next == "" {
		next = masterPos.Name
	}
	st := State{LastFile: next, LastPos: 0, UpdatedAt: time.Now()}
	if err := l.saveStateRetry(ctx, st); err != nil {
		l.setState(st)
		return true, errStateAdvance
	}
	l.setState(st)
	return true, nil
}

// skipStreamPreamble 丢弃流起始的公告轮转（fake ROTATE_EVENT）与当前文件的
// FORMAT_DESCRIPTION_EVENT，返回第一个内容事件；源为空（EOF）返回 (nil, "", nil)。
// 这些事件是 StartSync 时 master 为初始化 parser 重发的，不属于续拉内容——
// append 续写时必须剔除，否则会重复写入 FDE。
//
// 关键区分（final review C1）：流首 ROTATE_EVENT 有两种——
//   - 公告轮转：NextLogName == 本段文件（currentName），StartSync 时 master
//     重发的 fake rotate，跳过；
//   - 边界轮转：NextLogName != currentName——本段文件在 MySQL 侧已轮转封口。
//     若此时本段尚无内容事件（agent 追平+闲置，序列 [公告R, FDE, 边界R, ...]），
//     则返回 (nil, boundaryName, nil) 表示「空尾部轮转」，调用方跳过封口、
//     把状态推进到边界文件；若已有内容事件，边界轮转由 Consume* 正常结束段。
func skipStreamPreamble(ctx context.Context, src binlog.Source, currentName string) (*replication.BinlogEvent, string, error) {
	for {
		ev, err := src.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, "", nil
			}
			return nil, "", err
		}
		if ev.Header == nil {
			continue
		}
		switch ev.Header.EventType {
		case replication.ROTATE_EVENT:
			name, err := archive.RotateNextLogName(ev)
			if err != nil {
				return nil, "", err
			}
			if name != currentName {
				return nil, name, nil // 边界轮转（空尾部轮转）
			}
			continue // 公告轮转（fake rotate）：跳过
		case replication.FORMAT_DESCRIPTION_EVENT:
			continue // 当前文件被重发的 FDE：跳过
		default:
			return ev, "", nil
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
