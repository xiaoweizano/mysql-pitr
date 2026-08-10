package binlog

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
)

// Source 是一次 binlog 事件流迭代器；io.EOF 表示流结束。
// 实现：FileSource（本地文件）、internal/stream（binlogsyncer）。
type Source interface {
	Next(ctx context.Context) (*replication.BinlogEvent, error)
	Close() error
}

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e} // "\xfe\x62\x69\x6e"

// FileSource 顺序解析单个 binlog 文件。goroutine 后台跑 ParseReader（内部
// 已处理 missing-table-map 跳过与校验和），Next 从 channel 拉取。
type FileSource struct {
	ctx    context.Context
	cancel context.CancelFunc
	evs    chan *replication.BinlogEvent
	errs   chan error
	done   chan struct{}
	once   sync.Once
}

// OpenFileSource 打开文件、校验 magic，并从 offset 处开始后台解析。
// offset <= 4 表示从文件头开始；offset > 4 时先从文件头重读 FDE
// （与 go-mysql ParseFile 的语义一致），再跳到目标偏移开始解析。
func OpenFileSource(ctx context.Context, path string, offset int64, parser *replication.BinlogParser) (*FileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("binlog: open %s: %w", path, err)
	}
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		f.Close()
		return nil, fmt.Errorf("binlog: read magic %s: %w", path, err)
	}
	if string(magic) != string(binlogMagic) {
		f.Close()
		return nil, fmt.Errorf("binlog: bad magic in %s", path)
	}
	if offset < 4 {
		offset = 4
	}
	if offset > 4 {
		// 从文件中间开始（StartPos.Pos 等场景）必须先重读 FDE 初始化 parser
		// 的 format 状态（校验和算法、事件头长度表）——否则 ParseReader 从中间
		// 开始遇到 TableMap/Rows 事件时 p.format 为 nil，go-mysql 会 nil 解引用
		// panic（StartPos.Pos 引擎级测试暴露）。重读的 FDE 不投递给调用方
		// （no-op callback）；后续 ParseReader 会正常校验 CRC。
		if _, err := f.Seek(4, io.SeekStart); err != nil {
			f.Close()
			return nil, fmt.Errorf("binlog: seek %s to 4: %w", path, err)
		}
		if _, err := parser.ParseSingleEvent(f, func(*replication.BinlogEvent) error { return nil }); err != nil {
			f.Close()
			return nil, fmt.Errorf("binlog: re-parse FDE %s: %w", path, err)
		}
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("binlog: seek %s to %d: %w", path, offset, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &FileSource{
		ctx:    ctx,
		cancel: cancel,
		evs:    make(chan *replication.BinlogEvent, 16),
		errs:   make(chan error, 1),
		done:   make(chan struct{}),
	}
	go s.run(ctx, f, parser)
	return s, nil
}

func (s *FileSource) run(ctx context.Context, f *os.File, parser *replication.BinlogParser) {
	defer close(s.evs)
	defer close(s.errs)
	defer f.Close()

	err := parser.ParseReader(f, func(ev *replication.BinlogEvent) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.evs <- ev:
			return nil
		}
	})
	if err != nil && err != context.Canceled {
		s.errs <- err
	}
}

func (s *FileSource) Next(ctx context.Context) (*replication.BinlogEvent, error) {
	select {
	case ev, ok := <-s.evs:
		if !ok {
			if err, ok := <-s.errs; ok {
				return nil, err
			}
			return nil, io.EOF
		}
		return ev, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *FileSource) Close() error {
	s.once.Do(func() { s.cancel() })
	return nil
}
