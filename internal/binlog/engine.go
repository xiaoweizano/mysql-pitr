package binlog

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Scanner 扫描 binlog 文件并产出 Transaction 流。
type Scanner interface {
	Scan(ctx context.Context, f Filter) error
	Next() (*Transaction, error) // 返回 io.EOF 表示扫完
	Close() error
}

type scanner struct {
	sf           SchemaFetcher
	maxRowsPerTx int
	logger       *slog.Logger

	// 运行状态
	mu      sync.Mutex
	started bool
	closed  bool
	parser  *replication.BinlogParser
	txs     chan *Transaction // 解析协程 → Next() 消费
	errs    chan error        // 解析错误
	done    chan struct{}     // 解析协程结束信号
}

// Option 配置 Scanner。
type Option func(*scanner)

// WithMaxRowsPerTx 设置单事务最大行数；超过则截断 + 标 Truncated。
// 0 表示无限制。默认 1_000_000。
func WithMaxRowsPerTx(n int) Option {
	return func(s *scanner) { s.maxRowsPerTx = n }
}

// WithLogger 注入 slog logger；默认 slog.Default()。
func WithLogger(l *slog.Logger) Option {
	return func(s *scanner) { s.logger = l }
}

// NewScanner 创建一个 Scanner。同一个 Scanner 不可重入 Scan。
func NewScanner(sf SchemaFetcher, opts ...Option) Scanner {
	s := &scanner{
		sf:           sf,
		maxRowsPerTx: 1_000_000,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *scanner) Scan(ctx context.Context, f Filter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return fmt.Errorf("binlog: scanner already started (create a new scanner for a new scan)")
	}
	if s.closed {
		return fmt.Errorf("binlog: scanner closed")
	}
	if f.BinlogDir == "" {
		return fmt.Errorf("binlog: Filter.BinlogDir is required")
	}
	files, err := EnumerateBinlogFiles(f.BinlogDir, f.StartPos, f.EndPos)
	if err != nil {
		return fmt.Errorf("binlog: enumerate files: %w", err)
	}

	s.parser = replication.NewBinlogParser()
	s.parser.SetVerifyChecksum(true)
	s.txs = make(chan *Transaction, 16)
	s.errs = make(chan error, 1)
	s.done = make(chan struct{})
	s.started = true

	go s.runParseLoop(ctx, files, f)
	return nil
}

// Next 返回下一个事务；扫完返回 io.EOF，解析出错返回错误。
//
// 顺序保证：runParseLoop 用 LIFO defers 关闭 channel —— close(s.errs) 先于
// close(s.txs) 执行。因此消费者一旦观察到 txs 关闭（ok==false），errs 必然
// 已关闭，此时读 errs 不会阻塞；若解析出错，错误仍留在 errs 缓冲里，绝不会
// 被 io.EOF 吞掉。所以这里先阻塞等 txs，等 txs 关闭后再读 errs。
func (s *scanner) Next() (*Transaction, error) {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return nil, fmt.Errorf("binlog: scan not started")
	}
	tx, ok := <-s.txs
	if !ok {
		// txs 已关闭 → errs 必已关闭（LIFO defers）；取出待处理的解析错误，否则 EOF。
		if err, ok := <-s.errs; ok && err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	return tx, nil
}

func (s *scanner) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// 中断解析协程：emit 的 select 观察到 done 关闭后立即返回，避免消费者中途
	// 放弃（不再调用 Next）时解析协程永久阻塞在 s.txs <-（缓冲满）并泄漏。
	// done 由 Scan 创建；未 Scan 过则为 nil（此时也不可能有解析协程）。
	// closed 标志保证幂等：只有第一次 Close 会走到这里。
	if s.done != nil {
		close(s.done)
	}
	return nil
}

// runParseLoop 是核心解析循环；按文件顺序解析并把事务发到 s.txs。
func (s *scanner) runParseLoop(ctx context.Context, files []string, f Filter) {
	defer close(s.txs)
	defer close(s.errs)

	for _, file := range files {
		if err := s.parseFile(ctx, file, f); err != nil {
			s.errs <- err
			return
		}
	}
}

// parseFile 打开单个 binlog 文件（经 FileSource 后台解析），逐个消费事件，
// 并在事务边界（XID / COMMIT / DDL）聚合出 Transaction 发到 s.txs。
func (s *scanner) parseFile(ctx context.Context, path string, f Filter) error {
	// 起始偏移只对与 StartPos.Name 匹配的文件生效；其余文件（StartPos 之后的
	// 后续文件）从头解析，避免把第一个文件的偏移误用到后续文件。
	offset := int64(0)
	if f.StartPos.Name != "" && filepath.Base(path) == f.StartPos.Name {
		offset = int64(f.StartPos.Pos)
	}
	src, err := OpenFileSource(ctx, path, offset, s.parser)
	if err != nil {
		return err
	}
	defer src.Close()

	// 当前未提交事务的累积状态
	var pending *pendingTx
	tableMaps := map[uint64]*replication.TableMapEvent{}

	for {
		// FileSource 内部也处理取消，但取消后它可能以 io.EOF 收尾（errs 只收
		// 非 context.Canceled 错误）；这里显式检查，保证取消必然以 ctx 错误
		// 结束而不是被伪装成 EOF。
		if err := ctx.Err(); err != nil {
			return err
		}
		ev, err := src.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("binlog: parse events in %s: %w", path, err)
		}
		// EndPos.Pos 只在 EndPos.Name 非空且等于当前文件名时生效：事件结束
		// 位置（LogPos）超过 EndPos.Pos 即停止解析，不再继续。LogPos == Pos
		// 的事件（恰好结束于边界）仍被处理。
		if f.EndPos.Name != "" && filepath.Base(path) == f.EndPos.Name && ev.Header.LogPos > f.EndPos.Pos {
			break
		}
		if err := s.handleEvent(ev, &pending, tableMaps, f); err != nil {
			return err
		}
	}
	return nil
}

// handleEvent 处理单个 binlog 事件，按事务边界（XID / COMMIT / DDL）聚合出
// Transaction 并 emit。取消由 parseFile 循环与 FileSource 处理，这里不需要
// 再检查 ctx。
func (s *scanner) handleEvent(ev *replication.BinlogEvent, pending **pendingTx, tableMaps map[uint64]*replication.TableMapEvent, f Filter) error {
	s.logger.Debug("event", "type", ev.Header.EventType, "pos", ev.Header.LogPos)

	switch e := ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		// 让 parser 自己处理
	case *replication.RotateEvent:
		// 文件切换；忽略
	case *replication.TableMapEvent:
		tableMaps[e.TableID] = e
	case *replication.RowsEvent:
		rcs, err := s.rowChangeFromEvent(e, ev.Header.EventType, tableMaps)
		if err != nil {
			s.logger.Warn("skip row event", "err", err)
			return nil
		}
		if *pending == nil {
			*pending = &pendingTx{}
		}
		(*pending).rows = append((*pending).rows, rcs...)

	case *replication.QueryEvent:
		// QueryEvent 可能是事务边界（BEGIN / COMMIT）或 DDL
		q := string(e.Query)
		switch q {
		case "BEGIN":
			if *pending == nil {
				*pending = &pendingTx{}
			}
			// 保留可能已记录的 GTID（GTIDEvent 在 BEGIN 之前到达）
			(*pending).schema = string(e.Schema)
		case "COMMIT":
			if *pending != nil {
				(*pending).commitTs = eventTime(ev.Header)
				if err := s.emit(*pending, f); err != nil {
					return err
				}
				*pending = nil
			}
		default:
			// DDL：忽略（reverse 会标 warning），如果之前有 pending 也 emit
			if *pending != nil {
				if err := s.emit(*pending, f); err != nil {
					return err
				}
				*pending = nil
			}
		}
	case *replication.XIDEvent:
		// XID = autocommit 提交点
		if *pending != nil {
			(*pending).xid = e.XID
			(*pending).commitTs = eventTime(ev.Header)
			if err := s.emit(*pending, f); err != nil {
				return err
			}
			*pending = nil
		}
	case *replication.GTIDEvent:
		// GTID 事件出现在事务开头；记到 pending。
		// Anonymous GTID（SID 全 0）不是真实 GTID，跳过。
		if *pending == nil {
			*pending = &pendingTx{}
		}
		if !isZeroSID(e.SID) {
			gtid := formatGTID(e.SID, e.GNO)
			if gs, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtid); err == nil {
				(*pending).gtidSet = gs
				(*pending).gtid = gtid
			}
		}
	case *replication.MariadbGTIDEvent:
		// MariaDB；类似处理
		if *pending == nil {
			*pending = &pendingTx{}
		}
	default:
		// 其他事件忽略
	}
	return nil
}

// pendingTx 是当前未提交事务的累积
type pendingTx struct {
	schema   string
	rows     []RowChange
	xid      uint64
	gtid     string
	gtidSet  mysql.GTIDSet
	commitTs time.Time
}

// emit 把 pending 转成 Transaction 并发送到 s.txs。
func (s *scanner) emit(p *pendingTx, f Filter) error {
	if len(p.rows) == 0 {
		return nil // 空事务不输出
	}
	if p.commitTs.IsZero() {
		p.commitTs = time.Now().UTC() // fallback；正常情况下事件 header 时间就是 commit 时间
	}
	tx, err := NewTransaction(p.gtid, p.xid, p.commitTs, p.schema)
	if err != nil {
		return err
	}
	tx.Statements = p.rows

	// 应用 Filter
	if !s.matchesFilter(&tx, f) {
		return nil
	}

	// 截断：Filter.MaxRowsPerTx 优先；否则用 scanner 级 option（WithMaxRowsPerTx）
	limit := f.MaxRowsPerTx
	if limit == 0 {
		limit = s.maxRowsPerTx
	}
	if limit > 0 && len(tx.Statements) > limit {
		tx.Statements = tx.Statements[:limit]
		tx.MarkTruncated()
	}

	// 先尝试非阻塞投递：Close 之后消费者若仍在取数（缓冲区有空位），事务仍会
	// 送达（与既有测试"Close 后取完剩余事务"的语义一致）；仅当缓冲区满（消费者
	// 放弃）时才进入阻塞 select，由 done 中断，避免永久阻塞。
	select {
	case s.txs <- &tx:
		return nil
	default:
	}
	select {
	case s.txs <- &tx:
	case <-s.done: // closed by Close
	}
	return nil
}

// rowChangeFromEvent 把 RowsEvent 的每一行 image 转成 RowChange。
func (s *scanner) rowChangeFromEvent(e *replication.RowsEvent, eventType replication.EventType, tableMaps map[uint64]*replication.TableMapEvent) ([]RowChange, error) {
	tm := tableMaps[e.TableID]
	if tm == nil {
		return nil, fmt.Errorf("table map not found for table_id %d", e.TableID)
	}
	var out []RowChange
	switch eventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for _, row := range e.Rows {
			out = append(out, RowChange{
				Schema:      string(tm.Schema),
				Table:       string(tm.Table),
				Action:      ActionInsert,
				After:       interfaceSlice(row),
				ColumnNames: nil,
			})
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		// e.Rows 是 [before, after, before, after, ...] 配对
		for i := 0; i+1 < len(e.Rows); i += 2 {
			out = append(out, RowChange{
				Schema:      string(tm.Schema),
				Table:       string(tm.Table),
				Action:      ActionUpdate,
				Before:      interfaceSlice(e.Rows[i]),
				After:       interfaceSlice(e.Rows[i+1]),
				ColumnNames: nil,
			})
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for _, row := range e.Rows {
			out = append(out, RowChange{
				Schema:      string(tm.Schema),
				Table:       string(tm.Table),
				Action:      ActionDelete,
				Before:      interfaceSlice(row),
				ColumnNames: nil,
			})
		}
	default:
		return nil, fmt.Errorf("unsupported rows event type %v", eventType)
	}
	return out, nil
}

// interfaceSlice 拷贝行 image，避免共享 parser 内部缓冲。
func interfaceSlice(row []interface{}) []interface{} {
	out := make([]interface{}, len(row))
	copy(out, row)
	return out
}

// matchesFilter 应用 Filter（TimeRange / GTIDSet / Tables）。
func (s *scanner) matchesFilter(tx *Transaction, f Filter) bool {
	if f.TimeRange != nil {
		if tx.CommitTime.Before(f.TimeRange.Start) || tx.CommitTime.After(f.TimeRange.End) {
			return false
		}
	}
	if f.GTIDSet != nil && tx.GTID != "" {
		if !MatchGTID(f.GTIDSet, tx.GTID) {
			return false
		}
	}
	// SelectedTxIDs：SELECTED_SQL 定向二次扫描，只保留 TxID 命中的事务
	if len(f.SelectedTxIDs) > 0 {
		found := false
		for _, id := range f.SelectedTxIDs {
			if id == tx.TxID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(f.Tables) > 0 {
		ok := false
		for _, want := range f.Tables {
			for _, rc := range tx.Statements {
				if rc.Schema == want.Schema && rc.Table == want.Table {
					ok = true
					break
				}
			}
			if ok {
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// eventTime 把事件头时间戳转成 UTC time.Time。
func eventTime(h *replication.EventHeader) time.Time {
	return time.Unix(int64(h.Timestamp), 0).UTC()
}

// formatGTID 把 GTIDEvent 的 SID/GNO 转成 "uuid:gno" 字符串。
func formatGTID(sid []byte, gno int64) string {
	if len(sid) != 16 {
		return ""
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x:%d", sid[0:4], sid[4:6], sid[6:8], sid[8:10], sid[10:16], gno)
}

// isZeroSID 判断 SID 是否全 0（Anonymous GTID）。
func isZeroSID(sid []byte) bool {
	for _, b := range sid {
		if b != 0 {
			return false
		}
	}
	return true
}
