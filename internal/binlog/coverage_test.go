package binlog

// 本文件补齐覆盖率缺口。核心手段是手工构造 binlog 字节流，让 Scanner
// 走完真实解析管线，从而覆盖单靠 testdata fixture 无法触达的分支：
// BEGIN/COMMIT/DDL 事件、XID 空 pending、Anonymous GTID（全 0 SID）、
// MariaDB GTID、PARTIAL_UPDATE_ROWS_EVENT 跳过路径、Filter 拒绝路径等。
//
// 底层 craft* 构造 helpers 已迁至 internal/binlogtest（导出为 CraftXXX），
// 供 binlog / archive / stream 等包测试共用；本文件只保留便捷包装。

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlogtest"
)

// ---------- 手工构造 binlog 的 helpers ----------

// craftedBinlog 把 FDE + 事件序列组装成完整 binlog 文件字节。
func craftedBinlog(events ...binlogtest.Event) []byte {
	fde := binlogtest.MustCraft(binlogtest.CraftFDE())
	return binlogtest.CraftFile(append([]binlogtest.Event{fde}, events...))
}

// craftTestBinlog 生成覆盖核心解析分支的 binlog：
//
//	GTID(uuid:10) BEGIN TableMap WRITE[1,2] XID(100)        → tx1（2 行）
//	TableMap WRITE[3] XID(101)                              → tx2（1 行，无 GTID）
//	GTID(全0) GTID(uuid:11) BEGIN TableMap WRITE[4] DDL     → tx3（DDL flush，commitTs 回退 now）
//	COMMIT(无 pending) MariaDBGTID BEGIN COMMIT XID(200)
func craftTestBinlog() []byte {
	const sid = "3f9a5c8e1234567890abcdef01234567"
	return craftedBinlog(
		binlogtest.MustCraft(binlogtest.CraftGTID(sid, 10)),
		binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN", "shop")),
		binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
		binlogtest.MustCraft(binlogtest.CraftWriteRowsValues(1, 1, 2)),
		binlogtest.MustCraft(binlogtest.CraftXID(100)),
		binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
		binlogtest.MustCraft(binlogtest.CraftWriteRowsValues(1, 3)),
		binlogtest.MustCraft(binlogtest.CraftXID(101)),
		binlogtest.MustCraft(binlogtest.CraftGTID("", 0)),
		binlogtest.MustCraft(binlogtest.CraftGTID(sid, 11)),
		binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN", "shop")),
		binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
		binlogtest.MustCraft(binlogtest.CraftWriteRowsValues(1, 4)),
		binlogtest.MustCraft(binlogtest.CraftQuery("ALTER TABLE orders ADD COLUMN note VARCHAR(16)", "shop")),
		binlogtest.MustCraft(binlogtest.CraftQuery("COMMIT")),
		binlogtest.MustCraft(binlogtest.CraftMariaDBGTID(1, 0, 0)),
		binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN", "shop")),
		binlogtest.MustCraft(binlogtest.CraftQuery("COMMIT")),
		binlogtest.MustCraft(binlogtest.CraftXID(200)),
	)
}

// craftMultiTxBinlog 生成 n 个独立事务（GTID + BEGIN + TableMap + WRITE + XID），
// 用于填满 txs 缓冲、制造 emit 阻塞的 Close 中断/泄漏回归场景。
func craftMultiTxBinlog(n int) []byte {
	const sid = "3f9a5c8e1234567890abcdef01234567"
	evs := make([]binlogtest.Event, 0, n*5)
	for i := 0; i < n; i++ {
		evs = append(evs,
			binlogtest.MustCraft(binlogtest.CraftGTID(sid, int64(1000+i))),
			binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN", "shop")),
			binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
			binlogtest.MustCraft(binlogtest.CraftWriteRowsValues(1, int64(i))),
			binlogtest.MustCraft(binlogtest.CraftXID(uint64(1000+i))),
		)
	}
	return craftedBinlog(evs...)
}

func writeBinlog(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0o644))
	return dir
}

func scanAll(t *testing.T, sc Scanner) []*Transaction {
	t.Helper()
	var txs []*Transaction
	for {
		tx, err := sc.Next()
		if err == io.EOF {
			return txs
		}
		require.NoError(t, err)
		txs = append(txs, tx)
	}
}

// ---------- 匿名事务确定性 TxID（final review Important #2） ----------

// craftAnonTxBinlog 构造一个匿名事务（无 GTID 无 XID）的 binlog 文件：
// TableMap + WRITE[7] + COMMIT（COMMIT QueryEvent 触发 emit，无 XID）。
func craftAnonTxBinlog() []byte {
	return craftedBinlog(
		binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
		binlogtest.MustCraft(binlogtest.CraftWriteRowsValues(1, 7)),
		binlogtest.MustCraft(binlogtest.CraftQuery("COMMIT")),
	)
}

// TestScanner_AnonymousTxDeterministicTxID 回归 final review Important #2：
// 无 GTID 无 XID 的匿名事务 TxID 必须是确定性哈希——同一文件两次扫描得到
// 相同 TxID（SELECTED_SQL 两阶段定向二次扫描的匹配基础），而非随机占位。
func TestScanner_AnonymousTxDeterministicTxID(t *testing.T) {
	dir := writeBinlog(t, craftAnonTxBinlog())

	first := scanFilter(t, Filter{BinlogDir: dir})
	require.Len(t, first, 1)
	tx := first[0]
	require.Empty(t, tx.GTID)
	require.Zero(t, tx.XID)
	require.True(t, strings.HasPrefix(tx.TxID, "tx-"), "TxID = %q", tx.TxID)
	require.Len(t, tx.TxID, 3+64, "TxID = %q，应为 tx- + 64 位 hex sha256", tx.TxID)

	second := scanFilter(t, Filter{BinlogDir: dir})
	require.Len(t, second, 1)
	require.Equal(t, tx.TxID, second[0].TxID, "同一文件两次扫描匿名事务 TxID 必须一致")

	hit := scanFilter(t, Filter{BinlogDir: dir, SelectedTxIDs: []string{tx.TxID}})
	require.Len(t, hit, 1, "SELECTED_SQL 定向二次扫描必须命中")
	require.Equal(t, tx.TxID, hit[0].TxID)
}

// ---------- 解析分支覆盖 ----------

// TestScanner_CraftedEventSequence 用构造的 binlog 覆盖 GTID/BEGIN/DDL/COMMIT/
// XID/Anonymous-GTID/MariaDB-GTID 分支，产出 3 个 DML 事务。
func TestScanner_CraftedEventSequence(t *testing.T) {
	dir := writeBinlog(t, craftTestBinlog())
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir}))

	txs := scanAll(t, sc)
	require.Len(t, txs, 3, "want 3 DML transactions; got %+v", txs)

	// tx1：GTID uuid:10 + XID 100，2 行 INSERT
	assert.Equal(t, "3f9a5c8e-1234-5678-90ab-cdef01234567:10", txs[0].GTID)
	assert.Equal(t, "3f9a5c8e-1234-5678-90ab-cdef01234567:10", txs[0].TxID)
	assert.Equal(t, uint64(100), txs[0].XID)
	assert.Equal(t, "shop", txs[0].Schema)
	assert.Equal(t, time.Unix(1750000000, 0).UTC(), txs[0].CommitTime)
	require.Len(t, txs[0].Statements, 2)
	for i, rc := range txs[0].Statements {
		assert.Equal(t, ActionInsert, rc.Action)
		assert.Equal(t, "shop", rc.Schema)
		assert.Equal(t, "orders", rc.Table)
		assert.Equal(t, []interface{}{int64(1 + i)}, rc.After, "row %d", i)
	}

	// tx2：无 GTID，XID 101，1 行
	assert.Empty(t, txs[1].GTID)
	assert.Equal(t, "xid-101", txs[1].TxID)
	assert.Equal(t, uint64(101), txs[1].XID)
	require.Len(t, txs[1].Statements, 1)
	assert.Equal(t, int64(3), txs[1].Statements[0].After[0])

	// tx3：DDL 触发 flush，GTID uuid:11，commitTs 走 fallback（now）
	assert.Equal(t, "3f9a5c8e-1234-5678-90ab-cdef01234567:11", txs[2].GTID)
	assert.Equal(t, uint64(0), txs[2].XID)
	require.Len(t, txs[2].Statements, 1)
	assert.Equal(t, int64(4), txs[2].Statements[0].After[0])
	assert.WithinDuration(t, time.Now().UTC(), txs[2].CommitTime, time.Minute)
}

// TestScanner_CraftedPartialUpdateSkips 覆盖 RowsEvent 转换失败的跳过路径
// （PARTIAL_UPDATE_ROWS_EVENT 是 RowsEvent 但事件类型不受支持 → Warn + 跳过）。
func TestScanner_CraftedPartialUpdateSkips(t *testing.T) {
	data := craftedBinlog(
		binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)),
		binlogtest.MustCraft(binlogtest.CraftPartialUpdateRows(1, 1, 2)),
		binlogtest.MustCraft(binlogtest.CraftQuery("BEGIN", "shop")),
		binlogtest.MustCraft(binlogtest.CraftQuery("COMMIT")),
	)
	dir := writeBinlog(t, data)
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir}))
	txs := scanAll(t, sc)
	assert.Empty(t, txs, "unsupported rows event must be skipped, not emitted")
}

// TestScanner_CraftedMaxRowsPerTxFilter 覆盖 Filter.MaxRowsPerTx（非 0）的截断路径。
func TestScanner_CraftedMaxRowsPerTxFilter(t *testing.T) {
	dir := writeBinlog(t, craftTestBinlog())
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir, MaxRowsPerTx: 1}))
	txs := scanAll(t, sc)
	require.Len(t, txs, 3)
	assert.True(t, txs[0].Truncated, "2-row tx must be truncated to 1 row")
	assert.Len(t, txs[0].Statements, 1)
	assert.False(t, txs[1].Truncated)
	assert.False(t, txs[2].Truncated)
}

// TestScanner_CraftedTimeRangeRejects 覆盖 emit 中 matchesFilter 拒绝事务的路径。
func TestScanner_CraftedTimeRangeRejects(t *testing.T) {
	ts := time.Unix(1750000000, 0).UTC()
	dir := writeBinlog(t, craftTestBinlog())
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{
		BinlogDir: dir,
		TimeRange: &TimeRange{Start: ts.Add(-2 * time.Hour), End: ts.Add(-time.Hour)},
	}))
	txs := scanAll(t, sc)
	assert.Empty(t, txs, "all transactions must be filtered out by TimeRange")
}

// TestScanner_CraftedTimeRangeAccepts 校验 TimeRange 命中时事务正常产出。
func TestScanner_CraftedTimeRangeAccepts(t *testing.T) {
	ts := time.Unix(1750000000, 0).UTC()
	dir := writeBinlog(t, craftTestBinlog())
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{
		BinlogDir: dir,
		TimeRange: &TimeRange{Start: ts.Add(-time.Hour), End: ts.Add(time.Hour)},
	}))
	// tx1/tx2 的 commitTs=ts 在区间内；tx3 的 commitTs=now（fallback）在区间外
	assert.Len(t, scanAll(t, sc), 2)
}

// TestScanner_CraftedTablesFilter 覆盖 Tables 过滤命中与不命中。
func TestScanner_CraftedTablesFilter(t *testing.T) {
	dir := writeBinlog(t, craftTestBinlog())

	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir, Tables: []TableRef{{"shop", "orders"}}}))
	assert.Len(t, scanAll(t, sc), 3)

	sc2 := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc2.Scan(context.Background(), Filter{BinlogDir: dir, Tables: []TableRef{{"other", "table"}}}))
	assert.Empty(t, scanAll(t, sc2))
}

// ---------- Scanner 生命周期错误路径 ----------

func TestScanner_ScanTwice(t *testing.T) {
	dir := writeBinlog(t, craftTestBinlog())
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir}))
	err := sc.Scan(context.Background(), Filter{BinlogDir: dir})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already started")
	scanAll(t, sc)
}

func TestScanner_ScanAfterClose(t *testing.T) {
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Close())
	err := sc.Scan(context.Background(), Filter{BinlogDir: t.TempDir()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanner closed")
}

func TestScanner_ScanInvalidDir(t *testing.T) {
	sc := NewScanner(StaticSchemaFetcher{})
	err := sc.Scan(context.Background(), Filter{BinlogDir: filepath.Join(t.TempDir(), "nope")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "enumerate")
}

// TestScanner_CloseMidScan 校验解析协程运行期间调用 Close 安全（幂等、不死锁），
// 且 Close 后仍能取完剩余事务。
func TestScanner_CloseMidScan(t *testing.T) {
	dir := writeBinlog(t, craftTestBinlog())
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir}))
	require.NoError(t, sc.Close())
	require.NoError(t, sc.Close())
	assert.Len(t, scanAll(t, sc), 3)
}

// TestScanner_CtxCanceled 覆盖 onEvent 里的 ctx.Done() 分支：取消的上下文
// 使解析立即中止并经 Next() 报错。
func TestScanner_CtxCanceled(t *testing.T) {
	dir := writeBinlog(t, craftTestBinlog())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sc := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, sc.Scan(ctx, Filter{BinlogDir: dir}))
	_, err := sc.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

// ---------- parseFile 错误路径（白盒） ----------

func TestScanner_ParseFileOpenError(t *testing.T) {
	s := &scanner{}
	err := s.parseFile(context.Background(), filepath.Join(t.TempDir(), "missing.bin"), Filter{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "binlog: open")
}

func TestScanner_ParseFileEmptyMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	require.NoError(t, os.WriteFile(path, nil, 0o644))
	s := &scanner{}
	err := s.parseFile(context.Background(), path, Filter{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read magic")
}

func TestScanner_ParseFileBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	require.NoError(t, os.WriteFile(path, []byte{0xde, 0xad, 0xbe, 0xef}, 0o644))
	s := &scanner{}
	err := s.parseFile(context.Background(), path, Filter{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad magic")
}

// ---------- matchesFilter 白盒 ----------

func TestScanner_MatchesFilter(t *testing.T) {
	ts := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	gtidSet, err := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-10")
	require.NoError(t, err)

	s := &scanner{}
	tx := &Transaction{
		GTID:       "de278ad0-2106-11e4-9f8e-6edd0ca20947:5",
		CommitTime: ts,
		Statements: []RowChange{
			{Schema: "shop", Table: "orders"},
			{Schema: "shop", Table: "users"},
		},
	}

	// 无过滤 → 命中
	assert.True(t, s.matchesFilter(tx, Filter{}))

	// TimeRange
	assert.True(t, s.matchesFilter(tx, Filter{TimeRange: &TimeRange{Start: ts.Add(-time.Hour), End: ts.Add(time.Hour)}}))
	assert.False(t, s.matchesFilter(tx, Filter{TimeRange: &TimeRange{Start: ts.Add(time.Hour), End: ts.Add(2 * time.Hour)}}), "commit before Start")
	assert.False(t, s.matchesFilter(tx, Filter{TimeRange: &TimeRange{Start: ts.Add(-2 * time.Hour), End: ts.Add(-time.Hour)}}), "commit after End")

	// GTIDSet
	assert.True(t, s.matchesFilter(tx, Filter{GTIDSet: gtidSet}))
	outSet, err := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-3")
	require.NoError(t, err)
	assert.False(t, s.matchesFilter(tx, Filter{GTIDSet: outSet}))
	// GTID 为空 → GTIDSet 过滤被跳过
	assert.True(t, s.matchesFilter(&Transaction{CommitTime: ts}, Filter{GTIDSet: gtidSet}))

	// Tables
	assert.True(t, s.matchesFilter(tx, Filter{Tables: []TableRef{{"shop", "users"}}}))
	assert.False(t, s.matchesFilter(tx, Filter{Tables: []TableRef{{"other", "users"}}}))
	// 第一个 table 不命中、第二个命中 → 通过
	assert.True(t, s.matchesFilter(tx, Filter{Tables: []TableRef{{"x", "y"}, {"shop", "orders"}}}))
	// 空 statements → 不命中
	assert.False(t, s.matchesFilter(&Transaction{CommitTime: ts}, Filter{Tables: []TableRef{{"shop", "orders"}}}))
}

// ---------- rowChangeFromEvent 白盒 ----------

func TestScanner_RowChangeFromEvent(t *testing.T) {
	s := &scanner{}
	tableMaps := map[uint64]*replication.TableMapEvent{
		1: {Schema: []byte("shop"), Table: []byte("orders")},
	}

	// TableMap 缺失
	_, err := s.rowChangeFromEvent(&replication.RowsEvent{TableID: 99}, replication.WRITE_ROWS_EVENTv2, tableMaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table map not found")

	// 不支持的 RowsEvent 类型
	_, err = s.rowChangeFromEvent(&replication.RowsEvent{TableID: 1}, replication.QUERY_EVENT, tableMaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported rows event type")

	// INSERT v1/v2
	for _, et := range []replication.EventType{replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2} {
		rcs, err := s.rowChangeFromEvent(&replication.RowsEvent{
			TableID: 1,
			Rows:    [][]interface{}{{int64(1), "a"}, {int64(2), "b"}},
		}, et, tableMaps)
		require.NoError(t, err)
		require.Len(t, rcs, 2)
		assert.Equal(t, ActionInsert, rcs[0].Action)
		assert.Equal(t, "shop", rcs[0].Schema)
		assert.Equal(t, "orders", rcs[0].Table)
		assert.Equal(t, []interface{}{int64(1), "a"}, rcs[0].After)
		assert.Nil(t, rcs[0].Before)
	}

	// UPDATE v1/v2：before/after 配对
	for _, et := range []replication.EventType{replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2} {
		rcs, err := s.rowChangeFromEvent(&replication.RowsEvent{
			TableID: 1,
			Rows:    [][]interface{}{{int64(1)}, {int64(10)}, {int64(2)}, {int64(20)}},
		}, et, tableMaps)
		require.NoError(t, err)
		require.Len(t, rcs, 2)
		assert.Equal(t, ActionUpdate, rcs[0].Action)
		assert.Equal(t, []interface{}{int64(1)}, rcs[0].Before)
		assert.Equal(t, []interface{}{int64(10)}, rcs[0].After)
		assert.Equal(t, []interface{}{int64(2)}, rcs[1].Before)
		assert.Equal(t, []interface{}{int64(20)}, rcs[1].After)
	}

	// DELETE v1/v2
	for _, et := range []replication.EventType{replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2} {
		rcs, err := s.rowChangeFromEvent(&replication.RowsEvent{
			TableID: 1,
			Rows:    [][]interface{}{{int64(1)}},
		}, et, tableMaps)
		require.NoError(t, err)
		require.Len(t, rcs, 1)
		assert.Equal(t, ActionDelete, rcs[0].Action)
		assert.Equal(t, []interface{}{int64(1)}, rcs[0].Before)
		assert.Nil(t, rcs[0].After)
	}
}

// ---------- 引擎小 helper 白盒 ----------

func TestScanner_WithLoggerOption(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sc := NewScanner(StaticSchemaFetcher{}, WithLogger(logger))
	inner, ok := sc.(*scanner)
	require.True(t, ok)
	assert.Same(t, logger, inner.logger)
}

func TestFormatGTID(t *testing.T) {
	sid := []byte{0x3f, 0x9a, 0x5c, 0x8e, 0x12, 0x34, 0x56, 0x78,
		0x90, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67}
	assert.Equal(t, "3f9a5c8e-1234-5678-90ab-cdef01234567:10", formatGTID(sid, 10))
	// SID 长度不对 → 空串
	assert.Equal(t, "", formatGTID([]byte{1, 2, 3}, 1))
	assert.Equal(t, "", formatGTID(nil, 1))
}

func TestIsZeroSID(t *testing.T) {
	assert.True(t, isZeroSID(make([]byte, 16)))
	assert.True(t, isZeroSID(nil))
	assert.False(t, isZeroSID([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}))
}
