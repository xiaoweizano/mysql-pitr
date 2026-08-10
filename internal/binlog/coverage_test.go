package binlog

// 本文件补齐 Task 7 的覆盖率缺口。核心手段是手工构造 binlog 字节流
// （craft* 系列 helper），让 Scanner 走完真实解析管线，从而覆盖单靠
// testdata fixture 无法触达的分支：BEGIN/COMMIT/DDL 事件、XID 空 pending、
// Anonymous GTID（全 0 SID）、MariaDB GTID、PARTIAL_UPDATE_ROWS_EVENT
// 跳过路径、Filter 拒绝路径等。

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- 手工构造 binlog 的 helpers ----------

// craftEvent 构造一个完整 binlog 事件：19 字节 header + body + CRC32。
// 校验和按 MySQL 规范覆盖 header+body（与 parser.SetVerifyChecksum(true) 一致）。
func craftEvent(ts uint32, etype replication.EventType, serverID uint32, body []byte) []byte {
	ev := make([]byte, replication.EventHeaderSize,
		replication.EventHeaderSize+len(body)+replication.BinlogChecksumLength)
	binary.LittleEndian.PutUint32(ev[0:], ts)
	ev[4] = byte(etype)
	binary.LittleEndian.PutUint32(ev[5:], serverID)
	binary.LittleEndian.PutUint32(ev[9:], uint32(replication.EventHeaderSize+len(body)+replication.BinlogChecksumLength))
	// LogPos 不参与 go-mysql 的校验，保持 0
	binary.LittleEndian.PutUint16(ev[17:], 0)
	ev = append(ev, body...)
	crc := crc32.ChecksumIEEE(ev)
	var cb [replication.BinlogChecksumLength]byte
	binary.LittleEndian.PutUint32(cb[:], crc)
	return append(ev, cb[:]...)
}

// craftFDE 构造 FormatDescriptionEvent 的 body（不含 CRC；由 craftEvent 追加）。
// 事件类型 header 长度数组全部填 8 → go-mysql 用 6 字节 table id。
func craftFDE(serverVersion string) []byte {
	body := make([]byte, 0, 2+50+4+1+42+1)
	body = binary.LittleEndian.AppendUint16(body, 4) // binlog version
	sv := make([]byte, 50)
	copy(sv, serverVersion)
	body = append(body, sv...)
	body = binary.LittleEndian.AppendUint32(body, 0) // create timestamp
	body = append(body, replication.EventHeaderSize) // header length = 19
	body = append(body, bytes.Repeat([]byte{8}, 42)...)
	body = append(body, byte(replication.BINLOG_CHECKSUM_ALG_CRC32))
	return body
}

func craftQuery(schema, query string) []byte {
	body := make([]byte, 0, 13+len(schema)+1+len(query))
	body = binary.LittleEndian.AppendUint32(body, 1) // thread id
	body = binary.LittleEndian.AppendUint32(body, 0) // exec time
	body = append(body, byte(len(schema)))
	body = binary.LittleEndian.AppendUint16(body, 0) // error code
	body = binary.LittleEndian.AppendUint16(body, 0) // status vars length
	body = append(body, schema...)
	body = append(body, 0) // 结尾 NUL
	body = append(body, query...)
	return body
}

func craftXID(xid uint64) []byte {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, xid)
	return body
}

func craftGTID(sid []byte, gno int64) []byte {
	body := make([]byte, 0, 1+16+8)
	body = append(body, 1) // commit flag
	body = append(body, sid...)
	body = binary.LittleEndian.AppendUint64(body, uint64(gno))
	return body
}

func craftMariaDBGTID(seq uint64, domain uint32, flags byte) []byte {
	body := make([]byte, 13)
	binary.LittleEndian.PutUint64(body[0:], seq)
	binary.LittleEndian.PutUint32(body[8:], domain)
	body[12] = flags
	return body
}

// craftTableMap 构造 1 列（LONGLONG）的表映射事件 body。
func craftTableMap(tableID uint64, schema, table string) []byte {
	body := make([]byte, 0, 32)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = append(body, byte(len(schema)))
	body = append(body, schema...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, table...)
	body = append(body, 0)
	body = append(body, 1) // column count (lenenc)
	body = append(body, mysql.MYSQL_TYPE_LONGLONG)
	body = append(body, 1, 0) // meta: lenenc 长度 1，值 0
	body = append(body, 0)    // null bitmap
	return body
}

// craftWriteRows 构造 WRITE/DELETE ROWS_EVENTv2 的 body（单列 LONGLONG 行）。
func craftWriteRows(tableID uint64, values []int64) []byte {
	body := make([]byte, 0, 6+2+2+2+len(values)*9)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = binary.LittleEndian.AppendUint16(body, 2) // extra data length（无）
	body = append(body, 1)                           // column count
	body = append(body, 0x01)                        // bitmap1：第 0 列存在
	for _, v := range values {
		body = append(body, 0x00) // null bitmap：第 0 列非 NULL
		body = binary.LittleEndian.AppendUint64(body, uint64(v))
	}
	return body
}

// craftPartialUpdateRows 构造 PARTIAL_UPDATE_ROWS_EVENT 的 body。
// before/after image 各 1 行；after image 前缀是 binlog_row_value_options（0 = 非 partial JSON）。
func craftPartialUpdateRows(tableID uint64, before, after int64) []byte {
	body := make([]byte, 0, 6+2+2+2+19)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = binary.LittleEndian.AppendUint16(body, 2) // extra data length（无）
	body = append(body, 1)                           // column count
	body = append(body, 0x01)                        // bitmap1
	body = append(body, 0x01)                        // bitmap2
	body = append(body, 0x00)                        // before: null bitmap
	body = binary.LittleEndian.AppendUint64(body, uint64(before))
	body = append(body, 0x00) // after: binlog_row_value_options = 0
	body = append(body, 0x00) // after: null bitmap
	body = binary.LittleEndian.AppendUint64(body, uint64(after))
	return body
}

// craftBinlog 把 FDE + 事件序列组装成完整 binlog 文件字节。
func craftBinlog(ts uint32, events ...[]byte) []byte {
	out := make([]byte, 0)
	out = append(out, replication.BinLogFileHeader...)
	out = append(out, craftEvent(ts, replication.FORMAT_DESCRIPTION_EVENT, 1, craftFDE("8.0.36"))...)
	for _, e := range events {
		out = append(out, e...)
	}
	return out
}

// craftTestBinlog 生成覆盖核心解析分支的 binlog：
//
//	GTID(uuid:10) BEGIN TableMap WRITE[1,2] XID(100)        → tx1（2 行）
//	TableMap WRITE[3] XID(101)                              → tx2（1 行，无 GTID）
//	GTID(全0) GTID(uuid:11) BEGIN TableMap WRITE[4] DDL     → tx3（DDL flush，commitTs 回退 now）
//	COMMIT(无 pending) MariaDBGTID BEGIN COMMIT XID(200)
func craftTestBinlog() []byte {
	const ts = uint32(1750000000)
	sid := []byte{0x3f, 0x9a, 0x5c, 0x8e, 0x12, 0x34, 0x56, 0x78,
		0x90, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67}
	return craftBinlog(ts,
		craftEvent(ts, replication.GTID_EVENT, 1, craftGTID(sid, 10)),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("shop", "BEGIN")),
		craftEvent(ts, replication.TABLE_MAP_EVENT, 1, craftTableMap(1, "shop", "orders")),
		craftEvent(ts, replication.WRITE_ROWS_EVENTv2, 1, craftWriteRows(1, []int64{1, 2})),
		craftEvent(ts, replication.XID_EVENT, 1, craftXID(100)),
		craftEvent(ts, replication.TABLE_MAP_EVENT, 1, craftTableMap(1, "shop", "orders")),
		craftEvent(ts, replication.WRITE_ROWS_EVENTv2, 1, craftWriteRows(1, []int64{3})),
		craftEvent(ts, replication.XID_EVENT, 1, craftXID(101)),
		craftEvent(ts, replication.GTID_EVENT, 1, craftGTID(make([]byte, 16), 0)),
		craftEvent(ts, replication.GTID_EVENT, 1, craftGTID(sid, 11)),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("shop", "BEGIN")),
		craftEvent(ts, replication.TABLE_MAP_EVENT, 1, craftTableMap(1, "shop", "orders")),
		craftEvent(ts, replication.WRITE_ROWS_EVENTv2, 1, craftWriteRows(1, []int64{4})),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("shop", "ALTER TABLE orders ADD COLUMN note VARCHAR(16)")),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("", "COMMIT")),
		craftEvent(ts, replication.MARIADB_GTID_EVENT, 1, craftMariaDBGTID(1, 0, 0)),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("shop", "BEGIN")),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("", "COMMIT")),
		craftEvent(ts, replication.XID_EVENT, 1, craftXID(200)),
	)
}

// craftMultiTxBinlog 生成 n 个独立事务（GTID + BEGIN + TableMap + WRITE + XID），
// 用于填满 txs 缓冲、制造 emit 阻塞的 Close 中断/泄漏回归场景。
func craftMultiTxBinlog(n int) []byte {
	const ts = uint32(1750000000)
	sid := []byte{0x3f, 0x9a, 0x5c, 0x8e, 0x12, 0x34, 0x56, 0x78,
		0x90, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67}
	evs := make([][]byte, 0, n*5)
	for i := 0; i < n; i++ {
		evs = append(evs,
			craftEvent(ts, replication.GTID_EVENT, 1, craftGTID(sid, int64(1000+i))),
			craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("shop", "BEGIN")),
			craftEvent(ts, replication.TABLE_MAP_EVENT, 1, craftTableMap(1, "shop", "orders")),
			craftEvent(ts, replication.WRITE_ROWS_EVENTv2, 1, craftWriteRows(1, []int64{int64(i)})),
			craftEvent(ts, replication.XID_EVENT, 1, craftXID(uint64(1000+i))),
		)
	}
	return craftBinlog(ts, evs...)
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
	const ts = uint32(1750000000)
	data := craftBinlog(ts,
		craftEvent(ts, replication.TABLE_MAP_EVENT, 1, craftTableMap(1, "shop", "orders")),
		craftEvent(ts, replication.PARTIAL_UPDATE_ROWS_EVENT, 1, craftPartialUpdateRows(1, 1, 2)),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("shop", "BEGIN")),
		craftEvent(ts, replication.QUERY_EVENT, 1, craftQuery("", "COMMIT")),
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
