package binlog

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanner_EmptyFilterReturnsEOF(t *testing.T) {
	// 用 in-memory 模式：无文件，无 schema fetcher 也行
	s := NewScanner(StaticSchemaFetcher{})
	err := s.Scan(context.Background(), Filter{
		// 不指定 StartPos，扫描器应通过 EnumerateBinlogFiles 返回错误并经 Scan 返回
	})
	// 我们没给目录，Filter 里没有 BinlogDir 字段；这一节后续会扩展 Filter
	// 当前实现：Scan 应返回错误而不是 panic
	require.Error(t, err)

	// 但 Next 应在未启动时返回 io.EOF（避免死锁）
	tx, err := s.Next()
	assert.Nil(t, tx)
	assert.Error(t, err)
}

func TestScanner_NextWithoutScanReturnsError(t *testing.T) {
	s := NewScanner(StaticSchemaFetcher{})
	_, err := s.Next()
	require.Error(t, err)
	// 不是 io.EOF，而是 "scan not started"
	assert.NotEqual(t, io.EOF, err)
}

func TestScanner_CloseIdempotent(t *testing.T) {
	s := NewScanner(StaticSchemaFetcher{})
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

// TestScanner_ParsesKnownFixture 解析 fixture binlog，期望看到至少 3 个事务
// （INSERT、UPDATE、DELETE）。
// 使用本地 fixture 文件；如果不存在则 skip。
func TestScanner_ParsesKnownFixture(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	if _, err := os.Stat(fixture); err != nil {
		t.Skipf("fixture %s not present; run `make -C testdata all` to generate", fixture)
	}
	dir := t.TempDir()
	// 把 fixture 复制到 tempdir 命名为 mysql-bin.000002
	copyFile(t, fixture, filepath.Join(dir, "mysql-bin.000002"))

	sf := StaticSchemaFetcher{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []ColumnDef{
			{Name: "id", Type: "BIGINT", IsAutoInc: true},
			{Name: "user_id", Type: "BIGINT"},
			{Name: "amount", Type: "DECIMAL(10,2)"},
			{Name: "status", Type: "VARCHAR(32)"},
			{Name: "created_at", Type: "DATETIME"},
		}},
	}
	sc := NewScanner(sf)
	err := sc.Scan(context.Background(), Filter{
		BinlogDir: dir,
	})
	require.NoError(t, err)

	var txs []*Transaction
	for {
		tx, err := sc.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		txs = append(txs, tx)
	}

	// setup.sql 里有 3 个 DML 操作（INSERT、UPDATE、DELETE），都 autocommit
	// 所以期望 3 个 DML 事务（CREATE TABLE 是 DDL，不产生 RowChange）
	dml := filterDML(txs)
	require.Len(t, dml, 3, "want 3 DML transactions (insert/update/delete); got %+v", txs)

	// INSERT: user_id=1+2, status=new+paid
	require.Equal(t, ActionInsert, dml[0].Statements[0].Action)
	require.Equal(t, "shop.orders", dml[0].Statements[0].Schema+"."+dml[0].Statements[0].Table)

	// UPDATE: status → paid (id=1)
	require.Equal(t, ActionUpdate, dml[1].Statements[0].Action)

	// DELETE: id=2
	require.Equal(t, ActionDelete, dml[2].Statements[0].Action)
}

func filterDML(txs []*Transaction) []*Transaction {
	var out []*Transaction
	for _, tx := range txs {
		if len(tx.Statements) > 0 {
			out = append(out, tx)
		}
	}
	return out
}

func TestScanner_TruncatesLargeTransaction(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	if _, err := os.Stat(fixture); err != nil {
		t.Skipf("fixture missing; run `make -C testdata all`")
	}
	dir := t.TempDir()
	copyFile(t, fixture, filepath.Join(dir, "mysql-bin.000002"))

	sf := StaticSchemaFetcher{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []ColumnDef{
			{Name: "id", Type: "BIGINT"},
			{Name: "user_id", Type: "BIGINT"},
			{Name: "amount", Type: "DECIMAL(10,2)"},
			{Name: "status", Type: "VARCHAR(32)"},
			{Name: "created_at", Type: "DATETIME"},
		}},
	}
	// MaxRowsPerTx=1：任何超过 1 行的事务都截断
	sc := NewScanner(sf, WithMaxRowsPerTx(1))
	err := sc.Scan(context.Background(), Filter{BinlogDir: dir})
	require.NoError(t, err)

	var truncated int
	var total int
	for {
		tx, err := sc.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if len(tx.Statements) > 0 {
			total++
			if tx.Truncated {
				truncated++
			}
		}
	}
	// setup.sql 第一个 INSERT 插了 2 行 → 截断
	assert.Greater(t, truncated, 0, "expected at least one truncated transaction")
}

// TestScanner_ParseErrorSurfacesFromNext 回归测试：解析过程中出错时，Next()
// 必须把错误暴露给调用方，而不是伪装成 io.EOF（review round 1 finding）。
// 做法：把 fixture 拷到临时目录后翻转其中间一个字节，破坏该事件的 CRC32
// 校验（Scan 设置了 SetVerifyChecksum(true)），使 go-mysql 返回
// ErrChecksumMismatch。
func TestScanner_ParseErrorSurfacesFromNext(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	info, err := os.Stat(fixture)
	if err != nil {
		t.Skipf("fixture %s not present; run `make -C testdata all` to generate", fixture)
	}
	dir := t.TempDir()
	corrupt := filepath.Join(dir, "mysql-bin.000002")
	copyFile(t, fixture, corrupt)
	// 中点字节必然落在某个普通事件（非 FormatDescriptionEvent）的
	// header/body/校验和区域内；翻转后该校验和必然不匹配 → 解析报错。
	flipByte(t, corrupt, int(info.Size()/2))

	sc := NewScanner(StaticSchemaFetcher{})
	err = sc.Scan(context.Background(), Filter{BinlogDir: dir})
	require.NoError(t, err)

	var got error
	for {
		if _, err := sc.Next(); err != nil {
			got = err
			break
		}
	}
	require.Error(t, got, "corrupted binlog must surface a parse error")
	assert.NotEqual(t, io.EOF, got, "parse error must not be masked as io.EOF")
	// 错误来自 parseFile 的解析管线包装（而非其他来源）
	assert.Contains(t, got.Error(), "binlog: parse events in", "unexpected error: %v", got)

	// 错误被消费后 channel 已排空并关闭；后续 Next() 返回 io.EOF（而不是重复报错）
	tx, err := sc.Next()
	assert.Nil(t, tx)
	assert.Equal(t, io.EOF, err)
}

// TestScanner_CleanEndReturnsEOF 校验 happy path：完整 fixture 扫完后
// Next() 返回 io.EOF，且重复调用仍返回 io.EOF。
func TestScanner_CleanEndReturnsEOF(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	if _, err := os.Stat(fixture); err != nil {
		t.Skipf("fixture %s not present; run `make -C testdata all` to generate", fixture)
	}
	dir := t.TempDir()
	copyFile(t, fixture, filepath.Join(dir, "mysql-bin.000002"))

	sc := NewScanner(StaticSchemaFetcher{})
	err := sc.Scan(context.Background(), Filter{BinlogDir: dir})
	require.NoError(t, err)

	var txs int
	for {
		_, err := sc.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		txs++
	}
	assert.Greater(t, txs, 0, "expected transactions before EOF")

	tx, err := sc.Next()
	assert.Nil(t, tx)
	assert.Equal(t, io.EOF, err, "Next() after clean end must keep returning io.EOF")
}

// flipByte 把 path 文件中 offset 处的字节按位翻转。
func flipByte(t *testing.T, path string, offset int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	b := make([]byte, 1)
	_, err = f.ReadAt(b, int64(offset))
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b, int64(offset))
	require.NoError(t, err)
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	require.NoError(t, err)
	defer in.Close()
	out, err := os.Create(dst)
	require.NoError(t, err)
	defer out.Close()
	_, err = io.Copy(out, in)
	require.NoError(t, err)
}

// ---------- Task 3：SelectedTxIDs / EndPos.Pos / Close 中断 ----------

// scanFilter 用给定 Filter 完整扫描一次并返回所有事务。
// 注：coverage_test.go 已有 scanAll(t, sc Scanner)；这里 Filter 版本另命名避免重定义。
func scanFilter(t *testing.T, f Filter) []*Transaction {
	t.Helper()
	s := NewScanner(nil, WithMaxRowsPerTx(0))
	require.NoError(t, s.Scan(context.Background(), f))
	return scanAll(t, s)
}

// TestScanner_FilterSelectedTxIDs 校验 Filter.SelectedTxIDs：只保留 TxID
// 命中的事务（SELECTED_SQL 定向二次扫描的匹配基础）。
// 注：brief 原稿直接用 BinlogDir:"testdata"，但 testdata 下唯一的
// mysql-8.0-row-full.bin 不满足 isBinlogFile 命名（后缀 .bin 非全数字），
// EnumerateBinlogFiles 会报 "no binlog files"；故拷贝到临时目录并命名为
// mysql-bin.000001。
func TestScanner_FilterSelectedTxIDs(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	if _, err := os.Stat(fixture); err != nil {
		t.Skipf("fixture %s not present; run `make -C testdata all` to generate", fixture)
	}
	dir := t.TempDir()
	copyFile(t, fixture, filepath.Join(dir, "mysql-bin.000001"))

	all := scanFilter(t, Filter{BinlogDir: dir})
	require.NotEmpty(t, all)

	want := all[0].TxID
	got := scanFilter(t, Filter{BinlogDir: dir, SelectedTxIDs: []string{want}})
	require.Len(t, got, 1)
	require.Equal(t, want, got[0].TxID)
}

// firstXIDLogPos 返回文件中第一个 XID 事件的 LogPos（即第一个事务的结束位置）。
func firstXIDLogPos(t *testing.T, path string) uint32 {
	t.Helper()
	src, err := OpenFileSource(context.Background(), path, 0, replication.NewBinlogParser())
	require.NoError(t, err)
	defer src.Close()
	for {
		ev, err := src.Next(context.Background())
		if err == io.EOF {
			t.Fatalf("no XID event found in %s", path)
		}
		require.NoError(t, err)
		if ev.Header.EventType == replication.XID_EVENT {
			return ev.Header.LogPos
		}
	}
}

// TestScanner_EndPosStopsMidFile 校验 EndPos.Pos 生效：仅当 EndPos.Name 非空且
// 等于当前文件名时，解析到事件 LogPos > EndPos.Pos 即停止，不再继续。
// 以第一个 XID 事件的 LogPos 为界：该事件（LogPos == Pos，含）产出第 1 个事务，
// 其后的事件 LogPos > Pos 触发停止 → 返回的事务数比不设 EndPos 时少。
func TestScanner_EndPosStopsMidFile(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	if _, err := os.Stat(fixture); err != nil {
		t.Skipf("fixture %s not present; run `make -C testdata all` to generate", fixture)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	copyFile(t, fixture, path)

	all := scanFilter(t, Filter{BinlogDir: dir})
	require.Len(t, all, 3, "fixture 语义：3 个 DML 事务（见 TestScanner_ParsesKnownFixture）")

	pos := firstXIDLogPos(t, path)
	require.Greater(t, pos, uint32(0))

	got := scanFilter(t, Filter{BinlogDir: dir, EndPos: mysql.Position{Name: "mysql-bin.000001", Pos: pos}})
	require.Less(t, len(got), len(all), "EndPos.Pos 必须截断后续事务")
	require.Len(t, got, 1, "第一个 XID 的 LogPos 应恰好覆盖第 1 个事务")
	require.Equal(t, all[0].TxID, got[0].TxID)
}

// TestScanner_StartPosStartsMidFile 校验 Filter.StartPos.Pos 生效：仅对与
// StartPos.Name 匹配的文件生效，从该偏移开始解析。安全取值 = 第一个事务
// 的 XID 事件 LogPos（即第一个事务的结束位置、下一个事件的起点）——从那里
// 开始扫描会跳过第 1 个事务：事务数少于全扫、第一个事务不同。
//
// 此测试同时回归 OpenFileSource 的中间偏移解析：不重读 FDE 时 go-mysql 的
// p.format 为 nil，遇到 TableMap/Rows 会 nil 解引用 panic（StartPos.Pos 此前
// 无引擎级测试即因该缺陷）。
func TestScanner_StartPosStartsMidFile(t *testing.T) {
	fixture := filepath.Join("testdata", "mysql-8.0-row-full.bin")
	if _, err := os.Stat(fixture); err != nil {
		t.Skipf("fixture %s not present; run `make -C testdata all` to generate", fixture)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "mysql-bin.000001")
	copyFile(t, fixture, path)

	all := scanFilter(t, Filter{BinlogDir: dir})
	require.Len(t, all, 3, "fixture 语义：3 个 DML 事务（见 TestScanner_ParsesKnownFixture）")

	pos := firstXIDLogPos(t, path)
	require.Greater(t, pos, uint32(0))

	got := scanFilter(t, Filter{BinlogDir: dir, StartPos: mysql.Position{Name: "mysql-bin.000001", Pos: pos}})
	require.NotEmpty(t, got, "StartPos.Pos 定位必须仍能产出事务")
	require.Less(t, len(got), len(all), "StartPos.Pos 必须跳过开头的事务")
	require.NotEqual(t, all[0].TxID, got[0].TxID, "第一个事务应从第 2 个事务开始")
}

// TestScanner_CloseInterruptsMidScan 回归评审发现：Close() 从未关闭 s.done，
// 消费者中途放弃时解析协程会永久阻塞在 emit 的 s.txs <-（缓冲满）并泄漏。
// 64 个事务远超 txs 缓冲（16）：不消费时协程必然卡在 emit；Close 必须通过
// 关闭 s.done 中断它，使其退出并关闭 channel。断言：不消费 + Close 后解析
// 协程退出（goroutine 数回落），且 Next() 快速返回非 nil 错误（io.EOF 亦可），
// 而非死锁。
func TestScanner_CloseInterruptsMidScan(t *testing.T) {
	dir := writeBinlog(t, craftMultiTxBinlog(64))
	sc := NewScanner(StaticSchemaFetcher{})
	before := runtime.NumGoroutine()
	require.NoError(t, sc.Scan(context.Background(), Filter{BinlogDir: dir}))
	require.NoError(t, sc.Close())

	// 观察 1：Close 后不消费，runParseLoop 与 FileSource.run 两个协程必须退出。
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatal("parse goroutines leaked after Close: s.done 未关闭，emit 阻塞未中断")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 观察 2：Close 后 Next 必须快速返回非 nil 错误（缓冲中的事务可先被取走），
	// 而非死锁。
	doneCh := make(chan error, 1)
	go func() {
		var last error
		for {
			if _, err := sc.Next(); err != nil {
				last = err
				break
			}
		}
		doneCh <- last
	}()
	select {
	case err := <-doneCh:
		require.Error(t, err, "Close 后流必须以非 nil 错误（io.EOF 亦可）结束")
	case <-time.After(5 * time.Second):
		t.Fatal("Next() hung after Close")
	}
}
