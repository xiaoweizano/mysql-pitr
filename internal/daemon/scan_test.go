package daemon_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/binlogtest"
	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/daemon"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// fakeSink 记录所有事件（并发安全），测试用。
type fakeSink struct {
	mu     sync.Mutex
	events []ws.StreamEvent
}

func (f *fakeSink) Send(ev ws.StreamEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeSink) eventsCopy() []ws.StreamEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ws.StreamEvent, len(f.events))
	copy(out, f.events)
	return out
}

func (f *fakeSink) count() int { return len(f.eventsCopy()) }

func (f *fakeSink) hasKind(k string) bool {
	for _, ev := range f.eventsCopy() {
		if ev.Kind == k {
			return true
		}
	}
	return false
}

var fixtureSchema = binlog.StaticSchemaFetcher{
	"shop.orders": {
		Schema: "shop", Table: "orders",
		Columns: []binlog.ColumnDef{
			{Name: "id", IsAutoInc: true},
			{Name: "user_id"},
			{Name: "amount"},
			{Name: "status"},
			{Name: "created_at"},
		},
		PrimaryKey: []string{"id"},
	},
}

// fixtureDir 把 testdata 的 fixture 拷贝到临时目录并重命名为 mysql-bin.000001。
// 必须重命名：EnumerateBinlogFiles 只认数字后缀（isBinlogFile 规则）。
func fixtureDir(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("..", "binlog", "testdata", "mysql-8.0-row-full.bin"))
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), src, 0o644))
	return dir
}

// craftBigMultiTxBinlog 构造 n 个匿名事务（TableMap + WRITE + XID）的 binlog，
// 事件数 ≈ 2n+2。用于构造「cancel 时归档远未扫完」的大文件。
func craftBigMultiTxBinlog(n int) []byte {
	evs := make([]binlogtest.Event, 0, 2*n+2)
	evs = append(evs, binlogtest.MustCraft(binlogtest.CraftFDE()))
	evs = append(evs, binlogtest.MustCraft(binlogtest.CraftTableMap("shop", "orders", 1)))
	for i := 0; i < n; i++ {
		evs = append(evs,
			binlogtest.MustCraft(binlogtest.CraftWriteRowsValues(1, int64(i))),
			binlogtest.MustCraft(binlogtest.CraftXID(uint64(i+1))),
		)
	}
	return binlogtest.CraftFile(evs)
}

// txMetaWire 是 tx_meta 事件数据的线格式（需与 scan.TxMeta 的 json tag 对齐）。
type txMetaWire struct {
	TxID       string    `json:"txId"`
	GTID       string    `json:"gtid,omitempty"`
	XID        uint64    `json:"xid,omitempty"`
	CommitTime time.Time `json:"commitTime"`
	Schema     string    `json:"schema,omitempty"`
	Tables     []struct {
		Schema string `json:"schema"`
		Table  string `json:"table"`
	} `json:"tables,omitempty"`
	RowCount  int  `json:"rowCount"`
	Truncated bool `json:"truncated,omitempty"`
}

// TestScan_StreamsTxMetaAndScanDone 验证一次 meta 扫描：tx_meta 事件数 == fixture
// 事务数；事件 ID 均为命令 ID；最后一个事件是 scan_done。
func TestScan_StreamsTxMetaAndScanDone(t *testing.T) {
	ctx := context.Background()
	sink := &fakeSink{}
	d := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    fixtureDir(t),
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink)

	err := d.Scan(ctx, "scan-1", ws.ScanRequest{
		Filter: ws.ScanFilter{}, Mode: "meta", MaxPreview: 0,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.hasKind(ws.EvScanDone) },
		5*time.Second, 10*time.Millisecond, "scan_done should arrive")

	evs := sink.eventsCopy()
	var metas []txMetaWire
	for _, ev := range evs {
		require.Equal(t, "scan-1", ev.ID, "所有事件共享命令 ID")
		if ev.Kind == ws.EvTxMeta {
			var m txMetaWire
			require.NoError(t, json.Unmarshal(ev.Data, &m))
			metas = append(metas, m)
		}
	}
	require.Len(t, metas, 3, "fixture 含 3 个事务（INSERT 2 行 / UPDATE / DELETE 各一事务）")
	for _, m := range metas {
		require.NotEmpty(t, m.TxID)
		require.Greater(t, m.RowCount, 0)
	}
	require.Equal(t, ws.EvScanDone, evs[len(evs)-1].Kind, "scan_done 是最后一个事件")
	require.False(t, sink.hasKind(ws.EvSQL), "meta 模式不产出 SQL 事件")
	require.False(t, sink.hasKind(ws.EvOpError), "正常完成不产出 op_error")
}

// TestScan_CancelStopsStreaming 验证 CancelScan：流在归档扫完前停止（无 scan_done、
// 事件数 << 归档事务总数、终止后有 op_error 且不再有新事件）。
func TestScan_CancelStopsStreaming(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "mysql-bin.000001"),
		craftBigMultiTxBinlog(100_000), 0o644))

	ctx := context.Background()
	sink := &fakeSink{}
	d := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    dir,
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink)

	require.NoError(t, d.Scan(ctx, "scan-cancel", ws.ScanRequest{
		Filter: ws.ScanFilter{}, Mode: "meta", MaxPreview: 1_000_000,
	}))
	// 等扫描真正开始产出再 cancel，确保 cancel 落在扫描中途
	require.Eventually(t, func() bool { return sink.count() >= 1 },
		5*time.Second, 10*time.Millisecond, "scan should start streaming")
	require.NoError(t, d.CancelScan("scan-cancel"))

	require.Eventually(t, func() bool { return sink.hasKind(ws.EvOpError) },
		5*time.Second, 10*time.Millisecond, "cancelled scan reports op_error")
	require.False(t, sink.hasKind(ws.EvScanDone), "cancelled scan 不报 scan_done")

	n := sink.count()
	require.Less(t, n, 100_000, "扫描必须在大归档完成前停止")
	// 终止后不再有新事件
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, n, sink.count(), "cancel 后不再推送事件")
}

// TestScan_SelectedSQLModeGeneratesStatements 验证两阶段定向扫描：先 meta 扫拿
// TxID，再 selected 模式 + SelectedTxIDs 只产出命中事务的 SQL 事件。
func TestScan_SelectedSQLModeGeneratesStatements(t *testing.T) {
	ctx := context.Background()
	dir := fixtureDir(t)

	// 第一遍：meta 扫拿 TxID
	sink1 := &fakeSink{}
	d1 := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    dir,
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink1)
	require.NoError(t, d1.Scan(ctx, "scan-meta", ws.ScanRequest{
		Filter: ws.ScanFilter{}, Mode: "meta",
	}))
	require.Eventually(t, func() bool { return sink1.hasKind(ws.EvScanDone) },
		5*time.Second, 10*time.Millisecond)

	var selTxID string
	for _, ev := range sink1.eventsCopy() {
		if ev.Kind == ws.EvTxMeta {
			var m txMetaWire
			require.NoError(t, json.Unmarshal(ev.Data, &m))
			selTxID = m.TxID
			break
		}
	}
	require.NotEmpty(t, selTxID)

	// 第二遍：selected 定向二次扫描
	sink2 := &fakeSink{}
	d2 := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    dir,
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink2)
	require.NoError(t, d2.Scan(ctx, "scan-sel", ws.ScanRequest{
		Filter: ws.ScanFilter{SelectedTxIDs: []string{selTxID}},
		Mode:   "selected",
	}))
	require.Eventually(t, func() bool { return sink2.hasKind(ws.EvScanDone) },
		5*time.Second, 10*time.Millisecond)

	var sqlEvs []ws.StreamEvent
	for _, ev := range sink2.eventsCopy() {
		require.Equal(t, "scan-sel", ev.ID, "所有事件共享命令 ID")
		if ev.Kind == ws.EvSQL {
			sqlEvs = append(sqlEvs, ev)
		}
	}
	require.NotEmpty(t, sqlEvs, "selected 扫描必须产出 SQL 事件")
	for _, ev := range sqlEvs {
		require.NotEmpty(t, string(ev.Data), "SQL 事件 data 非空")
	}
	// 定向命中唯一：只产出 1 个 tx_meta（被选中的事务）
	txMetaN := 0
	for _, ev := range sink2.eventsCopy() {
		if ev.Kind == ws.EvTxMeta {
			txMetaN++
		}
	}
	require.Equal(t, 1, txMetaN, "selected 模式只命中一个事务")
}

// TestScan_SelectedSQLModeEmitsStatementWire 验证 selected 模式的 EvSQL 事件 Data
// 是归一化的 []ws.StatementWire：字段为小写驼峰键（sql/txId/txOrder/warnings）、
// 无 SourceRow、无 reverse.Statement 大写键泄漏。
func TestScan_SelectedSQLModeEmitsStatementWire(t *testing.T) {
	ctx := context.Background()
	dir := fixtureDir(t)

	// 第一遍：meta 扫拿 TxID
	sink1 := &fakeSink{}
	d1 := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    dir,
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink1)
	require.NoError(t, d1.Scan(ctx, "scan-wire-meta", ws.ScanRequest{
		Filter: ws.ScanFilter{}, Mode: "meta",
	}))
	require.Eventually(t, func() bool { return sink1.hasKind(ws.EvScanDone) },
		5*time.Second, 10*time.Millisecond)

	var selTxID string
	for _, ev := range sink1.eventsCopy() {
		if ev.Kind == ws.EvTxMeta {
			var m txMetaWire
			require.NoError(t, json.Unmarshal(ev.Data, &m))
			selTxID = m.TxID
			break
		}
	}
	require.NotEmpty(t, selTxID)

	// 第二遍：selected 定向二次扫描
	sink2 := &fakeSink{}
	d2 := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    dir,
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink2)
	require.NoError(t, d2.Scan(ctx, "scan-wire-sel", ws.ScanRequest{
		Filter: ws.ScanFilter{SelectedTxIDs: []string{selTxID}},
		Mode:   "selected",
	}))
	require.Eventually(t, func() bool { return sink2.hasKind(ws.EvScanDone) },
		5*time.Second, 10*time.Millisecond)

	var sqlEvs []ws.StreamEvent
	for _, ev := range sink2.eventsCopy() {
		if ev.Kind == ws.EvSQL {
			sqlEvs = append(sqlEvs, ev)
		}
	}
	require.NotEmpty(t, sqlEvs, "selected 扫描必须产出 SQL 事件")

	for _, ev := range sqlEvs {
		// 归一化反序列化：Data 必须能解析为 []ws.StatementWire
		var wires []ws.StatementWire
		require.NoError(t, json.Unmarshal(ev.Data, &wires))
		require.NotEmpty(t, wires, "SQL 事件 data 是 statement 数组")
		for _, w := range wires {
			require.NotEmpty(t, w.SQL, "每条 wire statement 都有 sql")
			require.Equal(t, selTxID, w.TxID, "wire statement 的 txId 命中被选事务")
			require.GreaterOrEqual(t, w.TxOrder, 0)
		}

		// 原始键检查：小写驼峰白名单，杜绝 SourceRow / reverse.Statement
		// 大写键（SQL/TxID/TxOrder/Warnings）泄漏上 wire
		var raw []map[string]interface{}
		require.NoError(t, json.Unmarshal(ev.Data, &raw))
		require.NotEmpty(t, raw)
		for _, stmt := range raw {
			for k := range stmt {
				switch k {
				case "sql", "txId", "txOrder", "warnings":
				default:
					t.Fatalf("EvSQL wire 泄漏非归一化键 %q（SourceRow/大写键不应上 wire）", k)
				}
			}
			require.Contains(t, stmt, "sql", "wire 键为小写驼峰 sql")
			require.Contains(t, stmt, "txId", "wire 键为小写驼峰 txId")
			require.Contains(t, stmt, "txOrder", "wire 键为小写驼峰 txOrder")
		}
	}
}

// runMetaScan 对 fixture 目录跑一次 meta 扫描，等 scan_done 后返回全部事件。
func runMetaScan(t *testing.T, id string, filter ws.ScanFilter) []ws.StreamEvent {
	t.Helper()
	ctx := context.Background()
	sink := &fakeSink{}
	d := daemon.NewDaemon(daemon.ScanDeps{
		ArchiveDir:    fixtureDir(t),
		SchemaFetcher: fixtureSchema,
	}, nil, nil, sink)

	require.NoError(t, d.Scan(ctx, id, ws.ScanRequest{Filter: filter, Mode: "meta"}))
	require.Eventually(t, func() bool { return sink.hasKind(ws.EvScanDone) },
		5*time.Second, 10*time.Millisecond, "scan_done should arrive")
	return sink.eventsCopy()
}

// txMetaCount 统计事件列表里的 tx_meta 数。
func txMetaCount(evs []ws.StreamEvent) int {
	n := 0
	for _, ev := range evs {
		if ev.Kind == ws.EvTxMeta {
			n++
		}
	}
	return n
}

// hasKind 检查事件列表是否含指定 kind。
func hasKind(evs []ws.StreamEvent, k string) bool {
	for _, ev := range evs {
		if ev.Kind == k {
			return true
		}
	}
	return false
}

// TestScan_TimeStartOnlyMatchesAll 回归：只设 TimeStart（2020，远早于 fixture
// 提交时间 2025-06-15）时必须返回全部事务。修复前缺 End 留 zero，引擎
// After(zero) 恒 true → 静默零结果（无错误）。
func TestScan_TimeStartOnlyMatchesAll(t *testing.T) {
	evs := runMetaScan(t, "scan-ts-only", ws.ScanFilter{TimeStart: "2020-01-01T00:00:00Z"})

	require.Equal(t, 3, txMetaCount(evs), "TimeStart-only 必须返回 fixture 全部 3 个事务")
	require.False(t, hasKind(evs, ws.EvOpError), "正常完成不产出 op_error")
	require.Equal(t, ws.EvScanDone, evs[len(evs)-1].Kind, "scan_done 收尾")
}

// TestScan_TimeEndOnlyMatchesAll 回归：只设 TimeEnd（2030，远晚于 fixture
// 提交时间）时必须返回全部事务（该场景修复前恰好能工作，补测防止回归）。
func TestScan_TimeEndOnlyMatchesAll(t *testing.T) {
	evs := runMetaScan(t, "scan-te-only", ws.ScanFilter{TimeEnd: "2030-01-01T00:00:00Z"})

	require.Equal(t, 3, txMetaCount(evs), "TimeEnd-only 必须返回 fixture 全部 3 个事务")
	require.False(t, hasKind(evs, ws.EvOpError), "正常完成不产出 op_error")
	require.Equal(t, ws.EvScanDone, evs[len(evs)-1].Kind, "scan_done 收尾")
}

// TestScan_FarFutureTimeStartEmptyNoError 验证 TimeStart 远晚于全部事务提交
// 时间（2100）时结果为空但不报错：scan_done 收尾、零 tx_meta、无 op_error。
func TestScan_FarFutureTimeStartEmptyNoError(t *testing.T) {
	evs := runMetaScan(t, "scan-ts-far", ws.ScanFilter{TimeStart: "2100-01-01T00:00:00Z"})

	require.Equal(t, 0, txMetaCount(evs), "提交时间均早于 2100，应零命中")
	require.False(t, hasKind(evs, ws.EvOpError), "零命中不是错误")
	require.Equal(t, ws.EvScanDone, evs[len(evs)-1].Kind, "scan_done 收尾")
}

// TestArchiveStatus 验证 ArchiveStatus 返回注入的 stateFn 结果。
func TestArchiveStatus(t *testing.T) {
	st := collector.State{LastFile: "mysql-bin.000003", LastPos: 42, LastGTID: "x-1:1"}
	d := daemon.NewDaemon(daemon.ScanDeps{}, nil, func() collector.State { return st }, nil)
	require.Equal(t, st, d.ArchiveStatus())
}
