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

// TestArchiveStatus 验证 ArchiveStatus 返回注入的 stateFn 结果。
func TestArchiveStatus(t *testing.T) {
	st := collector.State{LastFile: "mysql-bin.000003", LastPos: 42, LastGTID: "x-1:1"}
	d := daemon.NewDaemon(daemon.ScanDeps{}, nil, func() collector.State { return st }, nil)
	require.Equal(t, st, d.ArchiveStatus())
}
