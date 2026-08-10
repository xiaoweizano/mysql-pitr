package scan_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/binlogtest"
	"github.com/a-shan/mysql-pitr/internal/scan"
)

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
// 必须重命名：EnumerateBinlogFiles 只认数字后缀（isBinlogFile 规则），
// 而提交的 fixture 文件名为 mysql-8.0-row-full.bin。
func fixtureDir(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("..", "binlog", "testdata", "mysql-8.0-row-full.bin"))
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), src, 0o644))
	return dir
}

func collect(t *testing.T, cfg scan.Config) ([]scan.Result, error) {
	t.Helper()
	ctx := context.Background()
	ch, errCh := scan.Stream(ctx, cfg)
	var out []scan.Result
	for r := range ch {
		out = append(out, r)
	}
	return out, <-errCh
}

func TestStream_ModeMetaOnly(t *testing.T) {
	out, err := collect(t, scan.Config{
		ArchiveDir:    fixtureDir(t),
		Filter:        binlog.Filter{},
		Mode:          scan.ModeMetaOnly,
		SchemaFetcher: fixtureSchema,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out)
	for _, r := range out {
		require.Empty(t, r.SQL, "META_ONLY 不回传 SQL")
		require.NotEmpty(t, r.Meta.TxID)
		require.Greater(t, r.Meta.RowCount, 0)
	}
}

func TestStream_ModeWithSQL_ProducesReverseStatements(t *testing.T) {
	out, err := collect(t, scan.Config{
		ArchiveDir:    fixtureDir(t),
		Filter:        binlog.Filter{},
		Mode:          scan.ModeWithSQL,
		SchemaFetcher: fixtureSchema,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out)
	// fixture 的 setup.sql：INSERT 2 行、UPDATE 1 行、DELETE 1 行（分属若干事务）
	total := 0
	for _, r := range out {
		for _, s := range r.SQL {
			if s.SQL != "" {
				total++
			}
		}
	}
	require.GreaterOrEqual(t, total, 4)
}

func TestStream_ModeSelectedSQL_OnlySelected(t *testing.T) {
	all, err := collect(t, scan.Config{
		ArchiveDir:    fixtureDir(t),
		Filter:        binlog.Filter{},
		Mode:          scan.ModeMetaOnly,
		SchemaFetcher: fixtureSchema,
	})
	require.NoError(t, err)
	require.NotEmpty(t, all)

	sel := all[0].Meta.TxID
	out, err := collect(t, scan.Config{
		ArchiveDir:    fixtureDir(t),
		Filter:        binlog.Filter{SelectedTxIDs: []string{sel}},
		Mode:          scan.ModeSelectedSQL,
		SchemaFetcher: fixtureSchema,
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, sel, out[0].Meta.TxID)
	require.NotEmpty(t, out[0].SQL)
}

func TestStream_MaxPreviewCap(t *testing.T) {
	out, err := collect(t, scan.Config{
		ArchiveDir:    fixtureDir(t),
		Filter:        binlog.Filter{},
		Mode:          scan.ModeMetaOnly,
		SchemaFetcher: fixtureSchema,
		MaxPreview:    1,
	})
	require.NoError(t, err)
	require.Len(t, out, 1, "达到 MaxPreview 即停")
}

// craftBigMultiTxBinlog 构造 n 个匿名事务（TableMap + WRITE + XID）的 binlog
// 字节，事件数 ≈ 2n+2。用于构造「MaxPreview 早退时归档远未扫完」的大文件。
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

// TestStream_MaxPreviewStopsScan 回归 final review Minor：MaxPreview 早退必须
// 取消扫描，否则 FileSource 解析 goroutine 会继续扫完整个归档（大文件白耗 IO）。
//
// 构造一个 MaxPreview 远不能覆盖的大归档（30 万事务 ≈ 60 万事件 ≈ 数十 MB），
// MaxPreview=1 时 Stream 必须快速返回且只产出 1 条结果；随后扫描相关 goroutine
// 必须在短时限内退出——不取消时解析会继续读完整归档（数十 MB 白读），取消后
// FileSource.run 立即停止。
//
// 说明：goroutine 退出断言是启发式的（取消后退出 <100ms，不取消则要等整个
// 归档解析完，本文件规模远超该时限窗口），配合代码审查（defer cancel 覆盖
// 所有退出路径）构成该修复的验证。
func TestStream_MaxPreviewStopsScan(t *testing.T) {
	dir := t.TempDir()
	data := craftBigMultiTxBinlog(300_000)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0o644))

	before := runtime.NumGoroutine()
	start := time.Now()
	out, errCh := scan.Stream(context.Background(), scan.Config{
		ArchiveDir:    dir,
		Filter:        binlog.Filter{},
		Mode:          scan.ModeMetaOnly,
		SchemaFetcher: fixtureSchema,
		MaxPreview:    1,
	})
	var results []scan.Result
	for r := range out {
		results = append(results, r)
	}
	require.NoError(t, <-errCh)
	require.Len(t, results, 1, "MaxPreview=1 只产出 1 条结果")
	require.Less(t, time.Since(start), 2*time.Second, "MaxPreview 早退必须快速返回，不能等整个归档扫完")

	// 解析 goroutine（runParseLoop / FileSource.run）必须在短时限内退出。
	deadline := time.Now().Add(400 * time.Millisecond)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatal("MaxPreview 早退后扫描 goroutine 仍在运行（ctx 未被取消，仍在读归档）")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
