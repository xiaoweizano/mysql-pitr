package scan_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
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
