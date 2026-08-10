package binlogtest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

// TestCraftUpdateRows_ParsesInterleaved 用 go-mysql BinlogParser 解析
// CraftUpdateRows 生成的原始字节，验证 UPDATE 行按 (before, after) 配对正确。
//
// 布局须与 go-mysql RowsEvent.DecodeData 一致：每轮循环先用 ColumnBitmap1
// 解 before 镜像、再用 ColumnBitmap2 解 after 镜像，即字节流按行交错
// before(1), after(1), before(2), after(2), ...。若先输出全部 before 再输出
// 全部 after，n≥2 时解析仍成功但行被静默错配（本意 (1→4),(2→5),(3→6) 会
// 被解成 (1→2),(3→4),(5→6)）。
func TestCraftUpdateRows_ParsesInterleaved(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{
		{name: "single row", n: 1}, // n=1 时两种布局字节相同，作基线
		{name: "three rows", n: 3}, // n≥2 时暴露交错问题（回归用例）
	} {
		t.Run(tc.name, func(t *testing.T) {
			const tableID = 1
			f := CraftFile([]Event{
				MustCraft(CraftFDE()),
				MustCraft(CraftTableMap("shop", "orders", tableID)),
				MustCraft(CraftUpdateRows(tableID, tc.n)),
			})

			path := filepath.Join(t.TempDir(), "mysql-bin.000001")
			require.NoError(t, os.WriteFile(path, f, 0o644))

			parser := replication.NewBinlogParser()
			var rows *replication.RowsEvent
			require.NoError(t, parser.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
				if re, ok := ev.Event.(*replication.RowsEvent); ok {
					rows = re
				}
				return nil
			}), "CraftUpdateRows(%d) 应能被 go-mysql 完整解析", tc.n)

			require.NotNil(t, rows, "应解析出 RowsEvent")
			require.Equal(t, replication.EnumRowsEventTypeUpdate, rows.Type())
			require.Equal(t, tc.n*2, len(rows.Rows), "%d 行 UPDATE 应有 %d 幅镜像", tc.n, tc.n*2)

			for i := 0; i < tc.n; i++ {
				before := rows.Rows[2*i]
				after := rows.Rows[2*i+1]
				require.Equal(t, []any{int64(i + 1)}, before, "第 %d 行的 before 镜像应为 %d", i, i+1)
				require.Equal(t, []any{int64(tc.n + i + 1)}, after, "第 %d 行的 after 镜像应为 %d", i, tc.n+i+1)
			}
		})
	}
}
