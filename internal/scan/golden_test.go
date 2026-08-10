package scan_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/scan"
)

// fixture 由 testdata/setup.sql 生成：INSERT 2 行 → UPDATE id=1 → DELETE id=2，
// 每次操作一个事务（setup.sql 无显式事务，autocommit 各自成事务）。
// 此处断言 WITH_SQL 模式产出的逆向 SQL 精确覆盖三种操作类型。
func TestGolden_SetupSQLReverseStatements(t *testing.T) {
	out, err := collect(t, scan.Config{
		ArchiveDir:    fixtureDir(t),
		Filter:        binlog.Filter{Tables: []binlog.TableRef{{Schema: "shop", Table: "orders"}}},
		Mode:          scan.ModeWithSQL,
		SchemaFetcher: fixtureSchema,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out)

	var deletes, updates, inserts int // 逆向语句类型计数
	for _, r := range out {
		for _, s := range r.SQL {
			switch {
			case s.SQL == "":
				continue // warning-only
			case len(s.SQL) > 12 && s.SQL[:6] == "DELETE":
				deletes++
			case len(s.SQL) > 6 && s.SQL[:6] == "UPDATE":
				updates++
			case len(s.SQL) > 6 && s.SQL[:6] == "INSERT":
				inserts++
			}
		}
	}
	// setup.sql：1 个 INSERT 语句插入 2 行（→ 逆向 DELETE×2）、1 个 UPDATE（→ 逆向 UPDATE×1）、
	// 1 个 DELETE（→ 逆向 INSERT×1）
	require.Equal(t, 2, deletes, "逆向 DELETE（还原被误删行）")
	require.Equal(t, 1, updates, "逆向 UPDATE")
	require.Equal(t, 1, inserts, "逆向 INSERT")
}
