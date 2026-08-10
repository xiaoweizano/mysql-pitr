package reverse

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

func schemaFor(cols ...string) binlog.TableSchema {
	cd := make([]binlog.ColumnDef, len(cols))
	for i, c := range cols {
		cd[i] = binlog.ColumnDef{Name: c, Type: "VARCHAR(32)"}
	}
	return binlog.TableSchema{Schema: "shop", Table: "orders", Columns: cd}
}

func mustTx(t *testing.T, gtid string, rows ...binlog.RowChange) *binlog.Transaction {
	t.Helper()
	tx, err := binlog.NewTransaction(gtid, 0, time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC), "shop")
	require.NoError(t, err)
	for _, r := range rows {
		tx.AppendRow(r)
	}
	return &tx
}

func TestGenerate_DeleteToInsert(t *testing.T) {
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-1", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionDelete,
		Before: []interface{}{int64(42), 19.99},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].SQL, "INSERT INTO `shop`.`orders`")
	assert.Contains(t, stmts[0].SQL, "`id`")
	assert.Contains(t, stmts[0].SQL, "`amount`")
	assert.Contains(t, stmts[0].SQL, "42")
	assert.Contains(t, stmts[0].SQL, "19.99")
	assert.Equal(t, "uuid:1-1", stmts[0].TxID)
	assert.Equal(t, 0, stmts[0].TxOrder)
}

func TestGenerate_InsertToDelete(t *testing.T) {
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-2", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After: []interface{}{int64(42), 19.99},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].SQL, "DELETE FROM `shop`.`orders`")
	assert.Contains(t, stmts[0].SQL, "`id` = 42")
}

func TestGenerate_UpdateSwap(t *testing.T) {
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "status"),
	}
	tx := mustTx(t, "uuid:1-3", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(42), "new"},
		After:  []interface{}{int64(42), "paid"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].SQL, "UPDATE `shop`.`orders`")
	assert.Contains(t, stmts[0].SQL, "`status` = 'new'")  // SET 旧值
	assert.Contains(t, stmts[0].SQL, "`id` = 42")         // WHERE 用当前值（After）
	assert.Contains(t, stmts[0].SQL, "`status` = 'paid'") // WHERE 用当前值
}

func TestGenerate_UpdateBeforeShorterThanColumns(t *testing.T) {
	// schema 漂移：binlog 写入后 ALTER TABLE ADD COLUMN，Before image 比列少。
	// 不得 panic，缺失列以 NULL 补齐 SET，并携带 warning。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "status"),
	}
	tx := mustTx(t, "uuid:1-4", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(42)}, // 少一个值
		After:  []interface{}{int64(42), "paid"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `id` = 42, `status` = NULL WHERE `id` = 42 AND `status` = 'paid'",
		stmts[0].SQL)
	assert.Equal(t, []string{"before image has 1 values but schema has 2 columns"}, stmts[0].Warnings)
}

func TestGenerate_UpdateEmptyBefore(t *testing.T) {
	// Before image 为空：不得 panic，SET 全部列以 NULL 补齐。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "status"),
	}
	tx := mustTx(t, "uuid:1-5", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: nil,
		After:  []interface{}{int64(42), "paid"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `id` = NULL, `status` = NULL WHERE `id` = 42 AND `status` = 'paid'",
		stmts[0].SQL)
	assert.Equal(t, []string{"before image has 0 values but schema has 2 columns"}, stmts[0].Warnings)
}

func TestGenerate_InsertWithNullValue(t *testing.T) {
	// 可空列插入 NULL → WHERE 必须用 IS NULL（col = NULL 在 MySQL 中永不匹配）。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-6", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After: []interface{}{int64(42), nil},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "DELETE FROM `shop`.`orders` WHERE `id` = 42 AND `amount` IS NULL", stmts[0].SQL)
}

func TestGenerate_InsertAfterShorterThanColumns(t *testing.T) {
	// After image 缺位补 NULL → WHERE 同样渲染为 IS NULL。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-7", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After: []interface{}{int64(42)}, // 少一个值
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "DELETE FROM `shop`.`orders` WHERE `id` = 42 AND `amount` IS NULL", stmts[0].SQL)
}

func TestBuildWhere_NilAndMissingValues(t *testing.T) {
	cols := []string{"a", "b"}
	assert.Equal(t, "`a` = 1 AND `b` IS NULL", buildWhere(cols, []interface{}{int64(1), nil}))
	assert.Equal(t, "`a` = 1 AND `b` IS NULL", buildWhere(cols, []interface{}{int64(1)}))
	assert.Equal(t, "`a` IS NULL AND `b` IS NULL", buildWhere(cols, nil))
}

func TestGenerate_DDLWarning(t *testing.T) {
	// DDL 在 scanner 里就被跳过（不出 RowChange），但若有人造一个 Action=255 的
	// RowChange 喂进来，Generate 应标 warning 而非 panic。
	tx := mustTx(t, "uuid:1-9", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.RowAction(255),
	})
	sch := map[string]binlog.TableSchema{"shop.orders": schemaFor("id")}
	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Empty(t, stmts[0].SQL)
	assert.NotEmpty(t, stmts[0].Warnings)
}

func TestGenerate_SchemaMissing(t *testing.T) {
	tx := mustTx(t, "uuid:1-10", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionDelete,
		Before: []interface{}{int64(1)},
	})
	stmts, err := Generate(tx, map[string]binlog.TableSchema{}, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Empty(t, stmts[0].SQL)
	assert.Contains(t, stmts[0].Warnings[0], "schema not found")
}

func TestGenerate_OversizedSQLWarning(t *testing.T) {
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "payload"),
	}
	bigStr := strings.Repeat("x", 20_000)
	tx := mustTx(t, "uuid:1-11", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After: []interface{}{int64(1), bigStr},
	})
	stmts, err := Generate(tx, sch, Options{MaxStatementSize: 100})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Empty(t, stmts[0].SQL)
	assert.Contains(t, stmts[0].Warnings[0], "MaxStatementSize")
}

func TestGenerate_NilTx(t *testing.T) {
	_, err := Generate(nil, nil, Options{})
	require.Error(t, err)
}

func TestGenerate_EmptyTxID(t *testing.T) {
	tx := &binlog.Transaction{} // TxID 为空
	_, err := Generate(tx, nil, Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TxID is required")
}

func TestGenerate_MultipleRowsLIFO(t *testing.T) {
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id"),
	}
	tx := mustTx(t, "uuid:1-12",
		binlog.RowChange{Schema: "shop", Table: "orders", Action: binlog.ActionInsert, After: []interface{}{int64(1)}},
		binlog.RowChange{Schema: "shop", Table: "orders", Action: binlog.ActionInsert, After: []interface{}{int64(2)}},
		binlog.RowChange{Schema: "shop", Table: "orders", Action: binlog.ActionInsert, After: []interface{}{int64(3)}},
	)
	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	// LIFO：第一条 SQL 应是 id=3 的 DELETE
	assert.Contains(t, stmts[0].SQL, "`id` = 3")
	assert.Contains(t, stmts[2].SQL, "`id` = 1")
	// TxOrder 从 0 开始
	assert.Equal(t, 0, stmts[0].TxOrder)
	assert.Equal(t, 2, stmts[2].TxOrder)
}

func TestFormatValue_Types(t *testing.T) {
	assert.Equal(t, "42", formatValue(int64(42)))
	assert.Equal(t, "NULL", formatValue(nil))
	assert.Equal(t, "'hello'", formatValue("hello"))
	assert.Equal(t, "'it''s'", formatValue("it's"))
	assert.Equal(t, "1", formatValue(true))
	assert.Equal(t, "0", formatValue(false))
}

func TestFormatValue_MoreTypes(t *testing.T) {
	// formatValue 其余类型分支：float / []byte / 默认（未知类型）。
	assert.Equal(t, "19.99", formatValue(19.99))
	assert.Equal(t, "_binary '6162'", formatValue([]byte{0x61, 0x62}))
	assert.Equal(t, "'{1}'", formatValue(struct{ x int }{1}))
}

func TestGenerate_CleanRowsNoWarnings(t *testing.T) {
	// image 完整的正常行不得产生任何 warning（修复前会误报空 Before/After）。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tests := []struct {
		name string
		rc   binlog.RowChange
		want string
	}{
		{
			name: "insert",
			rc:   binlog.RowChange{Schema: "shop", Table: "orders", Action: binlog.ActionInsert, After: []interface{}{int64(42), 19.99}},
			want: "DELETE FROM `shop`.`orders` WHERE `id` = 42 AND `amount` = 19.99",
		},
		{
			name: "delete",
			rc:   binlog.RowChange{Schema: "shop", Table: "orders", Action: binlog.ActionDelete, Before: []interface{}{int64(42), 19.99}},
			want: "INSERT INTO `shop`.`orders` (`id`, `amount`) VALUES (42, 19.99)",
		},
		{
			name: "update",
			rc:   binlog.RowChange{Schema: "shop", Table: "orders", Action: binlog.ActionUpdate, Before: []interface{}{int64(42), "new"}, After: []interface{}{int64(42), "paid"}},
			want: "UPDATE `shop`.`orders` SET `id` = 42, `amount` = 'new' WHERE `id` = 42 AND `amount` = 'paid'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := mustTx(t, "uuid:1-13", tt.rc)
			stmts, err := Generate(tx, sch, Options{})
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Equal(t, tt.want, stmts[0].SQL)
			assert.Empty(t, stmts[0].Warnings)
		})
	}
}

func TestGenerate_DeleteBeforeShorterThanColumns(t *testing.T) {
	// DELETE 的 Before image 缺值：warning + INSERT 继续（空列位不补齐）。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-14", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionDelete,
		Before: []interface{}{int64(42)}, // 少一个值
	})
	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, []string{"before image has 1 values but schema has 2 columns"}, stmts[0].Warnings)
	assert.Equal(t, "INSERT INTO `shop`.`orders` (`id`, `amount`) VALUES (42)", stmts[0].SQL)
}

func TestGenerate_UpdateAfterShorterThanColumns(t *testing.T) {
	// UPDATE 的 After image 缺值：warning，缺失列 WHERE 以 IS NULL 渲染。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-15", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(42), "new"},
		After:  []interface{}{int64(42)}, // 少一个值
	})
	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, []string{"after image has 1 values but schema has 2 columns"}, stmts[0].Warnings)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `id` = 42, `amount` = 'new' WHERE `id` = 42 AND `amount` IS NULL",
		stmts[0].SQL)
}

func TestGenerate_ColumnNamesOverride(t *testing.T) {
	// RowChange.ColumnNames 优先于 schema 列名（columnNames 的 rc.ColumnNames 分支）。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "amount"),
	}
	tx := mustTx(t, "uuid:1-16", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After:       []interface{}{int64(7)},
		ColumnNames: []string{"order_id"},
	})
	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "DELETE FROM `shop`.`orders` WHERE `order_id` = 7", stmts[0].SQL)
}

func TestGenerate_NoColumnNames(t *testing.T) {
	// schema 无列且 RowChange 无 ColumnNames：warning + 跳过。
	tx := mustTx(t, "uuid:1-17", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After: []interface{}{int64(1)},
	})
	stmts, err := Generate(tx, map[string]binlog.TableSchema{"shop.orders": {}}, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Empty(t, stmts[0].SQL)
	assert.Contains(t, stmts[0].Warnings[0], "no column names available")
}

func TestQuoteIdent_EscapesBackticks(t *testing.T) {
	assert.Equal(t, "`a``b`", quoteIdent("a`b"))
}
