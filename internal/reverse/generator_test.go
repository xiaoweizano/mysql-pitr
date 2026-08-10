package reverse

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
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

func TestBuildWhereCols_NilAndMissingValues(t *testing.T) {
	// 低层全列匹配：nil 值或缺位值（image 比列数少）渲染为 IS NULL，绝不 panic。
	cols := []string{"a", "b"}
	assert.Equal(t, "`a` = 1 AND `b` IS NULL", buildWhereCols(cols, []interface{}{int64(1), nil}))
	assert.Equal(t, "`a` = 1 AND `b` IS NULL", buildWhereCols(cols, []interface{}{int64(1)}))
	assert.Equal(t, "`a` IS NULL AND `b` IS NULL", buildWhereCols(cols, nil))
}

func TestBuildWhere_PrimaryKeyPrefersPK(t *testing.T) {
	// 主键存在时 WHERE 只用主键列。
	rc := binlog.RowChange{After: []interface{}{int64(1), "x"}}
	where, warns := buildWhere(rc, []string{"id", "status"}, []string{"id"})
	assert.Equal(t, "`id` = 1", where)
	assert.Empty(t, warns)
}

func TestBuildWhere_NoPrimaryKeyAllColumns(t *testing.T) {
	// 无主键：全列匹配（与 v2 行为一致），nil 值渲染 IS NULL。
	rc := binlog.RowChange{After: []interface{}{int64(1), nil}}
	where, warns := buildWhere(rc, []string{"id", "status"}, nil)
	assert.Equal(t, "`id` = 1 AND `status` IS NULL", where)
	assert.Empty(t, warns)
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
	assert.Equal(t, "X'6162'", formatValue([]byte{0x61, 0x62}))
	assert.Equal(t, "'{1}'", formatValue(struct{ x int }{1}))
}

func TestFormatValue_TimeAndDecimal(t *testing.T) {
	loc := time.UTC
	ts := time.Date(2026, 8, 10, 12, 30, 45, 0, loc)
	// 通过 Generate 验证 time.Time / decimal.Decimal 被正确渲染
	tx := &binlog.Transaction{
		TxID: "test-2", CommitTime: ts,
		Statements: []binlog.RowChange{{
			Schema: "shop", Table: "orders",
			Action: binlog.ActionInsert,
			After: []interface{}{
				uint64(1), decimal.NewFromFloat(19.99), ts, []byte{0x01, 0xff},
			},
		}},
	}
	schema := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns: []binlog.ColumnDef{
				{Name: "id"}, {Name: "amount"}, {Name: "created_at"}, {Name: "blob_col"},
			},
		},
	}
	stmts, err := Generate(tx, schema, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].SQL, "'2026-08-10 12:30:45'")
	require.Contains(t, stmts[0].SQL, "19.99")
	require.Contains(t, stmts[0].SQL, "X'01ff'")
}

func TestFormatValue_StringEscapingAndBinary(t *testing.T) {
	// 字符串：单引号与反斜杠都要转义（MySQL 默认 NO_BACKSLASH_ESCAPES=off，
	// 反斜杠也是转义符，必须写双反斜杠）。
	// []byte：一律 X'hex' 字面量还原原始字节（评审发现，baseline 带入）。
	tests := []struct {
		name  string
		in    interface{}
		valid string
	}{
		{name: "backslash", in: "a\\b", valid: `'a\\b'`},
		{name: "quote and backslash", in: `it's\`, valid: `'it''s\\'`},
		{name: "binary bytes", in: []byte{0x01, 0xff}, valid: "X'01ff'"},
		{name: "binary text bytes", in: []byte{0x61, 0x62}, valid: "X'6162'"},
		{name: "binary empty", in: []byte{}, valid: "X''"},
		{name: "decimal", in: decimal.NewFromFloat(123.45), valid: "123.45"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.valid, formatValue(tt.in))
		})
	}
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

func TestGenerate_UpdateUsesPrimaryKeyInWhere(t *testing.T) {
	// v3 spec：UPDATE 回滚的 WHERE 只用主键列定位，不匹配非主键列；SET 仍用全部列（Before 镜像）。
	sch := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns:    []binlog.ColumnDef{{Name: "id"}, {Name: "status"}},
			PrimaryKey: []string{"id"},
		},
	}
	tx := mustTx(t, "uuid:1-18", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(1), "old"},
		After:  []interface{}{int64(1), "new"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `id` = 1, `status` = 'old' WHERE `id` = 1",
		stmts[0].SQL)
	require.Contains(t, stmts[0].SQL, "WHERE `id` = 1")
	require.NotContains(t, stmts[0].SQL, "`status` = 'new'")
	require.NotContains(t, stmts[0].SQL, "`status` IS NULL")
	assert.Empty(t, stmts[0].Warnings)
}

func TestGenerate_InsertUsesPrimaryKeyInWhere(t *testing.T) {
	// INSERT 回滚（DELETE）同样优先主键定位。
	sch := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns:    []binlog.ColumnDef{{Name: "id"}, {Name: "status"}},
			PrimaryKey: []string{"id"},
		},
	}
	tx := mustTx(t, "uuid:1-19", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionInsert,
		After: []interface{}{int64(7), "paid"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "DELETE FROM `shop`.`orders` WHERE `id` = 7", stmts[0].SQL)
	assert.Empty(t, stmts[0].Warnings)
}

func TestGenerate_PrimaryKeyValueMissingFallsBack(t *testing.T) {
	// 主键值在镜像中为 nil：跳过该主键列；全部主键列被跳过时回退全列匹配并告警。
	sch := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns:    []binlog.ColumnDef{{Name: "id"}, {Name: "status"}},
			PrimaryKey: []string{"id"},
		},
	}
	tx := mustTx(t, "uuid:1-20", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(1), "old"},
		After:  []interface{}{nil, "paid"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `id` = 1, `status` = 'old' WHERE `id` IS NULL AND `status` = 'paid'",
		stmts[0].SQL)
	assert.Equal(t, []string{`primary key column "id" has no value in row image; skipped in WHERE`}, stmts[0].Warnings)
}

func TestGenerate_PrimaryKeyOutOfBoundsFallsBack(t *testing.T) {
	// 镜像比列数少且主键列缺位：不 panic，跳过该主键列、回退全列匹配，两条告警。
	sch := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns:    []binlog.ColumnDef{{Name: "status"}, {Name: "id"}},
			PrimaryKey: []string{"id"},
		},
	}
	tx := mustTx(t, "uuid:1-21", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{"old", int64(1)},
		After:  []interface{}{"paid"}, // id 缺位
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `status` = 'old', `id` = 1 WHERE `status` = 'paid' AND `id` IS NULL",
		stmts[0].SQL)
	assert.Equal(t, []string{
		"after image has 1 values but schema has 2 columns",
		`primary key column "id" has no value in row image; skipped in WHERE`,
	}, stmts[0].Warnings)
}

func TestGenerate_CompositeKeyPartialSkip(t *testing.T) {
	// 复合主键：一列缺值 → 只用其余主键列定位，不回退全列。
	sch := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns:    []binlog.ColumnDef{{Name: "order_id"}, {Name: "line_no"}, {Name: "status"}},
			PrimaryKey: []string{"order_id", "line_no"},
		},
	}
	tx := mustTx(t, "uuid:1-22", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(9), int64(2), "old"},
		After:  []interface{}{int64(9), nil, "new"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `order_id` = 9, `line_no` = 2, `status` = 'old' WHERE `order_id` = 9",
		stmts[0].SQL)
	assert.Equal(t, []string{`primary key column "line_no" has no value in row image; skipped in WHERE`}, stmts[0].Warnings)
}

func TestGenerate_NoPrimaryKeyFullRowMatch(t *testing.T) {
	// 无主键（PrimaryKey 为空）：全列匹配，与 v2 行为一致。
	sch := map[string]binlog.TableSchema{
		"shop.orders": schemaFor("id", "status"), // PrimaryKey 为空
	}
	tx := mustTx(t, "uuid:1-23", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before: []interface{}{int64(1), "old"},
		After:  []interface{}{int64(1), "new"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `id` = 1, `status` = 'old' WHERE `id` = 1 AND `status` = 'new'",
		stmts[0].SQL)
	assert.Empty(t, stmts[0].Warnings)
}

func TestGenerate_PrimaryKeyColumnMismatchFallsBack(t *testing.T) {
	// RowChange.ColumnNames 覆盖 schema 列名时，主键名对不上行内列名 → 跳过并回退全列匹配。
	sch := map[string]binlog.TableSchema{
		"shop.orders": {
			Schema: "shop", Table: "orders",
			Columns:    []binlog.ColumnDef{{Name: "id"}, {Name: "status"}},
			PrimaryKey: []string{"id"},
		},
	}
	tx := mustTx(t, "uuid:1-24", binlog.RowChange{
		Schema: "shop", Table: "orders", Action: binlog.ActionUpdate,
		Before:      []interface{}{int64(1), "old"},
		After:       []interface{}{int64(1), "new"},
		ColumnNames: []string{"order_id", "status"},
	})

	stmts, err := Generate(tx, sch, Options{})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"UPDATE `shop`.`orders` SET `order_id` = 1, `status` = 'old' WHERE `order_id` = 1 AND `status` = 'new'",
		stmts[0].SQL)
	assert.Equal(t, []string{`primary key column "id" not found in row columns; skipped in WHERE`}, stmts[0].Warnings)
}
