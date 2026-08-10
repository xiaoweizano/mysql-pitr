package reverse

import (
	"fmt"
	"strings"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

// Generate 把一个 Transaction 翻成 0..N 条逆向 SQL。
//   - DELETE → INSERT（用 Before image）
//   - INSERT → DELETE（用 After image 拼 WHERE）
//   - UPDATE → UPDATE（用 After image 拼 WHERE，Before image 作 SET）
//
// 同事务内严格 LIFO（后写入先回滚）。
//
// schema 形如 {"shop.orders": TableSchema{...}}；缺表则 warning + 跳过。
func Generate(tx *binlog.Transaction, schema map[string]binlog.TableSchema, opts Options) ([]Statement, error) {
	if tx == nil {
		return nil, fmt.Errorf("reverse: nil transaction")
	}
	if tx.TxID == "" {
		return nil, fmt.Errorf("reverse: transaction.TxID is required")
	}
	if opts.MaxStatementSize == 0 {
		opts.MaxStatementSize = DefaultMaxStatementSize
	}

	var out []Statement
	// LIFO：倒序遍历 Statements
	n := len(tx.Statements)
	for i := n - 1; i >= 0; i-- {
		rc := tx.Statements[i]
		key := rc.Schema + "." + rc.Table
		sch, ok := schema[key]
		if !ok {
			out = append(out, Statement{
				SQL:       "",
				TxID:      tx.TxID,
				TxOrder:   n - 1 - i,
				SourceRow: rc,
				Warnings:  []string{"schema not found for " + key},
			})
			continue
		}

		cols := columnNames(rc, sch)
		if len(cols) == 0 {
			out = append(out, Statement{
				SQL:       "",
				TxID:      tx.TxID,
				TxOrder:   n - 1 - i,
				SourceRow: rc,
				Warnings:  []string{"no column names available for " + key},
			})
			continue
		}

		sql, warn := buildReverseSQL(rc, cols, sch.PrimaryKey)
		if sql == "" {
			out = append(out, Statement{
				SQL:       "",
				TxID:      tx.TxID,
				TxOrder:   n - 1 - i,
				SourceRow: rc,
				Warnings:  warn,
			})
			continue
		}
		if len(sql) > opts.MaxStatementSize {
			out = append(out, Statement{
				SQL:       "",
				TxID:      tx.TxID,
				TxOrder:   n - 1 - i,
				SourceRow: rc,
				Warnings:  []string{fmt.Sprintf("SQL exceeds MaxStatementSize %d", opts.MaxStatementSize)},
			})
			continue
		}
		out = append(out, Statement{
			SQL:       sql,
			TxID:      tx.TxID,
			TxOrder:   n - 1 - i,
			SourceRow: rc,
			Warnings:  warn,
		})
	}
	return out, nil
}

func columnNames(rc binlog.RowChange, sch binlog.TableSchema) []string {
	if len(rc.ColumnNames) > 0 {
		return rc.ColumnNames
	}
	names := make([]string, len(sch.Columns))
	for i, c := range sch.Columns {
		names[i] = c.Name
	}
	return names
}

func buildReverseSQL(rc binlog.RowChange, cols []string, pk []string) (string, []string) {
	q := quoteIdent
	switch rc.Action {
	case binlog.ActionDelete:
		// → INSERT INTO ... VALUES (...)
		// DELETE 的 After image 本就为空，只校验应存在的 Before image。
		var warns []string
		if len(rc.Before) != len(cols) {
			warns = append(warns, fmt.Sprintf("before image has %d values but schema has %d columns", len(rc.Before), len(cols)))
		}
		return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			q(rc.Schema), q(rc.Table),
			joinQuoted(cols, q),
			joinValues(rc.Before)), warns

	case binlog.ActionInsert:
		// → DELETE FROM ... WHERE <pk or all cols> = <after values>
		// INSERT 的 Before image 本就为空，只校验应存在的 After image。
		var warns []string
		if len(rc.After) != len(cols) {
			warns = append(warns, fmt.Sprintf("after image has %d values but schema has %d columns", len(rc.After), len(cols)))
		}
		where, w := buildWhere(rc, cols, pk)
		warns = append(warns, w...)
		return fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
			q(rc.Schema), q(rc.Table), where), warns

	case binlog.ActionUpdate:
		// → UPDATE ... SET <before> WHERE <after>
		// UPDATE 两幅 image 都应存在，任一缺失都告警。SET 用全部列（Before 镜像），WHERE 优先主键。
		var warns []string
		if len(rc.Before) != len(cols) {
			warns = append(warns, fmt.Sprintf("before image has %d values but schema has %d columns", len(rc.Before), len(cols)))
		}
		if len(rc.After) != len(cols) {
			warns = append(warns, fmt.Sprintf("after image has %d values but schema has %d columns", len(rc.After), len(cols)))
		}
		setParts := make([]string, len(cols))
		for i, c := range cols {
			setParts[i] = fmt.Sprintf("%s = %s", q(c), valueAt(rc.Before, i))
		}
		where, w := buildWhere(rc, cols, pk)
		warns = append(warns, w...)
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
			q(rc.Schema), q(rc.Table),
			strings.Join(setParts, ", "),
			where), warns

	default:
		return "", []string{fmt.Sprintf("unknown action %d", rc.Action)}
	}
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func joinQuoted(cols []string, q func(string) string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = q(c)
	}
	return strings.Join(out, ", ")
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32, float64:
		return fmt.Sprintf("%v", x)
	case string:
		// 简化：单引号转义
		escaped := strings.ReplaceAll(x, "'", "''")
		return "'" + escaped + "'"
	case []byte:
		// 二进制：用 _binary 'x'
		return fmt.Sprintf("_binary '%x'", x)
	case bool:
		if x {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("'%v'", x)
	}
}

func joinValues(vs []interface{}) string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = formatValue(v)
	}
	return strings.Join(out, ", ")
}

// valueAt 返回 values[i] 的 SQL 字面量；越界时以 NULL 补齐。
// image 可能比 schema 列少（schema 漂移 / 截断事务），此处绝不允许 panic。
func valueAt(values []interface{}, i int) string {
	if i < len(values) {
		return formatValue(values[i])
	}
	return "NULL"
}

// indexOf 返回 s 在 list 中的下标；不存在返回 -1。
func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}

// resolvePKIndices 把主键列名解析为列下标（按表列序）。
// 主键名在行列中不存在（如 RowChange.ColumnNames 覆盖了 schema 列名）时跳过并告警。
func resolvePKIndices(cols, pk []string) ([]int, []string) {
	idx := make([]int, 0, len(pk))
	var warns []string
	for _, p := range pk {
		if i := indexOf(cols, p); i >= 0 {
			idx = append(idx, i)
		} else {
			warns = append(warns, fmt.Sprintf("primary key column %q not found in row columns; skipped in WHERE", p))
		}
	}
	return idx, warns
}

// buildWhere 构造 WHERE 谓词：主键存在时只用主键列定位（值从 rc.After 按列序取值），
// 无主键或主键列全部不可用时回退全列匹配。主键列在行镜像中缺值（nil / 越界）
// 时跳过该列并告警——绝不 panic，也不用 IS NULL 猜测匹配。
func buildWhere(rc binlog.RowChange, cols []string, pk []string) (string, []string) {
	var warns []string
	if len(pk) > 0 {
		idxs, w := resolvePKIndices(cols, pk)
		warns = append(warns, w...)
		usable := make([]int, 0, len(idxs))
		for _, i := range idxs {
			if i >= len(rc.After) || rc.After[i] == nil {
				warns = append(warns, fmt.Sprintf("primary key column %q has no value in row image; skipped in WHERE", cols[i]))
				continue
			}
			usable = append(usable, i)
		}
		if len(usable) > 0 {
			return buildWhereColsAt(cols, usable, rc.After), warns
		}
		// 主键列全部不可用 → 回退全列匹配
	}
	return buildWhereCols(cols, rc.After), warns
}

// buildWhereCols 把 values 按列序拼成 WHERE 谓词；nil 值或缺失值（image 比列数少）
// 渲染为 `col` IS NULL 而非 `col` = NULL —— 后者在 MySQL 中恒为 NULL（未知），
// 永不匹配任何行，会让逆向 DELETE/UPDATE 静默地影响 0 行。
func buildWhereCols(cols []string, values []interface{}) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		col := quoteIdent(c)
		if i < len(values) && values[i] != nil {
			parts[i] = fmt.Sprintf("%s = %s", col, formatValue(values[i]))
		} else {
			parts[i] = fmt.Sprintf("%s IS NULL", col)
		}
	}
	return strings.Join(parts, " AND ")
}

// buildWhereColsAt 用 cols 中指定下标从 values 取值拼 WHERE 谓词（谓词列序按下标序）。
// 与 buildWhereCols 相同的 nil/缺位 → IS NULL 规则（valueAt 越界保护，绝不 panic）。
func buildWhereColsAt(cols []string, idxs []int, values []interface{}) string {
	parts := make([]string, len(idxs))
	for k, i := range idxs {
		col := quoteIdent(cols[i])
		if i < len(values) && values[i] != nil {
			parts[k] = fmt.Sprintf("%s = %s", col, formatValue(values[i]))
		} else {
			parts[k] = fmt.Sprintf("%s IS NULL", col)
		}
	}
	return strings.Join(parts, " AND ")
}
