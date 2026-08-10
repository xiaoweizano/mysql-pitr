package binlog

import (
	"context"
	"fmt"
)

// ColumnDef 描述一列的元数据。
type ColumnDef struct {
	Name      string
	Type      string
	Nullable  bool
	IsAutoInc bool
}

// TableSchema 是 SchemaFetcher 返回的表结构。
type TableSchema struct {
	Schema  string
	Table   string
	Columns []ColumnDef
}

// SchemaFetcher 拉取表结构信息。实现包括：
//   - StaticSchemaFetcher（map-based，用于测试）
//   - MySQLSchemaFetcher（连接真 MySQL，后续 task 实现）
type SchemaFetcher interface {
	FetchSchema(ctx context.Context, schema, table string) (TableSchema, error)
}

// StaticSchemaFetcher 用 map 提供静态 schema，仅用于测试和注入。
// 键格式："<schema>.<table>"。
type StaticSchemaFetcher map[string]TableSchema

func (s StaticSchemaFetcher) FetchSchema(_ context.Context, schema, table string) (TableSchema, error) {
	key := schema + "." + table
	sch, ok := s[key]
	if !ok {
		return TableSchema{}, fmt.Errorf("binlog: schema %q not found in static fetcher", key)
	}
	return sch, nil
}
