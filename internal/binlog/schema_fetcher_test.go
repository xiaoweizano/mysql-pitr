package binlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticSchemaFetcher_Found(t *testing.T) {
	s := StaticSchemaFetcher{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []ColumnDef{
			{Name: "id", Type: "BIGINT", IsAutoInc: true},
			{Name: "amount", Type: "DECIMAL(10,2)", Nullable: true},
		}, PrimaryKey: []string{"id"}},
	}
	sch, err := s.FetchSchema(context.Background(), "shop", "orders")
	require.NoError(t, err)
	assert.Equal(t, "shop", sch.Schema)
	assert.Len(t, sch.Columns, 2)
	assert.True(t, sch.Columns[0].IsAutoInc)
	assert.Equal(t, []string{"id"}, sch.PrimaryKey)
}

func TestStaticSchemaFetcher_NotFound(t *testing.T) {
	s := StaticSchemaFetcher{}
	_, err := s.FetchSchema(context.Background(), "shop", "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
