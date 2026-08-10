package binlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTransaction_WithGTID(t *testing.T) {
	ct := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	tx, err := NewTransaction("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5", 0, ct, "shop")
	require.NoError(t, err)
	assert.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5", tx.TxID)
	assert.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5", tx.GTID)
	assert.Equal(t, uint64(0), tx.XID)
	assert.Equal(t, ct, tx.CommitTime)
	assert.Equal(t, "shop", tx.Schema)
	assert.False(t, tx.Truncated)
	assert.Empty(t, tx.Statements)
}

func TestNewTransaction_WithXID(t *testing.T) {
	ct := time.Now().UTC()
	tx, err := NewTransaction("", 42, ct, "")
	require.NoError(t, err)
	assert.Equal(t, "xid-42", tx.TxID)
	assert.Equal(t, uint64(42), tx.XID)
	assert.Empty(t, tx.GTID)
}

func TestNewTransaction_NoID(t *testing.T) {
	ct := time.Now().UTC()
	tx, err := NewTransaction("", 0, ct, "")
	require.NoError(t, err)
	assert.NotEmpty(t, tx.TxID)
	assert.Contains(t, tx.TxID, "tx-")
}

func TestNewTransaction_ZeroTime(t *testing.T) {
	_, err := NewTransaction("uuid:1-1", 0, time.Time{}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit time")
}

func TestTransaction_AppendRow(t *testing.T) {
	tx, _ := NewTransaction("uuid:1-1", 0, time.Now().UTC(), "shop")
	rc := RowChange{Schema: "shop", Table: "orders", Action: ActionInsert}
	tx.AppendRow(rc)
	assert.Len(t, tx.Statements, 1)
}

func TestTransaction_MarkTruncated(t *testing.T) {
	tx, _ := NewTransaction("uuid:1-1", 0, time.Now().UTC(), "shop")
	tx.MarkTruncated()
	assert.True(t, tx.Truncated)
}

func TestTransaction_RowCount(t *testing.T) {
	tx, _ := NewTransaction("uuid:1-1", 0, time.Now().UTC(), "shop")
	tx.AppendRow(RowChange{Before: []interface{}{1, 2}, After: []interface{}{3}})
	tx.AppendRow(RowChange{After: []interface{}{"a", "b"}})
	// Before+After 总数：(2+1) + (0+2) = 5
	assert.Equal(t, 5, tx.RowCount())
}

func TestTransaction_RowCountFallback(t *testing.T) {
	tx, _ := NewTransaction("uuid:1-1", 0, time.Now().UTC(), "shop")
	tx.AppendRow(RowChange{})
	tx.AppendRow(RowChange{})
	// 没有 Before/After 值 → 回退为 statement 数
	assert.Equal(t, 2, tx.RowCount())
}
