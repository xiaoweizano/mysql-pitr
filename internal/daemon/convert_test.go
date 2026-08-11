package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// TestWSFilterToBinlog 验证 ScanFilter → binlog.Filter 转换：表格、时间（RFC3339）、
// GTIDSet、起止位置、MaxRowsPerTx、SelectedTxIDs 直传。
func TestWSFilterToBinlog(t *testing.T) {
	got, err := wsFilterToBinlog(ws.ScanFilter{
		Tables:        []ws.TableRefJSON{{Schema: "shop", Table: "orders"}},
		TimeStart:     "2026-08-01T00:00:00Z",
		TimeEnd:       "2026-08-02T12:30:00+08:00",
		GTIDSet:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		StartFile:     "mysql-bin.000001",
		StartPos:      4,
		EndFile:       "mysql-bin.000002",
		EndPos:        8,
		MaxRowsPerTx:  100,
		SelectedTxIDs: []string{"xid-19"},
	})
	require.NoError(t, err)

	require.Equal(t, []binlog.TableRef{{Schema: "shop", Table: "orders"}}, got.Tables)
	require.NotNil(t, got.TimeRange)
	require.True(t, got.TimeRange.Start.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)))
	require.True(t, got.TimeRange.End.Equal(time.Date(2026, 8, 2, 4, 30, 0, 0, time.UTC)), "+08:00 等价于 UTC 同一时刻")
	require.NotNil(t, got.GTIDSet)
	require.Equal(t, "mysql-bin.000001", got.StartPos.Name)
	require.Equal(t, uint32(4), got.StartPos.Pos)
	require.Equal(t, "mysql-bin.000002", got.EndPos.Name)
	require.Equal(t, uint32(8), got.EndPos.Pos)
	require.Equal(t, 100, got.MaxRowsPerTx)
	require.Equal(t, []string{"xid-19"}, got.SelectedTxIDs)
}

// TestWSFilterToBinlog_Empty 验证空 filter 转换成功（无时间范围、无 GTID、无位置）。
func TestWSFilterToBinlog_Empty(t *testing.T) {
	got, err := wsFilterToBinlog(ws.ScanFilter{})
	require.NoError(t, err)
	require.Nil(t, got.TimeRange)
	require.Nil(t, got.GTIDSet)
	require.Empty(t, got.StartPos.Name)
	require.Empty(t, got.Tables)
}

// TestWSFilterToBinlog_SingleSidedTime 验证单侧时间边界被钳制为无界：
// 只设 TimeStart → End=timeRangeFarFuture；只设 TimeEnd → Start=zero
// （引擎 Before(zero) 恒 false，即无下界）。回归：缺 End 留 zero 会导致
// 引擎 After(zero) 恒 true、全部事务被静默拒绝（无错误、零结果）。
func TestWSFilterToBinlog_SingleSidedTime(t *testing.T) {
	// 只设 TimeStart：End 必须钳制为远未来哨兵，否则引擎全拒
	got, err := wsFilterToBinlog(ws.ScanFilter{TimeStart: "2020-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.NotNil(t, got.TimeRange)
	require.True(t, got.TimeRange.Start.Equal(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.Equal(t, timeRangeFarFuture, got.TimeRange.End,
		"缺 End 时 End 必须钳制为远未来哨兵（无上界）")

	// 只设 TimeEnd：Start 保持 zero（无下界）
	got, err = wsFilterToBinlog(ws.ScanFilter{TimeEnd: "2030-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.NotNil(t, got.TimeRange)
	require.True(t, got.TimeRange.Start.IsZero(), "缺 Start 时 Start 保持 zero（无下界）")
	require.True(t, got.TimeRange.End.Equal(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)))

	// 两侧都缺 → nil（保持不变）
	got, err = wsFilterToBinlog(ws.ScanFilter{})
	require.NoError(t, err)
	require.Nil(t, got.TimeRange)

	// 两侧都有 → 原样
	got, err = wsFilterToBinlog(ws.ScanFilter{
		TimeStart: "2020-01-01T00:00:00Z",
		TimeEnd:   "2030-01-01T00:00:00Z",
	})
	require.NoError(t, err)
	require.NotNil(t, got.TimeRange)
	require.True(t, got.TimeRange.Start.Equal(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.True(t, got.TimeRange.End.Equal(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)))
}

// TestWSFilterToBinlog_Errors 验证非法输入返回错误。
func TestWSFilterToBinlog_Errors(t *testing.T) {
	_, err := wsFilterToBinlog(ws.ScanFilter{TimeStart: "not-a-time"})
	require.Error(t, err)

	_, err = wsFilterToBinlog(ws.ScanFilter{TimeEnd: "2026/08/01"})
	require.Error(t, err)

	_, err = wsFilterToBinlog(ws.ScanFilter{GTIDSet: "garbage-gtid"})
	require.Error(t, err)
}
