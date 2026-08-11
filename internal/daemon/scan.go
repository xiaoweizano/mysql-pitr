package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/scan"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// errNoSuchOp 是 CancelScan/CancelOp 遇到未知 op ID 时的错误。
func errNoSuchOp(id string) error {
	return fmt.Errorf("daemon: no running op %q", id)
}

// Scan 启动一次异步扫描。请求经 wsFilterToBinlog 转换后按 Mode 选 scan.Mode，
// 在 goroutine 里跑 scan.Stream，事务元数据/逆向 SQL 作为流事件经 sink 推送；
// 扫描结束推 scan_done，出错推 op_error。返回 opID（=命令 ID）供 CancelScan 取消。
//
// 事件序列（正常）：tx_meta[, sql]* → scan_done；出错：tx_meta[, sql]* → op_error。
// 取消：现有缓冲结果送达后以 op_error（context canceled）终止，绝不推 scan_done。
func (d *Daemon) Scan(ctx context.Context, id string, req ws.ScanRequest) error {
	filter, err := wsFilterToBinlog(req.Filter)
	if err != nil {
		return err
	}
	mode, err := scanMode(req.Mode)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	d.registerOp(id, cancel)

	go func() {
		defer d.unregisterOp(id)
		defer cancel()
		cfg := scan.Config{
			ArchiveDir:    d.scanDeps.ArchiveDir,
			Filter:        filter,
			Mode:          mode,
			MaxPreview:    req.MaxPreview,
			SchemaFetcher: d.scanDeps.SchemaFetcher,
			MaxRowsPerTx:  d.scanDeps.MaxRowsPerTx,
			Logger:        d.scanDeps.Logger,
		}
		ch, errCh := scan.Stream(ctx, cfg)
		for r := range ch {
			if data, err := json.Marshal(r.Meta); err == nil {
				d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvTxMeta, Data: data})
			}
			if len(r.SQL) > 0 {
				if data, err := json.Marshal(r.SQL); err == nil {
					d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvSQL, Data: data})
				}
			}
		}
		if err := <-errCh; err != nil {
			if data, merr := json.Marshal(err.Error()); merr == nil {
				d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvOpError, Data: data})
			}
			return
		}
		d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvScanDone, Data: json.RawMessage("{}")})
	}()
	return nil
}

// scanMode 把 wire 模式名映射到 scan.Mode。空串 = meta。
func scanMode(m string) (scan.Mode, error) {
	switch m {
	case "", "meta":
		return scan.ModeMetaOnly, nil
	case "sql":
		return scan.ModeWithSQL, nil
	case "selected":
		return scan.ModeSelectedSQL, nil
	default:
		return 0, fmt.Errorf("daemon: unknown scan mode %q", m)
	}
}

// wsFilterToBinlog 把线格式的 ScanFilter 转成 binlog.Filter：
//   - Tables 直传；
//   - TimeStart/TimeEnd 按 RFC3339 解析（缺一侧则只设该侧边界）；
//   - GTIDSet 用 binlog.ParseGTIDSet("mysql", ...) 解析；
//   - StartFile/StartPos、EndFile/EndPos 组 mysql.Position；
//   - MaxRowsPerTx、SelectedTxIDs 直传。
//
// BinlogDir 不在此设置：scan.Stream 会用 ArchiveDir 覆盖。
func wsFilterToBinlog(f ws.ScanFilter) (binlog.Filter, error) {
	var out binlog.Filter
	for _, t := range f.Tables {
		out.Tables = append(out.Tables, binlog.TableRef{Schema: t.Schema, Table: t.Table})
	}
	if f.TimeStart != "" || f.TimeEnd != "" {
		tr := &binlog.TimeRange{}
		if f.TimeStart != "" {
			t, err := time.Parse(time.RFC3339, f.TimeStart)
			if err != nil {
				return out, fmt.Errorf("daemon: parse timeStart %q: %w", f.TimeStart, err)
			}
			tr.Start = t
		}
		if f.TimeEnd != "" {
			t, err := time.Parse(time.RFC3339, f.TimeEnd)
			if err != nil {
				return out, fmt.Errorf("daemon: parse timeEnd %q: %w", f.TimeEnd, err)
			}
			tr.End = t
		}
		out.TimeRange = tr
	}
	if f.GTIDSet != "" {
		gs, err := binlog.ParseGTIDSet("mysql", f.GTIDSet)
		if err != nil {
			return out, err
		}
		out.GTIDSet = gs
	}
	if f.StartFile != "" {
		out.StartPos = mysql.Position{Name: f.StartFile, Pos: f.StartPos}
	}
	if f.EndFile != "" {
		out.EndPos = mysql.Position{Name: f.EndFile, Pos: f.EndPos}
	}
	out.MaxRowsPerTx = f.MaxRowsPerTx
	out.SelectedTxIDs = f.SelectedTxIDs
	return out, nil
}
