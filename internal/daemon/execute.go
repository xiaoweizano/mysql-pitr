package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/reverse"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// validateOperationID 校验网络侧不可信的 operationID（T3 评审 carry-in 安全要求）：
//   - 非空；
//   - filepath.Base(opID) == opID（拒绝含路径分隔符的输入，如 "../evil"、"a/b"）；
//   - 不含 ".."（兜底拒绝 Windows 的 "..\\evil" 等变体）。
//
// 目的：operationID 会作为 FileCheckpointStore 的文件名（<dir>/<opID>.json），
// 未校验的路径注入可让检查点写/删出目录。
func validateOperationID(opID string) error {
	if opID == "" {
		return fmt.Errorf("daemon: operationID is required")
	}
	if filepath.Base(opID) != opID {
		return fmt.Errorf("daemon: unsafe operationID %q: must not contain path separators", opID)
	}
	if strings.Contains(opID, "..") {
		return fmt.Errorf("daemon: unsafe operationID %q: must not contain \"..\"", opID)
	}
	return nil
}

// Execute 启动一次异步执行。请求转 executor.Plan 后在 goroutine 里 Run：
// 每条 SQL 的进度经 callback 推 EvProgress，结束推 EvOpDone（含 FinalReport，
// Paused=true 表示被取消暂停），出错推 EvOpError。
//
// 事件序列：progress[, progress]* → op_done | op_error。
func (d *Daemon) Execute(ctx context.Context, id string, req ws.ExecuteRequest) error {
	if err := validateOperationID(req.OperationID); err != nil {
		return err
	}
	return d.runExecute(ctx, id, req, false)
}

// Resume 重发 Plan 并检查点续跑：把 Plan 交给 executor.Resume（载入
// operationID 对应的检查点，从断点继续；无检查点则从 0 全跑）。与 Execute 的
// 区别仅在于调用的 executor 方法——检查点写入时机、进度/结果事件语义完全一致。
func (d *Daemon) Resume(ctx context.Context, id string, req ws.ExecuteRequest) error {
	if err := validateOperationID(req.OperationID); err != nil {
		return err
	}
	return d.runExecute(ctx, id, req, true)
}

// runExecute 是 Execute/Resume 的公共实现：转 Plan → 注册 op → goroutine
// Run/Resume → 进度/结果事件推送。resume=true 时走 executor.Resume（检查点续跑），
// 否则 executor.Run。Plan.DSN 在 Phase 3 由 server 层注入连接配置后填充。
func (d *Daemon) runExecute(ctx context.Context, id string, req ws.ExecuteRequest, resume bool) error {
	if d.exec == nil {
		return fmt.Errorf("daemon: executor not configured")
	}
	plan := planFromExecuteRequest(req)

	ctx, cancel := context.WithCancel(ctx)
	d.registerOp(id, cancel)

	run := d.exec.Run
	if resume {
		run = d.exec.Resume
	}

	go func() {
		defer d.unregisterOp(id)
		defer cancel()
		report, err := run(ctx, plan, func(p executor.Progress) {
			if data, merr := json.Marshal(p); merr == nil {
				d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvProgress, Data: data})
			}
		})
		if err != nil {
			if data, merr := json.Marshal(err.Error()); merr == nil {
				d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvOpError, Data: data})
			}
			return
		}
		data, merr := json.Marshal(report)
		if merr != nil {
			return
		}
		d.sink.Send(ws.StreamEvent{ID: id, Kind: ws.EvOpDone, Data: data})
	}()
	return nil
}

// planFromExecuteRequest 把线格式的 ExecuteRequest 转 executor.Plan。StatementWire
// 与 reverse.Statement 字段一一对应（SQL/TxID/TxOrder/Warnings；SourceRow 为空）。
func planFromExecuteRequest(req ws.ExecuteRequest) executor.Plan {
	stmts := make([]reverse.Statement, 0, len(req.Statements))
	for _, s := range req.Statements {
		stmts = append(stmts, reverse.Statement{
			SQL:      s.SQL,
			TxID:     s.TxID,
			TxOrder:  s.TxOrder,
			Warnings: s.Warnings,
		})
	}
	return executor.Plan{
		OperationID: req.OperationID,
		Statements:  stmts,
		BatchSize:   req.BatchSize,
	}
}
