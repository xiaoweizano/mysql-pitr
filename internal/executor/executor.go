package executor

import (
	"context"
	"fmt"
)

type executor struct {
	factory DBConnFactory
	store   CheckpointStore
}

func NewExecutor(factory DBConnFactory, store CheckpointStore) Executor {
	return &executor{factory: factory, store: store}
}

func (e *executor) Run(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error) {
	if plan.OperationID == "" {
		return FinalReport{}, fmt.Errorf("executor: plan.OperationID required")
	}
	if plan.BatchSize == 0 {
		plan.BatchSize = DefaultBatchSize
	}
	if plan.BatchSize < 1 {
		return FinalReport{}, fmt.Errorf("executor: BatchSize must be >= 1")
	}
	if e.factory == nil {
		return FinalReport{}, fmt.Errorf("executor: factory is nil")
	}
	if e.store == nil {
		return FinalReport{}, fmt.Errorf("executor: store is nil")
	}

	// 启动时清掉旧检查点（避免 Resume 误用）
	_ = e.store.Clear(plan.OperationID)
	if err := e.store.Save(Checkpoint{
		OperationID:            plan.OperationID,
		LastCompletedStatement: 0,
		Total:                  len(plan.Statements),
	}); err != nil {
		return FinalReport{}, fmt.Errorf("executor: init checkpoint: %w", err)
	}

	return e.runFromIndex(ctx, plan, 0, cb)
}

func (e *executor) Resume(ctx context.Context, operationID string, cb ProgressCallback) (FinalReport, error) {
	cp, err := e.store.Load(operationID)
	if err != nil {
		return FinalReport{}, fmt.Errorf("executor: load checkpoint: %w", err)
	}
	// Resume 需要 plan；通过 store 扩展保存 plan，或通过参数传入。
	// 简化：要求 Resume 之前 Run 已注册 plan；这里返回错误。
	// 实际生产：Plan 持久化由 server 层（SQLite）保存；agent 通过 WS 协议拿到 plan 后再 Resume。
	_ = cp
	return FinalReport{}, fmt.Errorf("executor: Resume requires plan; use Run-after-Load pattern (server-layer responsibility)")
}

// runFromIndex 从 plan.Statements[startIdx] 开始执行。
func (e *executor) runFromIndex(ctx context.Context, plan Plan, startIdx int, cb ProgressCallback) (FinalReport, error) {
	db, err := e.factory(plan)
	if err != nil {
		return FinalReport{}, fmt.Errorf("executor: open db: %w", err)
	}
	defer db.Close()

	var errs []ExecError
	completed := startIdx
	total := len(plan.Statements)

	for completed < total {
		// 检查 ctx 取消
		if err := ctx.Err(); err != nil {
			return e.pausedReport(plan, completed, errs), nil
		}

		batchEnd := completed + plan.BatchSize
		if batchEnd > total {
			batchEnd = total
		}

		// 开启事务
		tx, err := db.Begin()
		if err != nil {
			return FinalReport{
				Done: completed, Total: total, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: begin tx: %w", err)
		}

		var batchErrs []ExecError
		aborted := false
		for i := completed; i < batchEnd; i++ {
			// 每条 SQL 前检查取消
			if err := ctx.Err(); err != nil {
				aborted = true
				break
			}
			stmt := plan.Statements[i]
			if stmt.SQL == "" {
				continue
			}
			if _, err := tx.Exec(stmt.SQL); err != nil {
				batchErrs = append(batchErrs, ExecError{
					Statement: i, SQL: stmt.SQL, Err: err.Error(),
				})
			}
		}

		if aborted {
			// 当前批次回滚，已完成的批次保留
			_ = tx.Rollback()
			return e.pausedReport(plan, completed, errs), nil
		}

		// 提交
		if err := tx.Commit(); err != nil {
			// 整批回滚（tx.Commit 失败时 driver 通常已回滚）
			_ = tx.Rollback()
			return FinalReport{
				Done: completed, Total: total, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: commit batch [%d,%d): %w", completed, batchEnd, err)
		}

		completed = batchEnd
		errs = append(errs, batchErrs...)

		// 写检查点
		if err := e.store.Save(Checkpoint{
			OperationID:            plan.OperationID,
			LastCompletedStatement: completed,
			Total:                  total,
			Errors:                 errs,
		}); err != nil {
			return FinalReport{
				Done: completed, Total: total, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: save checkpoint: %w", err)
		}

		if cb != nil {
			cb(Progress{
				Done: completed, Total: total,
				LastTxID: lastTxID(plan, completed-1),
				LastSQL:  lastSQL(plan, completed-1),
				Errors:   errs,
			})
		}
	}

	return FinalReport{Done: completed, Total: total, Errors: errs, Paused: false}, nil
}

func (e *executor) pausedReport(plan Plan, completed int, errs []ExecError) FinalReport {
	return FinalReport{
		Done:   completed,
		Total:  len(plan.Statements),
		Errors: errs,
		Paused: true,
	}
}

func lastTxID(plan Plan, idx int) string {
	if idx < 0 || idx >= len(plan.Statements) {
		return ""
	}
	return plan.Statements[idx].TxID
}

func lastSQL(plan Plan, idx int) string {
	if idx < 0 || idx >= len(plan.Statements) {
		return ""
	}
	return plan.Statements[idx].SQL
}
