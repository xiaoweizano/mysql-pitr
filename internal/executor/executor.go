package executor

import (
	"context"
	"errors"
	"fmt"
)

type executor struct {
	factory DBConnFactory
	store   CheckpointStore
}

func NewExecutor(factory DBConnFactory, store CheckpointStore) Executor {
	return &executor{factory: factory, store: store}
}

// normalizePlan 校验并补齐 Plan 的默认值。Run/Resume 共用的前置校验。
func (e *executor) normalizePlan(plan Plan) (Plan, error) {
	if plan.OperationID == "" {
		return plan, fmt.Errorf("executor: plan.OperationID required")
	}
	if plan.BatchSize == 0 {
		plan.BatchSize = DefaultBatchSize
	}
	if plan.BatchSize < 1 {
		return plan, fmt.Errorf("executor: BatchSize must be >= 1")
	}
	if e.factory == nil {
		return plan, fmt.Errorf("executor: factory is nil")
	}
	if e.store == nil {
		return plan, fmt.Errorf("executor: store is nil")
	}
	return plan, nil
}

func (e *executor) Run(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error) {
	plan, err := e.normalizePlan(plan)
	if err != nil {
		return FinalReport{}, err
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

	return e.runFromIndex(ctx, plan, 0, nil, 0, cb)
}

// Resume 载入检查点并从断点续跑：
//   - 无检查点（ErrCheckpointNotFound）→ 从 0 全跑（等价 Run 语义，但不清检查点、
//     不预写 init 检查点——每批提交后的 Save 会自然建立检查点）；
//   - 有检查点 → 从 LastCompletedStatement（已完成的语句数，即下一语句的下标）续跑。
//
// 与 Run 的唯一区别：不清检查点、起点为检查点推进处。检查点写入时机与 Run 完全
// 一致（每批提交后 Save 含 LastCompletedStatement/Errors）；中途取消时当前批次
// 回滚、检查点停在上一已提交批次，Resume 可在任意中断点重入。
func (e *executor) Resume(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error) {
	plan, err := e.normalizePlan(plan)
	if err != nil {
		return FinalReport{}, err
	}

	cp, err := e.store.Load(plan.OperationID)
	if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
		return FinalReport{}, fmt.Errorf("executor: load checkpoint: %w", err)
	}

	startIdx := 0
	var carriedErrs []ExecError
	var carriedRows int64
	if cp != nil {
		if cp.LastCompletedStatement > 0 {
			startIdx = cp.LastCompletedStatement
		}
		carriedErrs = cp.Errors
		carriedRows = cp.RowsAffected
	}
	if startIdx > len(plan.Statements) {
		// 检查点推进已超出当前 Plan（Plan 变更过）：没有剩余语句，返回检查点口径。
		return FinalReport{
			Done: len(plan.Statements), Total: len(plan.Statements),
			RowsAffected: carriedRows, Errors: carriedErrs, Paused: false,
		}, nil
	}
	return e.runFromIndex(ctx, plan, startIdx, carriedErrs, carriedRows, cb)
}

// runFromIndex 从 plan.Statements[startIdx] 开始执行。initialErrs/initialRows
// 携带断点前的已记录错误与累计行数（Run 传 nil/0；Resume 传检查点值），随最终
// 报告一起返回。行数只累计成功提交的语句：语句失败不计；批次回滚整批不计
// （batchRows 只在 Commit 成功后并入 rows）。
func (e *executor) runFromIndex(ctx context.Context, plan Plan, startIdx int, initialErrs []ExecError, initialRows int64, cb ProgressCallback) (FinalReport, error) {
	db, err := e.factory(plan)
	if err != nil {
		return FinalReport{}, fmt.Errorf("executor: open db: %w", err)
	}
	defer db.Close()

	errs := append([]ExecError(nil), initialErrs...)
	rows := initialRows
	completed := startIdx
	total := len(plan.Statements)

	for completed < total {
		// 检查 ctx 取消
		if err := ctx.Err(); err != nil {
			return e.pausedReport(plan, completed, rows, errs), nil
		}

		batchEnd := completed + plan.BatchSize
		if batchEnd > total {
			batchEnd = total
		}

		// 开启事务
		tx, err := db.Begin()
		if err != nil {
			return FinalReport{
				Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: begin tx: %w", err)
		}

		var batchErrs []ExecError
		var batchRows int64
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
			res, err := tx.Exec(stmt.SQL)
			if err != nil {
				batchErrs = append(batchErrs, ExecError{
					Statement: i, SQL: stmt.SQL, Err: err.Error(),
				})
				continue
			}
			if n, rerr := res.RowsAffected(); rerr == nil {
				batchRows += n
			}
		}

		if aborted {
			// 当前批次回滚，已完成的批次保留（其行数一并丢弃）
			_ = tx.Rollback()
			return e.pausedReport(plan, completed, rows, errs), nil
		}

		// 提交
		if err := tx.Commit(); err != nil {
			// 整批回滚（tx.Commit 失败时 driver 通常已回滚）
			_ = tx.Rollback()
			return FinalReport{
				Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false,
			}, fmt.Errorf("executor: commit batch [%d,%d): %w", completed, batchEnd, err)
		}

		completed = batchEnd
		errs = append(errs, batchErrs...)
		rows += batchRows

		// 写检查点
		if err := e.store.Save(Checkpoint{
			OperationID:            plan.OperationID,
			LastCompletedStatement: completed,
			Total:                  total,
			RowsAffected:           rows,
			Errors:                 errs,
		}); err != nil {
			return FinalReport{
				Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false,
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

	return FinalReport{Done: completed, Total: total, RowsAffected: rows, Errors: errs, Paused: false}, nil
}

func (e *executor) pausedReport(plan Plan, completed int, rows int64, errs []ExecError) FinalReport {
	return FinalReport{
		Done:         completed,
		Total:        len(plan.Statements),
		RowsAffected: rows,
		Errors:       errs,
		Paused:       true,
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
