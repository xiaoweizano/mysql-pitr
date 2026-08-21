package executor

import (
	"context"
	"errors"

	"github.com/a-shan/mysql-pitr/internal/reverse"
)

// ExecError 是单条 SQL 执行失败时的错误记录。
type ExecError struct {
	Statement int    `json:"statement"`     // Plan.Statements 内的 index
	SQL       string `json:"sql,omitempty"` // 失败的 SQL（statement 序号即可定位，SQL 为冗余信息）
	Err       string `json:"err"`           // 错误信息
}

// Plan 描述一次执行。
type Plan struct {
	OperationID string
	Statements  []reverse.Statement
	DSN         string
	BatchSize   int // 0 = 默认 50
}

// Progress 是 callback 上报的进度快照。
type Progress struct {
	Done     int         `json:"done"`
	Total    int         `json:"total"`
	LastTxID string      `json:"lastTxId,omitempty"`
	LastSQL  string      `json:"lastSql,omitempty"`
	Errors   []ExecError `json:"errors,omitempty"`
}

// FinalReport 是 Run/Resume 的返回值。
type FinalReport struct {
	Done         int         `json:"done"`
	Total        int         `json:"total"`
	RowsAffected int64       `json:"rowsAffected"` // 成功提交语句的累计影响行数
	Errors       []ExecError `json:"errors,omitempty"`
	Paused       bool        `json:"paused"` // true = ctx 取消导致暂停；false = 正常完成或失败
}

// ProgressCallback 由调用方提供，每条 SQL 执行后调用。
type ProgressCallback func(p Progress)

// Checkpoint 持久化的执行进度。
type Checkpoint struct {
	OperationID            string
	LastCompletedStatement int
	Total                  int
	RowsAffected           int64
	Errors                 []ExecError
}

// CheckpointStore 抽象检查点存储。生产用 SQLite（后续 phase），测试用 InMemory。
type CheckpointStore interface {
	Load(operationID string) (*Checkpoint, error)
	Save(c Checkpoint) error
	Clear(operationID string) error
}

// ErrCheckpointNotFound 是 Load 在指定 operationID 没有检查点时的哨兵错误。
// Resume 用它区分"从未跑过（从 0 全跑）"与"检查点损坏/加载失败（报错）"。
var ErrCheckpointNotFound = errors.New("executor: checkpoint not found")

// Executor 是执行器接口。
type Executor interface {
	Run(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error)
	// Resume 载入 plan.OperationID 的检查点并从断点续跑：
	//   - 有检查点 → 从 LastCompletedStatement（已完成的语句数）继续；
	//   - 无检查点（ErrCheckpointNotFound）→ 从 0 全跑（等价 Run 但不清检查点）。
	Resume(ctx context.Context, plan Plan, cb ProgressCallback) (FinalReport, error)
}

// DefaultBatchSize 是 Plan.BatchSize=0 时使用的默认值。
const DefaultBatchSize = 50

// DB 是执行器对数据库连接的抽象。
// 真实实现由 connector 包提供；测试用 sqlmock。
type DB interface {
	Exec(query string, args ...interface{}) (Result, error)
	Begin() (Tx, error)
	Close() error
}

type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

type Tx interface {
	Exec(query string, args ...interface{}) (Result, error)
	Commit() error
	Rollback() error
}

// DBConnFactory 在 Plan 给定时创建一个 DB 连接。
// 用于在生产里用 Plan.DSN 创建连接；测试里返回 mock。
type DBConnFactory func(plan Plan) (DB, error)
