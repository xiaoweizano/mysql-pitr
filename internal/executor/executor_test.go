package executor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/reverse"
)

// fakeDB 记录所有执行的 SQL，可注入错误
type fakeDB struct {
	mu         sync.Mutex
	executed   []string
	failOn     map[int]error // statement index → error
	failCommit bool
	failBegin  bool
	closed     bool
	commits    int
	rollbacks  int
	// blockExec 非 nil 时，每条 Exec 在记录 SQL 后阻塞直到通道关闭（用于确定性的取消测试）。
	blockExec chan struct{}
	// rowsOn：全局语句下标 → RowsAffected 返回值；缺省 1（保持旧行为）。
	rowsOn map[int]int64
	// rowsErrOn：命中的语句 RowsAffected 返回 error（driver 不支持），行数按 0 计。
	rowsErrOn map[int]bool
	// blockFromIdx：blockExec 非 nil 时从该全局语句下标起阻塞（0 = 全部阻塞，旧行为）。
	blockFromIdx int
}

type fakeResult struct {
	rows    int64
	errRows bool
}

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) {
	if r.errRows {
		return 0, fmt.Errorf("RowsAffected not supported (injected)")
	}
	return r.rows, nil
}

type fakeTx struct {
	db         *fakeDB
	committed  bool
	rolledBack bool
}

func (t *fakeTx) Exec(query string, args ...interface{}) (Result, error) {
	t.db.mu.Lock()
	idx := len(t.db.executed)
	t.db.executed = append(t.db.executed, query)
	err := t.db.failOn[idx]
	t.db.mu.Unlock()
	if t.db.blockExec != nil && idx >= t.db.blockFromIdx {
		<-t.db.blockExec
	}
	if err != nil {
		return nil, err
	}
	if t.db.rowsErrOn[idx] {
		return fakeResult{errRows: true}, nil
	}
	rows := int64(1)
	if r, ok := t.db.rowsOn[idx]; ok {
		rows = r
	}
	return fakeResult{rows: rows}, nil
}
func (t *fakeTx) Commit() error {
	if t.db.failCommit {
		return fmt.Errorf("commit failed (injected)")
	}
	t.committed = true
	t.db.mu.Lock()
	t.db.commits++
	t.db.mu.Unlock()
	return nil
}
func (t *fakeTx) Rollback() error {
	t.rolledBack = true
	t.db.mu.Lock()
	t.db.rollbacks++
	t.db.mu.Unlock()
	return nil
}

func (db *fakeDB) Exec(query string, args ...interface{}) (Result, error) {
	return fakeResult{}, nil
}
func (db *fakeDB) Begin() (Tx, error) {
	if db.failBegin {
		return nil, fmt.Errorf("begin failed (injected)")
	}
	return &fakeTx{db: db}, nil
}
func (db *fakeDB) Close() error {
	db.closed = true
	return nil
}

func newFakeFactory(db *fakeDB) DBConnFactory {
	return func(plan Plan) (DB, error) { return db, nil }
}

func makeStmt(sql string, order int) reverse.Statement {
	return reverse.Statement{SQL: sql, TxID: "tx-1", TxOrder: order}
}

func TestExecutor_Run_HappyPath(t *testing.T) {
	db := &fakeDB{}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-1",
		Statements: []reverse.Statement{
			makeStmt("INSERT 1", 0),
			makeStmt("INSERT 2", 1),
		},
		BatchSize: 1,
	}

	var lastProgress Progress
	report, err := ex.Run(context.Background(), plan, func(p Progress) {
		lastProgress = p
	})

	require.NoError(t, err)
	assert.Equal(t, 2, report.Done)
	assert.Equal(t, 2, report.Total)
	assert.False(t, report.Paused)
	assert.Equal(t, 2, lastProgress.Done)
	assert.Len(t, db.executed, 2) // 通过 Tx 执行；db.Exec 不被调用

	// 检查点应记 LastCompletedStatement=2
	cp, _ := store.Load("op-1")
	assert.Equal(t, 2, cp.LastCompletedStatement)
}

func TestExecutor_Run_CancelRollsBackCurrentBatch(t *testing.T) {
	// 让第一条 SQL 执行后取消。用 blockExec 阻塞首条 Exec，使取消确定地发生在
	// 第一批尚未提交时（比 time.AfterFunc 延迟取消稳定）。
	db := &fakeDB{blockExec: make(chan struct{})}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-cancel",
		Statements: []reverse.Statement{
			makeStmt("SQL 1", 0),
			makeStmt("SQL 2", 1),
			makeStmt("SQL 3", 2),
			makeStmt("SQL 4", 3),
		},
		BatchSize: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var report FinalReport
	var runErr error
	runDone := make(chan struct{})
	go func() {
		report, runErr = ex.Run(ctx, plan, nil)
		close(runDone)
	}()

	// 等第一条 SQL 进入批次（batch 已开始，尚未提交）
	require.Eventually(t, func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		return len(db.executed) == 1
	}, time.Second, time.Millisecond)

	cancel()
	close(db.blockExec) // 放行阻塞的 Exec；下一条 SQL 前的 ctx 检查会中止批次
	<-runDone

	require.NoError(t, runErr) // paused 不算 error
	assert.True(t, report.Paused)
	// 0 个完成（第一批还没提交就被取消）
	assert.Equal(t, 0, report.Done)
	// 当前批次被回滚，未提交
	assert.Equal(t, 1, db.rollbacks)
	assert.Equal(t, 0, db.commits)
	// db.Close() 是 deferred 调用的
	assert.True(t, db.closed)
	// 检查点停留在启动时的 0，没有推进
	cp, err := store.Load("op-cancel")
	require.NoError(t, err)
	assert.Equal(t, 0, cp.LastCompletedStatement)
}

func TestExecutor_Run_CommitFailureFailsOp(t *testing.T) {
	db := &fakeDB{failCommit: true}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-commit-fail",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit")
}

func TestExecutor_Run_InvalidPlan(t *testing.T) {
	ex := NewExecutor(newFakeFactory(&fakeDB{}), NewInMemoryCheckpointStore())
	_, err := ex.Run(context.Background(), Plan{}, nil)
	require.Error(t, err)
}

// failingStore 在第 failOnSave 次 Save 调用时失败（1-based；0 = 从不失败）。
// Run 的第 1 次 Save 是 init checkpoint，之后的 Save 是每个批次结束后。
type failingStore struct {
	failOnSave int
	saves      int
}

func (s *failingStore) Load(operationID string) (*Checkpoint, error) {
	return nil, fmt.Errorf("injected load failure")
}
func (s *failingStore) Save(c Checkpoint) error {
	s.saves++
	if s.failOnSave > 0 && s.saves == s.failOnSave {
		return fmt.Errorf("injected save failure")
	}
	return nil
}
func (s *failingStore) Clear(operationID string) error { return nil }

func TestExecutor_Run_AlreadyCanceledCtxPauses(t *testing.T) {
	// ctx 在 Run 开始前已取消：外层循环的批次边界检查立即返回 Paused。
	db := &fakeDB{}
	ex := NewExecutor(newFakeFactory(db), NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-cancel-pre",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := ex.Run(ctx, plan, nil)
	require.NoError(t, err)
	assert.True(t, report.Paused)
	assert.Equal(t, 0, report.Done)
	assert.Len(t, db.executed, 0) // 没有语句被执行
}

func TestExecutor_Run_FactoryError(t *testing.T) {
	ex := NewExecutor(func(plan Plan) (DB, error) {
		return nil, fmt.Errorf("open failed (injected)")
	}, NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-factory",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open db")
}

func TestExecutor_Run_BeginError(t *testing.T) {
	db := &fakeDB{failBegin: true}
	ex := NewExecutor(newFakeFactory(db), NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-begin",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

func TestExecutor_Run_InitCheckpointError(t *testing.T) {
	ex := NewExecutor(newFakeFactory(&fakeDB{}), &failingStore{failOnSave: 1})
	plan := Plan{
		OperationID: "op-init",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "init checkpoint")
}

func TestExecutor_Run_BatchCheckpointError(t *testing.T) {
	ex := NewExecutor(newFakeFactory(&fakeDB{}), &failingStore{failOnSave: 2})
	plan := Plan{
		OperationID: "op-save",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	report, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "save checkpoint")
	assert.Equal(t, 1, report.Done) // 批次已提交，仅检查点保存失败
}

func TestExecutor_Run_NilCallback(t *testing.T) {
	db := &fakeDB{}
	ex := NewExecutor(newFakeFactory(db), NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-nocb",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Done)
	assert.True(t, db.closed)
}

func TestExecutor_Run_BatchSizeDefault(t *testing.T) {
	// BatchSize 0 → DefaultBatchSize(50)：60 条语句应拆成 50 + 10 两批。
	db := &fakeDB{}
	ex := NewExecutor(newFakeFactory(db), NewInMemoryCheckpointStore())
	stmts := make([]reverse.Statement, 60)
	for i := range stmts {
		stmts[i] = makeStmt(fmt.Sprintf("SQL %d", i), i)
	}
	plan := Plan{OperationID: "op-default", Statements: stmts}
	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 60, report.Done)
	assert.Equal(t, 60, len(db.executed))
	assert.Equal(t, 2, db.commits) // 证明默认批量是 50 而非 60
}

func TestExecutor_Run_EmptySQLSkipped(t *testing.T) {
	db := &fakeDB{}
	ex := NewExecutor(newFakeFactory(db), NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-empty",
		Statements: []reverse.Statement{
			makeStmt("", 0),
			makeStmt("SQL A", 1),
			makeStmt("", 2),
		},
		BatchSize: 3,
	}
	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, report.Done)
	require.Len(t, db.executed, 1) // 空 SQL 跳过，不执行
	assert.Equal(t, "SQL A", db.executed[0])
}

func TestExecutor_Run_StatementErrorContinues(t *testing.T) {
	// 单条语句失败：记录 ExecError，批次继续执行并提交。
	db := &fakeDB{failOn: map[int]error{1: fmt.Errorf("boom (injected)")}}
	ex := NewExecutor(newFakeFactory(db), NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-failon",
		Statements: []reverse.Statement{
			makeStmt("SQL 1", 0),
			makeStmt("SQL 2", 1),
			makeStmt("SQL 3", 2),
		},
		BatchSize: 3,
	}
	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err) // 单条失败不中止执行
	assert.Equal(t, 3, report.Done)
	require.Len(t, report.Errors, 1)
	assert.Equal(t, 1, report.Errors[0].Statement)
	assert.Equal(t, "SQL 2", report.Errors[0].SQL)
	assert.Contains(t, report.Errors[0].Err, "boom")
	assert.Equal(t, 1, db.commits) // 批次仍提交
}

func TestExecutor_Run_BatchSizeInvalid(t *testing.T) {
	ex := NewExecutor(newFakeFactory(&fakeDB{}), NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-neg",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   -1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BatchSize")
}

func TestExecutor_Run_NilFactory(t *testing.T) {
	ex := NewExecutor(nil, NewInMemoryCheckpointStore())
	plan := Plan{
		OperationID: "op-nilfac",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "factory")
}

func TestExecutor_Run_NilStore(t *testing.T) {
	ex := NewExecutor(newFakeFactory(&fakeDB{}), nil)
	plan := Plan{
		OperationID: "op-nilstore",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Run(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store")
}

func TestResume_ContinuesFromCheckpoint(t *testing.T) {
	// 100 条语句、BatchSize 10：先 Run 到第 20 条完成后取消（回调在第二批提交后
	// 触发 cancel，检查点停在 LastCompletedStatement=20），再 Resume 同 Plan——
	// 只执行剩余 80 条（索引 20..99，无重复），Done=Total=100。
	db := &fakeDB{}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	stmts := make([]reverse.Statement, 100)
	for i := range stmts {
		stmts[i] = makeStmt(fmt.Sprintf("SQL %d", i), i)
	}
	plan := Plan{OperationID: "op-resume", Statements: stmts, BatchSize: 10}

	// 阶段 1：Run 到第 2 批（20 条）提交后取消 → paused，检查点停在 20。
	ctx, cancel := context.WithCancel(context.Background())
	report, err := ex.Run(ctx, plan, func(p Progress) {
		if p.Done == 20 {
			cancel()
		}
	})
	require.NoError(t, err)
	assert.True(t, report.Paused)
	assert.Equal(t, 20, report.Done)
	cp, err := store.Load("op-resume")
	require.NoError(t, err)
	assert.Equal(t, 20, cp.LastCompletedStatement)
	assert.Equal(t, 20, len(db.executed))

	// 阶段 2：清空执行记录，Resume 同 Plan → 只执行剩余语句。
	db.mu.Lock()
	db.executed = nil
	db.mu.Unlock()

	report, err = ex.Resume(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 100, report.Done)
	assert.Equal(t, 100, report.Total)
	assert.False(t, report.Paused)

	// 恰好 80 条，从 "SQL 20" 开始（无跳过、无重复）。
	require.Len(t, db.executed, 80)
	assert.Equal(t, "SQL 20", db.executed[0])
	assert.Equal(t, "SQL 99", db.executed[len(db.executed)-1])

	cp, err = store.Load("op-resume")
	require.NoError(t, err)
	assert.Equal(t, 100, cp.LastCompletedStatement)
}

func TestResume_NoCheckpoint_FullRun(t *testing.T) {
	// 无检查点（store 空）→ 从 0 全跑（等价 Run，但不预写 init 检查点）。
	db := &fakeDB{}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-nocp",
		Statements: []reverse.Statement{
			makeStmt("SQL 1", 0),
			makeStmt("SQL 2", 1),
			makeStmt("SQL 3", 2),
		},
		BatchSize: 2,
	}
	report, err := ex.Resume(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, report.Done)
	assert.Equal(t, 3, report.Total)
	assert.False(t, report.Paused)
	require.Len(t, db.executed, 3)
	cp, err := store.Load("op-nocp")
	require.NoError(t, err)
	assert.Equal(t, 3, cp.LastCompletedStatement)
}

func TestResume_ErrorsSurfaced(t *testing.T) {
	// 检查点带 1 条既有错误；Resume 续跑中途再失败 1 条 → FinalReport.Errors
	// 汇总两条（续跑前检查点里的 + 续跑中新产生的）。
	//
	// fakeDB.failOn 按全局执行序（executed 列表下标）键控：Resume 从 plan 索引 2
	// 开始，executed[0] 对应 plan 索引 2、executed[1] 对应 plan 索引 3，因此
	// failOn[1] 使 plan 索引 3 的语句失败。
	db := &fakeDB{failOn: map[int]error{1: fmt.Errorf("boom (injected)")}}
	store := NewInMemoryCheckpointStore()
	require.NoError(t, store.Save(Checkpoint{
		OperationID:            "op-errs",
		LastCompletedStatement: 2,
		Total:                  4,
		Errors: []ExecError{
			{Statement: 1, SQL: "SQL 1", Err: "earlier failure"},
		},
	}))
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-errs",
		Statements: []reverse.Statement{
			makeStmt("SQL 1", 0),
			makeStmt("SQL 2", 1),
			makeStmt("SQL 3", 2),
			makeStmt("SQL 4", 3),
		},
		BatchSize: 4,
	}
	report, err := ex.Resume(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, report.Done)
	assert.False(t, report.Paused)
	require.Len(t, report.Errors, 2)
	assert.Equal(t, 1, report.Errors[0].Statement)
	assert.Equal(t, "earlier failure", report.Errors[0].Err)
	assert.Equal(t, 3, report.Errors[1].Statement)
	assert.Contains(t, report.Errors[1].Err, "boom")
	require.Len(t, db.executed, 2) // 只执行了索引 2、3 两条
	assert.Equal(t, "SQL 3", db.executed[0])
	assert.Equal(t, "SQL 4", db.executed[1])
}

func TestExecutor_Resume_LoadError(t *testing.T) {
	ex := NewExecutor(newFakeFactory(&fakeDB{}), &failingStore{})
	plan := Plan{
		OperationID: "op-resume",
		Statements:  []reverse.Statement{makeStmt("SQL 1", 0)},
		BatchSize:   1,
	}
	_, err := ex.Resume(context.Background(), plan, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load checkpoint")
}

func TestLastTxIDLastSQLBounds(t *testing.T) {
	// 覆盖 lastTxID/lastSQL 的越界守卫（正常流程中 idx 恒为合法值）。
	plan := Plan{Statements: []reverse.Statement{makeStmt("SQL 1", 0)}}
	assert.Equal(t, "tx-1", lastTxID(plan, 0))
	assert.Equal(t, "", lastTxID(plan, -1))
	assert.Equal(t, "", lastTxID(plan, 5))
	assert.Equal(t, "SQL 1", lastSQL(plan, 0))
	assert.Equal(t, "", lastSQL(plan, -1))
	assert.Equal(t, "", lastSQL(plan, 5))
}

func TestExecutor_Run_AccumulatesRowsAffected(t *testing.T) {
	db := &fakeDB{rowsOn: map[int]int64{0: 2, 1: 3, 2: 0}}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-rows",
		Statements: []reverse.Statement{
			makeStmt("S1", 0), makeStmt("S2", 1), makeStmt("S3", 2),
		},
		BatchSize: 2,
	}

	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, report.Done)
	assert.Equal(t, int64(5), report.RowsAffected, "2+3+0 成功语句行数累计")

	cp, err := store.Load("op-rows")
	require.NoError(t, err)
	assert.Equal(t, int64(5), cp.RowsAffected, "检查点携带累计行数")
}

func TestExecutor_Run_RowsAffectedErrorCountsZero(t *testing.T) {
	// driver 不支持 RowsAffected（返回 error）：该语句按 0 计，不影响执行。
	db := &fakeDB{rowsErrOn: map[int]bool{0: true}}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-rows-err",
		Statements: []reverse.Statement{
			makeStmt("S1", 0), makeStmt("S2", 1),
		},
		BatchSize: 10,
	}

	report, err := ex.Run(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), report.RowsAffected, "语句 0 行数错误按 0 计，语句 1 计 1")
}

func TestExecutor_Resume_AccumulatesRowsAcrossCheckpoint(t *testing.T) {
	// 检查点携带 RowsAffected=4（断点前 2 条语句）；Resume 续跑 2 条 → 累计 6。
	db := &fakeDB{}
	store := NewInMemoryCheckpointStore()
	require.NoError(t, store.Save(Checkpoint{
		OperationID: "op-resume", LastCompletedStatement: 2, Total: 4, RowsAffected: 4,
	}))
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-resume",
		Statements: []reverse.Statement{
			makeStmt("S1", 0), makeStmt("S2", 1), makeStmt("S3", 2), makeStmt("S4", 3),
		},
		BatchSize: 10,
	}

	report, err := ex.Resume(context.Background(), plan, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, report.Done)
	assert.Equal(t, int64(6), report.RowsAffected, "4（检查点携带）+ 2（续跑语句）")
}

func TestExecutor_Run_PausedReportCarriesRows(t *testing.T) {
	// BatchSize=2、4 条语句；第 3 条（idx 2，第二批内）阻塞。第一批提交后
	// （rows=2）取消 ctx → 当前批次回滚（其行数不计），paused 报告携带 2。
	db := &fakeDB{blockExec: make(chan struct{}), blockFromIdx: 2}
	store := NewInMemoryCheckpointStore()
	ex := NewExecutor(newFakeFactory(db), store)

	plan := Plan{
		OperationID: "op-pause-rows",
		Statements: []reverse.Statement{
			makeStmt("SQL 1", 0), makeStmt("SQL 2", 1), makeStmt("SQL 3", 2), makeStmt("SQL 4", 3),
		},
		BatchSize: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var report FinalReport
	var runErr error
	runDone := make(chan struct{})
	go func() {
		report, runErr = ex.Run(ctx, plan, nil)
		close(runDone)
	}()

	require.Eventually(t, func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		return len(db.executed) == 3
	}, time.Second, time.Millisecond)

	cancel()
	close(db.blockExec)
	<-runDone

	require.NoError(t, runErr)
	assert.True(t, report.Paused)
	assert.Equal(t, 2, report.Done)
	assert.Equal(t, int64(2), report.RowsAffected, "已提交批次的行数；回滚批次不计")
}
