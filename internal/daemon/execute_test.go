package daemon_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/daemon"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// fakeExecutor 记录最后一次 Plan，按预设回调进度，返回预设报告。Run 与 Resume
// 分别计数，供测试断言 daemon 把 execute 路由到 Run、resume 路由到 Resume。
type fakeExecutor struct {
	mu          sync.Mutex
	plan        executor.Plan
	runCalls    int
	resumeCalls int
	progresses  []executor.Progress
	report      executor.FinalReport
	runErr      error
	// blockRun 非 nil 时 Run/Resume 阻塞直到通道关闭（用于确定性的 op 生命周期测试）。
	blockRun chan struct{}
}

func (f *fakeExecutor) Run(ctx context.Context, plan executor.Plan, cb executor.ProgressCallback) (executor.FinalReport, error) {
	f.mu.Lock()
	f.plan = plan
	f.runCalls++
	f.mu.Unlock()
	if f.blockRun != nil {
		<-f.blockRun
	}
	for _, p := range f.progresses {
		cb(p)
	}
	return f.report, f.runErr
}

func (f *fakeExecutor) Resume(ctx context.Context, plan executor.Plan, cb executor.ProgressCallback) (executor.FinalReport, error) {
	f.mu.Lock()
	f.plan = plan
	f.resumeCalls++
	f.mu.Unlock()
	if f.blockRun != nil {
		<-f.blockRun
	}
	for _, p := range f.progresses {
		cb(p)
	}
	return f.report, f.runErr
}

func (f *fakeExecutor) getPlan() executor.Plan {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.plan
}

func (f *fakeExecutor) getRunCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runCalls
}

func (f *fakeExecutor) getResumeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resumeCalls
}

// TestExecute_StreamsProgressAndOpDone 验证 Execute：转 Plan → goroutine Run →
// EvProgress（按 executor 回调）→ 结束 EvOpDone；事件 ID 均为命令 ID。
func TestExecute_StreamsProgressAndOpDone(t *testing.T) {
	ctx := context.Background()
	sink := &fakeSink{}
	fex := &fakeExecutor{
		progresses: []executor.Progress{
			{Done: 1, Total: 2, LastTxID: "xid-19", LastSQL: "DELETE FROM shop.orders WHERE id=1"},
			{Done: 2, Total: 2, LastTxID: "xid-20", LastSQL: "DELETE FROM shop.orders WHERE id=2"},
		},
		report: executor.FinalReport{Done: 2, Total: 2},
	}
	d := daemon.NewDaemon(daemon.ScanDeps{}, fex, nil, sink)

	err := d.Execute(ctx, "exec-1", ws.ExecuteRequest{
		OperationID: "op-2026-08-10-001",
		Statements: []ws.StatementWire{
			{SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "xid-19", TxOrder: 0},
			{SQL: "DELETE FROM shop.orders WHERE id=2", TxID: "xid-20", TxOrder: 1},
		},
		BatchSize: 2,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.hasKind(ws.EvOpDone) },
		5*time.Second, 10*time.Millisecond, "op_done should arrive")

	evs := sink.eventsCopy()
	require.Equal(t, ws.EvOpDone, evs[len(evs)-1].Kind, "op_done 是最后一个事件")

	var progs []executor.Progress
	for _, ev := range evs {
		require.Equal(t, "exec-1", ev.ID, "所有事件共享命令 ID")
		if ev.Kind == ws.EvProgress {
			var p executor.Progress
			require.NoError(t, json.Unmarshal(ev.Data, &p))
			progs = append(progs, p)
		}
	}
	require.Len(t, progs, 2, "两条进度事件")
	require.Equal(t, 2, progs[len(progs)-1].Done)
	require.Equal(t, 2, progs[len(progs)-1].Total)

	// Plan 转换验证
	plan := fex.getPlan()
	require.Equal(t, "op-2026-08-10-001", plan.OperationID)
	require.Len(t, plan.Statements, 2)
	require.Equal(t, "DELETE FROM shop.orders WHERE id=1", plan.Statements[0].SQL)
	require.Equal(t, "xid-19", plan.Statements[0].TxID)
	require.Equal(t, 1, plan.Statements[1].TxOrder)
	require.Equal(t, 2, plan.BatchSize)
	require.Equal(t, 1, fex.getRunCalls())
}

// TestExecute_ReportsRunError 验证 Execute 执行出错时推送 EvOpError（且无 op_done）。
func TestExecute_ReportsRunError(t *testing.T) {
	ctx := context.Background()
	sink := &fakeSink{}
	fex := &fakeExecutor{runErr: errors.New("connection refused")}
	d := daemon.NewDaemon(daemon.ScanDeps{}, fex, nil, sink)

	require.NoError(t, d.Execute(ctx, "exec-err", ws.ExecuteRequest{OperationID: "op-bad"}))
	require.Eventually(t, func() bool { return sink.hasKind(ws.EvOpError) },
		5*time.Second, 10*time.Millisecond)
	require.False(t, sink.hasKind(ws.EvOpDone), "执行出错不报 op_done")
}

// TestExecute_RejectsUnsafeOperationID 安全要求（T3 评审 carry-in）：operationID
// 来自网络侧不可信——必须拒绝空值、含路径分隔符、含 ".." 的注入（防检查点文件
// 写/删出目录）。
func TestExecute_RejectsUnsafeOperationID(t *testing.T) {
	ctx := context.Background()
	sink := &fakeSink{}
	d := daemon.NewDaemon(daemon.ScanDeps{}, &fakeExecutor{}, nil, sink)

	for _, bad := range []string{"", "../evil", "a/b", "a\\b", "..", "foo..bar", "dir/.."} {
		err := d.Execute(ctx, "exec-x", ws.ExecuteRequest{OperationID: bad})
		require.Error(t, err, "operationID %q 必须被拒绝", bad)
	}
	require.Empty(t, sink.eventsCopy(), "被拒绝的 op 不产生任何事件")
}

// TestResume_ContinuesViaExecutorResume 验证 Resume：校验 operationID 后把 Plan
// 交给 executor.Resume（检查点续跑），而不是 Run（从零重跑）。
func TestResume_ContinuesViaExecutorResume(t *testing.T) {
	ctx := context.Background()
	sink := &fakeSink{}
	fex := &fakeExecutor{report: executor.FinalReport{Done: 1, Total: 1}}
	d := daemon.NewDaemon(daemon.ScanDeps{}, fex, nil, sink)

	require.NoError(t, d.Resume(ctx, "resume-1", ws.ExecuteRequest{
		OperationID: "op-abc",
		Statements:  []ws.StatementWire{{SQL: "DELETE FROM shop.orders WHERE id=1", TxID: "xid-19"}},
	}))
	require.Eventually(t, func() bool { return sink.hasKind(ws.EvOpDone) },
		5*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, fex.getResumeCalls(), "Resume 触发一次 executor.Resume")
	require.Equal(t, 0, fex.getRunCalls(), "Resume 不得调用 executor.Run")
	require.Equal(t, "op-abc", fex.getPlan().OperationID)
	require.Len(t, fex.getPlan().Statements, 1)
	require.Equal(t, "DELETE FROM shop.orders WHERE id=1", fex.getPlan().Statements[0].SQL)

	// Resume 同样拒绝不安全 operationID
	err := d.Resume(ctx, "resume-x", ws.ExecuteRequest{OperationID: "../evil"})
	require.Error(t, err)
}

// TestCancelOp 验证 CancelOp：不存在的 op 报错；运行中的 op 可取消（fakeExecutor
// 阻塞期间 op 仍在注册表，CancelOp 必须成功）。
func TestCancelOp(t *testing.T) {
	ctx := context.Background()
	sink := &fakeSink{}
	fex := &fakeExecutor{blockRun: make(chan struct{})}
	d := daemon.NewDaemon(daemon.ScanDeps{}, fex, nil, sink)

	require.Error(t, d.CancelOp("exec-none"), "不存在的 op 取消应报错")

	require.NoError(t, d.Execute(ctx, "exec-1", ws.ExecuteRequest{OperationID: "op-x"}))
	require.Eventually(t, func() bool { return fex.getRunCalls() == 1 },
		5*time.Second, 10*time.Millisecond, "Run 应已启动（op 注册中）")
	require.NoError(t, d.CancelOp("exec-1"), "运行中的 op 可取消")

	close(fex.blockRun)
	require.Eventually(t, func() bool { return sink.hasKind(ws.EvOpDone) },
		5*time.Second, 10*time.Millisecond)
}
