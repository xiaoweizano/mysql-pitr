// Package daemon 实现 agent 的命令处理逻辑层：scan / execute / resume / cancel /
// archive_status。命令 handler 不依赖 WS 连接本身——所有流式输出经 EventSink
// 抽象推送（生产 = server 包装的 ws client Send，测试 = fake），因此本包可脱离
// 连接层独立单测。
package daemon

import (
	"context"
	"log/slog"
	"sync"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// EventSink 抽象向 server 推送流事件（生产 = ws client Send，测试 = fake）。
type EventSink interface {
	Send(ev ws.StreamEvent) error
}

// noopSink 是 nil sink 的兜底（丢弃所有事件），保证 NewDaemon 语义不因注入方
// 遗漏 sink 而 panic。
type noopSink struct{}

func (noopSink) Send(ws.StreamEvent) error { return nil }

// ScanDeps 是 scan 依赖（可注入）。
type ScanDeps struct {
	ArchiveDir    string
	SchemaFetcher binlog.SchemaFetcher
	MaxRowsPerTx  int
	Logger        *slog.Logger
}

// Daemon 持有全部命令 handler 的共享依赖与运行中 op 的注册表。
//
// ops 注册表以命令 ID（= 命令的 ws Cmd 标识，如 UUID）为键登记 cancel 函数：
//   - scan / execute / resume 启动时 registerOp，goroutine 结束时 unregisterOp；
//   - CancelScan / CancelOp 通过 cancel 中断对应的 ctx。
type Daemon struct {
	scanDeps ScanDeps
	exec     executor.Executor
	stateFn  func() collector.State
	sink     EventSink

	mu  sync.Mutex
	ops map[string]context.CancelFunc
}

// NewDaemon 构造 Daemon。exec / stateFn / sink 均可为 nil：
//   - exec 为 nil 时 Execute/Resume 返回错误；
//   - stateFn 为 nil 时 ArchiveStatus 返回零值 State；
//   - sink 为 nil 时事件被丢弃（测试注入 fakeSink）。
func NewDaemon(scanDeps ScanDeps, exec executor.Executor, stateFn func() collector.State, sink EventSink) *Daemon {
	if sink == nil {
		sink = noopSink{}
	}
	return &Daemon{
		scanDeps: scanDeps,
		exec:     exec,
		stateFn:  stateFn,
		sink:     sink,
		ops:      make(map[string]context.CancelFunc),
	}
}

// registerOp 登记运行中的 op；unregisterOp 在 goroutine 结束时移除。
func (d *Daemon) registerOp(id string, cancel context.CancelFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ops[id] = cancel
}

func (d *Daemon) unregisterOp(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.ops, id)
}

// cancelOp 取消运行中的 op（scan 或 execute 通用）。未找到返回错误。
func (d *Daemon) cancelOp(id string) error {
	d.mu.Lock()
	cancel, ok := d.ops[id]
	d.mu.Unlock()
	if !ok {
		return errNoSuchOp(id)
	}
	cancel()
	return nil
}

// CancelScan 取消一次运行中的扫描。
func (d *Daemon) CancelScan(id string) error { return d.cancelOp(id) }

// CancelOp 取消一次运行中的执行（或任意已登记的 op）。
func (d *Daemon) CancelOp(id string) error { return d.cancelOp(id) }

// ArchiveStatus 返回归档循环的当前状态（来自注入的 stateFn；nil 时为零值）。
func (d *Daemon) ArchiveStatus() collector.State {
	if d.stateFn != nil {
		return d.stateFn()
	}
	return collector.State{}
}
