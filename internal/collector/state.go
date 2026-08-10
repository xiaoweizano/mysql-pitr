// Package collector 实现 agent 的归档循环：reconcile（缺口补齐 + 陈旧
// .partial 清理）→ 初始回填 → binlogsyncer 续拉 → 轮转封口 → 状态持久化 →
// 断线自愈（指数退避重连）。归档产物是 internal/archive 的 Writer 语义：
// 全量重建（magic 开头）或 append 续写（尾部追加到已回填文件）。
//
// 依赖约束：只使用标准库 + go-mysql + internal/{binlog, archive, stream}。
package collector

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// State 是 archive_state.json 的内容：归档循环的续拉断点。
type State struct {
	LastFile  string    `json:"last_file"`           // 最近一次轮转封口后的下一个 binlog 文件名
	LastPos   uint32    `json:"last_pos"`            // 续拉位置（轮转后新文件从头开始 = 0）
	LastGTID  string    `json:"last_gtid,omitempty"` // 预留：GTID 模式断点（当前实现未使用）
	UpdatedAt time.Time `json:"updated_at"`          // 最近一次轮转封口的时刻
}

// stateFileName 是归档目录中的状态文件名。
const stateFileName = "archive_state.json"

func statePath(dir string) string { return filepath.Join(dir, stateFileName) }

// LoadState 读取归档状态。文件不存在返回零值 State（表示「从未运行」），
// 不视为错误；文件存在但内容损坏（JSON 解析失败）才报错——「从未运行」与
// 「损坏」必须区分，前者走初始回填+从 master position 续拉，后者是数据
// 完整性问题，fatal。
func LoadState(dir string) (State, error) {
	data, err := os.ReadFile(statePath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("collector: read state %s: %w", statePath(dir), err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return State{}, fmt.Errorf("collector: parse state %s (corrupt): %w", statePath(dir), err)
	}
	return s, nil
}

// SaveState 原子写状态：先写同目录 .tmp 再 rename（与 checkpoint 包同手法），
// 崩溃/断电不会留下半截的 archive_state.json。
func SaveState(dir string, s State) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("collector: marshal state: %w", err)
	}
	tmp := statePath(dir) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("collector: write state tmp: %w", err)
	}
	if err := os.Rename(tmp, statePath(dir)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("collector: rename state: %w", err)
	}
	return nil
}
