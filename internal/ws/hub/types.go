package hub

import (
	"time"

	"github.com/a-shan/mysql-pitr/internal/ws"
)

// AgentInfo describes a connected agent and is used for status reporting and
// observability.
type AgentInfo struct {
	ID           string    `json:"id"`
	ConnectedAt  time.Time `json:"connectedAt"`
	LastSeen     time.Time `json:"lastSeen"`
	MySQLVersion string    `json:"mySQLVersion,omitempty"`
	Status       string    `json:"status"` // "online" or "offline"
}

// ProgressHandler receives pitr_progress notifications pushed by agents.
// It is invoked asynchronously from the hub read loop, so it must be safe for
// concurrent use.
type ProgressHandler func(agentID string, cmd ws.Command)

// StreamEventHandler receives agent→server stream_event envelopes (scan
// tx/SQL, execution progress, operation completion). It is invoked
// asynchronously from the hub read loop, so it must be safe for concurrent
// use.
type StreamEventHandler func(agentID string, cmd ws.Command)

// LifecycleHooks are invoked when an agent connects to or disconnects from the
// hub. They run in the hub's connection goroutines; implementations must be
// quick and safe for concurrent invocation.
type LifecycleHooks struct {
	OnConnect    func(agentID string)
	OnDisconnect func(agentID string)
}
