package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/a-shan/mysql-pitr/internal/ws"
	gorilla "github.com/gorilla/websocket"
)

const (
	// readDeadline matches the agent's pongWait (90s): the agent heartbeats
	// with a ping every 30s (client.go pingInterval), so 90s gives three
	// missed heartbeats before we declare the connection dead.
	readDeadline = 90 * time.Second
	writeTimeout = 10 * time.Second
)

// CSRHandler is the interface the hub requires for inline certificate renewal.
// Typically implemented by ca.CA.
type CSRHandler interface {
	SignCSR(csrPEM []byte, agentID string) ([]byte, error)
	IsRevoked(certSerial string) bool
}

// agentConn holds state for a single connected agent.
type agentConn struct {
	conn        *gorilla.Conn
	agentID     string
	connectedAt time.Time
	lastSeen    time.Time

	// mu protects conn writes (gorilla requires serialised writes).
	mu sync.Mutex

	// pending maps command IDs to response channels for in-flight commands
	// initiated by SendToAgent.
	pending   map[string]chan *ws.Response
	pendingMu sync.RWMutex
}

// Hub manages agent WebSocket connections. Agents authenticate via mTLS with
// a client certificate whose CommonName is used as the agent identifier.
type Hub struct {
	// conns maps agentID → *agentConn.
	conns sync.Map

	closed    chan struct{}
	closeOnce sync.Once

	// csrHandler is optional; if set, the hub handles cert_renewal commands
	// inline by delegating to this handler.
	csrHandler CSRHandler

	// progressHandler receives pitr_progress notifications from agents.
	progressHandler ProgressHandler

	// streamHandler receives agent→server stream_event envelopes (scan
	// tx/SQL, execution progress, operation completion).
	streamHandler StreamEventHandler

	// lifecycle hooks for agent connect/disconnect events.
	hooks LifecycleHooks
}

// NewHub creates a Hub. The caCertFile parameter is reserved for loading the
// platform CA certificate for revocation checks; it may be empty if the CA is
// injected later via SetCSRHandler.
func NewHub(caCertFile string) *Hub {
	return &Hub{
		closed: make(chan struct{}),
	}
}

// SetCSRHandler attaches a CSRHandler (typically a *ca.CA) for inline
// certificate renewal processing.
func (h *Hub) SetCSRHandler(csr CSRHandler) {
	h.csrHandler = csr
}

// SetProgressHandler registers a handler for agent-pushed pitr_progress
// notifications.
func (h *Hub) SetProgressHandler(fn ProgressHandler) {
	h.progressHandler = fn
}

// SetStreamEventHandler registers a handler for agent-pushed stream_event
// envelopes (scan tx/SQL, execution progress, operation completion).
func (h *Hub) SetStreamEventHandler(fn StreamEventHandler) {
	h.streamHandler = fn
}

// SetLifecycleHooks registers connect/disconnect callbacks.
func (h *Hub) SetLifecycleHooks(hooks LifecycleHooks) {
	h.hooks = hooks
}

// IsConnected reports whether the given agent currently has a live connection.
func (h *Hub) IsConnected(agentID string) bool {
	_, ok := h.conns.Load(agentID)
	return ok
}

// HandleConnection registers a WebSocket connection that has already been
// upgraded by the caller. It extracts the agent identifier from the mTLS
// client certificate's CommonName, rejects duplicate connections, checks
// revocation, and starts a read-pump goroutine.
func (h *Hub) HandleConnection(conn *gorilla.Conn, r *http.Request) {
	agentID := extractAgentID(r)
	if agentID == "" {
		log.Printf("hub: connection rejected — missing client certificate CN")
		_ = conn.Close()
		return
	}

	// Check revocation when a CSRHandler is available.
	if h.csrHandler != nil && r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		serial := r.TLS.PeerCertificates[0].SerialNumber.Text(16)
		if h.csrHandler.IsRevoked(serial) {
			log.Printf("hub: connection rejected — agent %s certificate revoked (serial %s)", agentID, serial)
			_ = conn.Close()
			return
		}
	}

	ac := &agentConn{
		conn:        conn,
		agentID:     agentID,
		connectedAt: time.Now(),
		lastSeen:    time.Now(),
		pending:     make(map[string]chan *ws.Response),
	}

	if _, loaded := h.conns.LoadOrStore(agentID, ac); loaded {
		log.Printf("hub: connection rejected — duplicate agent %s", agentID)
		_ = conn.Close()
		return
	}

	log.Printf("hub: agent %s connected", agentID)

	// The agent heartbeats by sending a ping every 30s; it never sends pong
	// frames itself. Refresh our read deadline on each ping and reply with a
	// pong. The default gorilla PingHandler would reply automatically but
	// would not refresh the deadline, which previously killed every
	// connection at readDeadline.
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		return conn.WriteControl(gorilla.PongMessage, []byte(appData), time.Now().Add(writeTimeout))
	})

	if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
		h.conns.Delete(agentID)
		_ = conn.Close()
		return
	}

	if h.hooks.OnConnect != nil {
		h.hooks.OnConnect(agentID)
	}

	go h.readPump(ac)
}

// readPump is the sole goroutine that reads from the agent's WebSocket
// connection. It routes responses to pending SendToAgent callers and handles
// cert_renewal commands inline.
func (h *Hub) readPump(ac *agentConn) {
	conn := ac.conn
	agentID := ac.agentID

	defer func() {
		h.conns.Delete(agentID)
		_ = conn.Close()

		// Drain pending command channels so blocked SendToAgent callers
		// observe the disconnection instead of hanging forever.
		ac.pendingMu.Lock()
		for id, ch := range ac.pending {
			close(ch)
			delete(ac.pending, id)
		}
		ac.pendingMu.Unlock()

		if h.hooks.OnDisconnect != nil {
			h.hooks.OnDisconnect(agentID)
		}
		log.Printf("hub: agent %s disconnected", agentID)
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		ac.mu.Lock()
		ac.lastSeen = time.Now()
		ac.mu.Unlock()

		// Try response first — this is the common case for agent responses
		// to commands sent via SendToAgent.
		var resp ws.Response
		if err := json.Unmarshal(message, &resp); err == nil && resp.Cmd != "" && resp.Status != "" {
			ac.pendingMu.RLock()
			ch, ok := ac.pending[resp.Cmd]
			ac.pendingMu.RUnlock()
			if ok {
				select {
				case ch <- &resp:
				default:
				}
				continue
			}
		}

		// Check for cert_renewal command.
		var cmd ws.Command
		if err := json.Unmarshal(message, &cmd); err == nil && cmd.Type == ws.CmdCertRenewal {
			h.handleCertRenewal(ac, cmd)
			continue
		}

		// Route agent-pushed progress notifications to the registered handler.
		if err := json.Unmarshal(message, &cmd); err == nil && cmd.Type == ws.CmdPITRProgress {
			if h.progressHandler != nil {
				go h.progressHandler(agentID, cmd)
			}
			continue
		}

		// Route agent-pushed stream events (scan tx/SQL, execution progress,
		// operation completion) to the registered handler.
		if err := json.Unmarshal(message, &cmd); err == nil && cmd.Type == ws.CmdStreamEvent {
			if h.streamHandler != nil {
				go h.streamHandler(agentID, cmd)
			}
			continue
		}

		// Unhandled message — in production this would route to a message bus.
		log.Printf("hub: unhandled message from agent %s", agentID)
	}
}

// handleCertRenewal processes a CSR from the agent and sends back the signed
// certificate as a response.
func (h *Hub) handleCertRenewal(ac *agentConn, cmd ws.Command) {
	if h.csrHandler == nil {
		h.sendResponse(ac, ws.Response{
			Cmd:    cmd.Cmd,
			Status: ws.StatusError,
			Error:  "certificate renewal not supported",
		})
		return
	}

	csrPEM, ok := cmd.Params["csr"].(string)
	if !ok {
		h.sendResponse(ac, ws.Response{
			Cmd:    cmd.Cmd,
			Status: ws.StatusError,
			Error:  "missing 'csr' parameter",
		})
		return
	}

	signedCert, err := h.csrHandler.SignCSR([]byte(csrPEM), ac.agentID)
	if err != nil {
		h.sendResponse(ac, ws.Response{
			Cmd:    cmd.Cmd,
			Status: ws.StatusError,
			Error:  fmt.Sprintf("sign CSR: %v", err),
		})
		return
	}

	h.sendResponse(ac, ws.Response{
		Cmd:    cmd.Cmd,
		Status: ws.StatusOK,
		Result: map[string]string{
			"certificate": string(signedCert),
		},
	})
}

// sendResponse writes a JSON response to the agent's WebSocket connection.
func (h *Hub) sendResponse(ac *agentConn, resp ws.Response) {
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()
	_ = ac.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = ac.conn.WriteMessage(gorilla.TextMessage, data)
}

// extractAgentID reads the CommonName from the mTLS client certificate
// presented during the TLS handshake.
func extractAgentID(r *http.Request) string {
	if r.TLS == nil {
		return ""
	}
	if len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	return r.TLS.PeerCertificates[0].Subject.CommonName
}

// SendToAgent sends a command to a connected agent and waits for the matching
// response. Returns an error if the agent is not connected.
func (h *Hub) SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error) {
	v, ok := h.conns.Load(agentID)
	if !ok {
		return nil, fmt.Errorf("hub: agent %s not connected", agentID)
	}
	ac := v.(*agentConn)

	// Register a pending channel for this command ID.
	ch := make(chan *ws.Response, 1)
	ac.pendingMu.Lock()
	ac.pending[cmd.Cmd] = ch
	ac.pendingMu.Unlock()

	defer func() {
		ac.pendingMu.Lock()
		delete(ac.pending, cmd.Cmd)
		ac.pendingMu.Unlock()
	}()

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("hub: marshal command: %w", err)
	}

	ac.mu.Lock()
	if err := ac.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		ac.mu.Unlock()
		return nil, fmt.Errorf("hub: set write deadline: %w", err)
	}
	if err := ac.conn.WriteMessage(gorilla.TextMessage, data); err != nil {
		ac.mu.Unlock()
		h.conns.Delete(agentID)
		return nil, fmt.Errorf("hub: write to agent %s: %w", agentID, err)
	}
	ac.mu.Unlock()

	select {
	case resp := <-ch:
		if resp == nil {
			// Channel closed by the read pump: the agent disconnected while
			// the command was in flight.
			return nil, fmt.Errorf("hub: agent %s disconnected while awaiting response", agentID)
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.closed:
		return nil, fmt.Errorf("hub: closed")
	}
}

// GetConnectedAgents returns a snapshot of all currently connected agents.
func (h *Hub) GetConnectedAgents() []AgentInfo {
	var agents []AgentInfo
	h.conns.Range(func(key, value interface{}) bool {
		ac := value.(*agentConn)
		ac.mu.Lock()
		info := AgentInfo{
			ID:          ac.agentID,
			ConnectedAt: ac.connectedAt,
			LastSeen:    ac.lastSeen,
			Status:      "online",
		}
		ac.mu.Unlock()
		agents = append(agents, info)
		return true
	})
	if agents == nil {
		return []AgentInfo{}
	}
	return agents
}

// Close gracefully shuts down the hub, closing all agent connections.
func (h *Hub) Close() error {
	var firstErr error
	h.closeOnce.Do(func() {
		close(h.closed)
		h.conns.Range(func(key, value interface{}) bool {
			ac := value.(*agentConn)
			ac.mu.Lock()
			if err := ac.conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			ac.mu.Unlock()
			return true
		})
	})
	return firstErr
}

// compile-time interface check.
var _ CSRHandler = (*mockCSRHandler)(nil)

// mockCSRHandler is used internally for testing, defined here so the
// interface compliance is verifiable.
type mockCSRHandler struct {
	signCSRFn   func(csrPEM []byte, agentID string) ([]byte, error)
	isRevokedFn func(certSerial string) bool
}

func (m *mockCSRHandler) SignCSR(csrPEM []byte, agentID string) ([]byte, error) {
	return m.signCSRFn(csrPEM, agentID)
}

func (m *mockCSRHandler) IsRevoked(certSerial string) bool {
	if m.isRevokedFn != nil {
		return m.isRevokedFn(certSerial)
	}
	return false
}
