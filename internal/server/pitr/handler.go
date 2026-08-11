package pitr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/a-shan/mysql-pitr/internal/server/agent"
	"github.com/a-shan/mysql-pitr/internal/server/audit"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/ws"
	"github.com/a-shan/mysql-pitr/internal/ws/hub"
)

// AgentCommander is the subset of the agent hub the PITR flow needs. The
// production implementation is *hub.Hub; tests provide a fake.
type AgentCommander interface {
	IsConnected(agentID string) bool
	SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error)
	SetProgressHandler(fn hub.ProgressHandler)
}

// Handler serves PITR workflow HTTP endpoints.
//
// NOTE (Task 4 placeholder): the v2 operation model (preflight/confirmed/
// parsing/previewed/executing states, per-operation parse/exec results, the
// InMemory store) was replaced by the v3 model (created/scanning/ready/
// executing/paused/done/failed/cancelled/blocked states, binlog.Filter,
// SQLite store). The v2 background pipeline (runOperation/executeOperation)
// no longer exists. This handler keeps the HTTP surface and the
// model-independent checks (auth, org membership, agent state) working, and
// creates/updates operations through the new store; the v3 flow
// (scanning -> ready -> executing with SSE) is implemented in Task 6.
type Handler struct {
	opStore    OperationStore
	agentStore agent.AgentStore
	orgStore   org.OrgStore
	auditStore audit.AuditStore
	jwtSecret  []byte

	// commander is the agent command channel used to route operation phases
	// to the selected agent. May be nil in tests / minimal setups;
	// operations are rejected when nil.
	commander AgentCommander
}

// NewHandler creates a PITR Handler. commander may be nil for setups without
// an agent channel.
func NewHandler(
	opStore OperationStore,
	agentStore agent.AgentStore,
	orgStore org.OrgStore,
	auditStore audit.AuditStore,
	jwtSecret []byte,
	commander AgentCommander,
) *Handler {
	return &Handler{
		opStore:    opStore,
		agentStore: agentStore,
		orgStore:   orgStore,
		auditStore: auditStore,
		jwtSecret:  jwtSecret,
		commander:  commander,
	}
}

// ---------- request / response types ----------

type startRequest struct {
	AgentID      string   `json:"agent_id"`
	TargetTable  string   `json:"target_table"`
	RecoveryTime string   `json:"recovery_time"`
	Mode         string   `json:"mode"` // "preview" or "execute"
	BinlogFiles  []string `json:"binlog_files,omitempty"`
	StartTime    string   `json:"start_time,omitempty"`
	StartPos     *uint32  `json:"start_pos,omitempty"`
	StopPos      *uint32  `json:"stop_pos,omitempty"`
}

type startResponse struct {
	OperationID string         `json:"operationId"`
	Status      OperationState `json:"status"`
}

type statusResponse struct {
	ID        string         `json:"id"`
	OrgID     string         `json:"orgId"`
	AgentID   string         `json:"agentId"`
	Type      string         `json:"type"`
	Mode      string         `json:"mode"`
	Status    OperationState `json:"status"`
	CreatedBy string         `json:"createdBy"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

type cancelResponse struct {
	OperationID string         `json:"operationId"`
	Status      OperationState `json:"status"`
}

// ---------- helpers ----------

func userIDFromRequest(r *http.Request) string {
	claims := auth.ClaimsFromContext(r.Context())
	if claims == nil {
		return ""
	}
	return claims.UserID
}

func emailFromRequest(r *http.Request) string {
	claims := auth.ClaimsFromContext(r.Context())
	if claims == nil {
		return ""
	}
	return claims.Email
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// ---------- handlers ----------

// List returns all PITR operations for an organisation, optionally filtered
// by time range.
//
// GET /api/pitr?org_id=X&from=ISO&to=ISO
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	orgID := r.URL.Query().Get("org_id")
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "org_id query parameter is required")
		return
	}

	// Verify org membership.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	if !orgMemberContains(members, userID) {
		writeError(w, http.StatusForbidden, "not a member of this organisation")
		return
	}

	ops, err := h.opStore.ListByOrg(orgID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	var filtered []operationView
	for _, op := range ops {
		if fromStr != "" {
			from, err := time.Parse(time.RFC3339, fromStr)
			if err == nil && op.CreatedAt.Before(from) {
				continue
			}
		}
		if toStr != "" {
			to, err := time.Parse(time.RFC3339, toStr)
			if err == nil && op.CreatedAt.After(to) {
				continue
			}
		}
		filtered = append(filtered, operationView{Operation: op, AgentConnected: h.agentConnected(op.AgentID)})
	}
	if filtered == nil {
		filtered = []operationView{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"operations": filtered})
}

// operationView decorates an operation with live agent-connection state for
// the list API.
type operationView struct {
	*Operation
	AgentConnected bool `json:"agentConnected"`
}

// Start initiates a new PITR recovery operation against an agent.
//
// Placeholder (Task 4): creates and persists the v3 operation record
// (Status: created) after the same auth/agent/org checks as before. The v3
// pipeline that advances it (created -> scanning -> ready -> executing, with
// SSE progress) is implemented in Task 6.
//
// POST /api/pitr/start
func (h *Handler) Start(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.AgentID == "" || req.TargetTable == "" || req.RecoveryTime == "" || req.Mode == "" {
		writeError(w, http.StatusBadRequest,
			"agent_id, target_table, recovery_time, and mode are required")
		return
	}
	if req.Mode != "preview" && req.Mode != "execute" {
		writeError(w, http.StatusBadRequest, "mode must be \"preview\" or \"execute\"")
		return
	}

	if _, err := time.Parse(time.RFC3339, req.RecoveryTime); err != nil {
		writeError(w, http.StatusBadRequest,
			"invalid recovery_time: expected ISO8601/RFC3339 format")
		return
	}

	// Fetch the agent to determine the organisation.
	agt, err := h.agentStore.Get(req.AgentID)
	if err != nil {
		writeError(w, http.StatusNotFound, "agent not found")
		return
	}

	// Verify the requesting user is a member of the agent's org.
	members, err := h.orgStore.ListMembers(agt.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	if !orgMemberContains(members, userID) {
		writeError(w, http.StatusForbidden,
			"not a member of the agent's organisation")
		return
	}

	// The agent must be approved and connected — operations run entirely
	// through the agent's command channel.
	if !agt.Approved {
		writeError(w, http.StatusConflict, "agent is not approved")
		return
	}
	if !h.agentConnected(req.AgentID) {
		writeError(w, http.StatusConflict, "agent is offline — start it with `mysql-pitr-agent serve` first")
		return
	}

	// TODO(Task 6): build the v3 Filter from the request and launch the
	// pipeline. For now the record is created with the default filter and
	// stays in `created`.
	op := &Operation{
		OrgID:     agt.OrgID,
		AgentID:   req.AgentID,
		Type:      "pitr",
		Mode:      req.Mode,
		Status:    StateCreated,
		CreatedBy: userID,
	}

	if err := h.opStore.Create(op); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, startResponse{
		OperationID: op.ID,
		Status:      op.Status,
	})
}

// Status returns the current state and metadata for an operation.
//
// GET /api/pitr/{id}/status
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	opID := chi.URLParam(r, "id")
	if opID == "" {
		writeError(w, http.StatusBadRequest, "missing operation id")
		return
	}

	op, err := h.opStore.Get(opID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Verify org membership.
	members, err := h.orgStore.ListMembers(op.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	if !orgMemberContains(members, userID) {
		writeError(w, http.StatusForbidden,
			"not a member of this organisation")
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{
		ID:        op.ID,
		OrgID:     op.OrgID,
		AgentID:   op.AgentID,
		Type:      op.Type,
		Mode:      op.Mode,
		Status:    op.Status,
		CreatedBy: op.CreatedBy,
		CreatedAt: op.CreatedAt,
		UpdatedAt: op.UpdatedAt,
	})
}

// Cancel cancels an operation that is in a cancellable state (scanning, ready,
// or paused under the v3 state machine).
//
// POST /api/pitr/{id}/cancel
func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	opID := chi.URLParam(r, "id")
	if opID == "" {
		writeError(w, http.StatusBadRequest, "missing operation id")
		return
	}

	op, err := h.opStore.Get(opID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Verify org membership.
	members, err := h.orgStore.ListMembers(op.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	if !orgMemberContains(members, userID) {
		writeError(w, http.StatusForbidden,
			"not a member of this organisation")
		return
	}

	if !TransitionValid(op.Status, StateCancelled) {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("cannot cancel operation in state %q", op.Status))
		return
	}

	op.Status = StateCancelled
	op.UpdatedAt = time.Now()
	if err := h.opStore.Update(op); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Record audit entry.
	_ = h.auditStore.Append(&audit.AuditEntry{
		OperationID: op.ID,
		Operator:    emailFromRequest(r),
		Timestamp:   time.Now(),
		OrgID:       op.OrgID,
		AgentID:     op.AgentID,
		Status:      string(StateCancelled),
	})

	writeJSON(w, http.StatusOK, cancelResponse{
		OperationID: op.ID,
		Status:      op.Status,
	})
}

// Preview returns the parsed/selected statements of a ready operation.
//
// Placeholder (Task 4): not implemented — the v3 preview flow (scanning ->
// ready, statements persisted via SaveStatements) lands in Task 6.
//
// GET /api/pitr/{id}/preview
func (h *Handler) Preview(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "preview: not implemented (v3 flow lands in Task 6)")
}

// Progress returns the current execution progress of an operation.
//
// Placeholder (Task 4): not implemented — the v3 executing/progress flow with
// SSE lands in Task 6.
//
// GET /api/pitr/{id}/progress
func (h *Handler) Progress(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "progress: not implemented (v3 flow lands in Task 6)")
}

// Execute runs the selected statements of a ready operation.
//
// Placeholder (Task 4): not implemented — the v3 executing flow lands in
// Task 6.
//
// POST /api/pitr/{id}/execute
func (h *Handler) Execute(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "execute: not implemented (v3 flow lands in Task 6)")
}

// ---------- helpers ----------

// agentConnected reports whether the operation's agent currently has a live
// hub connection.
func (h *Handler) agentConnected(agentID string) bool {
	return h.commander != nil && h.commander.IsConnected(agentID)
}

func orgMemberContains(members []org.Member, userID string) bool {
	for _, m := range members {
		if m.UserID == userID {
			return true
		}
	}
	return false
}
