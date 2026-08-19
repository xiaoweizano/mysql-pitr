package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// AgentCommander is the subset of the agent hub the archive endpoint needs:
// it checks connectivity and dispatches commands to a live agent. The
// production implementation is *hub.Hub; tests provide a fake. The interface
// mirrors the pitr package's AgentCommander — it cannot be reused because pitr
// imports agent, so importing pitr here would create an import cycle.
type AgentCommander interface {
	IsConnected(agentID string) bool
	SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error)
}

// Handler serves agent HTTP endpoints.
type Handler struct {
	agentStore AgentStore
	orgStore   org.OrgStore
	jwtSecret  []byte
	// commander is the agent command channel. It may be nil in minimal/dev
	// setups; archive status reports the agent as offline when nil.
	commander AgentCommander
}

// NewHandler creates an agent Handler. commander is the hub channel used to
// query a live agent (archive status); pass nil when the flow has no agent
// connectivity (minimal/dev routers).
func NewHandler(agentStore AgentStore, orgStore org.OrgStore, jwtSecret []byte, commander AgentCommander) *Handler {
	return &Handler{
		agentStore: agentStore,
		orgStore:   orgStore,
		jwtSecret:  jwtSecret,
		commander:  commander,
	}
}

// ---------- request / response types ----------

type registerAgentRequest struct {
	Hostname     string `json:"hostname"`
	MySQLVersion string `json:"mySQLVersion,omitempty"`
}

type registerAgentResponse struct {
	Agent             *AgentRecord `json:"agent"`
	RegistrationToken string       `json:"registrationToken"`
}

type listAgentsResponse struct {
	Agents []*AgentRecord `json:"agents"`
}

type approveAgentResponse struct {
	Agent *AgentRecord `json:"agent"`
}

// ---------- helpers ----------

func userIDFromRequest(r *http.Request) string {
	claims := auth.ClaimsFromContext(r.Context())
	if claims == nil {
		return ""
	}
	return claims.UserID
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

// Register creates a new agent registration. The agent is created in a
// pending state with a registration token that must be presented during the
// WebSocket handshake.
//
// POST /api/agents/register
func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	// Determine which org this agent belongs to. The request can include an
	// explicit orgId in the body, or we use the first org the user belongs to.
	var req struct {
		OrgID        string `json:"orgId,omitempty"`
		Hostname     string `json:"hostname"`
		MySQLVersion string `json:"mySQLVersion,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Hostname == "" {
		writeError(w, http.StatusBadRequest, "hostname is required")
		return
	}

	// We need a way to find a user's orgs. For simplicity, we look up the org
	// membership via ListMembers for every org. This is O(n) but fine for
	// in-memory. In a real DB, you'd query a user_orgs join table.
	// For now, we iterate through all orgs stored and check membership.

	// Actually, the OrgStore doesn't have a "list by user" method. Let me
	// accept an explicit orgId in the body instead. This is a pragmatic design
	// choice for the MVP.

	orgID := req.OrgID
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "orgId is required")
		return
	}

	// Verify the user is a member of the specified org.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	isMember := false
	for _, m := range members {
		if m.UserID == userID {
			isMember = true
			break
		}
	}
	if !isMember {
		writeError(w, http.StatusForbidden, "not a member of this organisation")
		return
	}

	token := generateRegToken()
	agent := &AgentRecord{
		OrgID:             orgID,
		Hostname:          req.Hostname,
		MySQLVersion:      req.MySQLVersion,
		Status:            "offline",
		RegistrationToken: token,
		Approved:          false, // pending — an admin must approve before use
	}

	if err := h.agentStore.Create(agent); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, registerAgentResponse{
		Agent:             agent,
		RegistrationToken: token,
	})
}

// List returns all agents belonging to the caller's organisations.
//
// GET /api/agents
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	// Determine which org to list. Default: the query param ?orgId=, or
	// iterate over all orgs the user belongs to.
	orgID := r.URL.Query().Get("orgId")
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "orgId query parameter is required")
		return
	}

	// Verify membership.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	isMember := false
	for _, m := range members {
		if m.UserID == userID {
			isMember = true
			break
		}
	}
	if !isMember {
		writeError(w, http.StatusForbidden, "not a member of this organisation")
		return
	}

	agents, err := h.agentStore.ListByOrg(orgID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, listAgentsResponse{Agents: agents})
}

// Approve marks an agent as approved. Only organisation admins may approve
// agents.
//
// POST /api/agents/{id}/approve
func (h *Handler) Approve(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	agentID := chi.URLParam(r, "id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "missing agent id")
		return
	}

	agent, err := h.agentStore.Get(agentID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Verify the requester is an admin of the agent's org.
	members, err := h.orgStore.ListMembers(agent.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	isAdmin := false
	for _, m := range members {
		if m.UserID == userID && m.Role == "admin" {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		writeError(w, http.StatusForbidden,
			"only organisation admins can approve agents")
		return
	}

	agent.Approved = true
	agent.CertSerial = generateCertSerial() // placeholder for CA signing

	if err := h.agentStore.Update(agent); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, approveAgentResponse{Agent: agent})
}

// Reject deletes an agent. Only organisation admins may reject agents.
//
// POST /api/agents/{id}/reject
func (h *Handler) Reject(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	agentID := chi.URLParam(r, "id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "missing agent id")
		return
	}

	agent, err := h.agentStore.Get(agentID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Verify the requester is an admin of the agent's org.
	members, err := h.orgStore.ListMembers(agent.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	isAdmin := false
	for _, m := range members {
		if m.UserID == userID && m.Role == "admin" {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		writeError(w, http.StatusForbidden,
			"only organisation admins can reject agents")
		return
	}

	if err := h.agentStore.Delete(agentID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// Get returns details for a single agent.
//
// GET /api/agents/{id}
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	agentID := chi.URLParam(r, "id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "missing agent id")
		return
	}

	agent, err := h.agentStore.Get(agentID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Verify the requester is a member of the agent's org.
	members, err := h.orgStore.ListMembers(agent.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	isMember := false
	for _, m := range members {
		if m.UserID == userID {
			isMember = true
			break
		}
	}
	if !isMember {
		writeError(w, http.StatusForbidden,
			"not a member of the agent's organisation")
		return
	}

	writeJSON(w, http.StatusOK, agent)
}

// archiveStateResponse is the camelCase wire form of the agent's archive state
// for the archive monitoring endpoint. collector.State's own json tags are
// snake_case (they mirror the agent's archive_state.json format, see
// internal/collector/state.go); the API contract uses camelCase, so the
// collector struct is mapped here rather than returned verbatim.
type archiveStateResponse struct {
	LastFile  string    `json:"lastFile"`
	LastPos   uint32    `json:"lastPos"`
	LastGTID  string    `json:"lastGtid,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// decodeResult round-trips a command response Result (already-decoded JSON as
// delivered by the hub) into a typed struct, mirroring the pitr package's
// paramsFrom approach.
func decodeResult(result interface{}, v interface{}) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// ArchiveStatus returns the agent archive loop's current state by dispatching
// CmdArchiveStatus to the connected agent. An offline agent cannot answer:
// 503 with no command dispatched. Transport failures and agent rejections are
// surfaced as 502 so the front-end can distinguish "agent down" from "agent
// responded with an error".
//
// GET /api/agents/{id}/archive
func (h *Handler) ArchiveStatus(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	agentID := chi.URLParam(r, "id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "missing agent id")
		return
	}

	agent, err := h.agentStore.Get(agentID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Verify the requester is a member of the agent's org.
	members, err := h.orgStore.ListMembers(agent.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	isMember := false
	for _, m := range members {
		if m.UserID == userID {
			isMember = true
			break
		}
	}
	if !isMember {
		writeError(w, http.StatusForbidden,
			"not a member of the agent's organisation")
		return
	}

	if h.commander == nil || !h.commander.IsConnected(agentID) {
		writeError(w, http.StatusServiceUnavailable, "agent is offline")
		return
	}

	resp, err := h.commander.SendToAgent(r.Context(), agentID, ws.Command{
		Cmd:    agentID,
		Type:   ws.CmdArchiveStatus,
		Params: map[string]interface{}{},
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("send archive status: %v", err))
		return
	}
	if resp.Status == ws.StatusError {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("agent rejected archive status: %s", resp.Error))
		return
	}

	var state collector.State
	if err := decodeResult(resp.Result, &state); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("parse archive state: %v", err))
		return
	}

	// Return 204 No Content when the archive loop has never run (empty state).
	if state.LastFile == "" && state.UpdatedAt.IsZero() {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	writeJSON(w, http.StatusOK, archiveStateResponse{
		LastFile:  state.LastFile,
		LastPos:   state.LastPos,
		LastGTID:  state.LastGTID,
		UpdatedAt: state.UpdatedAt,
	})
}

// generateRegToken creates a random hex token for agent registration.
func generateRegToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// generateCertSerial creates a placeholder certificate serial number.
// In production this would be the serial of the signed x509 certificate.
func generateCertSerial() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "serial_" + hex.EncodeToString(b)
}
