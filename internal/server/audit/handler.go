package audit

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
)

// Handler serves audit query and export HTTP endpoints.
type Handler struct {
	auditStore AuditStore
	orgStore   org.OrgStore
	jwtSecret  []byte
}

// NewHandler creates an audit Handler.
func NewHandler(auditStore AuditStore, orgStore org.OrgStore, jwtSecret []byte) *Handler {
	return &Handler{
		auditStore: auditStore,
		orgStore:   orgStore,
		jwtSecret:  jwtSecret,
	}
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

// Query returns filtered audit entries as a JSON array.
//
// GET /api/audit?org_id=X&from=ISO&to=ISO&status=Y&agent_id=Z
func (h *Handler) Query(w http.ResponseWriter, r *http.Request) {
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

	// Verify the requester is a member of this org.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	if !isMember(members, userID) {
		writeError(w, http.StatusForbidden, "not a member of this organisation")
		return
	}

	filter := AuditFilter{OrgID: orgID}

	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		t, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid from timestamp, use ISO8601/RFC3339")
			return
		}
		filter.From = t
	}
	if toStr := r.URL.Query().Get("to"); toStr != "" {
		t, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid to timestamp, use ISO8601/RFC3339")
			return
		}
		filter.To = t
	}
	if s := r.URL.Query().Get("status"); s != "" {
		filter.Status = s
	}
	if a := r.URL.Query().Get("agent_id"); a != "" {
		filter.AgentID = a
	}
		if oid := r.URL.Query().Get("operation_id"); oid != "" {
			filter.OperationID = oid
		}

	entries, err := h.auditStore.Query(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if entries == nil {
		entries = []AuditEntry{}
	}

	writeJSON(w, http.StatusOK, entries)
}

// Export returns audit entries as a CSV file.
//
// GET /api/audit/export?org_id=X
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
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

	// Verify the requester is a member of this org.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return
	}
	if !isMember(members, userID) {
		writeError(w, http.StatusForbidden, "not a member of this organisation")
		return
	}

	filter := AuditFilter{OrgID: orgID}

	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			filter.From = t
		}
	}
	if toStr := r.URL.Query().Get("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			filter.To = t
		}
	}
	if s := r.URL.Query().Get("status"); s != "" {
		filter.Status = s
	}
	if a := r.URL.Query().Get("agent_id"); a != "" {
		filter.AgentID = a
	}
		if oid := r.URL.Query().Get("operation_id"); oid != "" {
			filter.OperationID = oid
		}

	entries, err := h.auditStore.Query(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=\"audit_%s_%d.csv\"", orgID, time.Now().Unix()))
	w.WriteHeader(http.StatusOK)

	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Header row.
	_ = writer.Write([]string{
		"操作ID", "操作人", "时间", "组织ID", "实例ID",
		"目标表", "恢复时间", "影响行数", "状态", "错误详情",
	})

	for _, e := range entries {
		_ = writer.Write([]string{
			e.OperationID,
			e.Operator,
			e.Timestamp.Format(time.RFC3339),
			e.OrgID,
			e.AgentID,
			e.TargetTable,
			e.RecoveryTime.Format(time.RFC3339),
			strconv.FormatInt(e.RowsAffected, 10),
			e.Status,
			e.ErrorDetails,
		})
	}
}

// List returns all audit entries for an org (no filtering).
//
// Deprecated: use Query instead. Kept for backward compatibility.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	h.Query(w, r)
}

// ---------- internal ----------

func isMember(members []org.Member, userID string) bool {
	for _, m := range members {
		if m.UserID == userID {
			return true
		}
	}
	return false
}
