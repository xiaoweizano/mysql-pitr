package org

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/a-shan/mysql-pitr/internal/server/auth"
)

// Handler serves organisation HTTP endpoints.
type Handler struct {
	orgStore  OrgStore
	userStore auth.UserStore
	jwtSecret []byte
}

// NewHandler creates an org Handler.
func NewHandler(orgStore OrgStore, userStore auth.UserStore, jwtSecret []byte) *Handler {
	return &Handler{
		orgStore:  orgStore,
		userStore: userStore,
		jwtSecret: jwtSecret,
	}
}

// ---------- request / response types ----------

type createOrgRequest struct {
	Name string `json:"name"`
}

type createOrgResponse struct {
	Organization *Organization `json:"organization"`
}

type listOrgsResponse struct {
	Organizations []*Organization `json:"organizations"`
}

type inviteRequest struct {
}

type inviteResponse struct {
	Code string `json:"code"`
}

type acceptInviteRequest struct {
	Code string `json:"code"`
}

type acceptInviteResponse struct {
	Organization *Organization `json:"organization"`
}

type memberResponse struct {
	Members []Member `json:"members"`
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

// Create creates a new organisation. The requesting user becomes the admin.
//
// POST /api/orgs
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	var req createOrgRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	org := &Organization{Name: req.Name}
	if err := h.orgStore.Create(org); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := h.orgStore.AddMember(org.ID, userID, "admin"); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, createOrgResponse{Organization: org})
}

// List returns all organisations the authenticated user belongs to.
//
// GET /api/orgs
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	orgs, err := h.orgStore.ListByUserID(userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, listOrgsResponse{Organizations: orgs})
}

// Invite creates an invitation code for an organisation. Only admins may
// invite new members.
//
// POST /api/orgs/{id}/invite
func (h *Handler) Invite(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	orgID := chi.URLParam(r, "id")
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "missing org id")
		return
	}

	// Verify the requester is an admin of this org.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
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
		writeError(w, http.StatusForbidden, "only admins can invite members")
		return
	}

	invite, err := h.orgStore.CreateInvite(orgID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, inviteResponse{Code: invite.Code})
}

// AcceptInvite accepts an invitation and adds the requesting user as a member
// of the organisation.
//
// POST /api/orgs/{id}/accept
func (h *Handler) AcceptInvite(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	orgID := chi.URLParam(r, "id")
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "missing org id")
		return
	}

	var req acceptInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Code == "" {
		writeError(w, http.StatusBadRequest, "code is required")
		return
	}

	invite, err := h.orgStore.GetInviteByCode(req.Code)
	if err != nil {
		writeError(w, http.StatusNotFound, "invalid invitation code")
		return
	}

	if invite.OrgID != orgID {
		writeError(w, http.StatusBadRequest, "invitation code does not match organisation")
		return
	}

	org, err := h.orgStore.Get(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := h.orgStore.AddMember(orgID, userID, "member"); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	// Delete the invite so it cannot be reused.
	_ = h.orgStore.DeleteInvite(req.Code)

	writeJSON(w, http.StatusOK, acceptInviteResponse{Organization: org})
}

// Delete removes an organisation. Only admins of the org may delete it.
//
// DELETE /api/orgs/{id}
func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	orgID := chi.URLParam(r, "id")
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "missing org id")
		return
	}

	// Verify the requester is an admin of this org.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
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
		writeError(w, http.StatusForbidden, "only admins can delete organisations")
		return
	}

	if err := h.orgStore.Delete(orgID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// ListMembers returns all members of an organisation.
//
// GET /api/orgs/{id}/members
func (h *Handler) ListMembers(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	orgID := chi.URLParam(r, "id")
	if orgID == "" {
		writeError(w, http.StatusBadRequest, "missing org id")
		return
	}

	// Verify the requester is a member of this org.
	members, err := h.orgStore.ListMembers(orgID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
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

	writeJSON(w, http.StatusOK, memberResponse{Members: members})
}
