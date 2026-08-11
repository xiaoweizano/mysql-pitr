package pitr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/server/agent"
	"github.com/a-shan/mysql-pitr/internal/server/audit"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/ws"
)

// AgentCommander is the subset of the agent hub the PITR flow needs. The
// production implementation is *hub.Hub; tests provide a fake.
type AgentCommander interface {
	IsConnected(agentID string) bool
	SendToAgent(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error)
}

// Handler serves the v3 PITR workflow HTTP endpoints: start (create + scan),
// transactions preview, select, execute, pause/resume/cancel, status, list,
// and the SSE event stream. Agent stream events (tx_meta/sql/scan_done/
// progress/op_done/op_error) arrive via HandleStreamEvent from the hub and are
// fanned out to SSE subscribers through the event bus.
type Handler struct {
	opStore    OperationStore
	agentStore agent.AgentStore
	orgStore   org.OrgStore
	auditStore audit.AuditStore
	jwtSecret  []byte

	// bus fans agent stream events out to per-operation SSE subscribers.
	bus *eventBus
	// commander is the agent command channel. It may be nil in minimal/dev
	// setups; operations are rejected as offline when nil.
	commander AgentCommander

	// previews holds the in-memory scan preview per operation: the ordered
	// transaction metadata list plus the SQL statements staged from sql events
	// (persisted only when the operator selects transactions). Populated by the
	// stream handler (hub read loop goroutine) and read by the HTTP
	// transactions/select handlers — all access goes through the per-op mutex.
	previewMu sync.Mutex
	previews  map[string]*opPreview
}

// NewHandler creates a PITR Handler.
func NewHandler(
	opStore OperationStore,
	agentStore agent.AgentStore,
	orgStore org.OrgStore,
	auditStore audit.AuditStore,
	bus *eventBus,
	commander AgentCommander,
	jwtSecret []byte,
) *Handler {
	return &Handler{
		opStore:    opStore,
		agentStore: agentStore,
		orgStore:   orgStore,
		auditStore: auditStore,
		jwtSecret:  jwtSecret,
		bus:        bus,
		commander:  commander,
		previews:   make(map[string]*opPreview),
	}
}

// ---------- request / response types ----------

type startRequest struct {
	AgentID string `json:"agentId"`
	// Type is the operation kind; defaults to "pitr" when empty.
	Type string `json:"type"`
	// Mode selects the scan granularity: "meta" | "sql" | "selected".
	Mode       string        `json:"mode"`
	Filter     ws.ScanFilter `json:"filter"`
	MaxPreview int           `json:"maxPreview"`
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

// txMetaWire mirrors the agent's tx_meta event payload (scan.TxMeta). Tables
// use the ws.TableRefJSON shape (lowercase keys), matching the ScanFilter wire
// convention.
type txMetaWire struct {
	TxID       string          `json:"txId"`
	GTID       string          `json:"gtid,omitempty"`
	XID        uint64          `json:"xid,omitempty"`
	CommitTime time.Time       `json:"commitTime"`
	Schema     string          `json:"schema,omitempty"`
	Tables     []ws.TableRefJSON `json:"tables,omitempty"`
	RowCount   int             `json:"rowCount"`
	Truncated  bool            `json:"truncated,omitempty"`
}

// txPreview decorates a scanned transaction with the SQL staged from sql
// events (populated in sql/selected modes).
type txPreview struct {
	txMetaWire
	SQL []ws.StatementWire `json:"sql,omitempty"`
}

// opPreview is the in-memory scan result of one operation. Every field is
// accessed under mu.
type opPreview struct {
	mu      sync.Mutex
	metas   []txMetaWire                  // ordered transaction metadata
	sqlByTx map[string][]ws.StatementWire // staged SQL per transaction ID
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

// authorizeOp authenticates the request, resolves the operation by URL id and
// checks the requesting user is a member of the operation's organisation. On
// failure it writes the error response and returns ok=false.
func (h *Handler) authorizeOp(w http.ResponseWriter, r *http.Request) (*Operation, bool) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return nil, false
	}
	opID := chi.URLParam(r, "id")
	if opID == "" {
		writeError(w, http.StatusBadRequest, "missing operation id")
		return nil, false
	}
	op, err := h.opStore.Get(opID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return nil, false
	}
	members, err := h.orgStore.ListMembers(op.OrgID)
	if err != nil {
		writeError(w, http.StatusNotFound, "organisation not found")
		return nil, false
	}
	if !orgMemberContains(members, userID) {
		writeError(w, http.StatusForbidden, "not a member of this organisation")
		return nil, false
	}
	return op, true
}

func orgMemberContains(members []org.Member, userID string) bool {
	for _, m := range members {
		if m.UserID == userID {
			return true
		}
	}
	return false
}

// sendCommand routes a command to the agent via the commander. Without a
// configured commander (minimal/dev router) it fails so callers can degrade
// gracefully.
func (h *Handler) sendCommand(ctx context.Context, agentID string, cmd ws.Command) (*ws.Response, error) {
	if h.commander == nil {
		return nil, fmt.Errorf("pitr: agent command channel not configured")
	}
	return h.commander.SendToAgent(ctx, agentID, cmd)
}

// agentConnected reports whether the operation's agent currently has a live
// hub connection.
func (h *Handler) agentConnected(agentID string) bool {
	return h.commander != nil && h.commander.IsConnected(agentID)
}

// paramsFrom round-trips a wire struct (ws.ScanRequest / ws.ExecuteRequest)
// into a command params map, preserving the struct's json tags exactly as the
// agent's decodeParams expects.
func paramsFrom(v interface{}) map[string]interface{} {
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]interface{}{}
	}
	var m map[string]interface{}
	_ = json.Unmarshal(b, &m)
	return m
}

// scanFilterToBinlog converts the wire ScanFilter into the persisted
// binlog.Filter. The mapping mirrors the daemon's wsFilterToBinlog so the
// operation record reflects what the agent will scan. Callers must validate
// the filter first (validateScanFilter).
func scanFilterToBinlog(f ws.ScanFilter) binlog.Filter {
	var out binlog.Filter
	for _, t := range f.Tables {
		out.Tables = append(out.Tables, binlog.TableRef{Schema: t.Schema, Table: t.Table})
	}
	if f.TimeStart != "" || f.TimeEnd != "" {
		tr := &binlog.TimeRange{}
		if f.TimeStart != "" {
			tr.Start, _ = time.Parse(time.RFC3339, f.TimeStart)
		}
		if f.TimeEnd != "" {
			tr.End, _ = time.Parse(time.RFC3339, f.TimeEnd)
		}
		out.TimeRange = tr
	}
	if f.GTIDSet != "" {
		gs, _ := mysql.ParseGTIDSet("mysql", f.GTIDSet)
		out.GTIDSet = gs
	}
	if f.StartFile != "" {
		out.StartPos = mysql.Position{Name: f.StartFile, Pos: f.StartPos}
	}
	if f.EndFile != "" {
		out.EndPos = mysql.Position{Name: f.EndFile, Pos: f.EndPos}
	}
	out.MaxRowsPerTx = f.MaxRowsPerTx
	out.SelectedTxIDs = f.SelectedTxIDs
	return out
}

// binlogFilterToWire converts the persisted binlog.Filter back into the wire
// ScanFilter for a directed re-scan. The round-trip mirrors scanFilterToBinlog,
// so the agent receives the same scan constraints the operation record
// reflects; call sites adjust the result (e.g. set SelectedTxIDs).
func binlogFilterToWire(f binlog.Filter) ws.ScanFilter {
	var out ws.ScanFilter
	for _, t := range f.Tables {
		out.Tables = append(out.Tables, ws.TableRefJSON{Schema: t.Schema, Table: t.Table})
	}
	if f.TimeRange != nil {
		if !f.TimeRange.Start.IsZero() {
			out.TimeStart = f.TimeRange.Start.Format(time.RFC3339)
		}
		if !f.TimeRange.End.IsZero() {
			out.TimeEnd = f.TimeRange.End.Format(time.RFC3339)
		}
	}
	if f.GTIDSet != nil {
		out.GTIDSet = f.GTIDSet.String()
	}
	if f.StartPos.Name != "" {
		out.StartFile = f.StartPos.Name
		out.StartPos = f.StartPos.Pos
	}
	if f.EndPos.Name != "" {
		out.EndFile = f.EndPos.Name
		out.EndPos = f.EndPos.Pos
	}
	out.MaxRowsPerTx = f.MaxRowsPerTx
	out.SelectedTxIDs = f.SelectedTxIDs
	return out
}

// validateScanFilter rejects filters with unparseable time bounds or GTID sets
// up front, so the agent never receives a scan it would reject.
func validateScanFilter(f ws.ScanFilter) error {
	if f.TimeStart != "" {
		if _, err := time.Parse(time.RFC3339, f.TimeStart); err != nil {
			return fmt.Errorf("invalid filter.timeStart %q: expected RFC3339", f.TimeStart)
		}
	}
	if f.TimeEnd != "" {
		if _, err := time.Parse(time.RFC3339, f.TimeEnd); err != nil {
			return fmt.Errorf("invalid filter.timeEnd %q: expected RFC3339", f.TimeEnd)
		}
	}
	if f.GTIDSet != "" {
		if _, err := mysql.ParseGTIDSet("mysql", f.GTIDSet); err != nil {
			if _, err2 := mysql.ParseGTIDSet("mariadb", f.GTIDSet); err2 != nil {
				return fmt.Errorf("invalid filter.gtidSet %q", f.GTIDSet)
			}
		}
	}
	return nil
}

// statementsToWire converts persisted statements to the execute wire format.
func statementsToWire(stmts []Statement) []ws.StatementWire {
	wires := make([]ws.StatementWire, 0, len(stmts))
	for _, s := range stmts {
		wires = append(wires, ws.StatementWire{
			SQL:      s.SQL,
			TxID:     s.TxID,
			TxOrder:  s.TxOrder,
			Warnings: s.Warnings,
		})
	}
	return wires
}

// transitionTo applies the state machine transition from the operation's
// current status to `to` and persists it with a conditional update (CAS): the
// row is only touched while its persisted status still equals the status this
// call was made against. A concurrent transition therefore cannot be silently
// overwritten — the CAS either wins (ok=true, op.Status == to) or loses
// (ok=false, op.Status rolled back to the pre-call status), in which case the
// caller must treat the operation as already advanced by another actor and
// must not audit or publish. Invalid transitions (e.g. a late op_done arriving
// after the operation was cancelled locally) are rejected and reported, never
// applied.
func (h *Handler) transitionTo(op *Operation, to OperationState) (bool, error) {
	from := op.Status
	if err := TryTransitionErr(from, to); err != nil {
		return false, err
	}
	op.Status = to
	op.UpdatedAt = time.Now()
	ok, err := h.opStore.UpdateIfStatus(op, from)
	if err != nil {
		op.Status = from
		return false, err
	}
	if !ok {
		// Another actor won the CAS: roll the in-memory record back to the
		// status it was read with so callers that log or report op.Status stay
		// truthful about the failed attempt.
		op.Status = from
		return false, nil
	}
	return true, nil
}

// appendAudit records an audit entry for an operation state change. Stream
// events record the agent as the operator; HTTP actions record the user.
func (h *Handler) appendAudit(op *Operation, state OperationState, operator, errorDetails string) {
	_ = h.auditStore.Append(&audit.AuditEntry{
		OperationID:  op.ID,
		Operator:     operator,
		Timestamp:    time.Now(),
		OrgID:        op.OrgID,
		AgentID:      op.AgentID,
		Status:       string(state),
		ErrorDetails: errorDetails,
	})
}

// failOperation marks an operation failed after a command transport or agent
// rejection. The transition is only applied where the state machine allows it
// (created/scanning/executing), under CAS so a racing terminal event (e.g.
// op_done landing as done while the failure is in flight) wins cleanly;
// operations in stable, retryable states (ready, paused) are left untouched so
// the operator can retry or cancel.
func (h *Handler) failOperation(op *Operation, details string) {
	ok, err := h.transitionTo(op, StateFailed)
	if err != nil {
		log.Printf("pitr: op %s: agent failure %q left operation in state %q (retryable)", op.ID, details, op.Status)
		return
	}
	if !ok {
		log.Printf("pitr: op %s: agent failure %q raced a concurrent transition — no failed audit (state %q)", op.ID, details, op.Status)
		h.forgetPreview(op.ID)
		return
	}
	h.appendAudit(op, StateFailed, "agent", details)
	h.forgetPreview(op.ID)
}

// blockOperation moves an in-flight operation to blocked (terminal) under CAS:
// the transition applies only while the persisted status still equals the
// status the op was read with, so a racing terminal event (op_done / op_error)
// wins cleanly and is never overwritten. The blocked audit is written only on
// a successful transition; an invalid or lost transition is logged and the
// operation left as-is. blocked is terminal, so the in-memory preview is
// dropped — the operator starts a new attempt after the agent reconnects.
func (h *Handler) blockOperation(op *Operation, details string) {
	ok, err := h.transitionTo(op, StateBlocked)
	if err != nil {
		log.Printf("pitr: op %s: block: %v", op.ID, err)
		return
	}
	if !ok {
		log.Printf("pitr: op %s: block lost to a concurrent transition — no blocked audit (state %q)", op.ID, op.Status)
		return
	}
	h.appendAudit(op, StateBlocked, "system", details)
	h.forgetPreview(op.ID)
}

// HandleAgentDisconnect blocks every operation of the disconnected agent that
// is mid-flight (scanning or executing): the agent is gone, so neither can
// make progress, and the operation must not linger in a non-terminal state.
// Operations of other agents, and operations the agent already advanced (done
// / failed / cancelled / blocked), are never touched; blocked is terminal, so
// repeated disconnects write no duplicate audits. The hub invokes this from
// its OnDisconnect lifecycle hook after the agent store status has been
// updated.
func (h *Handler) HandleAgentDisconnect(agentID string) {
	for _, status := range []OperationState{StateScanning, StateExecuting} {
		ops, err := h.opStore.ListByStatus(status)
		if err != nil {
			log.Printf("pitr: agent %s disconnected: list %s operations: %v", agentID, status, err)
			continue
		}
		for _, op := range ops {
			if op.AgentID != agentID {
				continue
			}
			h.blockOperation(op, "agent disconnected")
		}
	}
}

// ReconcileOnStartup blocks every in-flight operation left over from a
// previous server run. At startup every agent is offline by definition, so a
// scanning or executing operation cannot make progress; blocking it makes the
// state truthful and lets the operator start a fresh attempt (or resume, for
// a reconnected agent) instead of waiting on a ghost operation. Operations in
// stable states (ready, paused) and terminal states are left untouched.
// A list failure returns an error (the caller logs it — startup must not fail
// because of historical operations); per-operation transition failures are
// logged and skipped.
func (h *Handler) ReconcileOnStartup() error {
	for _, status := range []OperationState{StateScanning, StateExecuting} {
		ops, err := h.opStore.ListByStatus(status)
		if err != nil {
			return fmt.Errorf("pitr: startup reconcile: list %s operations: %w", status, err)
		}
		for _, op := range ops {
			h.blockOperation(op, "server restart — operation blocked; retry after the agent reconnects")
		}
	}
	return nil
}

// ---------- preview (in-memory scan state) ----------

// preview returns the operation's preview, or nil when none exists.
func (h *Handler) preview(opID string) *opPreview {
	h.previewMu.Lock()
	defer h.previewMu.Unlock()
	return h.previews[opID]
}

// previewFor returns the operation's preview, creating it on first use.
func (h *Handler) previewFor(opID string) *opPreview {
	h.previewMu.Lock()
	defer h.previewMu.Unlock()
	p := h.previews[opID]
	if p == nil {
		p = &opPreview{sqlByTx: make(map[string][]ws.StatementWire)}
		h.previews[opID] = p
	}
	return p
}

// snapshot returns a consistent copy of the operation's preview for HTTP
// handlers, or nil when no scan has been collected.
func (h *Handler) snapshot(opID string) *opPreview {
	p := h.preview(opID)
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := &opPreview{
		metas:   append([]txMetaWire(nil), p.metas...),
		sqlByTx: make(map[string][]ws.StatementWire, len(p.sqlByTx)),
	}
	for txID, wires := range p.sqlByTx {
		snap.sqlByTx[txID] = append([]ws.StatementWire(nil), wires...)
	}
	return snap
}

// forgetPreview drops the in-memory preview of an operation once it reaches a
// terminal state — the preview can be regenerated by a fresh scan.
func (h *Handler) forgetPreview(opID string) {
	h.previewMu.Lock()
	defer h.previewMu.Unlock()
	delete(h.previews, opID)
}

// stageTxMeta appends a scanned transaction to the operation's preview.
func (h *Handler) stageTxMeta(opID string, meta txMetaWire) {
	p := h.previewFor(opID)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metas = append(p.metas, meta)
}

// stageSQL groups sql-event statements by transaction ID in the preview.
func (h *Handler) stageSQL(opID string, wires []ws.StatementWire) {
	p := h.previewFor(opID)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range wires {
		if w.TxID == "" {
			continue
		}
		p.sqlByTx[w.TxID] = append(p.sqlByTx[w.TxID], w)
	}
}

// ---------- stream event handling (hub → handler) ----------

// HandleStreamEvent is the hub callback for agent-pushed stream_event
// envelopes. It is invoked asynchronously from the hub read loop, so it must
// be safe for concurrent use: preview mutations run under the per-op mutex and
// store updates are serialised by SQLite.
//
// Wire contract (agent side, see cmd/agent/serve.go streamEventCommand):
//
//	{ "cmd": "ev-<opId>", "type": "stream_event",
//	  "params": { "id": "<opId>", "kind": "<kind>", "data": <raw JSON> } }
func (h *Handler) HandleStreamEvent(agentID string, cmd ws.Command) {
	opID, _ := cmd.Params["id"].(string)
	if opID == "" {
		log.Printf("pitr: stream event without operation id — ignoring")
		return
	}
	kind, _ := cmd.Params["kind"].(string)
	if kind == "" {
		log.Printf("pitr: op %s: stream event without kind — ignoring", opID)
		return
	}
	raw, err := json.Marshal(cmd.Params["data"])
	if err != nil {
		log.Printf("pitr: op %s: marshal event data: %v", opID, err)
		return
	}

	op, err := h.opStore.Get(opID)
	if err != nil {
		log.Printf("pitr: op %s: stream event for unknown operation — ignoring", opID)
		return
	}
	if op.AgentID != agentID {
		log.Printf("pitr: op %s: stream event from agent %s (expected %s) — ignoring", opID, agentID, op.AgentID)
		return
	}

	ev := ws.StreamEvent{ID: opID, Kind: kind, Data: raw}

	switch kind {
	case ws.EvTxMeta:
		var meta txMetaWire
		if err := json.Unmarshal(raw, &meta); err != nil {
			log.Printf("pitr: op %s: parse tx_meta: %v", opID, err)
			return
		}
		h.stageTxMeta(opID, meta)
		h.bus.Publish(opID, ev)

	case ws.EvSQL:
		var wires []ws.StatementWire
		if err := json.Unmarshal(raw, &wires); err != nil {
			log.Printf("pitr: op %s: parse sql event: %v", opID, err)
			return
		}
		h.stageSQL(opID, wires)
		h.bus.Publish(opID, ev)

	case ws.EvScanDone:
		// Idempotent fallback for the rescan transition race: the agent can
		// finish the directed second scan (scan_done arrives) before the Select
		// handler persists the ready -> scanning transition, leaving the op on
		// disk as ready even though the rescan ran. A ready op is treated as
		// scanning: the fallback CAS persists scanning (false only when a
		// concurrent actor already advanced the state), then the scanning ->
		// ready migration below is CAS-authoritative either way — a lost
		// fallback never writes a bogus ready.
		if op.Status == StateReady {
			op.Status = StateScanning
			op.UpdatedAt = time.Now()
			applied, ferr := h.opStore.UpdateIfStatus(op, StateReady)
			if ferr != nil {
				log.Printf("pitr: op %s: scan_done fallback: %v", opID, ferr)
				return
			}
			if !applied {
				log.Printf("pitr: op %s: scan_done fallback lost — operation advanced concurrently", opID)
			}
		}
		// Audit and publish only on a successful transition: a late scan_done
		// (e.g. after the operator cancelled the scan) must not write a ready
		// audit or move the terminal state. CAS failure means a concurrent actor
		// already advanced the operation — no audit.
		ok, err := h.transitionTo(op, StateReady)
		if err != nil {
			log.Printf("pitr: op %s: scan_done: %v", opID, err)
			return
		}
		if !ok {
			log.Printf("pitr: op %s: scan_done ignored — operation advanced concurrently", opID)
			return
		}
		h.appendAudit(op, StateReady, "agent", "")
		// Selected-mode completion: the directed second scan just staged the
		// SQL for the selected transactions; persist the statements so Execute
		// can load them. (sql-mode operations persist at Select time and have
		// no selection recorded here, so this is a no-op for them.)
		h.persistSelectedStatements(op)
		h.bus.Publish(opID, ev)

	case ws.EvProgress:
		// Server-side checkpoint double-write: the agent's progress carries the
		// executor's per-batch checkpoint (done/total/errors). Persist it so an
		// operation can be resumed from the server's own records after the
		// agent (or server) restarts. A malformed progress payload still gets
		// streamed to SSE subscribers; only the checkpoint write is skipped.
		var prog executor.Progress
		if err := json.Unmarshal(raw, &prog); err != nil {
			log.Printf("pitr: op %s: parse progress: %v", opID, err)
		} else {
			errs := prog.Errors
			if errs == nil {
				errs = []executor.ExecError{}
			}
			errsJSON, merr := json.Marshal(errs)
			if merr != nil {
				log.Printf("pitr: op %s: marshal progress errors: %v", opID, merr)
			} else if cerr := h.opStore.SaveCheckpoint(opID, prog.Done, prog.Total, string(errsJSON)); cerr != nil {
				log.Printf("pitr: op %s: save checkpoint: %v", opID, cerr)
			}
		}
		h.bus.Publish(opID, ev)

	case ws.EvOpDone:
		// The agent's op_done carries an executor.FinalReport payload
		// ({"done":..,"total":..,"paused":true|false}). paused=true is the
		// executor answering a cancel (the pause path): a pause confirmation,
		// not a terminal completion — no done audit, no terminal SSE close.
		var report opDonePayload
		if err := json.Unmarshal(raw, &report); err == nil && report.Paused {
			h.confirmPause(op, opID, report)
			return
		}
		// Non-paused completion: audit and publish only when the transition is
		// applied; a late op_done for a terminal operation is ignored. Statement
		// errors recorded by the executor surface in the audit detail (the SSE
		// payload passes the raw errors through to the front-end verbatim).
		//
		// Idempotent fallback for the execute transition race: the agent can
		// finish (op_done arrives) before the execute handler persists the
		// ready -> executing transition, leaving the op on disk as ready even
		// though the restore ran. A ready op is treated as executing: the
		// fallback CAS persists executing (false only when a concurrent actor
		// already advanced the state), then the executing -> done migration
		// below is CAS-authoritative either way — a lost fallback never writes
		// a bogus done.
		if op.Status == StateReady {
			op.Status = StateExecuting
			op.UpdatedAt = time.Now()
			applied, ferr := h.opStore.UpdateIfStatus(op, StateReady)
			if ferr != nil {
				log.Printf("pitr: op %s: op_done fallback: %v", opID, ferr)
				return
			}
			if !applied {
				log.Printf("pitr: op %s: op_done fallback lost — operation advanced concurrently", opID)
			}
		}
		ok, err := h.transitionTo(op, StateDone)
		if err != nil {
			log.Printf("pitr: op %s: op_done: %v", opID, err)
			return
		}
		if !ok {
			log.Printf("pitr: op %s: op_done ignored — operation advanced concurrently", opID)
			return
		}
		h.appendAudit(op, StateDone, "agent", errorSummary(report.Errors))
		h.forgetPreview(opID)
		h.bus.Publish(opID, ev)

	case ws.EvOpError:
		// A late op_error for a terminal operation (e.g. the scan goroutine
		// exiting with context.Canceled after a cancel) must not write a failed
		// audit or move the terminal state. CAS failure means a concurrent actor
		// already advanced the operation — no audit.
		ok, err := h.transitionTo(op, StateFailed)
		if err != nil {
			log.Printf("pitr: op %s: op_error: %v", opID, err)
			return
		}
		if !ok {
			log.Printf("pitr: op %s: op_error ignored — operation advanced concurrently", opID)
			return
		}
		h.appendAudit(op, StateFailed, "agent", errorDetailsFromEvent(raw))
		h.forgetPreview(opID)
		h.bus.Publish(opID, ev)

	default:
		log.Printf("pitr: op %s: unknown stream event kind %q — ignoring", opID, kind)
	}
}

// opDonePayload is the subset of the agent's executor.FinalReport that the
// server needs from an op_done event: the paused flag distinguishes a
// cancel/pause acknowledgement from a completed run, and Errors carries the
// per-statement failures recorded during execution (surfaced in the audit
// detail and passed through to SSE subscribers for front-end display).
type opDonePayload struct {
	Done   int                  `json:"done"`
	Total  int                  `json:"total"`
	Paused bool                 `json:"paused"`
	Errors []executor.ExecError `json:"errors,omitempty"`
}

// errorSummary renders the executor's statement errors as a bounded audit
// detail line (first maxErrorSummaryEntries entries, then an ellipsis), so a
// run with thousands of failed statements still yields a readable entry.
const maxErrorSummaryEntries = 5

func errorSummary(errs []executor.ExecError) string {
	if len(errs) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d statement(s) failed:", len(errs))
	for i, e := range errs {
		if i >= maxErrorSummaryEntries {
			b.WriteString(" ...")
			break
		}
		fmt.Fprintf(&b, " #%d %s", e.Statement, e.Err)
	}
	return b.String()
}

// confirmPause handles an op_done event whose FinalReport carries Paused=true:
// the agent's executor stopped on the pause cancel. When the operation is
// paused, or still executing while the pause transition lands, this is the
// pause acknowledgement: the operation stays resumable — a paused audit is
// recorded, no done audit is written, and an op_paused event is published so
// SSE subscribers can show the paused state without polling /status. The
// stream itself stays open (op_paused is not a terminal kind; the op is not
// terminal). The preview is kept for resume. A pause acknowledgement for an
// operation in any other state is stale and ignored.
func (h *Handler) confirmPause(op *Operation, opID string, report opDonePayload) {
	switch op.Status {
	case StatePaused:
		// already paused locally — the agent's acknowledgement confirms it.
	case StateExecuting:
		// The pause transition has not landed yet (HTTP pause handler in
		// flight): apply the valid executing → paused transition under CAS. A
		// CAS loss means a concurrent actor (e.g. the non-paused op_done)
		// already advanced the operation — no paused audit.
		ok, err := h.transitionTo(op, StatePaused)
		if err != nil {
			log.Printf("pitr: op %s: pause confirmation: %v", opID, err)
			return
		}
		if !ok {
			log.Printf("pitr: op %s: pause confirmation lost to a concurrent transition", opID)
			return
		}
	default:
		log.Printf("pitr: op %s: pause confirmation for op in state %q — ignoring", opID, op.Status)
		return
	}
	h.appendAudit(op, StatePaused, "agent", "")
	data, _ := json.Marshal(map[string]interface{}{
		"status": string(op.Status),
		"done":   report.Done,
		"total":  report.Total,
	})
	h.bus.Publish(opID, ws.StreamEvent{ID: opID, Kind: ws.EvOpPaused, Data: data})
	log.Printf("pitr: op %s: agent confirmed pause (done=%d total=%d)", opID, report.Done, report.Total)
}

// errorDetailsFromEvent extracts the error message from an op_error payload
// (the agent sends a JSON string).
func errorDetailsFromEvent(raw json.RawMessage) string {
	var msg string
	if err := json.Unmarshal(raw, &msg); err == nil {
		return msg
	}
	return string(raw)
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
		filtered = append(filtered, operationView{
			Operation:      op,
			Filter:         binlogFilterToWire(op.Filter),
			AgentConnected: h.agentConnected(op.AgentID),
		})
	}
	if filtered == nil {
		filtered = []operationView{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"operations": filtered})
}

// operationView decorates an operation with live agent-connection state for
// the list API. Filter is projected to the wire ScanFilter shape (lowercase
// keys, GTID set as a string) so the wizard can be repopulated from a List
// response — the embedded Operation.Filter (the internal binlog.Filter shape)
// never reaches the JSON payload.
type operationView struct {
	*Operation
	// Filter shadows the embedded Operation.Filter with the front-end DTO.
	Filter         ws.ScanFilter `json:"filter"`
	AgentConnected bool          `json:"agentConnected"`
}

// Start creates an operation record and launches the agent scan. On success
// the operation is in `scanning`; an offline agent leaves it `blocked`; a
// command failure leaves it `failed`.
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
	if req.AgentID == "" {
		writeError(w, http.StatusBadRequest, "agentId is required")
		return
	}
	if req.Type == "" {
		req.Type = "pitr"
	}
	switch req.Type {
	case "flashback", "update_rollback", "pitr", "tx_rollback":
	default:
		writeError(w, http.StatusBadRequest,
			`type must be one of "flashback", "update_rollback", "pitr", or "tx_rollback"`)
		return
	}
	if req.Mode != "meta" && req.Mode != "sql" && req.Mode != "selected" {
		writeError(w, http.StatusBadRequest,
			"mode must be one of \"meta\", \"sql\", or \"selected\"")
		return
	}
	if err := validateScanFilter(req.Filter); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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
		writeError(w, http.StatusForbidden, "not a member of the agent's organisation")
		return
	}

	if !agt.Approved {
		writeError(w, http.StatusConflict, "agent is not approved")
		return
	}

	op := &Operation{
		OrgID:     agt.OrgID,
		AgentID:   req.AgentID,
		Type:      req.Type,
		Mode:      req.Mode,
		Filter:    scanFilterToBinlog(req.Filter),
		Status:    StateCreated,
		CreatedBy: userID,
	}
	if err := h.opStore.Create(op); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Reserve the preview slot so stream events can populate it.
	h.previewFor(op.ID)

	if !h.agentConnected(req.AgentID) {
		op.Status = StateBlocked
		op.UpdatedAt = time.Now()
		_ = h.opStore.Update(op)
		writeError(w, http.StatusConflict,
			"agent is offline — start it with `mysql-pitr-agent serve` first")
		return
	}

	resp, err := h.sendCommand(r.Context(), op.AgentID, ws.Command{
		Cmd:  op.ID,
		Type: ws.CmdScan,
		Params: paramsFrom(ws.ScanRequest{
			Filter:     req.Filter,
			Mode:       req.Mode,
			MaxPreview: req.MaxPreview,
		}),
	})
	if err != nil {
		h.failOperation(op, err.Error())
		writeError(w, http.StatusBadGateway, fmt.Sprintf("start scan: %v", err))
		return
	}
	if resp != nil && resp.Status == ws.StatusError {
		h.failOperation(op, resp.Error)
		writeError(w, http.StatusBadGateway, fmt.Sprintf("agent rejected scan: %s", resp.Error))
		return
	}

	if ok, err := h.transitionTo(op, StateScanning); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusConflict, "operation state changed concurrently — reload and retry")
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
	op, ok := h.authorizeOp(w, r)
	if !ok {
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

// Transactions returns the in-memory scan preview of an operation: the ordered
// transaction metadata list, each decorated with the SQL staged from sql
// events. Partial results are returned while the scan is still running.
//
// GET /api/pitr/{id}/transactions
func (h *Handler) Transactions(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}

	txs := []txPreview{}
	if snap := h.snapshot(op.ID); snap != nil {
		for _, m := range snap.metas {
			txs = append(txs, txPreview{txMetaWire: m, SQL: snap.sqlByTx[m.TxID]})
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operationId":  op.ID,
		"status":       op.Status,
		"transactions": txs,
	})
}

// Select drives the second phase of the recovery flow. In `sql` mode the
// statements were staged during the first scan, so the selection is persisted
// locally. In `selected` mode the first scan was metadata-only (no SQL was ever
// produced), so Select dispatches a directed second scan — see selectRescan —
// whose sql events populate the preview; scan_done returns the operation to
// ready and persists the statements. Only `ready` operations can be selected.
//
// POST /api/pitr/{id}/select   {"txIds": ["..."]}
func (h *Handler) Select(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}

	var req struct {
		TxIDs []string `json:"txIds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(req.TxIDs) == 0 {
		writeError(w, http.StatusBadRequest, "txIds is required")
		return
	}
	if op.Status != StateReady {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("operation must be in state \"ready\" to select transactions (current state %q)", op.Status))
		return
	}

	if op.Mode == "selected" {
		h.selectRescan(w, r, op, req.TxIDs)
		return
	}

	snap := h.snapshot(op.ID)
	if snap == nil {
		writeError(w, http.StatusConflict, "no scan preview available for this operation")
		return
	}

	txIndex := make(map[string]int, len(snap.metas))
	for i, m := range snap.metas {
		txIndex[m.TxID] = i
	}

	var unknown []string
	for _, id := range req.TxIDs {
		if _, ok := txIndex[id]; !ok {
			unknown = append(unknown, id)
		}
	}
	if len(unknown) > 0 {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("transaction(s) not in scan preview: %s", strings.Join(unknown, ", ")))
		return
	}

	// Build the statements in the client's requested order; StmtIndex is a
	// global counter (the statements table is keyed by (op_id, stmt_index)).
	var stmts []Statement
	stmtIdx := 0
	for _, id := range req.TxIDs {
		wires := snap.sqlByTx[id]
		if len(wires) == 0 {
			writeError(w, http.StatusConflict,
				fmt.Sprintf("no SQL statements available for transaction %s (scan produced metadata only)", id))
			return
		}
		for _, w := range wires {
			stmts = append(stmts, Statement{
				TxIndex:   txIndex[id],
				StmtIndex: stmtIdx,
				SQL:       w.SQL,
				TxID:      w.TxID,
				TxOrder:   w.TxOrder,
				Warnings:  w.Warnings,
				Status:    "pending",
			})
			stmtIdx++
		}
	}

	// Persist the statements and the selection in one transaction: a failed
	// selection update rolls the statement writes back, so the store can never
	// hold statements without the selection that produced them.
	op.SelectedTxIDs = req.TxIDs
	op.UpdatedAt = time.Now()
	if err := h.opStore.SaveStatementsAndSelect(op.ID, stmts, req.TxIDs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operationId":    op.ID,
		"status":         op.Status,
		"selectedTxIds":  req.TxIDs,
		"statementCount": len(stmts),
	})
}

// selectRescan implements the second phase of a selected-mode operation: the
// operator picked transactions from the metadata preview, so a directed second
// scan (mode=selected, the original filter carrying the selected transaction
// IDs) is dispatched to generate the SQL statements the first (meta) scan never
// produced. The selection is persisted first (so a racing scan_done can save
// the staged statements), the operation moves ready -> scanning while the
// rescan runs, and scan_done returns it to ready. A transport failure or agent
// rejection leaves the operation ready and retryable (no state machine has a
// ready -> failed transition), matching the Execute path.
func (h *Handler) selectRescan(w http.ResponseWriter, r *http.Request, op *Operation, txIDs []string) {
	snap := h.snapshot(op.ID)
	if snap == nil {
		writeError(w, http.StatusConflict, "no scan preview available for this operation")
		return
	}

	// Every selected transaction must be in the metadata preview (the same
	// validation the sql-mode path applies).
	known := make(map[string]bool, len(snap.metas))
	for _, m := range snap.metas {
		known[m.TxID] = true
	}
	var unknown []string
	for _, id := range txIDs {
		if !known[id] {
			unknown = append(unknown, id)
		}
	}
	if len(unknown) > 0 {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("transaction(s) not in scan preview: %s", strings.Join(unknown, ", ")))
		return
	}

	// Persist the selection before the command goes out so a scan_done racing
	// the ready -> scanning transition knows which transactions to persist.
	op.SelectedTxIDs = txIDs
	op.UpdatedAt = time.Now()
	if err := h.opStore.Update(op); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	filter := binlogFilterToWire(op.Filter)
	filter.SelectedTxIDs = txIDs
	resp, err := h.sendCommand(r.Context(), op.AgentID, ws.Command{
		Cmd:  op.ID,
		Type: ws.CmdScan,
		Params: paramsFrom(ws.ScanRequest{
			Filter: filter,
			Mode:   "selected",
		}),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("send rescan: %v", err))
		return
	}
	if resp != nil && resp.Status == ws.StatusError {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("agent rejected rescan: %s", resp.Error))
		return
	}

	if ok, err := h.transitionTo(op, StateScanning); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusConflict, "operation state changed concurrently — reload and retry")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operationId":   op.ID,
		"status":        op.Status,
		"rescan":        true,
		"selectedTxIds": txIDs,
	})
}

// persistSelectedStatements saves the SQL staged by a selected-mode rescan for
// the recorded selection. It runs on scan_done (after the operation returns to
// ready): the preview's sqlByTx holds the statements emitted by the directed
// second scan, and they are persisted in the operator's requested order so
// Execute runs them exactly as selected. It is a no-op for modes that persist
// at Select time (sql) or that never record a selection. A missing preview or
// a transaction with no staged SQL is logged and skipped — the selection is
// persisted as far as the rescan produced statements for it.
func (h *Handler) persistSelectedStatements(op *Operation) {
	if op.Mode != "selected" || len(op.SelectedTxIDs) == 0 {
		return
	}
	snap := h.snapshot(op.ID)
	if snap == nil {
		log.Printf("pitr: op %s: selected-mode scan_done: no preview to save statements from", op.ID)
		return
	}
	txIndex := make(map[string]int, len(snap.metas))
	for i, m := range snap.metas {
		txIndex[m.TxID] = i
	}
	var stmts []Statement
	stmtIdx := 0
	for _, id := range op.SelectedTxIDs {
		wires := snap.sqlByTx[id]
		if len(wires) == 0 {
			log.Printf("pitr: op %s: selected-mode scan_done: no SQL staged for transaction %s", op.ID, id)
			continue
		}
		for _, w := range wires {
			stmts = append(stmts, Statement{
				TxIndex:   txIndex[id],
				StmtIndex: stmtIdx,
				SQL:       w.SQL,
				TxID:      w.TxID,
				TxOrder:   w.TxOrder,
				Warnings:  w.Warnings,
				Status:    "pending",
			})
			stmtIdx++
		}
	}
	if err := h.opStore.SaveStatements(op.ID, stmts); err != nil {
		log.Printf("pitr: op %s: selected-mode scan_done: save statements: %v", op.ID, err)
	}
}

// Execute issues CmdExecute with the persisted selected statements, moving a
// `ready` operation to `executing`.
//
// POST /api/pitr/{id}/execute   {"batchSize": 100}
func (h *Handler) Execute(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}
	if op.Status != StateReady {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("operation must be in state \"ready\" to execute (current state %q)", op.Status))
		return
	}

	var req struct {
		BatchSize int `json:"batchSize,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	h.sendExecute(w, r, op, req.BatchSize, StateExecuting)
}

// Resume re-issues CmdExecute for a paused operation, moving it back to
// `executing`.
//
// POST /api/pitr/{id}/resume
func (h *Handler) Resume(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}
	if op.Status != StatePaused {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("only paused operations can be resumed (current state %q)", op.Status))
		return
	}
	h.sendExecute(w, r, op, 0, StateExecuting)
}

// sendExecute loads the operation's persisted statements and issues CmdExecute
// to the agent, transitioning to `to` on acceptance or `failed` when the agent
// rejects the command.
func (h *Handler) sendExecute(w http.ResponseWriter, r *http.Request, op *Operation, batchSize int, to OperationState) {
	stmts, err := h.opStore.LoadStatements(op.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(stmts) == 0 {
		writeError(w, http.StatusConflict, "no selected statements to execute — select transactions first")
		return
	}

	execReq := ws.ExecuteRequest{
		OperationID: op.ID,
		Statements:  statementsToWire(stmts),
		BatchSize:   batchSize,
	}
	resp, err := h.sendCommand(r.Context(), op.AgentID, ws.Command{
		Cmd:    op.ID,
		Type:   ws.CmdExecute,
		Params: paramsFrom(execReq),
	})
	if err != nil {
		h.failOperation(op, err.Error())
		writeError(w, http.StatusBadGateway, fmt.Sprintf("send execute: %v", err))
		return
	}
	if resp != nil && resp.Status == ws.StatusError {
		h.failOperation(op, resp.Error)
		writeError(w, http.StatusBadGateway, fmt.Sprintf("agent rejected execute: %s", resp.Error))
		return
	}

	if ok, err := h.transitionTo(op, to); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	} else if !ok {
		// The CAS lost to a concurrent transition (e.g. op_done landed as done
		// while this command was in flight): answer 409 so the client reloads.
		writeError(w, http.StatusConflict, "operation state changed concurrently — reload and retry")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operationId":    op.ID,
		"status":         op.Status,
		"statementCount": len(stmts),
	})
}

// Pause suspends an executing operation: CmdCancel is dispatched to the agent
// and the local state moves to `paused` (resumable).
//
// POST /api/pitr/{id}/pause
func (h *Handler) Pause(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}
	if op.Status != StateExecuting {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("only executing operations can be paused (current state %q)", op.Status))
		return
	}

	resp, err := h.sendCommand(r.Context(), op.AgentID, ws.Command{
		Cmd:  op.ID,
		Type: ws.CmdCancel,
		Params: map[string]interface{}{
			"operationId": op.ID,
		},
	})
	if err != nil {
		// Transport failure: CmdCancel never reached the agent, which is still
		// executing. Failing the operation here would desync server and agent
		// state — the agent keeps running and its later op_done would be
		// rejected as a late event for a terminal operation. (Execute has the
		// same property, where the state machine already rejects ready ->
		// failed; executing -> failed is valid, so it must be suppressed
		// explicitly.) Keep the current state — executing, or paused if a
		// pause acknowledgement raced in — and answer 502 so the caller can
		// retry the pause.
		writeError(w, http.StatusBadGateway, fmt.Sprintf("send pause: %v", err))
		return
	}
	if resp != nil && resp.Status == ws.StatusError {
		h.failOperation(op, resp.Error)
		writeError(w, http.StatusBadGateway, fmt.Sprintf("agent rejected pause: %s", resp.Error))
		return
	}

	if ok, err := h.transitionTo(op, StatePaused); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusConflict, "operation state changed concurrently — reload and retry")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operationId": op.ID,
		"status":      op.Status,
	})
}

// Cancel terminates an operation in a cancellable state (scanning, ready, or
// paused). CmdCancel is dispatched to the agent best-effort: cancellation is
// an authoritative local transition, so an unreachable agent still leaves the
// operation cancelled (a stuck operation must remain cancellable).
//
// POST /api/pitr/{id}/cancel
func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}
	if !TransitionValid(op.Status, StateCancelled) {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("cannot cancel operation in state %q", op.Status))
		return
	}

	_, _ = h.sendCommand(r.Context(), op.AgentID, ws.Command{
		Cmd:  op.ID,
		Type: ws.CmdCancel,
		Params: map[string]interface{}{
			"operationId": op.ID,
		},
	})

	if ok, err := h.transitionTo(op, StateCancelled); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusConflict, "operation state changed concurrently — reload and retry")
		return
	}
	h.forgetPreview(op.ID)
	h.appendAudit(op, StateCancelled, emailFromRequest(r), "")

	writeJSON(w, http.StatusOK, cancelResponse{
		OperationID: op.ID,
		Status:      op.Status,
	})
}

// ---------- SSE ----------

// ssePollInterval is how often the SSE loop re-checks the operation state, so
// terminal transitions without an agent stream event (e.g. a local cancel) are
// still observed by connected clients.
const ssePollInterval = time.Second

// Events streams the operation's agent events as Server-Sent Events:
//
//	event: <kind>
//	data: <json>
//
// The loop terminates on a terminal event (op_done/op_error), on the client
// disconnecting, or when the operation reaches a terminal state. On exit the
// subscription is removed and the channel closed per the event bus contract
// (Unsubscribe before close — the bus holds the same lock as Publish, so no
// send can race the close).
//
// GET /api/pitr/{id}/events
func (h *Handler) Events(w http.ResponseWriter, r *http.Request) {
	op, ok := h.authorizeOp(w, r)
	if !ok {
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming responses not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// An operation that is already terminal has no live agent stream: emit a
	// synthetic terminal frame so the client does not hang.
	if IsTerminal(op.Status) {
		writeSSEFrame(w, syntheticTerminalEvent(op))
		flusher.Flush()
		return
	}

	ch := h.bus.Subscribe(op.ID)
	defer func() {
		h.bus.Unsubscribe(op.ID, ch)
		close(ch)
	}()
	flusher.Flush() // commit headers; also signals the subscription is live

	ticker := time.NewTicker(ssePollInterval)
	defer ticker.Stop()

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return // subscription closed
			}
			writeSSEFrame(w, ev)
			flusher.Flush()
			if isTerminalKind(ev.Kind) {
				return
			}
		case <-ticker.C:
			cur, err := h.opStore.Get(op.ID)
			if err != nil {
				continue
			}
			if IsTerminal(cur.Status) {
				writeSSEFrame(w, syntheticTerminalEvent(cur))
				flusher.Flush()
				return
			}
		case <-r.Context().Done():
			return // client disconnected
		}
	}
}

// writeSSEFrame writes one SSE frame: `event: <kind>\ndata: <json>\n\n`.
func writeSSEFrame(w http.ResponseWriter, ev ws.StreamEvent) {
	data := ev.Data
	if len(data) == 0 {
		data = json.RawMessage("{}")
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Kind, data)
}

// isTerminalKind reports whether a stream event kind ends the operation.
func isTerminalKind(kind string) bool {
	return kind == ws.EvOpDone || kind == ws.EvOpError
}

// syntheticTerminalEvent builds a terminal event for an operation that reached
// a terminal state without an agent notification (e.g. a local cancel), or was
// already terminal when the client subscribed. The payload carries the
// terminal state so the client can reconcile with /status; cancelled maps to
// op_done with {"status":"cancelled"} to disambiguate.
func syntheticTerminalEvent(op *Operation) ws.StreamEvent {
	kind := ws.EvOpDone
	switch op.Status {
	case StateFailed, StateBlocked:
		kind = ws.EvOpError
	}
	data, _ := json.Marshal(map[string]string{"status": string(op.Status)})
	return ws.StreamEvent{ID: op.ID, Kind: kind, Data: data}
}
