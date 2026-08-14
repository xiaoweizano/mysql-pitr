import { apiFetch } from './client.js';

/**
 * PITR operation wire types — field names aligned with the backend json tags
 * (internal/server/pitr/handler.go, model.go and internal/ws/types.go).
 * Everything here is the raw wire form; the wizard/detail pages map these into
 * their own UI state.
 */

/** Operation kind: flashback | update_rollback | pitr | tx_rollback. */
export type PitrType = 'flashback' | 'update_rollback' | 'pitr' | 'tx_rollback';

/** Scan granularity: meta (tx list only) | sql (tx + statements) | selected (two-phase). */
export type PitrMode = 'meta' | 'sql' | 'selected';

/**
 * Operation state machine states (internal/server/pitr/state.go):
 * created → scanning → ready → executing ⇄ paused; terminal: done | failed |
 * cancelled | blocked.
 */
export type PitrState =
	| 'created'
	| 'scanning'
	| 'ready'
	| 'executing'
	| 'paused'
	| 'done'
	| 'failed'
	| 'blocked'
	| 'cancelled';

export const PITR_TERMINAL_STATES: ReadonlySet<PitrState> = new Set([
	'done',
	'failed',
	'cancelled',
	'blocked'
]);

export function isTerminalState(status: PitrState): boolean {
	return PITR_TERMINAL_STATES.has(status);
}

/** One table reference in the wire ScanFilter (lowercase keys). */
export interface ScanTableRef {
	schema: string;
	table: string;
}

/**
 * Scan filter wire shape (internal/ws/types.go ScanFilter). `tables` empty
 * means "all tables". Times are RFC3339 strings.
 */
export interface ScanFilter {
	tables?: ScanTableRef[];
	/** RFC3339, optional. */
	timeStart?: string;
	/** RFC3339, optional. */
	timeEnd?: string;
	/** MySQL/MariaDB GTID set, e.g. "uuid:1-100". */
	gtidSet?: string;
	startFile?: string;
	startPos?: number;
	endFile?: string;
	endPos?: number;
	/** Per-transaction row ceiling (0 = agent default). */
	maxRowsPerTx?: number;
	selectedTxIds?: string[];
}

/** One reverse-SQL statement staged for a transaction (ws.StatementWire). */
export interface SqlStatement {
	sql: string;
	txId: string;
	txOrder: number;
	warnings?: string[];
}

/** Scanned transaction metadata (handler.go txMetaWire) plus staged SQL. */
export interface TxPreview {
	txId: string;
	gtid?: string;
	xid?: number;
	commitTime: string;
	schema?: string;
	tables?: ScanTableRef[];
	rowCount: number;
	/** True when the transaction itself was truncated (row cap). */
	truncated?: boolean;
	/** Staged statements, present in sql/selected modes. */
	sql?: SqlStatement[];
}

/** POST /api/pitr/start → { operationId, status }. */
export interface StartResponse {
	operationId: string;
	status: PitrState;
}

/** GET /api/pitr/{id}/status → statusResponse. */
export interface OperationStatus {
	id: string;
	orgId: string;
	agentId: string;
	type: PitrType;
	mode: PitrMode;
	status: PitrState;
	createdBy: string;
	createdAt: string;
	updatedAt: string;
	/** True when the in-memory preview was cut short by MaxPreview. */
	previewTruncated?: boolean;
	/** Statements executed (from checkpoint). Present for terminal operations. */
	checkpointDone?: number;
	/** Total statements (from checkpoint). Present for terminal operations. */
	checkpointTotal?: number;
}

/** GET /api/pitr/{id}/transactions. */
export interface TransactionsResponse {
	operationId: string;
	status: PitrState;
	transactions: TxPreview[];
	previewTruncated?: boolean;
}

/** POST /api/pitr/{id}/select. `statementCount` (sql mode) and `rescan` (selected mode) are mutually exclusive. */
export interface SelectResponse {
	operationId: string;
	status: PitrState;
	statementCount?: number;
	rescan?: boolean;
	selectedTxIds?: string[];
}

/** POST /api/pitr/{id}/execute|resume. */
export interface ExecuteResponse {
	operationId: string;
	status: PitrState;
	statementCount: number;
}

/** POST /api/pitr/{id}/pause|cancel. */
export interface ControlResponse {
	operationId: string;
	status: PitrState;
}

/**
 * One row of the operations list (GET /api/pitr). `filter` is the lowercase-key
 * ScanFilter DTO (Task 5) — never the internal binlog.Filter shape.
 */
export interface OperationListItem {
	id: string;
	orgId: string;
	agentId: string;
	type: PitrType;
	mode: PitrMode;
	filter: ScanFilter;
	status: PitrState;
	createdBy: string;
	createdAt: string;
	updatedAt: string;
	selectedTxIds?: string[];
	/** Live agent-connection state (server-side check). */
	agentConnected: boolean;
}

// ---------- SSE payloads ----------

/** Executor per-statement failure (internal/executor/types.go ExecError). */
export interface ExecError {
	/** Index into the executed statement list (1-based display in the UI). */
	statement: number;
	/** The failing SQL, when the executor included it. */
	sql?: string;
	err: string;
}

/** Executor progress snapshot (Progress). */
export interface ProgressPayload {
	done: number;
	total: number;
	lastTxId?: string;
	lastSql?: string;
	errors?: ExecError[];
}

/** op_done payload: executor FinalReport, or a synthetic { status } for local cancels. */
export interface OpDonePayload {
	done: number;
	total: number;
	/** true = a pause acknowledgement, NOT a terminal completion. */
	paused?: boolean;
	errors?: ExecError[];
	/** Present on synthetic frames (e.g. {"status":"cancelled"}). */
	status?: string;
}

/** op_error payload: a plain error string, or a synthetic { status } object. */
export interface OpErrorPayload {
	message: string;
	status?: string;
}

/** op_paused payload (server-confirmed pause). */
export interface OpPausedPayload {
	status: string;
	done: number;
	total: number;
}

// ---------- REST calls ----------

/**
 * Create an operation and launch the agent scan.
 * `POST /api/pitr/start` → { operationId, status } (status is "scanning").
 * Throws ApiError(409, …) when the agent is offline.
 */
export function startPITR(body: {
	agentId: string;
	type: PitrType;
	mode: PitrMode;
	filter: ScanFilter;
	maxPreview?: number;
}): Promise<StartResponse> {
	return apiFetch<StartResponse>('/pitr/start', {
		method: 'POST',
		body: JSON.stringify(body)
	});
}

/** `GET /api/pitr/{id}/status`. */
export function getStatus(id: string): Promise<OperationStatus> {
	return apiFetch<OperationStatus>(`/pitr/${encodeURIComponent(id)}/status`);
}

/**
 * `GET /api/pitr/{id}/transactions` — the in-memory scan preview (partial
 * while the scan still runs; empty once the operation reached a terminal
 * state and the preview was released).
 */
export function getTransactions(id: string): Promise<TransactionsResponse> {
	return apiFetch<TransactionsResponse>(`/pitr/${encodeURIComponent(id)}/transactions`);
}

/**
 * Select transactions. In sql mode this persists the statements locally and
 * returns `{ statementCount }`. In selected mode it dispatches a directed
 * second scan and returns `{ status: "scanning", rescan: true }` — the caller
 * then waits for scan_done before executing.
 * `POST /api/pitr/{id}/select` {"txIds": [...]}
 */
export function selectTx(id: string, txIds: string[]): Promise<SelectResponse> {
	return apiFetch<SelectResponse>(`/pitr/${encodeURIComponent(id)}/select`, {
		method: 'POST',
		body: JSON.stringify({ txIds })
	});
}

/** `POST /api/pitr/{id}/execute` {"batchSize": n}. */
export function executeOp(id: string, batchSize?: number): Promise<ExecuteResponse> {
	return apiFetch<ExecuteResponse>(`/pitr/${encodeURIComponent(id)}/execute`, {
		method: 'POST',
		body: JSON.stringify({ batchSize: batchSize ?? undefined })
	});
}

/** `POST /api/pitr/{id}/resume` — resume a paused execution. */
export function resumeOp(id: string): Promise<ExecuteResponse> {
	return apiFetch<ExecuteResponse>(`/pitr/${encodeURIComponent(id)}/resume`, {
		method: 'POST'
	});
}

/** `POST /api/pitr/{id}/pause`. */
export function pauseOp(id: string): Promise<ControlResponse> {
	return apiFetch<ControlResponse>(`/pitr/${encodeURIComponent(id)}/pause`, {
		method: 'POST'
	});
}

/** `POST /api/pitr/{id}/cancel` (scanning/ready/paused → cancelled). */
export function cancelOp(id: string): Promise<ControlResponse> {
	return apiFetch<ControlResponse>(`/pitr/${encodeURIComponent(id)}/cancel`, {
		method: 'POST'
	});
}

/**
 * List the operations of one organisation.
 * `GET /api/pitr?org_id=X` → { operations: OperationListItem[] }.
 * The backend requires the org_id query parameter, so pages fetch the user's
 * orgs first (see `$lib/api/orgs.ts`) and call this once per org.
 */
export function listOperations(orgId: string): Promise<OperationListItem[]> {
	return apiFetch<{ operations: OperationListItem[] }>(
		`/pitr?org_id=${encodeURIComponent(orgId)}`
	).then((r) => r.operations);
}
