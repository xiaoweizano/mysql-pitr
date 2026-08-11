import { apiFetch } from './client.js';

/**
 * Agent wire model — field names aligned with the backend `AgentRecord`
 * json tags (internal/server/agent/store.go):
 *   id, orgId, hostname, mySQLVersion (omitempty), status,
 *   lastSeen, createdAt, certSerial (omitempty), registrationToken (omitempty), approved.
 * Note the backend's non-standard casing: `mySQLVersion` (not `mysqlVersion`).
 */
export type AgentStatus = 'online' | 'offline' | 'error';

export interface AgentInfo {
	id: string;
	orgId: string;
	hostname: string;
	/** Backend json tag is `mySQLVersion` — keep this exact casing. */
	mySQLVersion?: string;
	/** 'online' | 'offline' | 'error' */
	status: AgentStatus;
	lastSeen: string;
	createdAt: string;
	certSerial?: string;
	registrationToken?: string;
	approved: boolean;
}

/**
 * Archive loop state served by GET /api/agents/{id}/archive (camelCase wire
 * form, internal/server/agent/handler.go archiveStateResponse).
 */
export interface ArchiveState {
	/** Latest binlog file the agent is archiving, e.g. "binlog.000123". */
	lastFile: string;
	/** Byte offset within `lastFile`. */
	lastPos: number;
	/** GTID set position, when the source has GTID mode enabled. */
	lastGtid?: string;
	/** When the archive loop last reported this state (RFC3339). */
	updatedAt: string;
}

/**
 * List agents of one organisation.
 * `GET /api/agents?orgId=…` → `{ agents: AgentInfo[] }`.
 * The backend requires the orgId query parameter, so callers fetch the user's
 * orgs first (see `$lib/api/orgs.ts`) and call this once per org.
 */
export function listAgents(orgId: string): Promise<AgentInfo[]> {
	return apiFetch<{ agents: AgentInfo[] }>(`/agents?orgId=${encodeURIComponent(orgId)}`).then(
		(r) => r.agents
	);
}

/**
 * Fetch a single agent.
 * `GET /api/agents/{id}` → AgentInfo.
 */
export function getAgent(id: string): Promise<AgentInfo> {
	return apiFetch<AgentInfo>(`/agents/${encodeURIComponent(id)}`);
}

/**
 * Approve a pending agent (org admins only).
 * `POST /api/agents/{id}/approve` → `{ agent: AgentInfo }`.
 */
export function approveAgent(id: string): Promise<AgentInfo> {
	return apiFetch<{ agent: AgentInfo }>(`/agents/${encodeURIComponent(id)}/approve`, {
		method: 'POST'
	}).then((r) => r.agent);
}

/**
 * Reject (delete) an agent (org admins only).
 * `POST /api/agents/{id}/reject` → `{ success: true }` (204-style, body ignored).
 */
export function rejectAgent(id: string): Promise<void> {
	return apiFetch<void>(`/agents/${encodeURIComponent(id)}/reject`, { method: 'POST' });
}

/**
 * Fetch the live archive state of an agent by dispatching a command to the
 * connected agent.
 * `GET /api/agents/{id}/archive` → ArchiveState.
 * Throws ApiError(503, …) when the agent is offline — callers should surface
 * that as the "agent down" state, not as a generic failure.
 */
export function getAgentArchive(id: string): Promise<ArchiveState> {
	return apiFetch<ArchiveState>(`/agents/${encodeURIComponent(id)}/archive`);
}
