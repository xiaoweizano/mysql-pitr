import { auth } from '$lib/auth.svelte.js';
import { ApiError, apiFetch } from './client.js';

/**
 * Audit entry wire model — aligned with the backend `AuditEntry` json tags
 * (internal/server/audit/store.go):
 *   operationId, operator, timestamp, orgId, agentId, targetTable,
 *   recoveryTime, rowsAffected, status, errorDetails (omitempty).
 */
export interface AuditEntry {
	operationId: string;
	operator: string;
	timestamp: string;
	orgId: string;
	agentId: string;
	targetTable: string;
	recoveryTime: string;
	rowsAffected: number;
	status: string;
	errorDetails?: string;
}

/**
 * Audit query filters. The backend requires `org_id`; every other field is
 * optional. `from`/`to` must be RFC3339 (see audit/handler.go).
 */
export interface AuditQuery {
	orgId: string;
	/** RFC3339 (UTC) lower bound on the entry timestamp. */
	from?: string;
	/** RFC3339 (UTC) upper bound on the entry timestamp. */
	to?: string;
	/** Operation state, e.g. 'done' | 'failed' | 'cancelled'. */
	status?: string;
	agentId?: string;
}

/**
 * Backend filter parameters use snake_case (`org_id`, `agent_id`) — see
 * audit/handler.go Query/Export.
 */
function auditQueryString(q: AuditQuery): string {
	const p = new URLSearchParams();
	p.set('org_id', q.orgId);
	if (q.from) p.set('from', q.from);
	if (q.to) p.set('to', q.to);
	if (q.status) p.set('status', q.status);
	if (q.agentId) p.set('agent_id', q.agentId);
	return p.toString();
}

/**
 * Query the audit log with filters.
 * `GET /api/audit?org_id=…&from=…&to=…&status=…&agent_id=…` → `AuditEntry[]`.
 * NOTE: the backend returns a bare JSON array (not a `{ entries }` envelope)
 * — the shape is matched in Query but the UI unwraps it directly here.
 */
export function queryAudit(q: AuditQuery): Promise<AuditEntry[]> {
	return apiFetch<AuditEntry[]>(`/audit?${auditQueryString(q)}`);
}

/**
 * Download the audit log as CSV with the current filters applied.
 *
 * The JWT lives in the Authorization header, so a plain `<a href>` link
 * cannot carry it — fetch the blob through an apiFetch-style request (with
 * the token attached) and trigger a download via an object URL instead.
 *
 * `GET /api/audit/export?org_id=…&…` → `text/csv` attachment.
 */
export async function exportAuditCsv(q: AuditQuery): Promise<void> {
	let res: Response;
	try {
		res = await fetch(`/api/audit/export?${auditQueryString(q)}`, {
			headers: auth.token ? { Authorization: `Bearer ${auth.token}` } : {}
		});
	} catch (err) {
		throw new ApiError(0, err instanceof Error ? err.message : 'network error');
	}
	if (!res.ok) {
		throw new ApiError(res.status, res.status === 401 ? 'Unauthorized' : `HTTP ${res.status}`);
	}
	const blob = await res.blob();
	const url = URL.createObjectURL(blob);
	const a = document.createElement('a');
	a.href = url;
	a.download = `audit_${q.orgId}.csv`;
	document.body.appendChild(a);
	a.click();
	a.remove();
	URL.revokeObjectURL(url);
}
