import { apiFetch } from './client.js';

/**
 * Organisation wire model — aligned with the backend `Organization` json tags
 * (internal/server/org/store.go): id, name, createdAt.
 */
export interface OrgInfo {
	id: string;
	name: string;
	createdAt: string;
}

/**
 * List organisations the current user belongs to.
 * `GET /api/orgs` → `{ organizations: OrgInfo[] }`.
 *
 * Needed by the instances page because `GET /api/agents` requires an `orgId`
 * query parameter — callers list orgs, then query agents per org.
 */
export function listOrgs(): Promise<OrgInfo[]> {
	return apiFetch<{ organizations: OrgInfo[] }>('/orgs').then((r) => r.organizations);
}
