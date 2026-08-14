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

/**
 * Organisation member wire model — aligned with the backend `Member` json tags
 * (internal/server/org/store.go): userId, orgId, role, joinedAt.
 */
export interface OrgMember {
	userId: string;
	orgId: string;
	/** 'admin' | 'member' */
	role: 'admin' | 'member';
	joinedAt: string;
}

/**
 * Create a new organisation; the requesting user becomes its admin.
 * `POST /api/orgs` `{ name }` → 201 `{ organization: OrgInfo }`.
 */
export function createOrg(name: string): Promise<OrgInfo> {
	return apiFetch<{ organization: OrgInfo }>('/orgs', {
		method: 'POST',
		body: JSON.stringify({ name })
	}).then((r) => r.organization);
}

/**
 * List members of an organisation.
 * `GET /api/orgs/{id}/members` → `{ members: OrgMember[] }`.
 */
export function listMembers(orgId: string): Promise<OrgMember[]> {
	return apiFetch<{ members: OrgMember[] }>(`/orgs/${encodeURIComponent(orgId)}/members`).then(
		(r) => r.members
	);
}

/**
 * Create an invitation code for an organisation (org admins only).
 * `POST /api/orgs/{id}/invite` → 201 `{ code }`.
 *
 * NOTE: the backend invite endpoint takes no request body — there is no email
 * field; the generated code is what gets shared with the invitee.
 */
export function inviteMember(orgId: string): Promise<string> {
	return apiFetch<{ code: string }>(`/orgs/${encodeURIComponent(orgId)}/invite`, {
		method: 'POST'
	}).then((r) => r.code);
}

/**
 * Accept an invitation and join the organisation as a member.
 * `POST /api/orgs/{id}/accept` `{ code }` → 200 `{ organization: OrgInfo }`.
 */
export function acceptInvite(orgId: string, code: string): Promise<OrgInfo> {
	return apiFetch<{ organization: OrgInfo }>(`/orgs/${encodeURIComponent(orgId)}/accept`, {
		method: 'POST',
		body: JSON.stringify({ code })
	}).then((r) => r.organization);
}

/**
 * Delete an organisation (admin only).
 * `DELETE /api/orgs/{id}` → 200 `{ success: true }`.
 */
export function deleteOrg(orgId: string): Promise<void> {
	return apiFetch<void>(`/orgs/${encodeURIComponent(orgId)}`, {
		method: 'DELETE'
	});
}
