import { auth, logout } from '$lib/auth.svelte.js';
import { goto } from '$app/navigation';

/** API base path — the Go backend mounts everything under `/api`. */
const BASE_URL = '/api';

/** Public routes that must not receive a 401 → /login bounce. */
const PUBLIC_PATHS = ['/login', '/register'];

/**
 * Normalized API error. `message` carries the server-provided `{ error }`
 * body when present, otherwise a generic description.
 */
export class ApiError extends Error {
	readonly status: number;

	constructor(status: number, message: string) {
		super(message);
		this.name = 'ApiError';
		this.status = status;
	}
}

function isOnPublicPath(): boolean {
	if (typeof window === 'undefined') return false;
	return PUBLIC_PATHS.includes(window.location.pathname);
}

/** Drop the session and redirect to /login on 401 (token missing/expired). */
function handleUnauthorized() {
	logout();
	if (!isOnPublicPath()) {
		// `goto` keeps it inside the SPA router (no full page reload); used
		// instead of `window.location` so navigation stays client-side and
		// the back button doesn't loop through the guarded page.
		void goto('/login', { replaceState: true });
	}
}

/**
 * Fetch wrapper for the PITR API.
 *
 * - Prefixes `path` with `/api` and sends JSON (`Content-Type` only set when
 *   a body is present).
 * - Attaches `Authorization: Bearer <token>` from the auth store whenever a
 *   session exists.
 * - On 401: clears the session and redirects to /login (except while already
 *   on a public route, e.g. a failed login attempt).
 * - Non-2xx responses throw an `ApiError` with the server's `{ error }`
 *   message. 204/empty bodies resolve to `undefined`.
 */
export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
	const headers = new Headers(init.headers);
	if (init.body && !headers.has('Content-Type')) {
		headers.set('Content-Type', 'application/json');
	}
	if (auth.token) {
		headers.set('Authorization', `Bearer ${auth.token}`);
	}

	let res: Response;
	try {
		res = await fetch(`${BASE_URL}${path}`, { ...init, headers });
	} catch (err) {
		throw new ApiError(0, err instanceof Error ? err.message : 'network error');
	}

	if (res.status === 401) {
		handleUnauthorized();
		throw new ApiError(401, await readErrorMessage(res, 'Unauthorized'));
	}

	if (!res.ok) {
		throw new ApiError(res.status, await readErrorMessage(res, `HTTP ${res.status}`));
	}

	if (res.status === 204) return undefined as T;
	return (await res.json()) as T;
}

async function readErrorMessage(res: Response, fallback: string): Promise<string> {
	try {
		const data = await res.json();
		if (data && typeof data.error === 'string' && data.error) return data.error;
	} catch {
		// not JSON — fall through
	}
	return fallback;
}
