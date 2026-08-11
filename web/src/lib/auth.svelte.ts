import { browser } from '$app/environment';

/**
 * Authenticated platform user (mirrors the Phase 3 `/api/auth` wire shape).
 * The backend currently emits `{ id, email, createdAt }`; `role` is declared
 * optional for forward-compatibility with a future roles model.
 */
export interface AuthUser {
	id: string;
	email: string;
	createdAt?: string;
	/** Not yet emitted by the backend — reserved for future roles. */
	role?: string;
}

const TOKEN_KEY = 'pitr.auth.token';
const USER_KEY = 'pitr.auth.user';

function readStored(): { token: string | null; user: AuthUser | null } {
	if (!browser) return { token: null, user: null };
	try {
		const token = localStorage.getItem(TOKEN_KEY);
		if (!token) return { token: null, user: null };
		const raw = localStorage.getItem(USER_KEY);
		const user = raw ? (JSON.parse(raw) as AuthUser) : null;
		return { token, user };
	} catch {
		// Corrupt or unreadable storage — treat as logged out and clear it.
		try {
			localStorage.removeItem(TOKEN_KEY);
			localStorage.removeItem(USER_KEY);
		} catch {
			// ignore
		}
		return { token: null, user: null };
	}
}

/**
 * Auth session store.
 *
 * Object-wrapped `$state` (not a bare binding): Svelte 5 forbids reassigning
 * an *exported* module-level `$state` binding (`state_invalid_export`), so
 * callers read `auth.token` / `auth.user` and mutate via `auth.login()` /
 * `auth.logout()` instead. Persisted to localStorage so the session survives
 * reloads in the pure-SPA (ssr=false) build.
 */
export const auth = $state<{ token: string | null; user: AuthUser | null }>(readStored());

/**
 * Reactive convenience flag — true when a token is present.
 *
 * Exported as a function (not `$derived`): Svelte 5 rejects *exported*
 * module-level derived state (`derived_invalid_export`). State reads are
 * tracked wherever they happen in runes mode, so `isLoggedIn()` stays
 * reactive inside `$effect`, `$derived`, and template expressions.
 */
export function isLoggedIn(): boolean {
	return Boolean(auth.token);
}

/**
 * Store a fresh session and persist it. `login` here is the store-level
 * session setter; the network `login()` lives in `$lib/api/auth.ts`.
 */
export function login(token: string, user: AuthUser) {
	auth.token = token;
	auth.user = user;
	if (!browser) return;
	try {
		localStorage.setItem(TOKEN_KEY, token);
		localStorage.setItem(USER_KEY, JSON.stringify(user));
	} catch {
		// Storage unavailable (e.g. private mode quota) — session stays in memory.
	}
}

/** Clear the session and persisted storage. */
export function logout() {
	auth.token = null;
	auth.user = null;
	if (!browser) return;
	try {
		localStorage.removeItem(TOKEN_KEY);
		localStorage.removeItem(USER_KEY);
	} catch {
		// ignore
	}
}
