import { apiFetch } from './client.js';
import { login as setSession, type AuthUser } from '$lib/auth.svelte.js';

export interface LoginResult {
	token: string;
	refreshToken?: string;
	user: AuthUser;
}

export interface RegisterResult {
	token: string;
	user: AuthUser;
}

/**
 * Create a new account.
 * `POST /api/auth/register {email, password}` → 201 `{ token, user }`.
 * Does NOT start a session — the UI intentionally sends the user to /login
 * after a successful signup.
 */
export function register(email: string, password: string): Promise<RegisterResult> {
	return apiFetch<RegisterResult>('/auth/register', {
		method: 'POST',
		body: JSON.stringify({ email, password })
	});
}

/**
 * Authenticate and start a session.
 * `POST /api/auth/login {email, password}` → 200 `{ token, refreshToken, user }`.
 * On success the returned token/user are written into the auth store
 * (and localStorage) before the promise resolves.
 */
export async function login(email: string, password: string): Promise<LoginResult> {
	const result = await apiFetch<LoginResult>('/auth/login', {
		method: 'POST',
		body: JSON.stringify({ email, password })
	});
	setSession(result.token, result.user);
	return result;
}
