import { auth } from '$lib/auth.svelte.js';

/**
 * SSE client for `GET /api/pitr/{id}/events`.
 *
 * Native `EventSource` cannot send the `Authorization: Bearer` header the
 * backend requires (internal/server/auth/middleware.go), so this client speaks
 * the SSE wire protocol (text/event-stream frames) over a streaming `fetch`
 * instead — same event semantics, plus auth.
 *
 * Reconnect policy (mirrors EventSource auto-reconnect):
 * - Transient failures (network drop, server closing the stream without a
 *   terminal frame) retry with capped exponential backoff: 1s → 2s → 4s → 8s
 *   → 15s (cap), indefinitely, so a server or agent restart recovers without
 *   user action.
 * - HTTP errors (401/404/…) are fatal: no retry, `onerror(message, true)`.
 * - Terminal frames (`op_done` / `op_error`) dispatch to the handlers and then
 *   close the stream — no reconnect.
 *
 * Memory safety: `close()` aborts the in-flight request and clears timers;
 * callers MUST invoke it on component unmount (see the wizard/detail pages).
 * Handlers are never invoked after `close()`.
 *
 * Fallback note (tradeoff): while the stream is down, pages poll `/status` to
 * observe terminal transitions (e.g. an agent disconnect blocking the
 * operation) that a dead stream would otherwise hide.
 */

export const SSE_EVENT_KINDS = [
	'tx_meta',
	'sql',
	'scan_done',
	'progress',
	'op_paused',
	'op_done',
	'op_error'
] as const;

export type SSEEventKind = (typeof SSE_EVENT_KINDS)[number];

/** Kinds that end the operation — the stream closes after one of these. */
const TERMINAL_KINDS: ReadonlySet<SSEEventKind> = new Set(['op_done', 'op_error']);

export interface SSEHandlers {
	/** Fired when a stream connects (initial or after a successful retry). */
	onopen?: () => void;
	/**
	 * Fired on connection failures. `fatal=true` means no auto-reconnect
	 * happened (HTTP/auth error); `fatal=false` means a retry is scheduled.
	 */
	onerror?: (message: string, fatal: boolean) => void;
	/** Per-kind event dispatch; `data` is the JSON-parsed frame payload. */
	onkind: (kind: SSEEventKind, data: unknown) => void;
	/** Fired after the `onkind` dispatch of a terminal frame, stream closed. */
	onterminal?: (kind: SSEEventKind, data: unknown) => void;
}

export interface SSEClient {
	/** Abort the stream and cancel any pending reconnect. Idempotent. */
	close(): void;
}

const BASE_URL = '/api';
/** Backoff sequence for transient failures, in milliseconds. */
const RETRY_BACKOFF_MS = [1000, 2000, 4000, 8000, 15000];
/** Cap used after the sequence ends. */
const RETRY_CAP_MS = 15000;

/** Parse one SSE frame ("event: <kind>\ndata: <json>") into its parts. */
function parseFrame(frame: string): { kind: string; data: string } {
	let kind = '';
	const dataLines: string[] = [];
	for (const rawLine of frame.split('\n')) {
		const line = rawLine.replace(/\r$/, '');
		if (line.startsWith('event:')) {
			kind = line.slice('event:'.length).trim();
		} else if (line.startsWith('data:')) {
			dataLines.push(line.slice('data:'.length).trimStart());
		}
	}
	return { kind, data: dataLines.join('\n') };
}

export function connectSSE(opId: string, handlers: SSEHandlers): SSEClient {
	const url = `${BASE_URL}/pitr/${encodeURIComponent(opId)}/events`;

	let closed = false;
	let controller: AbortController | null = null;
	let retryTimer: ReturnType<typeof setTimeout> | undefined;
	let retryIndex = 0;
	let terminalSeen = false;

	function fail(message: string, fatal: boolean) {
		if (closed) return;
		handlers.onerror?.(message, fatal);
	}

	function scheduleRetry() {
		if (closed || terminalSeen) return;
		const delay = RETRY_BACKOFF_MS[Math.min(retryIndex, RETRY_BACKOFF_MS.length - 1)] ?? RETRY_CAP_MS;
		retryIndex++;
		retryTimer = setTimeout(() => {
			if (!closed && !terminalSeen) void connect();
		}, delay);
	}

	function deliver(kind: SSEEventKind, raw: string) {
		if (closed || terminalSeen) return;
		let data: unknown;
		try {
			data = raw ? JSON.parse(raw) : {};
		} catch {
			// Non-JSON payload (should not happen — the server always writes
			// JSON) — surface the raw text so callers can still show it.
			data = raw;
		}
		handlers.onkind(kind, data);
		if (TERMINAL_KINDS.has(kind)) {
			terminalSeen = true;
			handlers.onterminal?.(kind, data);
		}
	}

	async function connect() {
		if (closed || terminalSeen) return;
		retryTimer = undefined;

		controller = new AbortController();
		const signal = controller.signal;
		try {
			const res = await fetch(url, {
				headers: auth.token ? { Authorization: `Bearer ${auth.token}` } : {},
				signal
			});
			if (closed || terminalSeen) return;

			if (!res.ok) {
				// HTTP failure (401 = expired token, 404 = unknown op, …).
				// Retrying cannot help — report fatal and stop.
				fail(`SSE 连接失败（HTTP ${res.status}）`, true);
				return;
			}

			// The response headers only arrive with the first body chunk in
			// fetch streaming; treat reaching here as "open".
			handlers.onopen?.();
			retryIndex = 0; // a successful connect resets the backoff

			const reader = res.body?.getReader();
			if (!reader) {
				fail('SSE 响应无数据流', true);
				return;
			}
			const decoder = new TextDecoder();
			let buf = '';

			for (;;) {
				const { done, value } = await reader.read();
				if (done) break;
				buf += decoder.decode(value, { stream: true });
				let sep = buf.indexOf('\n\n');
				while (sep !== -1) {
					const frame = buf.slice(0, sep);
					buf = buf.slice(sep + 2);
					if (frame.trim()) {
						const { kind, data } = parseFrame(frame);
						if (kind) deliver(kind as SSEEventKind, data);
						if (terminalSeen) return;
					}
					sep = buf.indexOf('\n\n');
				}
			}

			// Stream ended without a terminal frame (server restart / proxy
			// timeout) — treat as a transient failure and reconnect, exactly
			// like EventSource's auto-reconnect.
			if (!terminalSeen && !closed) {
				scheduleRetry();
			}
		} catch (err) {
			if (closed || terminalSeen) return; // aborted by close()
			const msg = err instanceof Error ? err.message : String(err);
			fail(msg, false);
			scheduleRetry();
		}
	}

	void connect();

	return {
		close() {
			if (closed) return;
			closed = true;
			if (retryTimer !== undefined) clearTimeout(retryTimer);
			controller?.abort();
		}
	};
}
