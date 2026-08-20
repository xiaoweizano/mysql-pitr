/**
 * Pure decision functions for the wizard's scan/execute state machine.
 *
 * The two-phase selected-mode flow (step 3 selection → select → rescan →
 * step 4 → execute) is the trickiest state machine in the wizard. Keeping the
 * decisions here — free of DOM / SSE / fetch concerns — makes the flow
 * verifiable (see test/pitr-scan-flow.test.ts) and keeps +page.svelte as pure
 * wiring that applies these decisions to its state.
 */

/** Wizard mode: sql (single scan) | selected (two-phase rescan). */
export type ScanFlowMode = 'sql' | 'selected';

/** What the wizard must do when a scan finishes — whether that is the SSE
 * `scan_done` event or the /status polling safety net discovering `ready`
 * after the frame was dropped / never replayed. */
export interface ScanDoneDecision {
	/** Re-initialize the step-4 checkboxes from the step-3 selection / staged SQL. */
	initSqlChecked: boolean;
	/** Move the wizard to step 4. */
	advanceStep: boolean;
	/** Continue the pending execute chain (call startExecute again). */
	continueExecute: boolean;
}

export function decideScanDone(
	mode: ScanFlowMode,
	rescanning: boolean,
	pendingExecute: boolean
): ScanDoneDecision {
	if (mode === 'sql') {
		// Single scan: the SQL arrived with the scan — initialize the checkboxes
		// and proceed to the preview.
		return { initSqlChecked: true, advanceStep: true, continueExecute: false };
	}
	if (rescanning) {
		// Two-phase rescan. A rescan triggered by the step-4 execute button
		// (pendingExecute) must NOT rebuild the checkboxes from the step-3
		// selection — that would resurrect transactions the user just
		// unchecked. Only the initial step-3 select's rescan initializes them.
		return {
			initSqlChecked: !pendingExecute,
			advanceStep: true,
			continueExecute: pendingExecute
		};
	}
	// First scan in selected mode: park on step 3 for the user to select.
	return { initSqlChecked: false, advanceStep: false, continueExecute: false };
}

/** True when the persisted selection differs from the current checkboxes. */
export function selectionChanged(persisted: string[] | null, current: string[]): boolean {
	return persisted === null || !sameSelection(persisted, current);
}

/** Order-insensitive comparison of two transaction-id selections. */
export function sameSelection(a: string[], b: string[]): boolean {
	if (a.length !== b.length) return false;
	const key = (xs: string[]) => [...xs].sort().join('|');
	return key(a) === key(b);
}

/**
 * Sync the live SSE counters up from the authoritative /transactions snapshot.
 *
 * The live counters (received / sqlReceived) only grow on tx_meta / sql SSE
 * events, and the backend event bus is non-blocking with no replay: a scan
 * that finishes before the SSE subscription lands publishes to zero
 * subscribers and the events are discarded. The polling safety net then
 * advances the wizard from the snapshot, so the snapshot — never the live
 * stream — is the only source the counters can trust after any refresh.
 * Math.max keeps the larger side: a snapshot truncated at the preview cap
 * (default 500) must not shrink counts that live events already grew past.
 */
export function syncPreviewCounts<T extends { sql?: unknown[] }>(
	received: number,
	sqlReceived: number,
	txs: T[]
): { received: number; sqlReceived: number } {
	let sqlTotal = 0;
	for (const tx of txs) {
		if (tx.sql) sqlTotal += tx.sql.length;
	}
	return {
		received: Math.max(received, txs.length),
		sqlReceived: Math.max(sqlReceived, sqlTotal)
	};
}
