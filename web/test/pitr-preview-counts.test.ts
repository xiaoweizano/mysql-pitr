/**
 * Executable verification for the wizard's preview-count sync rule
 * (syncPreviewCounts in src/lib/pitr/scan-flow.ts). Run with:
 *
 *   node --experimental-strip-types test/pitr-preview-counts.test.ts
 *
 * Reproduces the finding "summary bar shows 已接收 0 个事务，0 条 SQL while
 * the table holds 1 transaction / 1 SQL":
 *
 * A fast scan (tiny archive) finishes during the startPITR round-trip, before
 * the SSE subscription lands. The backend event bus is non-blocking with no
 * replay (zero subscribers → discarded), so the live SSE counters stay 0 while
 * the 3s polling safety net advances the wizard from the authoritative
 * /transactions snapshot. refreshTransactions must therefore sync the counters
 * up from that snapshot (Math.max keeps a truncated snapshot from shrinking
 * counts that live events already grew past).
 *
 * Note: intentionally no node:/process imports so the file stays
 * svelte-check-clean without @types/node (failures throw → non-zero exit).
 */
import { syncPreviewCounts } from '../src/lib/pitr/scan-flow.ts';

let pass = 0;
let fail = 0;

function check(name: string, cond: boolean, detail?: string) {
	if (cond) {
		pass += 1;
		console.log(`  ok  - ${name}`);
	} else {
		fail += 1;
		console.error(`  FAIL - ${name}${detail ? `\n         ${detail}` : ''}`);
	}
}

console.log('=== missed-events scan: snapshot must lift the counters ===');
// The reported bug: 0/0 counters, snapshot holds 1 tx with 1 statement.
{
	const r = syncPreviewCounts(0, 0, [{ txId: 'A', sql: [{}] }]);
	check('tx count lifted 0 → 1', r.received === 1, JSON.stringify(r));
	check('sql count lifted 0 → 1', r.sqlReceived === 1, JSON.stringify(r));
}

console.log('=== happy path: live counters already match the snapshot ===');
{
	const txs = [
		{ txId: 'A', sql: [{}, {}] },
		{ txId: 'B', sql: [{}] }
	];
	const r = syncPreviewCounts(2, 3, txs);
	check('tx count stays 2', r.received === 2, JSON.stringify(r));
	check('sql count stays 3', r.sqlReceived === 3, JSON.stringify(r));
}

console.log('=== truncated snapshot: live counts must not shrink ===');
// preview cap (default 500) truncates the snapshot while live events counted
// more — Math.max keeps the larger, truthful count.
{
	const mkTx = (id: string): { txId: string; sql: unknown[] } => ({ txId: id, sql: [{}] });
	const txs = Array.from({ length: 500 }, (_, i) => mkTx(`t${i}`));
	const r = syncPreviewCounts(600, 1200, txs);
	check('tx count stays 600 (not shrunk to 500)', r.received === 600, JSON.stringify(r));
	check('sql count stays 1200', r.sqlReceived === 1200, JSON.stringify(r));
}

console.log('=== empty snapshot: counters preserved ===');
{
	const r = syncPreviewCounts(3, 5, []);
	check('tx count stays 3', r.received === 3, JSON.stringify(r));
	check('sql count stays 5', r.sqlReceived === 5, JSON.stringify(r));
}

console.log('=== tx without sql array tolerated ===');
{
	const metaOnly = { txId: 'A', sql: undefined };
	const r = syncPreviewCounts(0, 0, [metaOnly]);
	check('tx count 1, sql count 0', r.received === 1 && r.sqlReceived === 0, JSON.stringify(r));
}

console.log('');
console.log(`${pass} passed, ${fail} failed`);
if (fail > 0) {
	throw new Error(`PITR preview-counts verification FAILED (${fail} failing checks)`);
}
console.log('PITR preview-counts verification PASSED');
