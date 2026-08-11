/**
 * Executable verification for the wizard's rescan state machine
 * (src/lib/pitr/scan-flow.ts). Run with:
 *
 *   node --experimental-strip-types test/pitr-scan-flow.test.ts
 *
 * Simulates the reproduction chain behind the review finding "Critical 1":
 *
 *   step 3 check [A,B,C] → select → scan_done#1 (sqlChecked = [A,B,C])
 *   → step 4 user unchecks B → execute → selectTx([A,C]) → rescan#2
 *   → scan_done#2 → assert sqlChecked STAYS [A,C] (not rebuilt to [A,B,C])
 *   and the follow-up execute uses [A,C].
 *
 * Note: intentionally no node:/process imports so the file stays
 * svelte-check-clean without @types/node (failures throw → non-zero exit).
 */
import { decideScanDone, selectionChanged, sameSelection } from '../src/lib/pitr/scan-flow.ts';

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

// ---- simulated wizard state (mirrors the variables in +page.svelte) ----
let rescanning = false;
let pendingExecute = false;
let sqlChecked: string[] = [];
let selected: string[] = [];
let lastSelected: string[] | null = null;
const selectCalls: string[][] = [];
const executeCalls: string[][] = [];

const txs = [
	{ txId: 'A', sql: [{}] },
	{ txId: 'B', sql: [{}] },
	{ txId: 'C', sql: [{}] }
];
const withSql = txs.map((t) => t.txId);

// Same default-checking as the wizard's initSqlChecked (selected mode).
function initSqlChecked() {
	sqlChecked = [...selected.filter((id) => withSql.includes(id))];
}

console.log('=== Critical 1: selected mode, uncheck B in step 4, execute ===');

// step 3 first scan completes in selected mode → park (no rescan yet).
const d0 = decideScanDone('selected', false, false);
check('first scan_done (selected, no rescan) parks on step 3', !d0.initSqlChecked && !d0.advanceStep && !d0.continueExecute);

// user checks [A,B,C] and clicks select
selected = ['A', 'B', 'C'];
lastSelected = [...selected];
rescanning = true;

// rescan#1 completes → initial select's rescan initializes the checkboxes
const d1 = decideScanDone('selected', rescanning, pendingExecute);
check('rescan#1 (initial select) initializes checkboxes + advances', d1.initSqlChecked && d1.advanceStep && !d1.continueExecute);
if (d1.initSqlChecked) initSqlChecked();
rescanning = false;
check('sqlChecked initialized from step-3 selection = [A,B,C]', sameSelection(sqlChecked, ['A', 'B', 'C']), JSON.stringify(sqlChecked));

// step 4: user unchecks B → [A,C], clicks execute
sqlChecked = ['A', 'C'];
const changed = selectionChanged(lastSelected, sqlChecked);
check('execute sees selection changed vs persisted [A,B,C]', changed);
if (changed) {
	selectCalls.push([...sqlChecked]); // selectTx([A,C])
	lastSelected = [...sqlChecked];
	pendingExecute = true;
	rescanning = true;
}
check('selectTx received [A,C] (not [A,B,C])', JSON.stringify(selectCalls.at(-1)) === JSON.stringify(['A', 'C']), JSON.stringify(selectCalls.at(-1)));

// rescan#2 completes → THE FIX: must NOT rebuild the checkboxes
const d2 = decideScanDone('selected', rescanning, pendingExecute);
check('rescan#2 (pendingExecute) does NOT rebuild checkboxes', !d2.initSqlChecked, JSON.stringify(d2));
if (d2.initSqlChecked) initSqlChecked(); // the buggy build would execute this line
rescanning = false;
check('sqlChecked preserved after rescan#2 = [A,C]', sameSelection(sqlChecked, ['A', 'C']), JSON.stringify(sqlChecked));
if (d2.continueExecute) {
	pendingExecute = false;
	if (!selectionChanged(lastSelected, sqlChecked)) executeCalls.push([...sqlChecked]); // executeOp
}
check('continue-execute uses [A,C] (user deselection honored)', JSON.stringify(executeCalls.at(-1)) === JSON.stringify(['A', 'C']), JSON.stringify(executeCalls.at(-1)));

console.log('=== sql mode: single scan initializes + advances ===');
const ds = decideScanDone('sql', false, false);
check('sql scan_done initializes checkboxes + advances', ds.initSqlChecked && ds.advanceStep && !ds.continueExecute);

console.log('=== selectionChanged edge cases ===');
check('null persisted selection always reselects', selectionChanged(null, ['A']));
check('same selection (any order) does NOT reselect', !selectionChanged(['A', 'C'], ['C', 'A']));
check('different selection reselects', selectionChanged(['A', 'C'], ['A', 'B']));

console.log('');
console.log(`${pass} passed, ${fail} failed`);
if (fail > 0) {
	throw new Error(`PITR scan-flow verification FAILED (${fail} failing checks)`);
}
console.log('PITR scan-flow verification PASSED');
