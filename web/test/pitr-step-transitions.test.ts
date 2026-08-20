/**
 * Executable verification for the wizard's idempotent step transitions. Run:
 *
 *   node --experimental-strip-types test/pitr-step-transitions.test.ts
 *
 * Reproduces the finding "step 5 still shows 已接收": the redesign replaced
 * the original idempotent `step = N` assignments with an incrementing
 * advanceStep(), so a fast operation whose op_done SSE event races the
 * startExecute advance fires the transition TWICE (4 → 5 → 6). At step 6 no
 * step branch matches: the summary falls into the scan-total branch and the
 * execution card stops rendering. resolveStepTransition restores idempotency:
 * explicit targets, no overshoot; target 3 is the only legal backward move
 * (selected-mode rescan).
 *
 * Note: intentionally no node:/process imports so the file stays
 * svelte-check-clean without @types/node (failures throw → non-zero exit).
 */
import { resolveStepTransition } from '../src/lib/pitr/scan-flow.ts';

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

console.log('=== the reported bug: duplicate advance must not overshoot ===');
check('op_done re-advance at step 5 stays 5', resolveStepTransition(5, 5) === 5);
check('double advance 4→5→5 stays 5', resolveStepTransition(5, 5) === 5);
check('late stale advance at 5 does not reach 6', resolveStepTransition(5, 5) !== 6);

console.log('=== forward transitions ===');
check('1 → 2', resolveStepTransition(1, 2) === 2);
check('2 → 3 (start scan)', resolveStepTransition(2, 3) === 3);
check('3 → 4 (scan done)', resolveStepTransition(3, 4) === 4);
check('4 → 5 (execute)', resolveStepTransition(4, 5) === 5);

console.log('=== selected-mode rescan: the only legal backward move ===');
check('4 → 3 (rescan) allowed', resolveStepTransition(4, 3) === 3);
check('duplicate rescan 3 → 3 stays 3', resolveStepTransition(3, 3) === 3);

console.log('=== stale backward targets are rejected ===');
check('late 4 after 5 rejected (stays 5)', resolveStepTransition(5, 4) === 5);
check('backward 5 → 2 rejected', resolveStepTransition(5, 2) === 5);
check('backward 4 → 2 rejected', resolveStepTransition(4, 2) === 4);

console.log('');
console.log(`${pass} passed, ${fail} failed`);
if (fail > 0) {
	throw new Error(`PITR step-transition verification FAILED (${fail} failing checks)`);
}
console.log('PITR step-transition verification PASSED');
