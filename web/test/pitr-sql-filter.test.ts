/**
 * Executable verification for the recovery-type SQL filter shared by the
 * wizard's default selection and the SqlPreview display. Run with:
 *
 *   node --experimental-strip-types test/pitr-sql-filter.test.ts
 *
 * Reproduces the finding "SQL preview shows 2 transactions but the execute
 * button says 33 (update_rollback)": the preview filters displayed groups by
 * recovery type, while initSqlChecked() defaulted to EVERY transaction with
 * SQL — silently selecting (and executing) transactions the operator never
 * saw. The default selection must apply the same filter as the display.
 *
 * Note: intentionally no node:/process imports so the file stays
 * svelte-check-clean without @types/node (failures throw → non-zero exit).
 */
import { stmtOpKind, txMatchesOpType } from '../src/lib/pitr/scan-flow.ts';

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

// Reverse-SQL statements: INSERT reverses a DELETE, DELETE reverses an
// INSERT, UPDATE reverses an UPDATE.
const TX_UPDATE_ONLY = { txId: 'A', sql: [{ sql: 'UPDATE t SET a=1 WHERE id=2' }] };
const TX_INSERT_ONLY = { txId: 'B', sql: [{ sql: 'INSERT INTO t VALUES (1)' }] };
const TX_DELETE_ONLY = { txId: 'C', sql: [{ sql: 'DELETE FROM t WHERE id=3' }] };
const TX_MIXED = { txId: 'D', sql: [{ sql: 'UPDATE t SET a=2 WHERE id=9' }, { sql: 'INSERT INTO t VALUES (5)' }] };
const TX_NO_SQL = { txId: 'E', sql: [] };

console.log('=== stmtOpKind: infer the original operation from reverse SQL ===');
check('reverse INSERT ⇒ original delete', stmtOpKind('INSERT INTO t VALUES (1)') === 'delete');
check('reverse DELETE ⇒ original insert', stmtOpKind('  DELETE FROM t WHERE id=3') === 'insert');
check('reverse UPDATE ⇒ original update', stmtOpKind('UPDATE t SET a=1 WHERE id=2') === 'update');
check('other verb ⇒ null', stmtOpKind('TRUNCATE t') === null);

console.log('=== update_rollback: only transactions carrying UPDATE statements match ===');
check('update-only tx matches', txMatchesOpType(TX_UPDATE_ONLY, 'update_rollback'));
check('mixed tx matches (contains UPDATE)', txMatchesOpType(TX_MIXED, 'update_rollback'));
check('insert-only tx does NOT match', !txMatchesOpType(TX_INSERT_ONLY, 'update_rollback'));
check('delete-only tx does NOT match', !txMatchesOpType(TX_DELETE_ONLY, 'update_rollback'));
check('empty-sql tx does NOT match', !txMatchesOpType(TX_NO_SQL, 'update_rollback'));

console.log('=== flashback: only transactions carrying reverse-INSERT match ===');
check('insert(reverse-of-DELETE) tx matches', txMatchesOpType(TX_INSERT_ONLY, 'flashback'));
check('update-only tx does NOT match', !txMatchesOpType(TX_UPDATE_ONLY, 'flashback'));

console.log('=== pitr / pitr_tx / gtid: every transaction with SQL matches ===');
check('pitr matches update tx', txMatchesOpType(TX_UPDATE_ONLY, 'pitr'));
check('pitr matches insert tx', txMatchesOpType(TX_INSERT_ONLY, 'pitr'));
check('pitr still requires sql', !txMatchesOpType(TX_NO_SQL, 'pitr'));

console.log('=== the reported scenario: 33 with SQL, only 2 relevant to update_rollback ===');
// 33 transactions with SQL, of which 2 contain UPDATE statements.
const many: { txId: string; sql: { sql: string }[] }[] = [
	TX_UPDATE_ONLY,
	{ txId: 'A2', sql: [{ sql: 'UPDATE t SET b=1 WHERE id=1' }] },
	...Array.from({ length: 31 }, (_, i) => ({ txId: `n${i}`, sql: [{ sql: 'INSERT INTO t VALUES (1)' }] }))
];
const defaults = many.filter((tx) => txMatchesOpType(tx, 'update_rollback')).map((tx) => tx.txId);
check(
	'default selection = 2 visible transactions, not 33',
	defaults.length === 2,
	`got ${defaults.length}: ${JSON.stringify(defaults)}`
);

console.log('');
console.log(`${pass} passed, ${fail} failed`);
if (fail > 0) {
	throw new Error(`PITR sql-filter verification FAILED (${fail} failing checks)`);
}
console.log('PITR sql-filter verification PASSED');
