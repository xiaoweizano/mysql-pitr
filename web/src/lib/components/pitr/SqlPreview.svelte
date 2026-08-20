<script lang="ts">
	import { t } from '$lib/i18n.svelte.js';
	import type { PitrType, SqlStatement, TxPreview } from '$lib/api/pitr.js';
	import { stmtOpKind } from '$lib/pitr/scan-flow.js';
	import { formatDateTime, shortId } from '$lib/format.js';
	import { Check, AlertTriangle } from '@lucide/svelte';

	/**
	 * Reverse-SQL preview grouped by transaction, with per-transaction
	 * selection. Shared by the wizard step 4 and the operation detail page.
	 *
	 * Selection granularity is the TRANSACTION (matching the backend `select`
	 * API — statements are persisted per transaction): each transaction group
	 * carries one checkbox, and every SQL row of a checked transaction renders
	 * checked. Clicking any row (or the group's "全选事务" button) toggles the
	 * whole transaction, so a partial row selection can never silently execute
	 * SQL the operator did not intend.
	 *
	 * Props:
	 * - `transactions`: preview list; only entries with `sql` are rendered.
	 * - `checked`: bindable txId array of selected transactions.
	 * - `readonly`: hide all checkboxes (detail page / after execution).
	 * - `expandedByDefault`: render every group open (default true).
	 * - `opType`: recovery type for auto-filtering the displayed SQL.
	 *   flashback → only INSERT (reverse of DELETE);
	 *   update_rollback → only UPDATE;
	 *   pitr/pitr_tx/gtid → show all.
	 */
	let {
		transactions,
		checked = $bindable([] as string[]),
		readonly = false,
		expandedByDefault = true,
		opType = 'pitr'
	}: {
		transactions: TxPreview[];
		checked?: string[];
		readonly?: boolean;
		expandedByDefault?: boolean;
		opType?: PitrType;
	} = $props();

	/** Flatten transactions into individual stmts with their inferred op type.
	 * stmtOpKind is the shared rule — the wizard's default selection applies
	 * the same one via txMatchesOpType, so display and selection cannot
	 * diverge. */
	type TxStmt = { txId: string; commitTime: string; rowCount: number; stmt: SqlStatement; opType: string | null };
	const allStmts = $derived(
		transactions
			.filter((tx) => tx.sql && tx.sql.length > 0)
			.flatMap((tx) =>
				tx.sql!.map((s) => ({ txId: tx.txId, commitTime: tx.commitTime, rowCount: tx.rowCount, stmt: s, opType: stmtOpKind(s.sql) }))
			)
	);

	/** Filter by recovery type. */
	const filteredStmts = $derived.by(() => {
		if (opType === 'flashback') return allStmts.filter((s) => s.opType === 'delete');
		if (opType === 'update_rollback') return allStmts.filter((s) => s.opType === 'update');
		return allStmts; // pitr / pitr_tx / gtid → show all
	});

	/** Re-group filtered statements by transaction, sorted by commitTime descending. */
	const groups = $derived.by(() => {
		const byTx = new Map<string, TxStmt[]>();
		for (const s of filteredStmts) {
			const list = byTx.get(s.txId);
			if (list) list.push(s);
			else byTx.set(s.txId, [s]);
		}
		// Sort by commitTime descending, using the first stmt's time per tx.
		return [...byTx.entries()]
			.sort((a, b) => new Date(b[1][0].commitTime).getTime() - new Date(a[1][0].commitTime).getTime())
			.map(([txId, stmts]) => ({
				txId,
				commitTime: stmts[0].commitTime,
				rowCount: stmts[0].rowCount,
				sql: stmts.map((s) => s.stmt)
			}));
	});

	/** All txIds in the filtered group (for select-all / deselect-all). */
	const groupTxIds = $derived(groups.map((g) => g.txId));

	const allChecked = $derived(groupTxIds.length > 0 && groupTxIds.every((id) => checked.includes(id)));

	function toggleAll() {
		if (allChecked) {
			// 取消全选：清空所有勾选（被筛选掉的事务也不应残留）
			checked = [];
		} else {
			// 全选：只勾选当前可见的事务
			const set = new Set(checked);
			for (const id of groupTxIds) set.add(id);
			checked = [...set];
		}
	}

	/** Expanded state per txId; a Set mutated in place is not reactive, so we reassign. */
	let collapsed = $state<Set<string>>(new Set());

	function isOpen(txId: string): boolean {
		return expandedByDefault ? !collapsed.has(txId) : collapsed.has(txId);
	}

	function toggleCollapse(txId: string) {
		const next = new Set(collapsed);
		if (next.has(txId)) next.delete(txId);
		else next.add(txId);
		collapsed = next;
	}

	function toggleTx(txId: string) {
		checked = checked.includes(txId)
			? checked.filter((id) => id !== txId)
			: [...checked, txId];
	}

	/** "全选事务" — select all statements of a transaction (i.e. the tx). */
	function selectTxAll(txId: string) {
		if (!checked.includes(txId)) toggleTx(txId);
	}

	function txChecked(txId: string): boolean {
		return checked.includes(txId);
	}

	/** Count of checked transactions within the visible (filtered) group. */
	const checkedCount = $derived(groupTxIds.filter((id) => checked.includes(id)).length);
	const totalSql = $derived(groups.reduce((n, g) => n + (g.sql?.length ?? 0), 0));
</script>

{#if !readonly}
	<div class="flex items-center justify-between gap-2 text-xs text-muted-foreground">
		<span>
			{t('pitr.sql.checkedSummary', {
				checked: String(checkedCount),
				total: String(groups.length),
				sql: String(totalSql)
			})}
		</span>
		<button
			type="button"
			class="text-primary hover:underline disabled:cursor-not-allowed disabled:opacity-50"
			onclick={toggleAll}
			disabled={groups.length === 0}
		>
			{allChecked ? t('pitr.tx.uncheckAll') : t('pitr.tx.checkAll')}
		</button>
	</div>
{/if}

<div class="mt-2 space-y-2">
	{#each groups as tx (tx.txId)}
		<div class="overflow-hidden rounded-lg border border-border">
			<div
				class="flex cursor-pointer items-center gap-2 bg-muted/50 px-2.5 py-1.5 text-xs"
				role="button"
				tabindex="0"
				aria-expanded={isOpen(tx.txId)}
				onclick={() => toggleCollapse(tx.txId)}
				onkeydown={(e) => {
					if (e.key === 'Enter' || e.key === ' ') {
						e.preventDefault();
						toggleCollapse(tx.txId);
					}
				}}
			>
				{#if !readonly}
					<input
						type="checkbox"
						class="accent-primary"
						checked={txChecked(tx.txId)}
						onchange={() => toggleTx(tx.txId)}
						onclick={(e) => e.stopPropagation()}
					/>
				{/if}
				<span class="font-mono font-medium text-foreground/80" title={tx.txId}>
					{shortId(tx.txId)}
				</span>
				<span class="text-muted-foreground/70">{formatDateTime(tx.commitTime)}</span>
				<span class="ml-auto flex items-center gap-2">
					{#if tx.rowCount > 0}
						<span class="text-muted-foreground">{tx.rowCount} 行</span>
					{/if}
					{#if !readonly}
						<button
							type="button"
							class="inline-flex items-center gap-0.5 text-primary hover:underline"
							onclick={(e) => {
								e.stopPropagation();
								selectTxAll(tx.txId);
							}}
						>
							<Check class="size-3" />
							{t('pitr.sql.selectAllTx')}
						</button>
					{/if}
				</span>
			</div>

			{#if isOpen(tx.txId)}
				<div class="border-t border-border/50">
					{#each tx.sql ?? [] as stmt, i (i)}
						<div class="flex items-start gap-2 border-b border-border/50 px-2.5 py-1.5 last:border-b-0">
							{#if !readonly}
								<input
									type="checkbox"
									class="mt-1.5 accent-primary"
									checked={txChecked(tx.txId)}
									onchange={() => toggleTx(tx.txId)}
								/>
							{/if}
							<div class="min-w-0 flex-1">
								<pre
									class="overflow-x-auto whitespace-pre-wrap break-all font-mono text-xs leading-relaxed text-foreground/80"
								>{stmt.sql}</pre
								>
								{#if stmt.warnings && stmt.warnings.length > 0}
									<ul class="mt-1 space-y-0.5">
										{#each stmt.warnings as warning}
											<li class="flex items-start gap-1 text-[11px] text-amber-600">
												<AlertTriangle class="mt-0.5 size-3 shrink-0" />
												<span class="break-all">{warning}</span>
											</li>
										{/each}
									</ul>
								{/if}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/each}

	{#if groups.length === 0}
		<p class="py-6 text-center text-sm text-muted-foreground/70">{t('pitr.sql.empty')}</p>
	{/if}
</div>
