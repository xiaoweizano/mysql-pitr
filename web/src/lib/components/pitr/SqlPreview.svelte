<script lang="ts">
	import { t } from '$lib/i18n.svelte.js';
	import type { SqlStatement, TxPreview } from '$lib/api/pitr.js';
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
	 */
	let {
		transactions,
		checked = $bindable([] as string[]),
		readonly = false,
		expandedByDefault = true
	}: {
		transactions: TxPreview[];
		checked?: string[];
		readonly?: boolean;
		expandedByDefault?: boolean;
	} = $props();

	const groups = $derived(transactions.filter((tx) => tx.sql && tx.sql.length > 0));

	/** Expanded state per txId; a Set mutated in place is not reactive, so we reassign. */
	let collapsed = $state<Set<string>>(new Set());

	function isOpen(txId: string): boolean {
		return expandedByDefault ? !collapsed.has(txId) : collapsed.has(txId);
	}

	function toggleCollapse(txId: string) {
		const next = new Set(collapsed);
		if (next.has(txId)) {
			next.delete(txId);
		} else {
			next.add(txId);
		}
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

	const checkedCount = $derived(checked.length);
	const totalSql = $derived(groups.reduce((n, tx) => n + (tx.sql?.length ?? 0), 0));
</script>

{#if !readonly}
	<div class="flex items-center justify-between gap-2 text-xs text-zinc-500">
		<span>
			{t('pitr.sql.checkedSummary', {
				checked: String(checkedCount),
				total: String(groups.length),
				sql: String(totalSql)
			})}
		</span>
	</div>
{/if}

<div class="mt-2 space-y-2">
	{#each groups as tx (tx.txId)}
		<div class="overflow-hidden rounded-lg border border-zinc-200">
			<div
				class="flex cursor-pointer items-center gap-2 bg-zinc-50 px-2.5 py-1.5 text-xs"
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
						class="accent-zinc-900"
						checked={txChecked(tx.txId)}
						onchange={() => toggleTx(tx.txId)}
						onclick={(e) => e.stopPropagation()}
					/>
				{/if}
				<span class="font-mono font-medium text-zinc-700" title={tx.txId}>
					{shortId(tx.txId)}
				</span>
				<span class="text-zinc-400">{formatDateTime(tx.commitTime)}</span>
				<span class="ml-auto flex items-center gap-2">
					{#if tx.rowCount > 0}
						<span class="text-zinc-500">{tx.rowCount} 行</span>
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
				<div class="border-t border-zinc-100">
					{#each tx.sql ?? [] as stmt, i (i)}
						<div class="flex items-start gap-2 border-b border-zinc-100 px-2.5 py-1.5 last:border-b-0">
							{#if !readonly}
								<input
									type="checkbox"
									class="mt-1.5 accent-zinc-900"
									checked={txChecked(tx.txId)}
									onchange={() => toggleTx(tx.txId)}
								/>
							{/if}
							<div class="min-w-0 flex-1">
								<pre
									class="overflow-x-auto whitespace-pre-wrap break-all font-mono text-xs leading-relaxed text-zinc-700"
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
		<p class="py-6 text-center text-sm text-zinc-400">{t('pitr.sql.empty')}</p>
	{/if}
</div>
