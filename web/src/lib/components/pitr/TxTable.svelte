<script lang="ts">
	import { t } from '$lib/i18n.svelte.js';
	import type { TxPreview } from '$lib/api/pitr.js';
	import { formatDateTime, shortId, txTablesLabel } from '$lib/format.js';

	/**
	 * Scanned-transaction table (streamed live from SSE tx_meta events, or a
	 * `/transactions` snapshot). Shared by the wizard step 3 and the operation
	 * detail page.
	 *
	 * Props:
	 * - `transactions`: ordered tx metadata list.
	 * - `selectable`: show the checkbox column (selection granularity is the
	 *   transaction — matching the backend select API).
	 * - `checked`: bindable txId array, only meaningful when `selectable`.
	 * - `previewTruncated`: server-side preview cap notice (MaxPreview).
	 */
	let {
		transactions,
		selectable = false,
		checked = $bindable([] as string[]),
		previewTruncated = false
	}: {
		transactions: TxPreview[];
		selectable?: boolean;
		checked?: string[];
		previewTruncated?: boolean;
	} = $props();

	const allChecked = $derived(
		transactions.length > 0 && transactions.every((tx) => checked.includes(tx.txId))
	);

	function toggleAll() {
		if (allChecked) {
			checked = [];
		} else {
			checked = transactions.map((tx) => tx.txId);
		}
	}

	function toggleTx(txId: string) {
		checked = checked.includes(txId)
			? checked.filter((id) => id !== txId)
			: [...checked, txId];
	}

	function badgeClass(kind: 'truncated' | 'gtid'): string {
		return kind === 'truncated'
			? 'bg-amber-100 text-amber-700'
			: 'bg-zinc-100 text-zinc-500';
	}
</script>

<div class="flex items-center justify-between gap-2 text-xs text-zinc-500">
	<span>
		{t('pitr.tx.count')}：<span class="font-semibold text-zinc-700">{transactions.length}</span>
	</span>
	{#if selectable}
		<button
			type="button"
			class="text-primary hover:underline disabled:cursor-not-allowed disabled:opacity-50"
			onclick={toggleAll}
			disabled={transactions.length === 0}
		>
			{allChecked ? t('pitr.tx.uncheckAll') : t('pitr.tx.checkAll')}
		</button>
	{/if}
</div>

{#if previewTruncated}
	<p class="mt-1 rounded-md bg-amber-50 px-2 py-1 text-xs text-amber-700">
		{t('pitr.tx.previewTruncated')}
	</p>
{/if}

<div class="mt-2 overflow-x-auto">
	<table class="w-full text-sm">
		<thead>
			<tr class="border-b text-left text-xs text-zinc-500">
				{#if selectable}
					<th class="w-8 px-2 py-1.5">
						<input
							type="checkbox"
							class="accent-zinc-900"
							checked={allChecked}
							onchange={toggleAll}
							disabled={transactions.length === 0}
						/>
					</th>
				{/if}
				<th class="px-2 py-1.5 font-medium">{t('pitr.tx.id')}</th>
				<th class="px-2 py-1.5 font-medium">{t('pitr.tx.commitTime')}</th>
				<th class="px-2 py-1.5 font-medium">{t('pitr.tx.tables')}</th>
				<th class="px-2 py-1.5 text-right font-medium">{t('pitr.tx.rows')}</th>
				<th class="px-2 py-1.5 font-medium"></th>
			</tr>
		</thead>
		<tbody>
			{#each transactions as tx (tx.txId)}
				<tr class="border-b border-zinc-100 align-middle">
					{#if selectable}
						<td class="px-2 py-1.5">
							<input
								type="checkbox"
								class="accent-zinc-900"
								checked={checked.includes(tx.txId)}
								onchange={() => toggleTx(tx.txId)}
							/>
						</td>
					{/if}
					<td class="max-w-44 px-2 py-1.5">
						<span class="block truncate font-mono text-xs text-zinc-700" title={tx.txId}>
							{shortId(tx.txId)}
						</span>
						{#if tx.gtid}
							<span class="block truncate font-mono text-[10px] text-zinc-400" title={tx.gtid}>
								{shortId(tx.gtid, 12, 8)}
							</span>
						{/if}
					</td>
					<td class="whitespace-nowrap px-2 py-1.5 text-zinc-600">
						{formatDateTime(tx.commitTime)}
					</td>
					<td class="max-w-56 truncate px-2 py-1.5 text-xs text-zinc-600" title={txTablesLabel(tx.tables)}>
						{txTablesLabel(tx.tables)}
					</td>
					<td class="whitespace-nowrap px-2 py-1.5 text-right font-mono text-xs text-zinc-700">
						{tx.rowCount}
					</td>
					<td class="whitespace-nowrap px-2 py-1.5 text-right">
						{#if tx.truncated}
							<span class={`inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium ${badgeClass('truncated')}`}>
								{t('pitr.tx.truncated')}
							</span>
						{/if}
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
	{#if transactions.length === 0}
		<p class="py-6 text-center text-sm text-zinc-400">{t('pitr.tx.empty')}</p>
	{/if}
</div>
