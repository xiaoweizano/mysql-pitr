<script lang="ts">
	import { t } from '$lib/i18n.svelte.js';
	import type { TxPreview } from '$lib/api/pitr.js';
	import { formatDateTime, shortId, txTablesLabel } from '$lib/format.js';

	/**
	 * Scanned-transaction table with operation-type filtering.
	 *
	 * The filter only affects display — checked state and execution are
	 * independent of the filter. Operation types are inferred from the
	 * reverse SQL: INSERT→原DELETE, DELETE→原INSERT, UPDATE→原UPDATE.
	 */

	type OpType = 'delete' | 'insert' | 'update';

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

	/** Infer original operation types from reverse SQL statements. */
	function txOpTypes(tx: TxPreview): Set<OpType> {
		const types = new Set<OpType>();
		if (!tx.sql || tx.sql.length === 0) return types;
		for (const s of tx.sql) {
			const sql = s.sql.trimStart().toUpperCase();
			if (sql.startsWith('INSERT')) types.add('delete'); // reverse of DELETE
			else if (sql.startsWith('DELETE')) types.add('insert'); // reverse of INSERT
			else if (sql.startsWith('UPDATE')) types.add('update'); // reverse of UPDATE
		}
		return types;
	}

	/** Active filter types. Empty = show all. */
	let activeFilters = $state<Set<OpType>>(new Set());

	function toggleFilter(t: OpType) {
		const next = new Set(activeFilters);
		if (next.has(t)) next.delete(t);
		else next.add(t);
		activeFilters = next;
	}

	/** Count of transactions containing each operation type. */
	const opTypeCounts = $derived(() => {
		const counts: Record<OpType, number> = { delete: 0, insert: 0, update: 0 };
		for (const tx of transactions) {
			for (const t of txOpTypes(tx)) {
				counts[t]++;
			}
		}
		return counts;
	});

	/** Whether any SQL data is available (sql/selected mode). */
	const hasSqlData = $derived(transactions.some((tx) => tx.sql && tx.sql.length > 0));

	/** Transactions sorted by commit time descending (newest first). */
	const sortedTx = $derived(
		[...transactions].sort(
			(a, b) => new Date(b.commitTime).getTime() - new Date(a.commitTime).getTime()
		)
	);

	/** Filtered transactions based on active op-type filters. */
	const displayedTx = $derived(
		activeFilters.size === 0
			? sortedTx
			: sortedTx.filter((tx) => {
					const types = txOpTypes(tx);
					for (const f of activeFilters) {
						if (types.has(f)) return true;
					}
					return false;
				})
	);

	const allChecked = $derived(
		displayedTx.length > 0 && displayedTx.every((tx) => checked.includes(tx.txId))
	);

	function toggleAll() {
		if (allChecked) {
			const displayedIds = new Set(displayedTx.map((tx) => tx.txId));
			checked = checked.filter((id) => !displayedIds.has(id));
		} else {
			const newChecked = new Set(checked);
			for (const tx of displayedTx) {
				newChecked.add(tx.txId);
			}
			checked = [...newChecked];
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

	const filterBtnBase = 'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium transition-colors cursor-pointer';
</script>

<div class="flex items-center justify-between gap-2 text-xs text-zinc-500">
	<span>
		{t('pitr.tx.count')}：<span class="font-semibold text-zinc-700">{transactions.length}</span>
		{#if activeFilters.size > 0}
			<span class="text-zinc-400">（筛选显示 {displayedTx.length} 条）</span>
		{/if}
	</span>
	<div class="flex items-center gap-2">
		{#if selectable}
			<button
				type="button"
				class="text-primary hover:underline disabled:cursor-not-allowed disabled:opacity-50"
				onclick={toggleAll}
				disabled={displayedTx.length === 0}
			>
				{allChecked ? t('pitr.tx.uncheckAll') : t('pitr.tx.checkAll')}
			</button>
		{/if}
	</div>
</div>

{#if hasSqlData}
	<div class="mt-2 flex flex-wrap items-center gap-2">
		<span class="text-xs text-zinc-400">操作类型筛选：</span>
		<button
			type="button"
			class="{filterBtnBase} {activeFilters.has('delete')
				? 'border-red-300 bg-red-50 text-red-700'
				: 'border-zinc-200 bg-white text-zinc-500 hover:border-red-200 hover:text-red-600'}"
			onclick={() => toggleFilter('delete')}
		>
			DELETE ({opTypeCounts().delete})
		</button>
		<button
			type="button"
			class="{filterBtnBase} {activeFilters.has('insert')
				? 'border-emerald-300 bg-emerald-50 text-emerald-700'
				: 'border-zinc-200 bg-white text-zinc-500 hover:border-emerald-200 hover:text-emerald-600'}"
			onclick={() => toggleFilter('insert')}
		>
			INSERT ({opTypeCounts().insert})
		</button>
		<button
			type="button"
			class="{filterBtnBase} {activeFilters.has('update')
				? 'border-blue-300 bg-blue-50 text-blue-700'
				: 'border-zinc-200 bg-white text-zinc-500 hover:border-blue-200 hover:text-blue-600'}"
			onclick={() => toggleFilter('update')}
		>
			UPDATE ({opTypeCounts().update})
		</button>
		{#if activeFilters.size > 0}
			<button
				type="button"
				class="text-xs text-zinc-400 hover:text-zinc-600"
				onclick={() => (activeFilters = new Set())}
			>
				清除筛选
			</button>
		{/if}
	</div>
{/if}

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
							disabled={displayedTx.length === 0}
						/>
					</th>
				{/if}
				<th class="px-2 py-1.5 font-medium">{t('pitr.tx.id')}</th>
				<th class="px-2 py-1.5 font-medium">{t('pitr.tx.commitTime')}</th>
				<th class="px-2 py-1.5 font-medium">{t('pitr.tx.tables')}</th>
				{#if hasSqlData}
					<th class="px-2 py-1.5 font-medium">操作类型</th>
				{/if}
				<th class="px-2 py-1.5 text-right font-medium">{t('pitr.tx.rows')}</th>
				<th class="px-2 py-1.5 font-medium"></th>
			</tr>
		</thead>
		<tbody>
			{#each displayedTx as tx (tx.txId)}
				{@const opTypes = txOpTypes(tx)}
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
					{#if hasSqlData}
						<td class="px-2 py-1.5">
							<div class="flex flex-wrap gap-1">
								{#if opTypes.has('delete')}
									<span class="inline-flex items-center rounded-full bg-red-100 px-1.5 py-0.5 text-[10px] font-medium text-red-700">DELETE</span>
								{/if}
								{#if opTypes.has('insert')}
									<span class="inline-flex items-center rounded-full bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700">INSERT</span>
								{/if}
								{#if opTypes.has('update')}
									<span class="inline-flex items-center rounded-full bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-700">UPDATE</span>
								{/if}
								{#if opTypes.size === 0}
									<span class="text-xs text-zinc-400">—</span>
								{/if}
							</div>
						</td>
					{/if}
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
	{:else if displayedTx.length === 0}
		<p class="py-6 text-center text-sm text-zinc-400">当前筛选条件下暂无事务</p>
	{/if}
</div>
