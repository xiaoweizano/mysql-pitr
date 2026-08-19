<script lang="ts">
	import { onMount } from 'svelte';
	import { t } from '$lib/i18n.svelte.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import {
		Card,
		CardContent,
		CardDescription,
		CardHeader,
		CardTitle
	} from '$lib/components/ui/card/index.js';
	import { listOperations, type OperationListItem } from '$lib/api/pitr.js';
	import { listOrgs } from '$lib/api/orgs.js';
	import { formatDateTime, tablesLabel } from '$lib/format.js';
	import { History } from '@lucide/svelte';
	import Pagination from '$lib/components/Pagination.svelte';

	/**
	 * Operation history list — all PITR operations of the user's
	 * organisations, newest first. Each row links to the operation detail page.
	 * `GET /api/pitr?org_id=X` requires the org query param, so we list the
	 * user's orgs first and merge (same pattern as the instances page).
	 */

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let operations = $state<OperationListItem[]>([]);
	let page = $state(1);
	let pageSize = $state(10);

	const pagedOps = $derived(operations.slice((page - 1) * pageSize, page * pageSize));

	async function load() {
		loading = true;
		loadError = null;
		try {
			const orgs = await listOrgs();
			const results = await Promise.allSettled(orgs.map((o) => listOperations(o.id)));
			const merged = results
				.filter((r): r is PromiseFulfilledResult<Awaited<ReturnType<typeof listOperations>>> => r.status === 'fulfilled')
				.flatMap((r) => r.value)
				.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
			operations = merged;
		} catch (e) {
			loadError = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		void load();
	});

	function badgeClass(s: string): string {
		switch (s) {
			case 'done':
				return 'bg-emerald-100 text-emerald-700';
			case 'failed':
				return 'bg-red-100 text-red-700';
			case 'scanning':
				return 'bg-sky-100 text-sky-700';
			case 'executing':
				return 'bg-violet-100 text-violet-700';
			case 'paused':
				return 'bg-amber-100 text-amber-700';
			case 'ready':
				return 'bg-teal-100 text-teal-700';
			case 'blocked':
				return 'bg-orange-100 text-orange-700';
			case 'created':
			case 'cancelled':
			default:
				return 'bg-zinc-100 text-zinc-600';
		}
	}

	/** Filter summary from the lowercase-key DTO (Task 5). */
	function filterSummary(op: OperationListItem): string {
		const parts: string[] = [];
		const f = op.filter ?? {};
		const tables = tablesLabel(f.tables, 2);
		if (tables) parts.push(`${t('operations.filter.tables')}: ${tables}`);
		const times: string[] = [];
		if (f.timeStart) times.push(formatDateTime(f.timeStart));
		if (f.timeEnd) times.push(formatDateTime(f.timeEnd));
		if (times.length > 0) parts.push(`${t('operations.filter.time')}: ${times.join(' → ')}`);
		if (f.gtidSet) parts.push(`${t('operations.filter.gtid')}: ${f.gtidSet}`);
		return parts.join(' · ');
	}
</script>

<svelte:head>
	<title>{t('operations.title')} · {t('app.name')}</title>
</svelte:head>

<div class="flex h-full flex-col p-6">
	<div class="flex shrink-0 flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-zinc-900">{t('operations.title')}</h1>
			<p class="text-sm text-zinc-500">{t('operations.subtitle')}</p>
		</div>
		<Button href="/pitr">{t('operations.new')}</Button>
	</div>

	{#if loadError}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">{t('operations.loadError')}：{loadError}</p>
				<Button variant="outline" size="sm" onclick={load} disabled={loading}>
					{loading ? t('common.loading') : t('common.retry')}
				</Button>
			</CardContent>
		</Card>
	{:else if loading}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else if operations.length === 0}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-2 py-10 text-center">
				<History class="size-8 text-zinc-300" />
				<p class="text-sm text-zinc-500">{t('operations.empty')}</p>
			</CardContent>
		</Card>
	{:else}
		<Card class="mt-4 min-h-0 flex-1">
			<CardContent class="min-h-0 flex-1 overflow-y-auto p-0">
				<table class="w-full text-sm">
					<thead>
						<tr class="text-left text-xs text-zinc-500">
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 font-medium">{t('operations.status')}</th>
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 font-medium">{t('operations.type')}</th>
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 font-medium">{t('operations.mode')}</th>
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 font-medium">{t('operations.agent')}</th>
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 font-medium">{t('operations.filter')}</th>
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 font-medium">{t('operations.createdAt')}</th>
							<th class="sticky top-0 z-10 bg-card px-4 py-2.5 text-right font-medium">{t('operations.actions')}</th>
						</tr>
					</thead>
					<tbody>
						{#each pagedOps as op (op.id)}
							<tr class="border-b border-zinc-100 align-middle">
								<td class="px-4 py-2.5">
									<div class="flex items-center gap-2">
										<span
											class={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${badgeClass(op.status)}`}
										>
											{t(`pitr.status.${op.status}`)}
										</span>
										<span
											class={`inline-block size-1.5 rounded-full ${op.agentConnected ? 'bg-emerald-500' : 'bg-zinc-300'}`}
											title={op.agentConnected
												? t('operations.agentConnected')
												: t('operations.agentDisconnected')}
										></span>
									</div>
								</td>
								<td class="px-4 py-2.5 text-zinc-700">{t(`pitr.type.short.${op.type}`)}</td>
								<td class="px-4 py-2.5 text-zinc-500">{t(`pitr.mode.${op.mode}`)}</td>
								<td class="max-w-40 truncate px-4 py-2.5 font-mono text-xs text-zinc-600" title={op.agentId}>
									{op.agentId}
								</td>
								<td class="max-w-72 truncate px-4 py-2.5 text-xs text-zinc-600" title={filterSummary(op)}>
									{filterSummary(op) || '—'}
								</td>
								<td class="whitespace-nowrap px-4 py-2.5 text-zinc-500">
									{formatDateTime(op.createdAt)}
								</td>
								<td class="px-4 py-2.5 text-right">
									<Button variant="ghost" size="sm" href={`/pitr/${encodeURIComponent(op.id)}`}>
										{t('operations.view')}
									</Button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</CardContent>
		</Card>
		<div class="mt-3 shrink-0">
			<Pagination total={operations.length} bind:page bind:pageSize />
		</div>
	{/if}
</div>
