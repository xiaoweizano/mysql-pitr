<script lang="ts">
	import { onMount } from 'svelte';
	import { t } from '$lib/i18n.svelte.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import {
		Card,
		CardContent
	} from '$lib/components/ui/card/index.js';
	import { listOperations, type OperationListItem } from '$lib/api/pitr.js';
	import { listOrgs } from '$lib/api/orgs.js';
	import { formatDateTime, tablesLabel } from '$lib/format.js';
	import { History } from '@lucide/svelte';
	import Pagination from '$lib/components/Pagination.svelte';

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

	/** 状态徽章：圆点样式映射 */
	function statusDot(s: string): string {
		switch (s) {
			case 'done': return 'status-dot status-dot--online';
			case 'failed': return 'status-dot status-dot--error';
			case 'scanning': return 'status-dot status-dot--info status-dot--pulse';
			case 'executing': return 'status-dot status-dot--info status-dot--pulse';
			case 'paused': return 'status-dot status-dot--paused';
			case 'ready': return 'status-dot status-dot--info';
			case 'blocked': return 'status-dot status-dot--warning';
			case 'created':
			case 'cancelled':
			default: return 'status-dot status-dot--offline';
		}
	}

	/** 状态文字颜色 */
	function statusText(s: string): string {
		switch (s) {
			case 'done': return 'text-success';
			case 'failed': return 'text-destructive';
			case 'scanning': return 'text-info';
			case 'executing': return 'text-info';
			case 'paused': return 'text-warning';
			case 'ready': return 'text-info';
			case 'blocked': return 'text-warning';
			default: return 'text-muted-foreground';
		}
	}

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

<div class="flex h-full flex-col p-6 animate-fade-in">
	<div class="flex shrink-0 flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-foreground">{t('operations.title')}</h1>
			<p class="text-sm text-muted-foreground">{t('operations.subtitle')}</p>
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
			<CardContent class="space-y-3 py-6">
				{#each [1, 2, 3, 4, 5] as _}
					<div class="flex items-center gap-4">
						<div class="skeleton h-5 w-20"></div>
						<div class="skeleton h-5 w-24"></div>
						<div class="skeleton h-5 w-16"></div>
						<div class="skeleton h-5 w-40"></div>
						<div class="skeleton ml-auto h-5 w-28"></div>
					</div>
				{/each}
			</CardContent>
		</Card>
	{:else if operations.length === 0}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-2 py-10 text-center">
				<History class="size-8 text-muted-foreground/40" />
				<p class="text-sm text-muted-foreground">{t('operations.empty')}</p>
			</CardContent>
		</Card>
	{:else}
		<Card class="mt-4 min-h-0 flex-1">
			<CardContent class="min-h-0 flex-1 overflow-y-auto p-0">
				<table class="w-full text-sm">
					<thead>
						<tr class="text-left text-xs text-muted-foreground">
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
							<tr class="border-b border-border/50 align-middle transition-colors duration-150 hover:bg-muted/40">
								<td class="px-4 py-2.5">
									<div class="flex items-center gap-2">
										<span class="status-badge border border-border/50 {statusText(op.status)}">
											<span class={statusDot(op.status)}></span>
											{t(`pitr.status.${op.status}`)}
										</span>
									</div>
								</td>
								<td class="px-4 py-2.5 text-foreground/80">{t(`pitr.type.short.${op.type}`)}</td>
								<td class="px-4 py-2.5 text-muted-foreground">{t(`pitr.mode.${op.mode}`)}</td>
								<td class="max-w-40 truncate px-4 py-2.5 font-mono text-xs text-muted-foreground" title={op.agentId}>
									{op.agentId}
								</td>
								<td class="max-w-72 truncate px-4 py-2.5 text-xs text-muted-foreground" title={filterSummary(op)}>
									{filterSummary(op) || '—'}
								</td>
								<td class="whitespace-nowrap px-4 py-2.5 text-muted-foreground">
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