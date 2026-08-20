<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { t } from '$lib/i18n.svelte.js';
	import { ApiError } from '$lib/api/client.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import {
		Card,
		CardContent,
		CardDescription,
		CardFooter,
		CardHeader,
		CardTitle
	} from '$lib/components/ui/card/index.js';
	import {
		getAgent,
		getAgentArchive,
		type AgentInfo,
		type ArchiveState
	} from '$lib/api/agents.js';

	const agentId = $derived(page.params.id ?? '');

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let agent = $state<AgentInfo | null>(null);

	let archive = $state<ArchiveState | null>(null);
	let archiveLoading = $state(false);
	let archiveError = $state<'offline' | 'error' | null>(null);

	async function loadAgent() {
		loading = true;
		loadError = null;
		try {
			agent = await getAgent(agentId);
		} catch (e) {
			loadError = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function loadArchive() {
		archiveLoading = true;
		archiveError = null;
		try {
			archive = await getAgentArchive(agentId);
		} catch (e) {
			if (e instanceof ApiError && e.status === 503) {
				archiveError = 'offline';
			} else {
				archiveError = 'error';
			}
		} finally {
			archiveLoading = false;
		}
	}

	onMount(() => {
		void loadAgent();
		void loadArchive();
	});

	function statusDot(status: string): string {
		switch (status) {
			case 'online': return 'status-dot status-dot--online status-dot--pulse';
			case 'error': return 'status-dot status-dot--error';
			case 'rejected': return 'status-dot status-dot--warning';
			case 'offline':
			default: return 'status-dot status-dot--offline';
		}
	}

	function statusLabel(status: string): string {
		switch (status) {
			case 'online': return t('instances.status.online');
			case 'error': return t('instances.status.error');
			case 'rejected': return '已拒绝';
			case 'offline':
			default: return t('instances.status.offline');
		}
	}

	function formatDate(value: string): string {
		if (!value) return '—';
		const d = new Date(value);
		if (Number.isNaN(d.getTime())) return value;
		return d.toLocaleString();
	}
</script>

<svelte:head>
	<title>
		{agent?.hostname ?? t('instance.detail.title')} · {t('app.name')}
	</title>
</svelte:head>

<div class="h-full overflow-y-auto p-6 animate-fade-in">
	{#if loadError}
		<Card>
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">{t('instance.loadError')}：{loadError}</p>
				<Button variant="outline" size="sm" onclick={loadAgent} disabled={loading}>
					{loading ? t('common.loading') : t('common.retry')}
				</Button>
			</CardContent>
		</Card>
	{:else if loading || !agent}
		<Card>
			<CardContent class="space-y-3 py-6">
				<div class="skeleton h-6 w-48"></div>
				<div class="skeleton h-4 w-64"></div>
				<div class="mt-4 grid grid-cols-2 gap-4">
					{#each [1, 2, 3, 4] as _}
						<div class="space-y-2">
							<div class="skeleton h-3 w-16"></div>
							<div class="skeleton h-4 w-32"></div>
						</div>
					{/each}
				</div>
			</CardContent>
		</Card>
	{:else}
		<div class="flex flex-wrap items-center justify-between gap-3">
			<div>
				<h1 class="text-xl font-semibold text-foreground">{agent.hostname}</h1>
				<p class="text-sm text-muted-foreground">{t('instance.detail.title')}</p>
			</div>
			<Button href={`/pitr?agentId=${encodeURIComponent(agent.id)}`}>
				{t('instance.pitr')}
			</Button>
		</div>

		<div class="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
			<Card class="card-hover">
				<CardHeader>
					<CardTitle>{t('instance.basicInfo')}</CardTitle>
				</CardHeader>
				<CardContent class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
					<span class="text-muted-foreground">{t('instance.id')}</span>
					<span class="break-all font-mono text-xs text-foreground/70">{agent.id}</span>

					<span class="text-muted-foreground">{t('instances.org')}</span>
					<span>{agent.orgId}</span>

					<span class="text-muted-foreground">{t('instances.hostname')}</span>
					<span class="font-medium">{agent.hostname}</span>

					<span class="text-muted-foreground">{t('instances.mysqlVersion')}</span>
					<span>{agent.mySQLVersion || '—'}</span>

					<span class="text-muted-foreground">{t('instance.status')}</span>
					<span>
						<span class="status-badge border border-border/50">
							<span class={statusDot(agent.status)}></span>
							{statusLabel(agent.status)}
						</span>
					</span>

					<span class="text-muted-foreground">{t('instance.approved')}</span>
					<span>{agent.approved ? t('instance.approved.yes') : t('instance.approved.no')}</span>

					<span class="text-muted-foreground">{t('instances.lastSeen')}</span>
					<span>{formatDate(agent.lastSeen)}</span>

					<span class="text-muted-foreground">{t('instances.createdAt')}</span>
					<span>{formatDate(agent.createdAt)}</span>
				</CardContent>
			</Card>

			<Card class="card-hover">
				<CardHeader>
					<CardTitle>{t('instance.archive.title')}</CardTitle>
					<CardDescription>{t('instance.archive.desc')}</CardDescription>
				</CardHeader>
				<CardContent>
					{#if archiveLoading}
						<div class="space-y-2 py-4">
							{#each [1, 2, 3, 4] as _}
								<div class="flex items-center gap-4">
									<div class="skeleton h-3 w-20"></div>
									<div class="skeleton h-3 w-40"></div>
								</div>
							{/each}
						</div>
					{:else if archiveError === 'offline'}
						<div class="flex flex-col items-center gap-2 py-6 text-center">
							<span class="status-badge border border-border/50">
								<span class="status-dot status-dot--offline"></span>
								{t('instances.status.offline')}
							</span>
							<p class="text-sm text-muted-foreground">{t('instance.archive.offline')}</p>
						</div>
					{:else if archiveError === 'error'}
						<div class="flex flex-col items-center gap-2 py-6 text-center">
							<p class="text-sm text-destructive">{t('instance.archive.loadError')}</p>
						</div>
					{:else if archive}
						<div class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
							<span class="text-muted-foreground">{t('instance.archive.lastFile')}</span>
							<span class="font-mono text-xs text-foreground/70">{archive.lastFile || '—'}</span>

							<span class="text-muted-foreground">{t('instance.archive.lastPos')}</span>
							<span class="font-mono text-xs text-foreground/70">{archive.lastPos}</span>

							<span class="text-muted-foreground">{t('instance.archive.lastGtid')}</span>
							<span class="break-all font-mono text-xs text-foreground/70">{archive.lastGtid || '—'}</span>

							<span class="text-muted-foreground">{t('instance.archive.updatedAt')}</span>
							<span>{formatDate(archive.updatedAt)}</span>
						</div>
					{:else}
						<p class="py-6 text-center text-sm text-muted-foreground">{t('instance.archive.noData')}</p>
					{/if}
				</CardContent>
				<CardFooter class="justify-end">
					<Button variant="outline" size="sm" onclick={loadArchive} disabled={archiveLoading}>
						{archiveLoading ? t('common.loading') : t('instance.archive.refresh')}
					</Button>
				</CardFooter>
			</Card>
		</div>
	{/if}
</div>