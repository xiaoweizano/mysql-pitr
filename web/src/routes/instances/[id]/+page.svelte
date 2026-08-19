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

	// Route segment `[id]` is guaranteed by the router; `?? ''` satisfies the
	// `string | undefined` param typing (unreachable at runtime).
	const agentId = $derived(page.params.id ?? '');

	// Agent (basic info) state.
	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let agent = $state<AgentInfo | null>(null);

	// Archive monitoring state — refreshed independently so a refresh failure
	// (e.g. agent went offline) does not tear down the whole page.
	let archive = $state<ArchiveState | null>(null);
	let archiveLoading = $state(false);
	/** null = ok; 'offline' = 503 (agent down); string = other failure. */
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
			// The backend answers 503 when the agent is offline — that's an
			// expected operational state, not a crash: show a dedicated message.
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

	function badgeClass(status: string): string {
		switch (status) {
			case 'online':
				return 'bg-emerald-100 text-emerald-700';
			case 'error':
				return 'bg-red-100 text-red-700';
			case 'rejected':
				return 'bg-rose-100 text-rose-700';
			case 'offline':
			default:
				return 'bg-zinc-100 text-zinc-600';
		}
	}

	function statusLabel(status: string): string {
		switch (status) {
			case 'online':
				return t('instances.status.online');
			case 'error':
				return t('instances.status.error');
			case 'rejected':
				return '已拒绝';
			case 'offline':
			default:
				return t('instances.status.offline');
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

<div class="p-6">
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
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else}
		<div class="flex flex-wrap items-center justify-between gap-3">
			<div>
				<h1 class="text-xl font-semibold text-zinc-900">{agent.hostname}</h1>
				<p class="text-sm text-zinc-500">{t('instance.detail.title')}</p>
			</div>
			<Button href={`/pitr?agentId=${encodeURIComponent(agent.id)}`}>
				{t('instance.pitr')}
			</Button>
		</div>

		<div class="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
			<Card>
				<CardHeader>
					<CardTitle>{t('instance.basicInfo')}</CardTitle>
				</CardHeader>
				<CardContent class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
					<span class="text-zinc-500">{t('instance.id')}</span>
					<span class="break-all font-mono text-xs text-zinc-700">{agent.id}</span>

					<span class="text-zinc-500">{t('instances.org')}</span>
					<span>{agent.orgId}</span>

					<span class="text-zinc-500">{t('instances.hostname')}</span>
					<span class="font-medium">{agent.hostname}</span>

					<span class="text-zinc-500">{t('instances.mysqlVersion')}</span>
					<span>{agent.mySQLVersion || '—'}</span>

					<span class="text-zinc-500">{t('instance.status')}</span>
					<span>
						<span
							class={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${badgeClass(agent.status)}`}
						>
							{statusLabel(agent.status)}
						</span>
					</span>

					<span class="text-zinc-500">{t('instance.approved')}</span>
					<span>{agent.approved ? t('instance.approved.yes') : t('instance.approved.no')}</span>

					<span class="text-zinc-500">{t('instances.lastSeen')}</span>
					<span>{formatDate(agent.lastSeen)}</span>

					<span class="text-zinc-500">{t('instances.createdAt')}</span>
					<span>{formatDate(agent.createdAt)}</span>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>{t('instance.archive.title')}</CardTitle>
					<CardDescription>{t('instance.archive.desc')}</CardDescription>
				</CardHeader>
				<CardContent>
					{#if archiveLoading}
						<p class="py-6 text-center text-sm text-zinc-400">{t('common.loading')}</p>
					{:else if archiveError === 'offline'}
						<div class="flex flex-col items-center gap-2 py-6 text-center">
							<span class="inline-flex items-center rounded-full bg-zinc-100 px-2 py-0.5 text-xs font-medium text-zinc-600">
								{t('instances.status.offline')}
							</span>
							<p class="text-sm text-zinc-500">{t('instance.archive.offline')}</p>
						</div>
					{:else if archiveError === 'error'}
						<div class="flex flex-col items-center gap-2 py-6 text-center">
							<p class="text-sm text-destructive">{t('instance.archive.loadError')}</p>
						</div>
					{:else if archive}
						<div class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
							<span class="text-zinc-500">{t('instance.archive.lastFile')}</span>
							<span class="font-mono text-xs text-zinc-700">{archive.lastFile || '—'}</span>

							<span class="text-zinc-500">{t('instance.archive.lastPos')}</span>
							<span class="font-mono text-xs text-zinc-700">{archive.lastPos}</span>

							<span class="text-zinc-500">{t('instance.archive.lastGtid')}</span>
							<span class="break-all font-mono text-xs text-zinc-700">{archive.lastGtid || '—'}</span>

							<span class="text-zinc-500">{t('instance.archive.updatedAt')}</span>
							<span>{formatDate(archive.updatedAt)}</span>
						</div>
					{:else}
						<p class="py-6 text-center text-sm text-zinc-400">{t('instance.archive.noData')}</p>
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