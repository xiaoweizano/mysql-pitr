<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { t } from '$lib/i18n.svelte.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import {
		Card,
		CardContent,
		CardDescription,
		CardHeader,
		CardTitle
	} from '$lib/components/ui/card/index.js';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table/index.js';
	import { listOrgs } from '$lib/api/orgs.js';
	import {
		listAgents,
		approveAgent,
		rejectAgent,
		type AgentInfo
	} from '$lib/api/agents.js';
	import Pagination from '$lib/components/Pagination.svelte';

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let agents = $state<AgentInfo[]>([]);
	let orgNames = $state<Record<string, string>>({});

	let actionError = $state<string | null>(null);
	let pendingId = $state<string | null>(null);
	let page = $state(1);
	let pageSize = $state(10);

	const pagedAgents = $derived(agents.slice((page - 1) * pageSize, page * pageSize));

	async function load() {
		loading = true;
		loadError = null;
		actionError = null;
		try {
			const orgs = await listOrgs();
			const names: Record<string, string> = {};
			const all: AgentInfo[] = [];
			for (const org of orgs) {
				names[org.id] = org.name;
				try {
					const list = await listAgents(org.id);
					all.push(...list);
				} catch {
					// Per-org agent fetch failure should not break the whole page.
				}
			}
			orgNames = names;
			agents = all;
		} catch (e) {
			loadError = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	onMount(load);

	/** Status badge styling — uses the global status-badge / status-dot system. */
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
		const d = new Date(value);
		if (Number.isNaN(d.getTime())) return value || '—';
		return d.toLocaleString();
	}

	function goDetail(id: string) {
		void goto(`/instances/${encodeURIComponent(id)}`);
	}

	async function handleApprove(a: AgentInfo) {
		pendingId = a.id;
		actionError = null;
		try {
			const updated = await approveAgent(a.id);
			agents = agents.map((x) => (x.id === updated.id ? updated : x));
		} catch (e) {
			actionError = e instanceof Error ? e.message : String(e);
		} finally {
			pendingId = null;
		}
	}

	async function handleReject(a: AgentInfo) {
		if (!window.confirm(t('instances.rejectConfirm'))) return;
		pendingId = a.id;
		actionError = null;
		try {
			await rejectAgent(a.id);
			agents = agents.filter((x) => x.id !== a.id);
		} catch (e) {
			actionError = e instanceof Error ? e.message : String(e);
		} finally {
			pendingId = null;
		}
	}
</script>

<svelte:head>
	<title>{t('instances.title')} · {t('app.name')}</title>
</svelte:head>

<div class="flex h-full flex-col p-6 animate-fade-in">
	<div class="flex shrink-0 flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-foreground">{t('instances.title')}</h1>
			<p class="text-sm text-muted-foreground">{t('instances.subtitle')}</p>
		</div>
		<Button variant="outline" size="sm" onclick={load} disabled={loading}>
			{loading ? t('common.loading') : t('common.retry')}
		</Button>
	</div>

	{#if loadError}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">
					{t('instances.loadError')}：{loadError}
				</p>
				<Button variant="outline" size="sm" onclick={load} disabled={loading}>
					{loading ? t('common.loading') : t('common.retry')}
				</Button>
			</CardContent>
		</Card>
	{:else if loading}
		<!-- 骨架屏 -->
		<Card class="mt-4 shrink-0">
			<CardContent class="space-y-3 py-6">
				{#each [1, 2, 3, 4, 5] as _}
					<div class="flex items-center gap-4">
						<div class="skeleton h-5 w-40"></div>
						<div class="skeleton h-5 w-24"></div>
						<div class="skeleton h-5 w-20"></div>
						<div class="skeleton h-5 w-16"></div>
						<div class="skeleton ml-auto h-5 w-32"></div>
					</div>
				{/each}
			</CardContent>
		</Card>
	{:else if agents.length === 0}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-2 py-12 text-center">
				<p class="text-sm font-medium text-foreground/80">{t('instances.empty')}</p>
				<p class="text-xs text-muted-foreground">{t('instances.emptyDesc')}</p>
			</CardContent>
		</Card>
	{:else}
		<Card class="mt-4 min-h-0 flex-1">
			<CardContent class="flex min-h-0 flex-1 flex-col">
				{#if actionError}
					<p class="mb-4 shrink-0 text-sm text-destructive" role="alert">
						{t('instances.actionError')}：{actionError}
					</p>
				{/if}

				<div class="min-h-0 flex-1 overflow-y-auto">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead class="sticky top-0 z-10 bg-card">{t('instances.hostname')}</TableHead>
								<TableHead class="sticky top-0 z-10 bg-card">{t('instances.org')}</TableHead>
								<TableHead class="sticky top-0 z-10 bg-card">{t('instances.mysqlVersion')}</TableHead>
								<TableHead class="sticky top-0 z-10 bg-card">{t('instance.status')}</TableHead>
								<TableHead class="sticky top-0 z-10 bg-card">{t('instance.approved')}</TableHead>
								<TableHead class="sticky top-0 z-10 bg-card text-right">{t('instances.actions')}</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{#each pagedAgents as a (a.id)}
								<TableRow
									class="cursor-pointer transition-colors duration-150 hover:bg-muted/50"
									onclick={() => goDetail(a.id)}
								>
									<TableCell class="font-medium">{a.hostname}</TableCell>
									<TableCell>{orgNames[a.orgId] ?? a.orgId}</TableCell>
									<TableCell class="text-muted-foreground">{a.mySQLVersion || '—'}</TableCell>
									<TableCell>
										<span class="status-badge border border-border/50">
											<span class={statusDot(a.status)}></span>
											{statusLabel(a.status)}
										</span>
									</TableCell>
									<TableCell>
										{#if a.approved}
											<span class="text-xs text-muted-foreground">{t('instance.approved.yes')}</span>
										{:else}
											<span class="text-xs font-medium text-warning">
												{t('instance.approved.no')}
											</span>
										{/if}
									</TableCell>
									<TableCell class="text-right">
										<div class="flex items-center justify-end gap-2" role="group">
											{#if !a.approved}
												<Button
													variant="outline"
													size="xs"
													disabled={pendingId !== null}
													onclick={(e) => {
														e.stopPropagation();
														void handleApprove(a);
													}}
												>
													{t('instances.approve')}
												</Button>
											{/if}
											<Button
												variant={a.approved ? 'outline' : 'destructive'}
												size="xs"
												disabled={pendingId !== null}
												onclick={(e) => {
													e.stopPropagation();
													void handleReject(a);
												}}
											>
												{t('instances.reject')}
											</Button>
											<Button
												variant="ghost"
												size="xs"
												onclick={(e) => {
													e.stopPropagation();
													goDetail(a.id);
												}}
											>
												{t('instances.detail')}
											</Button>
										</div>
									</TableCell>
								</TableRow>
							{/each}
						</TableBody>
					</Table>
				</div>
			</CardContent>
		</Card>
		<div class="mt-3 shrink-0">
			<Pagination total={agents.length} bind:page bind:pageSize />
		</div>
	{/if}
</div>