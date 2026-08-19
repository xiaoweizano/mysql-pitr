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

	// State (Svelte 5 runes). Everything optional → no blank flash: an empty
	// list renders the empty state, a failed fetch renders an inline error with
	// a retry button — never a white screen.
	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let agents = $state<AgentInfo[]>([]);
	/** orgId → org name, for the table column. */
	let orgNames = $state<Record<string, string>>({});

	let actionError = $state<string | null>(null);
	let pendingId = $state<string | null>(null);
	let page = $state(1);
	let pageSize = $state(10);

	const pagedAgents = $derived(agents.slice((page - 1) * pageSize, page * pageSize));

	/**
	 * Load orgs, then agents per org. `GET /api/agents` requires an `orgId`
	 * query param, so we fetch the user's orgs first and query each in turn.
	 * Archive state is deliberately NOT fetched here — the detail page owns
	 * archive monitoring, so the list page avoids a per-row N+1 request.
	 */
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
			agents = agents.map((x) => x.id === a.id ? { ...x, approved: false, status: 'rejected' } : x);
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

<div class="p-6">
	<div class="flex items-center justify-between">
		<div>
			<h1 class="text-xl font-semibold text-zinc-900">{t('instances.title')}</h1>
			<p class="text-sm text-zinc-500">{t('instances.subtitle')}</p>
		</div>
		<Button variant="outline" size="sm" onclick={load} disabled={loading}>
			{loading ? t('common.loading') : t('common.retry')}
		</Button>
	</div>

	{#if loadError}
		<Card class="mt-6">
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
		<Card class="mt-6">
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else if agents.length === 0}
		<Card class="mt-6">
			<CardContent class="flex flex-col items-center gap-2 py-12 text-center">
				<p class="text-sm font-medium text-zinc-600">{t('instances.empty')}</p>
				<p class="text-xs text-zinc-400">{t('instances.emptyDesc')}</p>
			</CardContent>
		</Card>
	{:else}
		<Card class="mt-6">
			<CardHeader>
				<CardTitle>{t('instances.title')}</CardTitle>
				<CardDescription>{t('instances.subtitle')}</CardDescription>
			</CardHeader>
			<CardContent>
				{#if actionError}
					<p class="mb-4 text-sm text-destructive" role="alert">
						{t('instances.actionError')}：{actionError}
					</p>
				{/if}

				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>{t('instances.hostname')}</TableHead>
							<TableHead>{t('instances.org')}</TableHead>
							<TableHead>{t('instances.mysqlVersion')}</TableHead>
							<TableHead>{t('instance.status')}</TableHead>
							<TableHead>{t('instance.approved')}</TableHead>
							<TableHead class="text-right">{t('instances.actions')}</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{#each pagedAgents as a (a.id)}
							<TableRow
								class="cursor-pointer"
								onclick={() => goDetail(a.id)}
							>
								<TableCell class="font-medium">{a.hostname}</TableCell>
								<TableCell>{orgNames[a.orgId] ?? a.orgId}</TableCell>
								<TableCell class="text-zinc-500">{a.mySQLVersion || '—'}</TableCell>
								<TableCell>
									<span
										class={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${badgeClass(a.status)}`}
									>
										{statusLabel(a.status)}
									</span>
								</TableCell>
								<TableCell>
									{#if a.approved}
										<span class="text-xs text-zinc-500">{t('instance.approved.yes')}</span>
									{:else}
										<span class="text-xs font-medium text-amber-600">
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
			</CardContent>
		</Card>
		<div class="mt-3">
			<Pagination total={agents.length} bind:page bind:pageSize />
		</div>
	{/if}
</div>
