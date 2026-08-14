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
	import { Input } from '$lib/components/ui/input/index.js';
	import { Label } from '$lib/components/ui/label/index.js';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table/index.js';
	import { listOrgs, type OrgInfo } from '$lib/api/orgs.js';
	import { listAgents, type AgentInfo } from '$lib/api/agents.js';
	import { queryAudit, exportAuditCsv, type AuditEntry } from '$lib/api/audit.js';
	import { formatDateTime, shortId } from '$lib/format.js';
	import { ChevronDown, ChevronRight, Download } from '@lucide/svelte';
	import Pagination from '$lib/components/Pagination.svelte';

	/**
	 * Audit log page — filtered query over `GET /api/audit` with CSV export.
	 *
	 * The backend requires the `org_id` query param (snake_case — the client
	 * encodes it in `$lib/api/audit.ts`), so the org select is the primary
	 * filter. Selecting an org loads its agents for the agent filter, then a
	 * "查询" applies the remaining (time range / status / agent) filters.
	 *
	 * CSV export goes through a fetch with the Authorization header and an
	 * object-URL download link — the JWT cannot ride a plain `<a href>`.
	 */

	/** Operation states written to the audit trail (pitr/handler.go appendAudit). */
	const AUDIT_STATUSES = [
		'created',
		'scanning',
		'ready',
		'executing',
		'paused',
		'done',
		'failed',
		'blocked',
		'cancelled'
	];

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let entries = $state<AuditEntry[]>([]);

	let orgs = $state<OrgInfo[]>([]);
	let agents = $state<AgentInfo[]>([]);

	let selectedOrgId = $state('');
	let from = $state('');
	let to = $state('');
	let status = $state('');
	let agentId = $state('');

	/** operationId of the row whose detail (errorDetails etc.) is expanded. */
	let expandedId = $state<string | null>(null);

	let page = $state(1);
	let pageSize = $state(10);

	const pagedEntries = $derived(entries.slice((page - 1) * pageSize, page * pageSize));

	let exporting = $state(false);
	let exportError = $state<string | null>(null);

	/** datetime-local value → RFC3339 (UTC) for the backend. */
	function toRfc3339(v: string): string | undefined {
		if (!v) return undefined;
		const d = new Date(v);
		if (Number.isNaN(d.getTime())) return undefined;
		return d.toISOString();
	}

	async function loadAgents(orgId: string) {
		agents = [];
		agentId = '';
		if (!orgId) return;
		try {
			agents = await listAgents(orgId);
		} catch {
			// Agent filter is optional — a failed agent fetch must not break
			// the audit query itself.
			agents = [];
		}
	}

	async function load() {
		if (!selectedOrgId) return;
		loading = true;
		loadError = null;
		try {
			entries = await queryAudit({
				orgId: selectedOrgId,
				from: toRfc3339(from),
				to: toRfc3339(to),
				status: status || undefined,
				agentId: agentId || undefined
			});
		} catch (e) {
			loadError = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	/** Org change resets agents + reloads immediately (org is a required filter). */
	async function handleOrgChange() {
		expandedId = null;
		await loadAgents(selectedOrgId);
		await load();
	}

	function resetFilters() {
		from = '';
		to = '';
		status = '';
		agentId = '';
		void load();
	}

	async function handleExport() {
		if (!selectedOrgId || exporting) return;
		exporting = true;
		exportError = null;
		try {
			await exportAuditCsv({
				orgId: selectedOrgId,
				from: toRfc3339(from),
				to: toRfc3339(to),
				status: status || undefined,
				agentId: agentId || undefined
			});
		} catch (e) {
			exportError = e instanceof Error ? e.message : String(e);
		} finally {
			exporting = false;
		}
	}

	onMount(async () => {
		try {
			orgs = await listOrgs();
			if (orgs.length > 0) {
				selectedOrgId = orgs[0].id;
				await loadAgents(selectedOrgId);
				await load();
			} else {
				loading = false;
			}
		} catch (e) {
			loadError = e instanceof Error ? e.message : String(e);
			loading = false;
		}
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

	/** Zero recoveryTime serialises as year 1 — render as a dash. */
	function recoveryLabel(e: AuditEntry): string {
		if (!e.recoveryTime || e.recoveryTime.startsWith('0001')) return '—';
		return formatDateTime(e.recoveryTime);
	}

	const selectClass =
		'h-8 w-full rounded-lg border border-input bg-background px-2.5 text-sm transition-colors focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:pointer-events-none disabled:opacity-50';
</script>

<svelte:head>
	<title>{t('audit.title')} · {t('app.name')}</title>
</svelte:head>

<div class="p-6">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-zinc-900">{t('audit.title')}</h1>
			<p class="text-sm text-zinc-500">{t('audit.subtitle')}</p>
		</div>
		<Button
			variant="outline"
			size="sm"
			disabled={!selectedOrgId || loading || exporting}
			onclick={handleExport}
		>
			<Download class="size-4" />
			{exporting ? t('audit.exporting') : t('audit.export')}
		</Button>
	</div>

	{#if exportError}
		<p class="mt-3 text-sm text-destructive" role="alert">{t('audit.exportError')}：{exportError}</p>
	{/if}

	{#if orgs.length > 0}
		<Card class="mt-4">
			<CardContent>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						void load();
					}}
					class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-6"
				>
					<div class="grid gap-1.5">
						<Label for="audit-org">{t('audit.filter.org')}</Label>
						<select
							id="audit-org"
							class={selectClass}
							bind:value={selectedOrgId}
							onchange={() => void handleOrgChange()}
						>
							{#each orgs as o (o.id)}
								<option value={o.id}>{o.name}</option>
							{/each}
						</select>
					</div>
					<div class="grid gap-1.5">
						<Label for="audit-from">{t('audit.filter.from')}</Label>
						<Input id="audit-from" type="datetime-local" bind:value={from} />
					</div>
					<div class="grid gap-1.5">
						<Label for="audit-to">{t('audit.filter.to')}</Label>
						<Input id="audit-to" type="datetime-local" bind:value={to} />
					</div>
					<div class="grid gap-1.5">
						<Label for="audit-status">{t('audit.filter.status')}</Label>
						<select id="audit-status" class={selectClass} bind:value={status}>
							<option value="">{t('audit.filter.statusAll')}</option>
							{#each AUDIT_STATUSES as s (s)}
								<option value={s}>{t(`pitr.status.${s}`)}</option>
							{/each}
						</select>
					</div>
					<div class="grid gap-1.5">
						<Label for="audit-agent">{t('audit.filter.agent')}</Label>
						<select id="audit-agent" class={selectClass} bind:value={agentId}>
							<option value="">{t('audit.filter.agentAll')}</option>
							{#each agents as a (a.id)}
								<option value={a.id}>{a.hostname}</option>
							{/each}
						</select>
					</div>
					<div class="flex items-end gap-2">
						<Button type="submit" size="sm">{t('audit.filter.query')}</Button>
						<Button type="button" variant="outline" size="sm" onclick={resetFilters}>
							{t('audit.filter.reset')}
						</Button>
					</div>
				</form>
			</CardContent>
		</Card>
	{/if}

	{#if loadError}
		<Card class="mt-4">
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">{t('audit.loadError')}：{loadError}</p>
				<Button variant="outline" size="sm" onclick={load} disabled={loading}>
					{loading ? t('common.loading') : t('common.retry')}
				</Button>
			</CardContent>
		</Card>
	{:else if orgs.length === 0}
		<Card class="mt-4">
			<CardContent class="flex flex-col items-center gap-2 py-10 text-center">
				<p class="text-sm text-zinc-500">{t('audit.noOrg')}</p>
			</CardContent>
		</Card>
	{:else if loading}
		<Card class="mt-4">
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else if entries.length === 0}
		<Card class="mt-4">
			<CardContent class="flex flex-col items-center gap-2 py-10 text-center">
				<p class="text-sm font-medium text-zinc-600">{t('audit.empty')}</p>
				<p class="text-xs text-zinc-400">{t('audit.emptyDesc')}</p>
			</CardContent>
		</Card>
	{:else}
		<Card class="mt-4">
			<CardHeader>
				<CardTitle>{t('audit.title')}</CardTitle>
				<CardDescription>{t('audit.subtitle')}</CardDescription>
			</CardHeader>
			<CardContent>
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead class="w-8"></TableHead>
							<TableHead>{t('audit.operationId')}</TableHead>
							<TableHead>{t('audit.operator')}</TableHead>
							<TableHead>{t('audit.timestamp')}</TableHead>
							<TableHead>{t('audit.targetTable')}</TableHead>
							<TableHead class="text-right">{t('audit.rowsAffected')}</TableHead>
							<TableHead>{t('audit.status')}</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{#each pagedEntries as e (e.operationId)}
							{@const expanded = expandedId === e.operationId}
							<TableRow
								class="cursor-pointer"
								onclick={() => (expandedId = expanded ? null : e.operationId)}
							>
								<TableCell>
									{#if expanded}
										<ChevronDown class="size-4 text-zinc-400" />
									{:else}
										<ChevronRight class="size-4 text-zinc-400" />
									{/if}
								</TableCell>
								<TableCell class="font-mono text-xs" title={e.operationId}>
									{shortId(e.operationId)}
								</TableCell>
								<TableCell>{e.operator || '—'}</TableCell>
								<TableCell>{formatDateTime(e.timestamp)}</TableCell>
								<TableCell class="font-mono text-xs">{e.targetTable || '—'}</TableCell>
								<TableCell class="text-right tabular-nums">{e.rowsAffected}</TableCell>
								<TableCell>
									<span
										class={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${badgeClass(e.status)}`}
									>
										{t(`pitr.status.${e.status}`)}
									</span>
								</TableCell>
							</TableRow>
							{#if expanded}
								<TableRow>
									<TableCell></TableCell>
									<TableCell colspan={6}>
										<div class="grid grid-cols-1 gap-1.5 py-1 text-xs sm:grid-cols-2">
											<div>
												<span class="text-zinc-400">{t('audit.operationId')}：</span>
												<span class="font-mono">{e.operationId}</span>
											</div>
											<div>
												<span class="text-zinc-400">{t('audit.agent')}：</span>
												<span class="font-mono">{e.agentId || '—'}</span>
											</div>
											<div>
												<span class="text-zinc-400">{t('audit.orgId')}：</span>
												<span class="font-mono">{e.orgId || '—'}</span>
											</div>
											<div>
												<span class="text-zinc-400">{t('audit.recoveryTime')}：</span>
												{recoveryLabel(e)}
											</div>
											<div class="sm:col-span-2">
												<span class="text-zinc-400">{t('audit.errorDetails')}：</span>
												<span class="whitespace-pre-wrap break-all font-mono text-destructive">
													{e.errorDetails || '—'}
												</span>
											</div>
										</div>
									</TableCell>
								</TableRow>
							{/if}
						{/each}
					</TableBody>
				</Table>
			</CardContent>
		</Card>
		<div class="mt-3">
			<Pagination total={entries.length} bind:page bind:pageSize />
		</div>
	{/if}
</div>
