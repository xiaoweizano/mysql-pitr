<script lang="ts">
	import { page } from '$app/state';
	import { t } from '$lib/i18n.svelte.js';
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
		getStatus,
		getTransactions,
		pauseOp,
		resumeOp,
		cancelOp,
		isTerminalState,
		type ExecError,
		type OperationStatus,
		type ProgressPayload,
		type TxPreview
	} from '$lib/api/pitr.js';
	import { connectSSE, type SSEEventKind } from '$lib/sse.js';
	import TxTable from '$lib/components/pitr/TxTable.svelte';
	import SqlPreview from '$lib/components/pitr/SqlPreview.svelte';
	import ExecutePanel from '$lib/components/pitr/ExecutePanel.svelte';
	import { LoaderCircle, CircleAlert, ArrowLeft, Square } from '@lucide/svelte';
	import { formatDateTime } from '$lib/format.js';

	/**
	 * Operation detail page — live monitoring of a PITR operation:
	 * status + progress + transaction preview + SQL list + errors, reusing the
	 * same components as the wizard (TxTable / SqlPreview / ExecutePanel).
	 * Opens one SSE connection for live events (closed on unmount) and falls
	 * back to /status polling while the stream is down.
	 */

	const opId = $derived(page.params.id ?? '');

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let status = $state<OperationStatus | null>(null);
	let txs = $state<TxPreview[]>([]);
	let previewTruncated = $state(false);
	let progress = $state<ProgressPayload | null>(null);
	let execErrors = $state<ExecError[]>([]);
	let terminalMessage = $state<string | null>(null);
	let busy = $state(false);
	let flowError = $state<string | null>(null);
	let sseConnected = $state(false);
	let sseReconnecting = $state(false);

	const opState = $derived(status?.status ?? 'created');
	const isTerminal = $derived(isTerminalState(opState));

	// ---------- load + SSE (one effect, keyed on the route id) ----------

	function extractErrorMessage(data: unknown): string {
		if (typeof data === 'string') return data;
		if (data && typeof data === 'object') {
			const o = data as Record<string, unknown>;
			if (typeof o.message === 'string' && o.message) return o.message;
			if (typeof o.status === 'string') {
				if (o.status === 'blocked') return t('pitr.error.blocked');
				if (o.status === 'failed') return t('pitr.error.failed');
				return o.status;
			}
		}
		return t('pitr.error.generic');
	}

	$effect(() => {
		const id = opId;
		let stale = false;

		loading = true;
		loadError = null;
		sseConnected = false;
		sseReconnecting = false;

		void (async () => {
			try {
				const [st, txResp] = await Promise.all([getStatus(id), getTransactions(id)]);
				if (stale) return;
				status = st;
				txs = txResp.transactions;
				previewTruncated = txResp.previewTruncated ?? false;
				if (isTerminalState(st.status)) {
					// 终态预览已释放 — nothing more to stream.
				}
			} catch (e) {
				if (!stale) loadError = e instanceof Error ? e.message : String(e);
			} finally {
				if (!stale) loading = false;
			}
		})();

		const client = connectSSE(id, {
			onopen: () => {
				sseConnected = true;
				sseReconnecting = false;
			},
			onerror: (msg, fatal) => {
				sseConnected = false;
				sseReconnecting = !fatal;
				if (fatal) flowError = msg;
			},
			onkind: (kind, data) => handleSseKind(kind, data),
			onterminal: () => {
				sseConnected = false;
				sseReconnecting = false;
			}
		});

		return () => {
			stale = true;
			client.close();
		};
	});

	// Polling fallback while SSE is down: observe terminal transitions.
	$effect(() => {
		if (!opId || isTerminal || sseConnected) return;
		const iv = setInterval(async () => {
			if (!opId || isTerminalState(opState)) {
				clearInterval(iv);
				return;
			}
			try {
				const st = await getStatus(opId);
				if (st.status !== opState) status = st;
			} catch {
				// transient — keep polling
			}
		}, 15000);
		return () => clearInterval(iv);
	});

	async function refreshTransactions() {
		if (!opId) return;
		try {
			const resp = await getTransactions(opId);
			status = { ...(status ?? ({} as OperationStatus)), status: resp.status } as OperationStatus;
			txs = resp.transactions;
			previewTruncated = resp.previewTruncated ?? false;
		} catch {
			// transient
		}
	}

	function handleSseKind(kind: SSEEventKind, data: unknown) {
		switch (kind) {
			case 'tx_meta': {
				const meta = data as TxPreview;
				const exists = txs.some((x) => x.txId === meta.txId);
				txs = exists ? txs.map((x) => (x.txId === meta.txId ? meta : x)) : [...txs, meta];
				break;
			}
			case 'scan_done':
				void refreshTransactions();
				break;
			case 'progress': {
				const p = data as ProgressPayload;
				progress = p;
				if (p?.errors?.length) execErrors = p.errors;
				break;
			}
			case 'op_paused': {
				const p = data as { status?: string; done?: number; total?: number };
				if (status) status = { ...status, status: 'paused' };
				if (p && (p.done !== undefined || p.total !== undefined)) {
					progress = { done: p.done ?? progress?.done ?? 0, total: p.total ?? progress?.total ?? 0 };
				}
				break;
			}
			case 'op_done': {
				const payload = data as {
					done?: number;
					total?: number;
					paused?: boolean;
					errors?: ExecError[];
					status?: string;
				};
				if (payload?.paused) {
					if (status) status = { ...status, status: 'paused' };
					return;
				}
				const next = payload?.status === 'cancelled' ? 'cancelled' : 'done';
				if (status) status = { ...status, status: next };
				if (next === 'cancelled') terminalMessage = t('pitr.error.cancelled');
				if (payload?.done !== undefined) progress = { done: payload.done, total: payload.total ?? 0 };
				if (payload?.errors?.length) execErrors = payload.errors;
				break;
			}
			case 'op_error': {
				if (status) status = { ...status, status: 'failed' };
				terminalMessage = extractErrorMessage(data);
				break;
			}
			default:
				break;
		}
	}

	// ---------- controls ----------

	async function handlePause() {
		if (!opId) return;
		busy = true;
		try {
			const r = await pauseOp(opId);
			if (status) status = { ...status, status: r.status };
		} catch (e) {
			flowError = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function handleResume() {
		if (!opId) return;
		busy = true;
		try {
			const r = await resumeOp(opId);
			if (status) status = { ...status, status: r.status };
		} catch (e) {
			flowError = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function handleCancel() {
		if (!opId) return;
		busy = true;
		try {
			const r = await cancelOp(opId);
			if (status) status = { ...status, status: r.status };
			if (r.status === 'cancelled') terminalMessage = t('pitr.error.cancelled');
		} catch (e) {
			flowError = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	// ---------- presentation ----------

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
</script>

<svelte:head>
	<title>{t('pitr.detail.title')} · {t('app.name')}</title>
</svelte:head>

<div class="p-6">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<div class="flex items-center gap-3">
			<Button variant="outline" size="sm" href="/operations">
				<ArrowLeft class="size-3.5" />
				{t('pitr.detail.back')}
			</Button>
			<div>
				<h1 class="text-xl font-semibold text-zinc-900">{t('pitr.detail.title')}</h1>
				<p class="font-mono text-xs text-zinc-400">{opId}</p>
			</div>
		</div>
		{#if status}
			<span class={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${badgeClass(opState)}`}>
				{t(`pitr.status.${opState}`)}
			</span>
		{/if}
	</div>

	{#if loadError}
		<Card class="mt-4">
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">{t('pitr.detail.loadError')}：{loadError}</p>
			</CardContent>
		</Card>
	{:else if loading || !status}
		<Card class="mt-4">
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else}
		{#if flowError}
			<div class="mt-4 flex items-start gap-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">
				<CircleAlert class="mt-0.5 size-4 shrink-0" />
				<span class="break-all">{flowError}</span>
			</div>
		{/if}
		{#if sseReconnecting && !isTerminal}
			<div class="mt-4 flex items-center gap-2 rounded-md bg-amber-50 px-3 py-2 text-sm text-amber-700">
				<LoaderCircle class="size-4 animate-spin" />
				<span>{t('pitr.sse.reconnecting')}</span>
			</div>
		{/if}

		<div class="mt-4 grid grid-cols-1 gap-6 lg:grid-cols-2">
			<Card>
				<CardHeader>
					<CardTitle>{t('pitr.detail.meta')}</CardTitle>
				</CardHeader>
				<CardContent class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
					<span class="text-zinc-500">{t('pitr.detail.type')}</span>
					<span>{t(`pitr.type.short.${status.type}`)}</span>

					<span class="text-zinc-500">{t('pitr.detail.mode')}</span>
					<span>{t(`pitr.mode.${status.mode}`)}</span>

					<span class="text-zinc-500">{t('pitr.detail.agent')}</span>
					<span class="break-all font-mono text-xs text-zinc-700">{status.agentId}</span>

					<span class="text-zinc-500">{t('pitr.detail.createdAt')}</span>
					<span>{formatDateTime(status.createdAt)}</span>

					<span class="text-zinc-500">{t('pitr.detail.createdBy')}</span>
					<span class="break-all font-mono text-xs text-zinc-700">{status.createdBy}</span>
				</CardContent>
				<CardFooter class="justify-end">
					{#if (opState === 'scanning' || opState === 'ready') && !isTerminal}
						<Button variant="destructive" size="sm" onclick={handleCancel} disabled={busy}>
							<Square class="size-3.5" />
							{t('pitr.exec.cancel')}
						</Button>
					{/if}
				</CardFooter>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>{t('pitr.detail.execution')}</CardTitle>
					<CardDescription>{t('pitr.detail.executionDesc')}</CardDescription>
				</CardHeader>
				<CardContent>
					<ExecutePanel
						status={opState}
						{progress}
						errors={execErrors}
						errorMessage={terminalMessage}
						{busy}
						onPause={handlePause}
						onResume={handleResume}
						onCancel={handleCancel}
					/>
				</CardContent>
			</Card>
		</div>

		{#if txs.length > 0}
			<Card class="mt-6">
				<CardHeader>
					<CardTitle>{t('pitr.detail.transactions')}</CardTitle>
				</CardHeader>
				<CardContent>
					<TxTable transactions={txs} {previewTruncated} />
				</CardContent>
			</Card>

			<Card class="mt-6">
				<CardHeader>
					<CardTitle>{t('pitr.detail.sql')}</CardTitle>
				</CardHeader>
				<CardContent>
					<SqlPreview transactions={txs} readonly />
				</CardContent>
			</Card>
		{:else if isTerminal}
			<Card class="mt-6">
				<CardContent class="py-6 text-center text-sm text-zinc-400">
					{t('pitr.detail.terminalNoPreview')}
				</CardContent>
			</Card>
		{/if}
	{/if}
</div>
