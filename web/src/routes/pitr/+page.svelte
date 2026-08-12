<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
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
	import { Input } from '$lib/components/ui/input/index.js';
	import { Label } from '$lib/components/ui/label/index.js';
	import {
		startPITR,
		getStatus,
		getTransactions,
		selectTx,
		executeOp,
		pauseOp,
		resumeOp,
		cancelOp,
		isTerminalState,
		type ExecError,
		type PitrMode,
		type PitrState,
		type PitrType,
		type ProgressPayload,
		type ScanFilter,
		type ScanTableRef,
		type SqlStatement,
		type TxPreview
	} from '$lib/api/pitr.js';
	import { listAgents, type AgentInfo } from '$lib/api/agents.js';
	import { listOrgs } from '$lib/api/orgs.js';
	import { connectSSE, type SSEClient, type SSEEventKind } from '$lib/sse.js';
	import { decideScanDone, selectionChanged, type ScanFlowMode } from '$lib/pitr/scan-flow.js';
	import TxTable from '$lib/components/pitr/TxTable.svelte';
	import SqlPreview from '$lib/components/pitr/SqlPreview.svelte';
	import ExecutePanel from '$lib/components/pitr/ExecutePanel.svelte';
	import { LoaderCircle, CircleAlert, Database, RefreshCcw } from '@lucide/svelte';

	/**
	 * PITR recovery wizard — five steps driving the v3 API:
	 *   1 类型与实例   →  type (flashback/update_rollback/pitr) + mode (sql/selected)
	 *   2 过滤条件     →  tables / time range / GTID set → POST /api/pitr/start
	 *   3 事务扫描     →  SSE tx_meta stream + scan_done (selected 模式在此勾选事务→select→二次扫描)
	 *   4 SQL 预览勾选 →  SqlPreview → select → execute
	 *   5 执行         →  SSE progress + pause/resume/cancel + 终态
	 *
	 * One SSE connection lives for the whole operation (opId set → terminal);
	 * the $effect cleanup closes it on page unmount (memory safety).
	 */

	// ---------- step 1: recovery type + agent ----------

	type TypeChoice = 'flashback' | 'update_rollback' | 'pitr_time' | 'pitr_tx' | 'gtid';

	const TYPE_OPTIONS: { value: TypeChoice; title: string; desc: string }[] = [
		{ value: 'flashback', title: 'pitr.type.flashback', desc: 'pitr.type.flashback.desc' },
		{ value: 'update_rollback', title: 'pitr.type.updateRollback', desc: 'pitr.type.updateRollback.desc' },
		{ value: 'pitr_time', title: 'pitr.type.pitrTime', desc: 'pitr.type.pitrTime.desc' },
		{ value: 'pitr_tx', title: 'pitr.type.pitrTx', desc: 'pitr.type.pitrTx.desc' },
		{ value: 'gtid', title: 'pitr.type.gtid', desc: 'pitr.type.gtid.desc' }
	];

	let step = $state(1);
	let typeChoice = $state<TypeChoice>('pitr_time');
	let agentId = $state('');
	let agents = $state<AgentInfo[]>([]);
	let agentsLoading = $state(true);
	let agentsError = $state<string | null>(null);
	let offlinePreselect = $state(false);

	/** "sql" for flashback/update_rollback/指定时间; "selected" for 指定事务/GTID. */
	const mode = $derived<PitrMode>(typeChoice === 'pitr_tx' || typeChoice === 'gtid' ? 'selected' : 'sql');
	/** The wizard only ever runs sql or selected — meta is a server-internal mode. */
	const scanMode = $derived<ScanFlowMode>(mode === 'selected' ? 'selected' : 'sql');
	const opType = $derived<PitrType>(
		typeChoice === 'flashback' ? 'flashback' : typeChoice === 'update_rollback' ? 'update_rollback' : 'pitr'
	);

	// ---------- step 2: filter ----------

	let tablesText = $state('');
	let timeStart = $state('');
	let timeEnd = $state('');
	let gtidText = $state('');
	// type="number" 输入的 bind:value 绑定为 number | null（空输入为 null）
	let maxRowsPerTx = $state<number | null>(null);
	let formError = $state<string | null>(null);
	let starting = $state(false);

	// ---------- operation / scan / execution state ----------

	let opId = $state<string | null>(null);
	let opStatus = $state<PitrState>('created');
	let previewTruncated = $state(false);
	let txs = $state<TxPreview[]>([]);
	let received = $state(0);
	let sqlReceived = $state(0);
	let selected = $state<string[]>([]);
	let selecting = $state(false);
	let rescanning = $state(false);
	let pendingExecute = $state(false);
	let lastSelected = $state<string[] | null>(null);
	let sqlChecked = $state<string[]>([]);
	let startingExec = $state(false);
	let progress = $state<ProgressPayload | null>(null);
	let execErrors = $state<ExecError[]>([]);
	let terminalMessage = $state<string | null>(null);
	let busy = $state(false);
	let flowError = $state<string | null>(null);

	// ---------- SSE ----------

	let sseConnected = $state(false);
	let sseReconnecting = $state(false);
	let sseClient: SSEClient | null = null;

	$effect(() => {
		const id = opId;
		if (!id) return;
		sseConnected = false;
		sseReconnecting = false;
		const client = connectSSE(id, {
			onopen: () => {
				sseConnected = true;
				sseReconnecting = false;
				flowError = null;
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
		sseClient = client;
		return () => {
			client.close();
			sseClient = null;
		};
	});

	// Polling safety net while the wizard is parked waiting for a scan (step 3,
	// or a selected-mode rescan — which also runs at step 3):
	//
	//   - The server event bus is small and non-blocking, so a scan_done frame
	//     can be dropped even while the SSE stream stays connected.
	//   - A reconnect after the stream dropped at scan completion never replays
	//     history.
	//
	// So the step advance cannot rely on the event alone: watch /status (the
	// authoritative archive state) and advance the wizard from the snapshot when
	// it reports ready. Outside step 3 this poll also preserves the original
	// fallback of observing terminal transitions while the stream is down.
	const SCAN_POLL_MS = 10_000;
	$effect(() => {
		const id = opId;
		if (
			!id ||
			isTerminalState(opStatus) ||
			(step !== 3 && sseConnected && !rescanning && !pendingExecute)
		) {
			return;
		}
		let inFlight = false;
		const iv = setInterval(async () => {
			if (inFlight) return;
			if (!opId || isTerminalState(opStatus)) {
				clearInterval(iv);
				return;
			}
			inFlight = true;
			try {
				const st = await getStatus(opId);
				if (st.status !== opStatus) {
					opStatus = st.status;
					if (isTerminalState(st.status)) {
						clearInterval(iv);
						return;
					}
				}
				if (opStatus === 'ready' && step === 3) {
					await advanceAfterScanDone();
				}
			} catch {
				// transient — keep polling
			} finally {
				inFlight = false;
			}
		}, SCAN_POLL_MS);
		return () => clearInterval(iv);
	});

	// ---------- agent loading ----------

	async function loadAgents() {
		agentsLoading = true;
		agentsError = null;
		offlinePreselect = false;
		try {
			const orgs = await listOrgs();
			const perOrg = await Promise.all(orgs.map((o) => listAgents(o.id)));
			const approved = perOrg
				.flat()
				.filter((a) => a.approved)
				.sort((a, b) => {
					const oa = a.status === 'online' ? 0 : 1;
					const ob = b.status === 'online' ? 0 : 1;
					return oa - ob || a.hostname.localeCompare(b.hostname);
				});
			agents = approved;
			const pre = page.url.searchParams.get('agentId');
			if (pre && approved.some((a) => a.id === pre)) {
				agentId = pre;
				offlinePreselect = approved.find((a) => a.id === pre)?.status !== 'online';
			}
		} catch (e) {
			agentsError = e instanceof Error ? e.message : String(e);
		} finally {
			agentsLoading = false;
		}
	}

	onMount(() => {
		void loadAgents();
	});

	// ---------- filter parsing ----------

	function parseTables(text: string): { refs: ScanTableRef[]; error: string | null } {
		const parts = text
			.split(/[\n,;]+/)
			.map((s) => s.trim())
			.filter(Boolean);
		const refs: ScanTableRef[] = [];
		for (const p of parts) {
			const dot = p.indexOf('.');
			if (dot <= 0 || dot === p.length - 1) {
				return { refs, error: `${t('pitr.filter.error.invalidTable')} ${p}` };
			}
			refs.push({ schema: p.slice(0, dot), table: p.slice(dot + 1) });
		}
		return { refs, error: null };
	}

	function buildFilter(): { filter: ScanFilter; error: string | null } {
		const { refs, error: tableError } = parseTables(tablesText);
		if (tableError) return { filter: {}, error: tableError };

		const filter: ScanFilter = {};
		if (refs.length > 0) filter.tables = refs;
		// datetime-local is browser-local time; toISOString converts to the
		// UTC RFC3339 form the backend validates against.
		if (timeStart) filter.timeStart = new Date(timeStart).toISOString();
		if (timeEnd) filter.timeEnd = new Date(timeEnd).toISOString();
		if (gtidText.trim()) filter.gtidSet = gtidText.trim();
		// maxRowsPerTx 是 number | null：null/0/非有限值均视为「未填」，由后端用默认值
		if (maxRowsPerTx != null && Number.isFinite(maxRowsPerTx) && maxRowsPerTx > 0) {
			if (maxRowsPerTx > 1_000_000) return { filter, error: t('pitr.filter.error.maxRows') };
			filter.maxRowsPerTx = maxRowsPerTx;
		}

		if (typeChoice === 'flashback' || typeChoice === 'update_rollback' || typeChoice === 'pitr_time') {
			if (!filter.timeEnd) return { filter, error: t('pitr.filter.error.timeRequired') };
		}
		if (typeChoice === 'gtid') {
			if (!filter.gtidSet) return { filter, error: t('pitr.filter.error.gtidRequired') };
		}
		if (typeChoice === 'pitr_tx') {
			if (!filter.timeStart && !filter.timeEnd && !filter.gtidSet) {
				return { filter, error: t('pitr.filter.error.rangeOrGtid') };
			}
		}
		return { filter, error: null };
	}

	async function handleStart() {
		formError = null;
		if (!agentId) {
			formError = t('pitr.agent.required');
			return;
		}
		// 整体包在 try 里：任何意外异常都显示具体原因，而不是静默失败
		try {
			const { filter, error } = buildFilter();
			if (error) {
				formError = error;
				return;
			}
			starting = true;
			try {
				const res = await startPITR({ agentId, type: opType, mode, filter });
				opId = res.operationId;
				opStatus = res.status;
				received = 0;
				sqlReceived = 0;
				txs = [];
				progress = null;
				execErrors = [];
				step = 3;
			} finally {
				starting = false;
			}
		} catch (e) {
			formError = e instanceof Error ? e.message : String(e);
		}
	}

	// ---------- SSE event handling ----------

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

	function mergeErrors(errors: ExecError[] | undefined) {
		if (!errors || errors.length === 0) return;
		execErrors = errors;
	}

	async function refreshTransactions() {
		if (!opId) return;
		try {
			const resp = await getTransactions(opId);
			if (resp.status) opStatus = resp.status;
			previewTruncated = resp.previewTruncated ?? false;
			txs = resp.transactions;
		} catch {
			// SSE keeps streaming; the next scan_done retries the snapshot.
		}
	}

	/**
	 * A scan finished — either via the SSE `scan_done` event or via the
	 * /status polling safety net (see the effect above; the frame can be
	 * dropped by the small non-blocking event bus, or the stream may have been
	 * down at scan completion and reconnects never replay history). Both paths
	 * converge here, advancing the wizard from the authoritative
	 * /transactions snapshot and the pure state-machine decisions.
	 *
	 * Critical: when the rescan was triggered by the step-4 execute button
	 * (pendingExecute), the checkboxes are NOT rebuilt from the step-3
	 * selection — rebuilding would resurrect transactions the user just
	 * unchecked (decideScanDone initSqlChecked:false).
	 */
	async function advanceAfterScanDone() {
		await refreshTransactions();
		const d = decideScanDone(scanMode, rescanning, pendingExecute);
		if (d.initSqlChecked) initSqlChecked();
		if (d.advanceStep) {
			rescanning = false;
			step = 4;
		}
		if (d.continueExecute) {
			pendingExecute = false;
			void startExecute();
		}
	}

	function initSqlChecked() {
		const withSql = txs.filter((tx) => tx.sql && tx.sql.length > 0).map((tx) => tx.txId);
		// 默认勾选所有已有 SQL 的事务；selected 模式默认勾选步骤 3 所选事务。
		sqlChecked =
			mode === 'selected' ? [...selected.filter((id) => withSql.includes(id))] : [...withSql];
	}

	async function handleSseKind(kind: SSEEventKind, data: unknown) {
		switch (kind) {
			case 'tx_meta': {
				const meta = data as TxPreview;
				received += 1;
				const exists = txs.some((x) => x.txId === meta.txId);
				txs = exists ? txs.map((x) => (x.txId === meta.txId ? meta : x)) : [...txs, meta];
				break;
			}
			case 'sql': {
				const stmts = (data as SqlStatement[]) ?? [];
				sqlReceived += stmts.length;
				break;
			}
			case 'scan_done':
				await advanceAfterScanDone();
				break;
			case 'progress': {
				const p = data as ProgressPayload;
				progress = p;
				mergeErrors(p?.errors);
				break;
			}
			case 'op_paused': {
				const p = data as { status?: string; done?: number; total?: number };
				opStatus = 'paused';
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
					// 暂停确认 — 流保持打开，操作可恢复
					opStatus = 'paused';
					return;
				}
				opStatus = payload?.status === 'cancelled' ? 'cancelled' : 'done';
				if (opStatus === 'cancelled') {
					terminalMessage = t('pitr.error.cancelled');
				}
				if (payload?.done !== undefined) progress = { done: payload.done, total: payload.total ?? 0 };
				mergeErrors(payload?.errors);
				step = 5;
				break;
			}
			case 'op_error': {
				opStatus = 'failed';
				terminalMessage = extractErrorMessage(data);
				step = 5;
				break;
			}
		}
	}

	// ---------- selected-mode selection (step 3) ----------

	async function handleSelect() {
		if (!opId) return;
		if (selected.length === 0) {
			flowError = t('pitr.scan.selectRequired');
			return;
		}
		selecting = true;
		flowError = null;
		try {
			const res = await selectTx(opId, selected);
			lastSelected = [...selected];
			opStatus = res.status;
			rescanning = true;
		} catch (e) {
			flowError = e instanceof Error ? e.message : String(e);
		} finally {
			selecting = false;
		}
	}

	// ---------- execute (step 4 → 5) ----------

	/**
	 * executeOp failed (network blip / 5xx / 409). The POST may or may not have
	 * reached the server — reload the authoritative status: if the operation
	 * actually started executing (or already finished), move to step 5 and let
	 * the SSE stream drive the view; otherwise stay on step 4, where the
	 * execute button remains enabled for a retry. (Minor: without this, a
	 * response lost after the server started executing stranded the wizard on
	 * step 4 forever.)
	 */
	async function reconcileAfterExecuteFailure() {
		if (!opId) return;
		try {
			const st = await getStatus(opId);
			opStatus = st.status;
			if (st.status === 'executing' || st.status === 'paused' || isTerminalState(st.status)) {
				step = 5;
				flowError = null;
			}
		} catch {
			// status unreachable — stay on step 4; the retry stays available
		}
	}

	async function startExecute() {
		if (!opId) return;
		const txIds = [...sqlChecked];
		if (txIds.length === 0) {
			flowError = t('pitr.sql.required');
			return;
		}
		startingExec = true;
		flowError = null;
		try {
			// 所选事务与已持久化的选择一致时直接执行；否则先 select。
			if (selectionChanged(lastSelected, txIds)) {
				const sel = await selectTx(opId, txIds);
				lastSelected = [...txIds];
				if (sel.rescan) {
					// selected 模式：二次扫描在途，scan_done 后自动执行
					pendingExecute = true;
					rescanning = true;
					opStatus = sel.status;
					step = 3;
					return;
				}
			}
			const ex = await executeOp(opId);
			opStatus = ex.status;
			step = 5;
		} catch (e) {
			flowError = e instanceof Error ? e.message : String(e);
			await reconcileAfterExecuteFailure();
		} finally {
			startingExec = false;
		}
	}

	// ---------- execution controls (step 5) ----------

	async function handlePause() {
		if (!opId) return;
		busy = true;
		try {
			const r = await pauseOp(opId);
			opStatus = r.status;
		} catch (e) {
			terminalMessage = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function handleResume() {
		if (!opId) return;
		busy = true;
		try {
			const r = await resumeOp(opId);
			opStatus = r.status;
		} catch (e) {
			terminalMessage = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function handleCancel() {
		if (!opId) return;
		busy = true;
		try {
			const r = await cancelOp(opId);
			opStatus = r.status;
			if (r.status === 'cancelled') {
				terminalMessage = t('pitr.error.cancelled');
				step = 5;
			}
		} catch (e) {
			terminalMessage = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	// ---------- navigation / reset ----------

	function resetWizard() {
		step = 1;
		opId = null;
		opStatus = 'created';
		txs = [];
		received = 0;
		sqlReceived = 0;
		selected = [];
		sqlChecked = [];
		lastSelected = null;
		progress = null;
		execErrors = [];
		terminalMessage = null;
		previewTruncated = false;
		rescanning = false;
		pendingExecute = false;
		flowError = null;
		formError = null;
	}

	function goToStep(n: number) {
		// 仅允许在未发起操作前回退（步骤 1↔2）；步骤 3-5 由状态机驱动。
		if (opId === null && (n === 1 || n === 2)) step = n;
	}

	const isTerminal = $derived(isTerminalState(opStatus));

	function selectedAgentOnline(): boolean {
		const a = agents.find((x) => x.id === agentId);
		return a ? a.status === 'online' : true;
	}

	function formatTypeLabel(): string {
		return t(`pitr.type.short.${opType}`);
	}
</script>

<svelte:head>
	<title>{t('pitr.title')} · {t('app.name')}</title>
</svelte:head>

<div class="p-6">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-zinc-900">{t('pitr.title')}</h1>
			<p class="text-sm text-zinc-500">{t('pitr.subtitle')}</p>
		</div>
	</div>

	<!-- 步骤条 -->
	<ol class="mt-6 mb-6 flex flex-wrap items-center gap-1 text-xs">
		{#each [1, 2, 3, 4, 5] as n, i}
			<li class="flex items-center gap-1">
				<button
					type="button"
					onclick={() => goToStep(n)}
					disabled={opId !== null}
					class={`flex items-center gap-1.5 rounded-full px-2 py-1 transition-colors disabled:cursor-not-allowed ${
						step === n ? 'bg-zinc-900 text-white' : 'text-zinc-500 hover:bg-zinc-100'
					} ${opId !== null && n !== step ? 'opacity-60' : ''}`}
				>
					<span
						class={`inline-flex size-4 items-center justify-center rounded-full text-[10px] ${
							step === n ? 'bg-white/20' : 'bg-zinc-200'
						}`}
					>
						{i + 1}
					</span>
					{t(`pitr.step.${n}.label`)}
				</button>
				{#if i < 4}<span class="mx-0.5 h-px w-3 bg-zinc-200"></span>{/if}
			</li>
		{/each}
	</ol>

	<!-- 全局错误横幅 -->
	{#if flowError}
		<div class="mb-4 flex items-start gap-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span class="break-all">{flowError}</span>
		</div>
	{/if}
	{#if sseReconnecting && !isTerminal}
		<div class="mb-4 flex items-center gap-2 rounded-md bg-amber-50 px-3 py-2 text-sm text-amber-700">
			<LoaderCircle class="size-4 animate-spin" />
			<span>{t('pitr.sse.reconnecting')}</span>
		</div>
	{/if}

	<!-- ============ 步骤 1：类型与实例 ============ -->
	{#if step === 1}
		<Card>
			<CardHeader>
				<CardTitle>{t('pitr.type.title')}</CardTitle>
				<CardDescription>{t('pitr.type.subtitle')}</CardDescription>
			</CardHeader>
			<CardContent>
				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
					{#each TYPE_OPTIONS as opt}
						<button
							type="button"
							onclick={() => {
								typeChoice = opt.value;
								formError = null;
							}}
							class={`rounded-lg border p-3 text-left transition-colors ${
								typeChoice === opt.value
									? 'border-zinc-900 bg-zinc-900 text-white'
									: 'border-zinc-200 bg-white hover:border-zinc-400'
							}`}
						>
							<p class="text-sm font-medium">{t(opt.title)}</p>
							<p class={`mt-1 text-xs ${typeChoice === opt.value ? 'text-zinc-300' : 'text-zinc-500'}`}>
								{t(opt.desc)}
							</p>
						</button>
					{/each}
				</div>

				<div class="mt-5">
					<Label>{t('pitr.agent.title')}</Label>
					{#if agentsLoading}
						<p class="mt-2 text-sm text-zinc-400">{t('common.loading')}</p>
					{:else if agentsError}
						<div class="mt-2 flex items-center justify-between gap-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">
							<span class="break-all">{agentsError}</span>
							<Button variant="outline" size="xs" onclick={loadAgents}>{t('common.retry')}</Button>
						</div>
					{:else if agents.length === 0}
						<div class="mt-2 rounded-md bg-zinc-50 px-3 py-2 text-sm text-zinc-500">
							{t('pitr.agent.none')}
						</div>
					{:else}
						<div class="mt-2 grid grid-cols-1 gap-1.5 sm:grid-cols-2">
							{#each agents as agent}
								<button
									type="button"
									onclick={() => {
										agentId = agent.id;
										formError = null;
									}}
									disabled={agent.status !== 'online'}
									class={`flex items-center gap-2 rounded-lg border px-3 py-2 text-left text-sm transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${
										agentId === agent.id
											? 'border-zinc-900 bg-zinc-900 text-white'
											: 'border-zinc-200 bg-white hover:border-zinc-400'
									}`}
									title={agent.status !== 'online' ? t('pitr.agent.offline') : agent.hostname}
								>
									<Database class="size-4 shrink-0" />
									<span class="min-w-0 flex-1 truncate">{agent.hostname}</span>
									<span
										class={`inline-flex shrink-0 items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium ${
											agent.status === 'online' ? 'bg-emerald-100 text-emerald-700' : 'bg-zinc-100 text-zinc-500'
										}`}
									>
										{agent.status === 'online' ? t('instances.status.online') : t('instances.status.offline')}
									</span>
								</button>
							{/each}
						</div>
					{/if}
					{#if offlinePreselect}
						<p class="mt-2 text-xs text-amber-600">{t('pitr.agent.offlineHint')}</p>
					{/if}
				</div>
			</CardContent>
			<CardFooter class="justify-end">
				{#if formError}
					<p class="mr-3 text-xs text-destructive">{formError}</p>
				{/if}
				<Button onclick={() => { step = 2; formError = null; }} disabled={!agentId || !selectedAgentOnline()}>
					{t('pitr.next')}
				</Button>
			</CardFooter>
		</Card>

	<!-- ============ 步骤 2：过滤条件 ============ -->
	{:else if step === 2}
		<Card>
			<CardHeader>
				<CardTitle>{t('pitr.filter.title')}</CardTitle>
				<CardDescription>{t('pitr.filter.subtitle', { mode, type: formatTypeLabel() })}</CardDescription>
			</CardHeader>
			<CardContent class="space-y-4">
				<div>
					<Label for="tables">{t('pitr.filter.tables')}</Label>
					<textarea
						id="tables"
						class="mt-1.5 w-full rounded-lg border border-input bg-transparent px-2.5 py-1.5 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
						rows="3"
						placeholder={t('pitr.filter.tables.placeholder')}
						bind:value={tablesText}
					></textarea>
				</div>
				<div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
					<div>
						<Label for="timeStart">{t('pitr.filter.timeStart')}</Label>
						<Input id="timeStart" type="datetime-local" class="mt-1.5" bind:value={timeStart} />
					</div>
					<div>
						<Label for="timeEnd">{t('pitr.filter.timeEnd')}</Label>
						<Input id="timeEnd" type="datetime-local" class="mt-1.5" bind:value={timeEnd} />
					</div>
				</div>
				<div>
					<Label for="gtid">{t('pitr.filter.gtid')}</Label>
					<textarea
						id="gtid"
						class="mt-1.5 w-full rounded-lg border border-input bg-transparent px-2.5 py-1.5 font-mono text-xs outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
						rows="2"
						placeholder={t('pitr.filter.gtid.placeholder')}
						bind:value={gtidText}
					></textarea>
				</div>
				<div>
					<Label for="maxRows">{t('pitr.filter.maxRowsPerTx')}</Label>
					<Input id="maxRows" type="number" min="0" step="1" class="mt-1.5 max-w-56" bind:value={maxRowsPerTx} />
				</div>
			</CardContent>
			<CardFooter class="justify-between">
				<Button variant="outline" onclick={() => { step = 1; formError = null; }}>
					{t('pitr.back')}
				</Button>
				<div class="flex items-center gap-3">
					{#if formError}
						<p class="text-xs text-destructive">{formError}</p>
					{/if}
					<Button onclick={handleStart} disabled={starting || !agentId}>
						{#if starting}
							<LoaderCircle class="size-4 animate-spin" />
							{t('common.loading')}
						{:else}
							{t('pitr.filter.start')}
						{/if}
					</Button>
				</div>
			</CardFooter>
		</Card>

	<!-- ============ 步骤 3：事务扫描 ============ -->
	{:else if step === 3}
		<Card>
			<CardHeader>
				<CardTitle>{t('pitr.scan.title')}</CardTitle>
				<CardDescription>
					{#if rescanning}
						<span class="text-amber-600">{t('pitr.scan.rescanning')}</span>
					{:else if opStatus === 'scanning'}
						{t('pitr.scan.scanning')}
					{:else if opStatus === 'ready' && mode === 'selected'}
						{t('pitr.scan.selectHint')}
					{:else}
						{t('pitr.scan.title')}
					{/if}
				</CardDescription>
			</CardHeader>
			<CardContent>
				{#if opStatus === 'scanning'}
					<div class="mb-2 flex items-center gap-2 text-sm text-zinc-500">
						<LoaderCircle class="size-4 animate-spin text-zinc-700" />
						<span>
							{t('pitr.scan.received', { count: String(received), sql: String(sqlReceived) })}
						</span>
					</div>
				{/if}
				<TxTable
					transactions={txs}
					{previewTruncated}
					selectable={opStatus === 'ready' && mode === 'selected'}
					bind:checked={selected}
				/>
				{#if opStatus === 'ready' && mode === 'selected'}
					<div class="mt-4 flex items-center gap-2">
						<Button onclick={handleSelect} disabled={selecting || selected.length === 0}>
							{#if selecting}
								<LoaderCircle class="size-4 animate-spin" />
								{t('common.loading')}
							{:else}
								{t('pitr.scan.selectBtn', { count: String(selected.length) })}
							{/if}
						</Button>
						{#if rescanning}
							<span class="text-sm text-amber-600">{t('pitr.scan.rescanning')}</span>
						{/if}
					</div>
				{/if}
			</CardContent>
			<CardFooter class="justify-between">
				<Button variant="destructive" size="sm" onclick={handleCancel} disabled={busy || isTerminal}>
					{t('pitr.scan.cancel')}
				</Button>
			</CardFooter>
		</Card>

	<!-- ============ 步骤 4：SQL 预览勾选 ============ -->
	{:else if step === 4}
		<Card>
			<CardHeader>
				<CardTitle>{t('pitr.sql.title')}</CardTitle>
				<CardDescription>
					{#if mode === 'selected'}
						{t('pitr.sql.selectedHint', { count: String(sqlChecked.length) })}
					{:else}
						{t('pitr.sql.checkAllHint')}
					{/if}
				</CardDescription>
			</CardHeader>
			<CardContent>
				<SqlPreview transactions={txs} bind:checked={sqlChecked} />
			</CardContent>
			<CardFooter class="justify-between">
				<Button variant="outline" onclick={handleCancel} disabled={busy || isTerminal}>
					{t('pitr.sql.cancelOp')}
				</Button>
				<div class="flex items-center gap-3">
					{#if flowError}
						<p class="text-xs text-destructive">{flowError}</p>
					{/if}
					<Button
						onclick={startExecute}
						disabled={startingExec || sqlChecked.length === 0}
					>
						{#if startingExec}
							<LoaderCircle class="size-4 animate-spin" />
							{t('common.loading')}
						{:else}
							{t('pitr.sql.execute', { count: String(sqlChecked.length) })}
						{/if}
					</Button>
				</div>
			</CardFooter>
		</Card>

	<!-- ============ 步骤 5：执行 ============ -->
	{:else if step === 5}
		<Card>
			<CardHeader>
				<CardTitle>{t('pitr.exec.title')}</CardTitle>
				<CardDescription>{t('pitr.exec.subtitle', { mode, type: formatTypeLabel() })}</CardDescription>
			</CardHeader>
			<CardContent>
				<ExecutePanel
					status={opStatus}
					{progress}
					errors={execErrors}
					errorMessage={terminalMessage}
					{busy}
					onPause={handlePause}
					onResume={handleResume}
					onCancel={handleCancel}
				/>
			</CardContent>
			<CardFooter class="justify-end gap-2">
				{#if isTerminal}
					{#if opId}
						<Button variant="outline" onclick={() => goto(`/pitr/${encodeURIComponent(opId ?? '')}`)}>
							{t('pitr.viewDetail')}
						</Button>
					{/if}
					<Button onclick={resetWizard}>
						<RefreshCcw class="size-4" />
						{t('pitr.restart')}
					</Button>
				{/if}
			</CardFooter>
		</Card>
	{/if}
</div>
