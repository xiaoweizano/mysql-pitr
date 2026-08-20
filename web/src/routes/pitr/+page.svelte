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
	import { decideScanDone, selectionChanged, syncPreviewCounts, txMatchesOpType, type ScanFlowMode } from '$lib/pitr/scan-flow.js';
	import TxTable from '$lib/components/pitr/TxTable.svelte';
	import SqlPreview from '$lib/components/pitr/SqlPreview.svelte';
	import ExecutePanel from '$lib/components/pitr/ExecutePanel.svelte';
	import { LoaderCircle, CircleAlert, Database, RefreshCcw, ArrowLeft, ArrowRight, Check } from '@lucide/svelte';

	// ── 类型 ──

	type TypeChoice = 'flashback' | 'update_rollback' | 'pitr_time' | 'pitr_tx' | 'gtid';

	const TYPE_OPTIONS: { value: TypeChoice; title: string; desc: string; icon: string }[] = [
		{ value: 'flashback', title: 'pitr.type.flashback', desc: 'pitr.type.flashback.desc', icon: 'trash' },
		{ value: 'update_rollback', title: 'pitr.type.updateRollback', desc: 'pitr.type.updateRollback.desc', icon: 'undo' },
		{ value: 'pitr_time', title: 'pitr.type.pitrTime', desc: 'pitr.type.pitrTime.desc', icon: 'clock' },
		{ value: 'pitr_tx', title: 'pitr.type.pitrTx', desc: 'pitr.type.pitrTx.desc', icon: 'list-checks' },
		{ value: 'gtid', title: 'pitr.type.gtid', desc: 'pitr.type.gtid.desc', icon: 'hash' }
	];

	const STEP_LABELS = ['pitr.step.1.label', 'pitr.step.2.label', 'pitr.step.3.label', 'pitr.step.4.label', 'pitr.step.5.label'];

	let step = $state(1);
	let typeChoice = $state<TypeChoice>('pitr_time');
	let agentId = $state('');
	let agents = $state<AgentInfo[]>([]);
	let agentsLoading = $state(true);
	let agentsError = $state<string | null>(null);
	let offlinePreselect = $state(false);
	let transitioning = $state(false);

	const mode = $derived<PitrMode>(typeChoice === 'pitr_tx' || typeChoice === 'gtid' ? 'selected' : 'sql');
	const scanMode = $derived<ScanFlowMode>(mode === 'selected' ? 'selected' : 'sql');
	const opType = $derived<PitrType>(
		typeChoice === 'flashback' ? 'flashback' : typeChoice === 'update_rollback' ? 'update_rollback' : 'pitr'
	);

	// Step transition: show new step content with animation key
	let stepKey = $state(1);

	function goToStep(n: number) {
		if (opId === null && (n === 1 || n === 2)) {
			transitioning = true;
			setTimeout(() => {
				step = n;
				stepKey = n;
				setTimeout(() => { transitioning = false; }, 20);
			}, 150);
		}
	}

	function advanceStep() {
		transitioning = true;
		setTimeout(() => {
			step = step + 1;
			stepKey = step;
			setTimeout(() => { transitioning = false; }, 20);
		}, 150);
	}

	// ── 过滤条件 ──

	let tablesText = $state('');
	let timeStart = $state('');
	let timeEnd = $state('');
	let gtidText = $state('');
	let maxRowsPerTx = $state<number | null>(null);
	let formError = $state<string | null>(null);
	let starting = $state(false);

	// ── 操作状态 ──

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

	// ── SSE ──

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

	const SCAN_POLL_MS = 3_000;
	$effect(() => {
		const id = opId;
		if (
			!id ||
			isTerminalState(opStatus) ||
			(step < 3 && sseConnected) ||
			(step > 4 && sseConnected)
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
						if (step === 4) advanceStep();
						return;
					}
					if (st.status === 'executing' || st.status === 'paused') {
						if (step === 4) advanceStep();
						return;
					}
				}
				if (opStatus === 'ready') {
					if (step === 3) {
						await advanceAfterScanDone();
					} else if (step === 4) {
						await refreshTransactions();
					}
				}
			} catch {
				// transient
			} finally {
				inFlight = false;
			}
		}, SCAN_POLL_MS);
		return () => clearInterval(iv);
	});

	// ── agent 加载 ──

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

	// ── filter 解析 ──

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

	function toRFC3339(value: string): string | null {
		const d = new Date(value);
		return Number.isNaN(d.getTime()) ? null : d.toISOString();
	}

	function buildFilter(): { filter: ScanFilter; error: string | null } {
		const { refs, error: tableError } = parseTables(tablesText);
		if (tableError) return { filter: {}, error: tableError };

		const filter: ScanFilter = {};
		if (refs.length > 0) filter.tables = refs;
		const startIso = timeStart ? toRFC3339(timeStart) : null;
		if (timeStart && !startIso) return { filter, error: t('pitr.filter.error.invalidTime') };
		if (startIso) filter.timeStart = startIso;
		const endIso = timeEnd ? toRFC3339(timeEnd) : null;
		if (timeEnd && !endIso) return { filter, error: t('pitr.filter.error.invalidTime') };
		if (endIso) filter.timeEnd = endIso;
		if (gtidText.trim()) filter.gtidSet = gtidText.trim();
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
				advanceStep();
			} finally {
				starting = false;
			}
		} catch (e) {
			formError = e instanceof Error ? e.message : String(e);
		}
	}

	// ── SSE 事件 ──

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
			// SSE events missed before the subscription landed (or dropped by the
			// non-replaying bus) leave the live counters at 0 — the snapshot is
			// authoritative, so lift the counters to match it.
			const synced = syncPreviewCounts(received, sqlReceived, txs);
			received = synced.received;
			sqlReceived = synced.sqlReceived;
		} catch {
			// SSE keeps streaming
		}
	}

	async function advanceAfterScanDone() {
		await refreshTransactions();
		const d = decideScanDone(scanMode, rescanning, pendingExecute);
		if (d.initSqlChecked) initSqlChecked();
		if (d.advanceStep) {
			rescanning = false;
			advanceStep();
		}
		if (d.continueExecute) {
			pendingExecute = false;
			void startExecute();
		}
	}

	function initSqlChecked() {
		// Default selection = what the preview SHOWS: apply the same
		// recovery-type filter SqlPreview uses (e.g. update_rollback → only
		// transactions carrying UPDATE statements). Selecting hidden
		// transactions would execute SQL the operator never reviewed.
		const withSql = txs
			.filter((tx) => txMatchesOpType(tx, opType))
			.map((tx) => tx.txId);
		// selected 模式默认勾选步骤 3 所选事务。
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
				opStatus = 'paused';
				break;
			}
			case 'op_done': {
				const payload = data as { done?: number; total?: number; paused?: boolean; errors?: ExecError[]; status?: string };
				if (payload?.paused) { opStatus = 'paused'; return; }
				opStatus = payload?.status === 'cancelled' ? 'cancelled' : 'done';
				if (opStatus === 'cancelled') terminalMessage = t('pitr.error.cancelled');
				if (payload?.done !== undefined) progress = { done: payload.done, total: payload.total ?? 0 };
				mergeErrors(payload?.errors);
				advanceStep();
				break;
			}
			case 'op_error': {
				opStatus = 'failed';
				terminalMessage = extractErrorMessage(data);
				advanceStep();
				break;
			}
		}
	}

	// ── selected-mode ──

	async function handleSelect() {
		if (!opId) return;
		if (selected.length === 0) { flowError = t('pitr.scan.selectRequired'); return; }
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

	// ── execute ──

	async function reconcileAfterExecuteFailure() {
		if (!opId) return;
		try {
			const st = await getStatus(opId);
			opStatus = st.status;
			if (st.status === 'executing' || st.status === 'paused' || isTerminalState(st.status)) {
				flowError = null;
			}
		} catch { }
	}

	async function startExecute() {
		if (!opId) return;
		const txIds = [...sqlChecked];
		if (txIds.length === 0) { flowError = t('pitr.sql.required'); return; }
		startingExec = true;
		flowError = null;
		try {
			if (selectionChanged(lastSelected, txIds)) {
				const sel = await selectTx(opId, txIds);
				lastSelected = [...txIds];
				if (sel.rescan) {
					pendingExecute = true;
					rescanning = true;
					opStatus = sel.status;
					advanceStep();
					return;
				}
			}
			const ex = await executeOp(opId);
			opStatus = ex.status;
			advanceStep();
		} catch (e) {
			flowError = e instanceof Error ? e.message : String(e);
			await reconcileAfterExecuteFailure();
		} finally {
			startingExec = false;
		}
	}

	// ── execution controls ──

	async function handlePause() {
		if (!opId) return;
		busy = true;
		try { const r = await pauseOp(opId); opStatus = r.status; }
		catch (e) { terminalMessage = e instanceof Error ? e.message : String(e); }
		finally { busy = false; }
	}

	async function handleResume() {
		if (!opId) return;
		busy = true;
		try { const r = await resumeOp(opId); opStatus = r.status; }
		catch (e) { terminalMessage = e instanceof Error ? e.message : String(e); }
		finally { busy = false; }
	}

	async function handleCancel() {
		if (!opId) return;
		busy = true;
		try {
			const r = await cancelOp(opId);
			opStatus = r.status;
			if (r.status === 'cancelled') terminalMessage = t('pitr.error.cancelled');
		} catch (e) { terminalMessage = e instanceof Error ? e.message : String(e); }
		finally { busy = false; }
	}

	// ── navigation / reset ──

	function resetWizard() {
		step = 1; stepKey = 1;
		opId = null; opStatus = 'created';
		txs = []; received = 0; sqlReceived = 0;
		selected = []; sqlChecked = []; lastSelected = null;
		progress = null; execErrors = []; terminalMessage = null;
		previewTruncated = false; rescanning = false; pendingExecute = false;
		flowError = null; formError = null;
	}

	const isTerminal = $derived(isTerminalState(opStatus));

	function selectedAgentOnline(): boolean {
		const a = agents.find((x) => x.id === agentId);
		return a ? a.status === 'online' : true;
	}

	function formatTypeLabel(): string {
		return t(`pitr.type.short.${opType}`);
	}

	function formatAgentName(): string {
		const a = agents.find((x) => x.id === agentId);
		return a ? a.hostname : '—';
	}
</script>

<svelte:head>
	<title>{t('pitr.title')} · {t('app.name')}</title>
</svelte:head>

<div class="h-full overflow-y-auto p-6 animate-fade-in">
	<!-- 页面标题 -->
	<div class="flex flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-foreground">{t('pitr.title')}</h1>
			<p class="text-sm text-muted-foreground">{t('pitr.subtitle')}</p>
		</div>
	</div>

	<!-- 步骤指示器 -->
	<div class="mt-6 mb-6">
		<ol class="flex items-center gap-0">
			{#each [1, 2, 3, 4, 5] as n, i}
				<li class="flex items-center">
					<button
						type="button"
						onclick={() => goToStep(n)}
						disabled={opId !== null}
						class="flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-medium transition-all duration-200 disabled:cursor-not-allowed
							{step === n
								? 'bg-primary/15 text-primary'
								: step > n
									? 'text-success'
									: 'text-muted-foreground hover:bg-muted/50'}"
					>
						<span class="inline-flex size-5 items-center justify-center rounded-full text-[11px] font-semibold
							{step === n
								? 'bg-primary text-primary-foreground'
								: step > n
									? 'bg-success/15 text-success'
									: 'bg-muted text-muted-foreground'}">
							{#if step > n}
								<Check class="size-3" />
							{:else}
								{i + 1}
							{/if}
						</span>
						<span class="hidden sm:inline">{t(STEP_LABELS[n - 1])}</span>
					</button>
					{#if i < 4}
						<div class="mx-1 h-px w-6 bg-border {step > n ? 'bg-success/30' : ''}"></div>
					{/if}
				</li>
			{/each}
		</ol>
	</div>

	<!-- 操作摘要（步骤 3-5 时显示） -->
	{#if opId && step >= 3}
		<div class="mb-4 flex items-center gap-3 rounded-lg bg-muted/50 px-3 py-2 text-xs text-muted-foreground">
			<span class="font-medium text-foreground/70">{t('pitr.type.title')}：{formatTypeLabel()}</span>
			<span class="text-border">|</span>
			<span>{t('pitr.agent.title')}：{formatAgentName()}</span>
			{#if step >= 3}
				<span class="text-border">|</span>
				<span>{t('pitr.scan.received', { count: String(received), sql: String(sqlReceived) })}</span>
			{/if}
		</div>
	{/if}

	<!-- 全局横幅 -->
	{#if flowError}
		<div class="mb-4 flex items-start gap-2 rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">
			<CircleAlert class="mt-0.5 size-4 shrink-0" />
			<span class="break-all">{flowError}</span>
		</div>
	{/if}
	{#if sseReconnecting && !isTerminal}
		<div class="mb-4 flex items-center gap-2 rounded-lg bg-warning/10 px-3 py-2 text-sm text-warning">
			<LoaderCircle class="size-4 animate-spin" />
			<span>{t('pitr.sse.reconnecting')}</span>
		</div>
	{/if}

	<!-- 步骤内容（带过渡动画） -->
	<div class="transition-all duration-300 ease-out"
		class:opacity-0={transitioning}
		class:translate-y-2={transitioning}
		class:opacity-100={!transitioning}
		class:translate-y-0={!transitioning}>
		<!-- ═══════ 步骤 1：类型与实例 ═══════ -->
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
								onclick={() => { typeChoice = opt.value; formError = null; }}
								class="rounded-lg border p-3 text-left transition-all duration-150 card-hover
									{typeChoice === opt.value
										? 'border-primary bg-primary/10 text-foreground'
										: 'border-border bg-card hover:border-primary/50 text-foreground/80 hover:text-foreground'}"
							>
								<p class="text-sm font-medium">{t(opt.title)}</p>
								<p class="mt-1 text-xs text-muted-foreground">
									{t(opt.desc)}
								</p>
							</button>
						{/each}
					</div>

					<div class="mt-5">
						<Label>{t('pitr.agent.title')}</Label>
						{#if agentsLoading}
							<div class="mt-2 grid grid-cols-1 gap-1.5 sm:grid-cols-2">
								{#each [1, 2, 3] as _}
									<div class="skeleton h-12 rounded-lg"></div>
								{/each}
							</div>
						{:else if agentsError}
							<div class="mt-2 flex items-center justify-between gap-2 rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">
								<span class="break-all">{agentsError}</span>
								<Button variant="outline" size="xs" onclick={loadAgents}>{t('common.retry')}</Button>
							</div>
						{:else if agents.length === 0}
							<div class="mt-2 rounded-lg bg-muted px-3 py-2 text-sm text-muted-foreground">
								{t('pitr.agent.none')}
							</div>
						{:else}
							<div class="mt-2 grid grid-cols-1 gap-1.5 sm:grid-cols-2">
								{#each agents as agent}
									<button
										type="button"
										onclick={() => { agentId = agent.id; formError = null; }}
										disabled={agent.status !== 'online'}
										class="flex items-center gap-2 rounded-lg border px-3 py-2 text-left text-sm transition-all duration-150 disabled:cursor-not-allowed disabled:opacity-40
											{agentId === agent.id
												? 'border-primary bg-primary/10 text-foreground'
												: 'border-border bg-card hover:border-primary/50 text-foreground/80 hover:text-foreground'}"
										title={agent.status !== 'online' ? t('pitr.agent.offline') : agent.hostname}
									>
										<Database class="size-4 shrink-0 text-muted-foreground" />
										<span class="min-w-0 flex-1 truncate">{agent.hostname}</span>
										<span class="status-badge border border-border/50">
											<span class={agent.status === 'online' ? 'status-dot status-dot--online status-dot--pulse' : 'status-dot status-dot--offline'}></span>
											{agent.status === 'online' ? t('instances.status.online') : t('instances.status.offline')}
										</span>
									</button>
								{/each}
							</div>
						{/if}
						{#if offlinePreselect}
							<p class="mt-2 text-xs text-warning">{t('pitr.agent.offlineHint')}</p>
						{/if}
					</div>
				</CardContent>
				<CardFooter class="justify-end gap-3">
					{#if formError}
						<p class="text-xs text-destructive">{formError}</p>
					{/if}
					<Button onclick={() => { advanceStep(); formError = null; }} disabled={!agentId || !selectedAgentOnline()}>
						{t('pitr.next')}
						<ArrowRight class="size-3.5" />
					</Button>
				</CardFooter>
			</Card>

		<!-- ═══════ 步骤 2：过滤条件 ═══════ -->
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
							class="mt-1.5 w-full rounded-lg border border-input bg-transparent px-2.5 py-1.5 text-sm outline-none transition-colors duration-150 focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
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
							class="mt-1.5 w-full rounded-lg border border-input bg-transparent px-2.5 py-1.5 font-mono text-xs outline-none transition-colors duration-150 focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
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
					<Button variant="outline" onclick={() => goToStep(1)}>
						<ArrowLeft class="size-3.5" />
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

		<!-- ═══════ 步骤 3：事务扫描 ═══════ -->
		{:else if step === 3}
			<Card>
				<CardHeader>
					<CardTitle>{t('pitr.scan.title')}</CardTitle>
					<CardDescription>
						{#if rescanning}
							<span class="text-warning">{t('pitr.scan.rescanning')}</span>
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
						<div class="mb-2 flex items-center gap-2 text-sm text-muted-foreground">
							<LoaderCircle class="size-4 animate-spin text-primary" />
							<span>{t('pitr.scan.received', { count: String(received), sql: String(sqlReceived) })}</span>
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
								<span class="text-sm text-warning">{t('pitr.scan.rescanning')}</span>
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

		<!-- ═══════ 步骤 4：SQL 预览 ═══════ -->
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
					<SqlPreview transactions={txs} bind:checked={sqlChecked} opType={opType} />
				</CardContent>
				<CardFooter class="justify-between">
					<Button variant="outline" onclick={handleCancel} disabled={busy || isTerminal}>
						{t('pitr.sql.cancelOp')}
					</Button>
					<div class="flex items-center gap-3">
						{#if flowError}
							<p class="text-xs text-destructive">{flowError}</p>
						{/if}
						<Button onclick={startExecute} disabled={startingExec || sqlChecked.length === 0}>
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

		<!-- ═══════ 步骤 5：执行 ═══════ -->
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
</div>