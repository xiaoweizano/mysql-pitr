<script lang="ts">
	import { t } from '$lib/i18n.svelte.js';
	import type { ExecError, PitrState, ProgressPayload } from '$lib/api/pitr.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import { CircleAlert, Pause, Play, Square, CheckCircle2 } from '@lucide/svelte';

	/**
	 * Execution progress + control panel. Shared by the wizard step 5 and the
	 * operation detail page. Pure presentation: the parent owns the SSE
	 * subscription and the API calls, and feeds `status` / `progress` /
	 * `errors` in.
	 *
	 * Props:
	 * - `status`: current operation state.
	 * - `progress`: latest SSE progress payload (null until the first event).
	 * - `errors`: accumulated statement errors (progress + terminal payload).
	 * - `errorMessage`: terminal failure message (op_error / blocked), null when
	 *   the operation did not fail.
	 * - `busy`: a pause/resume/cancel call is in flight (buttons disabled).
	 * - `onPause` / `onResume` / `onCancel`: control actions.
	 */
	let {
		status,
		progress = null,
		errors = [],
		errorMessage = null,
		busy = false,
		onPause,
		onResume,
		onCancel
	}: {
		status: PitrState;
		progress?: ProgressPayload | null;
		errors?: ExecError[];
		errorMessage?: string | null;
		busy?: boolean;
		onPause?: () => void;
		onResume?: () => void;
		onCancel?: () => void;
	} = $props();

	const done = $derived(progress?.done ?? 0);
	const total = $derived(progress?.total ?? 0);
	const percent = $derived(total > 0 ? Math.min(100, Math.round((done / total) * 100)) : null);

	const showErrors = $derived(
		errors.length > 0 &&
			(status === 'executing' ||
				status === 'paused' ||
				status === 'done' ||
				status === 'failed' ||
				status === 'blocked')
	);
</script>

{#if status === 'executing' || status === 'paused'}
	<div>
		<div class="flex items-center justify-between text-xs text-zinc-500">
			<span>{t('pitr.exec.progress')}</span>
			<span class="font-mono">
				{done} / {total > 0 ? total : '—'}
				{#if percent !== null}（{percent}%）{/if}
			</span>
		</div>
		<div class="mt-1.5 h-2 w-full overflow-hidden rounded-full bg-zinc-100">
			{#if percent !== null}
				<div class="h-full rounded-full bg-zinc-900 transition-all" style={`width: ${percent}%`}></div>
			{:else}
				<div class="h-full w-1/3 animate-pulse rounded-full bg-zinc-300"></div>
			{/if}
		</div>

		{#if status === 'paused'}
			<div class="mt-3 flex items-center gap-2 rounded-md bg-amber-50 px-2.5 py-2 text-sm text-amber-700">
				<Pause class="size-4 shrink-0" />
				<span>{t('pitr.exec.pausedBanner')}</span>
			</div>
		{/if}

		<div class="mt-3 flex items-center gap-2">
			{#if status === 'executing' && onPause}
				<Button variant="outline" size="sm" onclick={onPause} disabled={busy}>
					<Pause class="size-3.5" />
					{t('pitr.exec.pause')}
				</Button>
			{/if}
			{#if status === 'paused' && onResume}
				<Button variant="default" size="sm" onclick={onResume} disabled={busy}>
					<Play class="size-3.5" />
					{t('pitr.exec.resume')}
				</Button>
			{/if}
			{#if onCancel}
				<Button variant="destructive" size="sm" onclick={onCancel} disabled={busy}>
					<Square class="size-3.5" />
					{t('pitr.exec.cancel')}
				</Button>
			{/if}
		</div>
	</div>
{:else if status === 'done'}
	<div class="flex items-start gap-2 rounded-md bg-emerald-50 px-2.5 py-2 text-sm text-emerald-700">
		<CheckCircle2 class="mt-0.5 size-4 shrink-0" />
		<div>
			<p class="font-medium">
				{t('pitr.exec.done', { done: String(done), total: String(total) })}
			</p>
			{#if errorMessage}
				<p class="mt-1 text-red-600">{errorMessage}</p>
			{/if}
		</div>
	</div>
{:else if status === 'failed' || status === 'blocked'}
	<div class="flex items-start gap-2 rounded-md bg-red-50 px-2.5 py-2 text-sm text-red-700">
		<CircleAlert class="mt-0.5 size-4 shrink-0" />
		<div>
			<p class="font-medium">{status === 'failed' ? t('pitr.exec.failed') : t('pitr.exec.blocked')}</p>
			{#if errorMessage}
				<p class="mt-1 break-all">{errorMessage}</p>
			{/if}
		</div>
	</div>
{:else if status === 'cancelled'}
	<div class="flex items-center gap-2 rounded-md bg-zinc-100 px-2.5 py-2 text-sm text-zinc-600">
		<Square class="size-4 shrink-0" />
		<span>{t('pitr.exec.cancelled')}</span>
	</div>
{/if}

{#if showErrors}
	<div class="mt-3">
		<p class="mb-1 flex items-center gap-1 text-sm font-medium text-destructive">
			<CircleAlert class="size-4" />
			{t('pitr.exec.errorsTitle', { count: String(errors.length) })}
		</p>
		<ul class="max-h-56 space-y-1 overflow-y-auto rounded-md bg-red-50 p-2 text-xs">
			{#each errors as err, i (i)}
				<li class="flex items-start gap-2 text-red-700">
					<span class="shrink-0 font-mono font-semibold">#{err.statement + 1}</span>
					<span class="min-w-0 break-all">{err.err}</span>
				</li>
			{/each}
		</ul>
	</div>
{/if}
