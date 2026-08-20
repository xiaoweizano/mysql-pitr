<script lang="ts">
	import { t } from '$lib/i18n.svelte.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import { ChevronLeft, ChevronRight } from '@lucide/svelte';

	/**
	 * Reusable pagination controls. The parent owns the `page` and `pageSize`
	 * state; this component emits changes via bindable props.
	 *
	 * Props:
	 * - `total`: total number of items.
	 * - `page`: current 1-based page number (bindable).
	 * - `pageSize`: items per page (bindable, defaults to 10).
	 */
	let {
		total,
		page = $bindable(1),
		pageSize = $bindable(10)
	}: {
		total: number;
		page?: number;
		pageSize?: number;
	} = $props();

	const totalPages = $derived(Math.max(1, Math.ceil(total / pageSize)));
	const startItem = $derived(total === 0 ? 0 : (page - 1) * pageSize + 1);
	const endItem = $derived(Math.min(page * pageSize, total));

	function prev() {
		if (page > 1) page--;
	}

	function next() {
		if (page < totalPages) page++;
	}

	$effect(() => {
		// Reset to page 1 if current page exceeds total pages.
		if (page > totalPages) page = totalPages;
	});
</script>

{#if total > 0}
	<div class="flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
		<div class="flex items-center gap-3">
			<span>{t('common.pagination.total', { total: String(total) })}</span>
			<span>{startItem}–{endItem}</span>
			<label class="flex items-center gap-1">
				{t('common.pagination.pageSize')}：
				<select
					class="h-7 rounded border border-input bg-background px-1.5 text-xs"
					bind:value={pageSize}
					onchange={() => (page = 1)}
				>
					<option value={10}>10</option>
					<option value={20}>20</option>
					<option value={50}>50</option>
					<option value={100}>100</option>
				</select>
			</label>
		</div>
		<div class="flex items-center gap-2">
			<span>{t('common.pagination.page', { page: String(page), pages: String(totalPages) })}</span>
			<Button variant="outline" size="sm" onclick={prev} disabled={page <= 1}>
				<ChevronLeft class="size-3.5" />
				{t('common.pagination.prev')}
			</Button>
			<Button variant="outline" size="sm" onclick={next} disabled={page >= totalPages}>
				{t('common.pagination.next')}
				<ChevronRight class="size-3.5" />
			</Button>
		</div>
	</div>
{/if}
