<script lang="ts">
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
	import {
		Dialog,
		DialogContent,
		DialogDescription,
		DialogFooter,
		DialogHeader,
		DialogTitle,
		DialogTrigger
	} from '$lib/components/ui/dialog/index.js';

	let dialogOpen = $state(false);

	const demoTables = [
		{ name: 'mysql-01', status: 'online', binlog: 'binlog.000123' },
		{ name: 'mysql-02', status: 'online', binlog: 'binlog.000119' },
		{ name: 'mysql-03', status: 'paused', binlog: 'binlog.000085' }
	];
</script>

<svelte:head>
	<title>{t('app.name')}</title>
</svelte:head>

<div class="flex min-h-screen flex-col items-center bg-zinc-50 p-8">
	<div class="w-full max-w-2xl">
		<div class="flex items-center gap-3">
			<img src="/favicon.svg" alt={t('app.name')} class="h-10 w-10" />
			<div>
				<h1 class="text-2xl font-semibold text-zinc-900">{t('home.title')}</h1>
				<p class="text-sm text-zinc-500">{t('home.subtitle')}</p>
			</div>
		</div>

		<Card class="mt-8">
			<CardHeader>
				<CardTitle>{t('home.title')} · 脚手架状态</CardTitle>
				<CardDescription>SvelteKit SPA · adapter-static · Tailwind v4 · shadcn-svelte</CardDescription>
			</CardHeader>
			<CardContent>
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>实例</TableHead>
							<TableHead>状态</TableHead>
							<TableHead>Binlog</TableHead>
							<TableHead class="text-right">操作</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{#each demoTables as row}
							<TableRow>
								<TableCell class="font-medium">{row.name}</TableCell>
								<TableCell>{row.status}</TableCell>
								<TableCell>{row.binlog}</TableCell>
								<TableCell class="text-right">
									<Button variant="outline" size="sm" onclick={() => (dialogOpen = true)}>
										{t('common.confirm')}
									</Button>
								</TableCell>
							</TableRow>
						{/each}
					</TableBody>
				</Table>

				<div class="mt-6 flex items-center justify-between">
					<p class="text-xs text-zinc-400">{t('common.loading')}</p>
					<Dialog bind:open={dialogOpen}>
						<DialogTrigger>
							<Button>{t('common.save')}</Button>
						</DialogTrigger>
						<DialogContent>
							<DialogHeader>
								<DialogTitle>对话框组件</DialogTitle>
								<DialogDescription>shadcn-svelte dialog + bits-ui 已可用</DialogDescription>
							</DialogHeader>
							<DialogFooter>
								<Button variant="outline" onclick={() => (dialogOpen = false)}>
									{t('common.cancel')}
								</Button>
								<Button onclick={() => (dialogOpen = false)}>{t('common.confirm')}</Button>
							</DialogFooter>
						</DialogContent>
					</Dialog>
				</div>
			</CardContent>
		</Card>

		<p class="mt-6 text-center text-xs text-zinc-400">i18n · {t('app.name')} / 缺省中文</p>
	</div>
</div>
