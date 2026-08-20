<script lang="ts">
	import { goto } from '$app/navigation';
	import { auth } from '$lib/auth.svelte.js';
	import { t } from '$lib/i18n.svelte.js';

	// 根路径根据会话状态跳转：已登录 → 实例列表；未登录 → 登录页（由 layout 守卫处理）
	$effect(() => {
		void goto(auth.token ? '/instances' : '/login', { replaceState: true });
	});
</script>

<svelte:head>
	<title>{t('app.name')}</title>
</svelte:head>

<div class="flex min-h-screen items-center justify-center bg-background">
	<div class="flex flex-col items-center gap-3 animate-fade-in">
		<img src="/favicon.svg" alt={t('app.name')} class="h-10 w-10 animate-pulse" />
		<p class="text-sm text-muted-foreground">{t('common.loading')}</p>
	</div>
</div>