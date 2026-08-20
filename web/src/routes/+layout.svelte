<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { auth, logout } from '$lib/auth.svelte.js';
	import { t } from '$lib/i18n.svelte.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import {
		Server,
		History,
		ShieldCheck,
		Building2,
		LogOut,
		Sun,
		Moon
	} from '@lucide/svelte';
	import '../app.css';

	const PUBLIC_PATHS = ['/login', '/register'];
	const HOME_PATH = '/instances';

	const NAV_ITEMS = [
		{ href: '/instances', label: 'nav.instances', icon: Server },
		{ href: '/operations', label: 'nav.operations', icon: History },
		{ href: '/audit', label: 'nav.audit', icon: ShieldCheck },
		{ href: '/orgs', label: 'nav.orgs', icon: Building2 }
	];

	let { children } = $props();

	let isAuthed = $derived(Boolean(auth.token));
	let isPublic = $derived(PUBLIC_PATHS.includes(page.url.pathname));

	// 主题切换（深色默认）
	let dark = $state(true);

	onMount(() => {
		// 初始应用深色类，确保 Tailwind dark: 变体生效
		document.documentElement.classList.add('dark');
	});

	function toggleTheme() {
		dark = !dark;
		document.documentElement.classList.toggle('dark', dark);
		document.documentElement.classList.toggle('light', !dark);
	}

	$effect(() => {
		const path = page.url.pathname;
		if (!isPublic && !auth.token) {
			void goto('/login', { replaceState: true });
		} else if (isPublic && auth.token) {
			void goto(HOME_PATH, { replaceState: true });
		}
	});

	function isActive(href: string): boolean {
		const path = page.url.pathname;
		return path === href || path.startsWith(`${href}/`);
	}

	function handleLogout() {
		logout();
		void goto('/login', { replaceState: true });
	}
</script>

{#if isAuthed && !isPublic}
	<div class="flex h-screen overflow-hidden bg-background">
		<!-- 侧边栏 -->
		<aside class="relative flex h-screen w-52 shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground">
			<!-- Logo 区域 -->
			<div class="flex items-center gap-2.5 px-4 py-4">
				<img src="/favicon.svg" alt={t('app.name')} class="h-7 w-7 shrink-0" />
				<span class="text-sm font-semibold leading-tight tracking-tight">{t('app.name')}</span>
			</div>

			<!-- 导航 -->
			<nav class="flex-1 space-y-0.5 overflow-y-auto px-2 py-2">
				{#each NAV_ITEMS as item}
					{@const Icon = item.icon}
					{@const active = isActive(item.href)}
					<a
						href={item.href}
						class="relative flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-all duration-150
							{active
								? 'bg-sidebar-accent text-sidebar-accent-foreground'
								: 'text-sidebar-foreground/60 hover:bg-sidebar-accent/60 hover:text-sidebar-foreground/90'}"
					>
						{#if active}
							<span class="sidebar-active-indicator"></span>
						{/if}
						<Icon class="size-4 shrink-0" />
						{t(item.label)}
					</a>
				{/each}
			</nav>

			<!-- 底部：主题切换 + 用户 -->
			<div class="border-t border-sidebar-border p-3 space-y-2">
				<button
					type="button"
					onclick={toggleTheme}
					class="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-sm text-sidebar-foreground/60
						transition-colors duration-150 hover:bg-sidebar-accent/60 hover:text-sidebar-foreground/90"
				>
					{#if dark}
						<Sun class="size-4 shrink-0" />
						<span>{t('nav.lightMode')}</span>
					{:else}
						<Moon class="size-4 shrink-0" />
						<span>{t('nav.darkMode')}</span>
					{/if}
				</button>

				{#if auth.user?.email}
					<p class="truncate px-3 text-xs text-sidebar-foreground/40" title={auth.user.email}>
						{auth.user.email}
					</p>
				{/if}
				<Button
					variant="ghost"
					size="sm"
					class="w-full justify-start gap-2.5 text-sidebar-foreground/60 hover:text-sidebar-accent-foreground"
					onclick={handleLogout}
				>
					<LogOut class="size-4 shrink-0" />
					{t('nav.logout')}
				</Button>
			</div>
		</aside>

		<!-- 主内容区 -->
		<main class="flex h-screen min-w-0 flex-1 flex-col overflow-hidden animate-fade-in">
			{@render children()}
		</main>
	</div>
{:else}
	{@render children()}
{/if}