<script lang="ts">
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
		LogOut
	} from '@lucide/svelte';
	import '../app.css';

	/** Routes reachable without a session. */
	const PUBLIC_PATHS = ['/login', '/register'];

	/** Where a signed-in user lands after auth (the instances list). */
	const HOME_PATH = '/instances';

	/**
	 * Sidebar navigation. `/operations`, `/audit` and `/orgs` land in Task 10 —
	 * the links are part of the intended structure and render now (a click
	 * before those routes exist falls through to the SPA error page, then
	 * resolves once the pages land).
	 */
	const NAV_ITEMS = [
		{ href: '/instances', label: 'nav.instances', icon: Server },
		{ href: '/operations', label: 'nav.operations', icon: History },
		{ href: '/audit', label: 'nav.audit', icon: ShieldCheck },
		{ href: '/orgs', label: 'nav.orgs', icon: Building2 }
	];

	let { children } = $props();

	let isAuthed = $derived(Boolean(auth.token));
	let isPublic = $derived(PUBLIC_PATHS.includes(page.url.pathname));

	// Route guard (pure SPA — runs client-side only, ssr=false).
	// Tracks both the current route (`page.url.pathname`) and the auth store,
	// so it reacts to logins, logouts and 401-driven session clears alike.
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
	<div class="flex min-h-screen bg-background">
		<aside class="flex h-screen w-60 shrink-0 flex-col border-r bg-sidebar text-sidebar-foreground">
			<div class="flex items-center gap-2 px-4 py-4">
				<img src="/favicon.svg" alt={t('app.name')} class="h-8 w-8" />
				<span class="text-sm font-semibold leading-tight">{t('app.name')}</span>
			</div>

			<nav class="flex-1 space-y-1 overflow-y-auto px-2 py-2">
				{#each NAV_ITEMS as item}
					{@const Icon = item.icon}
					<a
						href={item.href}
						class={isActive(item.href)
							? 'flex items-center gap-2 rounded-lg bg-sidebar-accent px-3 py-2 text-sm font-medium text-sidebar-accent-foreground'
							: 'flex items-center gap-2 rounded-lg px-3 py-2 text-sm text-sidebar-foreground/70 hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground'}
					>
						<Icon class="size-4 shrink-0" />
						{t(item.label)}
					</a>
				{/each}
			</nav>

			<div class="border-t p-3">
				{#if auth.user?.email}
					<p class="truncate px-2 text-xs text-muted-foreground" title={auth.user.email}>
						{auth.user.email}
					</p>
				{/if}
				<Button
					variant="ghost"
					size="sm"
					class="mt-1 w-full justify-start gap-2 text-sidebar-foreground/70 hover:text-sidebar-accent-foreground"
					onclick={handleLogout}
				>
					<LogOut class="size-4 shrink-0" />
					{t('nav.logout')}
				</Button>
			</div>
		</aside>

		<main class="min-w-0 flex-1">
			{@render children()}
		</main>
	</div>
{:else}
	{@render children()}
{/if}
