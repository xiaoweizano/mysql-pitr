<script lang="ts">
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { auth } from '$lib/auth.svelte.js';
	import '../app.css';

	/** Routes reachable without a session. */
	const PUBLIC_PATHS = ['/login', '/register'];

	/** Where a signed-in user lands after auth (the instances list, Task 8). */
	const HOME_PATH = '/instances';

	let { children } = $props();

	// Route guard (pure SPA — runs client-side only, ssr=false).
	// Tracks both the current route (`page.url.pathname`) and the auth store,
	// so it reacts to logins, logouts and 401-driven session clears alike.
	$effect(() => {
		const path = page.url.pathname;
		const isPublic = PUBLIC_PATHS.includes(path);
		if (!isPublic && !auth.token) {
			void goto('/login', { replaceState: true });
		} else if (isPublic && auth.token) {
			void goto(HOME_PATH, { replaceState: true });
		}
	});
</script>

{@render children()}
