<script lang="ts">
	import { goto } from '$app/navigation';
	import { t } from '$lib/i18n.svelte.js';
	import { login } from '$lib/api/auth.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import {
		Card,
		CardContent,
		CardDescription,
		CardHeader,
		CardTitle
	} from '$lib/components/ui/card/index.js';
	import { Input } from '$lib/components/ui/input/index.js';
	import { Label } from '$lib/components/ui/label/index.js';

	let email = $state('');
	let password = $state('');
	let error = $state<string | null>(null);
	let submitting = $state(false);

	async function handleSubmit() {
		if (!email.trim() || !password) {
			error = t('auth.error.required');
			return;
		}
		submitting = true;
		error = null;
		try {
			await login(email.trim(), password);
			await goto('/instances');
		} catch (e) {
			error = e instanceof Error ? e.message : t('auth.error.generic');
		} finally {
			submitting = false;
		}
	}
</script>

<svelte:head>
	<title>{t('auth.login.title')} · {t('app.name')}</title>
</svelte:head>

<div class="flex min-h-screen items-center justify-center bg-zinc-50 p-4">
	<Card class="w-full max-w-sm">
		<CardHeader class="text-center">
			<img src="/favicon.svg" alt={t('app.name')} class="mx-auto mb-2 h-10 w-10" />
			<CardTitle>{t('auth.login.title')}</CardTitle>
			<CardDescription>{t('auth.login.subtitle')}</CardDescription>
		</CardHeader>
		<CardContent>
			<form
				onsubmit={(e) => {
					e.preventDefault();
					void handleSubmit();
				}}
				class="grid gap-4"
			>
				<div class="grid gap-2">
					<Label for="login-email">{t('auth.email')}</Label>
					<Input
						id="login-email"
						type="email"
						autocomplete="email"
						placeholder={t('auth.email.placeholder')}
						bind:value={email}
						required
					/>
				</div>
				<div class="grid gap-2">
					<Label for="login-password">{t('auth.password')}</Label>
					<Input
						id="login-password"
						type="password"
						autocomplete="current-password"
						bind:value={password}
						required
					/>
				</div>

				{#if error}
					<p class="text-sm text-destructive" role="alert">{error}</p>
				{/if}

				<Button type="submit" class="w-full" disabled={submitting}>
					{submitting ? t('common.loading') : t('auth.login.submit')}
				</Button>
			</form>

			<p class="mt-4 text-center text-sm text-zinc-500">
				{t('auth.noAccount')}
				<a href="/register" class="font-medium text-primary hover:underline">
					{t('auth.register.link')}
				</a>
			</p>
		</CardContent>
	</Card>
</div>
