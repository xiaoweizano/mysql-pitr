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

<div class="relative flex min-h-screen items-center justify-center overflow-hidden bg-background">
	<!-- 背景网格 -->
	<div class="login-grid-bg absolute inset-0 opacity-30 animate-fade-in"></div>

	<!-- 浮动光晕 -->
	<div
		class="pointer-events-none absolute -left-32 -top-32 size-96 rounded-full bg-primary/10 blur-3xl animate-float"
		style="animation-delay: 0s;"
	></div>
	<div
		class="pointer-events-none absolute -bottom-40 -right-24 size-80 rounded-full bg-info/10 blur-3xl animate-float"
		style="animation-delay: -7s;"
	></div>

	<!-- 登录卡片 -->
	<Card class="relative w-full max-w-sm animate-fade-slide-up shadow-xl">
		<CardHeader class="text-center">
			<img
				src="/favicon.svg"
				alt={t('app.name')}
				class="mx-auto mb-3 h-10 w-10 animate-fade-in"
				style="animation-delay: 200ms;"
			/>
			<CardTitle class="animate-fade-in" style="animation-delay: 300ms;">
				{t('auth.login.title')}
			</CardTitle>
			<CardDescription class="animate-fade-in" style="animation-delay: 400ms;">
				{t('auth.login.subtitle')}
			</CardDescription>
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
					<p class="text-sm text-destructive" role="alert">
						{error}
					</p>
				{/if}

				<Button type="submit" class="w-full" disabled={submitting}>
					{submitting ? t('common.loading') : t('auth.login.submit')}
				</Button>
			</form>

			<p class="mt-4 text-center text-sm text-muted-foreground">
				{t('auth.noAccount')}
				<a href="/register" class="font-medium text-primary hover:underline">
					{t('auth.register.link')}
				</a>
			</p>
		</CardContent>
	</Card>
</div>