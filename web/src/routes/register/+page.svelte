<script lang="ts">
	import { goto } from '$app/navigation';
	import { t } from '$lib/i18n.svelte.js';
	import { login } from '$lib/auth.svelte.js';
	import { register } from '$lib/api/auth.js';
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
	let confirmPassword = $state('');
	let error = $state<string | null>(null);
	let submitting = $state(false);

	const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

	async function handleSubmit() {
		error = null;
		if (!email.trim() || !password || !confirmPassword) {
			error = t('auth.error.required');
			return;
		}
		if (!EMAIL_RE.test(email.trim())) {
			error = t('auth.error.invalidEmail');
			return;
		}
		if (password !== confirmPassword) {
			error = t('auth.error.passwordMismatch');
			return;
		}
		submitting = true;
		try {
			// 后端注册即返回 token —— 直接写入会话并进入控制台，无需再走登录页。
			const result = await register(email.trim(), password);
			login(result.token, result.user);
			await goto('/instances');
		} catch (e) {
			error = e instanceof Error ? e.message : t('auth.error.generic');
		} finally {
			submitting = false;
		}
	}
</script>

<svelte:head>
	<title>{t('auth.register.title')} · {t('app.name')}</title>
</svelte:head>

<div class="relative flex min-h-screen items-center justify-center overflow-hidden bg-background p-4">
	<!-- 背景网格 -->
	<div class="login-grid-bg absolute inset-0 opacity-30 animate-fade-in"></div>

	<!-- 浮动光晕 -->
	<div
		class="pointer-events-none absolute -left-32 -top-32 size-96 rounded-full bg-primary/10 blur-3xl animate-float"
		style="animation-delay: -3s;"
	></div>
	<div
		class="pointer-events-none absolute -bottom-40 -right-24 size-80 rounded-full bg-info/10 blur-3xl animate-float"
		style="animation-delay: -11s;"
	></div>

	<!-- 注册卡片 -->
	<Card class="relative w-full max-w-sm animate-fade-slide-up shadow-xl">
		<CardHeader class="text-center">
			<img
				src="/favicon.svg"
				alt={t('app.name')}
				class="mx-auto mb-3 h-10 w-10 animate-fade-in"
				style="animation-delay: 200ms;"
			/>
			<CardTitle class="animate-fade-in" style="animation-delay: 300ms;">
				{t('auth.register.title')}
			</CardTitle>
			<CardDescription class="animate-fade-in" style="animation-delay: 400ms;">
				{t('auth.register.subtitle')}
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
					<Label for="register-email">{t('auth.email')}</Label>
					<Input
						id="register-email"
						type="email"
						autocomplete="email"
						placeholder={t('auth.email.placeholder')}
						bind:value={email}
						required
					/>
				</div>
				<div class="grid gap-2">
					<Label for="register-password">{t('auth.password')}</Label>
					<Input
						id="register-password"
						type="password"
						autocomplete="new-password"
						bind:value={password}
						required
					/>
				</div>
				<div class="grid gap-2">
					<Label for="register-confirm-password">{t('auth.register.confirmPassword')}</Label>
					<Input
						id="register-confirm-password"
						type="password"
						autocomplete="new-password"
						bind:value={confirmPassword}
						required
					/>
				</div>

				{#if error}
					<p class="text-sm text-destructive" role="alert">{error}</p>
				{/if}

				<Button type="submit" class="w-full" disabled={submitting}>
					{submitting ? t('common.loading') : t('auth.register.submit')}
				</Button>
			</form>

			<p class="mt-4 text-center text-sm text-muted-foreground">
				{t('auth.hasAccount')}
				<a href="/login" class="font-medium text-primary hover:underline">
					{t('auth.login.link')}
				</a>
			</p>
		</CardContent>
	</Card>
</div>
