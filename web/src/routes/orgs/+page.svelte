<script lang="ts">
	import { onMount } from 'svelte';
	import { t } from '$lib/i18n.svelte.js';
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
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table/index.js';
	import {
		listOrgs,
		createOrg,
		listMembers,
		inviteMember,
		acceptInvite,
		type OrgInfo,
		type OrgMember
	} from '$lib/api/orgs.js';
	import { formatDateTime, shortId } from '$lib/format.js';
	import { Building2, UserPlus } from '@lucide/svelte';

	/**
	 * Organisations page — list the user's orgs with their members, create a
	 * new org, invite members and accept invitations.
	 *
	 * NOTE on invites: `POST /api/orgs/{id}/invite` takes no request body (no
	 * email field) — it returns a one-time code to share out-of-band. The
	 * invitee pastes that code (plus the org id, which is not listed until
	 * they are a member) into the "加入组织" form.
	 */

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let orgs = $state<OrgInfo[]>([]);
	/** orgId → members of that org (fetched per card). */
	let membersByOrg = $state<Record<string, OrgMember[]>>({});

	// Create org form.
	let newOrgName = $state('');
	let creating = $state(false);
	let createError = $state<string | null>(null);

	// Invite form (per org card).
	let invitingOrgId = $state<string | null>(null);
	let inviteCodes = $state<Record<string, string>>({});
	let inviteError = $state<string | null>(null);

	// Accept invite form.
	let acceptOrgId = $state('');
	let acceptCode = $state('');
	let accepting = $state(false);
	let acceptError = $state<string | null>(null);
	let acceptSuccess = $state<string | null>(null);

	async function load() {
		loading = true;
		loadError = null;
		try {
			orgs = await listOrgs();
			await Promise.all(orgs.map((o) => loadMembers(o.id)));
		} catch (e) {
			loadError = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function loadMembers(orgId: string) {
		try {
			membersByOrg = { ...membersByOrg, [orgId]: await listMembers(orgId) };
		} catch {
			membersByOrg = { ...membersByOrg, [orgId]: [] };
		}
	}

	onMount(() => {
		void load();
	});

	async function handleCreate() {
		if (!newOrgName.trim() || creating) return;
		creating = true;
		createError = null;
		try {
			const org = await createOrg(newOrgName.trim());
			newOrgName = '';
			orgs = [...orgs, org];
			await loadMembers(org.id);
		} catch (e) {
			createError = e instanceof Error ? e.message : String(e);
		} finally {
			creating = false;
		}
	}

	async function handleInvite(orgId: string) {
		if (invitingOrgId !== null) return;
		invitingOrgId = orgId;
		inviteError = null;
		try {
			const code = await inviteMember(orgId);
			inviteCodes = { ...inviteCodes, [orgId]: code };
		} catch (e) {
			inviteError = e instanceof Error ? e.message : String(e);
		} finally {
			invitingOrgId = null;
		}
	}

	async function handleAccept() {
		if (!acceptOrgId.trim() || !acceptCode.trim() || accepting) return;
		accepting = true;
		acceptError = null;
		acceptSuccess = null;
		try {
			const org = await acceptInvite(acceptOrgId.trim(), acceptCode.trim());
			acceptCode = '';
			acceptSuccess = org.name;
			await load();
		} catch (e) {
			acceptError = e instanceof Error ? e.message : String(e);
		} finally {
			accepting = false;
		}
	}

	function roleLabel(role: string): string {
		return role === 'admin' ? t('orgs.role.admin') : t('orgs.role.member');
	}
</script>

<svelte:head>
	<title>{t('orgs.title')} · {t('app.name')}</title>
</svelte:head>

<div class="p-6">
	<div>
		<h1 class="text-xl font-semibold text-zinc-900">{t('orgs.title')}</h1>
		<p class="text-sm text-zinc-500">{t('orgs.subtitle')}</p>
	</div>

	<div class="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
		<Card>
			<CardHeader>
				<CardTitle class="flex items-center gap-2">
					<UserPlus class="size-4 text-zinc-400" />
					{t('orgs.create')}
				</CardTitle>
				<CardDescription>{t('orgs.createDesc')}</CardDescription>
			</CardHeader>
			<CardContent>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						void handleCreate();
					}}
					class="flex items-end gap-2"
				>
					<div class="grid flex-1 gap-1.5">
						<Label for="org-name">{t('orgs.createName')}</Label>
						<Input
							id="org-name"
							bind:value={newOrgName}
							placeholder={t('orgs.createNamePlaceholder')}
							required
						/>
					</div>
					<Button type="submit" disabled={creating}>
						{creating ? t('common.loading') : t('orgs.createSubmit')}
					</Button>
				</form>
				{#if createError}
					<p class="mt-3 text-sm text-destructive" role="alert">
						{t('orgs.createError')}：{createError}
					</p>
				{/if}
			</CardContent>
		</Card>

		<Card>
			<CardHeader>
				<CardTitle class="flex items-center gap-2">
					<Building2 class="size-4 text-zinc-400" />
					{t('orgs.accept')}
				</CardTitle>
				<CardDescription>{t('orgs.acceptDesc')}</CardDescription>
			</CardHeader>
			<CardContent>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						void handleAccept();
					}}
					class="grid gap-3 sm:grid-cols-2"
				>
					<div class="grid gap-1.5">
						<Label for="accept-org-id">{t('orgs.acceptOrgId')}</Label>
						<Input
							id="accept-org-id"
							bind:value={acceptOrgId}
							placeholder={t('orgs.acceptOrgIdPlaceholder')}
							required
						/>
					</div>
					<div class="grid gap-1.5">
						<Label for="accept-code">{t('orgs.acceptCode')}</Label>
						<Input
							id="accept-code"
							bind:value={acceptCode}
							placeholder={t('orgs.acceptCodePlaceholder')}
							required
						/>
					</div>
					<div class="sm:col-span-2">
						<Button type="submit" disabled={accepting}>
							{accepting ? t('common.loading') : t('orgs.acceptSubmit')}
						</Button>
					</div>
				</form>
				{#if acceptError}
					<p class="mt-3 text-sm text-destructive" role="alert">
						{t('orgs.acceptError')}：{acceptError}
					</p>
				{:else if acceptSuccess}
					<p class="mt-3 text-sm text-emerald-600" role="status">
						{t('orgs.acceptSuccess', { org: acceptSuccess })}
					</p>
				{/if}
			</CardContent>
		</Card>
	</div>

	{#if loadError}
		<Card class="mt-4">
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">{t('orgs.loadError')}：{loadError}</p>
				<Button variant="outline" size="sm" onclick={load} disabled={loading}>
					{loading ? t('common.loading') : t('common.retry')}
				</Button>
			</CardContent>
		</Card>
	{:else if loading}
		<Card class="mt-4">
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else if orgs.length === 0}
		<Card class="mt-4">
			<CardContent class="flex flex-col items-center gap-2 py-10 text-center">
				<p class="text-sm font-medium text-zinc-600">{t('orgs.empty')}</p>
				<p class="text-xs text-zinc-400">{t('orgs.emptyDesc')}</p>
			</CardContent>
		</Card>
	{:else}
		<div class="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
			{#each orgs as o (o.id)}
				{@const members = membersByOrg[o.id] ?? []}
				<Card>
					<CardHeader>
						<CardTitle>{o.name}</CardTitle>
						<CardDescription>
							<span class="font-mono text-xs" title={o.id}>{shortId(o.id)}</span>
							<span class="mx-1">·</span>
							{t('orgs.createdAt')}：{formatDateTime(o.createdAt)}
						</CardDescription>
					</CardHeader>
					<CardContent>
						{#if inviteError}
							<p class="mb-3 text-sm text-destructive" role="alert">
								{t('orgs.inviteError')}：{inviteError}
							</p>
						{/if}
						{#if inviteCodes[o.id]}
							<div class="mb-3 flex items-center gap-2 rounded-lg border border-dashed px-3 py-2">
								<span class="text-sm text-zinc-500">{t('orgs.inviteCode')}：</span>
								<code class="break-all font-mono text-sm text-zinc-900">{inviteCodes[o.id]}</code>
							</div>
						{/if}
						<Table>
							<TableHeader>
								<TableRow>
									<TableHead>{t('orgs.memberId')}</TableHead>
									<TableHead>{t('orgs.memberRole')}</TableHead>
									<TableHead>{t('orgs.memberJoined')}</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{#each members as m (m.userId)}
									<TableRow>
										<TableCell class="font-mono text-xs">{m.userId}</TableCell>
										<TableCell>{roleLabel(m.role)}</TableCell>
										<TableCell>{formatDateTime(m.joinedAt)}</TableCell>
									</TableRow>
								{:else}
									<TableRow>
										<TableCell colspan={3} class="text-center text-sm text-zinc-400">
											{t('orgs.membersEmpty')}
										</TableCell>
									</TableRow>
								{/each}
							</TableBody>
						</Table>
						<div class="mt-3">
							<Button
								variant="outline"
								size="sm"
								disabled={invitingOrgId !== null}
								onclick={() => void handleInvite(o.id)}
							>
								{invitingOrgId === o.id ? t('common.loading') : t('orgs.invite')}
							</Button>
						</div>
					</CardContent>
				</Card>
			{/each}
		</div>
	{/if}
</div>
