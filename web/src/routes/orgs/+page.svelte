<script lang="ts">
	import { onMount } from 'svelte';
	import { t } from '$lib/i18n.svelte.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import { Card, CardContent } from '$lib/components/ui/card/index.js';
	import * as Dialog from '$lib/components/ui/dialog/index.js';
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
		deleteOrg,
		type OrgInfo,
		type OrgMember
	} from '$lib/api/orgs.js';
	import { formatDateTime, shortId } from '$lib/format.js';
	import {
		Building2,
		Check,
		ChevronDown,
		ChevronRight,
		Copy,
		Trash2,
		UserPlus,
		Users
	} from '@lucide/svelte';

	/**
	 * Organisations page — a single list of the user's orgs. Create and join
	 * moved into dialogs (they are one-shot actions, not permanent page
	 * blocks); the org list itself is a table: one row per org, expandable to
	 * its members, with invite/delete actions inline.
	 *
	 * NOTE on invites: `POST /api/orgs/{id}/invite` takes no request body (no
	 * email field) — it returns a one-time code to share out-of-band. The
	 * invitee pastes that code (plus the org id, which is not listed until
	 * they are a member) into the "加入组织" dialog.
	 */

	let loading = $state(true);
	let loadError = $state<string | null>(null);
	let orgs = $state<OrgInfo[]>([]);
	/** orgId → members of that org (fetched per row). */
	let membersByOrg = $state<Record<string, OrgMember[]>>({});

	// Create-org dialog.
	let createOpen = $state(false);
	let newOrgName = $state('');
	let creating = $state(false);
	let createError = $state<string | null>(null);

	// Join-org dialog.
	let joinOpen = $state(false);
	let acceptOrgId = $state('');
	let acceptCode = $state('');
	let accepting = $state(false);
	let acceptError = $state<string | null>(null);
	let acceptSuccess = $state<string | null>(null);

	// Invite (per org row).
	let invitingOrgId = $state<string | null>(null);
	let inviteCodes = $state<Record<string, string>>({});
	let inviteError = $state<string | null>(null);

	// Delete org.
	let deleteError = $state<string | null>(null);
	let deletingOrgId = $state<string | null>(null);

	/** orgId of the row whose members are expanded. */
	let expandedOrgId = $state<string | null>(null);

	// 复制组织 ID / 邀请码（完整值复制，页面显示截断不影响复制）
	let copied = $state<string | null>(null);
	let copyTimer: ReturnType<typeof setTimeout> | undefined;

	async function copyValue(label: string, value: string) {
		if (await copyText(value)) {
			copied = label;
			clearTimeout(copyTimer);
			copyTimer = setTimeout(() => (copied = null), 1500);
		}
	}

	// navigator.clipboard 只在 HTTPS/localhost 下可用;http://<IP> 访问时回退
	// textarea + execCommand,否则复制按钮会静默失效(此前 catch 为空 = 点了没反应)。
	async function copyText(value: string): Promise<boolean> {
		try {
			await navigator.clipboard.writeText(value);
			return true;
		} catch {
			try {
				const ta = document.createElement('textarea');
				ta.value = value;
				ta.style.position = 'fixed';
				ta.style.opacity = '0';
				document.body.appendChild(ta);
				ta.select();
				const ok = document.execCommand('copy');
				ta.remove();
				return ok;
			} catch {
				return false;
			}
		}
	}

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

	function openCreate() {
		newOrgName = '';
		createError = null;
		createOpen = true;
	}

	async function handleCreate() {
		if (!newOrgName.trim() || creating) return;
		creating = true;
		createError = null;
		try {
			const org = await createOrg(newOrgName.trim());
			createOpen = false;
			expandedOrgId = org.id;
			await load();
		} catch (e) {
			createError = e instanceof Error ? e.message : String(e);
		} finally {
			creating = false;
		}
	}

	function openJoin() {
		acceptOrgId = '';
		acceptCode = '';
		acceptError = null;
		acceptSuccess = null;
		joinOpen = true;
	}

	async function handleAccept() {
		if (!acceptOrgId.trim() || !acceptCode.trim() || accepting) return;
		accepting = true;
		acceptError = null;
		acceptSuccess = null;
		try {
			const org = await acceptInvite(acceptOrgId.trim(), acceptCode.trim());
			acceptSuccess = org.name;
			await load();
			setTimeout(() => (joinOpen = false), 800);
		} catch (e) {
			acceptError = e instanceof Error ? e.message : String(e);
		} finally {
			accepting = false;
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

	async function handleDelete(orgId: string) {
		if (!window.confirm(t('orgs.deleteConfirm'))) return;
		deletingOrgId = orgId;
		deleteError = null;
		try {
			await deleteOrg(orgId);
			orgs = orgs.filter((o) => o.id !== orgId);
			const { [orgId]: _, ...rest } = membersByOrg;
			membersByOrg = rest;
			if (expandedOrgId === orgId) expandedOrgId = null;
		} catch (e) {
			deleteError = e instanceof Error ? e.message : String(e);
		} finally {
			deletingOrgId = null;
		}
	}

	function roleLabel(role: string): string {
		return role === 'admin' ? t('orgs.role.admin') : t('orgs.role.member');
	}
</script>

<svelte:head>
	<title>{t('orgs.title')} · {t('app.name')}</title>
</svelte:head>

<div class="flex h-full flex-col p-6">
	<div class="flex shrink-0 flex-wrap items-center justify-between gap-3">
		<div>
			<h1 class="text-xl font-semibold text-zinc-900">{t('orgs.title')}</h1>
			<p class="text-sm text-zinc-500">{t('orgs.subtitle')}</p>
		</div>
		<div class="flex items-center gap-2">
			<Button variant="outline" onclick={openJoin}>
				<UserPlus class="size-4" />
				{t('orgs.accept')}
			</Button>
			<Button onclick={openCreate}>
				<Building2 class="size-4" />
				{t('orgs.create')}
			</Button>
		</div>
	</div>

	{#if loadError}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-3 py-8 text-center">
				<p class="text-sm text-destructive">{t('orgs.loadError')}：{loadError}</p>
				<Button variant="outline" size="sm" onclick={load} disabled={loading}>
					{loading ? t('common.loading') : t('common.retry')}
				</Button>
			</CardContent>
		</Card>
	{:else if loading}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex items-center justify-center py-8 text-sm text-zinc-400">
				{t('common.loading')}
			</CardContent>
		</Card>
	{:else if orgs.length === 0}
		<Card class="mt-4 shrink-0">
			<CardContent class="flex flex-col items-center gap-2 py-10 text-center">
				<Building2 class="size-8 text-zinc-300" />
				<p class="text-sm font-medium text-zinc-600">{t('orgs.empty')}</p>
				<p class="text-xs text-zinc-400">{t('orgs.emptyDesc')}</p>
				<div class="mt-2 flex items-center gap-2">
					<Button variant="outline" size="sm" onclick={openJoin}>{t('orgs.accept')}</Button>
					<Button size="sm" onclick={openCreate}>{t('orgs.create')}</Button>
				</div>
			</CardContent>
		</Card>
	{:else}
		{#if inviteError}
			<p class="mt-3 shrink-0 text-sm text-destructive" role="alert">
				{t('orgs.inviteError')}：{inviteError}
			</p>
		{/if}
		{#if deleteError}
			<p class="mt-3 shrink-0 text-sm text-destructive" role="alert">
				{t('orgs.deleteError')}：{deleteError}
			</p>
		{/if}

		<Card class="mt-4 min-h-0 flex-1">
			<CardContent class="min-h-0 flex-1 overflow-y-auto">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead class="sticky top-0 z-10 w-8 bg-card"></TableHead>
							<TableHead class="sticky top-0 z-10 bg-card">{t('orgs.name')}</TableHead>
							<TableHead class="sticky top-0 z-10 bg-card">{t('orgs.id')}</TableHead>
							<TableHead class="sticky top-0 z-10 bg-card">{t('orgs.createdAt')}</TableHead>
							<TableHead class="sticky top-0 z-10 bg-card text-right">{t('orgs.membersCount')}</TableHead>
							<TableHead class="sticky top-0 z-10 bg-card text-right">{t('orgs.actions')}</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{#each orgs as o (o.id)}
							{@const expanded = expandedOrgId === o.id}
							{@const members = membersByOrg[o.id] ?? []}
							<TableRow
								class="cursor-pointer"
								onclick={() => (expandedOrgId = expanded ? null : o.id)}
							>
								<TableCell>
									{#if expanded}
										<ChevronDown class="size-4 text-zinc-400" />
									{:else}
										<ChevronRight class="size-4 text-zinc-400" />
									{/if}
								</TableCell>
								<TableCell class="font-medium">{o.name}</TableCell>
								<TableCell>
									<span class="flex items-center gap-2">
										<span class="font-mono text-xs text-zinc-500" title={o.id}>
											{shortId(o.id)}
										</span>
										<button
											type="button"
											class="inline-flex shrink-0 items-center gap-1 rounded-md border border-zinc-200 px-1.5 py-0.5 text-xs text-zinc-500 transition-colors hover:border-primary/50 hover:text-primary"
											onclick={(e) => {
												e.stopPropagation();
												copyValue(o.id, o.id);
											}} title={o.id}
										>
											{#if copied === o.id}
												<Check class="size-3.5 text-emerald-600" />
												<span class="text-emerald-600">{t('orgs.copied')}</span>
											{:else}
												<Copy class="size-3.5" />
												<span>{t('orgs.copyId')}</span>
											{/if}
										</button>
									</span>
								</TableCell>
								<TableCell class="whitespace-nowrap text-zinc-500">{formatDateTime(o.createdAt)}</TableCell>
								<TableCell class="text-right text-zinc-500">
									<span class="inline-flex items-center gap-1">
										<Users class="size-3.5" />
										{members.length}
									</span>
								</TableCell>
								<TableCell class="text-right">
									<div class="flex items-center justify-end gap-2" role="group">
										<Button
											variant="outline"
											size="xs"
											disabled={invitingOrgId !== null}
											onclick={(e) => {
												e.stopPropagation();
												void handleInvite(o.id);
											}}
										>
											{invitingOrgId === o.id ? t('common.loading') : t('orgs.invite')}
										</Button>
										<Button
											variant="destructive"
											size="xs"
											disabled={deletingOrgId !== null}
											onclick={(e) => {
												e.stopPropagation();
												void handleDelete(o.id);
											}}
										>
											<Trash2 class="size-3.5" />
											{deletingOrgId === o.id ? t('common.loading') : t('orgs.delete')}
										</Button>
									</div>
								</TableCell>
							</TableRow>
							{#if expanded}
								<TableRow>
									<TableCell></TableCell>
									<TableCell colspan={5}>
										<div class="py-1">
											{#if inviteCodes[o.id]}
												<div class="mb-3 flex flex-wrap items-center gap-2 rounded-lg border border-dashed px-3 py-2">
													<span class="text-sm text-zinc-500">{t('orgs.inviteCode')}：</span>
													<code class="break-all font-mono text-sm text-zinc-900">{inviteCodes[o.id]}</code>
													<button
														type="button"
														class="inline-flex shrink-0 items-center gap-1 rounded-md border border-zinc-200 px-1.5 py-0.5 text-xs text-zinc-500 transition-colors hover:border-primary/50 hover:text-primary"
														onclick={() => copyValue(`code-${o.id}`, inviteCodes[o.id])}
														title={inviteCodes[o.id]}
													>
														{#if copied === `code-${o.id}`}
															<Check class="size-3.5 text-emerald-600" />
															<span class="text-emerald-600">{t('orgs.copied')}</span>
														{:else}
															<Copy class="size-3.5" />
															<span>{t('orgs.copyId')}</span>
														{/if}
													</button>
													<span class="text-xs text-zinc-400">{t('orgs.inviteCodeHint')}</span>
												</div>
											{/if}
											<div class="rounded-lg border border-zinc-100">
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
											</div>
										</div>
									</TableCell>
								</TableRow>
							{/if}
						{/each}
					</TableBody>
				</Table>
			</CardContent>
		</Card>
	{/if}
</div>

<!-- 创建组织弹窗 -->
<Dialog.Root bind:open={createOpen}>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{t('orgs.create')}</Dialog.Title>
			<Dialog.Description>{t('orgs.createDesc')}</Dialog.Description>
		</Dialog.Header>
		<form
			class="grid gap-1.5"
			onsubmit={(e) => {
				e.preventDefault();
				void handleCreate();
			}}
		>
			<Label for="org-name">{t('orgs.createName')}</Label>
			<Input
				id="org-name"
				bind:value={newOrgName}
				placeholder={t('orgs.createNamePlaceholder')}
				required
			/>
			{#if createError}
				<p class="mt-2 text-sm text-destructive" role="alert">
					{t('orgs.createError')}：{createError}
				</p>
			{/if}
			<Dialog.Footer class="mt-2">
				<Button type="button" variant="outline" onclick={() => (createOpen = false)}>
					{t('common.cancel')}
				</Button>
				<Button type="submit" disabled={creating}>
					{creating ? t('common.loading') : t('orgs.createSubmit')}
				</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>

<!-- 加入组织弹窗 -->
<Dialog.Root bind:open={joinOpen}>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{t('orgs.accept')}</Dialog.Title>
			<Dialog.Description>{t('orgs.acceptDesc')}</Dialog.Description>
		</Dialog.Header>
		<form
			class="grid gap-3"
			onsubmit={(e) => {
				e.preventDefault();
				void handleAccept();
			}}
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
			{#if acceptError}
				<p class="text-sm text-destructive" role="alert">
					{t('orgs.acceptError')}：{acceptError}
				</p>
			{:else if acceptSuccess}
				<p class="text-sm text-emerald-600" role="status">
					{t('orgs.acceptSuccess', { org: acceptSuccess })}
				</p>
			{/if}
			<Dialog.Footer>
				<Button type="button" variant="outline" onclick={() => (joinOpen = false)}>
					{t('common.cancel')}
				</Button>
				<Button type="submit" disabled={accepting}>
					{accepting ? t('common.loading') : t('orgs.acceptSubmit')}
				</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>
