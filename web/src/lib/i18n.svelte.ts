import zhCN from './locales/zh-CN.json';

export type Locale = 'zh-CN';

const messages: Record<Locale, Record<string, string>> = {
	'zh-CN': zhCN
};

/**
 * Current locale (Svelte 5 runes `$state` — reactive).
 * Object-wrapped: Svelte 5 forbids reassigning an *exported* module-level
 * `$state` binding, so callers mutate `locale.value` instead.
 * Default is zh-CN; the web console is Chinese-first.
 */
export const locale = $state<{ value: Locale }>({ value: 'zh-CN' });

/**
 * Translate a key. Falls back to the key itself when missing,
 * so new UI strings degrade gracefully before translations land.
 *
 * Optional `params` enables `{name}` interpolation in the message —
 * e.g. `t('pitr.sql.checkedSummary', { checked: '2', total: '5', sql: '9' })`.
 */
export function t(key: string, params?: Record<string, string | number>): string {
	const msg = messages[locale.value][key] ?? key;
	if (!params) return msg;
	return msg.replace(/\{(\w+)\}/g, (m, name: string) =>
		params[name] !== undefined ? String(params[name]) : m
	);
}

export function setLocale(next: Locale) {
	locale.value = next;
}
