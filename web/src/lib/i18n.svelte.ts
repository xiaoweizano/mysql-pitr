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
 */
export function t(key: string): string {
	return messages[locale.value][key] ?? key;
}

export function setLocale(next: Locale) {
	locale.value = next;
}
