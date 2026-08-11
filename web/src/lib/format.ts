/**
 * Small presentation helpers shared by the PITR pages/components.
 */

/** Format an ISO timestamp (RFC3339 from the backend) for display. */
export function formatDateTime(value?: string | null): string {
	if (!value) return '—';
	const d = new Date(value);
	if (Number.isNaN(d.getTime())) return value;
	return d.toLocaleString();
}

/** Compact a long id (transaction id, operation id) for table cells. */
export function shortId(id: string, head = 8, tail = 6): string {
	if (id.length <= head + tail + 1) return id;
	return `${id.slice(0, head)}…${id.slice(-tail)}`;
}

/** Render a table list ("db.users, db.orders") for filter summaries. */
export function tablesLabel(
	tables: { schema?: string; table?: string }[] | undefined,
	max = 3
): string {
	if (!tables || tables.length === 0) return '';
	const parts = tables.map((t) => (t.schema ? `${t.schema}.${t.table}` : t.table));
	if (parts.length > max) return `${parts.slice(0, max).join(', ')} +${parts.length - max}`;
	return parts.join(', ');
}

/** Render a transaction's table list, bounded for the row cell. */
export function txTablesLabel(tables?: { schema?: string; table?: string }[]): string {
	if (!tables || tables.length === 0) return '—';
	return tables.map((t) => (t.schema ? `${t.schema}.${t.table}` : t.table)).join(', ');
}

/** Truncate a long SQL statement for preview rows. */
export function truncateSql(sql: string, max = 200): string {
	if (sql.length <= max) return sql;
	return `${sql.slice(0, max)}…`;
}
