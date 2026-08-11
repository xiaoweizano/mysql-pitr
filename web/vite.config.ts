import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';

// No SvelteKit options are passed here — all Kit config (adapter, etc.)
// lives in svelte.config.js. Passing options to `sveltekit()` would cause
// svelte.config.js to be ignored entirely (SvelteKit >= 2.62).
export default defineConfig({
	plugins: [sveltekit(), tailwindcss()]
});
