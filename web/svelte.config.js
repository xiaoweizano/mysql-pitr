import adapter from '@sveltejs/adapter-static';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	kit: {
		// Pure SPA: adapter-static with a single fallback page.
		// Combined with `export const ssr = false` in src/hooks.js,
		// every route is served from index.html and rendered client-side.
		adapter: adapter({ fallback: 'index.html' })
	}
};

export default config;
